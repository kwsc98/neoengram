use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    AgentResourceLifecycleAssignment, ArtifactId, DecimalU64, DeletionCompletion, DeletionId,
    DeletionImpact, DeletionMutation, DeletionMutationKind, DeletionOperation,
    DeletionOperationState, DeletionProof, DeliveryGeneration, LifecycleEvent, LifecycleGeneration,
    ProjectId, RequestId, ResourceLifecycle, ResourceLifecycleState, ResourceRef, ResourceVersion,
    RetentionHold, RetentionHoldId, RetentionHoldState, SnapshotDeliveryId, SnapshotDeliveryState,
    SnapshotId, StorageVolumeId, TenantId, UnixMillis, WorkspaceId, DELETION_IMPACT_TTL_MILLIS,
    DELETION_RECOVERY_WINDOW_MILLIS,
};

use crate::{
    validate_snapshot_delivery_mutation_request, validate_snapshot_delivery_parents,
    validate_snapshot_delivery_retention_roots, AdvanceWorkspaceCommitOutcome,
    AdvanceWorkspaceCommitRequest, ArtifactHeadExpectation, ArtifactInitialization,
    ArtifactListCursor, ArtifactListPage, ArtifactListRequest, ArtifactRecord,
    CatalogInsertOutcome, CentralError, CentralErrorCode, CentralResult, ControlCatalogRepository,
    CreateDeletionRequest, CreateRetentionHoldRequest, DeletionImpactQuery, DeletionImpactRecord,
    DeletionListCursor, DeletionListPage, DeletionListRequest, DeletionTransitionRequest,
    LifecycleAssignmentInsertOutcome, LifecycleAssignmentOutboxRecord, LifecycleEvidenceBatch,
    ProjectListCursor, ProjectListPage, ProjectListRequest, ProjectRecord,
    ReleaseRetentionHoldRequest, RestoreDeletionRequest, RetryDeletionRequest,
    S3AccessPointCreateResult, S3AccessPointInsertOutcome, S3AccessPointListPage,
    S3AccessPointListRequest, S3AccessPointRecord, S3AccessPointState, S3CredentialInsertOutcome,
    S3CredentialRecord, S3CredentialState, S3MutationKind, S3MutationRecord,
    SnapshotDeliveryInsertOutcome, SnapshotDeliveryInsertRequest, SnapshotDeliveryListRequest,
    SnapshotDeliveryMutationRecord, SnapshotDeliveryMutationRequest, SnapshotDeliveryRecord,
    SnapshotDeliveryRetentionRoot, SnapshotInsertRequest, SnapshotListCursor, SnapshotListPage,
    SnapshotListRequest, SnapshotRecord, SnapshotState, SnapshotWithDeliveryInsertRequest,
    SnapshotWithDeliveryInsertResult, StorageBackendType, StorageVolumeListCursor,
    StorageVolumeListPage, StorageVolumeListRequest, StorageVolumeRecord, StorageVolumeState,
    TenantListCursor, TenantListPage, TenantListRequest, TenantRecord, WorkspaceInsertRequest,
    WorkspaceListCursor, WorkspaceListPage, WorkspaceListRequest, WorkspaceRecord,
};

#[derive(Debug, Default)]
pub struct InMemoryControlCatalog {
    tenants: Mutex<BTreeMap<TenantId, TenantRecord>>,
    projects: Mutex<BTreeMap<(TenantId, ProjectId), ProjectRecord>>,
    artifacts: Mutex<BTreeMap<(TenantId, ArtifactId), ArtifactRecord>>,
    volumes: Mutex<BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>>,
    workspaces: Mutex<BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>>,
    snapshots: Mutex<BTreeMap<(TenantId, SnapshotId), SnapshotRecord>>,
    /// Serializes the aggregate publication path. The individual maps remain separately
    /// lockable for the legacy repository methods, while this fence keeps a Snapshot and its
    /// first Delivery on one linearizable in-memory write path.
    snapshot_delivery_aggregate: Mutex<()>,
    snapshot_deliveries: Mutex<BTreeMap<(TenantId, SnapshotDeliveryId), SnapshotDeliveryRecord>>,
    snapshot_delivery_requests: Mutex<BTreeMap<(TenantId, RequestId), SnapshotDeliveryRecord>>,
    snapshot_delivery_retention_roots: Mutex<
        std::collections::BTreeSet<(
            TenantId,
            SnapshotDeliveryId,
            neoengram_domain::core::ObjectId,
        )>,
    >,
    snapshot_delivery_mutations:
        Mutex<BTreeMap<(TenantId, RequestId), SnapshotDeliveryMutationRecord>>,
    s3_access_points: Mutex<
        BTreeMap<(TenantId, neoengram_domain::protocol::S3AccessPointId), S3AccessPointRecord>,
    >,
    s3_credentials: Mutex<BTreeMap<neoengram_domain::protocol::S3CredentialId, S3CredentialRecord>>,
    s3_mutations: Mutex<BTreeMap<(TenantId, RequestId), S3MutationRecord>>,
    deletion_impacts: Mutex<BTreeMap<(TenantId, ContentDigest), DeletionImpactRecord>>,
    deletion_operations: Mutex<BTreeMap<(TenantId, DeletionId), DeletionOperation>>,
    deletion_mutations: Mutex<BTreeMap<(TenantId, RequestId), DeletionMutation>>,
    retention_holds: Mutex<BTreeMap<(TenantId, RetentionHoldId), RetentionHold>>,
    lifecycle_events: Mutex<BTreeMap<neoengram_domain::protocol::LifecycleEventId, LifecycleEvent>>,
    deletion_proofs: Mutex<BTreeMap<neoengram_domain::protocol::DeletionProofId, DeletionProof>>,
    lifecycle_assignments: Mutex<
        BTreeMap<
            (TenantId, neoengram_domain::protocol::LifecycleAssignmentId),
            LifecycleAssignmentOutboxRecord,
        >,
    >,
}

impl InMemoryControlCatalog {
    /// Deterministic evidence inspection for state-machine tests.
    pub fn deletion_proofs(&self) -> CentralResult<Vec<DeletionProof>> {
        Ok(lock(&self.deletion_proofs)?.values().cloned().collect())
    }

    /// Returns the Artifact identities owned by a Project. Lifecycle cleanup uses this
    /// relationship to remove placement evidence without putting Project ownership on object
    /// receipts themselves.
    pub fn artifact_ids_for_project(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
    ) -> CentralResult<BTreeSet<ArtifactId>> {
        Ok(lock(&self.artifacts)?
            .values()
            .filter(|record| &record.tenant_id == tenant_id && &record.project_id == project_id)
            .map(|record| record.artifact_id.clone())
            .collect())
    }
}

#[async_trait]
impl ControlCatalogRepository for InMemoryControlCatalog {
    async fn get_tenant(&self, tenant_id: &TenantId) -> CentralResult<Option<TenantRecord>> {
        Ok(lock(&self.tenants)?.get(tenant_id).cloned())
    }

    async fn list_tenants(&self, request: &TenantListRequest) -> CentralResult<TenantListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.tenants)?
            .values()
            .filter(|record| {
                request
                    .visible_tenant_ids
                    .as_ref()
                    .is_none_or(|ids| ids.iter().any(|tenant_id| tenant_id == &record.tenant_id))
            })
            .filter(|record| {
                matches_query(
                    &record.tenant_id.to_string(),
                    &record.display_name,
                    request.query.as_deref(),
                )
            })
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| tenant_after(record, after))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.tenant_id.cmp(&right.tenant_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty keyset page");
            TenantListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                tenant_id: last.tenant_id.clone(),
            }
        });
        Ok(TenantListPage { records, next })
    }

    async fn insert_tenant(
        &self,
        record: TenantRecord,
    ) -> CentralResult<CatalogInsertOutcome<TenantRecord>> {
        let mut records = lock(&self.tenants)?;
        if let Some(existing) = records.get(&record.tenant_id) {
            return if existing.display_name == record.display_name
                && existing.description == record.description
            {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("Tenant ID is already used"))
            };
        }
        records.insert(record.tenant_id.clone(), record.clone());
        Ok(CatalogInsertOutcome::Inserted(record))
    }

    async fn get_project(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
    ) -> CentralResult<Option<ProjectRecord>> {
        Ok(lock(&self.projects)?
            .get(&(tenant_id.clone(), project_id.clone()))
            .cloned())
    }

    async fn list_projects(&self, request: &ProjectListRequest) -> CentralResult<ProjectListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.projects)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| record.lifecycle.is_active())
            .filter(|record| {
                matches_query(
                    &record.project_id.to_string(),
                    &record.display_name,
                    request.query.as_deref(),
                )
            })
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| project_after(record, after))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.project_id.cmp(&right.project_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty project page");
            ProjectListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                project_id: last.project_id.clone(),
            }
        });
        Ok(ProjectListPage { records, next })
    }

    async fn insert_project(
        &self,
        record: ProjectRecord,
    ) -> CentralResult<CatalogInsertOutcome<ProjectRecord>> {
        if !lock(&self.tenants)?.contains_key(&record.tenant_id) {
            return Err(conflict("Project Tenant does not exist"));
        }
        let mut records = lock(&self.projects)?;
        let key = (record.tenant_id.clone(), record.project_id.clone());
        if let Some(existing) = records.get(&key) {
            return if project_matches(existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("Project ID is already used"))
            };
        }
        records.insert(key, record.clone());
        Ok(CatalogInsertOutcome::Inserted(record))
    }

    async fn get_artifact(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Option<ArtifactRecord>> {
        Ok(lock(&self.artifacts)?
            .get(&(tenant_id.clone(), artifact_id.clone()))
            .filter(|record| &record.project_id == project_id)
            .filter(|record| record.lifecycle.is_active())
            .cloned())
    }

    async fn get_artifact_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Option<ArtifactRecord>> {
        Ok(lock(&self.artifacts)?
            .get(&(tenant_id.clone(), artifact_id.clone()))
            .filter(|record| &record.project_id == project_id)
            .cloned())
    }

    async fn list_artifacts(
        &self,
        request: &ArtifactListRequest,
    ) -> CentralResult<ArtifactListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.artifacts)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| record.lifecycle.is_active())
            .filter(|record| {
                request
                    .project_id
                    .as_ref()
                    .is_none_or(|id| &record.project_id == id)
            })
            .filter(|record| {
                matches_query(
                    &record.artifact_id.to_string(),
                    &record.display_name,
                    request.query.as_deref(),
                )
            })
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| artifact_after(record, after))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.project_id.cmp(&right.project_id))
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty keyset page");
            ArtifactListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                project_id: last.project_id.clone(),
                artifact_id: last.artifact_id.clone(),
            }
        });
        Ok(ArtifactListPage { records, next })
    }

    async fn insert_artifact(
        &self,
        record: ArtifactRecord,
    ) -> CentralResult<CatalogInsertOutcome<ArtifactRecord>> {
        validate_new_resource(record.resource_version, &record.lifecycle, "Artifact")?;
        if !lock(&self.tenants)?.contains_key(&record.tenant_id) {
            return Err(conflict("Artifact Tenant does not exist"));
        }
        let mut records = lock(&self.artifacts)?;
        let key = (record.tenant_id.clone(), record.artifact_id.clone());
        if let Some(existing) = records.get(&key) {
            return if artifact_matches(existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("Artifact ID is already used"))
            };
        }
        if let ArtifactInitialization::Derived {
            source_project_id,
            source_artifact_id,
            ..
        } = &record.initialization
        {
            let source_key = (record.tenant_id.clone(), source_artifact_id.clone());
            if records.get(&source_key).is_none_or(|source| {
                &source.project_id != source_project_id || !source.lifecycle.is_active()
            }) {
                return Err(conflict("Artifact initialization source does not exist"));
            }
        }
        records.insert(key, record.clone());
        Ok(CatalogInsertOutcome::Inserted(record))
    }

    async fn get_storage_volume(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Option<StorageVolumeRecord>> {
        Ok(lock(&self.volumes)?
            .get(&(tenant_id.clone(), storage_volume_id.clone()))
            .filter(|record| record.lifecycle.is_active())
            .cloned())
    }

    async fn get_storage_volume_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Option<StorageVolumeRecord>> {
        Ok(lock(&self.volumes)?
            .get(&(tenant_id.clone(), storage_volume_id.clone()))
            .cloned())
    }

    async fn list_storage_volumes(
        &self,
        request: &StorageVolumeListRequest,
    ) -> CentralResult<StorageVolumeListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.volumes)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| record.lifecycle.is_active())
            .filter(|record| {
                request
                    .region
                    .as_ref()
                    .is_none_or(|region| &record.region == region)
            })
            .filter(|record| {
                request
                    .backend_type
                    .is_none_or(|backend| record.backend_type == backend)
            })
            .filter(|record| {
                matches_query(
                    &record.storage_volume_id.to_string(),
                    &record.display_name,
                    request.query.as_deref(),
                )
            })
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| volume_after(record, after))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.storage_volume_id.cmp(&right.storage_volume_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty keyset page");
            StorageVolumeListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                storage_volume_id: last.storage_volume_id.clone(),
            }
        });
        Ok(StorageVolumeListPage { records, next })
    }

    async fn insert_storage_volume(
        &self,
        record: StorageVolumeRecord,
    ) -> CentralResult<CatalogInsertOutcome<StorageVolumeRecord>> {
        validate_new_resource(record.resource_version, &record.lifecycle, "StorageVolume")?;
        let mut records = lock(&self.volumes)?;
        let key = (record.tenant_id.clone(), record.storage_volume_id.clone());
        if let Some(existing) = records.get(&key) {
            return if volume_matches(existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("StorageVolume ID is already used"))
            };
        }
        if record.backend_type == StorageBackendType::Pvc
            && records.values().any(|existing| {
                existing.edge_cluster_id == record.edge_cluster_id
                    && existing.pvc_reference == record.pvc_reference
            })
        {
            return Err(CentralError::new(
                CentralErrorCode::VolumeOwnerConflict,
                "PVC identity is already registered",
            )
            .with_retryable(false));
        }
        records.insert(key, record.clone());
        Ok(CatalogInsertOutcome::Inserted(record))
    }

    async fn get_workspace(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        workspace_id: &WorkspaceId,
    ) -> CentralResult<Option<WorkspaceRecord>> {
        Ok(lock(&self.workspaces)?
            .get(&(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                workspace_id.clone(),
            ))
            .filter(|record| record.lifecycle.is_active())
            .cloned())
    }

    async fn get_workspace_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        workspace_id: &WorkspaceId,
    ) -> CentralResult<Option<WorkspaceRecord>> {
        Ok(lock(&self.workspaces)?
            .get(&(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                workspace_id.clone(),
            ))
            .cloned())
    }

    async fn list_workspaces(
        &self,
        request: &WorkspaceListRequest,
    ) -> CentralResult<WorkspaceListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.workspaces)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| record.lifecycle.is_active())
            .filter(|record| {
                request
                    .project_id
                    .as_ref()
                    .is_none_or(|id| &record.project_id == id)
            })
            .filter(|record| {
                request
                    .artifact_id
                    .as_ref()
                    .is_none_or(|id| &record.artifact_id == id)
            })
            .filter(|record| {
                request
                    .region
                    .as_ref()
                    .is_none_or(|region| &record.region == region)
            })
            .filter(|record| request.state.is_none_or(|state| record.state == state))
            .filter(|record| {
                matches_query(
                    &record.workspace_id.to_string(),
                    &record.display_name,
                    request.query.as_deref(),
                )
            })
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| workspace_after(record, after))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.project_id.cmp(&right.project_id))
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
                .then_with(|| left.workspace_id.cmp(&right.workspace_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty keyset page");
            WorkspaceListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                project_id: last.project_id.clone(),
                artifact_id: last.artifact_id.clone(),
                workspace_id: last.workspace_id.clone(),
            }
        });
        Ok(WorkspaceListPage { records, next })
    }

    async fn insert_workspace_fenced(
        &self,
        request: WorkspaceInsertRequest,
    ) -> CentralResult<CatalogInsertOutcome<WorkspaceRecord>> {
        let WorkspaceInsertRequest {
            record,
            artifact_head,
        } = request;
        validate_new_resource(record.resource_version, &record.lifecycle, "Workspace")?;
        // Keep this order fixed so validation and insertion form one atomic critical section.
        let artifacts = lock(&self.artifacts)?;
        let volumes = lock(&self.volumes)?;
        let mut records = lock(&self.workspaces)?;
        let key = (
            record.tenant_id.clone(),
            record.project_id.clone(),
            record.artifact_id.clone(),
            record.workspace_id.clone(),
        );
        if let Some(existing) = records.get(&key) {
            require_active(&existing.lifecycle, "Workspace")?;
            return if workspace_matches_insert(existing, &record, &artifact_head) {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("Workspace ID is already used"))
            };
        }
        let artifact = artifacts
            .get(&(record.tenant_id.clone(), record.artifact_id.clone()))
            .filter(|artifact| artifact.project_id == record.project_id)
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Workspace Artifact does not exist",
                )
            })?;
        require_active(&artifact.lifecycle, "Workspace Artifact")?;
        if record.base_commit_id != record.head_commit_id {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactHeadMismatch,
                "Workspace base Commit must match its initial head Commit",
            ));
        }
        if let ArtifactHeadExpectation::Exact(expected) = artifact_head {
            if record.base_commit_id != expected {
                return Err(CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "fenced Workspace base Commit does not match the observed Artifact Head",
                )
                .with_retryable(false));
            }
            if artifact.head_commit_id != expected {
                return Err(artifact_head_changed());
            }
        }
        let volume = volumes
            .get(&(record.tenant_id.clone(), record.storage_volume_id.clone()))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::StorageVolumeNotFound,
                    "Workspace StorageVolume does not exist",
                )
            })?;
        require_active(&volume.lifecycle, "Workspace StorageVolume")?;
        if volume.state != StorageVolumeState::Ready {
            return Err(catalog_parent_error(
                CentralErrorCode::StorageVolumeNotReady,
                "Workspace StorageVolume is not ready",
            ));
        }
        if record.region != volume.region {
            return Err(catalog_parent_error(
                CentralErrorCode::StorageVolumeRegionMismatch,
                "Workspace region must match the StorageVolume region",
            ));
        }
        records.insert(key, record.clone());
        Ok(CatalogInsertOutcome::Inserted(record))
    }

    async fn transition_workspace_state(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        workspace_id: &WorkspaceId,
        expected: crate::WorkspaceState,
        next: crate::WorkspaceState,
        updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<WorkspaceRecord> {
        let key = (
            tenant_id.clone(),
            project_id.clone(),
            artifact_id.clone(),
            workspace_id.clone(),
        );
        let mut records = lock(&self.workspaces)?;
        let record = records.get_mut(&key).ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "Workspace does not exist",
            )
        })?;
        require_active(&record.lifecycle, "Workspace")?;
        if record.state == next {
            return Ok(record.clone());
        }
        if record.state != expected {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Workspace is in {:?}, expected {:?} for state transition",
                    record.state, expected
                ),
            ));
        }
        record.state = next;
        record.updated_at_unix_ms = updated_at_unix_ms;
        Ok(record.clone())
    }

    async fn advance_workspace_commit(
        &self,
        request: AdvanceWorkspaceCommitRequest,
    ) -> CentralResult<AdvanceWorkspaceCommitOutcome> {
        let artifact_key = (request.tenant_id.clone(), request.artifact_id.clone());
        let workspace_key = (
            request.tenant_id.clone(),
            request.project_id.clone(),
            request.artifact_id.clone(),
            request.workspace_id.clone(),
        );
        let mut artifacts = lock(&self.artifacts)?;
        let mut workspaces = lock(&self.workspaces)?;
        let artifact = artifacts
            .get(&artifact_key)
            .filter(|record| record.project_id == request.project_id)
            .cloned()
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Commit Artifact does not exist",
                )
            })?;
        let workspace = workspaces.get(&workspace_key).cloned().ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "Commit Workspace does not exist",
            )
        })?;
        require_active(&artifact.lifecycle, "Commit Artifact")?;
        require_active(&workspace.lifecycle, "Commit Workspace")?;
        // A Workspace can publish from a historical Commit while another Workspace has moved
        // the Artifact's convenience Head. Once this Workspace already observes the new Commit,
        // treat the request as a replay without moving the Artifact pointer backwards.
        if workspace.head_commit_id == Some(request.commit_id) {
            return Ok(AdvanceWorkspaceCommitOutcome {
                artifact,
                workspace,
                replayed: true,
            });
        }
        if workspace.head_commit_id != request.expected_head_commit_id {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactHeadMismatch,
                "Workspace Head changed after Pre-commit",
            ));
        }
        if workspace.state != crate::WorkspaceState::Ready {
            return Err(catalog_parent_error(
                CentralErrorCode::InvalidState,
                "only a Ready Workspace can publish a Commit",
            ));
        }
        let next_resource_version = artifact.resource_version.checked_add(1).ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Artifact ResourceVersion is exhausted",
            )
        })?;
        let artifact = artifacts
            .get_mut(&artifact_key)
            .expect("Artifact was validated while holding the catalog lock");
        artifact.head_commit_id = Some(request.commit_id);
        artifact.resource_version = next_resource_version;
        artifact.updated_at_unix_ms = request.updated_at_unix_ms;
        let artifact = artifact.clone();
        let workspace = workspaces
            .get_mut(&workspace_key)
            .expect("Workspace was validated while holding the catalog lock");
        workspace.head_commit_id = Some(request.commit_id);
        workspace.updated_at_unix_ms = request.updated_at_unix_ms;
        let workspace = workspace.clone();
        Ok(AdvanceWorkspaceCommitOutcome {
            artifact,
            workspace,
            replayed: false,
        })
    }

    async fn get_snapshot(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
    ) -> CentralResult<Option<SnapshotRecord>> {
        Ok(lock(&self.snapshots)?
            .get(&(tenant_id.clone(), snapshot_id.clone()))
            .filter(|record| record.lifecycle.is_active())
            .cloned())
    }

    async fn get_snapshot_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
    ) -> CentralResult<Option<SnapshotRecord>> {
        Ok(lock(&self.snapshots)?
            .get(&(tenant_id.clone(), snapshot_id.clone()))
            .cloned())
    }

    async fn list_snapshots(
        &self,
        request: &SnapshotListRequest,
    ) -> CentralResult<SnapshotListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.snapshots)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| record.lifecycle.is_active())
            .filter(|record| {
                request
                    .project_id
                    .as_ref()
                    .is_none_or(|id| &record.project_id == id)
            })
            .filter(|record| {
                request
                    .artifact_id
                    .as_ref()
                    .is_none_or(|id| &record.artifact_id == id)
            })
            .filter(|record| request.commit_id.is_none_or(|id| record.commit_id == id))
            .filter(|record| request.state.is_none_or(|state| record.state == state))
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| snapshot_after(record, after))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.snapshot_id.cmp(&right.snapshot_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty keyset page");
            SnapshotListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                snapshot_id: last.snapshot_id.clone(),
            }
        });
        Ok(SnapshotListPage { records, next })
    }

    async fn insert_snapshot_with_delivery(
        &self,
        request: SnapshotWithDeliveryInsertRequest,
    ) -> CentralResult<SnapshotWithDeliveryInsertResult> {
        let SnapshotWithDeliveryInsertRequest {
            snapshot:
                SnapshotInsertRequest {
                    record: snapshot_request,
                    artifact_head,
                },
            delivery,
        } = request;
        let delivery_request = delivery.request_id.clone();
        let delivery_record = delivery.record.clone();

        validate_new_resource(
            snapshot_request.resource_version,
            &snapshot_request.lifecycle,
            "Snapshot",
        )?;
        validate_snapshot_delivery_retention_roots(&delivery)?;
        if snapshot_request.delivery_id != delivery_record.delivery_id
            || snapshot_request.tenant_id != delivery_record.tenant_id
            || snapshot_request.snapshot_id != delivery_record.snapshot_id
            || snapshot_request.commit_id != delivery_record.commit_id
            || snapshot_request.storage_volume_id != delivery_record.storage_volume_id
            || snapshot_request.delivery_mode != delivery_record.mode
        {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot and Delivery immutable identities do not match",
            )
            .with_retryable(false));
        }
        if delivery_record.create_request_id != delivery_request {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot delivery create request identity is inconsistent",
            )
            .with_retryable(false));
        }
        if snapshot_request.snapshot_request_id != delivery_request {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot and Delivery must share the same create request identity",
            )
            .with_retryable(false));
        }
        if delivery_record.delivery_generation.get() == 0 || delivery_record.resource_version == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot delivery generations must be positive",
            )
            .with_retryable(false));
        }

        // No await occurs while these guards are held. This makes the aggregate operation a
        // single in-memory critical section and ensures all validation completes before either
        // map is mutated.
        let _aggregate = lock(&self.snapshot_delivery_aggregate)?;
        let artifacts = lock(&self.artifacts)?;
        let volumes = lock(&self.volumes)?;
        let mut snapshots = lock(&self.snapshots)?;
        let mut deliveries = lock(&self.snapshot_deliveries)?;
        let mut delivery_requests = lock(&self.snapshot_delivery_requests)?;
        let mut retention_roots = lock(&self.snapshot_delivery_retention_roots)?;

        let snapshot_key = (
            snapshot_request.tenant_id.clone(),
            snapshot_request.snapshot_id.clone(),
        );
        let mut snapshot_replayed = false;
        let snapshot = if let Some(existing) = snapshots.values().find(|existing| {
            existing.tenant_id == snapshot_request.tenant_id
                && existing.snapshot_request_id == snapshot_request.snapshot_request_id
        }) {
            require_active(&existing.lifecycle, "Snapshot")?;
            if !snapshot_request_matches(existing, &snapshot_request) {
                return Err(conflict("Snapshot request ID is already used"));
            }
            snapshot_replayed = true;
            existing.clone()
        } else if snapshots.contains_key(&snapshot_key) {
            return Err(conflict("Snapshot ID is already used"));
        } else {
            let artifact = artifacts
                .get(&(
                    snapshot_request.tenant_id.clone(),
                    snapshot_request.artifact_id.clone(),
                ))
                .filter(|artifact| artifact.project_id == snapshot_request.project_id)
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::ArtifactNotFound,
                        "Snapshot Artifact does not exist",
                    )
                })?;
            require_active(&artifact.lifecycle, "Snapshot Artifact")?;
            if let ArtifactHeadExpectation::Exact(expected) = artifact_head {
                if expected != Some(snapshot_request.commit_id) {
                    return Err(CentralError::new(
                        CentralErrorCode::ProtocolInvalid,
                        "fenced Snapshot Commit does not match the observed Artifact Head",
                    )
                    .with_retryable(false));
                }
                if artifact.head_commit_id != expected {
                    return Err(artifact_head_changed());
                }
            }
            snapshot_request.clone()
        };

        let delivery_key = (
            delivery_record.tenant_id.clone(),
            delivery_record.delivery_id.clone(),
        );
        let delivery_result = if let Some(existing) = delivery_requests
            .get(&(delivery_record.tenant_id.clone(), delivery_request.clone()))
            .or_else(|| deliveries.get(&delivery_key))
        {
            if !existing.same_create_request(&delivery_record) {
                return Err(conflict("Snapshot delivery identity is already used"));
            }
            (existing.clone(), true)
        } else if deliveries.values().any(|existing| {
            existing.tenant_id == delivery_record.tenant_id
                && existing.snapshot_id == delivery_record.snapshot_id
        }) {
            return Err(conflict("Snapshot already has a SnapshotDelivery"));
        } else {
            let volume = volumes
                .get(&(
                    delivery_record.tenant_id.clone(),
                    delivery_record.storage_volume_id.clone(),
                ))
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::StorageVolumeNotFound,
                        "SnapshotDelivery StorageVolume does not exist",
                    )
                })?;
            validate_snapshot_delivery_parents(&delivery_record, &snapshot, volume)?;
            (delivery_record.clone(), false)
        };

        // A replay may find the Snapshot but not its Delivery only in a corrupted store. Never
        // silently recreate a missing child, because that would break the immutable aggregate
        // identity promised by the API.
        if snapshot_replayed && !delivery_result.1 {
            return Err(corruption(
                "Snapshot exists without its required SnapshotDelivery",
            ));
        }
        if !snapshot_replayed && delivery_result.1 {
            return Err(corruption(
                "SnapshotDelivery exists without its required Snapshot",
            ));
        }

        if !snapshot_replayed {
            snapshots.insert(snapshot_key, snapshot.clone());
        }
        if !delivery_result.1 {
            let delivery_key = (
                delivery_result.0.tenant_id.clone(),
                delivery_result.0.delivery_id.clone(),
            );
            deliveries.insert(delivery_key, delivery_result.0.clone());
            delivery_requests.insert(
                (
                    delivery_result.0.tenant_id.clone(),
                    delivery_result.0.create_request_id.clone(),
                ),
                delivery_result.0.clone(),
            );
            for root in delivery.retention_roots {
                retention_roots.insert((root.tenant_id, root.delivery_id, root.object_id));
            }
        }
        Ok(SnapshotWithDeliveryInsertResult {
            snapshot,
            delivery: delivery_result.0,
            replayed: snapshot_replayed || delivery_result.1,
        })
    }

    async fn transition_snapshot_state(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
        expected: SnapshotState,
        next: SnapshotState,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<SnapshotRecord> {
        let mut snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        let record = snapshots
            .get_mut(&(tenant_id.clone(), snapshot_id.clone()))
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ArtifactNotFound,
                    "Snapshot does not exist",
                )
            })?;
        if record.state == next {
            return Ok(record.clone());
        }
        if record.state != expected {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Snapshot state changed",
            ));
        }
        if next == SnapshotState::Ready {
            let delivery = deliveries
                .get(&(record.tenant_id.clone(), record.delivery_id.clone()))
                .ok_or_else(|| {
                    corruption("Snapshot exists without its required SnapshotDelivery")
                })?;
            if delivery.state != SnapshotDeliveryState::Ready {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Snapshot cannot become Ready before its SnapshotDelivery is Ready",
                )
                .with_retryable(false));
            }
        }
        record.state = next;
        record.resource_version = record.resource_version.checked_add(1).ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Snapshot resource version exhausted",
            )
        })?;
        record.updated_at_unix_ms = updated_at_unix_ms;
        Ok(record.clone())
    }

    async fn get_snapshot_delivery(
        &self,
        tenant_id: &TenantId,
        delivery_id: &SnapshotDeliveryId,
    ) -> CentralResult<Option<SnapshotDeliveryRecord>> {
        Ok(lock(&self.snapshot_deliveries)?
            .get(&(tenant_id.clone(), delivery_id.clone()))
            .cloned())
    }

    async fn get_snapshot_delivery_by_create_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<SnapshotDeliveryRecord>> {
        Ok(lock(&self.snapshot_delivery_requests)?
            .get(&(tenant_id.clone(), request_id.clone()))
            .cloned())
    }

    async fn list_snapshot_deliveries(
        &self,
        request: &SnapshotDeliveryListRequest,
    ) -> CentralResult<Vec<SnapshotDeliveryRecord>> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.snapshot_deliveries)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| {
                request
                    .snapshot_id
                    .as_ref()
                    .is_none_or(|id| &record.snapshot_id == id)
            })
            .filter(|record| request.mode.is_none_or(|mode| record.mode == mode))
            .filter(|record| request.state.is_none_or(|state| record.state == state))
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.delivery_id.cmp(&right.delivery_id))
        });
        records.truncate(usize::from(request.limit));
        Ok(records)
    }

    async fn insert_snapshot_delivery_idempotent(
        &self,
        request: SnapshotDeliveryInsertRequest,
    ) -> CentralResult<SnapshotDeliveryInsertOutcome> {
        validate_snapshot_delivery_retention_roots(&request)?;
        // Lifecycle deletion takes these locks in Volume -> Snapshot -> Delivery order. Holding
        // the parents through insertion makes the active/identity fence linearizable with it.
        let volumes = lock(&self.volumes)?;
        let snapshots = lock(&self.snapshots)?;
        let mut records = lock(&self.snapshot_deliveries)?;
        let mut requests = lock(&self.snapshot_delivery_requests)?;
        let mut roots = lock(&self.snapshot_delivery_retention_roots)?;
        if let Some(existing) =
            requests.get(&(request.record.tenant_id.clone(), request.request_id.clone()))
        {
            if existing.same_create_request(&request.record) {
                return Ok(SnapshotDeliveryInsertOutcome::Existing(existing.clone()));
            }
            return Err(conflict("Snapshot delivery request ID is already used"));
        }
        if request.record.create_request_id != request.request_id {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot delivery create request identity is inconsistent",
            ));
        }
        let key = (
            request.record.tenant_id.clone(),
            request.record.delivery_id.clone(),
        );
        if let Some(existing) = records.get(&key) {
            if existing.same_create_request(&request.record) {
                return Ok(SnapshotDeliveryInsertOutcome::Existing(existing.clone()));
            }
            return Err(conflict("Snapshot delivery ID is already used"));
        }
        if records.values().any(|existing| {
            existing.tenant_id == request.record.tenant_id
                && existing.snapshot_id == request.record.snapshot_id
        }) {
            return Err(conflict("Snapshot already has a SnapshotDelivery"));
        }
        if request.record.delivery_generation.get() == 0 || request.record.resource_version == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot delivery generations must be positive",
            ));
        }
        let volume = volumes
            .get(&(
                request.record.tenant_id.clone(),
                request.record.storage_volume_id.clone(),
            ))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::StorageVolumeNotFound,
                    "SnapshotDelivery StorageVolume does not exist",
                )
            })?;
        let snapshot = snapshots
            .get(&(
                request.record.tenant_id.clone(),
                request.record.snapshot_id.clone(),
            ))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "SnapshotDelivery Snapshot does not exist",
                )
            })?;
        validate_snapshot_delivery_parents(&request.record, snapshot, volume)?;
        records.insert(key, request.record.clone());
        requests.insert(
            (request.record.tenant_id.clone(), request.request_id),
            request.record.clone(),
        );
        for root in request.retention_roots {
            roots.insert((root.tenant_id, root.delivery_id, root.object_id));
        }
        Ok(SnapshotDeliveryInsertOutcome::Inserted(request.record))
    }

    async fn replace_snapshot_delivery(
        &self,
        expected_resource_version: u64,
        mut record: SnapshotDeliveryRecord,
    ) -> CentralResult<SnapshotDeliveryRecord> {
        let mut records = lock(&self.snapshot_deliveries)?;
        // Keep the same lock order as create, and hold all affected indexes until the current
        // Delivery state and its terminal GC roots have changed together.
        let mut requests = lock(&self.snapshot_delivery_requests)?;
        let mut retention_roots = (record.state == SnapshotDeliveryState::Deleted)
            .then(|| lock(&self.snapshot_delivery_retention_roots))
            .transpose()?;
        let key = (record.tenant_id.clone(), record.delivery_id.clone());
        let current = records.get(&key).cloned().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ArtifactNotFound,
                "Snapshot delivery does not exist",
            )
        })?;
        if current == record {
            return Ok(current);
        }
        if current.resource_version != expected_resource_version {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Snapshot delivery ResourceVersion changed",
            ));
        }
        if !current.same_create_request(&record) {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "SnapshotDelivery replacement cannot change immutable identity",
            )
            .with_retryable(false));
        }
        record.resource_version = expected_resource_version.checked_add(1).ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Snapshot delivery ResourceVersion exhausted",
            )
        })?;
        records.insert(key, record.clone());
        requests.insert(
            (record.tenant_id.clone(), record.create_request_id.clone()),
            record.clone(),
        );
        if let Some(roots) = retention_roots.as_mut() {
            roots
                .retain(|(tenant, id, _)| tenant != &record.tenant_id || id != &record.delivery_id);
        }
        Ok(record)
    }

    async fn insert_snapshot_delivery_retention_roots(
        &self,
        roots: &[SnapshotDeliveryRetentionRoot],
    ) -> CentralResult<()> {
        let mut stored = lock(&self.snapshot_delivery_retention_roots)?;
        for root in roots {
            stored.insert((
                root.tenant_id.clone(),
                root.delivery_id.clone(),
                root.object_id,
            ));
        }
        Ok(())
    }

    async fn list_snapshot_delivery_retention_roots(
        &self,
        tenant_id: &TenantId,
        delivery_id: &SnapshotDeliveryId,
    ) -> CentralResult<Vec<SnapshotDeliveryRetentionRoot>> {
        Ok(lock(&self.snapshot_delivery_retention_roots)?
            .iter()
            .filter(|(tenant, id, _)| tenant == tenant_id && id == delivery_id)
            .map(
                |(tenant_id, delivery_id, object_id)| SnapshotDeliveryRetentionRoot {
                    tenant_id: tenant_id.clone(),
                    delivery_id: delivery_id.clone(),
                    object_id: *object_id,
                },
            )
            .collect())
    }

    async fn release_snapshot_delivery_retention_roots(
        &self,
        tenant_id: &TenantId,
        delivery_id: &SnapshotDeliveryId,
    ) -> CentralResult<()> {
        lock(&self.snapshot_delivery_retention_roots)?
            .retain(|(tenant, id, _)| tenant != tenant_id || id != delivery_id);
        Ok(())
    }

    async fn get_snapshot_delivery_mutation(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<SnapshotDeliveryMutationRecord>> {
        Ok(lock(&self.snapshot_delivery_mutations)?
            .get(&(tenant_id.clone(), request_id.clone()))
            .cloned())
    }

    async fn apply_snapshot_delivery_mutation_idempotent(
        &self,
        request: SnapshotDeliveryMutationRequest,
    ) -> CentralResult<CatalogInsertOutcome<SnapshotDeliveryMutationRecord>> {
        validate_snapshot_delivery_mutation_request(&request)?;
        let mut records = lock(&self.snapshot_deliveries)?;
        let mut create_requests = lock(&self.snapshot_delivery_requests)?;
        let mut retention_roots = lock(&self.snapshot_delivery_retention_roots)?;
        let mut mutations = lock(&self.snapshot_delivery_mutations)?;
        let mutation_key = (request.tenant_id.clone(), request.request_id.clone());
        if let Some(existing) = mutations.get(&mutation_key) {
            return if existing.delivery_id == request.delivery_id
                && existing.kind == request.kind
                && existing.request_digest == request.request_digest
            {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict(
                    "SnapshotDelivery mutation request ID is already used",
                ))
            };
        }
        let delivery_key = (request.tenant_id.clone(), request.delivery_id.clone());
        let current = records.get(&delivery_key).cloned().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ArtifactNotFound,
                "Snapshot delivery does not exist",
            )
        })?;
        if current.resource_version != request.expected_resource_version {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Snapshot delivery ResourceVersion changed",
            ));
        }
        if !current.same_create_request(&request.desired_delivery) {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "SnapshotDelivery mutation cannot change immutable identity",
            )
            .with_retryable(false));
        }
        let delivery = if current == request.desired_delivery {
            current
        } else {
            let mut next = request.desired_delivery;
            next.resource_version = request
                .expected_resource_version
                .checked_add(1)
                .ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::ConcurrentUpdate,
                        "Snapshot delivery ResourceVersion exhausted",
                    )
                })?;
            records.insert(delivery_key, next.clone());
            create_requests.insert(
                (next.tenant_id.clone(), next.create_request_id.clone()),
                next.clone(),
            );
            if next.state == SnapshotDeliveryState::Deleted {
                retention_roots
                    .retain(|(tenant, id, _)| tenant != &next.tenant_id || id != &next.delivery_id);
            }
            next
        };
        let mutation = SnapshotDeliveryMutationRecord {
            tenant_id: request.tenant_id,
            request_id: request.request_id,
            delivery_id: request.delivery_id,
            kind: request.kind,
            request_digest: request.request_digest,
            delivery,
        };
        mutations.insert(mutation_key, mutation.clone());
        Ok(CatalogInsertOutcome::Inserted(mutation))
    }

    async fn get_s3_access_point(
        &self,
        tenant_id: &TenantId,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
    ) -> CentralResult<Option<S3AccessPointRecord>> {
        Ok(lock(&self.s3_access_points)?
            .get(&(tenant_id.clone(), access_point_id.clone()))
            .cloned())
    }

    async fn get_s3_access_point_by_snapshot(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
    ) -> CentralResult<Option<S3AccessPointRecord>> {
        Ok(lock(&self.s3_access_points)?
            .values()
            .find(|record| &record.tenant_id == tenant_id && &record.snapshot_id == snapshot_id)
            .cloned())
    }

    async fn get_s3_access_point_by_bucket(
        &self,
        bucket_name: &str,
    ) -> CentralResult<Option<S3AccessPointRecord>> {
        Ok(lock(&self.s3_access_points)?
            .values()
            .find(|record| record.bucket_name == bucket_name)
            .cloned())
    }

    async fn list_s3_access_points(
        &self,
        request: &S3AccessPointListRequest,
    ) -> CentralResult<S3AccessPointListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.s3_access_points)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| {
                request.after.as_ref().is_none_or(|after| {
                    record.created_at_unix_ms < after.created_at_unix_ms
                        || (record.created_at_unix_ms == after.created_at_unix_ms
                            && record.access_point_id > after.access_point_id)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.access_point_id.cmp(&right.access_point_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty S3 access point page");
            crate::S3AccessPointListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                access_point_id: last.access_point_id.clone(),
            }
        });
        Ok(S3AccessPointListPage { records, next })
    }

    async fn insert_s3_access_point(
        &self,
        record: S3AccessPointRecord,
    ) -> CentralResult<S3AccessPointInsertOutcome> {
        let snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        require_s3_snapshot(&snapshots, &deliveries, &record)?;
        let mut records = lock(&self.s3_access_points)?;
        if let Some(existing) =
            records.get(&(record.tenant_id.clone(), record.access_point_id.clone()))
        {
            if existing == &record {
                return Ok(S3AccessPointInsertOutcome::Existing(existing.clone()));
            }
            return Err(conflict("S3 Access Point ID is already used"));
        }
        if records
            .values()
            .any(|existing| existing.bucket_name == record.bucket_name)
        {
            return Err(conflict("S3 bucket name is already used"));
        }
        if records.values().any(|existing| {
            existing.tenant_id == record.tenant_id && existing.snapshot_id == record.snapshot_id
        }) {
            return Err(conflict("Snapshot already has an S3 Access Point"));
        }
        records.insert(
            (record.tenant_id.clone(), record.access_point_id.clone()),
            record.clone(),
        );
        Ok(S3AccessPointInsertOutcome::Inserted(record))
    }

    async fn get_s3_mutation(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<S3MutationRecord>> {
        Ok(lock(&self.s3_mutations)?
            .get(&(tenant_id.clone(), request_id.clone()))
            .cloned())
    }

    async fn create_s3_access_point_idempotent(
        &self,
        mutation: S3MutationRecord,
        access_point: S3AccessPointRecord,
        credential: S3CredentialRecord,
    ) -> CentralResult<CatalogInsertOutcome<S3AccessPointCreateResult>> {
        if mutation.operation != S3MutationKind::AccessPointCreate
            || mutation.tenant_id != access_point.tenant_id
            || credential.access_point_id != access_point.access_point_id
        {
            return Err(conflict("invalid S3 Access Point mutation binding"));
        }
        let key = (mutation.tenant_id.clone(), mutation.request_id.clone());
        let snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        let mut mutations = lock(&self.s3_mutations)?;
        let mut access_points = lock(&self.s3_access_points)?;
        let mut credentials = lock(&self.s3_credentials)?;
        if let Some(existing) = mutations.get(&key) {
            if !same_s3_mutation_identity(existing, &mutation) {
                return Err(conflict("S3 request identity is already used"));
            }
            let existing_access_point = access_points
                .get(&(
                    access_point.tenant_id.clone(),
                    access_point.access_point_id.clone(),
                ))
                .cloned()
                .ok_or_else(|| corruption("S3 mutation references a missing Access Point"))?;
            let existing_credential = credentials
                .get(&credential.credential_id)
                .cloned()
                .ok_or_else(|| corruption("S3 mutation references a missing credential"))?;
            require_s3_snapshot(&snapshots, &deliveries, &existing_access_point)?;
            return Ok(CatalogInsertOutcome::Existing(S3AccessPointCreateResult {
                access_point: existing_access_point,
                credential: existing_credential,
            }));
        }
        if let Some(existing_access_point) = access_points
            .get(&(
                access_point.tenant_id.clone(),
                access_point.access_point_id.clone(),
            ))
            .cloned()
        {
            if !same_s3_access_point_create_identity(&existing_access_point, &access_point) {
                return Err(conflict("S3 Access Point identity is already used"));
            }
            let result = if let Some(existing_credential) =
                credentials.get(&credential.credential_id).cloned()
            {
                if !same_s3_credential_identity(&existing_credential, &credential) {
                    return Err(conflict("S3 credential identity is already used"));
                }
                require_s3_snapshot(&snapshots, &deliveries, &existing_access_point)?;
                CatalogInsertOutcome::Existing(S3AccessPointCreateResult {
                    access_point: existing_access_point,
                    credential: existing_credential,
                })
            } else {
                require_s3_snapshot(&snapshots, &deliveries, &existing_access_point)?;
                if existing_access_point.state != S3AccessPointState::Active {
                    return Err(conflict("S3 Access Point is disabled"));
                }
                credentials.insert(credential.credential_id.clone(), credential.clone());
                CatalogInsertOutcome::Inserted(S3AccessPointCreateResult {
                    access_point: existing_access_point,
                    credential,
                })
            };
            mutations.insert(key, mutation);
            return Ok(result);
        }
        require_s3_snapshot(&snapshots, &deliveries, &access_point)?;
        if access_points
            .values()
            .any(|existing| existing.bucket_name == access_point.bucket_name)
            || access_points.values().any(|existing| {
                existing.tenant_id == access_point.tenant_id
                    && existing.snapshot_id == access_point.snapshot_id
            })
        {
            return Err(conflict("S3 Access Point identity is already used"));
        }
        if credentials.contains_key(&credential.credential_id)
            || credentials
                .values()
                .any(|existing| existing.access_key_id == credential.access_key_id)
        {
            return Err(conflict("S3 credential identity is already used"));
        }
        access_points.insert(
            (
                access_point.tenant_id.clone(),
                access_point.access_point_id.clone(),
            ),
            access_point.clone(),
        );
        credentials.insert(credential.credential_id.clone(), credential.clone());
        mutations.insert(key, mutation);
        Ok(CatalogInsertOutcome::Inserted(S3AccessPointCreateResult {
            access_point,
            credential,
        }))
    }

    async fn update_s3_access_point_state_idempotent(
        &self,
        mutation: S3MutationRecord,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        state: S3AccessPointState,
        updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<CatalogInsertOutcome<S3AccessPointRecord>> {
        let expected_operation = match state {
            S3AccessPointState::Active => S3MutationKind::AccessPointEnable,
            S3AccessPointState::Disabled => S3MutationKind::AccessPointDisable,
            S3AccessPointState::Deleted => {
                return Err(conflict(
                    "deleted state requires the Access Point delete operation",
                ));
            }
        };
        if mutation.operation != expected_operation {
            return Err(conflict("invalid S3 Access Point state mutation binding"));
        }
        let mutation_key = (mutation.tenant_id.clone(), mutation.request_id.clone());
        let access_point_key = (mutation.tenant_id.clone(), access_point_id.clone());
        let snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        let mut mutations = lock(&self.s3_mutations)?;
        let mut access_points = lock(&self.s3_access_points)?;
        let mut credentials = lock(&self.s3_credentials)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            if !same_s3_mutation_identity(existing, &mutation) {
                return Err(conflict("S3 request identity is already used"));
            }
            let current = access_points
                .get(&access_point_key)
                .cloned()
                .ok_or_else(|| corruption("S3 mutation references a missing Access Point"))?;
            if state == S3AccessPointState::Active {
                require_s3_snapshot(&snapshots, &deliveries, &current)?;
            }
            return Ok(CatalogInsertOutcome::Existing(current));
        }
        let access_point = access_points.get_mut(&access_point_key).ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 Access Point does not exist",
            )
        })?;
        if state == S3AccessPointState::Active && access_point.state == S3AccessPointState::Deleted
        {
            return Err(conflict("deleted S3 Access Point cannot be enabled"));
        }
        if state == S3AccessPointState::Active {
            require_s3_snapshot(&snapshots, &deliveries, access_point)?;
        }
        let next_policy_generation = (access_point.state != state)
            .then(|| {
                access_point
                    .policy_generation
                    .checked_add(1)
                    .ok_or_else(|| conflict("S3 policy generation exhausted"))
            })
            .transpose()?;
        if state == S3AccessPointState::Disabled
            || (state == S3AccessPointState::Active
                && access_point.state != S3AccessPointState::Active)
        {
            for credential in credentials.values_mut().filter(|credential| {
                credential.access_point_id == *access_point_id
                    && credential.state == S3CredentialState::Active
            }) {
                credential.state = S3CredentialState::Revoked;
                credential.encrypted_secret.clear();
            }
        }
        if let Some(next_policy_generation) = next_policy_generation {
            access_point.state = state;
            access_point.policy_generation = next_policy_generation;
            access_point.updated_at_unix_ms = updated_at_unix_ms;
        }
        let result = access_point.clone();
        mutations.insert(mutation_key, mutation);
        Ok(CatalogInsertOutcome::Inserted(result))
    }

    async fn delete_s3_access_point_idempotent(
        &self,
        mutation: S3MutationRecord,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<CatalogInsertOutcome<S3AccessPointRecord>> {
        if mutation.operation != S3MutationKind::AccessPointDelete {
            return Err(conflict("invalid S3 Access Point delete mutation binding"));
        }
        let mutation_key = (mutation.tenant_id.clone(), mutation.request_id.clone());
        let access_point_key = (mutation.tenant_id.clone(), access_point_id.clone());
        let mut mutations = lock(&self.s3_mutations)?;
        let mut access_points = lock(&self.s3_access_points)?;
        let mut credentials = lock(&self.s3_credentials)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            if !same_s3_mutation_identity(existing, &mutation) {
                return Err(conflict("S3 request identity is already used"));
            }
            let current = access_points
                .get(&access_point_key)
                .cloned()
                .ok_or_else(|| corruption("S3 mutation references a missing Access Point"))?;
            return Ok(CatalogInsertOutcome::Existing(current));
        }
        let access_point = access_points.get_mut(&access_point_key).ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 Access Point does not exist",
            )
        })?;
        if access_point.state != S3AccessPointState::Deleted {
            access_point.policy_generation = access_point
                .policy_generation
                .checked_add(1)
                .ok_or_else(|| conflict("S3 policy generation exhausted"))?;
            access_point.state = S3AccessPointState::Deleted;
            access_point.updated_at_unix_ms = updated_at_unix_ms;
        }
        for credential in credentials.values_mut().filter(|credential| {
            credential.access_point_id == *access_point_id
                && credential.state == S3CredentialState::Active
        }) {
            credential.state = S3CredentialState::Revoked;
            credential.encrypted_secret.clear();
        }
        let result = access_point.clone();
        mutations.insert(mutation_key, mutation);
        Ok(CatalogInsertOutcome::Inserted(result))
    }

    async fn update_s3_access_point_state(
        &self,
        tenant_id: &TenantId,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        state: S3AccessPointState,
        policy_generation: u64,
        updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<S3AccessPointRecord> {
        let snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        let mut records = lock(&self.s3_access_points)?;
        let mut credentials = lock(&self.s3_credentials)?;
        let record = records
            .get_mut(&(tenant_id.clone(), access_point_id.clone()))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "S3 Access Point does not exist",
                )
            })?;
        if state == S3AccessPointState::Active {
            require_s3_snapshot(&snapshots, &deliveries, record)?;
        }
        record.state = state;
        record.policy_generation = policy_generation;
        record.updated_at_unix_ms = updated_at_unix_ms;
        if state == S3AccessPointState::Disabled {
            for credential in credentials.values_mut().filter(|credential| {
                credential.access_point_id == *access_point_id
                    && credential.state == S3CredentialState::Active
            }) {
                credential.state = S3CredentialState::Revoked;
                credential.encrypted_secret.clear();
            }
        }
        Ok(record.clone())
    }

    async fn list_s3_credentials(
        &self,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
    ) -> CentralResult<Vec<S3CredentialRecord>> {
        let mut records = lock(&self.s3_credentials)?
            .values()
            .filter(|record| &record.access_point_id == access_point_id)
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.created_at_unix_ms);
        Ok(records)
    }

    async fn get_s3_credential_by_access_key(
        &self,
        access_key_id: &str,
    ) -> CentralResult<Option<S3CredentialRecord>> {
        Ok(lock(&self.s3_credentials)?
            .values()
            .find(|record| record.access_key_id == access_key_id)
            .cloned())
    }

    async fn insert_s3_credential(
        &self,
        record: S3CredentialRecord,
    ) -> CentralResult<S3CredentialInsertOutcome> {
        let snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        let access_points = lock(&self.s3_access_points)?;
        let access_point = access_points
            .values()
            .find(|access_point| access_point.access_point_id == record.access_point_id)
            .ok_or_else(|| conflict("S3 Access Point does not exist"))?;
        if access_point.state != S3AccessPointState::Active {
            return Err(conflict("S3 Access Point is disabled"));
        }
        require_s3_snapshot(&snapshots, &deliveries, access_point)?;
        let mut records = lock(&self.s3_credentials)?;
        if let Some(existing) = records.get(&record.credential_id) {
            if same_s3_credential_identity(existing, &record) {
                return Ok(S3CredentialInsertOutcome::Existing(existing.clone()));
            }
            return Err(conflict("S3 credential ID is already used"));
        }
        if records
            .values()
            .any(|existing| existing.access_key_id == record.access_key_id)
        {
            return Err(conflict("S3 access key is already used"));
        }
        if record.state == S3CredentialState::Active
            && records
                .values()
                .filter(|existing| {
                    existing.access_point_id == record.access_point_id
                        && existing.state == S3CredentialState::Active
                })
                .count()
                >= 2
        {
            return Err(conflict(
                "an S3 Access Point can have at most two active credentials",
            ));
        }
        records.insert(record.credential_id.clone(), record.clone());
        Ok(S3CredentialInsertOutcome::Inserted(record))
    }

    async fn create_s3_credential_idempotent(
        &self,
        mutation: S3MutationRecord,
        credential: S3CredentialRecord,
    ) -> CentralResult<CatalogInsertOutcome<S3CredentialRecord>> {
        if mutation.operation != S3MutationKind::CredentialCreate {
            return Err(conflict("invalid S3 credential create mutation binding"));
        }
        let mutation_key = (mutation.tenant_id.clone(), mutation.request_id.clone());
        let snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        let mut mutations = lock(&self.s3_mutations)?;
        let access_points = lock(&self.s3_access_points)?;
        let mut credentials = lock(&self.s3_credentials)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            if !same_s3_mutation_identity(existing, &mutation) {
                return Err(conflict("S3 request identity is already used"));
            }
            let existing = credentials
                .get(&credential.credential_id)
                .cloned()
                .ok_or_else(|| corruption("S3 mutation references a missing credential"))?;
            let access_point = access_points
                .get(&(
                    mutation.tenant_id.clone(),
                    credential.access_point_id.clone(),
                ))
                .ok_or_else(|| corruption("S3 mutation references a missing Access Point"))?;
            if access_point.state != S3AccessPointState::Active {
                return Err(conflict("S3 Access Point is disabled"));
            }
            require_s3_snapshot(&snapshots, &deliveries, access_point)?;
            return Ok(CatalogInsertOutcome::Existing(existing));
        }
        let access_point = access_points
            .get(&(
                mutation.tenant_id.clone(),
                credential.access_point_id.clone(),
            ))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "S3 Access Point does not exist",
                )
            })?;
        if access_point.state != S3AccessPointState::Active {
            return Err(conflict("S3 Access Point is disabled"));
        }
        require_s3_snapshot(&snapshots, &deliveries, access_point)?;
        if credentials.contains_key(&credential.credential_id)
            || credentials
                .values()
                .any(|existing| existing.access_key_id == credential.access_key_id)
        {
            return Err(conflict("S3 credential identity is already used"));
        }
        if credential.state == S3CredentialState::Active
            && credentials
                .values()
                .filter(|existing| {
                    existing.access_point_id == credential.access_point_id
                        && existing.state == S3CredentialState::Active
                })
                .count()
                >= 2
        {
            return Err(conflict(
                "an S3 Access Point can have at most two active credentials",
            ));
        }
        credentials.insert(credential.credential_id.clone(), credential.clone());
        mutations.insert(mutation_key, mutation);
        Ok(CatalogInsertOutcome::Inserted(credential))
    }

    async fn revoke_s3_credential_idempotent(
        &self,
        mutation: S3MutationRecord,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        credential_id: &neoengram_domain::protocol::S3CredentialId,
    ) -> CentralResult<CatalogInsertOutcome<S3CredentialRecord>> {
        if mutation.operation != S3MutationKind::CredentialRevoke {
            return Err(conflict("invalid S3 credential revoke mutation binding"));
        }
        let mutation_key = (mutation.tenant_id.clone(), mutation.request_id.clone());
        let mut mutations = lock(&self.s3_mutations)?;
        let access_points = lock(&self.s3_access_points)?;
        let mut credentials = lock(&self.s3_credentials)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            if !same_s3_mutation_identity(existing, &mutation) {
                return Err(conflict("S3 request identity is already used"));
            }
            let existing = credentials
                .get(credential_id)
                .cloned()
                .ok_or_else(|| corruption("S3 mutation references a missing credential"))?;
            return Ok(CatalogInsertOutcome::Existing(existing));
        }
        if !access_points.contains_key(&(mutation.tenant_id.clone(), access_point_id.clone())) {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 Access Point does not exist",
            ));
        }
        let credential = credentials.get_mut(credential_id).ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 credential does not exist",
            )
        })?;
        if credential.access_point_id != *access_point_id {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 credential does not belong to the Access Point",
            ));
        }
        credential.state = S3CredentialState::Revoked;
        credential.encrypted_secret.clear();
        let result = credential.clone();
        mutations.insert(mutation_key, mutation);
        Ok(CatalogInsertOutcome::Inserted(result))
    }

    async fn update_s3_credential_state(
        &self,
        credential_id: &neoengram_domain::protocol::S3CredentialId,
        state: S3CredentialState,
    ) -> CentralResult<S3CredentialRecord> {
        let snapshots = lock(&self.snapshots)?;
        let deliveries = lock(&self.snapshot_deliveries)?;
        let access_points = lock(&self.s3_access_points)?;
        let mut records = lock(&self.s3_credentials)?;
        let record = records.get_mut(credential_id).ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 credential does not exist",
            )
        })?;
        if state == S3CredentialState::Active {
            if record.state != S3CredentialState::Active {
                return Err(conflict(
                    "revoked or expired S3 credentials cannot be reactivated",
                ));
            }
            let access_point = access_points
                .values()
                .find(|access_point| access_point.access_point_id == record.access_point_id)
                .ok_or_else(|| corruption("S3 credential references a missing Access Point"))?;
            if access_point.state != S3AccessPointState::Active {
                return Err(conflict("S3 Access Point is disabled"));
            }
            require_s3_snapshot(&snapshots, &deliveries, access_point)?;
        }
        record.state = state;
        if state == S3CredentialState::Revoked {
            record.encrypted_secret.clear();
        }
        Ok(record.clone())
    }

    async fn update_s3_credential_last_used(
        &self,
        credential_id: &neoengram_domain::protocol::S3CredentialId,
        last_used_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<S3CredentialRecord> {
        let mut records = lock(&self.s3_credentials)?;
        let record = records.get_mut(credential_id).ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 credential does not exist",
            )
        })?;
        if last_used_at_unix_ms.get() < record.created_at_unix_ms.get() {
            return Err(conflict("S3 credential usage time predates creation"));
        }
        if record
            .last_used_at_unix_ms
            .is_none_or(|current| last_used_at_unix_ms.get() > current.get())
        {
            record.last_used_at_unix_ms = Some(last_used_at_unix_ms);
        }
        Ok(record.clone())
    }

    async fn expire_s3_credentials(
        &self,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        now_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<u64> {
        let mut expired = 0_u64;
        for record in lock(&self.s3_credentials)?.values_mut() {
            if &record.access_point_id == access_point_id
                && record.state == S3CredentialState::Active
                && record.expires_at_unix_ms.get() <= now_unix_ms.get()
            {
                record.state = S3CredentialState::Expired;
                expired = expired.saturating_add(1);
            }
        }
        Ok(expired)
    }

    async fn query_deletion_impact(
        &self,
        request: DeletionImpactQuery,
    ) -> CentralResult<DeletionImpactRecord> {
        let projects = lock(&self.projects)?;
        let artifacts = lock(&self.artifacts)?;
        let volumes = lock(&self.volumes)?;
        let workspaces = lock(&self.workspaces)?;
        let snapshots = lock(&self.snapshots)?;
        let access_points = lock(&self.s3_access_points)?;
        let credentials = lock(&self.s3_credentials)?;
        let impact = build_deletion_impact(
            &request,
            &projects,
            &artifacts,
            &volumes,
            &workspaces,
            &snapshots,
            &access_points,
            &credentials,
        )?;
        let impact_digest =
            neoengram_domain::protocol::jcs_blake3(&impact).map_err(protocol_error)?;
        let record = DeletionImpactRecord {
            impact,
            impact_digest,
        };
        lock(&self.deletion_impacts)?
            .insert((request.tenant_id, record.impact_digest), record.clone());
        Ok(record)
    }

    async fn get_deletion_operation(
        &self,
        tenant_id: &TenantId,
        deletion_id: &DeletionId,
    ) -> CentralResult<Option<DeletionOperation>> {
        Ok(lock(&self.deletion_operations)?
            .get(&(tenant_id.clone(), deletion_id.clone()))
            .cloned())
    }

    async fn list_deletion_operations(
        &self,
        request: &DeletionListRequest,
    ) -> CentralResult<DeletionListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.deletion_operations)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
            .filter(|record| {
                request
                    .states
                    .as_ref()
                    .is_none_or(|states| states.contains(&record.state))
            })
            .filter(|record| {
                request.after.as_ref().is_none_or(|after| {
                    record.created_at_unix_ms < after.created_at_unix_ms
                        || (record.created_at_unix_ms == after.created_at_unix_ms
                            && record.deletion_id > after.deletion_id)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.deletion_id.cmp(&right.deletion_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty lifecycle keyset page");
            DeletionListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                deletion_id: last.deletion_id.clone(),
            }
        });
        Ok(DeletionListPage { records, next })
    }

    async fn create_deletion_idempotent(
        &self,
        request: CreateDeletionRequest,
    ) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
        let mutation_key = (request.tenant_id.clone(), request.request_id.clone());
        let mut mutations = lock(&self.deletion_mutations)?;
        let mut operations = lock(&self.deletion_operations)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            validate_deletion_mutation(
                existing,
                DeletionMutationKind::Create,
                &request.deletion_id,
                &request.request_digest,
                None,
            )?;
            let operation = operations
                .get(&(request.tenant_id.clone(), existing.deletion_id.clone()))
                .cloned()
                .ok_or_else(|| corruption("deletion mutation references a missing operation"))?;
            return Ok(CatalogInsertOutcome::Existing(operation));
        }
        if operations.contains_key(&(request.tenant_id.clone(), request.deletion_id.clone())) {
            return Err(conflict("Deletion ID is already bound to another request"));
        }
        let impact = lock(&self.deletion_impacts)?
            .get(&(request.tenant_id.clone(), request.impact_digest))
            .cloned()
            .ok_or_else(|| conflict("deletion impact digest is unknown or expired"))?;
        if impact.impact.expires_at_unix_ms < request.now_unix_ms {
            return Err(conflict("deletion impact digest has expired"));
        }
        if impact.impact.root != request.root
            || impact.impact.cascade != request.cascade
            || impact.impact.confirm_managed_data_erase != request.confirm_managed_data_erase
        {
            return Err(conflict(
                "deletion impact does not match the requested resource or cascade confirmation",
            ));
        }
        if !impact.impact.blockers.is_empty() {
            return Err(conflict("deletion impact contains blocking conditions"));
        }
        let root_version = impact
            .impact
            .targets
            .iter()
            .find(|target| target.resource == impact.impact.root)
            .map(|target| target.resource_version.get())
            .ok_or_else(|| corruption("deletion impact omits its root target"))?;
        if root_version != request.expected_resource_version {
            return Err(concurrent("resource version changed after impact query"));
        }

        let purge_after_unix_ms = checked_timestamp_add(
            request.now_unix_ms,
            DELETION_RECOVERY_WINDOW_MILLIS,
            "deletion recovery deadline overflow",
        )?;
        let mut projects = lock(&self.projects)?;
        let mut artifacts = lock(&self.artifacts)?;
        let mut volumes = lock(&self.volumes)?;
        let mut workspaces = lock(&self.workspaces)?;
        let mut snapshots = lock(&self.snapshots)?;
        validate_current_targets(
            &request.tenant_id,
            &impact.impact.targets,
            &projects,
            &artifacts,
            &volumes,
            &workspaces,
            &snapshots,
        )?;
        let targets = fence_targets_for_delete(
            &request.tenant_id,
            &impact.impact.targets,
            &request.deletion_id,
            request.now_unix_ms,
            purge_after_unix_ms,
            &mut projects,
            &mut artifacts,
            &mut volumes,
            &mut workspaces,
            &mut snapshots,
        )?;

        let snapshot_ids = targets
            .iter()
            .filter_map(|target| match &target.resource {
                ResourceRef::Snapshot { snapshot_id } => Some(snapshot_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut access_points = lock(&self.s3_access_points)?;
        let mut credentials = lock(&self.s3_credentials)?;
        disable_snapshot_s3_access(
            &request.tenant_id,
            &snapshot_ids,
            request.now_unix_ms,
            &mut access_points,
            &mut credentials,
        )?;

        let operation = DeletionOperation {
            deletion_id: request.deletion_id.clone(),
            tenant_id: request.tenant_id.clone(),
            root: impact.impact.root,
            state: DeletionOperationState::Requested,
            resource_version: ResourceVersion::new(1),
            targets,
            request_id: request.request_id.clone(),
            request_digest: request.request_digest,
            impact_digest: request.impact_digest,
            cascade: impact.impact.cascade,
            confirm_managed_data_erase: impact.impact.confirm_managed_data_erase,
            purge_after_unix_ms,
            created_at_unix_ms: request.now_unix_ms,
            updated_at_unix_ms: request.now_unix_ms,
            completion: None,
            last_error: None,
            resume_state: None,
            retry_count: DecimalU64::new(0),
        };
        operations.insert(
            (request.tenant_id.clone(), request.deletion_id.clone()),
            operation.clone(),
        );
        mutations.insert(
            mutation_key,
            DeletionMutation {
                tenant_id: request.tenant_id,
                request_id: request.request_id,
                kind: DeletionMutationKind::Create,
                request_digest: request.request_digest,
                deletion_id: request.deletion_id,
                retention_hold_id: None,
                created_at_unix_ms: request.now_unix_ms,
            },
        );
        Ok(CatalogInsertOutcome::Inserted(operation))
    }

    async fn restore_deletion_idempotent(
        &self,
        request: RestoreDeletionRequest,
    ) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
        let mutation_key = (request.tenant_id.clone(), request.request_id.clone());
        let mut mutations = lock(&self.deletion_mutations)?;
        let mut operations = lock(&self.deletion_operations)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            validate_deletion_mutation(
                existing,
                DeletionMutationKind::Restore,
                &request.deletion_id,
                &request.request_digest,
                None,
            )?;
            let operation = operations
                .get(&(request.tenant_id.clone(), request.deletion_id.clone()))
                .cloned()
                .ok_or_else(|| corruption("restore mutation references a missing operation"))?;
            return Ok(CatalogInsertOutcome::Existing(operation));
        }
        let operation = operations
            .get_mut(&(request.tenant_id.clone(), request.deletion_id.clone()))
            .ok_or_else(|| conflict("deletion operation does not exist"))?;
        require_operation_version(operation, request.expected_resource_version)?;
        if !matches!(
            operation.state,
            DeletionOperationState::Recoverable
                | DeletionOperationState::Blocked
                | DeletionOperationState::Failed
        ) || request.now_unix_ms >= operation.purge_after_unix_ms
        {
            return Err(conflict("deletion operation is no longer restorable"));
        }
        let mut projects = lock(&self.projects)?;
        let mut artifacts = lock(&self.artifacts)?;
        let mut volumes = lock(&self.volumes)?;
        let mut workspaces = lock(&self.workspaces)?;
        let mut snapshots = lock(&self.snapshots)?;
        operation.targets = set_target_lifecycle_state(
            &request.tenant_id,
            &operation.targets,
            &request.deletion_id,
            ResourceLifecycleState::Restoring,
            request.now_unix_ms,
            &mut projects,
            &mut artifacts,
            &mut volumes,
            &mut workspaces,
            &mut snapshots,
        )?;
        operation.state = DeletionOperationState::Restoring;
        operation.resource_version = next_resource_version(operation.resource_version)?;
        operation.updated_at_unix_ms = request.now_unix_ms;
        operation.last_error = None;
        operation.resume_state = None;
        let result = operation.clone();
        mutations.insert(
            mutation_key,
            DeletionMutation {
                tenant_id: request.tenant_id,
                request_id: request.request_id,
                kind: DeletionMutationKind::Restore,
                request_digest: request.request_digest,
                deletion_id: request.deletion_id,
                retention_hold_id: None,
                created_at_unix_ms: request.now_unix_ms,
            },
        );
        Ok(CatalogInsertOutcome::Inserted(result))
    }

    async fn retry_deletion_idempotent(
        &self,
        request: RetryDeletionRequest,
    ) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
        let mutation_key = (request.tenant_id.clone(), request.request_id.clone());
        let mut mutations = lock(&self.deletion_mutations)?;
        let mut operations = lock(&self.deletion_operations)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            validate_deletion_mutation(
                existing,
                DeletionMutationKind::Retry,
                &request.deletion_id,
                &request.request_digest,
                None,
            )?;
            let operation = operations
                .get(&(request.tenant_id.clone(), request.deletion_id.clone()))
                .cloned()
                .ok_or_else(|| corruption("retry mutation references a missing operation"))?;
            return Ok(CatalogInsertOutcome::Existing(operation));
        }
        let operation = operations
            .get_mut(&(request.tenant_id.clone(), request.deletion_id.clone()))
            .ok_or_else(|| conflict("deletion operation does not exist"))?;
        require_operation_version(operation, request.expected_resource_version)?;
        if !matches!(
            operation.state,
            DeletionOperationState::Blocked | DeletionOperationState::Failed
        ) {
            return Err(conflict(
                "only blocked or failed deletion operations can be retried",
            ));
        }
        let resume_state = operation
            .resume_state
            .filter(|state| retryable_deletion_resume_state(*state))
            .ok_or_else(|| {
                corruption("blocked or failed deletion operation has no valid resume state")
            })?;
        let next_version = next_resource_version(operation.resource_version)?;
        let next_retry_count = operation
            .retry_count
            .get()
            .checked_add(1)
            .ok_or_else(|| conflict("deletion retry counter exhausted"))?;
        operation.state = resume_state;
        operation.resource_version = next_version;
        operation.retry_count = DecimalU64::new(next_retry_count);
        operation.updated_at_unix_ms = request.now_unix_ms;
        operation.last_error = None;
        operation.resume_state = None;
        let result = operation.clone();
        mutations.insert(
            mutation_key,
            DeletionMutation {
                tenant_id: request.tenant_id,
                request_id: request.request_id,
                kind: DeletionMutationKind::Retry,
                request_digest: request.request_digest,
                deletion_id: request.deletion_id,
                retention_hold_id: None,
                created_at_unix_ms: request.now_unix_ms,
            },
        );
        Ok(CatalogInsertOutcome::Inserted(result))
    }

    async fn transition_deletion_state(
        &self,
        request: DeletionTransitionRequest,
    ) -> CentralResult<DeletionOperation> {
        let mut operations = lock(&self.deletion_operations)?;
        let operation = operations
            .get_mut(&(request.tenant_id.clone(), request.deletion_id.clone()))
            .ok_or_else(|| conflict("deletion operation does not exist"))?;
        require_operation_version(operation, request.expected_resource_version)?;
        if operation.state == request.next_state {
            return Ok(operation.clone());
        }
        if operation.state != request.expected_state
            || !valid_deletion_transition(request.expected_state, request.next_state)
        {
            return Err(conflict("invalid deletion operation state transition"));
        }
        if request.next_state == DeletionOperationState::Purging {
            if request.now_unix_ms < operation.purge_after_unix_ms {
                return Err(conflict("deletion recovery window has not elapsed"));
            }
            let holds = lock(&self.retention_holds)?;
            if holds.values().any(|hold| {
                hold.tenant_id == request.tenant_id
                    && hold.deletion_id == request.deletion_id
                    && retention_hold_is_active(hold, request.now_unix_ms)
            }) {
                return Err(conflict("an active retention hold prevents purge"));
            }
        }

        let mut projects = lock(&self.projects)?;
        let mut artifacts = lock(&self.artifacts)?;
        let mut volumes = lock(&self.volumes)?;
        let mut workspaces = lock(&self.workspaces)?;
        let mut snapshots = lock(&self.snapshots)?;
        if request.next_state == DeletionOperationState::Quarantining {
            operation.targets = set_target_lifecycle_state(
                &request.tenant_id,
                &operation.targets,
                &request.deletion_id,
                ResourceLifecycleState::Deleting,
                request.now_unix_ms,
                &mut projects,
                &mut artifacts,
                &mut volumes,
                &mut workspaces,
                &mut snapshots,
            )?;
        } else if request.expected_state == DeletionOperationState::Restoring
            && request.next_state == DeletionOperationState::Completed
        {
            operation.targets = finalize_targets(
                &request.tenant_id,
                &operation.targets,
                &request.deletion_id,
                DeletionCompletion::Restored,
                request.now_unix_ms,
                &mut projects,
                &mut artifacts,
                &mut volumes,
                &mut workspaces,
                &mut snapshots,
            )?;
            operation.completion = Some(DeletionCompletion::Restored);
        } else if request.expected_state == DeletionOperationState::Finalizing
            && request.next_state == DeletionOperationState::Completed
        {
            operation.targets = finalize_targets(
                &request.tenant_id,
                &operation.targets,
                &request.deletion_id,
                DeletionCompletion::Purged,
                request.now_unix_ms,
                &mut projects,
                &mut artifacts,
                &mut volumes,
                &mut workspaces,
                &mut snapshots,
            )?;
            operation.completion = Some(DeletionCompletion::Purged);
            let purged_snapshots = operation
                .targets
                .iter()
                .filter_map(|target| match &target.resource {
                    ResourceRef::Snapshot { snapshot_id, .. } => Some(snapshot_id.clone()),
                    _ => None,
                })
                .collect::<std::collections::BTreeSet<_>>();
            if !purged_snapshots.is_empty() {
                let mut deliveries = lock(&self.snapshot_deliveries)?;
                for ((tenant_id, _), delivery) in deliveries.iter_mut() {
                    if tenant_id != &request.tenant_id
                        || !purged_snapshots.contains(&delivery.snapshot_id)
                        || delivery.state == SnapshotDeliveryState::Deleted
                    {
                        continue;
                    }
                    delivery.state = SnapshotDeliveryState::Deleted;
                    delivery.delivery_generation = DeliveryGeneration::new(
                        delivery
                            .delivery_generation
                            .get()
                            .checked_add(1)
                            .ok_or_else(|| {
                                conflict(
                                    "SnapshotDelivery generation exhausted during lifecycle purge",
                                )
                            })?,
                    );
                    delivery.resource_version =
                        delivery.resource_version.checked_add(1).ok_or_else(|| {
                            conflict(
                                "SnapshotDelivery ResourceVersion exhausted during lifecycle purge",
                            )
                        })?;
                    delivery.updated_at_unix_ms = request.now_unix_ms;
                }
                lock(&self.snapshot_delivery_retention_roots)?.retain(
                    |(tenant, delivery_id, _)| {
                        if tenant != &request.tenant_id {
                            return true;
                        }
                        deliveries
                            .get(&(tenant.clone(), delivery_id.clone()))
                            .is_none_or(|delivery| delivery.state != SnapshotDeliveryState::Deleted)
                    },
                );
            }
        }
        if matches!(
            request.next_state,
            DeletionOperationState::Blocked | DeletionOperationState::Failed
        ) {
            if !matches!(
                operation.state,
                DeletionOperationState::Blocked | DeletionOperationState::Failed
            ) {
                operation.resume_state = Some(operation.state);
            }
        } else {
            operation.resume_state = None;
        }
        operation.state = request.next_state;
        operation.resource_version = next_resource_version(operation.resource_version)?;
        operation.updated_at_unix_ms = request.now_unix_ms;
        operation.last_error = request.last_error;
        Ok(operation.clone())
    }

    async fn list_retention_holds(
        &self,
        tenant_id: &TenantId,
        deletion_id: &DeletionId,
    ) -> CentralResult<Vec<RetentionHold>> {
        let mut holds = lock(&self.retention_holds)?
            .values()
            .filter(|hold| &hold.tenant_id == tenant_id && &hold.deletion_id == deletion_id)
            .cloned()
            .collect::<Vec<_>>();
        holds.sort_by(|left, right| {
            left.created_at_unix_ms
                .cmp(&right.created_at_unix_ms)
                .then_with(|| left.retention_hold_id.cmp(&right.retention_hold_id))
        });
        Ok(holds)
    }

    async fn create_retention_hold_idempotent(
        &self,
        request: CreateRetentionHoldRequest,
    ) -> CentralResult<CatalogInsertOutcome<RetentionHold>> {
        if request.reason.trim().is_empty() {
            return Err(conflict("retention hold reason must not be empty"));
        }
        if request
            .expires_at_unix_ms
            .is_some_and(|expires| expires <= request.now_unix_ms)
        {
            return Err(conflict("retention hold expiry must be in the future"));
        }
        let mutation_key = (request.tenant_id.clone(), request.request_id.clone());
        let mut mutations = lock(&self.deletion_mutations)?;
        let mut operations = lock(&self.deletion_operations)?;
        let mut holds = lock(&self.retention_holds)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            validate_deletion_mutation(
                existing,
                DeletionMutationKind::RetentionHoldCreate,
                &request.deletion_id,
                &request.request_digest,
                Some(&request.retention_hold_id),
            )?;
            let hold = holds
                .get(&(request.tenant_id.clone(), request.retention_hold_id.clone()))
                .cloned()
                .ok_or_else(|| corruption("hold mutation references a missing hold"))?;
            return Ok(CatalogInsertOutcome::Existing(hold));
        }
        if holds.contains_key(&(request.tenant_id.clone(), request.retention_hold_id.clone())) {
            return Err(conflict(
                "RetentionHold ID is already bound to another request",
            ));
        }
        let operation = operations
            .get_mut(&(request.tenant_id.clone(), request.deletion_id.clone()))
            .ok_or_else(|| conflict("deletion operation does not exist"))?;
        require_operation_version(operation, request.expected_resource_version)?;
        if matches!(
            operation.state,
            DeletionOperationState::Purging
                | DeletionOperationState::Finalizing
                | DeletionOperationState::Completed
        ) {
            return Err(conflict("retention hold is too late for this deletion"));
        }
        let hold = RetentionHold {
            retention_hold_id: request.retention_hold_id.clone(),
            tenant_id: request.tenant_id.clone(),
            deletion_id: request.deletion_id.clone(),
            reason: request.reason,
            state: RetentionHoldState::Active,
            expires_at_unix_ms: request.expires_at_unix_ms,
            created_at_unix_ms: request.now_unix_ms,
            released_at_unix_ms: None,
        };
        holds.insert(
            (request.tenant_id.clone(), request.retention_hold_id.clone()),
            hold.clone(),
        );
        operation.resource_version = next_resource_version(operation.resource_version)?;
        operation.updated_at_unix_ms = request.now_unix_ms;
        mutations.insert(
            mutation_key,
            DeletionMutation {
                tenant_id: request.tenant_id,
                request_id: request.request_id,
                kind: DeletionMutationKind::RetentionHoldCreate,
                request_digest: request.request_digest,
                deletion_id: request.deletion_id,
                retention_hold_id: Some(request.retention_hold_id),
                created_at_unix_ms: request.now_unix_ms,
            },
        );
        Ok(CatalogInsertOutcome::Inserted(hold))
    }

    async fn release_retention_hold_idempotent(
        &self,
        request: ReleaseRetentionHoldRequest,
    ) -> CentralResult<CatalogInsertOutcome<RetentionHold>> {
        let mutation_key = (request.tenant_id.clone(), request.request_id.clone());
        let mut mutations = lock(&self.deletion_mutations)?;
        let mut operations = lock(&self.deletion_operations)?;
        let mut holds = lock(&self.retention_holds)?;
        if let Some(existing) = mutations.get(&mutation_key) {
            validate_deletion_mutation(
                existing,
                DeletionMutationKind::RetentionHoldRelease,
                &request.deletion_id,
                &request.request_digest,
                Some(&request.retention_hold_id),
            )?;
            let hold = holds
                .get(&(request.tenant_id.clone(), request.retention_hold_id.clone()))
                .cloned()
                .ok_or_else(|| corruption("hold release mutation references a missing hold"))?;
            return Ok(CatalogInsertOutcome::Existing(hold));
        }
        let operation = operations
            .get_mut(&(request.tenant_id.clone(), request.deletion_id.clone()))
            .ok_or_else(|| conflict("deletion operation does not exist"))?;
        require_operation_version(operation, request.expected_resource_version)?;
        let hold = holds
            .get_mut(&(request.tenant_id.clone(), request.retention_hold_id.clone()))
            .ok_or_else(|| conflict("retention hold does not exist"))?;
        if hold.deletion_id != request.deletion_id {
            return Err(conflict("retention hold belongs to another deletion"));
        }
        hold.state = RetentionHoldState::Released;
        hold.released_at_unix_ms = Some(request.now_unix_ms);
        let result = hold.clone();
        operation.resource_version = next_resource_version(operation.resource_version)?;
        operation.updated_at_unix_ms = request.now_unix_ms;
        mutations.insert(
            mutation_key,
            DeletionMutation {
                tenant_id: request.tenant_id,
                request_id: request.request_id,
                kind: DeletionMutationKind::RetentionHoldRelease,
                request_digest: request.request_digest,
                deletion_id: request.deletion_id,
                retention_hold_id: Some(request.retention_hold_id),
                created_at_unix_ms: request.now_unix_ms,
            },
        );
        Ok(CatalogInsertOutcome::Inserted(result))
    }

    async fn append_lifecycle_evidence(
        &self,
        tenant_id: &TenantId,
        deletion_id: &DeletionId,
        batch: LifecycleEvidenceBatch,
    ) -> CentralResult<()> {
        if !lock(&self.deletion_operations)?.contains_key(&(tenant_id.clone(), deletion_id.clone()))
        {
            return Err(conflict("deletion operation does not exist"));
        }
        if let Some(event) = batch.event {
            if &event.tenant_id != tenant_id || &event.deletion_id != deletion_id {
                return Err(conflict("lifecycle event scope does not match deletion"));
            }
            let mut events = lock(&self.lifecycle_events)?;
            if let Some(existing) = events.get(&event.event_id) {
                if existing != &event {
                    return Err(conflict("LifecycleEvent ID is already used"));
                }
            } else {
                events.insert(event.event_id.clone(), event);
            }
        }
        if let Some(proof) = batch.proof {
            if &proof.tenant_id != tenant_id || &proof.deletion_id != deletion_id {
                return Err(conflict("deletion proof scope does not match deletion"));
            }
            let mut proofs = lock(&self.deletion_proofs)?;
            if let Some(existing) = proofs.get(&proof.proof_id) {
                if existing != &proof {
                    return Err(conflict("DeletionProof ID is already used"));
                }
            } else {
                proofs.insert(proof.proof_id.clone(), proof);
            }
        }
        Ok(())
    }

    async fn enqueue_lifecycle_assignment(
        &self,
        record: LifecycleAssignmentOutboxRecord,
    ) -> CentralResult<LifecycleAssignmentInsertOutcome> {
        if record.published || record.retired || record.terminal_report_digest.is_some() {
            return Err(conflict(
                "new lifecycle assignments must be unpublished and active",
            ));
        }
        record.assignment.validate().map_err(protocol_error)?;
        let command = &record.assignment.assignment;
        let operations = lock(&self.deletion_operations)?;
        let operation = operations
            .get(&(command.tenant_id.clone(), command.deletion_id.clone()))
            .ok_or_else(|| conflict("lifecycle assignment deletion does not exist"))?;
        validate_lifecycle_assignment_operation(operation, &record.assignment)?;
        drop(operations);
        let mut assignments = lock(&self.lifecycle_assignments)?;
        let key = (command.tenant_id.clone(), command.assignment_id.clone());
        if let Some(existing) = assignments.get(&key) {
            return if existing.assignment == record.assignment {
                Ok(LifecycleAssignmentInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("LifecycleAssignment ID is already used"))
            };
        }
        assignments.insert(key, record.clone());
        Ok(LifecycleAssignmentInsertOutcome::Inserted(record))
    }

    async fn get_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<Option<LifecycleAssignmentOutboxRecord>> {
        Ok(lock(&self.lifecycle_assignments)?
            .get(&(tenant_id.clone(), assignment_id.clone()))
            .cloned())
    }

    async fn pending_lifecycle_assignments_for_agent(
        &self,
        agent_id: &neoengram_domain::protocol::AgentId,
        limit: usize,
    ) -> CentralResult<Vec<LifecycleAssignmentOutboxRecord>> {
        Ok(lock(&self.lifecycle_assignments)?
            .values()
            .filter(|record| record.published && !record.retired)
            .filter(|record| &record.assignment.agent_id == agent_id)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn publish_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<LifecycleAssignmentOutboxRecord> {
        let mut assignments = lock(&self.lifecycle_assignments)?;
        let record = assignments
            .get_mut(&(tenant_id.clone(), assignment_id.clone()))
            .ok_or_else(|| conflict("lifecycle assignment is not reserved"))?;
        if !record.retired {
            record.published = true;
        }
        Ok(record.clone())
    }

    async fn retire_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<LifecycleAssignmentOutboxRecord> {
        let mut assignments = lock(&self.lifecycle_assignments)?;
        let record = assignments
            .get_mut(&(tenant_id.clone(), assignment_id.clone()))
            .ok_or_else(|| conflict("lifecycle assignment is not reserved"))?;
        if !record.published {
            return Err(conflict("lifecycle assignment is not published"));
        }
        record.retired = true;
        Ok(record.clone())
    }

    async fn record_lifecycle_report(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
        report_digest: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<LifecycleAssignmentOutboxRecord> {
        let mut assignments = lock(&self.lifecycle_assignments)?;
        let record = assignments
            .get_mut(&(tenant_id.clone(), assignment_id.clone()))
            .ok_or_else(|| conflict("lifecycle assignment is not reserved"))?;
        if let Some(existing) = &record.terminal_report_digest {
            if existing != report_digest {
                return Err(conflict(
                    "lifecycle assignment already has a different terminal report",
                ));
            }
        } else {
            record.terminal_report_digest = Some(*report_digest);
        }
        Ok(record.clone())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_deletion_impact(
    request: &DeletionImpactQuery,
    projects: &BTreeMap<(TenantId, ProjectId), ProjectRecord>,
    artifacts: &BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    workspaces: &BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>,
    snapshots: &BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
    access_points: &BTreeMap<
        (TenantId, neoengram_domain::protocol::S3AccessPointId),
        S3AccessPointRecord,
    >,
    credentials: &BTreeMap<neoengram_domain::protocol::S3CredentialId, S3CredentialRecord>,
) -> CentralResult<DeletionImpact> {
    let mut targets = Vec::new();
    let mut blockers = request.additional_blockers.clone();
    match &request.root {
        ResourceRef::Project { project_id } => {
            let project = projects
                .get(&(request.tenant_id.clone(), project_id.clone()))
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::ArtifactNotFound,
                        "Project does not exist",
                    )
                })?;
            targets.push(deletion_target(
                request.root.clone(),
                project.resource_version,
                &project.lifecycle,
                false,
            ));
            let mut dependencies = artifacts
                .values()
                .filter(|record| {
                    record.tenant_id == request.tenant_id
                        && record.project_id == *project_id
                        && record.lifecycle.state != ResourceLifecycleState::Deleted
                })
                .map(|record| {
                    deletion_target(
                        ResourceRef::Artifact {
                            project_id: record.project_id.clone(),
                            artifact_id: record.artifact_id.clone(),
                        },
                        record.resource_version,
                        &record.lifecycle,
                        true,
                    )
                })
                .chain(
                    workspaces
                        .values()
                        .filter(|record| {
                            record.tenant_id == request.tenant_id
                                && record.project_id == *project_id
                                && record.lifecycle.state != ResourceLifecycleState::Deleted
                        })
                        .map(|record| {
                            deletion_target(
                                ResourceRef::Workspace {
                                    project_id: record.project_id.clone(),
                                    artifact_id: record.artifact_id.clone(),
                                    workspace_id: record.workspace_id.clone(),
                                },
                                record.resource_version,
                                &record.lifecycle,
                                true,
                            )
                        }),
                )
                .chain(
                    snapshots
                        .values()
                        .filter(|record| {
                            record.tenant_id == request.tenant_id
                                && record.project_id == *project_id
                                && record.lifecycle.state != ResourceLifecycleState::Deleted
                        })
                        .map(|record| {
                            deletion_target(
                                ResourceRef::Snapshot {
                                    snapshot_id: record.snapshot_id.clone(),
                                },
                                record.resource_version,
                                &record.lifecycle,
                                true,
                            )
                        }),
                )
                .collect::<Vec<_>>();
            if !dependencies.is_empty() && !request.cascade {
                blockers.push(neoengram_domain::protocol::DeletionBlocker {
                    code: "DEPENDENCIES_REQUIRE_CASCADE".to_owned(),
                    resource: Some(request.root.clone()),
                    message: "Project still has Artifact, Workspace or Snapshot dependencies"
                        .to_owned(),
                });
            }
            targets.append(&mut dependencies);
        }
        ResourceRef::StorageVolume { storage_volume_id } => {
            let volume = volumes
                .get(&(request.tenant_id.clone(), storage_volume_id.clone()))
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::StorageVolumeNotFound,
                        "StorageVolume does not exist",
                    )
                })?;
            targets.push(deletion_target(
                request.root.clone(),
                volume.resource_version,
                &volume.lifecycle,
                true,
            ));
            let mut dependencies = workspaces
                .values()
                .filter(|record| {
                    record.tenant_id == request.tenant_id
                        && record.storage_volume_id == *storage_volume_id
                        && record.lifecycle.state != ResourceLifecycleState::Deleted
                })
                .map(|record| {
                    deletion_target(
                        ResourceRef::Workspace {
                            project_id: record.project_id.clone(),
                            artifact_id: record.artifact_id.clone(),
                            workspace_id: record.workspace_id.clone(),
                        },
                        record.resource_version,
                        &record.lifecycle,
                        true,
                    )
                })
                .collect::<Vec<_>>();
            if !dependencies.is_empty() && !request.cascade {
                blockers.push(neoengram_domain::protocol::DeletionBlocker {
                    code: "DEPENDENCIES_REQUIRE_CASCADE".to_owned(),
                    resource: Some(request.root.clone()),
                    message: "StorageVolume still contains Workspace resources".to_owned(),
                });
            }
            if !request.confirm_managed_data_erase {
                blockers.push(neoengram_domain::protocol::DeletionBlocker {
                    code: "MANAGED_DATA_ERASE_CONFIRMATION_REQUIRED".to_owned(),
                    resource: Some(request.root.clone()),
                    message: "StorageVolume deletion requires explicit managed-data confirmation"
                        .to_owned(),
                });
            }
            targets.append(&mut dependencies);
        }
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => {
            let artifact = artifacts
                .get(&(request.tenant_id.clone(), artifact_id.clone()))
                .filter(|record| record.project_id == *project_id)
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::ArtifactNotFound,
                        "Artifact does not exist",
                    )
                })?;
            targets.push(deletion_target(
                request.root.clone(),
                artifact.resource_version,
                &artifact.lifecycle,
                true,
            ));
            let mut dependencies = workspaces
                .values()
                .filter(|record| {
                    record.tenant_id == request.tenant_id
                        && record.project_id == *project_id
                        && record.artifact_id == *artifact_id
                        && record.lifecycle.state != ResourceLifecycleState::Deleted
                })
                .map(|record| {
                    deletion_target(
                        ResourceRef::Workspace {
                            project_id: record.project_id.clone(),
                            artifact_id: record.artifact_id.clone(),
                            workspace_id: record.workspace_id.clone(),
                        },
                        record.resource_version,
                        &record.lifecycle,
                        true,
                    )
                })
                .chain(
                    snapshots
                        .values()
                        .filter(|record| {
                            record.tenant_id == request.tenant_id
                                && record.project_id == *project_id
                                && record.artifact_id == *artifact_id
                                && record.lifecycle.state != ResourceLifecycleState::Deleted
                        })
                        .map(|record| {
                            deletion_target(
                                ResourceRef::Snapshot {
                                    snapshot_id: record.snapshot_id.clone(),
                                },
                                record.resource_version,
                                &record.lifecycle,
                                true,
                            )
                        }),
                )
                .collect::<Vec<_>>();
            if !dependencies.is_empty() && !request.cascade {
                blockers.push(neoengram_domain::protocol::DeletionBlocker {
                    code: "DEPENDENCIES_REQUIRE_CASCADE".to_owned(),
                    resource: Some(request.root.clone()),
                    message: "Artifact still has Workspace or Snapshot dependencies".to_owned(),
                });
            }
            targets.append(&mut dependencies);
        }
        ResourceRef::Workspace {
            project_id,
            artifact_id,
            workspace_id,
        } => {
            let workspace = workspaces
                .get(&(
                    request.tenant_id.clone(),
                    project_id.clone(),
                    artifact_id.clone(),
                    workspace_id.clone(),
                ))
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::ArtifactNotFound,
                        "Workspace does not exist",
                    )
                })?;
            targets.push(deletion_target(
                request.root.clone(),
                workspace.resource_version,
                &workspace.lifecycle,
                true,
            ));
        }
        ResourceRef::Snapshot { snapshot_id } => {
            let snapshot = snapshots
                .get(&(request.tenant_id.clone(), snapshot_id.clone()))
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::ArtifactNotFound,
                        "Snapshot does not exist",
                    )
                })?;
            targets.push(deletion_target(
                request.root.clone(),
                snapshot.resource_version,
                &snapshot.lifecycle,
                true,
            ));
        }
    }
    if let Some(target) = targets
        .iter()
        .find(|target| target.resource == request.root)
    {
        if target.lifecycle_generation.get() == 0
            || current_lifecycle_state(
                &request.tenant_id,
                &target.resource,
                projects,
                artifacts,
                volumes,
                workspaces,
                snapshots,
            )? != ResourceLifecycleState::Active
        {
            blockers.push(neoengram_domain::protocol::DeletionBlocker {
                code: "RESOURCE_NOT_ACTIVE".to_owned(),
                resource: Some(request.root.clone()),
                message: "Resource is not in the active lifecycle state".to_owned(),
            });
        }
    }
    for target in &targets {
        if target.resource != request.root
            && current_lifecycle_state(
                &request.tenant_id,
                &target.resource,
                projects,
                artifacts,
                volumes,
                workspaces,
                snapshots,
            )? != ResourceLifecycleState::Active
        {
            blockers.push(neoengram_domain::protocol::DeletionBlocker {
                code: "DEPENDENCY_NOT_ACTIVE".to_owned(),
                resource: Some(target.resource.clone()),
                message: "A dependent resource already has an active lifecycle operation"
                    .to_owned(),
            });
        }
    }
    targets.sort_by(|left, right| left.resource.cmp(&right.resource));
    targets.dedup_by(|left, right| left.resource == right.resource);
    let snapshot_ids = targets
        .iter()
        .filter_map(|target| match &target.resource {
            ResourceRef::Snapshot { snapshot_id } => Some(snapshot_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    let access_point_ids = access_points
        .values()
        .filter(|access_point| {
            access_point.tenant_id == request.tenant_id
                && snapshot_ids.contains(&&access_point.snapshot_id)
        })
        .map(|access_point| &access_point.access_point_id)
        .collect::<Vec<_>>();
    let active_s3_credentials = credentials
        .values()
        .filter(|credential| {
            credential.state == S3CredentialState::Active
                && access_point_ids.contains(&&credential.access_point_id)
        })
        .count();
    let expires_at_unix_ms = checked_timestamp_add(
        request.now_unix_ms,
        DELETION_IMPACT_TTL_MILLIS,
        "deletion impact expiry overflow",
    )?;
    let authority_impact = request.authority_impact.as_ref();
    Ok(DeletionImpact {
        tenant_id: request.tenant_id.clone(),
        root: request.root.clone(),
        cascade: request.cascade,
        confirm_managed_data_erase: request.confirm_managed_data_erase,
        targets,
        active_job_count: authority_impact
            .map(|impact| impact.active_job_count)
            .unwrap_or_else(|| DecimalU64::new(0)),
        active_s3_credential_count: DecimalU64::new(
            u64::try_from(active_s3_credentials)
                .map_err(|_| conflict("active S3 credential count exceeds the supported range"))?,
        ),
        estimated_file_count: authority_impact
            .map(|impact| impact.estimated_file_count)
            .unwrap_or_else(|| DecimalU64::new(0)),
        estimated_bytes: authority_impact
            .map(|impact| impact.estimated_bytes)
            .unwrap_or_else(|| DecimalU64::new(0)),
        blockers,
        issued_at_unix_ms: request.now_unix_ms,
        expires_at_unix_ms,
    })
}

fn deletion_target(
    resource: ResourceRef,
    resource_version: u64,
    lifecycle: &ResourceLifecycle,
    requires_agent_cleanup: bool,
) -> neoengram_domain::protocol::DeletionTarget {
    neoengram_domain::protocol::DeletionTarget {
        resource,
        resource_version: ResourceVersion::new(resource_version),
        lifecycle_generation: lifecycle.generation,
        requires_agent_cleanup,
    }
}

#[allow(clippy::too_many_arguments)]
fn current_lifecycle_state(
    tenant_id: &TenantId,
    resource: &ResourceRef,
    projects: &BTreeMap<(TenantId, ProjectId), ProjectRecord>,
    artifacts: &BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    workspaces: &BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>,
    snapshots: &BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
) -> CentralResult<ResourceLifecycleState> {
    match resource {
        ResourceRef::Project { project_id } => projects
            .get(&(tenant_id.clone(), project_id.clone()))
            .map(|record| record.lifecycle.state),
        ResourceRef::StorageVolume { storage_volume_id } => volumes
            .get(&(tenant_id.clone(), storage_volume_id.clone()))
            .map(|record| record.lifecycle.state),
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => artifacts
            .get(&(tenant_id.clone(), artifact_id.clone()))
            .filter(|record| record.project_id == *project_id)
            .map(|record| record.lifecycle.state),
        ResourceRef::Workspace {
            project_id,
            artifact_id,
            workspace_id,
        } => workspaces
            .get(&(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                workspace_id.clone(),
            ))
            .map(|record| record.lifecycle.state),
        ResourceRef::Snapshot { snapshot_id } => snapshots
            .get(&(tenant_id.clone(), snapshot_id.clone()))
            .map(|record| record.lifecycle.state),
    }
    .ok_or_else(|| corruption("deletion target disappeared from the catalog"))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_current_targets(
    tenant_id: &TenantId,
    targets: &[neoengram_domain::protocol::DeletionTarget],
    projects: &BTreeMap<(TenantId, ProjectId), ProjectRecord>,
    artifacts: &BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    workspaces: &BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>,
    snapshots: &BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
) -> CentralResult<()> {
    for target in targets {
        let current = match &target.resource {
            ResourceRef::Project { project_id } => projects
                .get(&(tenant_id.clone(), project_id.clone()))
                .map(|record| (record.resource_version, &record.lifecycle)),
            ResourceRef::StorageVolume { storage_volume_id } => volumes
                .get(&(tenant_id.clone(), storage_volume_id.clone()))
                .map(|record| (record.resource_version, &record.lifecycle)),
            ResourceRef::Artifact {
                project_id,
                artifact_id,
            } => artifacts
                .get(&(tenant_id.clone(), artifact_id.clone()))
                .filter(|record| record.project_id == *project_id)
                .map(|record| (record.resource_version, &record.lifecycle)),
            ResourceRef::Workspace {
                project_id,
                artifact_id,
                workspace_id,
            } => workspaces
                .get(&(
                    tenant_id.clone(),
                    project_id.clone(),
                    artifact_id.clone(),
                    workspace_id.clone(),
                ))
                .map(|record| (record.resource_version, &record.lifecycle)),
            ResourceRef::Snapshot { snapshot_id } => snapshots
                .get(&(tenant_id.clone(), snapshot_id.clone()))
                .map(|record| (record.resource_version, &record.lifecycle)),
        }
        .ok_or_else(|| concurrent("deletion target no longer exists"))?;
        if current.0 != target.resource_version.get()
            || current.1.generation != target.lifecycle_generation
            || !current.1.is_active()
        {
            return Err(concurrent("deletion impact changed before confirmation"));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn fence_targets_for_delete(
    tenant_id: &TenantId,
    targets: &[neoengram_domain::protocol::DeletionTarget],
    deletion_id: &DeletionId,
    now_unix_ms: UnixMillis,
    purge_after_unix_ms: UnixMillis,
    projects: &mut BTreeMap<(TenantId, ProjectId), ProjectRecord>,
    artifacts: &mut BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &mut BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    workspaces: &mut BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>,
    snapshots: &mut BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
) -> CentralResult<Vec<neoengram_domain::protocol::DeletionTarget>> {
    let mut fenced = Vec::with_capacity(targets.len());
    for target in targets {
        let (resource_version, lifecycle) = match &target.resource {
            ResourceRef::Project { project_id } => {
                let record = projects
                    .get_mut(&(tenant_id.clone(), project_id.clone()))
                    .ok_or_else(|| concurrent("Project disappeared during delete"))?;
                record.resource_version = next_plain_version(record.resource_version)?;
                record.updated_at_unix_ms = now_unix_ms;
                (record.resource_version, &mut record.lifecycle)
            }
            ResourceRef::StorageVolume { storage_volume_id } => {
                let record = volumes
                    .get_mut(&(tenant_id.clone(), storage_volume_id.clone()))
                    .ok_or_else(|| concurrent("StorageVolume disappeared during delete"))?;
                record.resource_version = next_plain_version(record.resource_version)?;
                record.updated_at_unix_ms = now_unix_ms;
                (record.resource_version, &mut record.lifecycle)
            }
            ResourceRef::Artifact {
                project_id,
                artifact_id,
            } => {
                let record = artifacts
                    .get_mut(&(tenant_id.clone(), artifact_id.clone()))
                    .filter(|record| record.project_id == *project_id)
                    .ok_or_else(|| concurrent("Artifact disappeared during delete"))?;
                record.resource_version = next_plain_version(record.resource_version)?;
                record.updated_at_unix_ms = now_unix_ms;
                (record.resource_version, &mut record.lifecycle)
            }
            ResourceRef::Workspace {
                project_id,
                artifact_id,
                workspace_id,
            } => {
                let record = workspaces
                    .get_mut(&(
                        tenant_id.clone(),
                        project_id.clone(),
                        artifact_id.clone(),
                        workspace_id.clone(),
                    ))
                    .ok_or_else(|| concurrent("Workspace disappeared during delete"))?;
                record.resource_version = next_plain_version(record.resource_version)?;
                record.updated_at_unix_ms = now_unix_ms;
                (record.resource_version, &mut record.lifecycle)
            }
            ResourceRef::Snapshot { snapshot_id } => {
                let record = snapshots
                    .get_mut(&(tenant_id.clone(), snapshot_id.clone()))
                    .ok_or_else(|| concurrent("Snapshot disappeared during delete"))?;
                record.resource_version = next_plain_version(record.resource_version)?;
                record.updated_at_unix_ms = now_unix_ms;
                (record.resource_version, &mut record.lifecycle)
            }
        };
        if !lifecycle.is_active() {
            return Err(concurrent("deletion target is no longer active"));
        }
        lifecycle.state = ResourceLifecycleState::PendingDelete;
        lifecycle.generation = next_lifecycle_generation(lifecycle.generation)?;
        lifecycle.active_deletion_id = Some(deletion_id.clone());
        lifecycle.delete_requested_at_unix_ms = Some(now_unix_ms);
        lifecycle.purge_after_unix_ms = Some(purge_after_unix_ms);
        lifecycle.deleted_at_unix_ms = None;
        fenced.push(deletion_target(
            target.resource.clone(),
            resource_version,
            lifecycle,
            target.requires_agent_cleanup,
        ));
    }
    Ok(fenced)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn set_target_lifecycle_state(
    tenant_id: &TenantId,
    targets: &[neoengram_domain::protocol::DeletionTarget],
    deletion_id: &DeletionId,
    state: ResourceLifecycleState,
    now_unix_ms: UnixMillis,
    projects: &mut BTreeMap<(TenantId, ProjectId), ProjectRecord>,
    artifacts: &mut BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &mut BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    workspaces: &mut BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>,
    snapshots: &mut BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
) -> CentralResult<Vec<neoengram_domain::protocol::DeletionTarget>> {
    let mut updated = Vec::with_capacity(targets.len());
    for target in targets {
        let (resource_version, lifecycle) = mutable_resource_lifecycle(
            tenant_id,
            &target.resource,
            now_unix_ms,
            projects,
            artifacts,
            volumes,
            workspaces,
            snapshots,
        )?;
        if lifecycle.active_deletion_id.as_ref() != Some(deletion_id)
            || lifecycle.state == ResourceLifecycleState::Deleted
        {
            return Err(concurrent(
                "resource lifecycle is fenced by another operation",
            ));
        }
        lifecycle.state = state;
        lifecycle.generation = next_lifecycle_generation(lifecycle.generation)?;
        updated.push(deletion_target(
            target.resource.clone(),
            resource_version,
            lifecycle,
            target.requires_agent_cleanup,
        ));
    }
    Ok(updated)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn finalize_targets(
    tenant_id: &TenantId,
    targets: &[neoengram_domain::protocol::DeletionTarget],
    deletion_id: &DeletionId,
    completion: DeletionCompletion,
    now_unix_ms: UnixMillis,
    projects: &mut BTreeMap<(TenantId, ProjectId), ProjectRecord>,
    artifacts: &mut BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &mut BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    workspaces: &mut BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>,
    snapshots: &mut BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
) -> CentralResult<Vec<neoengram_domain::protocol::DeletionTarget>> {
    let mut updated = Vec::with_capacity(targets.len());
    for target in targets {
        let (resource_version, lifecycle) = mutable_resource_lifecycle(
            tenant_id,
            &target.resource,
            now_unix_ms,
            projects,
            artifacts,
            volumes,
            workspaces,
            snapshots,
        )?;
        if lifecycle.active_deletion_id.as_ref() != Some(deletion_id) {
            return Err(concurrent(
                "resource lifecycle is fenced by another operation",
            ));
        }
        lifecycle.generation = next_lifecycle_generation(lifecycle.generation)?;
        match completion {
            DeletionCompletion::Restored => {
                lifecycle.state = ResourceLifecycleState::Active;
                lifecycle.active_deletion_id = None;
                lifecycle.delete_requested_at_unix_ms = None;
                lifecycle.purge_after_unix_ms = None;
                lifecycle.deleted_at_unix_ms = None;
            }
            DeletionCompletion::Purged => {
                lifecycle.state = ResourceLifecycleState::Deleted;
                lifecycle.deleted_at_unix_ms = Some(now_unix_ms);
            }
        }
        updated.push(deletion_target(
            target.resource.clone(),
            resource_version,
            lifecycle,
            target.requires_agent_cleanup,
        ));
    }
    Ok(updated)
}

#[allow(clippy::too_many_arguments)]
fn mutable_resource_lifecycle<'a>(
    tenant_id: &TenantId,
    resource: &ResourceRef,
    now_unix_ms: UnixMillis,
    projects: &'a mut BTreeMap<(TenantId, ProjectId), ProjectRecord>,
    artifacts: &'a mut BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &'a mut BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    workspaces: &'a mut BTreeMap<(TenantId, ProjectId, ArtifactId, WorkspaceId), WorkspaceRecord>,
    snapshots: &'a mut BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
) -> CentralResult<(u64, &'a mut ResourceLifecycle)> {
    match resource {
        ResourceRef::Project { project_id } => {
            let record = projects
                .get_mut(&(tenant_id.clone(), project_id.clone()))
                .ok_or_else(|| concurrent("Project disappeared during lifecycle update"))?;
            record.resource_version = next_plain_version(record.resource_version)?;
            record.updated_at_unix_ms = now_unix_ms;
            Ok((record.resource_version, &mut record.lifecycle))
        }
        ResourceRef::StorageVolume { storage_volume_id } => {
            let record = volumes
                .get_mut(&(tenant_id.clone(), storage_volume_id.clone()))
                .ok_or_else(|| concurrent("StorageVolume disappeared during lifecycle update"))?;
            record.resource_version = next_plain_version(record.resource_version)?;
            record.updated_at_unix_ms = now_unix_ms;
            Ok((record.resource_version, &mut record.lifecycle))
        }
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => {
            let record = artifacts
                .get_mut(&(tenant_id.clone(), artifact_id.clone()))
                .filter(|record| record.project_id == *project_id)
                .ok_or_else(|| concurrent("Artifact disappeared during lifecycle update"))?;
            record.resource_version = next_plain_version(record.resource_version)?;
            record.updated_at_unix_ms = now_unix_ms;
            Ok((record.resource_version, &mut record.lifecycle))
        }
        ResourceRef::Workspace {
            project_id,
            artifact_id,
            workspace_id,
        } => {
            let record = workspaces
                .get_mut(&(
                    tenant_id.clone(),
                    project_id.clone(),
                    artifact_id.clone(),
                    workspace_id.clone(),
                ))
                .ok_or_else(|| concurrent("Workspace disappeared during lifecycle update"))?;
            record.resource_version = next_plain_version(record.resource_version)?;
            record.updated_at_unix_ms = now_unix_ms;
            Ok((record.resource_version, &mut record.lifecycle))
        }
        ResourceRef::Snapshot { snapshot_id } => {
            let record = snapshots
                .get_mut(&(tenant_id.clone(), snapshot_id.clone()))
                .ok_or_else(|| concurrent("Snapshot disappeared during lifecycle update"))?;
            record.resource_version = next_plain_version(record.resource_version)?;
            record.updated_at_unix_ms = now_unix_ms;
            Ok((record.resource_version, &mut record.lifecycle))
        }
    }
}

fn disable_snapshot_s3_access(
    tenant_id: &TenantId,
    snapshot_ids: &[SnapshotId],
    now_unix_ms: UnixMillis,
    access_points: &mut BTreeMap<
        (TenantId, neoengram_domain::protocol::S3AccessPointId),
        S3AccessPointRecord,
    >,
    credentials: &mut BTreeMap<neoengram_domain::protocol::S3CredentialId, S3CredentialRecord>,
) -> CentralResult<()> {
    let affected = access_points
        .values_mut()
        .filter(|access_point| {
            access_point.tenant_id == *tenant_id
                && snapshot_ids.contains(&access_point.snapshot_id)
                && access_point.state != S3AccessPointState::Deleted
        })
        .map(|access_point| {
            access_point.state = S3AccessPointState::Disabled;
            access_point.policy_generation = access_point
                .policy_generation
                .checked_add(1)
                .ok_or_else(|| conflict("S3 policy generation exhausted"))?;
            access_point.updated_at_unix_ms = now_unix_ms;
            Ok(access_point.access_point_id.clone())
        })
        .collect::<CentralResult<Vec<_>>>()?;
    for credential in credentials.values_mut() {
        if affected.contains(&credential.access_point_id) {
            credential.state = S3CredentialState::Revoked;
            credential.encrypted_secret.clear();
        }
    }
    Ok(())
}

fn validate_deletion_mutation(
    existing: &DeletionMutation,
    kind: DeletionMutationKind,
    deletion_id: &DeletionId,
    request_digest: &ContentDigest,
    retention_hold_id: Option<&RetentionHoldId>,
) -> CentralResult<()> {
    if existing.kind == kind
        && &existing.deletion_id == deletion_id
        && &existing.request_digest == request_digest
        && existing.retention_hold_id.as_ref() == retention_hold_id
    {
        Ok(())
    } else {
        Err(conflict(
            "lifecycle request identity is already bound to another mutation",
        ))
    }
}

fn require_operation_version(
    operation: &DeletionOperation,
    expected_resource_version: u64,
) -> CentralResult<()> {
    if operation.resource_version.get() == expected_resource_version {
        Ok(())
    } else {
        Err(concurrent("deletion operation resource version changed"))
    }
}

pub(crate) fn valid_deletion_transition(
    expected: DeletionOperationState,
    next: DeletionOperationState,
) -> bool {
    matches!(
        (expected, next),
        (
            DeletionOperationState::Requested,
            DeletionOperationState::Quiescing
        ) | (
            DeletionOperationState::Quiescing,
            DeletionOperationState::Quarantining
        ) | (
            DeletionOperationState::Quarantining,
            DeletionOperationState::Recoverable
        ) | (
            DeletionOperationState::Recoverable,
            DeletionOperationState::Purging
        ) | (
            DeletionOperationState::Purging,
            DeletionOperationState::Finalizing
        ) | (
            DeletionOperationState::Finalizing,
            DeletionOperationState::Completed
        ) | (
            DeletionOperationState::Restoring,
            DeletionOperationState::Completed
        )
    ) || (!matches!(expected, DeletionOperationState::Completed)
        && matches!(
            next,
            DeletionOperationState::Blocked | DeletionOperationState::Failed
        ))
}

pub(crate) const fn retryable_deletion_resume_state(state: DeletionOperationState) -> bool {
    matches!(
        state,
        DeletionOperationState::Requested
            | DeletionOperationState::Quiescing
            | DeletionOperationState::Quarantining
            | DeletionOperationState::Recoverable
            | DeletionOperationState::Restoring
            | DeletionOperationState::Purging
            | DeletionOperationState::Finalizing
    )
}

pub(crate) fn retention_hold_is_active(hold: &RetentionHold, now_unix_ms: UnixMillis) -> bool {
    hold.state == RetentionHoldState::Active
        && hold
            .expires_at_unix_ms
            .is_none_or(|expires| expires > now_unix_ms)
}

fn checked_timestamp_add(
    timestamp: UnixMillis,
    delta: u64,
    message: &'static str,
) -> CentralResult<UnixMillis> {
    timestamp
        .get()
        .checked_add(delta)
        .map(UnixMillis::new)
        .ok_or_else(|| conflict(message))
}

fn next_plain_version(current: u64) -> CentralResult<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| conflict("resource version exhausted"))
}

fn next_resource_version(current: ResourceVersion) -> CentralResult<ResourceVersion> {
    next_plain_version(current.get()).map(ResourceVersion::new)
}

fn next_lifecycle_generation(current: LifecycleGeneration) -> CentralResult<LifecycleGeneration> {
    current
        .get()
        .checked_add(1)
        .map(LifecycleGeneration::new)
        .ok_or_else(|| conflict("resource lifecycle generation exhausted"))
}

fn concurrent(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::ConcurrentUpdate, message).with_retryable(true)
}

fn protocol_error(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(CentralErrorCode::ProtocolInvalid, error.to_string()).with_retryable(false)
}

fn tenant_after(record: &TenantRecord, after: &TenantListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && record.tenant_id > after.tenant_id)
}

fn project_after(record: &ProjectRecord, after: &ProjectListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && record.project_id > after.project_id)
}

fn project_matches(existing: &ProjectRecord, requested: &ProjectRecord) -> bool {
    existing.tenant_id == requested.tenant_id
        && existing.project_id == requested.project_id
        && existing.display_name == requested.display_name
        && existing.description == requested.description
}

fn volume_after(record: &StorageVolumeRecord, after: &StorageVolumeListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && record.storage_volume_id > after.storage_volume_id)
}

fn artifact_after(record: &ArtifactRecord, after: &ArtifactListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && (&record.project_id, &record.artifact_id) > (&after.project_id, &after.artifact_id))
}

fn workspace_after(record: &WorkspaceRecord, after: &WorkspaceListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && (
                &record.project_id,
                &record.artifact_id,
                &record.workspace_id,
            ) > (&after.project_id, &after.artifact_id, &after.workspace_id))
}

fn snapshot_after(record: &SnapshotRecord, after: &SnapshotListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && record.snapshot_id > after.snapshot_id)
}

fn matches_query(id: &str, name: &str, query: Option<&str>) -> bool {
    query.is_none_or(|query| {
        let query = query.to_lowercase();
        id.to_lowercase().contains(&query) || name.to_lowercase().contains(&query)
    })
}

fn volume_matches(left: &StorageVolumeRecord, right: &StorageVolumeRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.storage_volume_id == right.storage_volume_id
        && left.display_name == right.display_name
        && left.edge_cluster_id == right.edge_cluster_id
        && left.region == right.region
        && left.backend_type == right.backend_type
        && left.access_mode == right.access_mode
        && left.pvc_reference == right.pvc_reference
        && left.nfs_reference == right.nfs_reference
}

fn artifact_matches(left: &ArtifactRecord, right: &ArtifactRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.project_id == right.project_id
        && left.artifact_id == right.artifact_id
        && left.display_name == right.display_name
        && left.description == right.description
        && left.initialization == right.initialization
}

fn workspace_matches_insert(
    existing: &WorkspaceRecord,
    requested: &WorkspaceRecord,
    artifact_head: &ArtifactHeadExpectation,
) -> bool {
    let commit_selection_matches = match artifact_head {
        ArtifactHeadExpectation::Any => {
            existing.base_commit_id == requested.base_commit_id
                && existing.head_commit_id == requested.head_commit_id
        }
        // An omitted base is part of the public create identity. The resolved Head is an
        // optimistic first-insert fence, not a new field supplied by the caller on replay.
        ArtifactHeadExpectation::Exact(_) => true,
    };
    existing.tenant_id == requested.tenant_id
        && existing.project_id == requested.project_id
        && existing.artifact_id == requested.artifact_id
        && existing.workspace_id == requested.workspace_id
        && existing.storage_volume_id == requested.storage_volume_id
        && existing.region == requested.region
        && existing.display_name == requested.display_name
        && commit_selection_matches
}

fn snapshot_request_matches(existing: &SnapshotRecord, requested: &SnapshotRecord) -> bool {
    existing.tenant_id == requested.tenant_id
        && existing.project_id == requested.project_id
        && existing.artifact_id == requested.artifact_id
        && existing.snapshot_id == requested.snapshot_id
        && existing.snapshot_request_id == requested.snapshot_request_id
        && existing.commit_id == requested.commit_id
        && existing.delivery_id == requested.delivery_id
        && existing.edge_cluster_id == requested.edge_cluster_id
        && existing.storage_volume_id == requested.storage_volume_id
        && existing.delivery_mode == requested.delivery_mode
}

fn validate_limit(limit: u16) -> CentralResult<()> {
    if (1..=100).contains(&limit) {
        Ok(())
    } else {
        Err(CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            "catalog page size is outside the supported range",
        )
        .with_retryable(false))
    }
}

fn validate_new_resource(
    resource_version: u64,
    lifecycle: &ResourceLifecycle,
    kind: &'static str,
) -> CentralResult<()> {
    if resource_version != 1 || lifecycle != &ResourceLifecycle::active() {
        return Err(CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            format!("new {kind} must start at resource version 1 with an active lifecycle"),
        )
        .with_retryable(false));
    }
    Ok(())
}

pub(crate) fn validate_lifecycle_assignment_operation(
    operation: &DeletionOperation,
    assignment: &AgentResourceLifecycleAssignment,
) -> CentralResult<()> {
    let command = &assignment.assignment;
    if operation.state == DeletionOperationState::Completed
        || operation.request_digest != command.request_digest
        || !operation.targets.iter().any(|target| {
            target.resource == command.resource
                && target.lifecycle_generation == command.lifecycle_generation
                && target.requires_agent_cleanup
        })
    {
        return Err(conflict(
            "lifecycle assignment does not match the active deletion fence",
        ));
    }
    Ok(())
}

fn require_active(lifecycle: &ResourceLifecycle, kind: &'static str) -> CentralResult<()> {
    if lifecycle.is_active() {
        Ok(())
    } else {
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            format!("{kind} is not in the active lifecycle state"),
        )
        .with_retryable(false))
    }
}

fn require_s3_snapshot(
    snapshots: &BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
    deliveries: &BTreeMap<(TenantId, SnapshotDeliveryId), SnapshotDeliveryRecord>,
    access_point: &S3AccessPointRecord,
) -> CentralResult<()> {
    let snapshot = snapshots
        .get(&(
            access_point.tenant_id.clone(),
            access_point.snapshot_id.clone(),
        ))
        .filter(|snapshot| {
            snapshot.project_id == access_point.project_id
                && snapshot.artifact_id == access_point.artifact_id
                && snapshot.commit_id == access_point.commit_id
        })
        .ok_or_else(|| conflict("S3 Access Point Snapshot binding does not exist"))?;
    require_active(&snapshot.lifecycle, "S3 Access Point Snapshot")?;
    if snapshot.state != SnapshotState::Ready {
        return Err(conflict("S3 Access Point Snapshot is not Ready"));
    }
    if snapshot.delivery_id != access_point.delivery_id
        || snapshot.storage_volume_id != access_point.storage_volume_id
        || snapshot.edge_cluster_id != access_point.edge_cluster_id
    {
        return Err(conflict(
            "S3 Access Point Snapshot target binding does not match",
        ));
    }
    let delivery = deliveries
        .get(&(
            access_point.tenant_id.clone(),
            access_point.delivery_id.clone(),
        ))
        .ok_or_else(|| conflict("S3 Access Point SnapshotDelivery does not exist"))?;
    if delivery.snapshot_id != access_point.snapshot_id
        || delivery.commit_id != access_point.commit_id
        || delivery.storage_volume_id != access_point.storage_volume_id
        || delivery.mode != snapshot.delivery_mode
        || delivery.state != SnapshotDeliveryState::Ready
    {
        return Err(conflict("S3 Access Point SnapshotDelivery is not Ready"));
    }
    Ok(())
}

fn conflict(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(false)
}

fn same_s3_credential_identity(
    existing: &S3CredentialRecord,
    requested: &S3CredentialRecord,
) -> bool {
    existing.credential_id == requested.credential_id
        && existing.access_point_id == requested.access_point_id
        && existing.access_key_id == requested.access_key_id
        && existing.expires_at_unix_ms == requested.expires_at_unix_ms
        && existing.created_at_unix_ms == requested.created_at_unix_ms
}

fn same_s3_access_point_create_identity(
    existing: &S3AccessPointRecord,
    requested: &S3AccessPointRecord,
) -> bool {
    existing.access_point_id == requested.access_point_id
        && existing.tenant_id == requested.tenant_id
        && existing.project_id == requested.project_id
        && existing.artifact_id == requested.artifact_id
        && existing.snapshot_id == requested.snapshot_id
        && existing.commit_id == requested.commit_id
        && existing.delivery_id == requested.delivery_id
        && existing.storage_volume_id == requested.storage_volume_id
        && existing.edge_cluster_id == requested.edge_cluster_id
        && existing.bucket_name == requested.bucket_name
}

fn same_s3_mutation_identity(existing: &S3MutationRecord, requested: &S3MutationRecord) -> bool {
    existing.tenant_id == requested.tenant_id
        && existing.request_id == requested.request_id
        && existing.operation == requested.operation
        && existing.request_digest == requested.request_digest
}

fn corruption(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}

fn catalog_parent_error(code: CentralErrorCode, message: &'static str) -> CentralError {
    CentralError::new(code, message).with_retryable(false)
}

fn artifact_head_changed() -> CentralError {
    CentralError::new(
        CentralErrorCode::ArtifactHeadMismatch,
        "Artifact Head changed before Workspace creation",
    )
    .with_retryable(true)
}

fn lock<T>(mutex: &Mutex<T>) -> CentralResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        CentralError::new(
            CentralErrorCode::Internal,
            "in-memory catalog lock poisoned",
        )
    })
}
