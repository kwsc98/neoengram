use std::collections::BTreeMap;

use async_trait::async_trait;
use neoengram_domain::core::{ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    ArtifactId, DecimalU64, DeletionCompletion, DeletionId, DeletionMutation, DeletionMutationKind,
    DeletionOperation, DeletionOperationState, DeliveryGeneration, EdgeClusterId, HardlinkPolicy,
    PlaygroundId, ProjectId, RequestId, ResourceLifecycle, ResourceLifecycleState, ResourceRef,
    ResourceVersion, RetentionHold, RetentionHoldId, RetentionHoldState, S3AccessPointId,
    S3CredentialId, SnapshotDeliveryId, SnapshotDeliveryMode, SnapshotDeliveryState, SnapshotId,
    StorageVolumeId, TenantId, UnixMillis, DELETION_RECOVERY_WINDOW_MILLIS,
};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{sqlite::SqliteRow, QueryBuilder, Row, Sqlite, Transaction};

use crate::{
    AdvancePlaygroundCommitOutcome, AdvancePlaygroundCommitRequest, AgentRegistryRecord,
    ArtifactHeadExpectation, ArtifactInitialization, ArtifactListCursor, ArtifactListPage,
    ArtifactListRequest, ArtifactRecord, CatalogInsertOutcome, CatalogNfsReference,
    CatalogPvcReference, CentralError, CentralErrorCode, CentralResult, ControlCatalogRepository,
    CreateDeletionRequest, CreateRetentionHoldRequest, DeletionImpactQuery, DeletionImpactRecord,
    DeletionListCursor, DeletionListPage, DeletionListRequest, DeletionTransitionRequest,
    DerivedVolumeState, LifecycleAssignmentInsertOutcome, LifecycleAssignmentOutboxRecord,
    LifecycleEvidenceBatch, PlaygroundInsertRequest, PlaygroundListCursor, PlaygroundListPage,
    PlaygroundListRequest, PlaygroundRecord, PlaygroundState, ProjectListCursor, ProjectListPage,
    ProjectListRequest, ProjectRecord, ReleaseRetentionHoldRequest, RestoreDeletionRequest,
    RetryDeletionRequest, S3AccessPointCreateResult, S3AccessPointInsertOutcome,
    S3AccessPointListCursor, S3AccessPointListPage, S3AccessPointListRequest, S3AccessPointRecord,
    S3AccessPointState, S3CredentialInsertOutcome, S3CredentialRecord, S3CredentialState,
    S3MutationKind, S3MutationRecord, SnapshotDeliveryInsertOutcome, SnapshotDeliveryInsertRequest,
    SnapshotDeliveryListRequest, SnapshotDeliveryMutationKind, SnapshotDeliveryMutationRecord,
    SnapshotDeliveryMutationRequest, SnapshotDeliveryRecord, SnapshotDeliveryRetentionRoot,
    SnapshotListCursor, SnapshotListPage, SnapshotListRequest, SnapshotRecord, SnapshotState,
    SnapshotWithDeliveryInsertRequest, SnapshotWithDeliveryInsertResult, StorageAccessMode,
    StorageBackendType, StorageEnrollmentAccessMode, StorageVolumeListCursor,
    StorageVolumeListPage, StorageVolumeListRequest, StorageVolumeRecord, StorageVolumeState,
    TenantListCursor, TenantListPage, TenantListRequest, TenantRecord,
};

use super::agent_registry::SqliteAgentRegistryStore;

const TENANT_COLUMNS: &str =
    "tenant_id, display_name, description, resource_version, created_at_unix_ms, updated_at_unix_ms";
const PROJECT_COLUMNS: &str =
    "tenant_id, project_id, display_name, description, resource_version, created_at_unix_ms, updated_at_unix_ms";
const ARTIFACT_COLUMNS: &str = "tenant_id, project_id, artifact_id, display_name, description, \
    initialization_mode, source_project_id, source_artifact_id, source_commit_digest, \
    head_commit_digest, resource_version, lifecycle_state, lifecycle_generation, \
    active_deletion_id, delete_requested_at_unix_ms, purge_after_unix_ms, deleted_at_unix_ms, \
    created_at_unix_ms, updated_at_unix_ms";
const VOLUME_COLUMNS: &str =
    "tenant_id, storage_volume_id, display_name, edge_cluster_id, region, \
    backend_type, access_mode, allowed_delivery_modes, hardlink_policy, max_whole_file_bytes, \
    copy_reserve_bytes, pvc_namespace, pvc_claim_name, nfs_server, nfs_export_path, state, \
    resource_version, lifecycle_state, lifecycle_generation, active_deletion_id, \
    delete_requested_at_unix_ms, purge_after_unix_ms, deleted_at_unix_ms, created_at_unix_ms, \
    updated_at_unix_ms";
const PLAYGROUND_COLUMNS: &str = "tenant_id, project_id, artifact_id, playground_id, \
    storage_volume_id, region, display_name, base_commit_digest, head_commit_digest, \
    state, relative_root, resource_version, lifecycle_state, lifecycle_generation, \
    active_deletion_id, delete_requested_at_unix_ms, purge_after_unix_ms, deleted_at_unix_ms, \
    created_at_unix_ms, updated_at_unix_ms";
const SNAPSHOT_COLUMNS: &str = "tenant_id, project_id, artifact_id, snapshot_id, \
    snapshot_request_id, commit_digest, delivery_id, edge_cluster_id, storage_volume_id, \
    delivery_mode, state, \
    resource_version, lifecycle_state, lifecycle_generation, active_deletion_id, \
    delete_requested_at_unix_ms, purge_after_unix_ms, deleted_at_unix_ms, created_at_unix_ms, \
    updated_at_unix_ms";
const SNAPSHOT_DELIVERY_COLUMNS: &str = "tenant_id, delivery_id, create_request_id, snapshot_id, \
    commit_digest, storage_volume_id, mode, target_relative_root, state, source_index_digest, \
    delivery_generation, file_count, size_bytes, object_set_digest, resource_version, issue_code, \
    issue_message, issue_retryable, created_at_unix_ms, updated_at_unix_ms";
const S3_ACCESS_POINT_COLUMNS: &str = "access_point_id, tenant_id, project_id, artifact_id, \
    snapshot_id, commit_digest, delivery_id, storage_volume_id, edge_cluster_id, bucket_name, state, policy_generation, \
    created_at_unix_ms, updated_at_unix_ms";
const S3_CREDENTIAL_COLUMNS: &str = "credential_id, access_point_id, access_key_id, \
    encrypted_secret, state, expires_at_unix_ms, created_at_unix_ms, last_used_at_unix_ms";
const S3_MUTATION_COLUMNS: &str =
    "tenant_id, request_id, operation, request_digest, created_at_unix_ms";

#[async_trait]
impl ControlCatalogRepository for SqliteAgentRegistryStore {
    async fn get_tenant(&self, tenant_id: &TenantId) -> CentralResult<Option<TenantRecord>> {
        let sql =
            format!("SELECT {TENANT_COLUMNS} FROM tenant_catalog_records WHERE tenant_id = ?");
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_tenant)
            .transpose()
    }

    async fn list_tenants(&self, request: &TenantListRequest) -> CentralResult<TenantListPage> {
        validate_limit(request.limit)?;
        if request
            .visible_tenant_ids
            .as_ref()
            .is_some_and(Vec::is_empty)
        {
            return Ok(TenantListPage {
                records: Vec::new(),
                next: None,
            });
        }
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {TENANT_COLUMNS} FROM tenant_catalog_records WHERE 1 = 1"
        ));
        if let Some(tenant_ids) = &request.visible_tenant_ids {
            query.push(" AND tenant_id IN (");
            let mut separated = query.separated(", ");
            for tenant_id in tenant_ids {
                separated.push_bind(tenant_id.as_str());
            }
            separated.push_unseparated(")");
        }
        if let Some(search) = &request.query {
            let pattern = format!("%{}%", escape_like(&search.to_lowercase()));
            query
                .push(" AND (LOWER(tenant_id) LIKE ")
                .push_bind(pattern.clone());
            query
                .push(" ESCAPE '\\' OR LOWER(display_name) LIKE ")
                .push_bind(pattern)
                .push(" ESCAPE '\\')");
        }
        if let Some(after) = &request.after {
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" AND tenant_id > ")
                .push_bind(after.tenant_id.as_str())
                .push("))");
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, tenant_id ASC LIMIT ")
            .push_bind(i64::from(request.limit) + 1);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        tenant_page(rows, request.limit)
    }

    async fn insert_tenant(
        &self,
        record: TenantRecord,
    ) -> CentralResult<CatalogInsertOutcome<TenantRecord>> {
        if let Some(existing) = self.get_tenant(&record.tenant_id).await? {
            return if tenant_create_matches(&existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing))
            } else {
                Err(id_reused(
                    "Tenant ID is already bound to another create request",
                ))
            };
        }
        let result = sqlx::query(
            "INSERT INTO tenant_catalog_records \
             (tenant_id, display_name, description, resource_version, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(&record.display_name)
        .bind(&record.description)
        .bind(record.resource_version.to_string())
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(CatalogInsertOutcome::Inserted(record)),
            Err(error) if is_unique(&error) => {
                let existing = self.get_tenant(&record.tenant_id).await?.ok_or_else(|| {
                    storage_error("Tenant uniqueness conflict could not be resolved")
                })?;
                if tenant_create_matches(&existing, &record) {
                    Ok(CatalogInsertOutcome::Existing(existing))
                } else {
                    Err(id_reused(
                        "Tenant ID is already bound to another create request",
                    ))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn get_project(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
    ) -> CentralResult<Option<ProjectRecord>> {
        let sql = format!(
            "SELECT {PROJECT_COLUMNS} FROM project_catalog_records WHERE tenant_id = ? AND project_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(project_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_project)
            .transpose()
    }

    async fn list_projects(&self, request: &ProjectListRequest) -> CentralResult<ProjectListPage> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {PROJECT_COLUMNS} FROM project_catalog_records WHERE tenant_id = "
        ));
        query.push_bind(request.tenant_id.as_str());
        if let Some(search) = &request.query {
            let pattern = format!("%{}%", escape_like(&search.to_lowercase()));
            query
                .push(" AND (LOWER(project_id) LIKE ")
                .push_bind(pattern.clone())
                .push(" ESCAPE '\\' OR LOWER(display_name) LIKE ")
                .push_bind(pattern)
                .push(" ESCAPE '\\')");
        }
        if let Some(after) = &request.after {
            let created = as_i64(after.created_at_unix_ms)?;
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(created)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(created)
                .push(" AND project_id > ")
                .push_bind(after.project_id.as_str())
                .push("))");
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, project_id ASC LIMIT ")
            .push_bind(i64::from(request.limit) + 1);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        project_page(rows, request.limit)
    }

    async fn insert_project(
        &self,
        record: ProjectRecord,
    ) -> CentralResult<CatalogInsertOutcome<ProjectRecord>> {
        if let Some(existing) = self
            .get_project(&record.tenant_id, &record.project_id)
            .await?
        {
            return if project_create_matches(&existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing))
            } else {
                Err(id_reused(
                    "Project ID is already bound to another create request",
                ))
            };
        }
        let result = sqlx::query(
            "INSERT INTO project_catalog_records \
             (tenant_id, project_id, display_name, description, resource_version, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.project_id.as_str())
        .bind(&record.display_name)
        .bind(&record.description)
        .bind(record.resource_version.to_string())
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(CatalogInsertOutcome::Inserted(record)),
            Err(error) if is_unique(&error) => {
                let existing = self
                    .get_project(&record.tenant_id, &record.project_id)
                    .await?
                    .ok_or_else(|| {
                        storage_error("Project uniqueness conflict could not be resolved")
                    })?;
                if project_create_matches(&existing, &record) {
                    Ok(CatalogInsertOutcome::Existing(existing))
                } else {
                    Err(id_reused(
                        "Project ID is already bound to another create request",
                    ))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn get_artifact(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Option<ArtifactRecord>> {
        let sql = format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? \
               AND lifecycle_state = 'active'"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_artifact)
            .transpose()
    }

    async fn get_artifact_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Option<ArtifactRecord>> {
        let sql = format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_artifact)
            .transpose()
    }

    async fn list_artifacts(
        &self,
        request: &ArtifactListRequest,
    ) -> CentralResult<ArtifactListPage> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records WHERE tenant_id = "
        ));
        query.push_bind(request.tenant_id.as_str());
        query.push(" AND lifecycle_state = 'active'");
        if let Some(project_id) = &request.project_id {
            query
                .push(" AND project_id = ")
                .push_bind(project_id.as_str());
        }
        if let Some(search) = &request.query {
            let pattern = format!("%{}%", escape_like(&search.to_lowercase()));
            query
                .push(" AND (LOWER(artifact_id) LIKE ")
                .push_bind(pattern.clone())
                .push(" ESCAPE '\\' OR LOWER(display_name) LIKE ")
                .push_bind(pattern)
                .push(" ESCAPE '\\')");
        }
        if let Some(after) = &request.after {
            let created = as_i64(after.created_at_unix_ms)?;
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(created)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(created)
                .push(" AND (project_id > ")
                .push_bind(after.project_id.as_str())
                .push(" OR (project_id = ")
                .push_bind(after.project_id.as_str())
                .push(" AND artifact_id > ")
                .push_bind(after.artifact_id.as_str())
                .push("))))");
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, project_id ASC, artifact_id ASC LIMIT ")
            .push_bind(i64::from(request.limit) + 1);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        artifact_page(rows, request.limit)
    }

    async fn insert_artifact(
        &self,
        record: ArtifactRecord,
    ) -> CentralResult<CatalogInsertOutcome<ArtifactRecord>> {
        validate_new_resource(record.resource_version, &record.lifecycle, "Artifact")?;
        if let Some(existing) =
            get_artifact_by_id(self, &record.tenant_id, &record.artifact_id).await?
        {
            require_active(&existing.lifecycle, "Artifact")?;
            return if artifact_create_matches(&existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing))
            } else {
                Err(id_reused(
                    "Artifact ID is already bound to another create request",
                ))
            };
        }
        if self.get_tenant(&record.tenant_id).await?.is_none() {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "Artifact Tenant does not exist",
            )
            .with_retryable(false));
        }
        if let ArtifactInitialization::Derived {
            source_project_id,
            source_artifact_id,
            ..
        } = &record.initialization
        {
            if self
                .get_artifact(&record.tenant_id, source_project_id, source_artifact_id)
                .await?
                .is_none()
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Artifact initialization source does not exist",
                )
                .with_retryable(false));
            }
        }
        let (mode, source_project_id, source_artifact_id, source_commit_digest) =
            match &record.initialization {
                ArtifactInitialization::Empty => ("empty", None, None, None),
                ArtifactInitialization::Derived {
                    source_project_id,
                    source_artifact_id,
                    source_commit_id,
                } => (
                    "derived",
                    Some(source_project_id.as_str()),
                    Some(source_artifact_id.as_str()),
                    Some(source_commit_id.as_bytes().as_slice()),
                ),
            };
        let result = sqlx::query(
            "INSERT INTO artifact_catalog_records \
             (tenant_id, project_id, artifact_id, display_name, description, initialization_mode, \
              source_project_id, source_artifact_id, source_commit_digest, head_commit_digest, \
              resource_version, lifecycle_state, lifecycle_generation, active_deletion_id, \
              delete_requested_at_unix_ms, purge_after_unix_ms, deleted_at_unix_ms, \
              created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.project_id.as_str())
        .bind(record.artifact_id.as_str())
        .bind(&record.display_name)
        .bind(&record.description)
        .bind(mode)
        .bind(source_project_id)
        .bind(source_artifact_id)
        .bind(source_commit_digest)
        .bind(
            record
                .head_commit_id
                .map(|digest| digest.as_bytes().to_vec()),
        )
        .bind(record.resource_version.to_string())
        .bind(resource_lifecycle_state_name(record.lifecycle.state))
        .bind(record.lifecycle.generation.to_string())
        .bind(
            record
                .lifecycle
                .active_deletion_id
                .as_ref()
                .map(DeletionId::as_str),
        )
        .bind(optional_as_i64(
            record.lifecycle.delete_requested_at_unix_ms,
        )?)
        .bind(optional_as_i64(record.lifecycle.purge_after_unix_ms)?)
        .bind(optional_as_i64(record.lifecycle.deleted_at_unix_ms)?)
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(CatalogInsertOutcome::Inserted(record)),
            Err(error) if is_unique(&error) => {
                let existing = get_artifact_by_id(self, &record.tenant_id, &record.artifact_id)
                    .await?
                    .ok_or_else(|| {
                        storage_error("Artifact uniqueness conflict could not be resolved")
                    })?;
                if artifact_create_matches(&existing, &record) {
                    Ok(CatalogInsertOutcome::Existing(existing))
                } else {
                    Err(id_reused(
                        "Artifact ID is already bound to another create request",
                    ))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn get_storage_volume(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Option<StorageVolumeRecord>> {
        let sql = format!(
            "SELECT {VOLUME_COLUMNS} FROM storage_volume_catalog_records \
             WHERE tenant_id = ? AND storage_volume_id = ? AND lifecycle_state = 'active'"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(storage_volume_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_volume)
            .transpose()
    }

    async fn get_storage_volume_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Option<StorageVolumeRecord>> {
        let sql = format!(
            "SELECT {VOLUME_COLUMNS} FROM storage_volume_catalog_records \
             WHERE tenant_id = ? AND storage_volume_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(storage_volume_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_volume)
            .transpose()
    }

    async fn list_storage_volumes(
        &self,
        request: &StorageVolumeListRequest,
    ) -> CentralResult<StorageVolumeListPage> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {VOLUME_COLUMNS} FROM storage_volume_catalog_records WHERE tenant_id = "
        ));
        query.push_bind(request.tenant_id.as_str());
        query.push(" AND lifecycle_state = 'active'");
        if let Some(region) = &request.region {
            query.push(" AND region = ").push_bind(region);
        }
        if let Some(backend) = request.backend_type {
            query
                .push(" AND backend_type = ")
                .push_bind(backend_name(backend));
        }
        if let Some(search) = &request.query {
            let pattern = format!("%{}%", escape_like(&search.to_lowercase()));
            query
                .push(" AND (LOWER(storage_volume_id) LIKE ")
                .push_bind(pattern.clone())
                .push(" ESCAPE '\\' OR LOWER(display_name) LIKE ")
                .push_bind(pattern)
                .push(" ESCAPE '\\')");
        }
        if let Some(after) = &request.after {
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" AND storage_volume_id > ")
                .push_bind(after.storage_volume_id.as_str())
                .push("))");
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, storage_volume_id ASC LIMIT ")
            .push_bind(i64::from(request.limit) + 1);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        volume_page(rows, request.limit)
    }

    async fn insert_storage_volume(
        &self,
        record: StorageVolumeRecord,
    ) -> CentralResult<CatalogInsertOutcome<StorageVolumeRecord>> {
        validate_new_resource(record.resource_version, &record.lifecycle, "StorageVolume")?;
        validate_volume_shape(&record)?;
        if let Some(existing) = self
            .get_storage_volume(&record.tenant_id, &record.storage_volume_id)
            .await?
        {
            return if volume_create_matches(&existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing))
            } else {
                Err(id_reused(
                    "StorageVolume ID is already bound to another create request",
                ))
            };
        }
        let (pvc_namespace, pvc_claim_name) =
            record
                .pvc_reference
                .as_ref()
                .map_or((None, None), |reference| {
                    (
                        Some(reference.namespace.as_str()),
                        Some(reference.claim_name.as_str()),
                    )
                });
        let (nfs_server, nfs_export_path) =
            record
                .nfs_reference
                .as_ref()
                .map_or((None, None), |reference| {
                    (
                        Some(reference.server.as_str()),
                        Some(reference.export_path.as_str()),
                    )
                });
        let result = sqlx::query(
            "INSERT INTO storage_volume_catalog_records \
            (tenant_id, storage_volume_id, display_name, edge_cluster_id, region, backend_type, \
              access_mode, allowed_delivery_modes, hardlink_policy, max_whole_file_bytes, \
              copy_reserve_bytes, pvc_namespace, pvc_claim_name, nfs_server, nfs_export_path, state, \
              enrollment_id, resource_version, lifecycle_state, lifecycle_generation, \
              active_deletion_id, delete_requested_at_unix_ms, purge_after_unix_ms, \
              deleted_at_unix_ms, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.storage_volume_id.as_str())
        .bind(&record.display_name)
        .bind(record.edge_cluster_id.as_str())
        .bind(&record.region)
        .bind(backend_name(record.backend_type))
        .bind(access_mode_name(record.access_mode))
        .bind(serde_json::to_string(&record.allowed_delivery_modes).map_err(|error| {
            CentralError::new(CentralErrorCode::StorageFailure, error.to_string())
        })?)
        .bind(hardlink_policy_name(record.hardlink_policy))
        .bind(record.max_whole_file_bytes.to_string())
        .bind(record.copy_reserve_bytes.to_string())
        .bind(pvc_namespace)
        .bind(pvc_claim_name)
        .bind(nfs_server)
        .bind(nfs_export_path)
        .bind(volume_state_name(record.state))
        .bind(record.resource_version.to_string())
        .bind(resource_lifecycle_state_name(record.lifecycle.state))
        .bind(record.lifecycle.generation.to_string())
        .bind(
            record
                .lifecycle
                .active_deletion_id
                .as_ref()
                .map(DeletionId::as_str),
        )
        .bind(optional_as_i64(
            record.lifecycle.delete_requested_at_unix_ms,
        )?)
        .bind(optional_as_i64(record.lifecycle.purge_after_unix_ms)?)
        .bind(optional_as_i64(record.lifecycle.deleted_at_unix_ms)?)
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(CatalogInsertOutcome::Inserted(record)),
            Err(error) if is_unique(&error) => {
                if let Some(existing) = self
                    .get_storage_volume(&record.tenant_id, &record.storage_volume_id)
                    .await?
                {
                    if volume_create_matches(&existing, &record) {
                        return Ok(CatalogInsertOutcome::Existing(existing));
                    }
                }
                Err(CentralError::new(
                    CentralErrorCode::VolumeOwnerConflict,
                    "PVC identity or StorageVolume ID is already registered",
                )
                .with_retryable(false))
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn get_playground(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &PlaygroundId,
    ) -> CentralResult<Option<PlaygroundRecord>> {
        let sql = format!(
            "SELECT {PLAYGROUND_COLUMNS} FROM playground_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ? \
               AND lifecycle_state = 'active'"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .bind(playground_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_playground)
            .transpose()
    }

    async fn get_playground_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &PlaygroundId,
    ) -> CentralResult<Option<PlaygroundRecord>> {
        let sql = format!(
            "SELECT {PLAYGROUND_COLUMNS} FROM playground_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .bind(playground_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_playground)
            .transpose()
    }

    async fn list_playgrounds(
        &self,
        request: &PlaygroundListRequest,
    ) -> CentralResult<PlaygroundListPage> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {PLAYGROUND_COLUMNS} FROM playground_catalog_records WHERE tenant_id = "
        ));
        query.push_bind(request.tenant_id.as_str());
        query.push(" AND lifecycle_state = 'active'");
        if let Some(project_id) = &request.project_id {
            query
                .push(" AND project_id = ")
                .push_bind(project_id.as_str());
        }
        if let Some(artifact_id) = &request.artifact_id {
            query
                .push(" AND artifact_id = ")
                .push_bind(artifact_id.as_str());
        }
        if let Some(region) = &request.region {
            query.push(" AND region = ").push_bind(region);
        }
        if let Some(state) = request.state {
            query
                .push(" AND state = ")
                .push_bind(playground_state_name(state));
        }
        if let Some(search) = &request.query {
            let pattern = format!("%{}%", escape_like(&search.to_lowercase()));
            query
                .push(" AND (LOWER(playground_id) LIKE ")
                .push_bind(pattern.clone())
                .push(" ESCAPE '\\' OR LOWER(display_name) LIKE ")
                .push_bind(pattern)
                .push(" ESCAPE '\\')");
        }
        if let Some(after) = &request.after {
            let created = as_i64(after.created_at_unix_ms)?;
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(created)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(created)
                .push(" AND (project_id > ")
                .push_bind(after.project_id.as_str())
                .push(" OR (project_id = ")
                .push_bind(after.project_id.as_str())
                .push(" AND (artifact_id > ")
                .push_bind(after.artifact_id.as_str())
                .push(" OR (artifact_id = ")
                .push_bind(after.artifact_id.as_str())
                .push(" AND playground_id > ")
                .push_bind(after.playground_id.as_str())
                .push("))))))");
        }
        query
            .push(
                " ORDER BY created_at_unix_ms DESC, project_id ASC, artifact_id ASC, playground_id ASC LIMIT ",
            )
            .push_bind(i64::from(request.limit) + 1);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        playground_page(rows, request.limit)
    }

    async fn insert_playground_fenced(
        &self,
        request: PlaygroundInsertRequest,
    ) -> CentralResult<CatalogInsertOutcome<PlaygroundRecord>> {
        let PlaygroundInsertRequest {
            record,
            artifact_head,
        } = request;
        validate_new_resource(record.resource_version, &record.lifecycle, "Playground")?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let playground_sql = format!(
            "SELECT {PLAYGROUND_COLUMNS} FROM playground_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ?"
        );
        let existing = sqlx::query(&playground_sql)
            .bind(record.tenant_id.as_str())
            .bind(record.project_id.as_str())
            .bind(record.artifact_id.as_str())
            .bind(record.playground_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_playground)
            .transpose()?;
        if let Some(existing) = existing {
            require_active(&existing.lifecycle, "Playground")?;
            if !playground_create_matches_insert(&existing, &record, &artifact_head) {
                return Err(id_reused(
                    "Playground ID is already bound to another create request",
                ));
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(CatalogInsertOutcome::Existing(existing));
        }
        let artifact_sql = format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?"
        );
        let artifact = sqlx::query(&artifact_sql)
            .bind(record.tenant_id.as_str())
            .bind(record.project_id.as_str())
            .bind(record.artifact_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_artifact)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Playground Artifact does not exist",
                )
            })?;
        require_active(&artifact.lifecycle, "Playground Artifact")?;
        if record.base_commit_id != record.head_commit_id {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactHeadMismatch,
                "Playground base Commit must match its initial head Commit",
            ));
        }
        if let ArtifactHeadExpectation::Exact(expected) = artifact_head {
            if record.base_commit_id != expected {
                return Err(CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "fenced Playground base Commit does not match the observed Artifact Head",
                )
                .with_retryable(false));
            }
            if artifact.head_commit_id != expected {
                return Err(artifact_head_changed());
            }
        }
        let volume_sql = format!(
            "SELECT {VOLUME_COLUMNS} FROM storage_volume_catalog_records \
             WHERE tenant_id = ? AND storage_volume_id = ?"
        );
        let volume = sqlx::query(&volume_sql)
            .bind(record.tenant_id.as_str())
            .bind(record.storage_volume_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_volume)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::StorageVolumeNotFound,
                    "Playground StorageVolume does not exist",
                )
            })?;
        require_active(&volume.lifecycle, "Playground StorageVolume")?;
        if volume.state != StorageVolumeState::Ready {
            return Err(catalog_parent_error(
                CentralErrorCode::StorageVolumeNotReady,
                "Playground StorageVolume is not ready",
            ));
        }
        if record.region != volume.region {
            return Err(catalog_parent_error(
                CentralErrorCode::StorageVolumeRegionMismatch,
                "Playground region must match the StorageVolume region",
            ));
        }
        let result = sqlx::query(
            "INSERT INTO playground_catalog_records \
             (tenant_id, project_id, artifact_id, playground_id, storage_volume_id, region, \
              display_name, base_commit_digest, head_commit_digest, \
              state, relative_root, resource_version, lifecycle_state, lifecycle_generation, \
              active_deletion_id, delete_requested_at_unix_ms, purge_after_unix_ms, \
              deleted_at_unix_ms, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.project_id.as_str())
        .bind(record.artifact_id.as_str())
        .bind(record.playground_id.as_str())
        .bind(record.storage_volume_id.as_str())
        .bind(&record.region)
        .bind(&record.display_name)
        .bind(
            record
                .base_commit_id
                .map(|digest| digest.as_bytes().to_vec()),
        )
        .bind(
            record
                .head_commit_id
                .map(|digest| digest.as_bytes().to_vec()),
        )
        .bind(playground_state_name(record.state))
        .bind(&record.relative_root)
        .bind(record.resource_version.to_string())
        .bind(resource_lifecycle_state_name(record.lifecycle.state))
        .bind(record.lifecycle.generation.to_string())
        .bind(
            record
                .lifecycle
                .active_deletion_id
                .as_ref()
                .map(DeletionId::as_str),
        )
        .bind(optional_as_i64(
            record.lifecycle.delete_requested_at_unix_ms,
        )?)
        .bind(optional_as_i64(record.lifecycle.purge_after_unix_ms)?)
        .bind(optional_as_i64(record.lifecycle.deleted_at_unix_ms)?)
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&mut *transaction)
        .await;
        match result {
            Ok(_) => {
                transaction.commit().await.map_err(storage_error)?;
                Ok(CatalogInsertOutcome::Inserted(record))
            }
            Err(error) if is_unique(&error) => {
                let existing = sqlx::query(&playground_sql)
                    .bind(record.tenant_id.as_str())
                    .bind(record.project_id.as_str())
                    .bind(record.artifact_id.as_str())
                    .bind(record.playground_id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(storage_error)?
                    .map(decode_playground)
                    .transpose()?
                    .ok_or_else(|| {
                        storage_error("Playground uniqueness conflict could not be resolved")
                    })?;
                if playground_create_matches_insert(&existing, &record, &artifact_head) {
                    transaction.commit().await.map_err(storage_error)?;
                    Ok(CatalogInsertOutcome::Existing(existing))
                } else {
                    Err(id_reused(
                        "Playground ID is already bound to another create request",
                    ))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn transition_playground_state(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &PlaygroundId,
        expected: PlaygroundState,
        next: PlaygroundState,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<PlaygroundRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let update = sqlx::query(
            "UPDATE playground_catalog_records SET state = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ? \
               AND state = ?",
        )
        .bind(playground_state_name(next))
        .bind(as_i64(updated_at_unix_ms)?)
        .bind(tenant_id.as_str())
        .bind(project_id.as_str())
        .bind(artifact_id.as_str())
        .bind(playground_id.as_str())
        .bind(playground_state_name(expected))
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;

        let select = format!(
            "SELECT {PLAYGROUND_COLUMNS} FROM playground_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ?"
        );
        let record = sqlx::query(&select)
            .bind(tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .bind(playground_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_playground)
            .transpose()?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ArtifactNotFound,
                    "Playground does not exist",
                )
                .with_retryable(false)
            })?;
        require_active(&record.lifecycle, "Playground")?;
        if update.rows_affected() == 0 && record.state != next {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Playground is in {:?}, expected {:?} for state transition",
                    record.state, expected
                ),
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn advance_playground_commit(
        &self,
        request: AdvancePlaygroundCommitRequest,
    ) -> CentralResult<AdvancePlaygroundCommitOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let artifact_sql = format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?"
        );
        let mut artifact = sqlx::query(&artifact_sql)
            .bind(request.tenant_id.as_str())
            .bind(request.project_id.as_str())
            .bind(request.artifact_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_artifact)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Commit Artifact does not exist",
                )
            })?;
        let playground_sql = format!(
            "SELECT {PLAYGROUND_COLUMNS} FROM playground_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ?"
        );
        let mut playground = sqlx::query(&playground_sql)
            .bind(request.tenant_id.as_str())
            .bind(request.project_id.as_str())
            .bind(request.artifact_id.as_str())
            .bind(request.playground_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_playground)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Commit Playground does not exist",
                )
            })?;
        require_active(&artifact.lifecycle, "Commit Artifact")?;
        require_active(&playground.lifecycle, "Commit Playground")?;
        // The Playground Head is the branch-local CAS fence. Artifact Head is only a mutable
        // convenience pointer and may have advanced through another Playground branch.
        if playground.head_commit_id == Some(request.commit_id) {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(AdvancePlaygroundCommitOutcome {
                artifact,
                playground,
                replayed: true,
            });
        }
        if playground.head_commit_id != request.expected_head_commit_id {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactHeadMismatch,
                "Playground Head changed after Pre-commit",
            ));
        }
        if playground.state != PlaygroundState::Ready {
            return Err(catalog_parent_error(
                CentralErrorCode::InvalidState,
                "only a Ready Playground can publish a Commit",
            ));
        }
        let next_resource_version = artifact.resource_version.checked_add(1).ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Artifact ResourceVersion is exhausted",
            )
        })?;
        let expected_digest = request
            .expected_head_commit_id
            .map(|digest| digest.as_bytes().to_vec());
        let commit_digest = request.commit_id.as_bytes().to_vec();
        let artifact_update = sqlx::query(
            "UPDATE artifact_catalog_records \
             SET head_commit_digest = ?, resource_version = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?",
        )
        .bind(&commit_digest)
        .bind(next_resource_version.to_string())
        .bind(as_i64(request.updated_at_unix_ms)?)
        .bind(request.tenant_id.as_str())
        .bind(request.project_id.as_str())
        .bind(request.artifact_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if artifact_update.rows_affected() != 1 {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactHeadMismatch,
                "Artifact Head changed during Commit publication",
            ));
        }
        let playground_update = sqlx::query(
            "UPDATE playground_catalog_records \
             SET head_commit_digest = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ? \
               AND ((head_commit_digest IS NULL AND ? IS NULL) OR head_commit_digest = ?)",
        )
        .bind(&commit_digest)
        .bind(as_i64(request.updated_at_unix_ms)?)
        .bind(request.tenant_id.as_str())
        .bind(request.project_id.as_str())
        .bind(request.artifact_id.as_str())
        .bind(request.playground_id.as_str())
        .bind(expected_digest.clone())
        .bind(expected_digest)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if playground_update.rows_affected() != 1 {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactHeadMismatch,
                "Playground Head changed during Commit publication",
            ));
        }
        artifact.head_commit_id = Some(request.commit_id);
        artifact.resource_version = next_resource_version;
        artifact.updated_at_unix_ms = request.updated_at_unix_ms;
        playground.head_commit_id = Some(request.commit_id);
        playground.updated_at_unix_ms = request.updated_at_unix_ms;
        transaction.commit().await.map_err(storage_error)?;
        Ok(AdvancePlaygroundCommitOutcome {
            artifact,
            playground,
            replayed: false,
        })
    }

    async fn get_snapshot(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
    ) -> CentralResult<Option<SnapshotRecord>> {
        let sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records \
             WHERE tenant_id = ? AND snapshot_id = ? AND lifecycle_state = 'active'"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(snapshot_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot)
            .transpose()
    }

    async fn get_snapshot_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
    ) -> CentralResult<Option<SnapshotRecord>> {
        let sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records \
             WHERE tenant_id = ? AND snapshot_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(snapshot_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot)
            .transpose()
    }

    async fn list_snapshots(
        &self,
        request: &SnapshotListRequest,
    ) -> CentralResult<SnapshotListPage> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records WHERE tenant_id = "
        ));
        query.push_bind(request.tenant_id.as_str());
        query.push(" AND lifecycle_state = 'active'");
        if let Some(project_id) = &request.project_id {
            query
                .push(" AND project_id = ")
                .push_bind(project_id.as_str());
        }
        if let Some(artifact_id) = &request.artifact_id {
            query
                .push(" AND artifact_id = ")
                .push_bind(artifact_id.as_str());
        }
        if let Some(commit_id) = &request.commit_id {
            query
                .push(" AND commit_digest = ")
                .push_bind(commit_id.as_bytes().as_slice());
        }
        if let Some(state) = request.state {
            query
                .push(" AND state = ")
                .push_bind(snapshot_state_name(state));
        }
        if let Some(after) = &request.after {
            let created = as_i64(after.created_at_unix_ms)?;
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(created)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(created)
                .push(" AND snapshot_id > ")
                .push_bind(after.snapshot_id.as_str())
                .push("))");
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, snapshot_id ASC LIMIT ")
            .push_bind(i64::from(request.limit) + 1);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        snapshot_page(rows, request.limit)
    }

    async fn insert_snapshot_with_delivery(
        &self,
        request: SnapshotWithDeliveryInsertRequest,
    ) -> CentralResult<SnapshotWithDeliveryInsertResult> {
        let SnapshotWithDeliveryInsertRequest { snapshot, delivery } = request;
        if snapshot.record.delivery_id != delivery.record.delivery_id
            || snapshot.record.tenant_id != delivery.record.tenant_id
            || snapshot.record.snapshot_id != delivery.record.snapshot_id
            || snapshot.record.commit_id != delivery.record.commit_id
            || snapshot.record.storage_volume_id != delivery.record.storage_volume_id
            || snapshot.record.delivery_mode != delivery.record.mode
        {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot and Delivery immutable identities do not match",
            )
            .with_retryable(false));
        }
        validate_new_resource(
            snapshot.record.resource_version,
            &snapshot.record.lifecycle,
            "Snapshot",
        )?;
        crate::catalog::validate_snapshot_delivery_retention_roots(&delivery)?;
        if delivery.record.create_request_id != delivery.request_id
            || delivery.record.delivery_generation.get() == 0
            || delivery.record.resource_version == 0
        {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot delivery create identity or generations are invalid",
            )
            .with_retryable(false));
        }
        if snapshot.record.snapshot_request_id != delivery.request_id {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot and Delivery must share the same create request identity",
            )
            .with_retryable(false));
        }

        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let snapshot_request_sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records \
             WHERE tenant_id = ? AND snapshot_request_id = ?"
        );
        if let Some(existing_snapshot) = sqlx::query(&snapshot_request_sql)
            .bind(snapshot.record.tenant_id.as_str())
            .bind(snapshot.record.snapshot_request_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot)
            .transpose()?
        {
            require_active(&existing_snapshot.lifecycle, "Snapshot")?;
            if !snapshot_request_matches(&existing_snapshot, &snapshot.record) {
                return Err(id_reused(
                    "Snapshot request ID is already bound to another create request",
                ));
            }
            let existing_delivery_sql = format!(
                "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
                 WHERE tenant_id = ? AND delivery_id = ?"
            );
            let existing_delivery = sqlx::query(&existing_delivery_sql)
                .bind(existing_snapshot.tenant_id.as_str())
                .bind(existing_snapshot.delivery_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage_error)?
                .map(decode_snapshot_delivery)
                .transpose()?
                .ok_or_else(|| {
                    corruption("Snapshot exists without its required SnapshotDelivery")
                })?;
            if !existing_delivery.same_create_request(&delivery.record) {
                return Err(id_reused(
                    "Snapshot delivery identity changed during request replay",
                ));
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(SnapshotWithDeliveryInsertResult {
                snapshot: existing_snapshot,
                delivery: existing_delivery,
                replayed: true,
            });
        }

        let artifact_sql = format!(
            "SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?"
        );
        let artifact = sqlx::query(&artifact_sql)
            .bind(snapshot.record.tenant_id.as_str())
            .bind(snapshot.record.project_id.as_str())
            .bind(snapshot.record.artifact_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_artifact)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Snapshot Artifact does not exist",
                )
            })?;
        require_active(&artifact.lifecycle, "Snapshot Artifact")?;
        if let ArtifactHeadExpectation::Exact(expected) = snapshot.artifact_head {
            if expected != Some(snapshot.record.commit_id) {
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
        let volume_sql = format!(
            "SELECT {VOLUME_COLUMNS} FROM storage_volume_catalog_records \
             WHERE tenant_id = ? AND storage_volume_id = ?"
        );
        let volume = sqlx::query(&volume_sql)
            .bind(delivery.record.tenant_id.as_str())
            .bind(delivery.record.storage_volume_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_volume)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::StorageVolumeNotFound,
                    "SnapshotDelivery StorageVolume does not exist",
                )
            })?;
        crate::catalog::validate_snapshot_delivery_parents(
            &delivery.record,
            &snapshot.record,
            &volume,
        )?;

        let snapshot_record = snapshot.record;
        let snapshot_insert = sqlx::query(
            "INSERT INTO snapshot_catalog_records \
             (tenant_id, project_id, artifact_id, snapshot_id, snapshot_request_id, commit_digest, \
              delivery_id, edge_cluster_id, storage_volume_id, delivery_mode, state, created_at_unix_ms, \
              resource_version, lifecycle_state, lifecycle_generation, active_deletion_id, \
              delete_requested_at_unix_ms, purge_after_unix_ms, deleted_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(snapshot_record.tenant_id.as_str())
        .bind(snapshot_record.project_id.as_str())
        .bind(snapshot_record.artifact_id.as_str())
        .bind(snapshot_record.snapshot_id.as_str())
        .bind(snapshot_record.snapshot_request_id.as_str())
        .bind(snapshot_record.commit_id.as_bytes().as_slice())
        .bind(snapshot_record.delivery_id.as_str())
        .bind(snapshot_record.edge_cluster_id.as_str())
        .bind(snapshot_record.storage_volume_id.as_str())
        .bind(snapshot_delivery_mode_name(snapshot_record.delivery_mode))
        .bind(snapshot_state_name(snapshot_record.state))
        .bind(as_i64(snapshot_record.created_at_unix_ms)?)
        .bind(snapshot_record.resource_version.to_string())
        .bind(resource_lifecycle_state_name(snapshot_record.lifecycle.state))
        .bind(snapshot_record.lifecycle.generation.to_string())
        .bind(
            snapshot_record
                .lifecycle
                .active_deletion_id
                .as_ref()
                .map(DeletionId::as_str),
        )
        .bind(optional_as_i64(
            snapshot_record.lifecycle.delete_requested_at_unix_ms,
        )?)
        .bind(optional_as_i64(snapshot_record.lifecycle.purge_after_unix_ms)?)
        .bind(optional_as_i64(snapshot_record.lifecycle.deleted_at_unix_ms)?)
        .bind(as_i64(snapshot_record.updated_at_unix_ms)?)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = snapshot_insert {
            if !is_unique(&error) {
                return Err(storage_error(error));
            }
            // A concurrent identical request may have won the unique-key race after the
            // initial lookup. Resolve the winner inside this transaction instead of turning a
            // safe retry into a false identity conflict.
            let existing_snapshot = sqlx::query(&snapshot_request_sql)
                .bind(snapshot_record.tenant_id.as_str())
                .bind(snapshot_record.snapshot_request_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage_error)?
                .map(decode_snapshot)
                .transpose()?
                .ok_or_else(|| id_reused("Snapshot identity changed during concurrent creation"))?;
            require_active(&existing_snapshot.lifecycle, "Snapshot")?;
            if !snapshot_request_matches(&existing_snapshot, &snapshot_record) {
                return Err(id_reused(
                    "Snapshot request ID is already bound to another create request",
                ));
            }
            let existing_delivery_sql = format!(
                "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
                 WHERE tenant_id = ? AND delivery_id = ?"
            );
            let existing_delivery = sqlx::query(&existing_delivery_sql)
                .bind(existing_snapshot.tenant_id.as_str())
                .bind(existing_snapshot.delivery_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage_error)?
                .map(decode_snapshot_delivery)
                .transpose()?
                .ok_or_else(|| {
                    corruption("Snapshot exists without its required SnapshotDelivery")
                })?;
            if !existing_delivery.same_create_request(&delivery.record) {
                return Err(id_reused(
                    "Snapshot delivery identity changed during request replay",
                ));
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(SnapshotWithDeliveryInsertResult {
                snapshot: existing_snapshot,
                delivery: existing_delivery,
                replayed: true,
            });
        }

        let delivery_record = delivery.record;
        sqlx::query(
            "INSERT INTO snapshot_delivery_records \
             (tenant_id, delivery_id, create_request_id, snapshot_id, commit_digest, \
              storage_volume_id, mode, target_relative_root, state, source_index_digest, \
              delivery_generation, file_count, size_bytes, object_set_digest, resource_version, \
              issue_code, issue_message, issue_retryable, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(delivery_record.tenant_id.as_str())
        .bind(delivery_record.delivery_id.as_str())
        .bind(delivery_record.create_request_id.as_str())
        .bind(delivery_record.snapshot_id.as_str())
        .bind(delivery_record.commit_id.as_bytes().as_slice())
        .bind(delivery_record.storage_volume_id.as_str())
        .bind(snapshot_delivery_mode_name(delivery_record.mode))
        .bind(delivery_record.target_relative_root.as_str())
        .bind(snapshot_delivery_state_name(delivery_record.state))
        .bind(delivery_record.source_index_digest.as_bytes().as_slice())
        .bind(delivery_record.delivery_generation.to_string())
        .bind(
            i64::try_from(delivery_record.file_count)
                .map_err(|_| storage_error("file_count exceeds SQLite integer"))?,
        )
        .bind(
            i64::try_from(delivery_record.size_bytes)
                .map_err(|_| storage_error("size_bytes exceeds SQLite integer"))?,
        )
        .bind(delivery_record.object_set_digest.as_bytes().as_slice())
        .bind(delivery_record.resource_version.to_string())
        .bind(&delivery_record.issue_code)
        .bind(&delivery_record.issue_message)
        .bind(delivery_record.issue_retryable)
        .bind(as_i64(delivery_record.created_at_unix_ms)?)
        .bind(as_i64(delivery_record.updated_at_unix_ms)?)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if is_unique(&error) {
                id_reused("Snapshot Delivery identity changed during concurrent creation")
            } else {
                storage_error(error)
            }
        })?;
        for root in &delivery.retention_roots {
            sqlx::query(
                "INSERT INTO snapshot_delivery_object_retention_roots \
                 (tenant_id, delivery_id, object_id) VALUES (?, ?, ?)",
            )
            .bind(root.tenant_id.as_str())
            .bind(root.delivery_id.as_str())
            .bind(root.object_id.as_bytes().as_slice())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(SnapshotWithDeliveryInsertResult {
            snapshot: snapshot_record,
            delivery: delivery_record,
            replayed: false,
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
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let snapshot_sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records \
             WHERE tenant_id = ? AND snapshot_id = ?"
        );
        let current_snapshot = sqlx::query(&snapshot_sql)
            .bind(tenant_id.as_str())
            .bind(snapshot_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot)
            .transpose()?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ArtifactNotFound,
                    "Snapshot does not exist",
                )
                .with_retryable(false)
            })?;
        // State transitions are idempotent.  In particular, a replay of a successful
        // `Ready -> Ready` report must not consume another ResourceVersion.  Keep this
        // short-circuit aligned with the InMemory repository before running the CAS update.
        if current_snapshot.state == next {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(current_snapshot);
        }
        if next == SnapshotState::Ready {
            let delivery_sql = format!(
                "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
                 WHERE tenant_id = ? AND delivery_id = ?"
            );
            let delivery = sqlx::query(&delivery_sql)
                .bind(tenant_id.as_str())
                .bind(current_snapshot.delivery_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage_error)?
                .map(decode_snapshot_delivery)
                .transpose()?
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
        let update = sqlx::query(
            "UPDATE snapshot_catalog_records SET state = ?, resource_version = CAST(resource_version AS INTEGER) + 1, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND snapshot_id = ? AND state = ?",
        )
        .bind(snapshot_state_name(next))
        .bind(as_i64(updated_at_unix_ms)?)
        .bind(tenant_id.as_str())
        .bind(snapshot_id.as_str())
        .bind(snapshot_state_name(expected))
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let select = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records WHERE tenant_id = ? AND snapshot_id = ?"
        );
        let record = sqlx::query(&select)
            .bind(tenant_id.as_str())
            .bind(snapshot_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot)
            .transpose()?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ArtifactNotFound,
                    "Snapshot does not exist",
                )
                .with_retryable(false)
            })?;
        if update.rows_affected() == 0 && record.state != next {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Snapshot is in {:?}, expected {:?} for state transition",
                    record.state, expected
                ),
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn get_snapshot_delivery(
        &self,
        tenant_id: &TenantId,
        delivery_id: &SnapshotDeliveryId,
    ) -> CentralResult<Option<SnapshotDeliveryRecord>> {
        let sql = format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
             WHERE tenant_id = ? AND delivery_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(delivery_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot_delivery)
            .transpose()
    }

    async fn get_snapshot_delivery_by_create_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<SnapshotDeliveryRecord>> {
        let sql = format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
             WHERE tenant_id = ? AND create_request_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(request_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot_delivery)
            .transpose()
    }

    async fn list_snapshot_deliveries(
        &self,
        request: &SnapshotDeliveryListRequest,
    ) -> CentralResult<Vec<SnapshotDeliveryRecord>> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records WHERE tenant_id = "
        ));
        query.push_bind(request.tenant_id.as_str());
        if let Some(snapshot_id) = &request.snapshot_id {
            query
                .push(" AND snapshot_id = ")
                .push_bind(snapshot_id.as_str());
        }
        if let Some(mode) = request.mode {
            query
                .push(" AND mode = ")
                .push_bind(snapshot_delivery_mode_name(mode));
        }
        if let Some(state) = request.state {
            query
                .push(" AND state = ")
                .push_bind(snapshot_delivery_state_name(state));
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, delivery_id ASC LIMIT ")
            .push_bind(i64::from(request.limit));
        query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
            .into_iter()
            .map(decode_snapshot_delivery)
            .collect()
    }

    async fn insert_snapshot_delivery_idempotent(
        &self,
        request: SnapshotDeliveryInsertRequest,
    ) -> CentralResult<SnapshotDeliveryInsertOutcome> {
        crate::catalog::validate_snapshot_delivery_retention_roots(&request)?;
        if request.record.create_request_id != request.request_id {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot delivery create request identity is inconsistent",
            ));
        }
        if request.record.delivery_generation.get() == 0 || request.record.resource_version == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot delivery generations must be positive",
            ));
        }
        let request_sql = format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
             WHERE tenant_id = ? AND create_request_id = ?"
        );
        let delivery_sql = format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
             WHERE tenant_id = ? AND delivery_id = ?"
        );
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some(existing) = sqlx::query(&request_sql)
            .bind(request.record.tenant_id.as_str())
            .bind(request.request_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot_delivery)
            .transpose()?
        {
            if !existing.same_create_request(&request.record) {
                return Err(id_reused("Snapshot delivery request ID is already used"));
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(SnapshotDeliveryInsertOutcome::Existing(existing));
        }
        if let Some(existing) = sqlx::query(&delivery_sql)
            .bind(request.record.tenant_id.as_str())
            .bind(request.record.delivery_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot_delivery)
            .transpose()?
        {
            if !existing.same_create_request(&request.record) {
                return Err(id_reused("Snapshot delivery ID is already used"));
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(SnapshotDeliveryInsertOutcome::Existing(existing));
        }
        let duplicate_snapshot = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM snapshot_delivery_records WHERE tenant_id = ? AND snapshot_id = ? LIMIT 1",
        )
        .bind(request.record.tenant_id.as_str())
        .bind(request.record.snapshot_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if duplicate_snapshot.is_some() {
            return Err(id_reused("Snapshot already has a SnapshotDelivery"));
        }

        // Parent lifecycle and immutable placement identity are re-read in the same transaction
        // as the Delivery insert. A concurrent lifecycle fence therefore either wins first or
        // makes this transaction fail closed while upgrading to a writer.
        let volume_sql = format!(
            "SELECT {VOLUME_COLUMNS} FROM storage_volume_catalog_records \
             WHERE tenant_id = ? AND storage_volume_id = ?"
        );
        let volume = sqlx::query(&volume_sql)
            .bind(request.record.tenant_id.as_str())
            .bind(request.record.storage_volume_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_volume)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::StorageVolumeNotFound,
                    "SnapshotDelivery StorageVolume does not exist",
                )
            })?;
        let snapshot_sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records \
             WHERE tenant_id = ? AND snapshot_id = ?"
        );
        let snapshot = sqlx::query(&snapshot_sql)
            .bind(request.record.tenant_id.as_str())
            .bind(request.record.snapshot_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "SnapshotDelivery Snapshot does not exist",
                )
            })?;
        crate::catalog::validate_snapshot_delivery_parents(&request.record, &snapshot, &volume)?;
        let result = sqlx::query(
            "INSERT INTO snapshot_delivery_records \
             (tenant_id, delivery_id, create_request_id, snapshot_id, commit_digest, \
              storage_volume_id, mode, \
              target_relative_root, state, source_index_digest, delivery_generation, file_count, \
              size_bytes, object_set_digest, resource_version, issue_code, issue_message, \
              issue_retryable, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(request.record.tenant_id.as_str())
        .bind(request.record.delivery_id.as_str())
        .bind(request.record.create_request_id.as_str())
        .bind(request.record.snapshot_id.as_str())
        .bind(request.record.commit_id.as_bytes().as_slice())
        .bind(request.record.storage_volume_id.as_str())
        .bind(snapshot_delivery_mode_name(request.record.mode))
        .bind(request.record.target_relative_root.as_str())
        .bind(snapshot_delivery_state_name(request.record.state))
        .bind(request.record.source_index_digest.as_bytes().as_slice())
        .bind(request.record.delivery_generation.to_string())
        .bind(
            i64::try_from(request.record.file_count)
                .map_err(|_| storage_error("file_count exceeds SQLite integer"))?,
        )
        .bind(
            i64::try_from(request.record.size_bytes)
                .map_err(|_| storage_error("size_bytes exceeds SQLite integer"))?,
        )
        .bind(request.record.object_set_digest.as_bytes().as_slice())
        .bind(request.record.resource_version.to_string())
        .bind(&request.record.issue_code)
        .bind(&request.record.issue_message)
        .bind(request.record.issue_retryable)
        .bind(as_i64(request.record.created_at_unix_ms)?)
        .bind(as_i64(request.record.updated_at_unix_ms)?)
        .execute(&mut *transaction)
        .await;
        match result {
            Ok(_) => {
                for root in &request.retention_roots {
                    sqlx::query(
                        "INSERT INTO snapshot_delivery_object_retention_roots \
                         (tenant_id, delivery_id, object_id) VALUES (?, ?, ?)",
                    )
                    .bind(root.tenant_id.as_str())
                    .bind(root.delivery_id.as_str())
                    .bind(root.object_id.as_bytes().as_slice())
                    .execute(&mut *transaction)
                    .await
                    .map_err(storage_error)?;
                }
                transaction.commit().await.map_err(storage_error)?;
                Ok(SnapshotDeliveryInsertOutcome::Inserted(request.record))
            }
            Err(error) if is_unique(&error) => {
                transaction.rollback().await.map_err(storage_error)?;
                let existing = sqlx::query(&request_sql)
                    .bind(request.record.tenant_id.as_str())
                    .bind(request.request_id.as_str())
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(storage_error)?
                    .map(decode_snapshot_delivery)
                    .transpose()?
                    .or(sqlx::query(&delivery_sql)
                        .bind(request.record.tenant_id.as_str())
                        .bind(request.record.delivery_id.as_str())
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(storage_error)?
                        .map(decode_snapshot_delivery)
                        .transpose()?)
                    .ok_or_else(|| {
                        storage_error("Snapshot delivery uniqueness conflict could not be resolved")
                    })?;
                if existing.same_create_request(&request.record) {
                    Ok(SnapshotDeliveryInsertOutcome::Existing(existing))
                } else {
                    Err(id_reused(
                        "Snapshot delivery identity changed during concurrent creation",
                    ))
                }
            }
            Err(error) => {
                transaction.rollback().await.map_err(storage_error)?;
                Err(storage_error(error))
            }
        }
    }

    async fn replace_snapshot_delivery(
        &self,
        expected_resource_version: u64,
        mut record: SnapshotDeliveryRecord,
    ) -> CentralResult<SnapshotDeliveryRecord> {
        let next_resource_version = expected_resource_version.checked_add(1).ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Snapshot delivery ResourceVersion exhausted",
            )
        })?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let current_sql = format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
             WHERE tenant_id = ? AND delivery_id = ?"
        );
        let current = sqlx::query(&current_sql)
            .bind(record.tenant_id.as_str())
            .bind(record.delivery_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot_delivery)
            .transpose()?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ArtifactNotFound,
                    "Snapshot delivery does not exist",
                )
            })?;
        if !current.same_create_request(&record) {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "SnapshotDelivery replacement cannot change immutable identity",
            )
            .with_retryable(false));
        }
        let result = sqlx::query(
            "UPDATE snapshot_delivery_records SET state = ?, target_relative_root = ?, \
             delivery_generation = ?, file_count = ?, size_bytes = ?, object_set_digest = ?, \
             resource_version = ?, issue_code = ?, issue_message = ?, issue_retryable = ?, \
             updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND delivery_id = ? AND resource_version = ?",
        )
        .bind(snapshot_delivery_state_name(record.state))
        .bind(record.target_relative_root.as_str())
        .bind(record.delivery_generation.to_string())
        .bind(
            i64::try_from(record.file_count)
                .map_err(|_| storage_error("file_count exceeds SQLite integer"))?,
        )
        .bind(
            i64::try_from(record.size_bytes)
                .map_err(|_| storage_error("size_bytes exceeds SQLite integer"))?,
        )
        .bind(record.object_set_digest.as_bytes().as_slice())
        .bind(next_resource_version.to_string())
        .bind(&record.issue_code)
        .bind(&record.issue_message)
        .bind(record.issue_retryable)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .bind(record.tenant_id.as_str())
        .bind(record.delivery_id.as_str())
        .bind(expected_resource_version.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            transaction.rollback().await.map_err(storage_error)?;
            let existing = self
                .get_snapshot_delivery(&record.tenant_id, &record.delivery_id)
                .await?;
            if existing.as_ref() == Some(&record) {
                return Ok(record);
            }
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Snapshot delivery ResourceVersion changed",
            ));
        }
        record.resource_version = next_resource_version;
        if record.state == SnapshotDeliveryState::Deleted {
            sqlx::query(
                "DELETE FROM snapshot_delivery_object_retention_roots \
                 WHERE tenant_id = ? AND delivery_id = ?",
            )
            .bind(record.tenant_id.as_str())
            .bind(record.delivery_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn insert_snapshot_delivery_retention_roots(
        &self,
        roots: &[SnapshotDeliveryRetentionRoot],
    ) -> CentralResult<()> {
        if roots.is_empty() {
            return Ok(());
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        for root in roots {
            sqlx::query(
                "INSERT OR IGNORE INTO snapshot_delivery_object_retention_roots \
                 (tenant_id, delivery_id, object_id) VALUES (?, ?, ?)",
            )
            .bind(root.tenant_id.as_str())
            .bind(root.delivery_id.as_str())
            .bind(root.object_id.as_bytes().as_slice())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        transaction.commit().await.map_err(storage_error)
    }

    async fn list_snapshot_delivery_retention_roots(
        &self,
        tenant_id: &TenantId,
        delivery_id: &SnapshotDeliveryId,
    ) -> CentralResult<Vec<SnapshotDeliveryRetentionRoot>> {
        let rows = sqlx::query(
            "SELECT object_id FROM snapshot_delivery_object_retention_roots \
             WHERE tenant_id = ? AND delivery_id = ? ORDER BY object_id",
        )
        .bind(tenant_id.as_str())
        .bind(delivery_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.try_get("object_id").map_err(storage_error)?;
                let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
                    corruption(format!(
                        "stored retention root object ID has {} bytes",
                        bytes.len()
                    ))
                })?;
                let object_id = ObjectId::from_bytes(bytes);
                Ok(SnapshotDeliveryRetentionRoot {
                    tenant_id: tenant_id.clone(),
                    delivery_id: delivery_id.clone(),
                    object_id,
                })
            })
            .collect()
    }

    async fn release_snapshot_delivery_retention_roots(
        &self,
        tenant_id: &TenantId,
        delivery_id: &SnapshotDeliveryId,
    ) -> CentralResult<()> {
        sqlx::query(
            "DELETE FROM snapshot_delivery_object_retention_roots \
             WHERE tenant_id = ? AND delivery_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(delivery_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(())
    }

    async fn get_snapshot_delivery_mutation(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<SnapshotDeliveryMutationRecord>> {
        let row = sqlx::query(
            "SELECT tenant_id, request_id, delivery_id, operation, request_digest, payload \
             FROM snapshot_delivery_mutation_records WHERE tenant_id = ? AND request_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(request_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_snapshot_delivery_mutation).transpose()
    }

    async fn apply_snapshot_delivery_mutation_idempotent(
        &self,
        request: SnapshotDeliveryMutationRequest,
    ) -> CentralResult<CatalogInsertOutcome<SnapshotDeliveryMutationRecord>> {
        crate::catalog::validate_snapshot_delivery_mutation_request(&request)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let existing = sqlx::query(
            "SELECT tenant_id, request_id, delivery_id, operation, request_digest, payload \
             FROM snapshot_delivery_mutation_records WHERE tenant_id = ? AND request_id = ?",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.request_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(decode_snapshot_delivery_mutation)
        .transpose()?;
        if let Some(existing) = existing {
            if !same_snapshot_delivery_mutation_identity(&existing, &request) {
                return Err(id_reused(
                    "SnapshotDelivery mutation request ID is already used",
                ));
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(CatalogInsertOutcome::Existing(existing));
        }

        let delivery_sql = format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
             WHERE tenant_id = ? AND delivery_id = ?"
        );
        let current = sqlx::query(&delivery_sql)
            .bind(request.tenant_id.as_str())
            .bind(request.delivery_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(decode_snapshot_delivery)
            .transpose()?
            .ok_or_else(|| {
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
            let next_resource_version = request
                .expected_resource_version
                .checked_add(1)
                .ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::ConcurrentUpdate,
                        "Snapshot delivery ResourceVersion exhausted",
                    )
                })?;
            let mut next = request.desired_delivery;
            let update = sqlx::query(
                "UPDATE snapshot_delivery_records SET state = ?, target_relative_root = ?, \
                 delivery_generation = ?, file_count = ?, size_bytes = ?, object_set_digest = ?, \
                 resource_version = ?, issue_code = ?, issue_message = ?, issue_retryable = ?, \
                 updated_at_unix_ms = ? \
                 WHERE tenant_id = ? AND delivery_id = ? AND resource_version = ?",
            )
            .bind(snapshot_delivery_state_name(next.state))
            .bind(next.target_relative_root.as_str())
            .bind(next.delivery_generation.to_string())
            .bind(
                i64::try_from(next.file_count)
                    .map_err(|_| storage_error("file_count exceeds SQLite integer"))?,
            )
            .bind(
                i64::try_from(next.size_bytes)
                    .map_err(|_| storage_error("size_bytes exceeds SQLite integer"))?,
            )
            .bind(next.object_set_digest.as_bytes().as_slice())
            .bind(next_resource_version.to_string())
            .bind(&next.issue_code)
            .bind(&next.issue_message)
            .bind(next.issue_retryable)
            .bind(as_i64(next.updated_at_unix_ms)?)
            .bind(next.tenant_id.as_str())
            .bind(next.delivery_id.as_str())
            .bind(request.expected_resource_version.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if update.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "Snapshot delivery ResourceVersion changed",
                ));
            }
            next.resource_version = next_resource_version;
            if next.state == SnapshotDeliveryState::Deleted {
                sqlx::query(
                    "DELETE FROM snapshot_delivery_object_retention_roots \
                     WHERE tenant_id = ? AND delivery_id = ?",
                )
                .bind(next.tenant_id.as_str())
                .bind(next.delivery_id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(storage_error)?;
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
        let payload = encode_lifecycle_payload(&mutation)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO snapshot_delivery_mutation_records \
             (tenant_id, request_id, delivery_id, operation, request_digest, payload) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(mutation.tenant_id.as_str())
        .bind(mutation.request_id.as_str())
        .bind(mutation.delivery_id.as_str())
        .bind(snapshot_delivery_mutation_kind_name(mutation.kind))
        .bind(mutation.request_digest.as_bytes().as_slice())
        .bind(payload)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(CatalogInsertOutcome::Inserted(mutation));
        }
        transaction.rollback().await.map_err(storage_error)?;
        let existing = self
            .get_snapshot_delivery_mutation(&mutation.tenant_id, &mutation.request_id)
            .await?
            .ok_or_else(|| corruption("mutation insert was ignored without a receipt"))?;
        if existing.delivery_id == mutation.delivery_id
            && existing.kind == mutation.kind
            && existing.request_digest == mutation.request_digest
        {
            Ok(CatalogInsertOutcome::Existing(existing))
        } else {
            Err(id_reused(
                "SnapshotDelivery mutation request ID is already used",
            ))
        }
    }

    async fn get_s3_access_point(
        &self,
        tenant_id: &TenantId,
        access_point_id: &S3AccessPointId,
    ) -> CentralResult<Option<S3AccessPointRecord>> {
        let sql = format!(
            "SELECT {S3_ACCESS_POINT_COLUMNS} FROM s3_access_point_records \
             WHERE tenant_id = ? AND access_point_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(access_point_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_s3_access_point)
            .transpose()
    }

    async fn get_s3_access_point_by_snapshot(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
    ) -> CentralResult<Option<S3AccessPointRecord>> {
        let sql = format!(
            "SELECT {S3_ACCESS_POINT_COLUMNS} FROM s3_access_point_records \
             WHERE tenant_id = ? AND snapshot_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(snapshot_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_s3_access_point)
            .transpose()
    }

    async fn get_s3_access_point_by_bucket(
        &self,
        bucket_name: &str,
    ) -> CentralResult<Option<S3AccessPointRecord>> {
        let sql = format!(
            "SELECT {S3_ACCESS_POINT_COLUMNS} FROM s3_access_point_records \
             WHERE bucket_name = ?"
        );
        sqlx::query(&sql)
            .bind(bucket_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_s3_access_point)
            .transpose()
    }

    async fn list_s3_access_points(
        &self,
        request: &S3AccessPointListRequest,
    ) -> CentralResult<S3AccessPointListPage> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {S3_ACCESS_POINT_COLUMNS} FROM s3_access_point_records WHERE tenant_id = "
        ));
        query.push_bind(request.tenant_id.as_str());
        if let Some(after) = &request.after {
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" AND access_point_id > ")
                .push_bind(after.access_point_id.as_str())
                .push("))");
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, access_point_id ASC LIMIT ")
            .push_bind(i64::from(request.limit) + 1);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        let mut records = rows
            .into_iter()
            .map(decode_s3_access_point)
            .collect::<CentralResult<Vec<_>>>()?;
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty S3 access point page");
            S3AccessPointListCursor {
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
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // Replays must observe the current physical read gate before returning Existing.  A
        // previously valid Access Point cannot be used to bypass a failed/deleted Delivery.
        require_active_s3_snapshot(&mut transaction, &record).await?;
        if let Some(existing) =
            load_s3_access_point(&mut transaction, &record.tenant_id, &record.access_point_id)
                .await?
        {
            return if existing == record {
                transaction.commit().await.map_err(storage_error)?;
                Ok(S3AccessPointInsertOutcome::Existing(existing))
            } else {
                Err(id_reused("S3 Access Point ID is already used"))
            };
        }
        insert_s3_access_point_row(&mut transaction, &record).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(S3AccessPointInsertOutcome::Inserted(record))
    }

    async fn get_s3_mutation(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<S3MutationRecord>> {
        let sql = format!(
            "SELECT {S3_MUTATION_COLUMNS} FROM s3_mutation_records \
             WHERE tenant_id = ? AND request_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(request_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_s3_mutation)
            .transpose()
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
            return Err(id_reused("invalid S3 Access Point mutation binding"));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some(existing) =
            load_s3_mutation(&mut transaction, &mutation.tenant_id, &mutation.request_id).await?
        {
            ensure_same_s3_mutation(&existing, &mutation)?;
            let existing_access_point = load_s3_access_point(
                &mut transaction,
                &access_point.tenant_id,
                &access_point.access_point_id,
            )
            .await?
            .ok_or_else(|| corruption("S3 mutation references a missing Access Point"))?;
            let existing_credential =
                load_s3_credential(&mut transaction, &credential.credential_id)
                    .await?
                    .ok_or_else(|| corruption("S3 mutation references a missing credential"))?;
            require_active_s3_snapshot(&mut transaction, &existing_access_point).await?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(CatalogInsertOutcome::Existing(S3AccessPointCreateResult {
                access_point: existing_access_point,
                credential: existing_credential,
            }));
        }
        if let Some(existing_access_point) = load_s3_access_point(
            &mut transaction,
            &access_point.tenant_id,
            &access_point.access_point_id,
        )
        .await?
        {
            if !same_s3_access_point_create_identity(&existing_access_point, &access_point) {
                return Err(id_reused("S3 Access Point identity is already used"));
            }
            let result = if let Some(existing_credential) =
                load_s3_credential(&mut transaction, &credential.credential_id).await?
            {
                if !same_s3_credential_identity(&existing_credential, &credential) {
                    return Err(id_reused("S3 credential identity is already used"));
                }
                require_active_s3_snapshot(&mut transaction, &existing_access_point).await?;
                CatalogInsertOutcome::Existing(S3AccessPointCreateResult {
                    access_point: existing_access_point,
                    credential: existing_credential,
                })
            } else {
                require_active_s3_snapshot(&mut transaction, &existing_access_point).await?;
                if existing_access_point.state != S3AccessPointState::Active {
                    return Err(id_reused("S3 Access Point is disabled"));
                }
                insert_s3_credential_row(&mut transaction, &credential).await?;
                CatalogInsertOutcome::Inserted(S3AccessPointCreateResult {
                    access_point: existing_access_point,
                    credential,
                })
            };
            insert_s3_mutation_row(&mut transaction, &mutation).await?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(result);
        }
        require_active_s3_snapshot(&mut transaction, &access_point).await?;
        insert_s3_access_point_row(&mut transaction, &access_point).await?;
        insert_s3_credential_row(&mut transaction, &credential).await?;
        insert_s3_mutation_row(&mut transaction, &mutation).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(CatalogInsertOutcome::Inserted(S3AccessPointCreateResult {
            access_point,
            credential,
        }))
    }

    async fn update_s3_access_point_state_idempotent(
        &self,
        mutation: S3MutationRecord,
        access_point_id: &S3AccessPointId,
        state: S3AccessPointState,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<CatalogInsertOutcome<S3AccessPointRecord>> {
        let expected_operation = match state {
            S3AccessPointState::Active => S3MutationKind::AccessPointEnable,
            S3AccessPointState::Disabled => S3MutationKind::AccessPointDisable,
        };
        if mutation.operation != expected_operation {
            return Err(id_reused("invalid S3 Access Point state mutation binding"));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some(existing) =
            load_s3_mutation(&mut transaction, &mutation.tenant_id, &mutation.request_id).await?
        {
            ensure_same_s3_mutation(&existing, &mutation)?;
            let current =
                load_s3_access_point(&mut transaction, &mutation.tenant_id, access_point_id)
                    .await?
                    .ok_or_else(|| corruption("S3 mutation references a missing Access Point"))?;
            if state == S3AccessPointState::Active {
                require_active_s3_snapshot(&mut transaction, &current).await?;
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(CatalogInsertOutcome::Existing(current));
        }
        let mut access_point =
            load_s3_access_point(&mut transaction, &mutation.tenant_id, access_point_id)
                .await?
                .ok_or_else(|| {
                    catalog_parent_error(
                        CentralErrorCode::ArtifactNotFound,
                        "S3 Access Point does not exist",
                    )
                })?;
        if state == S3AccessPointState::Active {
            require_active_s3_snapshot(&mut transaction, &access_point).await?;
        }
        let next_policy_generation = (access_point.state != state)
            .then(|| {
                access_point
                    .policy_generation
                    .checked_add(1)
                    .ok_or_else(|| id_reused("S3 policy generation exhausted"))
            })
            .transpose()?;
        if state == S3AccessPointState::Disabled
            || (state == S3AccessPointState::Active
                && access_point.state != S3AccessPointState::Active)
        {
            sqlx::query(
                "UPDATE s3_credential_records SET state = 'revoked', encrypted_secret = X'00' \
                 WHERE access_point_id = ? AND state = 'active'",
            )
            .bind(access_point_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        if let Some(next_policy_generation) = next_policy_generation {
            access_point.state = state;
            access_point.policy_generation = next_policy_generation;
            access_point.updated_at_unix_ms = updated_at_unix_ms;
            sqlx::query(
                "UPDATE s3_access_point_records SET state = ?, policy_generation = ?, \
                 updated_at_unix_ms = ? WHERE tenant_id = ? AND access_point_id = ?",
            )
            .bind(s3_access_point_state_name(access_point.state))
            .bind(i64::try_from(access_point.policy_generation).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "S3 policy generation is too large",
                )
            })?)
            .bind(as_i64(access_point.updated_at_unix_ms)?)
            .bind(mutation.tenant_id.as_str())
            .bind(access_point_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        insert_s3_mutation_row(&mut transaction, &mutation).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(CatalogInsertOutcome::Inserted(access_point))
    }

    async fn update_s3_access_point_state(
        &self,
        tenant_id: &TenantId,
        access_point_id: &S3AccessPointId,
        state: S3AccessPointState,
        policy_generation: u64,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<S3AccessPointRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let mut access_point = load_s3_access_point(&mut transaction, tenant_id, access_point_id)
            .await?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "S3 Access Point does not exist",
                )
            })?;
        if state == S3AccessPointState::Active {
            require_active_s3_snapshot(&mut transaction, &access_point).await?;
        }
        sqlx::query(
            "UPDATE s3_access_point_records SET state = ?, policy_generation = ?, \
             updated_at_unix_ms = ? WHERE tenant_id = ? AND access_point_id = ?",
        )
        .bind(s3_access_point_state_name(state))
        .bind(i64::try_from(policy_generation).map_err(|_| {
            CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "S3 policy generation is too large",
            )
        })?)
        .bind(as_i64(updated_at_unix_ms)?)
        .bind(tenant_id.as_str())
        .bind(access_point_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if state == S3AccessPointState::Disabled {
            sqlx::query(
                "UPDATE s3_credential_records SET state = 'revoked', encrypted_secret = X'00' \
                 WHERE access_point_id = ? AND state = 'active'",
            )
            .bind(access_point_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        access_point.state = state;
        access_point.policy_generation = policy_generation;
        access_point.updated_at_unix_ms = updated_at_unix_ms;
        transaction.commit().await.map_err(storage_error)?;
        Ok(access_point)
    }

    async fn list_s3_credentials(
        &self,
        access_point_id: &S3AccessPointId,
    ) -> CentralResult<Vec<S3CredentialRecord>> {
        let sql = format!(
            "SELECT {S3_CREDENTIAL_COLUMNS} FROM s3_credential_records \
             WHERE access_point_id = ? ORDER BY created_at_unix_ms ASC, credential_id ASC"
        );
        sqlx::query(&sql)
            .bind(access_point_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
            .into_iter()
            .map(decode_s3_credential)
            .collect()
    }

    async fn get_s3_credential_by_access_key(
        &self,
        access_key_id: &str,
    ) -> CentralResult<Option<S3CredentialRecord>> {
        let sql = format!(
            "SELECT {S3_CREDENTIAL_COLUMNS} FROM s3_credential_records \
             WHERE access_key_id = ?"
        );
        sqlx::query(&sql)
            .bind(access_key_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_s3_credential)
            .transpose()
    }

    async fn insert_s3_credential(
        &self,
        record: S3CredentialRecord,
    ) -> CentralResult<S3CredentialInsertOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // Validate the parent before the idempotent Existing branch.  Otherwise an old
        // credential row could be replayed after its SnapshotDelivery became unavailable.
        let access_point = load_s3_access_point_by_id(&mut transaction, &record.access_point_id)
            .await?
            .ok_or_else(|| id_reused("S3 Access Point does not exist"))?;
        if access_point.state != S3AccessPointState::Active {
            return Err(id_reused("S3 Access Point is disabled"));
        }
        require_active_s3_snapshot(&mut transaction, &access_point).await?;
        if let Some(existing) = load_s3_credential(&mut transaction, &record.credential_id).await? {
            return if same_s3_credential_identity(&existing, &record) {
                transaction.commit().await.map_err(storage_error)?;
                Ok(S3CredentialInsertOutcome::Existing(existing))
            } else {
                Err(id_reused("S3 credential ID is already used"))
            };
        }
        let state = s3_credential_state_name(record.state);
        let result = sqlx::query(
            "INSERT INTO s3_credential_records \
             (credential_id, access_point_id, access_key_id, encrypted_secret, state, \
              expires_at_unix_ms, created_at_unix_ms, last_used_at_unix_ms) \
             SELECT ?, ?, ?, ?, ?, ?, ?, ? \
             WHERE ? <> 'active' OR (SELECT COUNT(*) FROM s3_credential_records \
                 WHERE access_point_id = ? AND state = 'active') < 2",
        )
        .bind(record.credential_id.as_str())
        .bind(record.access_point_id.as_str())
        .bind(&record.access_key_id)
        .bind(&record.encrypted_secret)
        .bind(state)
        .bind(as_i64(record.expires_at_unix_ms)?)
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(record.last_used_at_unix_ms.map(as_i64).transpose()?)
        .bind(state)
        .bind(record.access_point_id.as_str())
        .execute(&mut *transaction)
        .await;
        match result {
            Ok(result) if result.rows_affected() == 1 => {
                transaction.commit().await.map_err(storage_error)?;
                Ok(S3CredentialInsertOutcome::Inserted(record))
            }
            Ok(_) => Err(id_reused(
                "an S3 Access Point can have at most two active credentials",
            )),
            Err(error) if is_unique(&error) => {
                let existing = load_s3_credential(&mut transaction, &record.credential_id).await?;
                match existing {
                    Some(existing) if same_s3_credential_identity(&existing, &record) => {
                        transaction.commit().await.map_err(storage_error)?;
                        Ok(S3CredentialInsertOutcome::Existing(existing))
                    }
                    _ => Err(id_reused("S3 credential identity is already used")),
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn create_s3_credential_idempotent(
        &self,
        mutation: S3MutationRecord,
        credential: S3CredentialRecord,
    ) -> CentralResult<CatalogInsertOutcome<S3CredentialRecord>> {
        if mutation.operation != S3MutationKind::CredentialCreate {
            return Err(id_reused("invalid S3 credential create mutation binding"));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some(existing) =
            load_s3_mutation(&mut transaction, &mutation.tenant_id, &mutation.request_id).await?
        {
            ensure_same_s3_mutation(&existing, &mutation)?;
            let existing = load_s3_credential(&mut transaction, &credential.credential_id)
                .await?
                .ok_or_else(|| corruption("S3 mutation references a missing credential"))?;
            let access_point =
                load_s3_access_point_by_id(&mut transaction, &credential.access_point_id)
                    .await?
                    .ok_or_else(|| corruption("S3 mutation references a missing Access Point"))?;
            if access_point.state != S3AccessPointState::Active {
                return Err(id_reused("S3 Access Point is disabled"));
            }
            require_active_s3_snapshot(&mut transaction, &access_point).await?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(CatalogInsertOutcome::Existing(existing));
        }
        let access_point = load_s3_access_point(
            &mut transaction,
            &mutation.tenant_id,
            &credential.access_point_id,
        )
        .await?
        .ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "S3 Access Point does not exist",
            )
        })?;
        if access_point.state != S3AccessPointState::Active {
            return Err(id_reused("S3 Access Point is disabled"));
        }
        require_active_s3_snapshot(&mut transaction, &access_point).await?;
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM s3_credential_records \
             WHERE access_point_id = ? AND state = 'active'",
        )
        .bind(credential.access_point_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if active >= 2 {
            return Err(id_reused(
                "an S3 Access Point can have at most two active credentials",
            ));
        }
        insert_s3_credential_row(&mut transaction, &credential).await?;
        insert_s3_mutation_row(&mut transaction, &mutation).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(CatalogInsertOutcome::Inserted(credential))
    }

    async fn revoke_s3_credential_idempotent(
        &self,
        mutation: S3MutationRecord,
        access_point_id: &S3AccessPointId,
        credential_id: &S3CredentialId,
    ) -> CentralResult<CatalogInsertOutcome<S3CredentialRecord>> {
        if mutation.operation != S3MutationKind::CredentialRevoke {
            return Err(id_reused("invalid S3 credential revoke mutation binding"));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some(existing) =
            load_s3_mutation(&mut transaction, &mutation.tenant_id, &mutation.request_id).await?
        {
            ensure_same_s3_mutation(&existing, &mutation)?;
            let existing = load_s3_credential(&mut transaction, credential_id)
                .await?
                .ok_or_else(|| corruption("S3 mutation references a missing credential"))?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(CatalogInsertOutcome::Existing(existing));
        }
        load_s3_access_point(&mut transaction, &mutation.tenant_id, access_point_id)
            .await?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "S3 Access Point does not exist",
                )
            })?;
        let mut credential = load_s3_credential(&mut transaction, credential_id)
            .await?
            .ok_or_else(|| {
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
        sqlx::query(
            "UPDATE s3_credential_records \
             SET state = 'revoked', encrypted_secret = X'00' WHERE credential_id = ?",
        )
        .bind(credential_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        credential.state = S3CredentialState::Revoked;
        credential.encrypted_secret = vec![0];
        insert_s3_mutation_row(&mut transaction, &mutation).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(CatalogInsertOutcome::Inserted(credential))
    }

    async fn update_s3_credential_state(
        &self,
        credential_id: &S3CredentialId,
        state: S3CredentialState,
    ) -> CentralResult<S3CredentialRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let mut credential = load_s3_credential(&mut transaction, credential_id)
            .await?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "S3 credential does not exist",
                )
            })?;
        if state == S3CredentialState::Active {
            if credential.state != S3CredentialState::Active {
                return Err(id_reused(
                    "revoked or expired S3 credentials cannot be reactivated",
                ));
            }
            let access_point =
                load_s3_access_point_by_id(&mut transaction, &credential.access_point_id)
                    .await?
                    .ok_or_else(|| corruption("S3 credential references a missing Access Point"))?;
            if access_point.state != S3AccessPointState::Active {
                return Err(id_reused("S3 Access Point is disabled"));
            }
            require_active_s3_snapshot(&mut transaction, &access_point).await?;
        }
        let erased_secret = (state == S3CredentialState::Revoked).then_some(vec![0]);
        sqlx::query(
            "UPDATE s3_credential_records SET state = ?, \
             encrypted_secret = COALESCE(?, encrypted_secret) WHERE credential_id = ?",
        )
        .bind(s3_credential_state_name(state))
        .bind(erased_secret.as_deref())
        .bind(credential_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        credential.state = state;
        if erased_secret.is_some() {
            credential.encrypted_secret = vec![0];
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(credential)
    }

    async fn update_s3_credential_last_used(
        &self,
        credential_id: &S3CredentialId,
        last_used_at_unix_ms: UnixMillis,
    ) -> CentralResult<S3CredentialRecord> {
        sqlx::query(
            "UPDATE s3_credential_records \
             SET last_used_at_unix_ms = CASE \
                 WHEN last_used_at_unix_ms IS NULL OR last_used_at_unix_ms < ? THEN ? \
                 ELSE last_used_at_unix_ms END \
             WHERE credential_id = ? AND created_at_unix_ms <= ?",
        )
        .bind(as_i64(last_used_at_unix_ms)?)
        .bind(as_i64(last_used_at_unix_ms)?)
        .bind(credential_id.as_str())
        .bind(as_i64(last_used_at_unix_ms)?)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        let sql = format!(
            "SELECT {S3_CREDENTIAL_COLUMNS} FROM s3_credential_records WHERE credential_id = ?"
        );
        sqlx::query(&sql)
            .bind(credential_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(decode_s3_credential)
            .transpose()?
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "S3 credential does not exist or usage time predates creation",
                )
            })
    }

    async fn expire_s3_credentials(
        &self,
        access_point_id: &S3AccessPointId,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<u64> {
        let result = sqlx::query(
            "UPDATE s3_credential_records SET state = 'expired' \
             WHERE access_point_id = ? AND state = 'active' AND expires_at_unix_ms <= ?",
        )
        .bind(access_point_id.as_str())
        .bind(as_i64(now_unix_ms)?)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(result.rows_affected())
    }

    async fn query_deletion_impact(
        &self,
        request: DeletionImpactQuery,
    ) -> CentralResult<DeletionImpactRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (artifacts, volumes, playgrounds, snapshots, access_points, credentials) =
            load_lifecycle_catalog(&mut transaction, &request.tenant_id).await?;
        let impact = crate::catalog_memory::build_deletion_impact(
            &request,
            &artifacts,
            &volumes,
            &playgrounds,
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
        sqlx::query(
            "INSERT INTO deletion_impact_records \
             (tenant_id, impact_digest, expires_at_unix_ms, payload) VALUES (?, ?, ?, ?) \
             ON CONFLICT (tenant_id, impact_digest) DO UPDATE SET \
                 expires_at_unix_ms = excluded.expires_at_unix_ms, payload = excluded.payload",
        )
        .bind(request.tenant_id.as_str())
        .bind(record.impact_digest.as_bytes().as_slice())
        .bind(as_i64(record.impact.expires_at_unix_ms)?)
        .bind(encode_lifecycle_payload(&record.impact)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn get_deletion_operation(
        &self,
        tenant_id: &TenantId,
        deletion_id: &DeletionId,
    ) -> CentralResult<Option<DeletionOperation>> {
        load_deletion_operation_pool(&self.pool, tenant_id, deletion_id).await
    }

    async fn list_deletion_operations(
        &self,
        request: &DeletionListRequest,
    ) -> CentralResult<DeletionListPage> {
        validate_limit(request.limit)?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT payload FROM deletion_operation_records WHERE tenant_id = ",
        );
        query.push_bind(request.tenant_id.as_str());
        if let Some(states) = &request.states {
            if states.is_empty() {
                return Ok(DeletionListPage {
                    records: Vec::new(),
                    next: None,
                });
            }
            query.push(" AND state IN (");
            let mut separated = query.separated(", ");
            for state in states {
                separated.push_bind(deletion_operation_state_name(*state));
            }
            separated.push_unseparated(")");
        }
        if let Some(after) = &request.after {
            query
                .push(" AND (created_at_unix_ms < ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" OR (created_at_unix_ms = ")
                .push_bind(as_i64(after.created_at_unix_ms)?)
                .push(" AND deletion_id > ")
                .push_bind(after.deletion_id.as_str())
                .push("))");
        }
        query
            .push(" ORDER BY created_at_unix_ms DESC, deletion_id ASC LIMIT ")
            .push_bind(i64::from(request.limit) + 1);
        let payloads = query
            .build_query_scalar::<Vec<u8>>()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        let mut records = payloads
            .into_iter()
            .map(|payload| decode_lifecycle_payload(&payload, "DeletionOperation"))
            .collect::<CentralResult<Vec<DeletionOperation>>>()?;
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
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let outcome = create_deletion_transaction(&mut transaction, request).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn restore_deletion_idempotent(
        &self,
        request: RestoreDeletionRequest,
    ) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let outcome = restore_deletion_transaction(&mut transaction, request).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn retry_deletion_idempotent(
        &self,
        request: RetryDeletionRequest,
    ) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let outcome = retry_deletion_transaction(&mut transaction, request).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn transition_deletion_state(
        &self,
        request: DeletionTransitionRequest,
    ) -> CentralResult<DeletionOperation> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let operation = transition_deletion_transaction(&mut transaction, request).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(operation)
    }

    async fn list_retention_holds(
        &self,
        tenant_id: &TenantId,
        deletion_id: &DeletionId,
    ) -> CentralResult<Vec<RetentionHold>> {
        let payloads = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT payload FROM retention_hold_records \
             WHERE tenant_id = ? AND deletion_id = ? \
             ORDER BY created_at_unix_ms ASC, retention_hold_id ASC",
        )
        .bind(tenant_id.as_str())
        .bind(deletion_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        payloads
            .into_iter()
            .map(|payload| decode_lifecycle_payload(&payload, "RetentionHold"))
            .collect()
    }

    async fn create_retention_hold_idempotent(
        &self,
        request: CreateRetentionHoldRequest,
    ) -> CentralResult<CatalogInsertOutcome<RetentionHold>> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let outcome = create_retention_hold_transaction(&mut transaction, request).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn release_retention_hold_idempotent(
        &self,
        request: ReleaseRetentionHoldRequest,
    ) -> CentralResult<CatalogInsertOutcome<RetentionHold>> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let outcome = release_retention_hold_transaction(&mut transaction, request).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn append_lifecycle_evidence(
        &self,
        tenant_id: &TenantId,
        deletion_id: &DeletionId,
        batch: LifecycleEvidenceBatch,
    ) -> CentralResult<()> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        load_deletion_operation(&mut transaction, tenant_id, deletion_id)
            .await?
            .ok_or_else(|| id_reused("deletion operation does not exist"))?;
        if let Some(event) = batch.event {
            if &event.tenant_id != tenant_id || &event.deletion_id != deletion_id {
                return Err(id_reused(
                    "lifecycle event scope does not match deletion operation",
                ));
            }
            insert_idempotent_lifecycle_payload(
                &mut transaction,
                "lifecycle_events",
                "event_id",
                event.event_id.as_str(),
                tenant_id,
                deletion_id,
                event.occurred_at_unix_ms,
                &event,
            )
            .await?;
        }
        if let Some(proof) = batch.proof {
            if &proof.tenant_id != tenant_id || &proof.deletion_id != deletion_id {
                return Err(id_reused(
                    "deletion proof scope does not match deletion operation",
                ));
            }
            insert_idempotent_lifecycle_payload(
                &mut transaction,
                "deletion_proofs",
                "proof_id",
                proof.proof_id.as_str(),
                tenant_id,
                deletion_id,
                proof.completed_at_unix_ms,
                &proof,
            )
            .await?;
        }
        transaction.commit().await.map_err(storage_error)
    }

    async fn enqueue_lifecycle_assignment(
        &self,
        record: LifecycleAssignmentOutboxRecord,
    ) -> CentralResult<LifecycleAssignmentInsertOutcome> {
        if record.published || record.retired || record.terminal_report_digest.is_some() {
            return Err(id_reused(
                "new lifecycle assignments must be unpublished and active",
            ));
        }
        record.assignment.validate().map_err(protocol_error)?;
        let command = &record.assignment.assignment;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let operation =
            load_deletion_operation(&mut transaction, &command.tenant_id, &command.deletion_id)
                .await?
                .ok_or_else(|| id_reused("lifecycle assignment deletion does not exist"))?;
        crate::catalog_memory::validate_lifecycle_assignment_operation(
            &operation,
            &record.assignment,
        )?;
        let existing = sqlx::query(
            "SELECT payload, published, retired FROM lifecycle_assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(command.tenant_id.as_str())
        .bind(command.assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(row) = existing {
            let existing = decode_lifecycle_assignment_row(row)?;
            if existing.assignment != record.assignment {
                return Err(id_reused("LifecycleAssignment ID is already used"));
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(LifecycleAssignmentInsertOutcome::Existing(existing));
        }
        sqlx::query(
            "INSERT INTO lifecycle_assignment_outbox \
             (tenant_id, assignment_id, deletion_id, agent_id, published, retired, payload) \
             VALUES (?, ?, ?, ?, 0, 0, ?)",
        )
        .bind(command.tenant_id.as_str())
        .bind(command.assignment_id.as_str())
        .bind(command.deletion_id.as_str())
        .bind(record.assignment.agent_id.as_str())
        .bind(encode_lifecycle_payload(&record)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(LifecycleAssignmentInsertOutcome::Inserted(record))
    }

    async fn get_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<Option<LifecycleAssignmentOutboxRecord>> {
        sqlx::query(
            "SELECT payload, published, retired FROM lifecycle_assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(decode_lifecycle_assignment_row)
        .transpose()
    }

    async fn pending_lifecycle_assignments_for_agent(
        &self,
        agent_id: &neoengram_domain::protocol::AgentId,
        limit: usize,
    ) -> CentralResult<Vec<LifecycleAssignmentOutboxRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit).map_err(|_| {
            CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "lifecycle assignment page size exceeds the SQLite range",
            )
            .with_retryable(false)
        })?;
        sqlx::query(
            "SELECT payload, published, retired FROM lifecycle_assignment_outbox \
             WHERE agent_id = ? AND published = 1 AND retired = 0 \
             ORDER BY tenant_id, assignment_id LIMIT ?",
        )
        .bind(agent_id.as_str())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_lifecycle_assignment_row)
        .collect()
    }

    async fn publish_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<LifecycleAssignmentOutboxRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT payload, published, retired FROM lifecycle_assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| id_reused("lifecycle assignment is not reserved"))?;
        let mut record = decode_lifecycle_assignment_row(row)?;
        if !record.published && !record.retired {
            sqlx::query(
                "UPDATE lifecycle_assignment_outbox SET published = 1 \
                 WHERE tenant_id = ? AND assignment_id = ? AND published = 0 AND retired = 0",
            )
            .bind(tenant_id.as_str())
            .bind(assignment_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            record.published = true;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn retire_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<LifecycleAssignmentOutboxRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT payload, published, retired FROM lifecycle_assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| id_reused("lifecycle assignment is not reserved"))?;
        let mut record = decode_lifecycle_assignment_row(row)?;
        if !record.published {
            return Err(id_reused("lifecycle assignment is not published"));
        }
        if !record.retired {
            sqlx::query(
                "UPDATE lifecycle_assignment_outbox SET retired = 1 \
                 WHERE tenant_id = ? AND assignment_id = ? AND published = 1 AND retired = 0",
            )
            .bind(tenant_id.as_str())
            .bind(assignment_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            record.retired = true;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn record_lifecycle_report(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
        report_digest: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<LifecycleAssignmentOutboxRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT payload, published, retired FROM lifecycle_assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| id_reused("lifecycle assignment is not reserved"))?;
        let mut record = decode_lifecycle_assignment_row(row)?;
        if let Some(existing) = &record.terminal_report_digest {
            if existing != report_digest {
                return Err(id_reused(
                    "lifecycle assignment already has a different terminal report",
                ));
            }
        } else {
            record.terminal_report_digest = Some(*report_digest);
            sqlx::query(
                "UPDATE lifecycle_assignment_outbox SET payload = ? \
                 WHERE tenant_id = ? AND assignment_id = ?",
            )
            .bind(encode_lifecycle_payload(&record)?)
            .bind(tenant_id.as_str())
            .bind(assignment_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }
}

type LifecycleCatalogMaps = (
    BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    BTreeMap<(TenantId, ProjectId, ArtifactId, PlaygroundId), PlaygroundRecord>,
    BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
    BTreeMap<(TenantId, S3AccessPointId), S3AccessPointRecord>,
    BTreeMap<S3CredentialId, S3CredentialRecord>,
);

async fn load_lifecycle_catalog(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
) -> CentralResult<LifecycleCatalogMaps> {
    let artifact_sql =
        format!("SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records WHERE tenant_id = ?");
    let artifacts = sqlx::query(&artifact_sql)
        .bind(tenant_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_artifact)
        .map(|result| {
            result.map(|record| {
                (
                    (record.tenant_id.clone(), record.artifact_id.clone()),
                    record,
                )
            })
        })
        .collect::<CentralResult<BTreeMap<_, _>>>()?;
    let volume_sql =
        format!("SELECT {VOLUME_COLUMNS} FROM storage_volume_catalog_records WHERE tenant_id = ?");
    let volumes = sqlx::query(&volume_sql)
        .bind(tenant_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_volume)
        .map(|result| {
            result.map(|record| {
                (
                    (record.tenant_id.clone(), record.storage_volume_id.clone()),
                    record,
                )
            })
        })
        .collect::<CentralResult<BTreeMap<_, _>>>()?;
    let playground_sql =
        format!("SELECT {PLAYGROUND_COLUMNS} FROM playground_catalog_records WHERE tenant_id = ?");
    let playgrounds = sqlx::query(&playground_sql)
        .bind(tenant_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_playground)
        .map(|result| {
            result.map(|record| {
                (
                    (
                        record.tenant_id.clone(),
                        record.project_id.clone(),
                        record.artifact_id.clone(),
                        record.playground_id.clone(),
                    ),
                    record,
                )
            })
        })
        .collect::<CentralResult<BTreeMap<_, _>>>()?;
    let snapshot_sql =
        format!("SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records WHERE tenant_id = ?");
    let snapshots = sqlx::query(&snapshot_sql)
        .bind(tenant_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_snapshot)
        .map(|result| {
            result.map(|record| {
                (
                    (record.tenant_id.clone(), record.snapshot_id.clone()),
                    record,
                )
            })
        })
        .collect::<CentralResult<BTreeMap<_, _>>>()?;
    let access_point_sql = format!(
        "SELECT {S3_ACCESS_POINT_COLUMNS} FROM s3_access_point_records WHERE tenant_id = ?"
    );
    let access_points = sqlx::query(&access_point_sql)
        .bind(tenant_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_s3_access_point)
        .map(|result| {
            result.map(|record| {
                (
                    (record.tenant_id.clone(), record.access_point_id.clone()),
                    record,
                )
            })
        })
        .collect::<CentralResult<BTreeMap<_, _>>>()?;
    let credential_sql = format!(
        "SELECT {S3_CREDENTIAL_COLUMNS} FROM s3_credential_records AS credential \
         WHERE EXISTS (SELECT 1 FROM s3_access_point_records AS access_point \
                       WHERE access_point.access_point_id = credential.access_point_id \
                         AND access_point.tenant_id = ?)"
    );
    let credentials = sqlx::query(&credential_sql)
        .bind(tenant_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_s3_credential)
        .map(|result| result.map(|record| (record.credential_id.clone(), record)))
        .collect::<CentralResult<BTreeMap<_, _>>>()?;
    Ok((
        artifacts,
        volumes,
        playgrounds,
        snapshots,
        access_points,
        credentials,
    ))
}

async fn load_deletion_operation_pool(
    pool: &sqlx::SqlitePool,
    tenant_id: &TenantId,
    deletion_id: &DeletionId,
) -> CentralResult<Option<DeletionOperation>> {
    let payload: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM deletion_operation_records WHERE tenant_id = ? AND deletion_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(deletion_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(storage_error)?;
    payload
        .map(|payload| decode_lifecycle_payload(&payload, "DeletionOperation"))
        .transpose()
}

async fn load_deletion_operation(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    deletion_id: &DeletionId,
) -> CentralResult<Option<DeletionOperation>> {
    let payload: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM deletion_operation_records WHERE tenant_id = ? AND deletion_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(deletion_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    payload
        .map(|payload| decode_lifecycle_payload(&payload, "DeletionOperation"))
        .transpose()
}

async fn load_deletion_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> CentralResult<Option<neoengram_domain::protocol::DeletionMutation>> {
    let payload: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM deletion_mutation_records WHERE tenant_id = ? AND request_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(request_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    payload
        .map(|payload| decode_lifecycle_payload(&payload, "DeletionMutation"))
        .transpose()
}

fn encode_lifecycle_payload(value: &impl Serialize) -> CentralResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| {
        CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            format!("lifecycle payload cannot be encoded: {error}"),
        )
        .with_retryable(false)
    })
}

fn decode_lifecycle_payload<T: DeserializeOwned>(payload: &[u8], kind: &str) -> CentralResult<T> {
    serde_json::from_slice(payload)
        .map_err(|error| corruption(format!("stored {kind} payload is invalid: {error}")))
}

fn decode_lifecycle_assignment_row(
    row: SqliteRow,
) -> CentralResult<LifecycleAssignmentOutboxRecord> {
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let mut record: LifecycleAssignmentOutboxRecord =
        decode_lifecycle_payload(&payload, "LifecycleAssignment")?;
    record.published = row.try_get::<i64, _>("published").map_err(storage_error)? == 1;
    record.retired = row.try_get::<i64, _>("retired").map_err(storage_error)? == 1;
    Ok(record)
}

async fn create_deletion_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    request: CreateDeletionRequest,
) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
    if let Some(existing) =
        load_deletion_mutation(transaction, &request.tenant_id, &request.request_id).await?
    {
        validate_deletion_mutation(
            &existing,
            DeletionMutationKind::Create,
            &request.deletion_id,
            &request.request_digest,
            None,
        )?;
        let operation =
            load_deletion_operation(transaction, &request.tenant_id, &existing.deletion_id)
                .await?
                .ok_or_else(|| corruption("deletion mutation references a missing operation"))?;
        return Ok(CatalogInsertOutcome::Existing(operation));
    }
    if load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
        .await?
        .is_some()
    {
        return Err(id_reused(
            "Deletion ID is already bound to another create request",
        ));
    }
    let impact_payload: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM deletion_impact_records \
         WHERE tenant_id = ? AND impact_digest = ? AND expires_at_unix_ms >= ?",
    )
    .bind(request.tenant_id.as_str())
    .bind(request.impact_digest.as_bytes().as_slice())
    .bind(as_i64(request.now_unix_ms)?)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let impact: neoengram_domain::protocol::DeletionImpact = impact_payload
        .map(|payload| decode_lifecycle_payload(&payload, "DeletionImpact"))
        .transpose()?
        .ok_or_else(|| id_reused("deletion impact digest is unknown or expired"))?;
    if impact.root != request.root
        || impact.cascade != request.cascade
        || impact.confirm_managed_data_erase != request.confirm_managed_data_erase
    {
        return Err(id_reused(
            "deletion impact does not match the requested resource or cascade confirmation",
        ));
    }
    if !impact.blockers.is_empty() {
        return Err(id_reused("deletion impact contains blocking conditions"));
    }
    let root_version = impact
        .targets
        .iter()
        .find(|target| target.resource == impact.root)
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
    let (mut artifacts, mut volumes, mut playgrounds, mut snapshots, _access_points, _credentials) =
        load_lifecycle_catalog(transaction, &request.tenant_id).await?;
    crate::catalog_memory::validate_current_targets(
        &request.tenant_id,
        &impact.targets,
        &artifacts,
        &volumes,
        &playgrounds,
        &snapshots,
    )?;
    let targets = crate::catalog_memory::fence_targets_for_delete(
        &request.tenant_id,
        &impact.targets,
        &request.deletion_id,
        request.now_unix_ms,
        purge_after_unix_ms,
        &mut artifacts,
        &mut volumes,
        &mut playgrounds,
        &mut snapshots,
    )?;
    persist_lifecycle_targets(
        transaction,
        &request.tenant_id,
        &impact.targets,
        &targets,
        &artifacts,
        &volumes,
        &playgrounds,
        &snapshots,
    )
    .await?;
    disable_snapshot_s3_access_sqlite(
        transaction,
        &request.tenant_id,
        targets.iter().filter_map(|target| match &target.resource {
            ResourceRef::Snapshot { snapshot_id } => Some(snapshot_id),
            _ => None,
        }),
        request.now_unix_ms,
    )
    .await?;
    let operation = DeletionOperation {
        deletion_id: request.deletion_id.clone(),
        tenant_id: request.tenant_id.clone(),
        root: impact.root,
        state: DeletionOperationState::Requested,
        resource_version: ResourceVersion::new(1),
        targets,
        request_id: request.request_id.clone(),
        request_digest: request.request_digest,
        impact_digest: request.impact_digest,
        cascade: impact.cascade,
        confirm_managed_data_erase: impact.confirm_managed_data_erase,
        purge_after_unix_ms,
        created_at_unix_ms: request.now_unix_ms,
        updated_at_unix_ms: request.now_unix_ms,
        completion: None,
        last_error: None,
        resume_state: None,
        retry_count: DecimalU64::new(0),
    };
    insert_deletion_operation(transaction, &operation).await?;
    let mutation = DeletionMutation {
        tenant_id: request.tenant_id,
        request_id: request.request_id,
        kind: DeletionMutationKind::Create,
        request_digest: request.request_digest,
        deletion_id: request.deletion_id,
        retention_hold_id: None,
        created_at_unix_ms: request.now_unix_ms,
    };
    insert_deletion_mutation(transaction, &mutation).await?;
    Ok(CatalogInsertOutcome::Inserted(operation))
}

#[allow(clippy::too_many_arguments)]
async fn persist_lifecycle_targets(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    previous_targets: &[neoengram_domain::protocol::DeletionTarget],
    updated_targets: &[neoengram_domain::protocol::DeletionTarget],
    artifacts: &BTreeMap<(TenantId, ArtifactId), ArtifactRecord>,
    volumes: &BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>,
    playgrounds: &BTreeMap<(TenantId, ProjectId, ArtifactId, PlaygroundId), PlaygroundRecord>,
    snapshots: &BTreeMap<(TenantId, SnapshotId), SnapshotRecord>,
) -> CentralResult<()> {
    for updated in updated_targets {
        let previous = previous_targets
            .iter()
            .find(|target| target.resource == updated.resource)
            .ok_or_else(|| corruption("updated lifecycle target has no prior fence"))?;
        let result = match &updated.resource {
            ResourceRef::StorageVolume { storage_volume_id } => {
                let record = volumes
                    .get(&(tenant_id.clone(), storage_volume_id.clone()))
                    .ok_or_else(|| corruption("updated StorageVolume is missing"))?;
                update_lifecycle_row(
                    transaction,
                    tenant_id,
                    "storage_volume_catalog_records",
                    "storage_volume_id",
                    storage_volume_id.as_str(),
                    None,
                    None,
                    previous,
                    record.resource_version,
                    &record.lifecycle,
                    record.updated_at_unix_ms,
                )
                .await?
            }
            ResourceRef::Artifact {
                project_id,
                artifact_id,
            } => {
                let record = artifacts
                    .get(&(tenant_id.clone(), artifact_id.clone()))
                    .filter(|record| record.project_id == *project_id)
                    .ok_or_else(|| corruption("updated Artifact is missing"))?;
                update_lifecycle_row(
                    transaction,
                    tenant_id,
                    "artifact_catalog_records",
                    "artifact_id",
                    artifact_id.as_str(),
                    Some(("project_id", project_id.as_str())),
                    None,
                    previous,
                    record.resource_version,
                    &record.lifecycle,
                    record.updated_at_unix_ms,
                )
                .await?
            }
            ResourceRef::Playground {
                project_id,
                artifact_id,
                playground_id,
            } => {
                let record = playgrounds
                    .get(&(
                        tenant_id.clone(),
                        project_id.clone(),
                        artifact_id.clone(),
                        playground_id.clone(),
                    ))
                    .ok_or_else(|| corruption("updated Playground is missing"))?;
                update_lifecycle_row(
                    transaction,
                    tenant_id,
                    "playground_catalog_records",
                    "playground_id",
                    playground_id.as_str(),
                    Some(("project_id", project_id.as_str())),
                    Some(("artifact_id", artifact_id.as_str())),
                    previous,
                    record.resource_version,
                    &record.lifecycle,
                    record.updated_at_unix_ms,
                )
                .await?
            }
            ResourceRef::Snapshot { snapshot_id } => {
                let record = snapshots
                    .get(&(tenant_id.clone(), snapshot_id.clone()))
                    .ok_or_else(|| corruption("updated Snapshot is missing"))?;
                update_lifecycle_row(
                    transaction,
                    tenant_id,
                    "snapshot_catalog_records",
                    "snapshot_id",
                    snapshot_id.as_str(),
                    None,
                    None,
                    previous,
                    record.resource_version,
                    &record.lifecycle,
                    record.updated_at_unix_ms,
                )
                .await?
            }
        };
        if result != 1 {
            return Err(concurrent("resource lifecycle changed during transaction"));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn update_lifecycle_row(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    table: &'static str,
    id_column: &'static str,
    id: &str,
    scope_1: Option<(&'static str, &str)>,
    scope_2: Option<(&'static str, &str)>,
    previous: &neoengram_domain::protocol::DeletionTarget,
    resource_version: u64,
    lifecycle: &ResourceLifecycle,
    updated_at_unix_ms: UnixMillis,
) -> CentralResult<u64> {
    let mut query = QueryBuilder::<Sqlite>::new(format!("UPDATE {table} SET resource_version = "));
    query
        .push_bind(resource_version.to_string())
        .push(", lifecycle_state = ")
        .push_bind(resource_lifecycle_state_name(lifecycle.state))
        .push(", lifecycle_generation = ")
        .push_bind(lifecycle.generation.to_string())
        .push(", active_deletion_id = ")
        .push_bind(
            lifecycle
                .active_deletion_id
                .as_ref()
                .map(DeletionId::as_str),
        )
        .push(", delete_requested_at_unix_ms = ")
        .push_bind(optional_as_i64(lifecycle.delete_requested_at_unix_ms)?)
        .push(", purge_after_unix_ms = ")
        .push_bind(optional_as_i64(lifecycle.purge_after_unix_ms)?)
        .push(", deleted_at_unix_ms = ")
        .push_bind(optional_as_i64(lifecycle.deleted_at_unix_ms)?)
        .push(", updated_at_unix_ms = ")
        .push_bind(as_i64(updated_at_unix_ms)?)
        .push(" WHERE tenant_id = ")
        .push_bind(tenant_id.as_str())
        .push(format!(" AND {id_column} = "))
        .push_bind(id);
    if let Some((column, value)) = scope_1 {
        query.push(format!(" AND {column} = ")).push_bind(value);
    }
    if let Some((column, value)) = scope_2 {
        query.push(format!(" AND {column} = ")).push_bind(value);
    }
    query
        .push(" AND resource_version = ")
        .push_bind(previous.resource_version.to_string())
        .push(" AND lifecycle_generation = ")
        .push_bind(previous.lifecycle_generation.to_string());
    Ok(query
        .build()
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?
        .rows_affected())
}

async fn disable_snapshot_s3_access_sqlite<'a>(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    snapshot_ids: impl Iterator<Item = &'a SnapshotId>,
    now_unix_ms: UnixMillis,
) -> CentralResult<()> {
    let snapshot_ids = snapshot_ids.collect::<Vec<_>>();
    if snapshot_ids.is_empty() {
        return Ok(());
    }
    let mut select = QueryBuilder::<Sqlite>::new(
        "SELECT access_point_id FROM s3_access_point_records WHERE tenant_id = ",
    );
    select
        .push_bind(tenant_id.as_str())
        .push(" AND snapshot_id IN (");
    let mut separated = select.separated(", ");
    for snapshot_id in snapshot_ids {
        separated.push_bind(snapshot_id.as_str());
    }
    separated.push_unseparated(")");
    let access_point_ids = select
        .build_query_scalar::<String>()
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?;
    for access_point_id in &access_point_ids {
        let result = sqlx::query(
            "UPDATE s3_access_point_records SET state = 'disabled', \
                 policy_generation = policy_generation + 1, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND access_point_id = ? \
               AND policy_generation < 9223372036854775807",
        )
        .bind(as_i64(now_unix_ms)?)
        .bind(tenant_id.as_str())
        .bind(access_point_id)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() != 1 {
            return Err(id_reused("S3 policy generation exhausted during deletion"));
        }
        // X'00' is an irreversible tombstone, not encrypted Secret material. The current
        // schema requires a non-empty blob even for revoked credentials.
        sqlx::query(
            "UPDATE s3_credential_records SET state = 'revoked', encrypted_secret = X'00' \
             WHERE access_point_id = ?",
        )
        .bind(access_point_id)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    }
    Ok(())
}

async fn insert_deletion_operation(
    transaction: &mut Transaction<'_, Sqlite>,
    operation: &DeletionOperation,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO deletion_operation_records \
         (tenant_id, deletion_id, state, resource_version, request_id, request_digest, \
          impact_digest, purge_after_unix_ms, created_at_unix_ms, updated_at_unix_ms, payload) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(operation.tenant_id.as_str())
    .bind(operation.deletion_id.as_str())
    .bind(deletion_operation_state_name(operation.state))
    .bind(operation.resource_version.to_string())
    .bind(operation.request_id.as_str())
    .bind(operation.request_digest.as_bytes().as_slice())
    .bind(operation.impact_digest.as_bytes().as_slice())
    .bind(as_i64(operation.purge_after_unix_ms)?)
    .bind(as_i64(operation.created_at_unix_ms)?)
    .bind(as_i64(operation.updated_at_unix_ms)?)
    .bind(encode_lifecycle_payload(operation)?)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

async fn update_deletion_operation(
    transaction: &mut Transaction<'_, Sqlite>,
    previous_resource_version: ResourceVersion,
    operation: &DeletionOperation,
) -> CentralResult<()> {
    let result = sqlx::query(
        "UPDATE deletion_operation_records SET state = ?, resource_version = ?, \
             updated_at_unix_ms = ?, payload = ? \
         WHERE tenant_id = ? AND deletion_id = ? AND resource_version = ?",
    )
    .bind(deletion_operation_state_name(operation.state))
    .bind(operation.resource_version.to_string())
    .bind(as_i64(operation.updated_at_unix_ms)?)
    .bind(encode_lifecycle_payload(operation)?)
    .bind(operation.tenant_id.as_str())
    .bind(operation.deletion_id.as_str())
    .bind(previous_resource_version.to_string())
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if result.rows_affected() != 1 {
        return Err(concurrent("deletion operation changed concurrently"));
    }
    Ok(())
}

async fn insert_deletion_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    mutation: &DeletionMutation,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO deletion_mutation_records \
         (tenant_id, request_id, kind, deletion_id, retention_hold_id, request_digest, \
          created_at_unix_ms, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(mutation.tenant_id.as_str())
    .bind(mutation.request_id.as_str())
    .bind(deletion_mutation_kind_name(mutation.kind))
    .bind(mutation.deletion_id.as_str())
    .bind(
        mutation
            .retention_hold_id
            .as_ref()
            .map(RetentionHoldId::as_str),
    )
    .bind(mutation.request_digest.as_bytes().as_slice())
    .bind(as_i64(mutation.created_at_unix_ms)?)
    .bind(encode_lifecycle_payload(mutation)?)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
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
        Err(id_reused(
            "lifecycle request identity is already bound to another mutation",
        ))
    }
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
        .ok_or_else(|| id_reused(message))
}

fn next_resource_version(current: ResourceVersion) -> CentralResult<ResourceVersion> {
    current
        .get()
        .checked_add(1)
        .map(ResourceVersion::new)
        .ok_or_else(|| id_reused("resource version exhausted"))
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

async fn restore_deletion_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    request: RestoreDeletionRequest,
) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
    if let Some(existing) =
        load_deletion_mutation(transaction, &request.tenant_id, &request.request_id).await?
    {
        validate_deletion_mutation(
            &existing,
            DeletionMutationKind::Restore,
            &request.deletion_id,
            &request.request_digest,
            None,
        )?;
        let operation =
            load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
                .await?
                .ok_or_else(|| corruption("restore mutation references a missing operation"))?;
        return Ok(CatalogInsertOutcome::Existing(operation));
    }
    let mut operation =
        load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
            .await?
            .ok_or_else(|| id_reused("deletion operation does not exist"))?;
    require_operation_version(&operation, request.expected_resource_version)?;
    if !matches!(
        operation.state,
        DeletionOperationState::Recoverable
            | DeletionOperationState::Blocked
            | DeletionOperationState::Failed
    ) || request.now_unix_ms >= operation.purge_after_unix_ms
    {
        return Err(id_reused("deletion operation is no longer restorable"));
    }
    let previous_operation_version = operation.resource_version;
    let previous_targets = operation.targets.clone();
    let (mut artifacts, mut volumes, mut playgrounds, mut snapshots, _access_points, _credentials) =
        load_lifecycle_catalog(transaction, &request.tenant_id).await?;
    operation.targets = crate::catalog_memory::set_target_lifecycle_state(
        &request.tenant_id,
        &previous_targets,
        &request.deletion_id,
        ResourceLifecycleState::Restoring,
        request.now_unix_ms,
        &mut artifacts,
        &mut volumes,
        &mut playgrounds,
        &mut snapshots,
    )?;
    persist_lifecycle_targets(
        transaction,
        &request.tenant_id,
        &previous_targets,
        &operation.targets,
        &artifacts,
        &volumes,
        &playgrounds,
        &snapshots,
    )
    .await?;
    operation.state = DeletionOperationState::Restoring;
    operation.resource_version = next_resource_version(operation.resource_version)?;
    operation.updated_at_unix_ms = request.now_unix_ms;
    operation.last_error = None;
    operation.resume_state = None;
    update_deletion_operation(transaction, previous_operation_version, &operation).await?;
    insert_deletion_mutation(
        transaction,
        &DeletionMutation {
            tenant_id: request.tenant_id,
            request_id: request.request_id,
            kind: DeletionMutationKind::Restore,
            request_digest: request.request_digest,
            deletion_id: request.deletion_id,
            retention_hold_id: None,
            created_at_unix_ms: request.now_unix_ms,
        },
    )
    .await?;
    Ok(CatalogInsertOutcome::Inserted(operation))
}

async fn retry_deletion_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    request: RetryDeletionRequest,
) -> CentralResult<CatalogInsertOutcome<DeletionOperation>> {
    if let Some(existing) =
        load_deletion_mutation(transaction, &request.tenant_id, &request.request_id).await?
    {
        validate_deletion_mutation(
            &existing,
            DeletionMutationKind::Retry,
            &request.deletion_id,
            &request.request_digest,
            None,
        )?;
        let operation =
            load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
                .await?
                .ok_or_else(|| corruption("retry mutation references a missing operation"))?;
        return Ok(CatalogInsertOutcome::Existing(operation));
    }
    let mut operation =
        load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
            .await?
            .ok_or_else(|| id_reused("deletion operation does not exist"))?;
    require_operation_version(&operation, request.expected_resource_version)?;
    if !matches!(
        operation.state,
        DeletionOperationState::Blocked | DeletionOperationState::Failed
    ) {
        return Err(id_reused(
            "only blocked or failed deletion operations can be retried",
        ));
    }
    let previous = operation.resource_version;
    let resume_state = operation
        .resume_state
        .filter(|state| crate::catalog_memory::retryable_deletion_resume_state(*state))
        .ok_or_else(|| {
            corruption("blocked or failed deletion operation has no valid resume state")
        })?;
    let next_version = next_resource_version(operation.resource_version)?;
    let next_retry_count = operation
        .retry_count
        .get()
        .checked_add(1)
        .ok_or_else(|| id_reused("deletion retry counter exhausted"))?;
    operation.state = resume_state;
    operation.resource_version = next_version;
    operation.retry_count = DecimalU64::new(next_retry_count);
    operation.updated_at_unix_ms = request.now_unix_ms;
    operation.last_error = None;
    operation.resume_state = None;
    update_deletion_operation(transaction, previous, &operation).await?;
    insert_deletion_mutation(
        transaction,
        &DeletionMutation {
            tenant_id: request.tenant_id,
            request_id: request.request_id,
            kind: DeletionMutationKind::Retry,
            request_digest: request.request_digest,
            deletion_id: request.deletion_id,
            retention_hold_id: None,
            created_at_unix_ms: request.now_unix_ms,
        },
    )
    .await?;
    Ok(CatalogInsertOutcome::Inserted(operation))
}

async fn transition_deletion_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    request: DeletionTransitionRequest,
) -> CentralResult<DeletionOperation> {
    let mut operation =
        load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
            .await?
            .ok_or_else(|| id_reused("deletion operation does not exist"))?;
    require_operation_version(&operation, request.expected_resource_version)?;
    if operation.state == request.next_state {
        return Ok(operation);
    }
    if operation.state != request.expected_state
        || !crate::catalog_memory::valid_deletion_transition(
            request.expected_state,
            request.next_state,
        )
    {
        return Err(id_reused("invalid deletion operation state transition"));
    }
    if request.next_state == DeletionOperationState::Purging {
        if request.now_unix_ms < operation.purge_after_unix_ms {
            return Err(id_reused("deletion recovery window has not elapsed"));
        }
        let payloads = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT payload FROM retention_hold_records \
             WHERE tenant_id = ? AND deletion_id = ? AND state = 'active'",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.deletion_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?;
        let holds = payloads
            .into_iter()
            .map(|payload| decode_lifecycle_payload(&payload, "RetentionHold"))
            .collect::<CentralResult<Vec<RetentionHold>>>()?;
        if holds
            .iter()
            .any(|hold| crate::catalog_memory::retention_hold_is_active(hold, request.now_unix_ms))
        {
            return Err(id_reused("an active retention hold prevents purge"));
        }
    }
    let previous_operation_version = operation.resource_version;
    let previous_targets = operation.targets.clone();
    let (mut artifacts, mut volumes, mut playgrounds, mut snapshots, _access_points, _credentials) =
        load_lifecycle_catalog(transaction, &request.tenant_id).await?;
    if request.next_state == DeletionOperationState::Quarantining {
        operation.targets = crate::catalog_memory::set_target_lifecycle_state(
            &request.tenant_id,
            &previous_targets,
            &request.deletion_id,
            ResourceLifecycleState::Deleting,
            request.now_unix_ms,
            &mut artifacts,
            &mut volumes,
            &mut playgrounds,
            &mut snapshots,
        )?;
    } else if request.expected_state == DeletionOperationState::Restoring
        && request.next_state == DeletionOperationState::Completed
    {
        operation.targets = crate::catalog_memory::finalize_targets(
            &request.tenant_id,
            &previous_targets,
            &request.deletion_id,
            DeletionCompletion::Restored,
            request.now_unix_ms,
            &mut artifacts,
            &mut volumes,
            &mut playgrounds,
            &mut snapshots,
        )?;
        operation.completion = Some(DeletionCompletion::Restored);
    } else if request.expected_state == DeletionOperationState::Finalizing
        && request.next_state == DeletionOperationState::Completed
    {
        operation.targets = crate::catalog_memory::finalize_targets(
            &request.tenant_id,
            &previous_targets,
            &request.deletion_id,
            DeletionCompletion::Purged,
            request.now_unix_ms,
            &mut artifacts,
            &mut volumes,
            &mut playgrounds,
            &mut snapshots,
        )?;
        operation.completion = Some(DeletionCompletion::Purged);
    }
    if operation.targets != previous_targets {
        persist_lifecycle_targets(
            transaction,
            &request.tenant_id,
            &previous_targets,
            &operation.targets,
            &artifacts,
            &volumes,
            &playgrounds,
            &snapshots,
        )
        .await?;
    }
    if operation.completion == Some(DeletionCompletion::Purged) {
        finalize_snapshot_deliveries_for_purge(
            transaction,
            &request.tenant_id,
            &operation.targets,
            request.now_unix_ms,
        )
        .await?;
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
    update_deletion_operation(transaction, previous_operation_version, &operation).await?;
    Ok(operation)
}

async fn finalize_snapshot_deliveries_for_purge(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    targets: &[neoengram_domain::protocol::DeletionTarget],
    now_unix_ms: UnixMillis,
) -> CentralResult<()> {
    let snapshot_ids = targets
        .iter()
        .filter_map(|target| match &target.resource {
            ResourceRef::Snapshot { snapshot_id, .. } => Some(snapshot_id.clone()),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    for snapshot_id in snapshot_ids {
        let sql = format!(
            "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
             WHERE tenant_id = ? AND snapshot_id = ?"
        );
        let deliveries = sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(snapshot_id.as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(storage_error)?
            .into_iter()
            .map(decode_snapshot_delivery)
            .collect::<CentralResult<Vec<_>>>()?;
        for delivery in deliveries {
            if delivery.state == SnapshotDeliveryState::Deleted {
                continue;
            }
            let next_generation = DeliveryGeneration::new(
                delivery
                    .delivery_generation
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| {
                        id_reused("SnapshotDelivery generation exhausted during lifecycle purge")
                    })?,
            );
            let next_version = delivery.resource_version.checked_add(1).ok_or_else(|| {
                id_reused("SnapshotDelivery ResourceVersion exhausted during lifecycle purge")
            })?;
            let result = sqlx::query(
                "UPDATE snapshot_delivery_records SET state = 'deleted', \
                 delivery_generation = ?, resource_version = ?, updated_at_unix_ms = ?, \
                 issue_code = NULL, issue_message = NULL, issue_retryable = 0 \
                 WHERE tenant_id = ? AND delivery_id = ? AND resource_version = ?",
            )
            .bind(next_generation.to_string())
            .bind(next_version.to_string())
            .bind(as_i64(now_unix_ms)?)
            .bind(tenant_id.as_str())
            .bind(delivery.delivery_id.as_str())
            .bind(delivery.resource_version.to_string())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            if result.rows_affected() != 1 {
                return Err(concurrent(
                    "SnapshotDelivery changed during lifecycle purge",
                ));
            }
            sqlx::query(
                "DELETE FROM snapshot_delivery_object_retention_roots \
                 WHERE tenant_id = ? AND delivery_id = ?",
            )
            .bind(tenant_id.as_str())
            .bind(delivery.delivery_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        }
    }
    Ok(())
}

async fn load_retention_hold(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    retention_hold_id: &RetentionHoldId,
) -> CentralResult<Option<RetentionHold>> {
    let payload: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM retention_hold_records \
         WHERE tenant_id = ? AND retention_hold_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(retention_hold_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    payload
        .map(|payload| decode_lifecycle_payload(&payload, "RetentionHold"))
        .transpose()
}

async fn create_retention_hold_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    request: CreateRetentionHoldRequest,
) -> CentralResult<CatalogInsertOutcome<RetentionHold>> {
    if request.reason.trim().is_empty() {
        return Err(id_reused("retention hold reason must not be empty"));
    }
    if request
        .expires_at_unix_ms
        .is_some_and(|expires| expires <= request.now_unix_ms)
    {
        return Err(id_reused("retention hold expiry must be in the future"));
    }
    if let Some(existing) =
        load_deletion_mutation(transaction, &request.tenant_id, &request.request_id).await?
    {
        validate_deletion_mutation(
            &existing,
            DeletionMutationKind::RetentionHoldCreate,
            &request.deletion_id,
            &request.request_digest,
            Some(&request.retention_hold_id),
        )?;
        let hold = load_retention_hold(transaction, &request.tenant_id, &request.retention_hold_id)
            .await?
            .ok_or_else(|| corruption("hold mutation references a missing hold"))?;
        return Ok(CatalogInsertOutcome::Existing(hold));
    }
    if load_retention_hold(transaction, &request.tenant_id, &request.retention_hold_id)
        .await?
        .is_some()
    {
        return Err(id_reused(
            "RetentionHold ID is already bound to another request",
        ));
    }
    let mut operation =
        load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
            .await?
            .ok_or_else(|| id_reused("deletion operation does not exist"))?;
    require_operation_version(&operation, request.expected_resource_version)?;
    if matches!(
        operation.state,
        DeletionOperationState::Purging
            | DeletionOperationState::Finalizing
            | DeletionOperationState::Completed
    ) {
        return Err(id_reused("retention hold is too late for this deletion"));
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
    sqlx::query(
        "INSERT INTO retention_hold_records \
         (tenant_id, retention_hold_id, deletion_id, state, expires_at_unix_ms, \
          created_at_unix_ms, released_at_unix_ms, payload) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
    )
    .bind(hold.tenant_id.as_str())
    .bind(hold.retention_hold_id.as_str())
    .bind(hold.deletion_id.as_str())
    .bind(retention_hold_state_name(hold.state))
    .bind(optional_as_i64(hold.expires_at_unix_ms)?)
    .bind(as_i64(hold.created_at_unix_ms)?)
    .bind(encode_lifecycle_payload(&hold)?)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let previous = operation.resource_version;
    operation.resource_version = next_resource_version(operation.resource_version)?;
    operation.updated_at_unix_ms = request.now_unix_ms;
    update_deletion_operation(transaction, previous, &operation).await?;
    insert_deletion_mutation(
        transaction,
        &DeletionMutation {
            tenant_id: request.tenant_id,
            request_id: request.request_id,
            kind: DeletionMutationKind::RetentionHoldCreate,
            request_digest: request.request_digest,
            deletion_id: request.deletion_id,
            retention_hold_id: Some(request.retention_hold_id),
            created_at_unix_ms: request.now_unix_ms,
        },
    )
    .await?;
    Ok(CatalogInsertOutcome::Inserted(hold))
}

async fn release_retention_hold_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    request: ReleaseRetentionHoldRequest,
) -> CentralResult<CatalogInsertOutcome<RetentionHold>> {
    if let Some(existing) =
        load_deletion_mutation(transaction, &request.tenant_id, &request.request_id).await?
    {
        validate_deletion_mutation(
            &existing,
            DeletionMutationKind::RetentionHoldRelease,
            &request.deletion_id,
            &request.request_digest,
            Some(&request.retention_hold_id),
        )?;
        let hold = load_retention_hold(transaction, &request.tenant_id, &request.retention_hold_id)
            .await?
            .ok_or_else(|| corruption("hold release mutation references a missing hold"))?;
        return Ok(CatalogInsertOutcome::Existing(hold));
    }
    let mut operation =
        load_deletion_operation(transaction, &request.tenant_id, &request.deletion_id)
            .await?
            .ok_or_else(|| id_reused("deletion operation does not exist"))?;
    require_operation_version(&operation, request.expected_resource_version)?;
    let mut hold = load_retention_hold(transaction, &request.tenant_id, &request.retention_hold_id)
        .await?
        .ok_or_else(|| id_reused("retention hold does not exist"))?;
    if hold.deletion_id != request.deletion_id {
        return Err(id_reused("retention hold belongs to another deletion"));
    }
    hold.state = RetentionHoldState::Released;
    hold.released_at_unix_ms = Some(request.now_unix_ms);
    sqlx::query(
        "UPDATE retention_hold_records SET state = 'released', released_at_unix_ms = ?, \
             payload = ? WHERE tenant_id = ? AND retention_hold_id = ?",
    )
    .bind(as_i64(request.now_unix_ms)?)
    .bind(encode_lifecycle_payload(&hold)?)
    .bind(request.tenant_id.as_str())
    .bind(request.retention_hold_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let previous = operation.resource_version;
    operation.resource_version = next_resource_version(operation.resource_version)?;
    operation.updated_at_unix_ms = request.now_unix_ms;
    update_deletion_operation(transaction, previous, &operation).await?;
    insert_deletion_mutation(
        transaction,
        &DeletionMutation {
            tenant_id: request.tenant_id,
            request_id: request.request_id,
            kind: DeletionMutationKind::RetentionHoldRelease,
            request_digest: request.request_digest,
            deletion_id: request.deletion_id,
            retention_hold_id: Some(request.retention_hold_id),
            created_at_unix_ms: request.now_unix_ms,
        },
    )
    .await?;
    Ok(CatalogInsertOutcome::Inserted(hold))
}

#[allow(clippy::too_many_arguments)]
async fn insert_idempotent_lifecycle_payload(
    transaction: &mut Transaction<'_, Sqlite>,
    table: &'static str,
    id_column: &'static str,
    id: &str,
    tenant_id: &TenantId,
    deletion_id: &DeletionId,
    occurred_at_unix_ms: UnixMillis,
    value: &impl Serialize,
) -> CentralResult<()> {
    let payload = encode_lifecycle_payload(value)?;
    let select_sql = match (table, id_column) {
        ("lifecycle_events", "event_id") => {
            "SELECT payload FROM lifecycle_events WHERE tenant_id = ? AND event_id = ?"
        }
        ("deletion_proofs", "proof_id") => {
            "SELECT payload FROM deletion_proofs WHERE tenant_id = ? AND proof_id = ?"
        }
        _ => return Err(corruption("invalid lifecycle evidence table binding")),
    };
    let existing: Option<Vec<u8>> = sqlx::query_scalar(select_sql)
        .bind(tenant_id.as_str())
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
    if let Some(existing) = existing {
        if existing == payload {
            return Ok(());
        }
        return Err(id_reused("lifecycle evidence identity is already used"));
    }
    let insert_sql = match (table, id_column) {
        ("lifecycle_events", "event_id") => {
            "INSERT INTO lifecycle_events \
             (tenant_id, event_id, deletion_id, occurred_at_unix_ms, payload) \
             VALUES (?, ?, ?, ?, ?)"
        }
        ("deletion_proofs", "proof_id") => {
            "INSERT INTO deletion_proofs \
             (tenant_id, proof_id, deletion_id, completed_at_unix_ms, payload) \
             VALUES (?, ?, ?, ?, ?)"
        }
        _ => return Err(corruption("invalid lifecycle evidence table binding")),
    };
    sqlx::query(insert_sql)
        .bind(tenant_id.as_str())
        .bind(id)
        .bind(deletion_id.as_str())
        .bind(as_i64(occurred_at_unix_ms)?)
        .bind(payload)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    Ok(())
}

fn deletion_operation_state_name(value: DeletionOperationState) -> &'static str {
    match value {
        DeletionOperationState::Requested => "requested",
        DeletionOperationState::Quiescing => "quiescing",
        DeletionOperationState::Quarantining => "quarantining",
        DeletionOperationState::Recoverable => "recoverable",
        DeletionOperationState::Restoring => "restoring",
        DeletionOperationState::Purging => "purging",
        DeletionOperationState::Finalizing => "finalizing",
        DeletionOperationState::Completed => "completed",
        DeletionOperationState::Blocked => "blocked",
        DeletionOperationState::Failed => "failed",
    }
}

fn deletion_mutation_kind_name(value: DeletionMutationKind) -> &'static str {
    match value {
        DeletionMutationKind::Create => "create",
        DeletionMutationKind::Restore => "restore",
        DeletionMutationKind::Retry => "retry",
        DeletionMutationKind::RetentionHoldCreate => "retention_hold_create",
        DeletionMutationKind::RetentionHoldRelease => "retention_hold_release",
    }
}

fn retention_hold_state_name(value: RetentionHoldState) -> &'static str {
    match value {
        RetentionHoldState::Active => "active",
        RetentionHoldState::Released => "released",
    }
}

fn concurrent(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::ConcurrentUpdate, message).with_retryable(true)
}

fn protocol_error(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(CentralErrorCode::ProtocolInvalid, error.to_string()).with_retryable(false)
}

async fn load_s3_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> CentralResult<Option<S3MutationRecord>> {
    let sql = format!(
        "SELECT {S3_MUTATION_COLUMNS} FROM s3_mutation_records \
         WHERE tenant_id = ? AND request_id = ?"
    );
    sqlx::query(&sql)
        .bind(tenant_id.as_str())
        .bind(request_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .map(decode_s3_mutation)
        .transpose()
}

async fn load_s3_access_point(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    access_point_id: &S3AccessPointId,
) -> CentralResult<Option<S3AccessPointRecord>> {
    let sql = format!(
        "SELECT {S3_ACCESS_POINT_COLUMNS} FROM s3_access_point_records \
         WHERE tenant_id = ? AND access_point_id = ?"
    );
    sqlx::query(&sql)
        .bind(tenant_id.as_str())
        .bind(access_point_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .map(decode_s3_access_point)
        .transpose()
}

async fn load_s3_access_point_by_id(
    transaction: &mut Transaction<'_, Sqlite>,
    access_point_id: &S3AccessPointId,
) -> CentralResult<Option<S3AccessPointRecord>> {
    let sql = format!(
        "SELECT {S3_ACCESS_POINT_COLUMNS} FROM s3_access_point_records \
         WHERE access_point_id = ?"
    );
    sqlx::query(&sql)
        .bind(access_point_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .map(decode_s3_access_point)
        .transpose()
}

async fn require_active_s3_snapshot(
    transaction: &mut Transaction<'_, Sqlite>,
    access_point: &S3AccessPointRecord,
) -> CentralResult<()> {
    let sql = format!(
        "SELECT {SNAPSHOT_COLUMNS} FROM snapshot_catalog_records \
         WHERE tenant_id = ? AND snapshot_id = ? AND project_id = ? AND artifact_id = ? \
           AND commit_digest = ?"
    );
    let snapshot = sqlx::query(&sql)
        .bind(access_point.tenant_id.as_str())
        .bind(access_point.snapshot_id.as_str())
        .bind(access_point.project_id.as_str())
        .bind(access_point.artifact_id.as_str())
        .bind(access_point.commit_id.as_bytes().as_slice())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .map(decode_snapshot)
        .transpose()?
        .ok_or_else(|| id_reused("S3 Access Point Snapshot binding does not exist"))?;
    require_active(&snapshot.lifecycle, "S3 Access Point Snapshot")?;
    if snapshot.state != SnapshotState::Ready {
        return Err(id_reused("S3 Access Point Snapshot is not Ready"));
    }
    if snapshot.delivery_id != access_point.delivery_id
        || snapshot.storage_volume_id != access_point.storage_volume_id
        || snapshot.edge_cluster_id != access_point.edge_cluster_id
    {
        return Err(id_reused(
            "S3 Access Point Snapshot target binding does not match",
        ));
    }
    let delivery_sql = format!(
        "SELECT {SNAPSHOT_DELIVERY_COLUMNS} FROM snapshot_delivery_records \
         WHERE tenant_id = ? AND delivery_id = ?"
    );
    let delivery = sqlx::query(&delivery_sql)
        .bind(access_point.tenant_id.as_str())
        .bind(access_point.delivery_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .map(decode_snapshot_delivery)
        .transpose()?
        .ok_or_else(|| id_reused("S3 Access Point SnapshotDelivery does not exist"))?;
    if delivery.tenant_id != access_point.tenant_id
        || delivery.snapshot_id != access_point.snapshot_id
        || delivery.commit_id != access_point.commit_id
        || delivery.storage_volume_id != access_point.storage_volume_id
        || delivery.mode != snapshot.delivery_mode
    {
        return Err(id_reused(
            "S3 Access Point SnapshotDelivery binding does not match",
        ));
    }
    if delivery.state != SnapshotDeliveryState::Ready {
        return Err(id_reused("S3 Access Point SnapshotDelivery is not Ready"));
    }
    Ok(())
}

async fn load_s3_credential(
    transaction: &mut Transaction<'_, Sqlite>,
    credential_id: &S3CredentialId,
) -> CentralResult<Option<S3CredentialRecord>> {
    let sql = format!(
        "SELECT {S3_CREDENTIAL_COLUMNS} FROM s3_credential_records WHERE credential_id = ?"
    );
    sqlx::query(&sql)
        .bind(credential_id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .map(decode_s3_credential)
        .transpose()
}

async fn insert_s3_access_point_row(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &S3AccessPointRecord,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO s3_access_point_records \
         (access_point_id, tenant_id, project_id, artifact_id, snapshot_id, commit_digest, \
          delivery_id, storage_volume_id, edge_cluster_id, bucket_name, state, policy_generation, \
          created_at_unix_ms, updated_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(record.access_point_id.as_str())
    .bind(record.tenant_id.as_str())
    .bind(record.project_id.as_str())
    .bind(record.artifact_id.as_str())
    .bind(record.snapshot_id.as_str())
    .bind(record.commit_id.as_bytes().as_slice())
    .bind(record.delivery_id.as_str())
    .bind(record.storage_volume_id.as_str())
    .bind(record.edge_cluster_id.as_str())
    .bind(&record.bucket_name)
    .bind(s3_access_point_state_name(record.state))
    .bind(i64::try_from(record.policy_generation).map_err(|_| {
        CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            "S3 policy generation is too large",
        )
        .with_retryable(false)
    })?)
    .bind(as_i64(record.created_at_unix_ms)?)
    .bind(as_i64(record.updated_at_unix_ms)?)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        if is_unique(&error) {
            id_reused("S3 Access Point identity is already used")
        } else {
            storage_error(error)
        }
    })?;
    Ok(())
}

async fn insert_s3_credential_row(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &S3CredentialRecord,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO s3_credential_records \
         (credential_id, access_point_id, access_key_id, encrypted_secret, state, \
          expires_at_unix_ms, created_at_unix_ms, last_used_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(record.credential_id.as_str())
    .bind(record.access_point_id.as_str())
    .bind(&record.access_key_id)
    .bind(&record.encrypted_secret)
    .bind(s3_credential_state_name(record.state))
    .bind(as_i64(record.expires_at_unix_ms)?)
    .bind(as_i64(record.created_at_unix_ms)?)
    .bind(record.last_used_at_unix_ms.map(as_i64).transpose()?)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        if is_unique(&error) {
            id_reused("S3 credential identity is already used")
        } else {
            storage_error(error)
        }
    })?;
    Ok(())
}

async fn insert_s3_mutation_row(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &S3MutationRecord,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO s3_mutation_records \
         (tenant_id, request_id, operation, request_digest, created_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(record.tenant_id.as_str())
    .bind(record.request_id.as_str())
    .bind(s3_mutation_kind_name(record.operation))
    .bind(record.request_digest.as_bytes().as_slice())
    .bind(as_i64(record.created_at_unix_ms)?)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        if is_unique(&error) {
            id_reused("S3 request identity is already used")
        } else {
            storage_error(error)
        }
    })?;
    Ok(())
}

fn ensure_same_s3_mutation(
    existing: &S3MutationRecord,
    requested: &S3MutationRecord,
) -> CentralResult<()> {
    if same_s3_mutation_identity(existing, requested) {
        Ok(())
    } else {
        Err(id_reused("S3 request identity is already used"))
    }
}

async fn get_artifact_by_id(
    store: &SqliteAgentRegistryStore,
    tenant_id: &TenantId,
    artifact_id: &ArtifactId,
) -> CentralResult<Option<ArtifactRecord>> {
    let sql = format!(
        "SELECT {ARTIFACT_COLUMNS} FROM artifact_catalog_records \
         WHERE tenant_id = ? AND artifact_id = ?"
    );
    sqlx::query(&sql)
        .bind(tenant_id.as_str())
        .bind(artifact_id.as_str())
        .fetch_optional(&store.pool)
        .await
        .map_err(storage_error)?
        .map(decode_artifact)
        .transpose()
}

/// Keeps enrollment approval and the public Volume authority in the same SQLite transaction.
pub(super) async fn sync_volume_from_registry(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &AgentRegistryRecord,
) -> CentralResult<()> {
    if record.enrollment.state != neoengram_domain::protocol::AgentEnrollmentState::Approved {
        return Ok(());
    }
    let Some(descriptor) = record.storage_enrollment.descriptor.as_ref() else {
        return Ok(());
    };
    let tenant_id = record.enrollment.tenant_id.as_str();
    let volume_id = record.enrollment.storage_volume_id.as_str();
    let existing = sqlx::query(
        "SELECT display_name, edge_cluster_id, region, backend_type, access_mode, pvc_namespace, \
         pvc_claim_name, state, enrollment_id, resource_version, lifecycle_state, \
         created_at_unix_ms \
         FROM storage_volume_catalog_records WHERE tenant_id = ? AND storage_volume_id = ?",
    )
    .bind(tenant_id)
    .bind(volume_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let updated_at = record
        .storage_enrollment
        .updated_at_unix_ms
        .or(record.enrollment.decided_at_unix_ms)
        .unwrap_or(record.enrollment.created_at_unix_ms);
    let state = match record.derived_volume_state(updated_at, 30_000) {
        DerivedVolumeState::Ready => StorageVolumeState::Ready,
        DerivedVolumeState::Degraded => StorageVolumeState::Degraded,
        DerivedVolumeState::Unavailable => StorageVolumeState::Unavailable,
    };
    let access_mode = match descriptor.access_mode {
        StorageEnrollmentAccessMode::ReadWriteOnce => StorageAccessMode::ReadWriteOnce,
        StorageEnrollmentAccessMode::ReadWriteMany => StorageAccessMode::ReadWriteMany,
    };
    if let Some(row) = existing {
        if row
            .try_get::<String, _>("lifecycle_state")
            .map_err(storage_error)?
            != "active"
        {
            return Err(id_reused(
                "StorageVolume lifecycle does not allow enrollment updates",
            ));
        }
        let exact_descriptor = row
            .try_get::<String, _>("display_name")
            .map_err(storage_error)?
            == descriptor.display_name
            && row
                .try_get::<String, _>("edge_cluster_id")
                .map_err(storage_error)?
                == record.enrollment.edge_cluster_id.as_str()
            && row.try_get::<String, _>("region").map_err(storage_error)? == descriptor.region
            && row
                .try_get::<String, _>("backend_type")
                .map_err(storage_error)?
                == "pvc"
            && row
                .try_get::<String, _>("access_mode")
                .map_err(storage_error)?
                == access_mode_name(access_mode)
            && row
                .try_get::<Option<String>, _>("pvc_namespace")
                .map_err(storage_error)?
                == Some(descriptor.pvc_reference.namespace.clone())
            && row
                .try_get::<Option<String>, _>("pvc_claim_name")
                .map_err(storage_error)?
                == Some(descriptor.pvc_reference.claim_name.clone());
        if !exact_descriptor {
            return Err(CentralError::new(
                CentralErrorCode::VolumeOwnerConflict,
                "approved enrollment differs from the registered StorageVolume descriptor",
            )
            .with_retryable(false));
        }
        let old_state = row.try_get::<String, _>("state").map_err(storage_error)?;
        let old_enrollment = row
            .try_get::<Option<String>, _>("enrollment_id")
            .map_err(storage_error)?;
        if record.enrollment.replaces_enrollment_id.is_none()
            && old_enrollment.is_none()
            && old_state != "unavailable"
        {
            return Err(CentralError::new(
                CentralErrorCode::VolumeOwnerConflict,
                "initial enrollment can only bind an unavailable StorageVolume",
            )
            .with_retryable(false));
        }
        let new_state = volume_state_name(state);
        let enrollment_id = record.enrollment.enrollment_id.as_str();
        if old_state != new_state || old_enrollment.as_deref() != Some(enrollment_id) {
            let old_version = parse_u64(
                row.try_get::<String, _>("resource_version")
                    .map_err(storage_error)?,
                "StorageVolume resource version",
            )?;
            let new_version = old_version.checked_add(1).ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "StorageVolume resource version exhausted",
                )
                .with_retryable(false)
            })?;
            sqlx::query(
                "UPDATE storage_volume_catalog_records SET state = ?, enrollment_id = ?, \
                 resource_version = ?, updated_at_unix_ms = ? \
                 WHERE tenant_id = ? AND storage_volume_id = ? AND resource_version = ? \
                   AND lifecycle_state = 'active'",
            )
            .bind(new_state)
            .bind(enrollment_id)
            .bind(new_version.to_string())
            .bind(as_i64(updated_at)?)
            .bind(tenant_id)
            .bind(volume_id)
            .bind(old_version.to_string())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        }
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO storage_volume_catalog_records \
         (tenant_id, storage_volume_id, display_name, edge_cluster_id, region, backend_type, \
          access_mode, pvc_namespace, pvc_claim_name, nfs_server, nfs_export_path, state, \
          enrollment_id, resource_version, created_at_unix_ms, updated_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?, 'pvc', ?, ?, ?, NULL, NULL, ?, ?, '1', ?, ?)",
    )
    .bind(tenant_id)
    .bind(volume_id)
    .bind(&descriptor.display_name)
    .bind(record.enrollment.edge_cluster_id.as_str())
    .bind(&descriptor.region)
    .bind(access_mode_name(access_mode))
    .bind(&descriptor.pvc_reference.namespace)
    .bind(&descriptor.pvc_reference.claim_name)
    .bind(volume_state_name(state))
    .bind(record.enrollment.enrollment_id.as_str())
    .bind(as_i64(updated_at)?)
    .bind(as_i64(updated_at)?)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        if is_unique(&error) {
            CentralError::new(
                CentralErrorCode::VolumeOwnerConflict,
                "approved PVC identity is already registered to another StorageVolume",
            )
            .with_retryable(false)
        } else {
            storage_error(error)
        }
    })?;
    Ok(())
}

fn tenant_page(rows: Vec<SqliteRow>, limit: u16) -> CentralResult<TenantListPage> {
    let mut records = rows
        .into_iter()
        .map(decode_tenant)
        .collect::<CentralResult<Vec<_>>>()?;
    let has_more = records.len() > usize::from(limit);
    records.truncate(usize::from(limit));
    let next = has_more.then(|| {
        let last = records
            .last()
            .expect("a non-zero page with more rows has a cursor");
        TenantListCursor {
            created_at_unix_ms: last.created_at_unix_ms,
            tenant_id: last.tenant_id.clone(),
        }
    });
    Ok(TenantListPage { records, next })
}

fn project_page(rows: Vec<SqliteRow>, limit: u16) -> CentralResult<ProjectListPage> {
    let mut records = rows
        .into_iter()
        .map(decode_project)
        .collect::<CentralResult<Vec<_>>>()?;
    let has_more = records.len() > usize::from(limit);
    records.truncate(usize::from(limit));
    let next = has_more.then(|| {
        let last = records
            .last()
            .expect("a non-zero project page has a cursor");
        ProjectListCursor {
            created_at_unix_ms: last.created_at_unix_ms,
            project_id: last.project_id.clone(),
        }
    });
    Ok(ProjectListPage { records, next })
}

fn artifact_page(rows: Vec<SqliteRow>, limit: u16) -> CentralResult<ArtifactListPage> {
    let mut records = rows
        .into_iter()
        .map(decode_artifact)
        .collect::<CentralResult<Vec<_>>>()?;
    let has_more = records.len() > usize::from(limit);
    records.truncate(usize::from(limit));
    let next = has_more.then(|| {
        let last = records
            .last()
            .expect("a non-zero page with more rows has a cursor");
        ArtifactListCursor {
            created_at_unix_ms: last.created_at_unix_ms,
            project_id: last.project_id.clone(),
            artifact_id: last.artifact_id.clone(),
        }
    });
    Ok(ArtifactListPage { records, next })
}

fn volume_page(rows: Vec<SqliteRow>, limit: u16) -> CentralResult<StorageVolumeListPage> {
    let mut records = rows
        .into_iter()
        .map(decode_volume)
        .collect::<CentralResult<Vec<_>>>()?;
    let has_more = records.len() > usize::from(limit);
    records.truncate(usize::from(limit));
    let next = has_more.then(|| {
        let last = records
            .last()
            .expect("a non-zero page with more rows has a cursor");
        StorageVolumeListCursor {
            created_at_unix_ms: last.created_at_unix_ms,
            storage_volume_id: last.storage_volume_id.clone(),
        }
    });
    Ok(StorageVolumeListPage { records, next })
}

fn playground_page(rows: Vec<SqliteRow>, limit: u16) -> CentralResult<PlaygroundListPage> {
    let mut records = rows
        .into_iter()
        .map(decode_playground)
        .collect::<CentralResult<Vec<_>>>()?;
    let has_more = records.len() > usize::from(limit);
    records.truncate(usize::from(limit));
    let next = has_more.then(|| {
        let last = records
            .last()
            .expect("a non-zero page with more rows has a cursor");
        PlaygroundListCursor {
            created_at_unix_ms: last.created_at_unix_ms,
            project_id: last.project_id.clone(),
            artifact_id: last.artifact_id.clone(),
            playground_id: last.playground_id.clone(),
        }
    });
    Ok(PlaygroundListPage { records, next })
}

/// Legacy transaction helper retained for adapters that need to compose the lower-level insert.
#[allow(dead_code)]
fn snapshot_page(rows: Vec<SqliteRow>, limit: u16) -> CentralResult<SnapshotListPage> {
    let mut records = rows
        .into_iter()
        .map(decode_snapshot)
        .collect::<CentralResult<Vec<_>>>()?;
    let has_more = records.len() > usize::from(limit);
    records.truncate(usize::from(limit));
    let next = has_more.then(|| {
        let last = records
            .last()
            .expect("a non-zero page with more rows has a cursor");
        SnapshotListCursor {
            created_at_unix_ms: last.created_at_unix_ms,
            snapshot_id: last.snapshot_id.clone(),
        }
    });
    Ok(SnapshotListPage { records, next })
}

fn decode_tenant(row: SqliteRow) -> CentralResult<TenantRecord> {
    Ok(TenantRecord {
        tenant_id: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| corruption(format!("stored Tenant ID is invalid: {error}")))?,
        display_name: row.try_get("display_name").map_err(storage_error)?,
        description: row.try_get("description").map_err(storage_error)?,
        resource_version: parse_u64(
            row.try_get("resource_version").map_err(storage_error)?,
            "Tenant resource version",
        )?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn decode_project(row: SqliteRow) -> CentralResult<ProjectRecord> {
    Ok(ProjectRecord {
        tenant_id: parse_id(
            row.try_get("tenant_id").map_err(storage_error)?,
            TenantId::new,
        )?,
        project_id: parse_id(
            row.try_get("project_id").map_err(storage_error)?,
            ProjectId::new,
        )?,
        display_name: row.try_get("display_name").map_err(storage_error)?,
        description: row.try_get("description").map_err(storage_error)?,
        resource_version: parse_u64(
            row.try_get("resource_version").map_err(storage_error)?,
            "Project resource version",
        )?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn decode_artifact(row: SqliteRow) -> CentralResult<ArtifactRecord> {
    let mode: String = row.try_get("initialization_mode").map_err(storage_error)?;
    let source_project_id: Option<String> =
        row.try_get("source_project_id").map_err(storage_error)?;
    let source_artifact_id: Option<String> =
        row.try_get("source_artifact_id").map_err(storage_error)?;
    let source_commit_digest: Option<Vec<u8>> =
        row.try_get("source_commit_digest").map_err(storage_error)?;
    let initialization = match (
        mode.as_str(),
        source_project_id,
        source_artifact_id,
        source_commit_digest,
    ) {
        ("empty", None, None, None) => ArtifactInitialization::Empty,
        ("derived", Some(project_id), Some(artifact_id), Some(commit_digest)) => {
            ArtifactInitialization::Derived {
                source_project_id: parse_id(project_id, ProjectId::new)?,
                source_artifact_id: parse_id(artifact_id, ArtifactId::new)?,
                source_commit_id: exact_digest(commit_digest, "source Commit digest")?,
            }
        }
        _ => return Err(corruption("stored Artifact initialization is invalid")),
    };
    Ok(ArtifactRecord {
        tenant_id: parse_id(
            row.try_get("tenant_id").map_err(storage_error)?,
            TenantId::new,
        )?,
        project_id: parse_id(
            row.try_get("project_id").map_err(storage_error)?,
            ProjectId::new,
        )?,
        artifact_id: parse_id(
            row.try_get("artifact_id").map_err(storage_error)?,
            ArtifactId::new,
        )?,
        display_name: row.try_get("display_name").map_err(storage_error)?,
        description: row.try_get("description").map_err(storage_error)?,
        initialization,
        head_commit_id: optional_digest(
            row.try_get("head_commit_digest").map_err(storage_error)?,
            "head Commit digest",
        )?,
        resource_version: parse_u64(
            row.try_get("resource_version").map_err(storage_error)?,
            "Artifact resource version",
        )?,
        lifecycle: decode_resource_lifecycle(&row)?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn decode_volume(row: SqliteRow) -> CentralResult<StorageVolumeRecord> {
    let backend_type = parse_backend(row.try_get("backend_type").map_err(storage_error)?)?;
    let pvc_namespace: Option<String> = row.try_get("pvc_namespace").map_err(storage_error)?;
    let pvc_claim_name: Option<String> = row.try_get("pvc_claim_name").map_err(storage_error)?;
    let nfs_server: Option<String> = row.try_get("nfs_server").map_err(storage_error)?;
    let nfs_export_path: Option<String> = row.try_get("nfs_export_path").map_err(storage_error)?;
    let allowed_delivery_modes: Vec<SnapshotDeliveryMode> = serde_json::from_str(
        &row.try_get::<String, _>("allowed_delivery_modes")
            .map_err(storage_error)?,
    )
    .map_err(|error| corruption(format!("stored delivery mode policy is invalid: {error}")))?;
    let hardlink_policy =
        parse_hardlink_policy(row.try_get("hardlink_policy").map_err(storage_error)?)?;
    let max_whole_file_bytes = neoengram_domain::protocol::DecimalU64::new(parse_u64(
        row.try_get("max_whole_file_bytes").map_err(storage_error)?,
        "StorageVolume max whole-file bytes",
    )?);
    let copy_reserve_bytes = neoengram_domain::protocol::DecimalU64::new(parse_u64(
        row.try_get("copy_reserve_bytes").map_err(storage_error)?,
        "StorageVolume copy reserve bytes",
    )?);
    let record = StorageVolumeRecord {
        tenant_id: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| corruption(format!("stored Tenant ID is invalid: {error}")))?,
        storage_volume_id: StorageVolumeId::new(
            row.try_get::<String, _>("storage_volume_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| corruption(format!("stored StorageVolume ID is invalid: {error}")))?,
        display_name: row.try_get("display_name").map_err(storage_error)?,
        edge_cluster_id: EdgeClusterId::new(
            row.try_get::<String, _>("edge_cluster_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| corruption(format!("stored EdgeCluster ID is invalid: {error}")))?,
        region: row.try_get("region").map_err(storage_error)?,
        backend_type,
        access_mode: parse_access_mode(row.try_get("access_mode").map_err(storage_error)?)?,
        allowed_delivery_modes,
        hardlink_policy,
        max_whole_file_bytes,
        copy_reserve_bytes,
        pvc_reference: match (pvc_namespace, pvc_claim_name) {
            (Some(namespace), Some(claim_name)) => Some(CatalogPvcReference {
                namespace,
                claim_name,
            }),
            (None, None) => None,
            _ => return Err(corruption("stored PVC locator is incomplete")),
        },
        nfs_reference: match (nfs_server, nfs_export_path) {
            (Some(server), Some(export_path)) => Some(CatalogNfsReference {
                server,
                export_path,
            }),
            (None, None) => None,
            _ => return Err(corruption("stored NFS locator is incomplete")),
        },
        state: parse_volume_state(row.try_get("state").map_err(storage_error)?)?,
        resource_version: parse_u64(
            row.try_get("resource_version").map_err(storage_error)?,
            "StorageVolume resource version",
        )?,
        lifecycle: decode_resource_lifecycle(&row)?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    };
    validate_volume_shape(&record)?;
    Ok(record)
}

fn decode_playground(row: SqliteRow) -> CentralResult<PlaygroundRecord> {
    Ok(PlaygroundRecord {
        tenant_id: parse_id(
            row.try_get("tenant_id").map_err(storage_error)?,
            TenantId::new,
        )?,
        project_id: parse_id(
            row.try_get("project_id").map_err(storage_error)?,
            ProjectId::new,
        )?,
        artifact_id: parse_id(
            row.try_get("artifact_id").map_err(storage_error)?,
            ArtifactId::new,
        )?,
        playground_id: parse_id(
            row.try_get("playground_id").map_err(storage_error)?,
            PlaygroundId::new,
        )?,
        storage_volume_id: parse_id(
            row.try_get("storage_volume_id").map_err(storage_error)?,
            StorageVolumeId::new,
        )?,
        region: row.try_get("region").map_err(storage_error)?,
        display_name: row.try_get("display_name").map_err(storage_error)?,
        base_commit_id: optional_digest(
            row.try_get("base_commit_digest").map_err(storage_error)?,
            "base Commit digest",
        )?,
        head_commit_id: optional_digest(
            row.try_get("head_commit_digest").map_err(storage_error)?,
            "head Commit digest",
        )?,
        state: parse_playground_state(row.try_get("state").map_err(storage_error)?)?,
        relative_root: row.try_get("relative_root").map_err(storage_error)?,
        resource_version: parse_u64(
            row.try_get("resource_version").map_err(storage_error)?,
            "Playground resource version",
        )?,
        lifecycle: decode_resource_lifecycle(&row)?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn decode_snapshot(row: SqliteRow) -> CentralResult<SnapshotRecord> {
    Ok(SnapshotRecord {
        tenant_id: parse_id(
            row.try_get("tenant_id").map_err(storage_error)?,
            TenantId::new,
        )?,
        project_id: parse_id(
            row.try_get("project_id").map_err(storage_error)?,
            ProjectId::new,
        )?,
        artifact_id: parse_id(
            row.try_get("artifact_id").map_err(storage_error)?,
            ArtifactId::new,
        )?,
        snapshot_id: parse_id(
            row.try_get("snapshot_id").map_err(storage_error)?,
            SnapshotId::new,
        )?,
        snapshot_request_id: parse_id(
            row.try_get("snapshot_request_id").map_err(storage_error)?,
            RequestId::new,
        )?,
        commit_id: exact_digest(
            row.try_get("commit_digest").map_err(storage_error)?,
            "Snapshot Commit digest",
        )?,
        delivery_id: parse_id(
            row.try_get("delivery_id").map_err(storage_error)?,
            SnapshotDeliveryId::new,
        )?,
        edge_cluster_id: parse_id(
            row.try_get("edge_cluster_id").map_err(storage_error)?,
            EdgeClusterId::new,
        )?,
        storage_volume_id: parse_id(
            row.try_get("storage_volume_id").map_err(storage_error)?,
            StorageVolumeId::new,
        )?,
        delivery_mode: parse_snapshot_delivery_mode(
            row.try_get("delivery_mode").map_err(storage_error)?,
        )?,
        state: parse_snapshot_state(row.try_get("state").map_err(storage_error)?)?,
        resource_version: parse_u64(
            row.try_get("resource_version").map_err(storage_error)?,
            "Snapshot resource version",
        )?,
        lifecycle: decode_resource_lifecycle(&row)?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn decode_snapshot_delivery(row: SqliteRow) -> CentralResult<SnapshotDeliveryRecord> {
    let file_count = u64::try_from(row.try_get::<i64, _>("file_count").map_err(storage_error)?)
        .map_err(|_| corruption("stored Snapshot delivery file_count is negative"))?;
    let size_bytes = u64::try_from(row.try_get::<i64, _>("size_bytes").map_err(storage_error)?)
        .map_err(|_| corruption("stored Snapshot delivery size_bytes is negative"))?;
    Ok(SnapshotDeliveryRecord {
        tenant_id: parse_id(
            row.try_get("tenant_id").map_err(storage_error)?,
            TenantId::new,
        )?,
        delivery_id: parse_id(
            row.try_get("delivery_id").map_err(storage_error)?,
            SnapshotDeliveryId::new,
        )?,
        create_request_id: parse_id(
            row.try_get("create_request_id").map_err(storage_error)?,
            RequestId::new,
        )?,
        snapshot_id: parse_id(
            row.try_get("snapshot_id").map_err(storage_error)?,
            SnapshotId::new,
        )?,
        commit_id: exact_digest(
            row.try_get("commit_digest").map_err(storage_error)?,
            "Snapshot delivery Commit digest",
        )?,
        storage_volume_id: parse_id(
            row.try_get("storage_volume_id").map_err(storage_error)?,
            StorageVolumeId::new,
        )?,
        mode: parse_snapshot_delivery_mode(row.try_get("mode").map_err(storage_error)?)?,
        target_relative_root: neoengram_domain::core::LogicalPath::parse(
            row.try_get::<String, _>("target_relative_root")
                .map_err(storage_error)?,
        )
        .map_err(|error| {
            corruption(format!("stored Snapshot delivery path is invalid: {error}"))
        })?,
        state: parse_snapshot_delivery_state(row.try_get("state").map_err(storage_error)?)?,
        source_index_digest: exact_digest(
            row.try_get("source_index_digest").map_err(storage_error)?,
            "Snapshot delivery Index digest",
        )?,
        delivery_generation: neoengram_domain::protocol::DeliveryGeneration::new(parse_u64(
            row.try_get("delivery_generation").map_err(storage_error)?,
            "Snapshot delivery generation",
        )?),
        file_count,
        size_bytes,
        object_set_digest: exact_digest(
            row.try_get("object_set_digest").map_err(storage_error)?,
            "Snapshot delivery object set digest",
        )?,
        resource_version: parse_u64(
            row.try_get("resource_version").map_err(storage_error)?,
            "Snapshot delivery resource version",
        )?,
        issue_code: row.try_get("issue_code").map_err(storage_error)?,
        issue_message: row.try_get("issue_message").map_err(storage_error)?,
        issue_retryable: row
            .try_get::<i64, _>("issue_retryable")
            .map_err(storage_error)?
            != 0,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn decode_snapshot_delivery_mutation(
    row: SqliteRow,
) -> CentralResult<SnapshotDeliveryMutationRecord> {
    let tenant_id = parse_id(
        row.try_get("tenant_id").map_err(storage_error)?,
        TenantId::new,
    )?;
    let request_id = parse_id(
        row.try_get("request_id").map_err(storage_error)?,
        RequestId::new,
    )?;
    let delivery_id = parse_id(
        row.try_get("delivery_id").map_err(storage_error)?,
        SnapshotDeliveryId::new,
    )?;
    let operation: String = row.try_get("operation").map_err(storage_error)?;
    let request_digest = exact_digest(
        row.try_get("request_digest").map_err(storage_error)?,
        "SnapshotDelivery mutation request digest",
    )?;
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let mutation: SnapshotDeliveryMutationRecord =
        decode_lifecycle_payload(&payload, "SnapshotDelivery mutation")?;
    if mutation.tenant_id != tenant_id
        || mutation.request_id != request_id
        || mutation.delivery_id != delivery_id
        || snapshot_delivery_mutation_kind_name(mutation.kind) != operation
        || mutation.request_digest != request_digest
    {
        return Err(corruption(
            "SnapshotDelivery mutation payload differs from indexed columns",
        ));
    }
    Ok(mutation)
}

fn decode_resource_lifecycle(row: &SqliteRow) -> CentralResult<ResourceLifecycle> {
    let active_deletion_id = row
        .try_get::<Option<String>, _>("active_deletion_id")
        .map_err(storage_error)?
        .map(|value| parse_id(value, DeletionId::new))
        .transpose()?;
    Ok(ResourceLifecycle {
        state: parse_resource_lifecycle_state(
            row.try_get("lifecycle_state").map_err(storage_error)?,
        )?,
        generation: neoengram_domain::protocol::LifecycleGeneration::new(parse_u64(
            row.try_get("lifecycle_generation").map_err(storage_error)?,
            "resource lifecycle generation",
        )?),
        active_deletion_id,
        delete_requested_at_unix_ms: optional_unix_ms(
            row.try_get("delete_requested_at_unix_ms")
                .map_err(storage_error)?,
        )?,
        purge_after_unix_ms: optional_unix_ms(
            row.try_get("purge_after_unix_ms").map_err(storage_error)?,
        )?,
        deleted_at_unix_ms: optional_unix_ms(
            row.try_get("deleted_at_unix_ms").map_err(storage_error)?,
        )?,
    })
}

fn optional_unix_ms(value: Option<i64>) -> CentralResult<Option<UnixMillis>> {
    value.map(unix_ms).transpose()
}

fn decode_s3_access_point(row: SqliteRow) -> CentralResult<S3AccessPointRecord> {
    let policy_generation: i64 = row.try_get("policy_generation").map_err(storage_error)?;
    let policy_generation = u64::try_from(policy_generation)
        .map_err(|_| corruption("stored S3 policy generation is negative"))?;
    Ok(S3AccessPointRecord {
        access_point_id: parse_id(
            row.try_get("access_point_id").map_err(storage_error)?,
            S3AccessPointId::new,
        )?,
        tenant_id: parse_id(
            row.try_get("tenant_id").map_err(storage_error)?,
            TenantId::new,
        )?,
        project_id: parse_id(
            row.try_get("project_id").map_err(storage_error)?,
            ProjectId::new,
        )?,
        artifact_id: parse_id(
            row.try_get("artifact_id").map_err(storage_error)?,
            ArtifactId::new,
        )?,
        snapshot_id: parse_id(
            row.try_get("snapshot_id").map_err(storage_error)?,
            SnapshotId::new,
        )?,
        commit_id: exact_digest(
            row.try_get("commit_digest").map_err(storage_error)?,
            "S3 Access Point Commit digest",
        )?,
        delivery_id: parse_id(
            row.try_get("delivery_id").map_err(storage_error)?,
            SnapshotDeliveryId::new,
        )?,
        storage_volume_id: parse_id(
            row.try_get("storage_volume_id").map_err(storage_error)?,
            StorageVolumeId::new,
        )?,
        edge_cluster_id: parse_id(
            row.try_get("edge_cluster_id").map_err(storage_error)?,
            EdgeClusterId::new,
        )?,
        bucket_name: row.try_get("bucket_name").map_err(storage_error)?,
        state: parse_s3_access_point_state(row.try_get("state").map_err(storage_error)?)?,
        policy_generation,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn decode_s3_credential(row: SqliteRow) -> CentralResult<S3CredentialRecord> {
    Ok(S3CredentialRecord {
        credential_id: parse_id(
            row.try_get("credential_id").map_err(storage_error)?,
            S3CredentialId::new,
        )?,
        access_point_id: parse_id(
            row.try_get("access_point_id").map_err(storage_error)?,
            S3AccessPointId::new,
        )?,
        access_key_id: row.try_get("access_key_id").map_err(storage_error)?,
        encrypted_secret: row.try_get("encrypted_secret").map_err(storage_error)?,
        state: parse_s3_credential_state(row.try_get("state").map_err(storage_error)?)?,
        expires_at_unix_ms: unix_ms(row.try_get("expires_at_unix_ms").map_err(storage_error)?)?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        last_used_at_unix_ms: row
            .try_get::<Option<i64>, _>("last_used_at_unix_ms")
            .map_err(storage_error)?
            .map(unix_ms)
            .transpose()?,
    })
}

fn decode_s3_mutation(row: SqliteRow) -> CentralResult<S3MutationRecord> {
    Ok(S3MutationRecord {
        tenant_id: parse_id(
            row.try_get("tenant_id").map_err(storage_error)?,
            TenantId::new,
        )?,
        request_id: parse_id(
            row.try_get("request_id").map_err(storage_error)?,
            RequestId::new,
        )?,
        operation: parse_s3_mutation_kind(row.try_get("operation").map_err(storage_error)?)?,
        request_digest: exact_digest(
            row.try_get("request_digest").map_err(storage_error)?,
            "S3 mutation request digest",
        )?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
    })
}

fn parse_id<T, E>(value: String, parse: impl FnOnce(String) -> Result<T, E>) -> CentralResult<T>
where
    E: std::fmt::Display,
{
    parse(value)
        .map_err(|error| corruption(format!("stored catalog identifier is invalid: {error}")))
}

fn validate_volume_shape(record: &StorageVolumeRecord) -> CentralResult<()> {
    let valid = match record.backend_type {
        StorageBackendType::Pvc => record.pvc_reference.is_some() && record.nfs_reference.is_none(),
        StorageBackendType::Nfs => record.pvc_reference.is_none() && record.nfs_reference.is_some(),
    };
    if valid {
        Ok(())
    } else {
        Err(CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            "StorageVolume backend locator does not match backend type",
        )
        .with_retryable(false))
    }
}

fn tenant_create_matches(left: &TenantRecord, right: &TenantRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.display_name == right.display_name
        && left.description == right.description
}

fn project_create_matches(left: &ProjectRecord, right: &ProjectRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.project_id == right.project_id
        && left.display_name == right.display_name
        && left.description == right.description
}

fn artifact_create_matches(left: &ArtifactRecord, right: &ArtifactRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.project_id == right.project_id
        && left.artifact_id == right.artifact_id
        && left.display_name == right.display_name
        && left.description == right.description
        && left.initialization == right.initialization
}

fn volume_create_matches(left: &StorageVolumeRecord, right: &StorageVolumeRecord) -> bool {
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

fn playground_create_matches_insert(
    existing: &PlaygroundRecord,
    requested: &PlaygroundRecord,
    artifact_head: &ArtifactHeadExpectation,
) -> bool {
    let commit_selection_matches = match artifact_head {
        ArtifactHeadExpectation::Any => {
            existing.base_commit_id == requested.base_commit_id
                && existing.head_commit_id == requested.head_commit_id
        }
        ArtifactHeadExpectation::Exact(_) => true,
    };
    existing.tenant_id == requested.tenant_id
        && existing.project_id == requested.project_id
        && existing.artifact_id == requested.artifact_id
        && existing.playground_id == requested.playground_id
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

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn backend_name(value: StorageBackendType) -> &'static str {
    match value {
        StorageBackendType::Pvc => "pvc",
        StorageBackendType::Nfs => "nfs",
    }
}

fn parse_backend(value: String) -> CentralResult<StorageBackendType> {
    match value.as_str() {
        "pvc" => Ok(StorageBackendType::Pvc),
        "nfs" => Ok(StorageBackendType::Nfs),
        _ => Err(corruption("stored StorageVolume backend type is invalid")),
    }
}

fn access_mode_name(value: StorageAccessMode) -> &'static str {
    match value {
        StorageAccessMode::ReadWriteOnce => "read_write_once",
        StorageAccessMode::ReadWriteMany => "read_write_many",
        StorageAccessMode::ReadOnlyMany => "read_only_many",
    }
}

fn parse_access_mode(value: String) -> CentralResult<StorageAccessMode> {
    match value.as_str() {
        "read_write_once" => Ok(StorageAccessMode::ReadWriteOnce),
        "read_write_many" => Ok(StorageAccessMode::ReadWriteMany),
        "read_only_many" => Ok(StorageAccessMode::ReadOnlyMany),
        _ => Err(corruption("stored StorageVolume access mode is invalid")),
    }
}

fn hardlink_policy_name(value: HardlinkPolicy) -> &'static str {
    match value {
        HardlinkPolicy::Disabled => "disabled",
        HardlinkPolicy::SealedAcl => "sealed_acl",
        HardlinkPolicy::TrustedLocal => "trusted_local",
    }
}

fn parse_hardlink_policy(value: String) -> CentralResult<HardlinkPolicy> {
    match value.as_str() {
        "disabled" => Ok(HardlinkPolicy::Disabled),
        "sealed_acl" => Ok(HardlinkPolicy::SealedAcl),
        "trusted_local" => Ok(HardlinkPolicy::TrustedLocal),
        _ => Err(corruption(
            "stored StorageVolume hardlink policy is invalid",
        )),
    }
}

fn volume_state_name(value: StorageVolumeState) -> &'static str {
    match value {
        StorageVolumeState::Ready => "ready",
        StorageVolumeState::Degraded => "degraded",
        StorageVolumeState::Unavailable => "unavailable",
    }
}

fn parse_volume_state(value: String) -> CentralResult<StorageVolumeState> {
    match value.as_str() {
        "ready" => Ok(StorageVolumeState::Ready),
        "degraded" => Ok(StorageVolumeState::Degraded),
        "unavailable" => Ok(StorageVolumeState::Unavailable),
        _ => Err(corruption("stored StorageVolume state is invalid")),
    }
}

fn playground_state_name(value: PlaygroundState) -> &'static str {
    match value {
        PlaygroundState::Creating => "creating",
        PlaygroundState::Ready => "ready",
        PlaygroundState::Abnormal => "abnormal",
    }
}

fn parse_playground_state(value: String) -> CentralResult<PlaygroundState> {
    match value.as_str() {
        "creating" => Ok(PlaygroundState::Creating),
        "ready" => Ok(PlaygroundState::Ready),
        "abnormal" => Ok(PlaygroundState::Abnormal),
        _ => Err(corruption("stored Playground state is invalid")),
    }
}

fn snapshot_state_name(value: SnapshotState) -> &'static str {
    match value {
        SnapshotState::Creating => "creating",
        SnapshotState::Ready => "ready",
        SnapshotState::Abnormal => "abnormal",
    }
}

fn parse_snapshot_state(value: String) -> CentralResult<SnapshotState> {
    match value.as_str() {
        "creating" => Ok(SnapshotState::Creating),
        "ready" => Ok(SnapshotState::Ready),
        "abnormal" => Ok(SnapshotState::Abnormal),
        _ => Err(corruption("stored Snapshot state is invalid")),
    }
}

fn snapshot_delivery_mode_name(value: SnapshotDeliveryMode) -> &'static str {
    match value {
        SnapshotDeliveryMode::Fuse => "fuse",
        SnapshotDeliveryMode::Copy => "copy",
        SnapshotDeliveryMode::Hardlink => "hardlink",
    }
}

fn snapshot_delivery_mutation_kind_name(value: SnapshotDeliveryMutationKind) -> &'static str {
    match value {
        SnapshotDeliveryMutationKind::Retry => "retry",
        SnapshotDeliveryMutationKind::Delete => "delete",
    }
}

fn parse_snapshot_delivery_mode(value: String) -> CentralResult<SnapshotDeliveryMode> {
    match value.as_str() {
        "fuse" => Ok(SnapshotDeliveryMode::Fuse),
        "copy" => Ok(SnapshotDeliveryMode::Copy),
        "hardlink" => Ok(SnapshotDeliveryMode::Hardlink),
        _ => Err(corruption("stored Snapshot delivery mode is invalid")),
    }
}

fn snapshot_delivery_state_name(value: SnapshotDeliveryState) -> &'static str {
    match value {
        SnapshotDeliveryState::Requested => "requested",
        SnapshotDeliveryState::Validating => "validating",
        SnapshotDeliveryState::Materializing => "materializing",
        SnapshotDeliveryState::Ready => "ready",
        SnapshotDeliveryState::Failed => "failed",
        SnapshotDeliveryState::Deleting => "deleting",
        SnapshotDeliveryState::Deleted => "deleted",
    }
}

fn parse_snapshot_delivery_state(value: String) -> CentralResult<SnapshotDeliveryState> {
    match value.as_str() {
        "requested" => Ok(SnapshotDeliveryState::Requested),
        "validating" => Ok(SnapshotDeliveryState::Validating),
        "materializing" => Ok(SnapshotDeliveryState::Materializing),
        "ready" => Ok(SnapshotDeliveryState::Ready),
        "failed" => Ok(SnapshotDeliveryState::Failed),
        "deleting" => Ok(SnapshotDeliveryState::Deleting),
        "deleted" => Ok(SnapshotDeliveryState::Deleted),
        _ => Err(corruption("stored Snapshot delivery state is invalid")),
    }
}

fn s3_access_point_state_name(value: S3AccessPointState) -> &'static str {
    match value {
        S3AccessPointState::Active => "active",
        S3AccessPointState::Disabled => "disabled",
    }
}

fn parse_s3_access_point_state(value: String) -> CentralResult<S3AccessPointState> {
    match value.as_str() {
        "active" => Ok(S3AccessPointState::Active),
        "disabled" => Ok(S3AccessPointState::Disabled),
        _ => Err(corruption("stored S3 Access Point state is invalid")),
    }
}

fn s3_credential_state_name(value: S3CredentialState) -> &'static str {
    match value {
        S3CredentialState::Active => "active",
        S3CredentialState::Revoked => "revoked",
        S3CredentialState::Expired => "expired",
    }
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

fn same_snapshot_delivery_mutation_identity(
    existing: &SnapshotDeliveryMutationRecord,
    requested: &SnapshotDeliveryMutationRequest,
) -> bool {
    existing.tenant_id == requested.tenant_id
        && existing.request_id == requested.request_id
        && existing.delivery_id == requested.delivery_id
        && existing.kind == requested.kind
        && existing.request_digest == requested.request_digest
}

fn s3_mutation_kind_name(value: S3MutationKind) -> &'static str {
    match value {
        S3MutationKind::AccessPointCreate => "access_point_create",
        S3MutationKind::AccessPointEnable => "access_point_enable",
        S3MutationKind::AccessPointDisable => "access_point_disable",
        S3MutationKind::CredentialCreate => "credential_create",
        S3MutationKind::CredentialRevoke => "credential_revoke",
    }
}

fn parse_s3_mutation_kind(value: String) -> CentralResult<S3MutationKind> {
    match value.as_str() {
        "access_point_create" => Ok(S3MutationKind::AccessPointCreate),
        "access_point_enable" => Ok(S3MutationKind::AccessPointEnable),
        "access_point_disable" => Ok(S3MutationKind::AccessPointDisable),
        "credential_create" => Ok(S3MutationKind::CredentialCreate),
        "credential_revoke" => Ok(S3MutationKind::CredentialRevoke),
        _ => Err(corruption("stored S3 mutation kind is invalid")),
    }
}

fn parse_s3_credential_state(value: String) -> CentralResult<S3CredentialState> {
    match value.as_str() {
        "active" => Ok(S3CredentialState::Active),
        "revoked" => Ok(S3CredentialState::Revoked),
        "expired" => Ok(S3CredentialState::Expired),
        _ => Err(corruption("stored S3 credential state is invalid")),
    }
}

fn resource_lifecycle_state_name(value: ResourceLifecycleState) -> &'static str {
    match value {
        ResourceLifecycleState::Active => "active",
        ResourceLifecycleState::PendingDelete => "pending_delete",
        ResourceLifecycleState::Deleting => "deleting",
        ResourceLifecycleState::Restoring => "restoring",
        ResourceLifecycleState::Deleted => "deleted",
    }
}

fn parse_resource_lifecycle_state(value: String) -> CentralResult<ResourceLifecycleState> {
    match value.as_str() {
        "active" => Ok(ResourceLifecycleState::Active),
        "pending_delete" => Ok(ResourceLifecycleState::PendingDelete),
        "deleting" => Ok(ResourceLifecycleState::Deleting),
        "restoring" => Ok(ResourceLifecycleState::Restoring),
        "deleted" => Ok(ResourceLifecycleState::Deleted),
        _ => Err(corruption("stored resource lifecycle state is invalid")),
    }
}

fn parse_u64(value: String, field: &str) -> CentralResult<u64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(corruption(format!(
            "stored {field} is not canonical decimal u64"
        )));
    }
    value
        .parse()
        .map_err(|_| corruption(format!("stored {field} exceeds u64")))
}

fn unix_ms(value: i64) -> CentralResult<UnixMillis> {
    u64::try_from(value)
        .map(UnixMillis::new)
        .map_err(|_| corruption("stored UnixMillis is negative"))
}

fn as_i64(value: UnixMillis) -> CentralResult<i64> {
    i64::try_from(value.get()).map_err(|_| {
        CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            "UnixMillis exceeds the SQLite range",
        )
        .with_retryable(false)
    })
}

fn optional_as_i64(value: Option<UnixMillis>) -> CentralResult<Option<i64>> {
    value.map(as_i64).transpose()
}

fn optional_digest(value: Option<Vec<u8>>, field: &str) -> CentralResult<Option<ContentDigest>> {
    value.map(|bytes| exact_digest(bytes, field)).transpose()
}

fn exact_digest(value: Vec<u8>, field: &str) -> CentralResult<ContentDigest> {
    value
        .try_into()
        .map(ContentDigest::from_bytes)
        .map_err(|_| corruption(format!("stored {field} is not exactly 32 bytes")))
}

fn is_unique(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

fn id_reused(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(false)
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

fn catalog_parent_error(code: CentralErrorCode, message: &'static str) -> CentralError {
    CentralError::new(code, message).with_retryable(false)
}

fn artifact_head_changed() -> CentralError {
    CentralError::new(
        CentralErrorCode::ArtifactHeadMismatch,
        "Artifact Head changed before Playground creation",
    )
    .with_retryable(true)
}

fn storage_error(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(
        CentralErrorCode::StorageFailure,
        format!("SQLite control catalog storage operation failed: {error}"),
    )
}

fn corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}
