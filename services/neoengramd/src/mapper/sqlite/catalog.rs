use async_trait::async_trait;
use neoengram_core::ContentDigest;
use neoengram_protocol::{
    ArtifactId, EdgeClusterId, IndexRevision, PlaygroundId, ProjectId, StorageVolumeId, TenantId,
    UnixMillis, WireIndexVersion,
};
use sqlx::{sqlite::SqliteRow, QueryBuilder, Row, Sqlite, Transaction};

use crate::{
    AgentRegistryRecord, CatalogInsertOutcome, CatalogNfsReference, CatalogPvcReference,
    CentralError, CentralErrorCode, CentralResult, ControlCatalogRepository, DerivedVolumeState,
    PlaygroundListCursor, PlaygroundListPage, PlaygroundListRequest, PlaygroundRecord,
    PlaygroundState, StorageAccessMode, StorageBackendType, StorageEnrollmentAccessMode,
    StorageVolumeListCursor, StorageVolumeListPage, StorageVolumeListRequest, StorageVolumeRecord,
    StorageVolumeState, TenantListCursor, TenantListPage, TenantListRequest, TenantRecord,
};

use super::agent_registry::SqliteAgentRegistryStore;

const TENANT_COLUMNS: &str =
    "tenant_id, display_name, description, resource_version, created_at_unix_ms, updated_at_unix_ms";
const VOLUME_COLUMNS: &str =
    "tenant_id, storage_volume_id, display_name, edge_cluster_id, region, \
    backend_type, access_mode, pvc_namespace, pvc_claim_name, nfs_server, nfs_export_path, state, \
    resource_version, created_at_unix_ms, updated_at_unix_ms";
const PLAYGROUND_COLUMNS: &str = "tenant_id, project_id, artifact_id, playground_id, \
    storage_volume_id, region, display_name, base_commit_digest, head_commit_digest, \
    index_revision, index_digest, state, relative_root, created_at_unix_ms, updated_at_unix_ms";

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

    async fn get_storage_volume(
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
              access_mode, pvc_namespace, pvc_claim_name, nfs_server, nfs_export_path, state, \
              enrollment_id, resource_version, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.storage_volume_id.as_str())
        .bind(&record.display_name)
        .bind(record.edge_cluster_id.as_str())
        .bind(&record.region)
        .bind(backend_name(record.backend_type))
        .bind(access_mode_name(record.access_mode))
        .bind(pvc_namespace)
        .bind(pvc_claim_name)
        .bind(nfs_server)
        .bind(nfs_export_path)
        .bind(volume_state_name(record.state))
        .bind(record.resource_version.to_string())
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

    async fn insert_playground(
        &self,
        record: PlaygroundRecord,
    ) -> CentralResult<CatalogInsertOutcome<PlaygroundRecord>> {
        if let Some(existing) = self
            .get_playground(
                &record.tenant_id,
                &record.project_id,
                &record.artifact_id,
                &record.playground_id,
            )
            .await?
        {
            return if playground_create_matches(&existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing))
            } else {
                Err(id_reused(
                    "Playground ID is already bound to another create request",
                ))
            };
        }
        let result = sqlx::query(
            "INSERT INTO playground_catalog_records \
             (tenant_id, project_id, artifact_id, playground_id, storage_volume_id, region, \
              display_name, base_commit_digest, head_commit_digest, index_revision, index_digest, \
              state, relative_root, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .bind(record.index_version.revision.get().to_string())
        .bind(record.index_version.digest.as_bytes().as_slice())
        .bind(playground_state_name(record.state))
        .bind(&record.relative_root)
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(CatalogInsertOutcome::Inserted(record)),
            Err(error) if is_unique(&error) => {
                let existing = self
                    .get_playground(
                        &record.tenant_id,
                        &record.project_id,
                        &record.artifact_id,
                        &record.playground_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        storage_error("Playground uniqueness conflict could not be resolved")
                    })?;
                if playground_create_matches(&existing, &record) {
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
}

/// Keeps enrollment approval and the public Volume authority in the same SQLite transaction.
pub(super) async fn sync_volume_from_registry(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &AgentRegistryRecord,
) -> CentralResult<()> {
    if record.enrollment.state != neoengram_protocol::AgentEnrollmentState::Approved {
        return Ok(());
    }
    let Some(descriptor) = record.storage_enrollment.descriptor.as_ref() else {
        return Ok(());
    };
    let tenant_id = record.enrollment.tenant_id.as_str();
    let volume_id = record.enrollment.storage_volume_id.as_str();
    let existing = sqlx::query(
        "SELECT display_name, edge_cluster_id, region, backend_type, access_mode, pvc_namespace, \
         pvc_claim_name, state, enrollment_id, resource_version, created_at_unix_ms \
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
                 WHERE tenant_id = ? AND storage_volume_id = ? AND resource_version = ?",
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

fn decode_volume(row: SqliteRow) -> CentralResult<StorageVolumeRecord> {
    let backend_type = parse_backend(row.try_get("backend_type").map_err(storage_error)?)?;
    let pvc_namespace: Option<String> = row.try_get("pvc_namespace").map_err(storage_error)?;
    let pvc_claim_name: Option<String> = row.try_get("pvc_claim_name").map_err(storage_error)?;
    let nfs_server: Option<String> = row.try_get("nfs_server").map_err(storage_error)?;
    let nfs_export_path: Option<String> = row.try_get("nfs_export_path").map_err(storage_error)?;
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
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
    };
    validate_volume_shape(&record)?;
    Ok(record)
}

fn decode_playground(row: SqliteRow) -> CentralResult<PlaygroundRecord> {
    let digest = exact_digest(
        row.try_get("index_digest").map_err(storage_error)?,
        "Index digest",
    )?;
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
        index_version: WireIndexVersion {
            revision: IndexRevision::new(parse_u64(
                row.try_get("index_revision").map_err(storage_error)?,
                "Index revision",
            )?),
            digest,
            extensions: Default::default(),
        },
        state: parse_playground_state(row.try_get("state").map_err(storage_error)?)?,
        relative_root: row.try_get("relative_root").map_err(storage_error)?,
        created_at_unix_ms: unix_ms(row.try_get("created_at_unix_ms").map_err(storage_error)?)?,
        updated_at_unix_ms: unix_ms(row.try_get("updated_at_unix_ms").map_err(storage_error)?)?,
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

fn playground_create_matches(left: &PlaygroundRecord, right: &PlaygroundRecord) -> bool {
    left.tenant_id == right.tenant_id
        && left.project_id == right.project_id
        && left.artifact_id == right.artifact_id
        && left.playground_id == right.playground_id
        && left.storage_volume_id == right.storage_volume_id
        && left.display_name == right.display_name
        && left.base_commit_id == right.base_commit_id
        && left.relative_root == right.relative_root
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

fn storage_error(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(
        CentralErrorCode::StorageFailure,
        format!("SQLite control catalog storage operation failed: {error}"),
    )
}

fn corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}
