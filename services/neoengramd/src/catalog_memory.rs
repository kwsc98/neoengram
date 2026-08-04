use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use neoengram_protocol::{ArtifactId, PlaygroundId, ProjectId, StorageVolumeId, TenantId};

use crate::{
    CatalogInsertOutcome, CentralError, CentralErrorCode, CentralResult, ControlCatalogRepository,
    PlaygroundListCursor, PlaygroundListPage, PlaygroundListRequest, PlaygroundRecord,
    StorageBackendType, StorageVolumeListCursor, StorageVolumeListPage, StorageVolumeListRequest,
    StorageVolumeRecord, TenantListCursor, TenantListPage, TenantListRequest, TenantRecord,
};

#[derive(Default)]
pub struct InMemoryControlCatalog {
    tenants: Mutex<BTreeMap<TenantId, TenantRecord>>,
    volumes: Mutex<BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>>,
    playgrounds: Mutex<BTreeMap<(TenantId, ProjectId, ArtifactId, PlaygroundId), PlaygroundRecord>>,
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

    async fn get_storage_volume(
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

    async fn get_playground(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &PlaygroundId,
    ) -> CentralResult<Option<PlaygroundRecord>> {
        Ok(lock(&self.playgrounds)?
            .get(&(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                playground_id.clone(),
            ))
            .cloned())
    }

    async fn list_playgrounds(
        &self,
        request: &PlaygroundListRequest,
    ) -> CentralResult<PlaygroundListPage> {
        validate_limit(request.limit)?;
        let mut records = lock(&self.playgrounds)?
            .values()
            .filter(|record| record.tenant_id == request.tenant_id)
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
                    &record.playground_id.to_string(),
                    &record.display_name,
                    request.query.as_deref(),
                )
            })
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| playground_after(record, after))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.project_id.cmp(&right.project_id))
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
                .then_with(|| left.playground_id.cmp(&right.playground_id))
        });
        let has_more = records.len() > usize::from(request.limit);
        records.truncate(usize::from(request.limit));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty keyset page");
            PlaygroundListCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                project_id: last.project_id.clone(),
                artifact_id: last.artifact_id.clone(),
                playground_id: last.playground_id.clone(),
            }
        });
        Ok(PlaygroundListPage { records, next })
    }

    async fn insert_playground(
        &self,
        record: PlaygroundRecord,
    ) -> CentralResult<CatalogInsertOutcome<PlaygroundRecord>> {
        if !lock(&self.volumes)?
            .contains_key(&(record.tenant_id.clone(), record.storage_volume_id.clone()))
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "Playground StorageVolume does not exist",
            )
            .with_retryable(false));
        }
        let mut records = lock(&self.playgrounds)?;
        let key = (
            record.tenant_id.clone(),
            record.project_id.clone(),
            record.artifact_id.clone(),
            record.playground_id.clone(),
        );
        if let Some(existing) = records.get(&key) {
            return if playground_matches(existing, &record) {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("Playground ID is already used"))
            };
        }
        records.insert(key, record.clone());
        Ok(CatalogInsertOutcome::Inserted(record))
    }
}

fn tenant_after(record: &TenantRecord, after: &TenantListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && record.tenant_id > after.tenant_id)
}

fn volume_after(record: &StorageVolumeRecord, after: &StorageVolumeListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && record.storage_volume_id > after.storage_volume_id)
}

fn playground_after(record: &PlaygroundRecord, after: &PlaygroundListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && (
                &record.project_id,
                &record.artifact_id,
                &record.playground_id,
            ) > (&after.project_id, &after.artifact_id, &after.playground_id))
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

fn playground_matches(left: &PlaygroundRecord, right: &PlaygroundRecord) -> bool {
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

fn conflict(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(false)
}

fn lock<T>(mutex: &Mutex<T>) -> CentralResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        CentralError::new(
            CentralErrorCode::Internal,
            "in-memory catalog lock poisoned",
        )
    })
}
