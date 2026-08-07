use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use neoengram_protocol::{
    ArtifactId, PlaygroundId, ProjectId, SnapshotId, StorageVolumeId, TenantId,
};

use crate::{
    AdvancePlaygroundCommitOutcome, AdvancePlaygroundCommitRequest, ArtifactHeadExpectation,
    ArtifactInitialization, ArtifactListCursor, ArtifactListPage, ArtifactListRequest,
    ArtifactRecord, CatalogInsertOutcome, CentralError, CentralErrorCode, CentralResult,
    ControlCatalogRepository, PlaygroundInsertRequest, PlaygroundListCursor, PlaygroundListPage,
    PlaygroundListRequest, PlaygroundRecord, SnapshotInsertOutcome, SnapshotInsertRequest,
    SnapshotListCursor, SnapshotListPage, SnapshotListRequest, SnapshotPhase, SnapshotRecord,
    SnapshotState, StorageBackendType, StorageVolumeListCursor, StorageVolumeListPage,
    StorageVolumeListRequest, StorageVolumeRecord, StorageVolumeState, TenantListCursor,
    TenantListPage, TenantListRequest, TenantRecord,
};

#[derive(Default)]
pub struct InMemoryControlCatalog {
    tenants: Mutex<BTreeMap<TenantId, TenantRecord>>,
    artifacts: Mutex<BTreeMap<(TenantId, ArtifactId), ArtifactRecord>>,
    volumes: Mutex<BTreeMap<(TenantId, StorageVolumeId), StorageVolumeRecord>>,
    playgrounds: Mutex<BTreeMap<(TenantId, ProjectId, ArtifactId, PlaygroundId), PlaygroundRecord>>,
    snapshots: Mutex<BTreeMap<(TenantId, SnapshotId), SnapshotRecord>>,
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

    async fn get_artifact(
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
            if records
                .get(&source_key)
                .is_none_or(|source| &source.project_id != source_project_id)
            {
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

    async fn insert_playground_fenced(
        &self,
        request: PlaygroundInsertRequest,
    ) -> CentralResult<CatalogInsertOutcome<PlaygroundRecord>> {
        let PlaygroundInsertRequest {
            record,
            artifact_head,
        } = request;
        // Keep this order fixed so validation and insertion form one atomic critical section.
        let artifacts = lock(&self.artifacts)?;
        let volumes = lock(&self.volumes)?;
        let mut records = lock(&self.playgrounds)?;
        let key = (
            record.tenant_id.clone(),
            record.project_id.clone(),
            record.artifact_id.clone(),
            record.playground_id.clone(),
        );
        if let Some(existing) = records.get(&key) {
            return if playground_matches_insert(existing, &record, &artifact_head) {
                Ok(CatalogInsertOutcome::Existing(existing.clone()))
            } else {
                Err(conflict("Playground ID is already used"))
            };
        }
        let artifact = artifacts
            .get(&(record.tenant_id.clone(), record.artifact_id.clone()))
            .filter(|artifact| artifact.project_id == record.project_id)
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Playground Artifact does not exist",
                )
            })?;
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
        let volume = volumes
            .get(&(record.tenant_id.clone(), record.storage_volume_id.clone()))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::StorageVolumeNotFound,
                    "Playground StorageVolume does not exist",
                )
            })?;
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
        records.insert(key, record.clone());
        Ok(CatalogInsertOutcome::Inserted(record))
    }

    async fn transition_playground_state(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &PlaygroundId,
        expected: crate::PlaygroundState,
        next: crate::PlaygroundState,
        updated_at_unix_ms: neoengram_protocol::UnixMillis,
    ) -> CentralResult<PlaygroundRecord> {
        let key = (
            tenant_id.clone(),
            project_id.clone(),
            artifact_id.clone(),
            playground_id.clone(),
        );
        let mut records = lock(&self.playgrounds)?;
        let record = records.get_mut(&key).ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "Playground does not exist",
            )
        })?;
        if record.state == next {
            return Ok(record.clone());
        }
        if record.state != expected {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Playground is in {:?}, expected {:?} for state transition",
                    record.state, expected
                ),
            ));
        }
        record.state = next;
        record.updated_at_unix_ms = updated_at_unix_ms;
        Ok(record.clone())
    }

    async fn advance_playground_commit(
        &self,
        request: AdvancePlaygroundCommitRequest,
    ) -> CentralResult<AdvancePlaygroundCommitOutcome> {
        let artifact_key = (request.tenant_id.clone(), request.artifact_id.clone());
        let playground_key = (
            request.tenant_id.clone(),
            request.project_id.clone(),
            request.artifact_id.clone(),
            request.playground_id.clone(),
        );
        let mut artifacts = lock(&self.artifacts)?;
        let mut playgrounds = lock(&self.playgrounds)?;
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
        let playground = playgrounds.get(&playground_key).cloned().ok_or_else(|| {
            catalog_parent_error(
                CentralErrorCode::ArtifactNotFound,
                "Commit Playground does not exist",
            )
        })?;
        if artifact.head_commit_id == Some(request.commit_id)
            && playground.head_commit_id == Some(request.commit_id)
        {
            return Ok(AdvancePlaygroundCommitOutcome {
                artifact,
                playground,
                replayed: true,
            });
        }
        if artifact.head_commit_id != request.expected_head_commit_id
            || playground.head_commit_id != request.expected_head_commit_id
        {
            return Err(catalog_parent_error(
                CentralErrorCode::ArtifactHeadMismatch,
                "Artifact or Playground Head changed after Pre-commit",
            ));
        }
        if playground.state != crate::PlaygroundState::Ready {
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
        let artifact = artifacts
            .get_mut(&artifact_key)
            .expect("Artifact was validated while holding the catalog lock");
        artifact.head_commit_id = Some(request.commit_id);
        artifact.resource_version = next_resource_version;
        artifact.updated_at_unix_ms = request.updated_at_unix_ms;
        let artifact = artifact.clone();
        let playground = playgrounds
            .get_mut(&playground_key)
            .expect("Playground was validated while holding the catalog lock");
        playground.head_commit_id = Some(request.commit_id);
        playground.updated_at_unix_ms = request.updated_at_unix_ms;
        let playground = playground.clone();
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
            .filter(|record| {
                request
                    .storage_volume_id
                    .as_ref()
                    .is_none_or(|id| &record.storage_volume_id == id)
            })
            .filter(|record| {
                request
                    .region
                    .as_ref()
                    .is_none_or(|region| &record.region == region)
            })
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

    async fn insert_snapshot_fenced(
        &self,
        request: SnapshotInsertRequest,
    ) -> CentralResult<SnapshotInsertOutcome> {
        let SnapshotInsertRequest {
            record,
            artifact_head,
        } = request;
        let artifacts = lock(&self.artifacts)?;
        let volumes = lock(&self.volumes)?;
        let mut snapshots = lock(&self.snapshots)?;

        if let Some(existing) = snapshots.values().find(|existing| {
            existing.tenant_id == record.tenant_id
                && existing.snapshot_request_id == record.snapshot_request_id
        }) {
            return if snapshot_request_matches(existing, &record) {
                Ok(SnapshotInsertOutcome::ExistingRequest(existing.clone()))
            } else {
                Err(conflict("Snapshot request ID is already used"))
            };
        }
        if snapshots.contains_key(&(record.tenant_id.clone(), record.snapshot_id.clone())) {
            return Err(conflict("Snapshot ID is already used"));
        }
        if let Some(existing) = snapshots.values().find(|existing| {
            existing.tenant_id == record.tenant_id
                && existing.project_id == record.project_id
                && existing.artifact_id == record.artifact_id
                && existing.commit_id == record.commit_id
                && existing.storage_volume_id == record.storage_volume_id
        }) {
            return Ok(SnapshotInsertOutcome::ExistingPlacement(existing.clone()));
        }
        let artifact = artifacts
            .get(&(record.tenant_id.clone(), record.artifact_id.clone()))
            .filter(|artifact| artifact.project_id == record.project_id)
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Snapshot Artifact does not exist",
                )
            })?;
        if let ArtifactHeadExpectation::Exact(expected) = artifact_head {
            if expected != Some(record.commit_id) {
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
        let volume = volumes
            .get(&(record.tenant_id.clone(), record.storage_volume_id.clone()))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::StorageVolumeNotFound,
                    "Snapshot StorageVolume does not exist",
                )
            })?;
        if volume.state != StorageVolumeState::Ready {
            return Err(catalog_parent_error(
                CentralErrorCode::StorageVolumeNotReady,
                "Snapshot StorageVolume is not ready",
            ));
        }
        if record.region != volume.region {
            return Err(catalog_parent_error(
                CentralErrorCode::StorageVolumeRegionMismatch,
                "Snapshot region must match the StorageVolume region",
            ));
        }
        snapshots.insert(
            (record.tenant_id.clone(), record.snapshot_id.clone()),
            record.clone(),
        );
        Ok(SnapshotInsertOutcome::Inserted(record))
    }

    async fn transition_snapshot_state(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &SnapshotId,
        expected_state: SnapshotState,
        next_state: SnapshotState,
        next_phase: SnapshotPhase,
        updated_at_unix_ms: neoengram_protocol::UnixMillis,
    ) -> CentralResult<SnapshotRecord> {
        let mut records = lock(&self.snapshots)?;
        let record = records
            .get_mut(&(tenant_id.clone(), snapshot_id.clone()))
            .ok_or_else(|| {
                catalog_parent_error(
                    CentralErrorCode::ArtifactNotFound,
                    "Snapshot does not exist",
                )
            })?;
        if record.state == next_state && record.phase == next_phase {
            if updated_at_unix_ms > record.updated_at_unix_ms {
                record.updated_at_unix_ms = updated_at_unix_ms;
            }
            return Ok(record.clone());
        }
        if record.state != expected_state {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Snapshot is in {:?}, expected {:?} for state transition",
                    record.state, expected_state
                ),
            ));
        }
        record.state = next_state;
        record.phase = next_phase;
        record.updated_at_unix_ms = updated_at_unix_ms;
        Ok(record.clone())
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

fn artifact_after(record: &ArtifactRecord, after: &ArtifactListCursor) -> bool {
    record.created_at_unix_ms < after.created_at_unix_ms
        || (record.created_at_unix_ms == after.created_at_unix_ms
            && (&record.project_id, &record.artifact_id) > (&after.project_id, &after.artifact_id))
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

fn playground_matches_insert(
    existing: &PlaygroundRecord,
    requested: &PlaygroundRecord,
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
        && existing.playground_id == requested.playground_id
        && existing.storage_volume_id == requested.storage_volume_id
        && existing.region == requested.region
        && existing.display_name == requested.display_name
        && existing.relative_root == requested.relative_root
        && commit_selection_matches
}

fn snapshot_request_matches(existing: &SnapshotRecord, requested: &SnapshotRecord) -> bool {
    existing.tenant_id == requested.tenant_id
        && existing.project_id == requested.project_id
        && existing.artifact_id == requested.artifact_id
        && existing.snapshot_request_id == requested.snapshot_request_id
        && existing.commit_id == requested.commit_id
        && existing.storage_volume_id == requested.storage_volume_id
        && existing.region == requested.region
        && existing.relative_root == requested.relative_root
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

fn lock<T>(mutex: &Mutex<T>) -> CentralResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        CentralError::new(
            CentralErrorCode::Internal,
            "in-memory catalog lock poisoned",
        )
    })
}
