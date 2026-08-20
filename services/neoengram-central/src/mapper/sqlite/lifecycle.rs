use std::collections::BTreeSet;

use async_trait::async_trait;
use neoengram_domain::protocol::{DecimalU64, JobState, ResourceRef, ResourceVersion, TenantId};
use sqlx::{Row, Sqlite, Transaction};

use super::authority::{decode, encode, storage_corruption, storage_error, SqliteAuthorityStore};
use crate::{
    AuthorityLifecycleAction, AuthorityLifecycleImpact, AuthorityLifecycleMutationOutcome,
    AuthorityLifecycleRecord, AuthorityLifecycleRepository, AuthorityLifecycleRequest,
    CentralError, CentralErrorCode, CentralResult, JobOperation, JobRecord, PreCommitRecord,
    PreCommitState,
};

#[async_trait]
impl AuthorityLifecycleRepository for SqliteAuthorityStore {
    async fn impact(
        &self,
        tenant_id: &TenantId,
        target: &ResourceRef,
    ) -> CentralResult<AuthorityLifecycleImpact> {
        let job_rows = sqlx::query("SELECT payload FROM control_jobs WHERE tenant_id = ?")
            .bind(tenant_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        let active_job_count = job_rows
            .into_iter()
            .map(|row| {
                let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
                decode::<JobRecord>(&payload)
            })
            .collect::<CentralResult<Vec<_>>>()?
            .into_iter()
            .filter(|job| !job.state.is_terminal() && job_matches_target(job, target))
            .count();

        let placement_rows = sqlx::query(
            "SELECT artifact_id, storage_volume_id, object_id, size \
             FROM object_placements WHERE tenant_id = ?",
        )
        .bind(tenant_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut objects = BTreeSet::new();
        let mut estimated_bytes = 0_u64;
        for row in placement_rows {
            let artifact_id: String = row.try_get("artifact_id").map_err(storage_error)?;
            let storage_volume_id: String =
                row.try_get("storage_volume_id").map_err(storage_error)?;
            let matches = match target {
                ResourceRef::StorageVolume {
                    storage_volume_id: expected,
                } => storage_volume_id == expected.as_str(),
                ResourceRef::Artifact {
                    artifact_id: expected,
                    ..
                } => artifact_id == expected.as_str(),
                ResourceRef::Playground { .. } | ResourceRef::Snapshot { .. } => false,
            };
            if !matches {
                continue;
            }
            let object_id: Vec<u8> = row.try_get("object_id").map_err(storage_error)?;
            if objects.insert(object_id) {
                let size: String = row.try_get("size").map_err(storage_error)?;
                let size = size.parse::<u64>().map_err(|_| {
                    CentralError::new(
                        CentralErrorCode::StorageFailure,
                        "object placement size is outside the supported range",
                    )
                    .with_retryable(false)
                })?;
                estimated_bytes = estimated_bytes.checked_add(size).ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::StorageFailure,
                        "lifecycle byte estimate overflow",
                    )
                    .with_retryable(false)
                })?;
            }
        }
        Ok(AuthorityLifecycleImpact {
            active_job_count: DecimalU64::new(u64::try_from(active_job_count).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::StorageFailure,
                    "active Job count exceeds the supported range",
                )
                .with_retryable(false)
            })?),
            estimated_file_count: DecimalU64::new(u64::try_from(objects.len()).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::StorageFailure,
                    "lifecycle object count exceeds the supported range",
                )
                .with_retryable(false)
            })?),
            estimated_bytes: DecimalU64::new(estimated_bytes),
        })
    }

    async fn quiesce(
        &self,
        request: AuthorityLifecycleRequest,
    ) -> CentralResult<AuthorityLifecycleMutationOutcome> {
        apply_lifecycle(self, request, AuthorityLifecycleAction::Quiesce).await
    }

    async fn finalize(
        &self,
        request: AuthorityLifecycleRequest,
    ) -> CentralResult<AuthorityLifecycleMutationOutcome> {
        apply_lifecycle(self, request, AuthorityLifecycleAction::Finalize).await
    }

    async fn get(
        &self,
        tenant_id: &TenantId,
        deletion_id: &neoengram_domain::protocol::DeletionId,
        target: &ResourceRef,
        action: AuthorityLifecycleAction,
    ) -> CentralResult<Option<AuthorityLifecycleRecord>> {
        let (target_kind, target_id) = target_identity(target);
        let row = sqlx::query(
            "SELECT target_kind, action, request_digest, resource_generation, payload \
             FROM lifecycle_cleanup_records \
             WHERE tenant_id = ? AND deletion_id = ? AND target_id = ? AND action = ?",
        )
        .bind(tenant_id.as_str())
        .bind(deletion_id.as_str())
        .bind(&target_id)
        .bind(action_name(action))
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(|row| decode_record(row, tenant_id, deletion_id, target_kind, target, action))
            .transpose()
    }
}

async fn apply_lifecycle(
    store: &SqliteAuthorityStore,
    request: AuthorityLifecycleRequest,
    action: AuthorityLifecycleAction,
) -> CentralResult<AuthorityLifecycleMutationOutcome> {
    let (target_kind, target_id) = target_identity(&request.target);
    let mut transaction = store.pool.begin().await.map_err(storage_error)?;
    if let Some(existing) = load_record(
        &mut transaction,
        &request.tenant_id,
        &request.deletion_id,
        &request.target,
        action,
    )
    .await?
    {
        let expected = AuthorityLifecycleRecord {
            tenant_id: request.tenant_id,
            deletion_id: request.deletion_id,
            target: request.target,
            action,
            lifecycle_generation: request.lifecycle_generation,
            request_digest: request.request_digest,
            completed_at_unix_ms: existing.completed_at_unix_ms,
        };
        if existing != expected {
            return Err(conflict(
                "Authority lifecycle identity is already bound to another request",
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        return Ok(AuthorityLifecycleMutationOutcome {
            record: existing,
            replayed: true,
        });
    }

    match action {
        AuthorityLifecycleAction::Quiesce => {
            quiesce_metadata(&mut transaction, &request).await?;
        }
        AuthorityLifecycleAction::Finalize => {
            finalize_metadata(&mut transaction, &request).await?;
        }
    }

    let record = AuthorityLifecycleRecord {
        tenant_id: request.tenant_id.clone(),
        deletion_id: request.deletion_id.clone(),
        target: request.target,
        action,
        lifecycle_generation: request.lifecycle_generation,
        request_digest: request.request_digest,
        completed_at_unix_ms: request.occurred_at_unix_ms,
    };
    sqlx::query(
        "INSERT INTO lifecycle_cleanup_records \
         (tenant_id, deletion_id, target_id, target_kind, action, state, request_digest, \
          resource_generation, payload, created_at_unix_ms, updated_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?)",
    )
    .bind(record.tenant_id.as_str())
    .bind(record.deletion_id.as_str())
    .bind(target_id)
    .bind(target_kind)
    .bind(action_name(action))
    .bind(record.request_digest.as_bytes().as_slice())
    .bind(record.lifecycle_generation.to_string())
    .bind(encode(&record)?)
    .bind(as_i64(record.completed_at_unix_ms.get())?)
    .bind(as_i64(record.completed_at_unix_ms.get())?)
    .execute(&mut *transaction)
    .await
    .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)?;
    Ok(AuthorityLifecycleMutationOutcome {
        record,
        replayed: false,
    })
}

async fn load_record(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    deletion_id: &neoengram_domain::protocol::DeletionId,
    target: &ResourceRef,
    action: AuthorityLifecycleAction,
) -> CentralResult<Option<AuthorityLifecycleRecord>> {
    let (target_kind, target_id) = target_identity(target);
    let row = sqlx::query(
        "SELECT target_kind, action, request_digest, resource_generation, payload \
         FROM lifecycle_cleanup_records \
         WHERE tenant_id = ? AND deletion_id = ? AND target_id = ? AND action = ?",
    )
    .bind(tenant_id.as_str())
    .bind(deletion_id.as_str())
    .bind(target_id)
    .bind(action_name(action))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    row.map(|row| decode_record(row, tenant_id, deletion_id, target_kind, target, action))
        .transpose()
}

fn decode_record(
    row: sqlx::sqlite::SqliteRow,
    tenant_id: &TenantId,
    deletion_id: &neoengram_domain::protocol::DeletionId,
    target_kind: &str,
    target: &ResourceRef,
    action: AuthorityLifecycleAction,
) -> CentralResult<AuthorityLifecycleRecord> {
    let stored_kind: String = row.try_get("target_kind").map_err(storage_error)?;
    let stored_action: String = row.try_get("action").map_err(storage_error)?;
    let stored_digest: Vec<u8> = row.try_get("request_digest").map_err(storage_error)?;
    let stored_generation: String = row.try_get("resource_generation").map_err(storage_error)?;
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let record: AuthorityLifecycleRecord = decode(&payload)?;
    if stored_kind != target_kind
        || stored_action != action_name(action)
        || stored_digest.as_slice() != record.request_digest.as_bytes()
        || stored_generation != record.lifecycle_generation.to_string()
        || &record.tenant_id != tenant_id
        || &record.deletion_id != deletion_id
        || &record.target != target
        || record.action != action
    {
        return Err(storage_corruption(
            "Authority lifecycle payload differs from its indexed columns",
        ));
    }
    Ok(record)
}

async fn quiesce_metadata(
    transaction: &mut Transaction<'_, Sqlite>,
    request: &AuthorityLifecycleRequest,
) -> CentralResult<()> {
    let jobs = matching_jobs(transaction, &request.tenant_id, &request.target).await?;
    for mut job in jobs {
        if job.state.is_terminal() {
            continue;
        }
        let previous_version = job.resource_version.get();
        job.resource_version =
            ResourceVersion::new(previous_version.checked_add(1).ok_or_else(|| {
                conflict("Job resource version exhausted during lifecycle quiesce")
            })?);
        job.state = JobState::Cancelled;
        let result = sqlx::query(
            "UPDATE control_jobs SET state = ?, resource_version = ?, payload = ? \
             WHERE tenant_id = ? AND job_id = ? AND resource_version = ?",
        )
        .bind(job_state_name(job.state))
        .bind(job.resource_version.to_string())
        .bind(encode(&job)?)
        .bind(job.spec.tenant_id.as_str())
        .bind(job.spec.job_id.as_str())
        .bind(previous_version.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() != 1 {
            return Err(conflict("Job changed during lifecycle quiesce"));
        }
        sqlx::query(
            "UPDATE assignment_outbox SET published = 1, retired = 1 \
             WHERE tenant_id = ? AND job_id = ?",
        )
        .bind(job.spec.tenant_id.as_str())
        .bind(job.spec.job_id.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    }

    let rows = sqlx::query(
        "SELECT payload FROM precommit_records WHERE tenant_id = ? \
         AND state IN ('running', 'ready', 'abnormal')",
    )
    .bind(request.tenant_id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(storage_error)?;
    for row in rows {
        let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
        let mut record: PreCommitRecord = decode(&payload)?;
        if !precommit_matches_target(&record, &request.target) {
            continue;
        }
        let previous_version = record.resource_version.get();
        record.resource_version =
            ResourceVersion::new(previous_version.checked_add(1).ok_or_else(|| {
                conflict("Pre-commit resource version exhausted during lifecycle quiesce")
            })?);
        record.state = PreCommitState::Cancelled;
        record.phase = crate::PreCommitPhase::Idle;
        record.updated_at_unix_ms = request.occurred_at_unix_ms;
        let result = sqlx::query(
            "UPDATE precommit_records SET state = 'cancelled', resource_version = ?, payload = ? \
             WHERE tenant_id = ? AND precommit_id = ? AND resource_version = ?",
        )
        .bind(record.resource_version.to_string())
        .bind(encode(&record)?)
        .bind(record.tenant_id.as_str())
        .bind(record.precommit_id.as_str())
        .bind(previous_version.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() != 1 {
            return Err(conflict("Pre-commit changed during lifecycle quiesce"));
        }
    }
    Ok(())
}

async fn finalize_metadata(
    transaction: &mut Transaction<'_, Sqlite>,
    request: &AuthorityLifecycleRequest,
) -> CentralResult<()> {
    if let ResourceRef::StorageVolume { storage_volume_id } = &request.target {
        let blockers: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT candidate.artifact_id FROM object_placements AS candidate \
             WHERE candidate.tenant_id = ? AND candidate.storage_volume_id = ? \
               AND NOT EXISTS ( \
                   SELECT 1 FROM object_placements AS replica \
                   WHERE replica.tenant_id = candidate.tenant_id \
                     AND replica.artifact_id = candidate.artifact_id \
                     AND replica.object_id = candidate.object_id \
                     AND replica.size = candidate.size \
                     AND replica.storage_volume_id <> candidate.storage_volume_id \
               ) ORDER BY candidate.artifact_id",
        )
        .bind(request.tenant_id.as_str())
        .bind(storage_volume_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if !blockers.is_empty() {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                format!(
                    "StorageVolume contains unique object replicas for retained Artifacts: {}",
                    blockers.join(", ")
                ),
            )
            .with_retryable(false));
        }
    }

    delete_matching_jobs(transaction, &request.tenant_id, &request.target).await?;
    match &request.target {
        ResourceRef::Snapshot { .. } => {}
        ResourceRef::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => {
            sqlx::query(
                "DELETE FROM precommit_mutations WHERE tenant_id = ? AND precommit_id IN ( \
                     SELECT precommit_id FROM precommit_records WHERE tenant_id = ? \
                       AND project_id = ? AND artifact_id = ? AND playground_id = ? \
                       AND state <> 'committed' \
                 )",
            )
            .bind(request.tenant_id.as_str())
            .bind(request.tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .bind(playground_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query(
                "DELETE FROM precommit_records WHERE tenant_id = ? AND project_id = ? \
                 AND artifact_id = ? AND playground_id = ? AND state <> 'committed'",
            )
            .bind(request.tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .bind(playground_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query(
                "DELETE FROM playground_indexes WHERE tenant_id = ? AND project_id = ? \
                 AND artifact_id = ? AND playground_id = ?",
            )
            .bind(request.tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .bind(playground_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        }
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => {
            sqlx::query(
                "DELETE FROM precommit_mutations WHERE tenant_id = ? AND precommit_id IN ( \
                     SELECT precommit_id FROM precommit_records WHERE tenant_id = ? \
                       AND project_id = ? AND artifact_id = ? \
                 )",
            )
            .bind(request.tenant_id.as_str())
            .bind(request.tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query(
                "DELETE FROM commit_records WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?",
            )
            .bind(request.tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query(
                "DELETE FROM precommit_records WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?",
            )
            .bind(request.tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query(
                "DELETE FROM playground_indexes WHERE tenant_id = ? AND project_id = ? AND artifact_id = ?",
            )
            .bind(request.tenant_id.as_str())
            .bind(project_id.as_str())
            .bind(artifact_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            for table in [
                "immutable_manifests",
                "object_placements",
                "durable_objects",
            ] {
                let sql = format!("DELETE FROM {table} WHERE tenant_id = ? AND artifact_id = ?");
                sqlx::query(&sql)
                    .bind(request.tenant_id.as_str())
                    .bind(artifact_id.as_str())
                    .execute(&mut **transaction)
                    .await
                    .map_err(storage_error)?;
            }
        }
        ResourceRef::StorageVolume { storage_volume_id } => {
            sqlx::query(
                "DELETE FROM object_placements WHERE tenant_id = ? AND storage_volume_id = ?",
            )
            .bind(request.tenant_id.as_str())
            .bind(storage_volume_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        }
    }
    Ok(())
}

async fn matching_jobs(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    target: &ResourceRef,
) -> CentralResult<Vec<JobRecord>> {
    let rows = sqlx::query("SELECT payload FROM control_jobs WHERE tenant_id = ?")
        .bind(tenant_id.as_str())
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?;
    rows.into_iter()
        .map(|row| {
            let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
            decode::<JobRecord>(&payload)
        })
        .collect::<CentralResult<Vec<_>>>()
        .map(|jobs| {
            jobs.into_iter()
                .filter(|job| job_matches_target(job, target))
                .collect()
        })
}

async fn delete_matching_jobs(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    target: &ResourceRef,
) -> CentralResult<()> {
    for job in matching_jobs(transaction, tenant_id, target).await? {
        sqlx::query("DELETE FROM metadata_batch_descriptors WHERE tenant_id = ? AND job_id = ?")
            .bind(tenant_id.as_str())
            .bind(job.spec.job_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("DELETE FROM index_publications WHERE tenant_id = ? AND job_id = ?")
            .bind(tenant_id.as_str())
            .bind(job.spec.job_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("DELETE FROM assignment_outbox WHERE tenant_id = ? AND job_id = ?")
            .bind(tenant_id.as_str())
            .bind(job.spec.job_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("DELETE FROM control_jobs WHERE tenant_id = ? AND job_id = ?")
            .bind(tenant_id.as_str())
            .bind(job.spec.job_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
    }
    Ok(())
}

fn job_matches_target(job: &JobRecord, target: &ResourceRef) -> bool {
    match target {
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => &job.spec.project_id == project_id && &job.spec.artifact_id == artifact_id,
        ResourceRef::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => {
            &job.spec.project_id == project_id
                && &job.spec.artifact_id == artifact_id
                && match job.operation {
                    JobOperation::Add => &job.spec.playground_id == playground_id,
                    JobOperation::WorkspaceMaterialize => job
                        .workspace_spec
                        .as_ref()
                        .is_some_and(|spec| &spec.playground_id == playground_id),
                    JobOperation::SnapshotDelivery => false,
                }
        }
        ResourceRef::Snapshot { snapshot_id } => job
            .delivery_spec
            .as_ref()
            .is_some_and(|spec| &spec.snapshot_id == snapshot_id),
        ResourceRef::StorageVolume { storage_volume_id } => {
            job.workspace_spec
                .as_ref()
                .is_some_and(|spec| &spec.storage_volume_id == storage_volume_id)
                || job
                    .delivery_spec
                    .as_ref()
                    .is_some_and(|spec| &spec.storage_volume_id == storage_volume_id)
                || job
                    .assignment
                    .as_ref()
                    .is_some_and(|assignment| &assignment.storage_volume_id == storage_volume_id)
        }
    }
}

fn precommit_matches_target(record: &PreCommitRecord, target: &ResourceRef) -> bool {
    match target {
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => &record.project_id == project_id && &record.artifact_id == artifact_id,
        ResourceRef::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => {
            &record.project_id == project_id
                && &record.artifact_id == artifact_id
                && &record.playground_id == playground_id
        }
        ResourceRef::StorageVolume { .. } | ResourceRef::Snapshot { .. } => false,
    }
}

fn target_identity(target: &ResourceRef) -> (&'static str, String) {
    match target {
        ResourceRef::StorageVolume { storage_volume_id } => {
            ("storage_volume", storage_volume_id.to_string())
        }
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => ("artifact", format!("{project_id}/{artifact_id}")),
        ResourceRef::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => (
            "playground",
            format!("{project_id}/{artifact_id}/{playground_id}"),
        ),
        ResourceRef::Snapshot { snapshot_id } => ("snapshot", snapshot_id.to_string()),
    }
}

const fn action_name(action: AuthorityLifecycleAction) -> &'static str {
    match action {
        AuthorityLifecycleAction::Quiesce => "quiesce",
        AuthorityLifecycleAction::Finalize => "finalize",
    }
}

fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Assigned => "assigned",
        JobState::Accepted => "accepted",
        JobState::Running => "running",
        JobState::Prepared => "prepared",
        JobState::Publishing => "publishing",
        JobState::CancelRequested => "cancel_requested",
        JobState::Succeeded => "succeeded",
        JobState::Conflicted => "conflicted",
        JobState::Rejected => "rejected",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::TimedOut => "timed_out",
        JobState::RecoveryRequired => "recovery_required",
        JobState::Unknown => "unknown",
    }
}

fn as_i64(value: u64) -> CentralResult<i64> {
    i64::try_from(value).map_err(|_| {
        CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            "Authority lifecycle timestamp exceeds the SQLite range",
        )
        .with_retryable(false)
    })
}

fn conflict(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::ConcurrentUpdate, message).with_retryable(false)
}
