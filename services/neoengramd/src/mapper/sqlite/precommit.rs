use async_trait::async_trait;
use neoengram_core::CommitId;
use neoengram_protocol::{ArtifactId, ProjectId, TenantId, UnixMillis};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};

use super::authority::*;
use crate::{
    apply_cancel, apply_commit, apply_head_publication_ack, apply_job_sync, apply_restart,
    build_started, same_cancel_request, same_commit_request, same_restart_request,
    same_start_request, validate_commit, validate_record, CentralError, CentralErrorCode,
    CentralResult, CommitRecord, JobRecord, PreCommitCancelRequest, PreCommitCommitOutcome,
    PreCommitCommitRequest, PreCommitCommitSnapshot, PreCommitKey, PreCommitMutationOutcome,
    PreCommitRecord, PreCommitRepository, PreCommitRestartRequest, PreCommitStartRequest,
    PreCommitState,
};

#[async_trait]
impl PreCommitRepository for SqliteAuthorityStore {
    async fn start(
        &self,
        request: PreCommitStartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some((kind, request_payload, result_payload)) = load_mutation(
            &mut transaction,
            &request.tenant_id,
            request.precommit_request_id.as_str(),
        )
        .await?
        {
            let stored: PreCommitStartRequest = decode(&request_payload)?;
            let result: PreCommitRecord = decode(&result_payload)?;
            return if kind == "start" && same_start_request(&stored, &request) {
                Ok(PreCommitMutationOutcome {
                    precommit: result,
                    replayed: true,
                })
            } else {
                Err(request_conflict())
            };
        }

        let record = build_started(&request)?;
        if load_precommit(&mut transaction, &record.key())
            .await?
            .is_some()
            || active_precommit_exists(&mut transaction, &record, None).await?
            || job_identity_exists(
                &mut transaction,
                &record.tenant_id,
                record.job_id.as_str(),
                None,
            )
            .await?
        {
            return Err(request_conflict());
        }
        insert_precommit(&mut transaction, &record).await?;
        insert_mutation(
            &mut transaction,
            &record.tenant_id,
            request.precommit_request_id.as_str(),
            "start",
            &record.precommit_id,
            &request,
            &record,
        )
        .await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(PreCommitMutationOutcome {
            precommit: record,
            replayed: false,
        })
    }

    async fn get(&self, key: &PreCommitKey) -> CentralResult<Option<PreCommitRecord>> {
        let row = sqlx::query(
            "SELECT project_id, artifact_id, playground_id, precommit_request_id, \
             current_job_id, state, attempt, resource_version, payload \
             FROM precommit_records WHERE tenant_id = ? AND precommit_id = ?",
        )
        .bind(key.tenant_id.as_str())
        .bind(key.precommit_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(|row| decode_precommit_row(key, &row)).transpose()
    }

    async fn get_active(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &neoengram_protocol::PlaygroundId,
    ) -> CentralResult<Option<PreCommitRecord>> {
        let rows = sqlx::query(
            "SELECT precommit_id, project_id, artifact_id, playground_id, precommit_request_id, \
             current_job_id, state, attempt, resource_version, payload FROM precommit_records \
             WHERE tenant_id = ? AND project_id = ? AND artifact_id = ? AND playground_id = ? \
             AND state IN ('running', 'ready', 'abnormal') LIMIT 2",
        )
        .bind(tenant_id.as_str())
        .bind(project_id.as_str())
        .bind(artifact_id.as_str())
        .bind(playground_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        if rows.len() > 1 {
            return Err(storage_corruption(
                "more than one active Pre-commit exists for a Playground",
            ));
        }
        rows.into_iter()
            .next()
            .map(|row| {
                let key = PreCommitKey::new(
                    tenant_id.clone(),
                    row.try_get::<String, _>("precommit_id")
                        .map_err(storage_error)?
                        .parse()?,
                );
                decode_precommit_row(&key, &row)
            })
            .transpose()
    }

    async fn list_running(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT tenant_id, precommit_id, project_id, artifact_id, playground_id, \
                 precommit_request_id, current_job_id, state, attempt, resource_version, payload \
                 FROM precommit_records WHERE state = 'running' AND \
                 (tenant_id > ? OR (tenant_id = ? AND precommit_id > ?)) \
                 ORDER BY tenant_id, precommit_id LIMIT ?",
            )
            .bind(after.tenant_id.as_str())
            .bind(after.tenant_id.as_str())
            .bind(after.precommit_id.as_str())
            .bind(i64::try_from(limit).map_err(|_| request_conflict())?)
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        } else {
            sqlx::query(
                "SELECT tenant_id, precommit_id, project_id, artifact_id, playground_id, \
                 precommit_request_id, current_job_id, state, attempt, resource_version, payload \
                 FROM precommit_records WHERE state = 'running' \
                 ORDER BY tenant_id, precommit_id LIMIT ?",
            )
            .bind(i64::try_from(limit).map_err(|_| request_conflict())?)
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        };
        rows.into_iter()
            .map(|row| {
                let key = PreCommitKey::new(
                    TenantId::new(
                        row.try_get::<String, _>("tenant_id")
                            .map_err(storage_error)?,
                    )?,
                    row.try_get::<String, _>("precommit_id")
                        .map_err(storage_error)?
                        .parse()?,
                );
                decode_precommit_row(&key, &row)
            })
            .collect()
    }

    async fn list_unpublished_commits(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT tenant_id, precommit_id, project_id, artifact_id, playground_id, \
                 precommit_request_id, current_job_id, state, attempt, resource_version, payload \
                 FROM precommit_records WHERE state = 'committed' AND \
                 (tenant_id > ? OR (tenant_id = ? AND precommit_id > ?)) \
                 ORDER BY tenant_id, precommit_id",
            )
            .bind(after.tenant_id.as_str())
            .bind(after.tenant_id.as_str())
            .bind(after.precommit_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        } else {
            sqlx::query(
                "SELECT tenant_id, precommit_id, project_id, artifact_id, playground_id, \
                 precommit_request_id, current_job_id, state, attempt, resource_version, payload \
                 FROM precommit_records WHERE state = 'committed' \
                 ORDER BY tenant_id, precommit_id",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        };
        let mut records = Vec::with_capacity(limit.min(rows.len()));
        for row in rows {
            let key = PreCommitKey::new(
                TenantId::new(
                    row.try_get::<String, _>("tenant_id")
                        .map_err(storage_error)?,
                )?,
                row.try_get::<String, _>("precommit_id")
                    .map_err(storage_error)?
                    .parse()?,
            );
            let record = decode_precommit_row(&key, &row)?;
            if record.head_published_at_unix_ms.is_none() {
                records.push(record);
                if records.len() == limit {
                    break;
                }
            }
        }
        Ok(records)
    }

    async fn restart(
        &self,
        request: PreCommitRestartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some((kind, request_payload, result_payload)) = load_mutation(
            &mut transaction,
            &request.key.tenant_id,
            request.restart_request_id.as_str(),
        )
        .await?
        {
            let stored: PreCommitRestartRequest = decode(&request_payload)?;
            let result: PreCommitRecord = decode(&result_payload)?;
            return if kind == "restart" && same_restart_request(&stored, &request) {
                Ok(PreCommitMutationOutcome {
                    precommit: result,
                    replayed: true,
                })
            } else {
                Err(request_conflict())
            };
        }

        let stored = load_precommit(&mut transaction, &request.key)
            .await?
            .ok_or_else(not_found)?;
        let previous = stored.resource_version.get();
        let restarted = apply_restart(stored, &request)?;
        if active_precommit_exists(&mut transaction, &restarted, Some(&restarted.precommit_id))
            .await?
        {
            return Err(request_conflict());
        }
        if job_identity_exists(
            &mut transaction,
            &restarted.tenant_id,
            restarted.job_id.as_str(),
            Some(&restarted.precommit_id),
        )
        .await?
        {
            return Err(request_conflict());
        }
        replace_precommit(&mut transaction, previous, &restarted).await?;
        insert_mutation(
            &mut transaction,
            &restarted.tenant_id,
            request.restart_request_id.as_str(),
            "restart",
            &restarted.precommit_id,
            &request,
            &restarted,
        )
        .await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(PreCommitMutationOutcome {
            precommit: restarted,
            replayed: false,
        })
    }

    async fn cancel(
        &self,
        request: PreCommitCancelRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some((kind, request_payload, result_payload)) = load_mutation(
            &mut transaction,
            &request.key.tenant_id,
            request.cancel_request_id.as_str(),
        )
        .await?
        {
            let stored: PreCommitCancelRequest = decode(&request_payload)?;
            let result: PreCommitRecord = decode(&result_payload)?;
            return if kind == "cancel" && same_cancel_request(&stored, &request) {
                Ok(PreCommitMutationOutcome {
                    precommit: result,
                    replayed: true,
                })
            } else {
                Err(request_conflict())
            };
        }

        let stored = load_precommit(&mut transaction, &request.key)
            .await?
            .ok_or_else(not_found)?;
        let previous = stored.resource_version.get();
        let cancelled = apply_cancel(stored, &request)?;
        replace_precommit(&mut transaction, previous, &cancelled).await?;
        insert_mutation(
            &mut transaction,
            &cancelled.tenant_id,
            request.cancel_request_id.as_str(),
            "cancel",
            &cancelled.precommit_id,
            &request,
            &cancelled,
        )
        .await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(PreCommitMutationOutcome {
            precommit: cancelled,
            replayed: false,
        })
    }

    async fn sync_job(
        &self,
        job: JobRecord,
        published_index: Option<crate::PublishedIndex>,
        observed_at_unix_ms: UnixMillis,
    ) -> CentralResult<Option<PreCommitRecord>> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT precommit_id, project_id, artifact_id, playground_id, \
             precommit_request_id, current_job_id, state, attempt, resource_version, payload \
             FROM precommit_records WHERE tenant_id = ? AND current_job_id = ?",
        )
        .bind(job.spec.tenant_id.as_str())
        .bind(job.spec.job_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let key = PreCommitKey::new(
            job.spec.tenant_id.clone(),
            row.try_get::<String, _>("precommit_id")
                .map_err(storage_error)?
                .parse()?,
        );
        let stored = decode_precommit_row(&key, &row)?;
        let base_records = match stored.frozen_head_commit_id {
            None => Some(Vec::new()),
            Some(commit_id) => load_commit_in_transaction(
                &mut transaction,
                &stored.tenant_id,
                &stored.project_id,
                &stored.artifact_id,
                commit_id,
            )
            .await?
            .map(|commit| commit.records),
        };
        let Some(synchronized) = apply_job_sync(
            stored.clone(),
            &job,
            published_index.as_ref(),
            base_records.as_deref(),
            observed_at_unix_ms,
        )?
        else {
            return Ok(None);
        };
        if synchronized != stored {
            replace_precommit(
                &mut transaction,
                stored.resource_version.get(),
                &synchronized,
            )
            .await?;
            transaction.commit().await.map_err(storage_error)?;
        }
        Ok(Some(synchronized))
    }

    async fn commit(
        &self,
        request: PreCommitCommitRequest,
    ) -> CentralResult<PreCommitCommitOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some((kind, request_payload, result_payload)) = load_mutation(
            &mut transaction,
            &request.key.tenant_id,
            request.commit.commit_request_id.as_str(),
        )
        .await?
        {
            let stored: PreCommitCommitRequest = decode(&request_payload)?;
            let result: PreCommitCommitSnapshot = decode(&result_payload)?;
            return if kind == "commit" && same_commit_request(&stored, &request) {
                Ok(PreCommitCommitOutcome::from_snapshot(result, true))
            } else {
                Err(request_conflict())
            };
        }

        let stored = load_precommit(&mut transaction, &request.key)
            .await?
            .ok_or_else(not_found)?;
        let previous = stored.resource_version.get();
        let consumed = apply_commit(stored, &request)?;
        let duplicate: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM commit_records WHERE tenant_id = ? AND project_id = ? \
             AND artifact_id = ? AND commit_id = ?",
        )
        .bind(consumed.commit.tenant_id.as_str())
        .bind(consumed.commit.project_id.as_str())
        .bind(consumed.commit.artifact_id.as_str())
        .bind(consumed.commit.commit_id.as_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if duplicate.is_some() {
            return Err(request_conflict());
        }
        replace_precommit(&mut transaction, previous, &consumed.consumed_precommit).await?;
        insert_commit(&mut transaction, &consumed.commit).await?;
        insert_mutation(
            &mut transaction,
            &consumed.commit.tenant_id,
            consumed.commit.commit_request_id.as_str(),
            "commit",
            &consumed.consumed_precommit.precommit_id,
            &request,
            &consumed,
        )
        .await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(PreCommitCommitOutcome::from_snapshot(consumed, false))
    }

    async fn get_commit(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        commit_id: CommitId,
    ) -> CentralResult<Option<CommitRecord>> {
        let payload: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM commit_records WHERE tenant_id = ? AND project_id = ? \
             AND artifact_id = ? AND commit_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(project_id.as_str())
        .bind(artifact_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        payload
            .map(|payload| {
                let record: CommitRecord = decode(&payload)?;
                validate_commit(&record)?;
                if record.tenant_id != *tenant_id
                    || record.project_id != *project_id
                    || record.artifact_id != *artifact_id
                    || record.commit_id != commit_id
                {
                    return Err(storage_corruption(
                        "Commit relational identity differs from its payload",
                    ));
                }
                Ok(record)
            })
            .transpose()
    }

    async fn acknowledge_head_publication(
        &self,
        key: &PreCommitKey,
        commit_id: CommitId,
        published_at_unix_ms: UnixMillis,
    ) -> CentralResult<PreCommitRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let stored = load_precommit(&mut transaction, key)
            .await?
            .ok_or_else(not_found)?;
        let acknowledged =
            apply_head_publication_ack(stored.clone(), commit_id, published_at_unix_ms)?;
        if acknowledged != stored {
            replace_precommit(
                &mut transaction,
                stored.resource_version.get(),
                &acknowledged,
            )
            .await?;
            transaction.commit().await.map_err(storage_error)?;
        }
        Ok(acknowledged)
    }
}

async fn load_precommit(
    transaction: &mut Transaction<'_, Sqlite>,
    key: &PreCommitKey,
) -> CentralResult<Option<PreCommitRecord>> {
    let row = sqlx::query(
        "SELECT project_id, artifact_id, playground_id, precommit_request_id, current_job_id, \
         state, attempt, resource_version, payload FROM precommit_records \
         WHERE tenant_id = ? AND precommit_id = ?",
    )
    .bind(key.tenant_id.as_str())
    .bind(key.precommit_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    row.map(|row| decode_precommit_row(key, &row)).transpose()
}

async fn load_commit_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    project_id: &ProjectId,
    artifact_id: &ArtifactId,
    commit_id: CommitId,
) -> CentralResult<Option<CommitRecord>> {
    let payload: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM commit_records WHERE tenant_id = ? AND project_id = ? \
         AND artifact_id = ? AND commit_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(project_id.as_str())
    .bind(artifact_id.as_str())
    .bind(commit_id.as_bytes().as_slice())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    payload
        .map(|payload| {
            let record: CommitRecord = decode(&payload)?;
            validate_commit(&record)?;
            if record.tenant_id != *tenant_id
                || record.project_id != *project_id
                || record.artifact_id != *artifact_id
                || record.commit_id != commit_id
            {
                return Err(storage_corruption(
                    "Commit relational identity differs from its payload",
                ));
            }
            Ok(record)
        })
        .transpose()
}

async fn load_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    request_id: &str,
) -> CentralResult<Option<(String, Vec<u8>, Vec<u8>)>> {
    let row = sqlx::query(
        "SELECT kind, request_payload, result_payload FROM precommit_mutations \
         WHERE tenant_id = ? AND request_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(request_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    row.map(|row| {
        Ok((
            row.try_get("kind").map_err(storage_error)?,
            row.try_get("request_payload").map_err(storage_error)?,
            row.try_get("result_payload").map_err(storage_error)?,
        ))
    })
    .transpose()
}

#[allow(clippy::too_many_arguments)]
async fn insert_mutation<Request: serde::Serialize, Result: serde::Serialize>(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    request_id: &str,
    kind: &str,
    precommit_id: &crate::PreCommitId,
    request: &Request,
    result: &Result,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO precommit_mutations \
         (tenant_id, request_id, kind, precommit_id, request_payload, result_payload) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(tenant_id.as_str())
    .bind(request_id)
    .bind(kind)
    .bind(precommit_id.as_str())
    .bind(encode(request)?)
    .bind(encode(result)?)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

async fn insert_precommit(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &PreCommitRecord,
) -> CentralResult<()> {
    validate_record(record)?;
    sqlx::query(
        "INSERT INTO precommit_records \
         (tenant_id, precommit_id, precommit_request_id, project_id, artifact_id, playground_id, \
          current_job_id, state, attempt, resource_version, payload) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(record.tenant_id.as_str())
    .bind(record.precommit_id.as_str())
    .bind(record.precommit_request_id.as_str())
    .bind(record.project_id.as_str())
    .bind(record.artifact_id.as_str())
    .bind(record.playground_id.as_str())
    .bind(record.job_id.as_str())
    .bind(precommit_state_name(record.state))
    .bind(i64::from(record.attempt))
    .bind(record.resource_version.get().to_string())
    .bind(encode(record)?)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

async fn replace_precommit(
    transaction: &mut Transaction<'_, Sqlite>,
    expected_resource_version: u64,
    record: &PreCommitRecord,
) -> CentralResult<()> {
    validate_record(record)?;
    if record.resource_version.get() != expected_resource_version.saturating_add(1) {
        return Err(CentralError::new(
            CentralErrorCode::ConcurrentUpdate,
            "replacement Pre-commit does not advance ResourceVersion by one",
        ));
    }
    let updated = sqlx::query(
        "UPDATE precommit_records SET current_job_id = ?, state = ?, attempt = ?, \
         resource_version = ?, payload = ? WHERE tenant_id = ? AND precommit_id = ? \
         AND resource_version = ?",
    )
    .bind(record.job_id.as_str())
    .bind(precommit_state_name(record.state))
    .bind(i64::from(record.attempt))
    .bind(record.resource_version.get().to_string())
    .bind(encode(record)?)
    .bind(record.tenant_id.as_str())
    .bind(record.precommit_id.as_str())
    .bind(expected_resource_version.to_string())
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if updated.rows_affected() != 1 {
        return Err(CentralError::new(
            CentralErrorCode::ConcurrentUpdate,
            "Pre-commit ResourceVersion changed during authority CAS",
        ));
    }
    Ok(())
}

async fn insert_commit(
    transaction: &mut Transaction<'_, Sqlite>,
    commit: &CommitRecord,
) -> CentralResult<()> {
    validate_commit(commit)?;
    sqlx::query(
        "INSERT INTO commit_records \
         (tenant_id, project_id, artifact_id, commit_id, commit_request_id, precommit_id, payload) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(commit.tenant_id.as_str())
    .bind(commit.project_id.as_str())
    .bind(commit.artifact_id.as_str())
    .bind(commit.commit_id.as_bytes().as_slice())
    .bind(commit.commit_request_id.as_str())
    .bind(commit.source_precommit_id.as_str())
    .bind(encode(commit)?)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

async fn job_identity_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    job_id: &str,
    except: Option<&crate::PreCommitId>,
) -> CentralResult<bool> {
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT precommit_id FROM precommit_records WHERE tenant_id = ? AND current_job_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(job_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(existing.is_some_and(|existing| except.is_none_or(|except| existing != except.as_str())))
}

async fn active_precommit_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    candidate: &PreCommitRecord,
    except: Option<&crate::PreCommitId>,
) -> CentralResult<bool> {
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT precommit_id FROM precommit_records WHERE tenant_id = ? AND project_id = ? \
         AND artifact_id = ? AND playground_id = ? \
         AND state IN ('running', 'ready', 'abnormal') LIMIT 1",
    )
    .bind(candidate.tenant_id.as_str())
    .bind(candidate.project_id.as_str())
    .bind(candidate.artifact_id.as_str())
    .bind(candidate.playground_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(existing.is_some_and(|existing| except.is_none_or(|except| existing != except.as_str())))
}

fn decode_precommit_row(key: &PreCommitKey, row: &SqliteRow) -> CentralResult<PreCommitRecord> {
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let record: PreCommitRecord = decode(&payload)?;
    validate_record(&record)?;
    let state: String = row.try_get("state").map_err(storage_error)?;
    let attempt: i64 = row.try_get("attempt").map_err(storage_error)?;
    let resource_version = parse_canonical_u64(
        row.try_get("resource_version").map_err(storage_error)?,
        "Pre-commit ResourceVersion",
    )?;
    if record.key() != *key
        || record.project_id.as_str()
            != row
                .try_get::<String, _>("project_id")
                .map_err(storage_error)?
        || record.artifact_id.as_str()
            != row
                .try_get::<String, _>("artifact_id")
                .map_err(storage_error)?
        || record.playground_id.as_str()
            != row
                .try_get::<String, _>("playground_id")
                .map_err(storage_error)?
        || record.precommit_request_id.as_str()
            != row
                .try_get::<String, _>("precommit_request_id")
                .map_err(storage_error)?
        || record.job_id.as_str()
            != row
                .try_get::<String, _>("current_job_id")
                .map_err(storage_error)?
        || precommit_state_name(record.state) != state
        || i64::from(record.attempt) != attempt
        || record.resource_version.get() != resource_version
    {
        return Err(storage_corruption(
            "Pre-commit relational identity differs from its payload",
        ));
    }
    Ok(record)
}

const fn precommit_state_name(state: PreCommitState) -> &'static str {
    match state {
        PreCommitState::Running => "running",
        PreCommitState::Ready => "ready",
        PreCommitState::Abnormal => "abnormal",
        PreCommitState::Cancelled => "cancelled",
        PreCommitState::Committed => "committed",
    }
}

fn not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::JobNotFound,
        "tenant-scoped Pre-commit was not found",
    )
    .with_retryable(false)
}

fn request_conflict() -> CentralError {
    CentralError::new(
        CentralErrorCode::ConcurrentUpdate,
        "Pre-commit mutation identity was reused with another payload",
    )
    .with_retryable(false)
}
