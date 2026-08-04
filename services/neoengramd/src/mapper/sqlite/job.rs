use async_trait::async_trait;
use neoengram_protocol::{AgentId, TenantId};
use sqlx::{sqlite::SqliteRow, Row};

use super::authority::*;
use crate::{validation::invalid, *};

#[async_trait]
impl JobRepository for SqliteAuthorityStore {
    async fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        let row = sqlx::query(
            "SELECT state, resource_version, payload FROM control_jobs \
             WHERE tenant_id = ? AND job_id = ?",
        )
        .bind(key.tenant_id.as_str())
        .bind(key.job_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(|row| decode_job_row(key, &row)).transpose()
    }

    async fn list_recoverable(
        &self,
        after: Option<&JobKey>,
        now: neoengram_protocol::UnixMillis,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT tenant_id, job_id, state, resource_version, payload FROM control_jobs \
                 WHERE (tenant_id > ? OR (tenant_id = ? AND job_id > ?)) \
                   AND state IN ('queued', 'assigned', 'accepted', 'running', 'prepared', \
                                 'publishing', 'cancel_requested') \
                 ORDER BY tenant_id, job_id",
            )
            .bind(after.tenant_id.as_str())
            .bind(after.tenant_id.as_str())
            .bind(after.job_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        } else {
            sqlx::query(
                "SELECT tenant_id, job_id, state, resource_version, payload FROM control_jobs \
                 WHERE state IN ('queued', 'assigned', 'accepted', 'running', 'prepared', \
                                 'publishing', 'cancel_requested') \
                 ORDER BY tenant_id, job_id",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        };
        let mut jobs = Vec::with_capacity(limit.min(rows.len()));
        for row in rows {
            let tenant_id = TenantId::new(
                row.try_get::<String, _>("tenant_id")
                    .map_err(storage_error)?,
            )?;
            let job_id = neoengram_protocol::JobId::new(
                row.try_get::<String, _>("job_id").map_err(storage_error)?,
            )?;
            let job = decode_job_row(&JobKey::new(tenant_id, job_id), &row)?;
            if crate::mapper_recovery_predicate(&job, now) {
                jobs.push(job);
                if jobs.len() == limit {
                    break;
                }
            }
        }
        Ok(jobs)
    }

    async fn list_pending_decisions_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT tenant_id, job_id, state, resource_version, payload FROM control_jobs \
             ORDER BY tenant_id, job_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut jobs = Vec::with_capacity(limit.min(rows.len()));
        for row in rows {
            let tenant_id = TenantId::new(
                row.try_get::<String, _>("tenant_id")
                    .map_err(storage_error)?,
            )?;
            let job_id = neoengram_protocol::JobId::new(
                row.try_get::<String, _>("job_id").map_err(storage_error)?,
            )?;
            let job = decode_job_row(&JobKey::new(tenant_id, job_id), &row)?;
            if pending_decision_for_agent(&job, agent_id) {
                jobs.push(job);
                if jobs.len() == limit {
                    break;
                }
            }
        }
        Ok(jobs)
    }

    async fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        validate_job_identity(&job)?;
        let payload = encode(&job)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO control_jobs \
             (tenant_id, job_id, state, resource_version, payload) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(job.spec.tenant_id.as_str())
        .bind(job.spec.job_id.as_str())
        .bind(job_state_name(job.state))
        .bind(job.resource_version.get().to_string())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(JobInsertOutcome::Inserted(job));
        }
        JobRepository::get(self, &job.key())
            .await?
            .map(JobInsertOutcome::Existing)
            .ok_or_else(|| storage_corruption("conflicting Job disappeared"))
    }

    async fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        validate_job_identity(&job)?;
        if job.resource_version.get() != expected.saturating_add(1) {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replacement Job does not advance ResourceVersion by one",
            ));
        }
        let payload = encode(&job)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let result = sqlx::query(
            "UPDATE control_jobs SET state = ?, resource_version = ?, payload = ? \
             WHERE tenant_id = ? AND job_id = ? AND resource_version = ?",
        )
        .bind(job_state_name(job.state))
        .bind(job.resource_version.get().to_string())
        .bind(payload)
        .bind(job.spec.tenant_id.as_str())
        .bind(job.spec.job_id.as_str())
        .bind(expected.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            let exists: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM control_jobs WHERE tenant_id = ? AND job_id = ?")
                    .bind(job.spec.tenant_id.as_str())
                    .bind(job.spec.job_id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(storage_error)?;
            return Err(invalid(
                if exists.is_some() {
                    CentralErrorCode::ConcurrentUpdate
                } else {
                    CentralErrorCode::JobNotFound
                },
                "Job ResourceVersion changed during authority CAS",
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(job)
    }
}

fn pending_decision_for_agent(job: &JobRecord, agent_id: &AgentId) -> bool {
    job.decision.is_some()
        && job.finalized_ack.is_none()
        && job
            .assignment
            .as_ref()
            .is_some_and(|assignment| &assignment.agent_id == agent_id)
}

fn validate_job_identity(job: &JobRecord) -> CentralResult<()> {
    job.spec
        .job_id
        .as_str()
        .parse::<neoengram_protocol::JobId>()?;
    job.spec.tenant_id.as_str().parse::<TenantId>()?;
    Ok(())
}

fn decode_job_row(key: &JobKey, row: &SqliteRow) -> CentralResult<JobRecord> {
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let job: JobRecord = decode(&payload)?;
    let state: String = row.try_get("state").map_err(storage_error)?;
    let resource_version = parse_canonical_u64(
        row.try_get("resource_version").map_err(storage_error)?,
        "Job ResourceVersion",
    )?;
    if job.key() != *key
        || state != job_state_name(job.state)
        || resource_version != job.resource_version.get()
    {
        return Err(storage_corruption(
            "Job relational identity differs from its payload",
        ));
    }
    Ok(job)
}

pub(super) const fn job_state_name(state: neoengram_protocol::JobState) -> &'static str {
    use neoengram_protocol::JobState;
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
