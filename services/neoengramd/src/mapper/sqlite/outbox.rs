use async_trait::async_trait;
use neoengram_protocol::{AgentId, JobAssignment};
use sqlx::Row;

use super::authority::*;
use crate::{validation::invalid, *};

#[async_trait]
impl AssignmentOutbox for SqliteAuthorityStore {
    async fn reserve(&self, assignment: JobAssignment) -> CentralResult<AssignmentReserveOutcome> {
        let (tenant_id, job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let payload = encode(&assignment)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO assignment_outbox \
             (tenant_id, assignment_id, job_id, payload, published) VALUES (?, ?, ?, ?, 0)",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .bind(job_id.as_str())
        .bind(&payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(AssignmentReserveOutcome::Reserved);
        }
        let existing: Vec<u8> = sqlx::query_scalar(
            "SELECT payload FROM assignment_outbox WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        if decode::<JobAssignment>(&existing)? == assignment {
            Ok(AssignmentReserveOutcome::Existing)
        } else {
            Err(invalid(
                CentralErrorCode::JobIdReused,
                "AssignmentId was reused with another payload",
            ))
        }
    }

    async fn publish(&self, assignment: JobAssignment) -> CentralResult<AssignmentPublishOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT payload, published FROM assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "Assignment must be reserved first",
            )
        })?;
        let existing: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
        if decode::<JobAssignment>(&existing)? != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                "AssignmentId was reused with another payload",
            ));
        }
        let published: i64 = row.try_get("published").map_err(storage_error)?;
        if published == 1 {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        sqlx::query(
            "UPDATE assignment_outbox SET published = 1 \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(AssignmentPublishOutcome::Published)
    }

    async fn reactivate(
        &self,
        assignment: JobAssignment,
    ) -> CentralResult<AssignmentPublishOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT payload, published, retired FROM assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "Assignment must be reserved before reactivation",
            )
        })?;
        let existing: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
        if decode::<JobAssignment>(&existing)? != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                "AssignmentId was reused with another payload",
            ));
        }
        let published: i64 = row.try_get("published").map_err(storage_error)?;
        let retired: i64 = row.try_get("retired").map_err(storage_error)?;
        if published == 1 && retired == 0 {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        sqlx::query(
            "UPDATE assignment_outbox SET published = 1, retired = 0 \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(AssignmentPublishOutcome::Published)
    }

    async fn retire(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        assignment_id: &neoengram_protocol::AssignmentId,
    ) -> CentralResult<AssignmentRetireOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT published, retired FROM assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                format!("assignment {assignment_id} is not reserved"),
            )
        })?;
        let published: i64 = row.try_get("published").map_err(storage_error)?;
        let retired: i64 = row.try_get("retired").map_err(storage_error)?;
        if published != 1 {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("assignment {assignment_id} is not published"),
            ));
        }
        if retired == 1 {
            return Ok(AssignmentRetireOutcome::AlreadyRetired);
        }
        sqlx::query(
            "UPDATE assignment_outbox SET retired = 1 \
             WHERE tenant_id = ? AND assignment_id = ? AND retired = 0",
        )
        .bind(tenant_id.as_str())
        .bind(assignment_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(AssignmentRetireOutcome::Retired)
    }

    async fn pending_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobAssignment>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let payloads: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM assignment_outbox WHERE published = 1 AND retired = 0 \
             ORDER BY tenant_id, assignment_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut assignments = Vec::with_capacity(limit.min(payloads.len()));
        for payload in payloads {
            let assignment: JobAssignment = decode(&payload)?;
            if assignment_identity(&assignment).3 == agent_id {
                assignments.push(assignment);
                if assignments.len() == limit {
                    break;
                }
            }
        }
        Ok(assignments)
    }
}

impl SqliteAuthorityStore {
    pub(super) async fn published_assignments(&self) -> CentralResult<Vec<JobAssignment>> {
        let payloads: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM assignment_outbox WHERE published = 1 \
             ORDER BY tenant_id, assignment_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        payloads.iter().map(|payload| decode(payload)).collect()
    }
}
