use async_trait::async_trait;
use neoengram_protocol::{AssignmentOperation, JobAssignment};
use sqlx::Row;

use super::authority::*;
use crate::{validation::invalid, *};

#[async_trait]
impl AssignmentOutbox for SqliteAuthorityStore {
    async fn reserve(&self, assignment: JobAssignment) -> CentralResult<AssignmentReserveOutcome> {
        let AssignmentOperation::Add { input, .. } = &assignment.assignment;
        let payload = encode(&assignment)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO assignment_outbox \
             (tenant_id, assignment_id, job_id, payload, published) VALUES (?, ?, ?, ?, 0)",
        )
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
        .bind(input.job_id.as_str())
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
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
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
        let AssignmentOperation::Add { input, .. } = &assignment.assignment;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT payload, published FROM assignment_outbox \
             WHERE tenant_id = ? AND assignment_id = ?",
        )
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
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
        .bind(input.tenant_id.as_str())
        .bind(input.assignment_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(AssignmentPublishOutcome::Published)
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
