use async_trait::async_trait;

use super::authority::*;
use super::job::job_state_name;
use crate::{validation::invalid, *};

#[async_trait]
impl AuditSink for SqliteAuthorityStore {
    async fn record(&self, event: AuditEvent) -> CentralResult<bool> {
        let payload = encode(&event)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO audit_events \
             (tenant_id, event_id, job_id, kind, state, occurred_at, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.job_key.tenant_id.as_str())
        .bind(&event.event_id)
        .bind(event.job_key.job_id.as_str())
        .bind(audit_kind_name(event.kind))
        .bind(job_state_name(event.state))
        .bind(event.occurred_at_unix_ms.get().to_string())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(false);
        }
        let existing: Vec<u8> = sqlx::query_scalar(
            "SELECT payload FROM audit_events WHERE tenant_id = ? AND event_id = ?",
        )
        .bind(event.job_key.tenant_id.as_str())
        .bind(&event.event_id)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        let existing: AuditEvent = decode(&existing)?;
        if existing.kind == event.kind
            && existing.job_key == event.job_key
            && existing.state == event.state
        {
            Ok(true)
        } else {
            Err(invalid(
                CentralErrorCode::Internal,
                format!("audit event ID {} was reused", event.event_id),
            ))
        }
    }

    async fn record_enrollment_decision(
        &self,
        event: AgentEnrollmentAuditEvent,
    ) -> CentralResult<bool> {
        let payload = encode(&event)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO agent_enrollment_audit_events \
             (tenant_id, event_id, enrollment_id, kind, occurred_at, payload) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(event.tenant_id.as_str())
        .bind(&event.event_id)
        .bind(event.enrollment_id.as_str())
        .bind(enrollment_audit_kind_name(event.kind))
        .bind(event.occurred_at_unix_ms.get().to_string())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 1 {
            return Ok(false);
        }
        let existing: Vec<u8> = sqlx::query_scalar(
            "SELECT payload FROM agent_enrollment_audit_events \
             WHERE tenant_id = ? AND event_id = ?",
        )
        .bind(event.tenant_id.as_str())
        .bind(&event.event_id)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        let existing: AgentEnrollmentAuditEvent = decode(&existing)?;
        if existing.kind == event.kind
            && existing.enrollment_id == event.enrollment_id
            && existing.storage_volume_id == event.storage_volume_id
            && existing.decision_request_id == event.decision_request_id
            && existing.resource_version == event.resource_version
            && existing.actor == event.actor
        {
            Ok(true)
        } else {
            Err(invalid(
                CentralErrorCode::Internal,
                format!("enrollment audit event ID {} was reused", event.event_id),
            ))
        }
    }
}

impl SqliteAuthorityStore {
    pub(super) async fn audit_events(&self) -> CentralResult<Vec<AuditEvent>> {
        let payloads: Vec<Vec<u8>> =
            sqlx::query_scalar("SELECT payload FROM audit_events ORDER BY tenant_id, event_id")
                .fetch_all(&self.pool)
                .await
                .map_err(storage_error)?;
        payloads.iter().map(|payload| decode(payload)).collect()
    }

    pub(super) async fn enrollment_audit_events(
        &self,
    ) -> CentralResult<Vec<AgentEnrollmentAuditEvent>> {
        let payloads: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT payload FROM agent_enrollment_audit_events ORDER BY tenant_id, event_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        payloads.iter().map(|payload| decode(payload)).collect()
    }
}

const fn audit_kind_name(kind: AuditKind) -> &'static str {
    match kind {
        AuditKind::JobCreated => "job_created",
        AuditKind::AssignmentQueued => "assignment_queued",
        AuditKind::ReportReceived => "report_received",
        AuditKind::MetadataStaged => "metadata_staged",
        AuditKind::AddExpired => "add_expired",
        AuditKind::AddFinalized => "add_finalized",
    }
}

const fn enrollment_audit_kind_name(kind: AgentEnrollmentAuditKind) -> &'static str {
    match kind {
        AgentEnrollmentAuditKind::Approved => "approved",
        AgentEnrollmentAuditKind::Rejected => "rejected",
        AgentEnrollmentAuditKind::ReplacementApproved => "replacement_approved",
    }
}
