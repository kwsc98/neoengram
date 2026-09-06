//! SQLite mapper for the unified operation-task authority.
//!
//! Task rows keep a normalized query projection alongside a versioned JSON payload. The payload
//! is the canonical representation and is decoded/validated on every read; the projection only
//! exists to make the database schema auditable and to support indexed filters.

use std::collections::BTreeSet;

use async_trait::async_trait;
use neoengram_domain::protocol::{
    OperationTask, RequestId, ResourceVersion, SequenceNumber, StageOutcome, StageState, TaskActor,
    TaskAttempt, TaskEvent, TaskEventKind, TaskId, TaskIntent, TaskPurpose, TaskRelation,
    TaskRelationKind, TaskResourceKind, TaskResourceLink, TaskResourceRole, TaskStage, TaskState,
    TenantId, UnixMillis,
};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};

use super::authority::{
    decode, encode, parse_canonical_u64, storage_corruption, storage_error, SqliteAuthorityStore,
};
use crate::validation::invalid;
use crate::{
    CentralError, CentralErrorCode, CentralResult, TaskEventListPage, TaskEventListRequest,
    TaskInsertOutcome, TaskListPage, TaskListRequest, TaskMutationOutcome, TaskRelationRecord,
    TaskRepository, TaskResourceLinkRecord, TaskSummary,
};

fn text_u64(value: u64) -> String {
    value.to_string()
}

fn optional_text_u64_matches(
    row: &SqliteRow,
    column: &str,
    expected: Option<u64>,
    field: &str,
) -> CentralResult<bool> {
    let actual = row
        .try_get::<Option<String>, _>(column)
        .map_err(storage_error)?
        .map(|value| parse_canonical_u64(value, field))
        .transpose()?;
    Ok(actual == expected)
}

/// Returns the next append-only event sequence without coercing the decimal text through
/// SQLite's signed 64-bit integer type.  Task sequences are u64 on the wire and must remain
/// valid even when a long-lived task exceeds `i64::MAX` events.
async fn next_event_sequence(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    task_id: &TaskId,
) -> CentralResult<u64> {
    let rows = sqlx::query("SELECT sequence FROM task_events WHERE tenant_id=? AND task_id=?")
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .fetch_all(&mut **tx)
        .await
        .map_err(storage_error)?;
    let mut max = 0u64;
    for row in rows {
        let value = parse_canonical_u64(
            row.try_get::<String, _>("sequence")
                .map_err(storage_error)?,
            "task event sequence",
        )?;
        max = max.max(value);
    }
    max.checked_add(1)
        .ok_or_else(|| storage_corruption("task event sequence overflow"))
}

async fn insert_attempt_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    attempt: &TaskAttempt,
) -> CentralResult<()> {
    sqlx::query("INSERT INTO task_attempts (tenant_id,task_id,attempt_id,attempt,state,current_stage_key,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,issue,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(tenant_id.as_str())
        .bind(attempt.task_id.as_str())
        .bind(attempt.attempt_id.as_str())
        .bind(text_u64(attempt.attempt.get()))
        .bind(state_name(attempt.state))
        .bind(&attempt.current_stage_key)
        .bind(text_u64(attempt.created_at_unix_ms.get()))
        .bind(text_u64(attempt.updated_at_unix_ms.get()))
        .bind(attempt.started_at_unix_ms.map(|v| text_u64(v.get())))
        .bind(attempt.finished_at_unix_ms.map(|v| text_u64(v.get())))
        .bind(attempt.issue.as_ref().map(encode).transpose()?)
        .bind(text_u64(attempt.resource_version.get()))
        .bind(encode(attempt)?)
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    Ok(())
}

async fn insert_event_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    event: &TaskEvent,
) -> CentralResult<()> {
    sqlx::query("INSERT INTO task_events (tenant_id,task_id,event_id,sequence,attempt,kind,state,occurred_at_unix_ms,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?)")
        .bind(tenant_id.as_str())
        .bind(event.task_id.as_str())
        .bind(event.event_id.as_str())
        .bind(text_u64(event.sequence.get()))
        .bind(text_u64(event.attempt.get()))
        .bind(event_kind_name(event.kind))
        .bind(state_name(event.state))
        .bind(text_u64(event.occurred_at_unix_ms.get()))
        .bind(text_u64(event.resource_version.get()))
        .bind(encode(event)?)
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    Ok(())
}

fn state_name(state: TaskState) -> &'static str {
    match state {
        TaskState::Queued => "queued",
        TaskState::Running => "running",
        TaskState::Waiting => "waiting",
        TaskState::Verifying => "verifying",
        TaskState::Succeeded => "succeeded",
        TaskState::Stalled => "stalled",
        TaskState::Failed => "failed",
        TaskState::Cancelling => "cancelling",
        TaskState::Cancelled => "cancelled",
    }
}

fn kind_name(kind: TaskIntent) -> &'static str {
    kind.as_str()
}

fn purpose_name(purpose: Option<TaskPurpose>) -> Option<&'static str> {
    purpose.map(TaskPurpose::as_str)
}

fn parse_resource_kind(value: &str) -> CentralResult<TaskResourceKind> {
    match value {
        "tenant" => Ok(TaskResourceKind::Tenant),
        "project" => Ok(TaskResourceKind::Project),
        "artifact" => Ok(TaskResourceKind::Artifact),
        "object_namespace" => Ok(TaskResourceKind::ObjectNamespace),
        "commit" => Ok(TaskResourceKind::Commit),
        "workspace" => Ok(TaskResourceKind::Workspace),
        "snapshot" => Ok(TaskResourceKind::Snapshot),
        "snapshot_delivery" => Ok(TaskResourceKind::SnapshotDelivery),
        "storage_volume" => Ok(TaskResourceKind::StorageVolume),
        "storage_enrollment" => Ok(TaskResourceKind::StorageEnrollment),
        "agent" => Ok(TaskResourceKind::Agent),
        "gateway" => Ok(TaskResourceKind::Gateway),
        "s3_access_point" => Ok(TaskResourceKind::S3AccessPoint),
        "s3_credential" => Ok(TaskResourceKind::S3Credential),
        "deletion" => Ok(TaskResourceKind::Deletion),
        "retention_hold" => Ok(TaskResourceKind::RetentionHold),
        "materialization" => Ok(TaskResourceKind::Materialization),
        "materialization_batch" => Ok(TaskResourceKind::MaterializationBatch),
        "precommit" => Ok(TaskResourceKind::Precommit),
        "control_job" => Ok(TaskResourceKind::ControlJob),
        _ => Err(storage_corruption(format!(
            "unknown task resource kind projection {value:?}"
        ))),
    }
}

fn stage_state_name(state: StageState) -> &'static str {
    match state {
        StageState::Pending => "pending",
        StageState::Ready => "ready",
        StageState::Running => "running",
        StageState::Waiting => "waiting",
        StageState::Verifying => "verifying",
        StageState::Succeeded => "succeeded",
        StageState::Skipped => "skipped",
        StageState::NoOp => "no_op",
        StageState::Stalled => "stalled",
        StageState::Failed => "failed",
        StageState::Cancelling => "cancelling",
        StageState::Cancelled => "cancelled",
    }
}

fn parse_stage_state(value: &str) -> CentralResult<StageState> {
    match value {
        "pending" => Ok(StageState::Pending),
        "ready" => Ok(StageState::Ready),
        "running" => Ok(StageState::Running),
        "waiting" => Ok(StageState::Waiting),
        "verifying" => Ok(StageState::Verifying),
        "succeeded" => Ok(StageState::Succeeded),
        "skipped" => Ok(StageState::Skipped),
        "no_op" => Ok(StageState::NoOp),
        "stalled" => Ok(StageState::Stalled),
        "failed" => Ok(StageState::Failed),
        "cancelling" => Ok(StageState::Cancelling),
        "cancelled" => Ok(StageState::Cancelled),
        _ => Err(storage_corruption(format!(
            "unknown task stage state projection {value:?}"
        ))),
    }
}

fn stage_outcome_name(outcome: Option<StageOutcome>) -> Option<&'static str> {
    outcome.map(|value| match value {
        StageOutcome::Succeeded => "succeeded",
        StageOutcome::Skipped => "skipped",
        StageOutcome::NoOp => "no_op",
        StageOutcome::Reused => "reused",
        StageOutcome::Failed => "failed",
        StageOutcome::Cancelled => "cancelled",
    })
}

fn parse_stage_outcome(value: Option<String>) -> CentralResult<Option<StageOutcome>> {
    value
        .map(|value| match value.as_str() {
            "succeeded" => Ok(StageOutcome::Succeeded),
            "skipped" => Ok(StageOutcome::Skipped),
            "no_op" => Ok(StageOutcome::NoOp),
            "reused" => Ok(StageOutcome::Reused),
            "failed" => Ok(StageOutcome::Failed),
            "cancelled" => Ok(StageOutcome::Cancelled),
            _ => Err(storage_corruption(format!(
                "unknown task stage outcome projection {value:?}"
            ))),
        })
        .transpose()
}

fn decode_stage_row(row: &SqliteRow) -> CentralResult<TaskStage> {
    let stage: TaskStage = decode(
        &row.try_get::<Vec<u8>, _>("payload")
            .map_err(storage_error)?,
    )?;
    stage.validate().map_err(CentralError::from)?;
    let task_id = row.try_get::<String, _>("task_id").map_err(storage_error)?;
    let stage_key = row
        .try_get::<String, _>("stage_key")
        .map_err(storage_error)?;
    let ordinal = parse_canonical_u64(
        row.try_get::<String, _>("ordinal").map_err(storage_error)?,
        "task stage ordinal",
    )?;
    let stage_attempt = parse_canonical_u64(
        row.try_get::<String, _>("stage_attempt")
            .map_err(storage_error)?,
        "task stage attempt",
    )?;
    let state = parse_stage_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    let outcome = parse_stage_outcome(
        row.try_get::<Option<String>, _>("outcome")
            .map_err(storage_error)?,
    )?;
    let check_u64 = |column: &str, expected: u64, field: &str| {
        row.try_get::<String, _>(column)
            .map_err(storage_error)
            .and_then(|value| parse_canonical_u64(value, field))
            .map(|value| value == expected)
    };
    let issue = row
        .try_get::<Option<Vec<u8>>, _>("issue")
        .map_err(storage_error)?
        .map(|payload| decode::<neoengram_domain::protocol::TaskIssue>(&payload))
        .transpose()?;
    let identity_matches = task_id == stage.task_id.as_str()
        && stage_key == stage.stage_key
        && row
            .try_get::<String, _>("stage_kind")
            .map_err(storage_error)?
            == stage.stage_kind
        && ordinal == stage.ordinal.get()
        && state == stage.state
        && stage_attempt == stage.stage_attempt.get()
        && outcome == stage.outcome
        && check_u64(
            "progress_completed",
            stage.progress.completed.get(),
            "stage progress_completed",
        )?
        && check_u64(
            "progress_total",
            stage.progress.total.get(),
            "stage progress_total",
        )?
        && check_u64(
            "progress_completed_bytes",
            stage.progress.completed_bytes.get(),
            "stage progress_completed_bytes",
        )?
        && check_u64(
            "progress_total_bytes",
            stage.progress.total_bytes.get(),
            "stage progress_total_bytes",
        )?
        && row
            .try_get::<Option<String>, _>("detail_kind")
            .map_err(storage_error)?
            == stage.detail_kind
        && row
            .try_get::<Option<String>, _>("detail_id")
            .map_err(storage_error)?
            == stage.detail_id
        && issue == stage.issue
        && check_u64(
            "created_at_unix_ms",
            stage.created_at_unix_ms.get(),
            "stage created_at_unix_ms",
        )?
        && check_u64(
            "updated_at_unix_ms",
            stage.updated_at_unix_ms.get(),
            "stage updated_at_unix_ms",
        )?
        && optional_text_u64_matches(
            row,
            "started_at_unix_ms",
            stage.started_at_unix_ms.map(|value| value.get()),
            "stage started_at_unix_ms",
        )?
        && optional_text_u64_matches(
            row,
            "finished_at_unix_ms",
            stage.finished_at_unix_ms.map(|value| value.get()),
            "stage finished_at_unix_ms",
        )?
        && check_u64(
            "resource_version",
            stage.resource_version.get(),
            "stage ResourceVersion",
        )?;
    if !identity_matches {
        return Err(storage_corruption(
            "task stage relational projection differs from payload",
        ));
    }
    Ok(stage)
}

fn stage_columns() -> &'static str {
    "tenant_id,task_id,stage_key,stage_kind,ordinal,state,stage_attempt,outcome,progress_completed,progress_total,progress_completed_bytes,progress_total_bytes,detail_kind,detail_id,issue,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload"
}

async fn insert_stage_dependencies_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    stage: &TaskStage,
) -> CentralResult<()> {
    for dependency in &stage.dependencies {
        let exists = sqlx::query(
            "SELECT 1 FROM task_stages WHERE tenant_id=? AND task_id=? AND stage_key=?",
        )
        .bind(tenant_id.as_str())
        .bind(stage.task_id.as_str())
        .bind(dependency)
        .fetch_optional(&mut **tx)
        .await
        .map_err(storage_error)?
        .is_some();
        if !exists {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("stage dependency {dependency:?} does not exist"),
            ));
        }
        sqlx::query("INSERT OR IGNORE INTO task_stage_dependencies (tenant_id,task_id,stage_key,dependency_key) VALUES (?,?,?,?)")
            .bind(tenant_id.as_str())
            .bind(stage.task_id.as_str())
            .bind(&stage.stage_key)
            .bind(dependency)
            .execute(&mut **tx)
            .await
            .map_err(storage_error)?;
    }
    Ok(())
}

async fn insert_stage_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    stage: &TaskStage,
) -> CentralResult<()> {
    sqlx::query("INSERT INTO task_stages (tenant_id,task_id,stage_key,stage_kind,ordinal,state,stage_attempt,outcome,progress_completed,progress_total,progress_completed_bytes,progress_total_bytes,detail_kind,detail_id,issue,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(tenant_id.as_str())
        .bind(stage.task_id.as_str())
        .bind(&stage.stage_key)
        .bind(&stage.stage_kind)
        .bind(text_u64(stage.ordinal.get()))
        .bind(stage_state_name(stage.state))
        .bind(text_u64(stage.stage_attempt.get()))
        .bind(stage_outcome_name(stage.outcome))
        .bind(text_u64(stage.progress.completed.get()))
        .bind(text_u64(stage.progress.total.get()))
        .bind(text_u64(stage.progress.completed_bytes.get()))
        .bind(text_u64(stage.progress.total_bytes.get()))
        .bind(stage.detail_kind.clone())
        .bind(stage.detail_id.clone())
        .bind(stage.issue.as_ref().map(encode).transpose()?)
        .bind(text_u64(stage.created_at_unix_ms.get()))
        .bind(text_u64(stage.updated_at_unix_ms.get()))
        .bind(stage.started_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(stage.finished_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(text_u64(stage.resource_version.get()))
        .bind(encode(stage)?)
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    insert_stage_dependencies_tx(tx, tenant_id, stage).await
}

/// Inserts a complete stage DAG without requiring callers to provide topological order. SQLite
/// enforces foreign keys on dependency rows, so each layer is written only after all of its
/// declared dependencies have been inserted in the same transaction.
async fn insert_stages_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    stages: &[TaskStage],
) -> CentralResult<()> {
    let mut pending = stages.to_vec();
    let mut inserted = BTreeSet::new();
    while !pending.is_empty() {
        let mut next = Vec::with_capacity(pending.len());
        let mut inserted_any = false;
        for stage in pending {
            if stage
                .dependencies
                .iter()
                .all(|dependency| inserted.contains(dependency))
            {
                insert_stage_tx(tx, tenant_id, &stage).await?;
                inserted.insert(stage.stage_key.clone());
                inserted_any = true;
            } else {
                next.push(stage);
            }
        }
        if !inserted_any {
            // validate_task_stages normally catches this; keep the guard local to the write
            // helper so a future caller cannot accidentally create a partially inserted plan.
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "task stage dependencies cannot be inserted in a DAG order",
            ));
        }
        pending = next;
    }
    Ok(())
}

async fn write_stage<'a>(
    tx: &mut Transaction<'a, Sqlite>,
    tenant_id: &TenantId,
    stage: &TaskStage,
    expected_resource_version: ResourceVersion,
) -> CentralResult<()> {
    // Archive the exact row protected by the CAS fence before replacing it. This keeps every
    // prior stage execution (including attempts reset during a task retry) available to audit.
    let previous = sqlx::query(
        "SELECT payload FROM task_stages WHERE tenant_id=? AND task_id=? AND stage_key=? AND resource_version=?",
    )
    .bind(tenant_id.as_str())
    .bind(stage.task_id.as_str())
    .bind(&stage.stage_key)
    .bind(text_u64(expected_resource_version.get()))
    .fetch_optional(&mut **tx)
    .await
    .map_err(storage_error)?;
    let previous = previous.ok_or_else(|| {
        invalid(
            CentralErrorCode::ConcurrentUpdate,
            "task stage resource version changed",
        )
    })?;
    let previous_stage: TaskStage = decode(
        &previous
            .try_get::<Vec<u8>, _>("payload")
            .map_err(storage_error)?,
    )?;
    previous_stage.validate().map_err(CentralError::from)?;
    sqlx::query("INSERT OR IGNORE INTO task_stage_history (tenant_id,task_id,stage_key,stage_attempt,resource_version,recorded_at_unix_ms,payload) VALUES (?,?,?,?,?,?,?)")
        .bind(tenant_id.as_str())
        .bind(previous_stage.task_id.as_str())
        .bind(&previous_stage.stage_key)
        .bind(text_u64(previous_stage.stage_attempt.get()))
        .bind(text_u64(previous_stage.resource_version.get()))
        .bind(text_u64(previous_stage.updated_at_unix_ms.get()))
        .bind(encode(&previous_stage)?)
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    let result = sqlx::query("UPDATE task_stages SET stage_kind=?,ordinal=?,state=?,stage_attempt=?,outcome=?,progress_completed=?,progress_total=?,progress_completed_bytes=?,progress_total_bytes=?,detail_kind=?,detail_id=?,issue=?,created_at_unix_ms=?,updated_at_unix_ms=?,started_at_unix_ms=?,finished_at_unix_ms=?,resource_version=?,payload=? WHERE tenant_id=? AND task_id=? AND stage_key=? AND resource_version=?")
        .bind(&stage.stage_kind)
        .bind(text_u64(stage.ordinal.get()))
        .bind(stage_state_name(stage.state))
        .bind(text_u64(stage.stage_attempt.get()))
        .bind(stage_outcome_name(stage.outcome))
        .bind(text_u64(stage.progress.completed.get()))
        .bind(text_u64(stage.progress.total.get()))
        .bind(text_u64(stage.progress.completed_bytes.get()))
        .bind(text_u64(stage.progress.total_bytes.get()))
        .bind(stage.detail_kind.clone())
        .bind(stage.detail_id.clone())
        .bind(stage.issue.as_ref().map(encode).transpose()?)
        .bind(text_u64(stage.created_at_unix_ms.get()))
        .bind(text_u64(stage.updated_at_unix_ms.get()))
        .bind(stage.started_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(stage.finished_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(text_u64(stage.resource_version.get()))
        .bind(encode(stage)?)
        .bind(tenant_id.as_str())
        .bind(stage.task_id.as_str())
        .bind(&stage.stage_key)
        .bind(text_u64(expected_resource_version.get()))
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    if result.rows_affected() != 1 {
        return Err(invalid(
            CentralErrorCode::ConcurrentUpdate,
            "task stage resource version changed",
        ));
    }
    sqlx::query(
        "DELETE FROM task_stage_dependencies WHERE tenant_id=? AND task_id=? AND stage_key=?",
    )
    .bind(tenant_id.as_str())
    .bind(stage.task_id.as_str())
    .bind(&stage.stage_key)
    .execute(&mut **tx)
    .await
    .map_err(storage_error)?;
    insert_stage_dependencies_tx(tx, tenant_id, stage).await
}

async fn stage_rows(
    store: &SqliteAuthorityStore,
    tenant_id: &TenantId,
    task_id: &TaskId,
) -> CentralResult<Vec<TaskStage>> {
    let query = format!(
        "SELECT {} FROM task_stages WHERE tenant_id=? AND task_id=? ORDER BY LENGTH(ordinal), ordinal, stage_key",
        stage_columns()
    );
    let rows = sqlx::query(&query)
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .fetch_all(&store.pool)
        .await
        .map_err(storage_error)?;
    let mut stages = Vec::with_capacity(rows.len());
    for row in &rows {
        let stage = decode_stage_row(row)?;
        let dependency_rows = sqlx::query(
            "SELECT dependency_key FROM task_stage_dependencies WHERE tenant_id=? AND task_id=? AND stage_key=? ORDER BY dependency_key",
        )
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .bind(&stage.stage_key)
        .fetch_all(&store.pool)
        .await
        .map_err(storage_error)?;
        let persisted_dependencies = dependency_rows
            .iter()
            .map(|dependency| {
                dependency
                    .try_get::<String, _>("dependency_key")
                    .map_err(storage_error)
            })
            .collect::<CentralResult<Vec<_>>>()?;
        let mut expected_dependencies = stage.dependencies.clone();
        expected_dependencies.sort();
        if persisted_dependencies != expected_dependencies {
            return Err(storage_corruption(
                "task stage dependency projection differs from payload",
            ));
        }
        stages.push(stage);
    }
    Ok(stages)
}

async fn task_links(
    store: &SqliteAuthorityStore,
    tenant_id: &TenantId,
    task_id: &TaskId,
) -> CentralResult<Vec<TaskResourceLink>> {
    let rows = sqlx::query(
        "SELECT payload FROM task_resource_links WHERE tenant_id=? AND task_id=? ORDER BY resource_kind,resource_id,role",
    )
    .bind(tenant_id.as_str())
    .bind(task_id.as_str())
    .fetch_all(&store.pool)
    .await
    .map_err(storage_error)?;
    rows.iter()
        .map(|row| {
            let link: TaskResourceLink = decode(
                &row.try_get::<Vec<u8>, _>("payload")
                    .map_err(storage_error)?,
            )?;
            link.validate().map_err(CentralError::from)?;
            Ok(link)
        })
        .collect()
}

fn origin_name(origin: neoengram_domain::protocol::TaskOrigin) -> &'static str {
    match origin {
        neoengram_domain::protocol::TaskOrigin::User => "user",
        neoengram_domain::protocol::TaskOrigin::System => "system",
    }
}

fn resource_kind_name(kind: TaskResourceKind) -> &'static str {
    match kind {
        TaskResourceKind::Tenant => "tenant",
        TaskResourceKind::Project => "project",
        TaskResourceKind::Artifact => "artifact",
        TaskResourceKind::ObjectNamespace => "object_namespace",
        TaskResourceKind::Commit => "commit",
        TaskResourceKind::Workspace => "workspace",
        TaskResourceKind::Snapshot => "snapshot",
        TaskResourceKind::SnapshotDelivery => "snapshot_delivery",
        TaskResourceKind::StorageVolume => "storage_volume",
        TaskResourceKind::StorageEnrollment => "storage_enrollment",
        TaskResourceKind::Agent => "agent",
        TaskResourceKind::Gateway => "gateway",
        TaskResourceKind::S3AccessPoint => "s3_access_point",
        TaskResourceKind::S3Credential => "s3_credential",
        TaskResourceKind::Deletion => "deletion",
        TaskResourceKind::RetentionHold => "retention_hold",
        TaskResourceKind::Materialization => "materialization",
        TaskResourceKind::MaterializationBatch => "materialization_batch",
        TaskResourceKind::Precommit => "precommit",
        TaskResourceKind::ControlJob => "control_job",
    }
}

fn resource_role_name(role: TaskResourceRole) -> &'static str {
    match role {
        TaskResourceRole::Primary => "primary",
        TaskResourceRole::Source => "source",
        TaskResourceRole::Target => "target",
        TaskResourceRole::Related => "related",
    }
}

fn relation_name(relation: TaskRelationKind) -> &'static str {
    match relation {
        TaskRelationKind::CausedBy => "caused_by",
        TaskRelationKind::TriggeredBy => "triggered_by",
        TaskRelationKind::Supersedes => "supersedes",
    }
}

fn event_kind_name(kind: TaskEventKind) -> &'static str {
    match kind {
        TaskEventKind::Created => "created",
        TaskEventKind::StateChanged => "state_changed",
        TaskEventKind::AttemptStarted => "attempt_started",
        TaskEventKind::AttemptFinished => "attempt_finished",
        TaskEventKind::Retried => "retried",
        TaskEventKind::CancelRequested => "cancel_requested",
        TaskEventKind::Cancelled => "cancelled",
        TaskEventKind::Assigned => "assigned",
        TaskEventKind::Reported => "reported",
        TaskEventKind::ProgressUpdated => "progress_updated",
        TaskEventKind::ResourceLinked => "resource_linked",
        TaskEventKind::ResourcePublished => "resource_published",
        TaskEventKind::Failed => "failed",
    }
}

fn validate_page_size(page_size: usize) -> CentralResult<()> {
    if page_size > 500 {
        return Err(invalid(
            CentralErrorCode::ProtocolInvalid,
            "task page_size must be at most 500",
        ));
    }
    Ok(())
}

fn task_matches(task: &OperationTask, request: &TaskListRequest) -> bool {
    fn has_resource(task: &OperationTask, kind: TaskResourceKind, id: &str) -> bool {
        (task.primary_resource.resource_kind == kind && task.primary_resource.resource_id == id)
            || task
                .resource_links
                .iter()
                .any(|link| link.resource_kind == kind && link.resource_id == id)
    }
    task.tenant_id == request.tenant_id
        && request
            .project_id
            .as_ref()
            .is_none_or(|value| has_resource(task, TaskResourceKind::Project, value.as_str()))
        && request
            .artifact_id
            .as_ref()
            .is_none_or(|value| has_resource(task, TaskResourceKind::Artifact, value.as_str()))
        && request.object_namespace_id.as_ref().is_none_or(|value| {
            has_resource(task, TaskResourceKind::ObjectNamespace, value.as_str())
        })
        && request
            .commit_id
            .as_ref()
            .is_none_or(|value| has_resource(task, TaskResourceKind::Commit, &value.to_string()))
        && request
            .workspace_id
            .as_ref()
            .is_none_or(|value| has_resource(task, TaskResourceKind::Workspace, value.as_str()))
        && request
            .snapshot_id
            .as_ref()
            .is_none_or(|value| has_resource(task, TaskResourceKind::Snapshot, value.as_str()))
        && request
            .storage_volume_id
            .as_ref()
            .is_none_or(|value| has_resource(task, TaskResourceKind::StorageVolume, value.as_str()))
        && (request.intent_kinds.is_empty() || request.intent_kinds.contains(&task.intent_kind))
        && request
            .purpose
            .is_none_or(|value| task.purpose == Some(value))
        && (request.states.is_empty() || request.states.contains(&task.state))
        && request
            .created_after_unix_ms
            .is_none_or(|value| task.created_at_unix_ms >= value)
        && request
            .created_before_unix_ms
            .is_none_or(|value| task.created_at_unix_ms <= value)
        && request
            .updated_after_unix_ms
            .is_none_or(|value| task.updated_at_unix_ms >= value)
        && request
            .updated_before_unix_ms
            .is_none_or(|value| task.updated_at_unix_ms <= value)
}

fn decode_task_row(row: &SqliteRow) -> CentralResult<OperationTask> {
    let tenant = TenantId::new(
        row.try_get::<String, _>("tenant_id")
            .map_err(storage_error)?,
    )?;
    let task_id = TaskId::new(row.try_get::<String, _>("task_id").map_err(storage_error)?)?;
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let task: OperationTask = decode(&payload)?;
    // The v2 payload is authoritative for the task identity; relational projection columns below
    // are checked against it to detect partial writes or a tampered authority row.
    task.validate().map_err(CentralError::from)?;
    if task.tenant_id != tenant || task.task_id != task_id {
        return Err(storage_corruption(
            "operation task relational identity differs from payload",
        ));
    }
    let intent_kind = row
        .try_get::<String, _>("intent_kind")
        .map_err(storage_error)?
        .parse::<TaskIntent>()
        .map_err(CentralError::from)?;
    let purpose = row
        .try_get::<Option<String>, _>("purpose")
        .map_err(storage_error)?
        .map(|value| value.parse::<TaskPurpose>().map_err(CentralError::from))
        .transpose()?;
    let primary_resource_kind = parse_resource_kind(
        row.try_get::<String, _>("primary_resource_kind")
            .map_err(storage_error)?
            .as_str(),
    )?;
    let primary_resource_id = row
        .try_get::<String, _>("primary_resource_id")
        .map_err(storage_error)?;
    let execution_id: String = row.try_get("execution_id").map_err(storage_error)?;
    let execution_key_digest: Vec<u8> =
        row.try_get("execution_key_digest").map_err(storage_error)?;
    let current_stage_key: String = row.try_get("current_stage_key").map_err(storage_error)?;
    let state: String = row.try_get("state").map_err(storage_error)?;
    if intent_kind != task.intent_kind
        || purpose != task.purpose
        || primary_resource_kind != task.primary_resource.resource_kind
        || primary_resource_id != task.primary_resource.resource_id
        || execution_id != task.execution_id
        || execution_key_digest.as_slice() != task.execution_key_digest.as_bytes()
        || current_stage_key != task.current_stage_key
    {
        return Err(storage_corruption(
            "operation task v2 projection differs from payload",
        ));
    }
    if state != state_name(task.state) {
        return Err(storage_corruption(
            "operation task state projection differs from payload",
        ));
    }
    let version = parse_canonical_u64(
        row.try_get("resource_version").map_err(storage_error)?,
        "task ResourceVersion",
    )?;
    if version != task.resource_version.get() {
        return Err(storage_corruption(
            "operation task ResourceVersion projection differs from payload",
        ));
    }
    Ok(task)
}

fn decode_attempt_row(row: &SqliteRow) -> CentralResult<TaskAttempt> {
    let attempt: TaskAttempt = decode(
        &row.try_get::<Vec<u8>, _>("payload")
            .map_err(storage_error)?,
    )?;
    attempt.validate().map_err(CentralError::from)?;
    if row.try_get::<String, _>("task_id").map_err(storage_error)? != attempt.task_id.as_str()
        || row
            .try_get::<String, _>("attempt_id")
            .map_err(storage_error)?
            != attempt.attempt_id.as_str()
        || parse_canonical_u64(
            row.try_get::<String, _>("attempt").map_err(storage_error)?,
            "task attempt",
        )? != attempt.attempt.get()
        || row.try_get::<String, _>("state").map_err(storage_error)? != state_name(attempt.state)
        || row
            .try_get::<String, _>("current_stage_key")
            .map_err(storage_error)?
            != attempt.current_stage_key
        || parse_canonical_u64(
            row.try_get::<String, _>("created_at_unix_ms")
                .map_err(storage_error)?,
            "attempt created_at_unix_ms",
        )? != attempt.created_at_unix_ms.get()
        || parse_canonical_u64(
            row.try_get::<String, _>("updated_at_unix_ms")
                .map_err(storage_error)?,
            "attempt updated_at_unix_ms",
        )? != attempt.updated_at_unix_ms.get()
        || !optional_text_u64_matches(
            row,
            "started_at_unix_ms",
            attempt.started_at_unix_ms.map(|value| value.get()),
            "attempt started_at_unix_ms",
        )?
        || !optional_text_u64_matches(
            row,
            "finished_at_unix_ms",
            attempt.finished_at_unix_ms.map(|value| value.get()),
            "attempt finished_at_unix_ms",
        )?
        || parse_canonical_u64(
            row.try_get::<String, _>("resource_version")
                .map_err(storage_error)?,
            "attempt ResourceVersion",
        )? != attempt.resource_version.get()
    {
        return Err(storage_corruption(
            "task attempt relational projection differs from payload",
        ));
    }
    Ok(attempt)
}

fn decode_event_row(row: &SqliteRow) -> CentralResult<TaskEvent> {
    let event: TaskEvent = decode(
        &row.try_get::<Vec<u8>, _>("payload")
            .map_err(storage_error)?,
    )?;
    event.validate().map_err(CentralError::from)?;
    if row.try_get::<String, _>("task_id").map_err(storage_error)? != event.task_id.as_str()
        || row
            .try_get::<String, _>("event_id")
            .map_err(storage_error)?
            != event.event_id.as_str()
        || parse_canonical_u64(
            row.try_get::<String, _>("sequence")
                .map_err(storage_error)?,
            "task event sequence",
        )? != event.sequence.get()
        || parse_canonical_u64(
            row.try_get::<String, _>("attempt").map_err(storage_error)?,
            "task event attempt",
        )? != event.attempt.get()
        || row.try_get::<String, _>("kind").map_err(storage_error)? != event_kind_name(event.kind)
        || row.try_get::<String, _>("state").map_err(storage_error)? != state_name(event.state)
        || parse_canonical_u64(
            row.try_get::<String, _>("occurred_at_unix_ms")
                .map_err(storage_error)?,
            "task event occurred_at_unix_ms",
        )? != event.occurred_at_unix_ms.get()
        || parse_canonical_u64(
            row.try_get::<String, _>("resource_version")
                .map_err(storage_error)?,
            "task event ResourceVersion",
        )? != event.resource_version.get()
    {
        return Err(storage_corruption(
            "task event relational projection differs from payload",
        ));
    }
    Ok(event)
}

async fn task_rows(
    store: &SqliteAuthorityStore,
    tenant_id: &TenantId,
) -> CentralResult<Vec<OperationTask>> {
    let rows = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id = ? ORDER BY task_id")
        .bind(tenant_id.as_str()).fetch_all(&store.pool).await.map_err(storage_error)?;
    let mut tasks = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut task = decode_task_row(row)?;
        task.stages = stage_rows(store, tenant_id, &task.task_id).await?;
        task.resource_links = task_links(store, tenant_id, &task.task_id).await?;
        tasks.push(task);
    }
    Ok(tasks)
}

async fn write_task<'a>(
    tx: &mut Transaction<'a, Sqlite>,
    task: &OperationTask,
    expected_resource_version: ResourceVersion,
) -> CentralResult<()> {
    let payload = encode(task)?;
    let actor = encode(&task.actor)?;
    let issue = task.issue.as_ref().map(encode).transpose()?;
    let result = sqlx::query("UPDATE operation_tasks SET intent_kind=?, purpose=?, primary_resource_kind=?, primary_resource_id=?, execution_id=?, execution_key_digest=?, state=?, current_stage_key=?, request_id=?, request_digest=?, actor=?, attempt=?, progress_completed=?, progress_total=?, progress_completed_bytes=?, progress_total_bytes=?, deadline_unix_ms=?, issue=?, created_at_unix_ms=?, updated_at_unix_ms=?, started_at_unix_ms=?, finished_at_unix_ms=?, resource_version=?, origin=?, executable=?, payload=? WHERE tenant_id=? AND task_id=? AND resource_version=?")
        .bind(kind_name(task.intent_kind)).bind(purpose_name(task.purpose))
        .bind(resource_kind_name(task.primary_resource.resource_kind)).bind(&task.primary_resource.resource_id)
        .bind(&task.execution_id).bind(task.execution_key_digest.as_bytes().to_vec())
        .bind(state_name(task.state)).bind(&task.current_stage_key)
        .bind(task.request_id.as_str()).bind(task.request_digest.as_bytes().to_vec()).bind(actor)
        .bind(text_u64(task.attempt.get())).bind(text_u64(task.progress.completed.get())).bind(text_u64(task.progress.total.get()))
        .bind(text_u64(task.progress.completed_bytes.get())).bind(text_u64(task.progress.total_bytes.get()))
        .bind(text_u64(task.deadline_unix_ms.get())).bind(issue)
        .bind(text_u64(task.created_at_unix_ms.get())).bind(text_u64(task.updated_at_unix_ms.get()))
        .bind(task.started_at_unix_ms.map(|value| text_u64(value.get()))).bind(task.finished_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(text_u64(task.resource_version.get())).bind(origin_name(task.origin)).bind(i64::from(task.executable))
        .bind(payload).bind(task.tenant_id.as_str()).bind(task.task_id.as_str())
        .bind(text_u64(expected_resource_version.get()))
        .execute(&mut **tx).await.map_err(storage_error)?;
    if result.rows_affected() != 1 {
        return Err(invalid(
            CentralErrorCode::ConcurrentUpdate,
            "operation task resource version changed",
        ));
    }
    Ok(())
}

async fn write_attempt<'a>(
    tx: &mut Transaction<'a, Sqlite>,
    tenant_id: &TenantId,
    attempt: &TaskAttempt,
    expected_resource_version: ResourceVersion,
) -> CentralResult<()> {
    let result = sqlx::query("UPDATE task_attempts SET state=?,current_stage_key=?,created_at_unix_ms=?,updated_at_unix_ms=?,started_at_unix_ms=?,finished_at_unix_ms=?,issue=?,resource_version=?,payload=? WHERE tenant_id=? AND task_id=? AND attempt_id=? AND resource_version=?")
        .bind(state_name(attempt.state))
        .bind(&attempt.current_stage_key)
        .bind(text_u64(attempt.created_at_unix_ms.get()))
        .bind(text_u64(attempt.updated_at_unix_ms.get()))
        .bind(attempt.started_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(attempt.finished_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(attempt.issue.as_ref().map(encode).transpose()?)
        .bind(text_u64(attempt.resource_version.get()))
        .bind(encode(attempt)?)
        .bind(tenant_id.as_str())
        .bind(attempt.task_id.as_str())
        .bind(attempt.attempt_id.as_str())
        .bind(text_u64(expected_resource_version.get()))
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    if result.rows_affected() != 1 {
        return Err(invalid(
            CentralErrorCode::ConcurrentUpdate,
            "task attempt resource version changed",
        ));
    }
    Ok(())
}

async fn insert_task_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task: &OperationTask,
) -> CentralResult<()> {
    let payload = encode(task)?;
    let actor = encode(&task.actor)?;
    let issue = task.issue.as_ref().map(encode).transpose()?;
    sqlx::query("INSERT INTO operation_tasks (tenant_id,task_id,intent_kind,purpose,primary_resource_kind,primary_resource_id,execution_id,execution_key_digest,state,current_stage_key,request_id,request_digest,actor,attempt,progress_completed,progress_total,progress_completed_bytes,progress_total_bytes,deadline_unix_ms,issue,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,origin,executable,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(task.tenant_id.as_str())
        .bind(task.task_id.as_str())
        .bind(kind_name(task.intent_kind))
        .bind(purpose_name(task.purpose))
        .bind(resource_kind_name(task.primary_resource.resource_kind))
        .bind(&task.primary_resource.resource_id)
        .bind(&task.execution_id)
        .bind(task.execution_key_digest.as_bytes().to_vec())
        .bind(state_name(task.state))
        .bind(&task.current_stage_key)
        .bind(task.request_id.as_str())
        .bind(task.request_digest.as_bytes().to_vec())
        .bind(actor)
        .bind(text_u64(task.attempt.get()))
        .bind(text_u64(task.progress.completed.get()))
        .bind(text_u64(task.progress.total.get()))
        .bind(text_u64(task.progress.completed_bytes.get()))
        .bind(text_u64(task.progress.total_bytes.get()))
        .bind(text_u64(task.deadline_unix_ms.get()))
        .bind(issue)
        .bind(text_u64(task.created_at_unix_ms.get()))
        .bind(text_u64(task.updated_at_unix_ms.get()))
        .bind(task.started_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(task.finished_at_unix_ms.map(|value| text_u64(value.get())))
        .bind(text_u64(task.resource_version.get()))
        .bind(origin_name(task.origin))
        .bind(i64::from(task.executable))
        .bind(payload)
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    Ok(())
}

async fn insert_resource_link_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &TenantId,
    link: &TaskResourceLink,
) -> CentralResult<()> {
    link.validate().map_err(CentralError::from)?;
    sqlx::query("INSERT INTO task_resource_links (tenant_id,task_id,resource_kind,resource_id,role,payload) VALUES (?,?,?,?,?,?)")
        .bind(tenant_id.as_str())
        .bind(link.task_id.as_str())
        .bind(resource_kind_name(link.resource_kind))
        .bind(&link.resource_id)
        .bind(resource_role_name(link.role))
        .bind(encode(link)?)
        .execute(&mut **tx)
        .await
        .map_err(storage_error)?;
    Ok(())
}

#[async_trait]
impl TaskRepository for SqliteAuthorityStore {
    async fn get(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Option<OperationTask>> {
        let row = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
            .bind(tenant_id.as_str()).bind(task_id.as_str()).fetch_optional(&self.pool).await.map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut task = decode_task_row(&row)?;
        task.stages = stage_rows(self, tenant_id, &task.task_id).await?;
        task.resource_links = task_links(self, tenant_id, &task.task_id).await?;
        Ok(Some(task))
    }

    async fn get_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<OperationTask>> {
        // Request identities are aliases of the canonical execution row. A semantically reused
        // request therefore has a different request_id from the stored task and must be resolved
        // through the alias table before loading the task payload.
        let alias = sqlx::query(
            "SELECT task_id FROM task_request_identities WHERE tenant_id=? AND request_id=?",
        )
        .bind(tenant_id.as_str())
        .bind(request_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let row = if let Some(alias) = alias {
            let task_id: String = alias.try_get("task_id").map_err(storage_error)?;
            sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
                .bind(tenant_id.as_str())
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?
        } else {
            sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND request_id=?")
                .bind(tenant_id.as_str())
                .bind(request_id.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?
        };
        let Some(row) = row else {
            return Ok(None);
        };
        let mut task = decode_task_row(&row)?;
        task.execution_reused = task.request_id != *request_id;
        task.stages = stage_rows(self, tenant_id, &task.task_id).await?;
        task.resource_links = task_links(self, tenant_id, &task.task_id).await?;
        Ok(Some(task))
    }

    async fn list(&self, request: &TaskListRequest) -> CentralResult<TaskListPage> {
        validate_page_size(request.page_size)?;
        if request.page_size == 0 {
            return Ok(TaskListPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let cursor = request.cursor.as_deref().unwrap_or_default();
        let mut items = task_rows(self, &request.tenant_id)
            .await?
            .into_iter()
            .filter(|task| task_matches(task, request))
            .filter(|task| task.task_id.as_str() > cursor)
            .collect::<Vec<_>>();
        let has_more = items.len() > request.page_size;
        items.truncate(request.page_size);
        Ok(TaskListPage {
            next_cursor: has_more
                .then(|| items.last().map(|task| task.task_id.to_string()))
                .flatten(),
            items,
        })
    }

    async fn list_events(
        &self,
        request: &TaskEventListRequest,
    ) -> CentralResult<TaskEventListPage> {
        validate_page_size(request.page_size)?;
        if self
            .get(&request.tenant_id, &request.task_id)
            .await?
            .is_none()
        {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        if request.page_size == 0 {
            return Ok(TaskEventListPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let after = request.after_sequence.map_or(0, SequenceNumber::get);
        // Sequence is stored as canonical decimal text so it can represent the complete u64
        // range. SQLite integer casts would overflow for long-lived tasks; order and filter in
        // Rust after loading the bounded per-task event stream instead.
        let rows = sqlx::query("SELECT tenant_id,task_id,event_id,sequence,attempt,kind,state,occurred_at_unix_ms,resource_version,payload FROM task_events WHERE tenant_id=? AND task_id=? ORDER BY LENGTH(sequence), sequence")
            .bind(request.tenant_id.as_str()).bind(request.task_id.as_str())
            .fetch_all(&self.pool).await.map_err(storage_error)?;
        let mut items = rows
            .iter()
            .map(decode_event_row)
            .collect::<CentralResult<Vec<_>>>()?
            .into_iter()
            .filter(|event| event.sequence.get() > after)
            .take(request.page_size.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = items.len() > request.page_size;
        items.truncate(request.page_size);
        Ok(TaskEventListPage {
            next_cursor: has_more
                .then(|| items.last().map(|event| event.sequence.to_string()))
                .flatten(),
            items,
        })
    }

    async fn summary(&self, request: &TaskListRequest) -> CentralResult<TaskSummary> {
        let mut summary = TaskSummary::default();
        for task in task_rows(self, &request.tenant_id)
            .await?
            .into_iter()
            .filter(|task| task_matches(task, request))
        {
            summary.add(task.state);
        }
        Ok(summary)
    }

    async fn stages(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Vec<TaskStage>> {
        if self.get(tenant_id, task_id).await?.is_none() {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        stage_rows(self, tenant_id, task_id).await
    }

    async fn stage_history(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Vec<TaskStage>> {
        if self.get(tenant_id, task_id).await?.is_none() {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        let rows = sqlx::query(
            "SELECT payload FROM task_stage_history WHERE tenant_id=? AND task_id=? ORDER BY stage_key, LENGTH(stage_attempt), stage_attempt, LENGTH(resource_version), resource_version",
        )
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.iter()
            .map(|row| {
                let stage: TaskStage = decode(
                    &row.try_get::<Vec<u8>, _>("payload")
                        .map_err(storage_error)?,
                )?;
                stage.validate().map_err(CentralError::from)?;
                Ok(stage)
            })
            .collect()
    }

    async fn insert_stage(
        &self,
        tenant_id: &TenantId,
        stage: TaskStage,
    ) -> CentralResult<TaskStage> {
        stage.validate().map_err(CentralError::from)?;
        if stage.task_id.as_str().is_empty() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "task stage task_id must not be empty",
            ));
        }
        if self.get(tenant_id, &stage.task_id).await?.is_none() {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let query = format!(
            "SELECT {} FROM task_stages WHERE tenant_id=? AND task_id=? AND stage_key=?",
            stage_columns()
        );
        if let Some(row) = sqlx::query(&query)
            .bind(tenant_id.as_str())
            .bind(stage.task_id.as_str())
            .bind(&stage.stage_key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
        {
            let current = decode_stage_row(&row)?;
            let dependency_rows = sqlx::query("SELECT dependency_key FROM task_stage_dependencies WHERE tenant_id=? AND task_id=? AND stage_key=? ORDER BY dependency_key")
                .bind(tenant_id.as_str())
                .bind(stage.task_id.as_str())
                .bind(&stage.stage_key)
                .fetch_all(&mut *tx)
                .await
                .map_err(storage_error)?;
            let mut persisted_dependencies = dependency_rows
                .iter()
                .map(|row| {
                    row.try_get::<String, _>("dependency_key")
                        .map_err(storage_error)
                })
                .collect::<CentralResult<Vec<_>>>()?;
            let mut expected_dependencies = current.dependencies.clone();
            persisted_dependencies.sort();
            expected_dependencies.sort();
            if persisted_dependencies != expected_dependencies {
                return Err(storage_corruption(
                    "task stage dependency projection differs from payload",
                ));
            }
            if current == stage {
                tx.commit().await.map_err(storage_error)?;
                return Ok(current);
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task stage identity was reused",
            ));
        }
        insert_stage_tx(&mut tx, tenant_id, &stage).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(stage)
    }

    async fn replace_stage(
        &self,
        tenant_id: &TenantId,
        expected_resource_version: ResourceVersion,
        stage: TaskStage,
    ) -> CentralResult<TaskStage> {
        stage.validate().map_err(CentralError::from)?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let query = format!(
            "SELECT {} FROM task_stages WHERE tenant_id=? AND task_id=? AND stage_key=?",
            stage_columns()
        );
        let row = sqlx::query(&query)
            .bind(tenant_id.as_str())
            .bind(stage.task_id.as_str())
            .bind(&stage.stage_key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid(CentralErrorCode::ResourceNotFound, "task stage not found"))?;
        let current = decode_stage_row(&row)?;
        if current.resource_version != expected_resource_version
            || stage.resource_version.get() != expected_resource_version.get().saturating_add(1)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task stage resource version changed",
            ));
        }
        if current.task_id != stage.task_id || current.stage_key != stage.stage_key {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "immutable task stage identity changed",
            ));
        }
        write_stage(&mut tx, tenant_id, &stage, expected_resource_version).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(stage)
    }

    async fn insert(&self, task: OperationTask) -> CentralResult<TaskInsertOutcome> {
        self.insert_with_history(task, None, None).await
    }

    async fn insert_with_history(
        &self,
        task: OperationTask,
        attempt: Option<TaskAttempt>,
        event: Option<TaskEvent>,
    ) -> CentralResult<TaskInsertOutcome> {
        task.validate().map_err(CentralError::from)?;
        if let Some(value) = &attempt {
            value.validate().map_err(CentralError::from)?;
            if value.task_id != task.task_id || value.attempt != task.attempt {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "initial task attempt identity mismatch",
                ));
            }
        }
        if let Some(value) = &event {
            value.validate().map_err(CentralError::from)?;
            if value.task_id != task.task_id
                || value.attempt != task.attempt
                || value.sequence.get() != 1
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "initial task event identity mismatch",
                ));
            }
        }
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        // Request identity is intentionally separate from the canonical execution row. A request
        // that discovered an existing semantic execution gets an alias here, allowing its later
        // replay to be distinguished from another execution lookup.
        if let Some(identity_row) = sqlx::query(
            "SELECT request_digest, task_id FROM task_request_identities WHERE tenant_id=? AND request_id=?",
        )
        .bind(task.tenant_id.as_str())
        .bind(task.request_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?
        {
            let stored_digest: Vec<u8> = identity_row
                .try_get("request_digest")
                .map_err(storage_error)?;
            if stored_digest.as_slice() != task.request_digest.as_bytes() {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "request ID is already bound to a different request payload",
                ));
            }
            let canonical_task_id = TaskId::new(
                identity_row
                    .try_get::<String, _>("task_id")
                    .map_err(storage_error)?,
            )?;
            let canonical_row = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
                .bind(task.tenant_id.as_str())
                .bind(canonical_task_id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| storage_corruption("task request identity points to a missing canonical task"))?;
            let mut current = decode_task_row(&canonical_row)?;
            // Request replay is only valid for the same execution identity. The request digest
            // intentionally covers transport payload only, so an identical body sent to another
            // intent must be rejected instead of returning the wrong canonical task.
            if current.intent_kind != task.intent_kind
                || current.purpose != task.purpose
                || current.primary_resource != task.primary_resource
                || current.execution_id != task.execution_id
                || current.execution_key_digest != task.execution_key_digest
            {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "request ID is already bound to a different operation identity",
                ));
            }
            current.request_replayed = true;
            current.execution_reused = current.request_id != task.request_id;
            tx.commit().await.map_err(storage_error)?;
            return Ok(TaskInsertOutcome::Existing(current));
        }
        let existing = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
            .bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).fetch_optional(&mut *tx).await.map_err(storage_error)?;
        if let Some(row) = existing {
            let current = decode_task_row(&row)?;
            if current.request_id == task.request_id
                && current.request_digest == task.request_digest
                && current.intent_kind == task.intent_kind
            {
                sqlx::query("INSERT INTO task_request_identities (tenant_id,request_id,request_digest,task_id) VALUES (?,?,?,?) ON CONFLICT (tenant_id,request_id) DO NOTHING")
                    .bind(task.tenant_id.as_str())
                    .bind(task.request_id.as_str())
                    .bind(task.request_digest.as_bytes().to_vec())
                    .bind(current.task_id.as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(storage_error)?;
                let mut current = current;
                current.request_replayed = true;
                tx.commit().await.map_err(storage_error)?;
                return Ok(TaskInsertOutcome::Existing(current));
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "operation task identity is already bound to a different request",
            ));
        }
        if sqlx::query("SELECT 1 FROM operation_tasks WHERE tenant_id=? AND request_id=?")
            .bind(task.tenant_id.as_str())
            .bind(task.request_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .is_some()
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "request ID is already bound to another operation task",
            ));
        }
        let execution_existing = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND execution_id=?")
            .bind(task.tenant_id.as_str())
            .bind(&task.execution_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?;
        if let Some(row) = execution_existing {
            let current = decode_task_row(&row)?;
            if current.intent_kind == task.intent_kind
                && current.purpose == task.purpose
                && current.primary_resource == task.primary_resource
                && current.execution_key_digest == task.execution_key_digest
            {
                let mut reused = current;
                reused.execution_reused = true;
                reused.request_replayed = false;
                sqlx::query("INSERT INTO task_request_identities (tenant_id,request_id,request_digest,task_id) VALUES (?,?,?,?)")
                    .bind(task.tenant_id.as_str())
                    .bind(task.request_id.as_str())
                    .bind(task.request_digest.as_bytes().to_vec())
                    .bind(reused.task_id.as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(storage_error)?;
                tx.commit().await.map_err(storage_error)?;
                return Ok(TaskInsertOutcome::Existing(reused));
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "execution ID is already bound to a different operation",
            ));
        }
        insert_task_tx(&mut tx, &task).await?;
        // Persist the complete execution DAG in the same transaction as the root task. This
        // prevents schedulers from observing a task with only a partially-created plan.
        let stages = if task.stages.is_empty() {
            TaskStage::plan_for_intent(
                task.task_id.clone(),
                task.intent_kind,
                task.created_at_unix_ms,
            )
        } else {
            task.stages.clone()
        };
        neoengram_domain::protocol::validate_task_stages(&stages).map_err(CentralError::from)?;
        insert_stages_tx(&mut tx, &task.tenant_id, &stages).await?;
        for link in &task.resource_links {
            insert_resource_link_tx(&mut tx, &task.tenant_id, link).await?;
        }
        sqlx::query("INSERT INTO task_request_identities (tenant_id,request_id,request_digest,task_id) VALUES (?,?,?,?)")
            .bind(task.tenant_id.as_str())
            .bind(task.request_id.as_str())
            .bind(task.request_digest.as_bytes().to_vec())
            .bind(task.task_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
        if let Some(value) = attempt {
            insert_attempt_tx(&mut tx, &task.tenant_id, &value).await?;
        }
        if let Some(value) = event {
            sqlx::query("INSERT INTO task_events (tenant_id,task_id,event_id,sequence,attempt,kind,state,occurred_at_unix_ms,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?)").bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).bind(value.event_id.as_str()).bind(text_u64(value.sequence.get())).bind(text_u64(value.attempt.get())).bind(event_kind_name(value.kind)).bind(state_name(value.state)).bind(text_u64(value.occurred_at_unix_ms.get())).bind(text_u64(value.resource_version.get())).bind(encode(&value)?).execute(&mut *tx).await.map_err(storage_error)?;
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(TaskInsertOutcome::Inserted(task))
    }

    /// Persists a caller-supplied stage DAG in the same SQLite transaction as the root task.
    async fn insert_with_history_and_stages(
        &self,
        mut task: OperationTask,
        attempt: Option<TaskAttempt>,
        event: Option<TaskEvent>,
        stages: Vec<TaskStage>,
    ) -> CentralResult<TaskInsertOutcome> {
        neoengram_domain::protocol::validate_task_stages(&stages).map_err(CentralError::from)?;
        if stages.iter().any(|stage| stage.task_id != task.task_id) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "initial task stages do not match task identity",
            ));
        }
        task.stages = stages;
        self.insert_with_history(task, attempt, event).await
    }

    async fn replace(
        &self,
        expected_resource_version: ResourceVersion,
        task: OperationTask,
    ) -> CentralResult<OperationTask> {
        task.validate().map_err(CentralError::from)?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?").bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).fetch_optional(&mut *tx).await.map_err(storage_error)?.ok_or_else(|| invalid(CentralErrorCode::ResourceNotFound, "operation task not found"))?;
        let current = decode_task_row(&row)?;
        if current.resource_version != expected_resource_version
            || task.resource_version.get() != expected_resource_version.get().saturating_add(1)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "operation task resource version changed",
            ));
        }
        if current.request_id != task.request_id || current.request_digest != task.request_digest {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "immutable operation task identity changed",
            ));
        }
        write_task(&mut tx, &task, expected_resource_version).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(task)
    }

    async fn transition(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
        expected_resource_version: ResourceVersion,
        next: TaskState,
        actor: TaskActor,
        issue: Option<neoengram_domain::protocol::TaskIssue>,
        message: Option<String>,
        now: UnixMillis,
    ) -> CentralResult<TaskMutationOutcome> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
            .bind(tenant_id.as_str())
            .bind(task_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid(CentralErrorCode::ResourceNotFound, "operation task not found"))?;
        let current = decode_task_row(&row)?;
        if current.resource_version != expected_resource_version {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "operation task resource version changed",
            ));
        }
        if next == TaskState::Succeeded {
            let stage_rows =
                sqlx::query("SELECT payload FROM task_stages WHERE tenant_id=? AND task_id=?")
                    .bind(tenant_id.as_str())
                    .bind(task_id.as_str())
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(storage_error)?;
            let stages = stage_rows
                .iter()
                .map(|row| {
                    let stage: TaskStage = decode(
                        &row.try_get::<Vec<u8>, _>("payload")
                            .map_err(storage_error)?,
                    )?;
                    stage.validate().map_err(CentralError::from)?;
                    Ok(stage)
                })
                .collect::<CentralResult<Vec<_>>>()?;
            neoengram_domain::protocol::validate_task_stages(&stages)
                .map_err(CentralError::from)?;
            if stages.is_empty() || stages.iter().any(|stage| !stage.state.is_success()) {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "operation task cannot succeed before all required stages complete",
                ));
            }
        }
        if current.state == next {
            if current.issue == issue {
                tx.commit().await.map_err(storage_error)?;
                return Ok(TaskMutationOutcome {
                    task: current,
                    replayed: true,
                });
            }
            // A repeated stall/failure observation may update its diagnosis without changing the
            // coarse state. Persist the root and current Attempt together so their fences cannot
            // disagree after a reconnect.
            let mut task = current.clone();
            task.issue = issue;
            task.updated_at_unix_ms = UnixMillis::new(now.get().max(task.updated_at_unix_ms.get()));
            task.resource_version =
                ResourceVersion::new(task.resource_version.get().saturating_add(1));
            let attempt_row = sqlx::query("SELECT tenant_id,task_id,attempt_id,attempt,state,current_stage_key,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt=?")
                .bind(tenant_id.as_str())
                .bind(task_id.as_str())
                .bind(text_u64(task.attempt.get()))
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "operation task current attempt is missing"))?;
            let current_attempt = decode_attempt_row(&attempt_row)?;
            let mut attempt = current_attempt.clone();
            attempt.issue = task.issue.clone();
            attempt.updated_at_unix_ms =
                UnixMillis::new(now.get().max(attempt.updated_at_unix_ms.get()));
            attempt.resource_version =
                ResourceVersion::new(attempt.resource_version.get().saturating_add(1));
            task.validate().map_err(CentralError::from)?;
            attempt.validate().map_err(CentralError::from)?;
            write_task(&mut tx, &task, expected_resource_version).await?;
            write_attempt(
                &mut tx,
                tenant_id,
                &attempt,
                current_attempt.resource_version,
            )
            .await?;
            tx.commit().await.map_err(storage_error)?;
            return Ok(TaskMutationOutcome {
                task,
                replayed: false,
            });
        }

        let mut task = current.clone();
        if let Some(issue) = issue {
            task.issue = Some(issue);
        }
        task.transition_to(next, now).map_err(CentralError::from)?;
        let attempt_row = sqlx::query("SELECT tenant_id,task_id,attempt_id,attempt,state,current_stage_key,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt=?")
            .bind(tenant_id.as_str())
            .bind(task_id.as_str())
            .bind(text_u64(task.attempt.get()))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "operation task current attempt is missing"))?;
        let current_attempt = decode_attempt_row(&attempt_row)?;
        let mut attempt = current_attempt.clone();
        attempt.issue = task.issue.clone();
        attempt
            .transition_to(next, now)
            .map_err(CentralError::from)?;
        let sequence = next_event_sequence(&mut tx, tenant_id, task_id).await?;
        let mut event = TaskEvent::state_change(
            neoengram_domain::protocol::TaskEventId::new(format!(
                "{}-event-{sequence}",
                task.task_id
            ))?,
            task.task_id.clone(),
            SequenceNumber::new(sequence),
            task.attempt,
            actor,
            current.state,
            next,
            now,
            task.resource_version,
        );
        event.message = message;
        event.issue = task.issue.clone();
        event.progress = Some(task.progress);
        task.validate().map_err(CentralError::from)?;
        attempt.validate().map_err(CentralError::from)?;
        event.validate().map_err(CentralError::from)?;

        write_task(&mut tx, &task, expected_resource_version).await?;
        write_attempt(
            &mut tx,
            tenant_id,
            &attempt,
            current_attempt.resource_version,
        )
        .await?;
        insert_event_tx(&mut tx, tenant_id, &event).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(TaskMutationOutcome {
            task,
            replayed: false,
        })
    }

    async fn attempts(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Vec<TaskAttempt>> {
        if self.get(tenant_id, task_id).await?.is_none() {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        let rows = sqlx::query("SELECT tenant_id,task_id,attempt_id,attempt,state,current_stage_key,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=?")
            .bind(tenant_id.as_str())
            .bind(task_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        let mut attempts = rows
            .iter()
            .map(decode_attempt_row)
            .collect::<CentralResult<Vec<_>>>()?;
        attempts.sort_by_key(|attempt| attempt.attempt);
        Ok(attempts)
    }

    async fn insert_attempt(
        &self,
        tenant_id: &TenantId,
        attempt: TaskAttempt,
    ) -> CentralResult<TaskAttempt> {
        attempt.validate().map_err(CentralError::from)?;
        let task = self
            .get(tenant_id, &attempt.task_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "operation task not found",
                )
            })?;
        if attempt.attempt > task.attempt {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "attempt exceeds current task attempt",
            ));
        }
        let existing = sqlx::query(
            "SELECT tenant_id,task_id,attempt_id,attempt,state,current_stage_key,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt_id=?",
        )
        .bind(tenant_id.as_str())
        .bind(attempt.task_id.as_str())
        .bind(attempt.attempt_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if let Some(row) = existing {
            let current = decode_attempt_row(&row)?;
            if current == attempt {
                return Ok(current);
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task attempt ID was reused",
            ));
        }
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        insert_attempt_tx(&mut tx, tenant_id, &attempt).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(attempt)
    }

    async fn replace_attempt(
        &self,
        tenant_id: &TenantId,
        expected_resource_version: ResourceVersion,
        attempt: TaskAttempt,
    ) -> CentralResult<TaskAttempt> {
        let current = self
            .attempts(tenant_id, &attempt.task_id)
            .await?
            .into_iter()
            .find(|value| value.attempt_id == attempt.attempt_id)
            .ok_or_else(|| invalid(CentralErrorCode::ResourceNotFound, "task attempt not found"))?;
        if current.resource_version != expected_resource_version
            || attempt.resource_version.get() != expected_resource_version.get().saturating_add(1)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task attempt resource version changed",
            ));
        }
        let result = sqlx::query("UPDATE task_attempts SET state=?,current_stage_key=?,created_at_unix_ms=?,updated_at_unix_ms=?,started_at_unix_ms=?,finished_at_unix_ms=?,issue=?,resource_version=?,payload=? WHERE tenant_id=? AND task_id=? AND attempt_id=? AND resource_version=?")
            .bind(state_name(attempt.state)).bind(&attempt.current_stage_key).bind(text_u64(attempt.created_at_unix_ms.get())).bind(text_u64(attempt.updated_at_unix_ms.get())).bind(attempt.started_at_unix_ms.map(|v| text_u64(v.get()))).bind(attempt.finished_at_unix_ms.map(|v| text_u64(v.get()))).bind(attempt.issue.as_ref().map(encode).transpose()?).bind(text_u64(attempt.resource_version.get())).bind(encode(&attempt)?).bind(tenant_id.as_str()).bind(attempt.task_id.as_str()).bind(attempt.attempt_id.as_str()).bind(text_u64(expected_resource_version.get())).execute(&self.pool).await.map_err(storage_error)?;
        if result.rows_affected() != 1 {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task attempt resource version changed",
            ));
        }
        Ok(attempt)
    }

    async fn append_event(
        &self,
        tenant_id: &TenantId,
        event: TaskEvent,
    ) -> CentralResult<TaskEvent> {
        event.validate().map_err(CentralError::from)?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        if sqlx::query("SELECT 1 FROM operation_tasks WHERE tenant_id=? AND task_id=?")
            .bind(tenant_id.as_str())
            .bind(event.task_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .is_none()
        {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        if let Some(row) =
            sqlx::query("SELECT tenant_id,task_id,event_id,sequence,attempt,kind,state,occurred_at_unix_ms,resource_version,payload FROM task_events WHERE tenant_id=? AND event_id=?")
                .bind(tenant_id.as_str())
                .bind(event.event_id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?
        {
            let current = decode_event_row(&row)?;
            if current == event {
                tx.commit().await.map_err(storage_error)?;
                return Ok(current);
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task event ID was reused",
            ));
        }
        let expected = next_event_sequence(&mut tx, tenant_id, &event.task_id).await?;
        if event.sequence.get() != expected {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                format!("task event sequence must be {expected}"),
            ));
        }
        insert_event_tx(&mut tx, tenant_id, &event).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(event)
    }

    async fn link_resource(&self, record: TaskResourceLinkRecord) -> CentralResult<bool> {
        record.link.validate().map_err(CentralError::from)?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let task_row = sqlx::query("SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
            .bind(record.tenant_id.as_str())
            .bind(record.link.task_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ))?;
        let task = decode_task_row(&task_row)?;
        if let Some(row) = sqlx::query("SELECT payload FROM task_resource_links WHERE tenant_id=? AND task_id=? AND resource_kind=? AND resource_id=? AND role=?")
            .bind(record.tenant_id.as_str())
            .bind(record.link.task_id.as_str())
            .bind(resource_kind_name(record.link.resource_kind))
            .bind(&record.link.resource_id)
            .bind(resource_role_name(record.link.role))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
        {
            let existing: TaskResourceLink = decode(
                &row.try_get::<Vec<u8>, _>("payload")
                    .map_err(storage_error)?,
            )?;
            if existing != record.link {
                return Err(storage_corruption(
                    "task resource link projection differs from payload",
                ));
            }
            tx.commit().await.map_err(storage_error)?;
            return Ok(true);
        }
        sqlx::query("INSERT INTO task_resource_links (tenant_id,task_id,resource_kind,resource_id,role,payload) VALUES (?,?,?,?,?,?)")
            .bind(record.tenant_id.as_str())
            .bind(record.link.task_id.as_str())
            .bind(resource_kind_name(record.link.resource_kind))
            .bind(&record.link.resource_id)
            .bind(resource_role_name(record.link.role))
            .bind(encode(&record.link)?)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
        let sequence =
            next_event_sequence(&mut tx, &record.tenant_id, &record.link.task_id).await?;
        let event = TaskEvent {
            event_id: neoengram_domain::protocol::TaskEventId::new(format!(
                "{}-event-{sequence}",
                task.task_id
            ))?,
            task_id: task.task_id.clone(),
            sequence: SequenceNumber::new(sequence),
            attempt: task.attempt,
            kind: TaskEventKind::ResourceLinked,
            state: task.state,
            from_state: None,
            to_state: None,
            actor: task.actor,
            message: Some(
                format!(
                    "{:?}:{}:{:?}",
                    record.link.resource_kind, record.link.resource_id, record.link.role
                )
                .to_ascii_lowercase(),
            ),
            issue: task.issue,
            progress: Some(task.progress),
            occurred_at_unix_ms: task.updated_at_unix_ms,
            resource_version: task.resource_version,
        };
        event.validate().map_err(CentralError::from)?;
        insert_event_tx(&mut tx, &record.tenant_id, &event).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(false)
    }

    async fn resources(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Vec<TaskResourceLink>> {
        if self.get(tenant_id, task_id).await?.is_none() {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        let rows = sqlx::query("SELECT payload FROM task_resource_links WHERE tenant_id=? AND task_id=? ORDER BY resource_kind,resource_id,role").bind(tenant_id.as_str()).bind(task_id.as_str()).fetch_all(&self.pool).await.map_err(storage_error)?;
        rows.iter()
            .map(|row| {
                let link: TaskResourceLink = decode(
                    &row.try_get::<Vec<u8>, _>("payload")
                        .map_err(storage_error)?,
                )?;
                link.validate().map_err(CentralError::from)?;
                Ok(link)
            })
            .collect()
    }

    async fn add_relation(&self, record: TaskRelationRecord) -> CentralResult<bool> {
        record.relation.validate().map_err(CentralError::from)?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let task_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM operation_tasks WHERE tenant_id=? AND task_id IN (?,?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.relation.task_id.as_str())
        .bind(record.relation.related_task_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_error)?;
        if task_count != 2 {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        let rows = sqlx::query("SELECT payload FROM task_relations WHERE tenant_id = ?")
            .bind(record.tenant_id.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;
        let mut all = rows
            .iter()
            .map(|row| {
                decode::<TaskRelation>(
                    &row.try_get::<Vec<u8>, _>("payload")
                        .map_err(storage_error)?,
                )
            })
            .collect::<CentralResult<Vec<_>>>()?;
        if all.iter().any(|value| value == &record.relation) {
            tx.commit().await.map_err(storage_error)?;
            return Ok(true);
        }
        all.push(record.relation.clone());
        neoengram_domain::protocol::validate_task_relations(&all).map_err(CentralError::from)?;
        sqlx::query("INSERT INTO task_relations (tenant_id,task_id,related_task_id,relation,payload) VALUES (?,?,?,?,?)")
            .bind(record.tenant_id.as_str())
            .bind(record.relation.task_id.as_str())
            .bind(record.relation.related_task_id.as_str())
            .bind(relation_name(record.relation.relation))
            .bind(encode(&record.relation)?)
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                if error.to_string().contains("UNIQUE") {
                    invalid(
                        CentralErrorCode::ConcurrentUpdate,
                        "task relation already exists",
                    )
                } else {
                    storage_error(error)
                }
            })?;
        tx.commit().await.map_err(storage_error)?;
        Ok(false)
    }

    async fn relations(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Vec<TaskRelation>> {
        if self.get(tenant_id, task_id).await?.is_none() {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        let rows = sqlx::query("SELECT payload FROM task_relations WHERE tenant_id=? AND task_id=? ORDER BY related_task_id,relation").bind(tenant_id.as_str()).bind(task_id.as_str()).fetch_all(&self.pool).await.map_err(storage_error)?;
        rows.iter()
            .map(|row| {
                let relation: TaskRelation = decode(
                    &row.try_get::<Vec<u8>, _>("payload")
                        .map_err(storage_error)?,
                )?;
                relation.validate().map_err(CentralError::from)?;
                Ok(relation)
            })
            .collect()
    }

    async fn retry(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
        expected_resource_version: ResourceVersion,
        actor: TaskActor,
        now: UnixMillis,
    ) -> CentralResult<TaskMutationOutcome> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?",
        )
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
        let current = decode_task_row(&row)?;
        if current.resource_version != expected_resource_version {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "operation task resource version changed",
            ));
        }
        let mut task = current.clone();
        task.retry(now).map_err(CentralError::from)?;
        let stage_rows = sqlx::query("SELECT payload FROM task_stages WHERE tenant_id=? AND task_id=? ORDER BY LENGTH(ordinal), ordinal, stage_key")
            .bind(tenant_id.as_str())
            .bind(task_id.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;
        if stage_rows.is_empty() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "operation task has no stage plan",
            ));
        }
        let mut stages = Vec::with_capacity(stage_rows.len());
        for row in &stage_rows {
            let mut stage: TaskStage = decode(
                &row.try_get::<Vec<u8>, _>("payload")
                    .map_err(storage_error)?,
            )?;
            stage.reset_for_retry(now).map_err(CentralError::from)?;
            write_stage(
                &mut tx,
                tenant_id,
                &stage,
                ResourceVersion::new(stage.resource_version.get().saturating_sub(1)),
            )
            .await?;
            stages.push(stage);
        }
        task.stages = stages;
        let attempt = TaskAttempt::new(
            task.task_id.clone(),
            neoengram_domain::protocol::TaskAttemptId::new(format!(
                "{}-attempt-{}",
                task.task_id, task.attempt
            ))?,
            task.attempt,
            now,
        );
        let sequence = next_event_sequence(&mut tx, tenant_id, task_id).await?;
        let event = TaskEvent {
            event_id: neoengram_domain::protocol::TaskEventId::new(format!(
                "{}-event-{}",
                task.task_id, sequence
            ))?,
            task_id: task.task_id.clone(),
            sequence: SequenceNumber::new(sequence),
            attempt: task.attempt,
            kind: neoengram_domain::protocol::TaskEventKind::Retried,
            state: task.state,
            from_state: Some(current.state),
            to_state: Some(task.state),
            actor,
            message: None,
            issue: None,
            progress: Some(task.progress),
            occurred_at_unix_ms: now,
            resource_version: task.resource_version,
        };
        task.validate().map_err(CentralError::from)?;
        attempt.validate().map_err(CentralError::from)?;
        event.validate().map_err(CentralError::from)?;
        write_task(&mut tx, &task, expected_resource_version).await?;
        insert_attempt_tx(&mut tx, tenant_id, &attempt).await?;
        insert_event_tx(&mut tx, tenant_id, &event).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(TaskMutationOutcome {
            task,
            replayed: false,
        })
    }

    async fn cancel(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
        expected_resource_version: Option<ResourceVersion>,
        actor: TaskActor,
        now: UnixMillis,
    ) -> CentralResult<TaskMutationOutcome> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?",
        )
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
        let current = decode_task_row(&row)?;
        if let Some(expected) = expected_resource_version {
            if current.resource_version != expected {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "operation task resource version changed",
                ));
            }
        }
        if matches!(current.state, TaskState::Cancelling | TaskState::Cancelled) {
            tx.commit().await.map_err(storage_error)?;
            return Ok(TaskMutationOutcome {
                task: current,
                replayed: true,
            });
        }
        let mut task = current.clone();
        task.cancel(now).map_err(CentralError::from)?;
        let stage_rows = sqlx::query("SELECT payload FROM task_stages WHERE tenant_id=? AND task_id=? ORDER BY LENGTH(ordinal), ordinal, stage_key")
            .bind(tenant_id.as_str())
            .bind(task_id.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;
        let mut stages = Vec::with_capacity(stage_rows.len());
        for row in &stage_rows {
            let mut stage: TaskStage = decode(
                &row.try_get::<Vec<u8>, _>("payload")
                    .map_err(storage_error)?,
            )?;
            if !stage.state.is_terminal() {
                let expected = stage.resource_version;
                stage
                    .transition_to(neoengram_domain::protocol::StageState::Cancelling, now)
                    .map_err(CentralError::from)?;
                write_stage(&mut tx, tenant_id, &stage, expected).await?;
            }
            stages.push(stage);
        }
        task.stages = stages;
        let attempt_row = sqlx::query("SELECT tenant_id,task_id,attempt_id,attempt,state,current_stage_key,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt=?")
            .bind(tenant_id.as_str())
            .bind(task_id.as_str())
            .bind(text_u64(task.attempt.get()))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "operation task current attempt is missing"))?;
        let current_attempt = decode_attempt_row(&attempt_row)?;
        let mut attempt = current_attempt.clone();
        // Cancellation first fences all new work as cancelling. A reconciliation pass moves the
        // attempt and root task to cancelled after Agent leases and disk operations converge.
        attempt
            .transition_to(TaskState::Cancelling, now)
            .map_err(CentralError::from)?;
        let sequence = next_event_sequence(&mut tx, tenant_id, task_id).await?;
        let event = TaskEvent::state_change(
            neoengram_domain::protocol::TaskEventId::new(format!(
                "{}-event-{}",
                task.task_id, sequence
            ))?,
            task.task_id.clone(),
            SequenceNumber::new(sequence),
            task.attempt,
            actor,
            current.state,
            task.state,
            now,
            task.resource_version,
        );
        let mut event = event;
        event.kind = neoengram_domain::protocol::TaskEventKind::CancelRequested;
        task.validate().map_err(CentralError::from)?;
        attempt.validate().map_err(CentralError::from)?;
        event.validate().map_err(CentralError::from)?;
        write_task(&mut tx, &task, current.resource_version).await?;
        write_attempt(
            &mut tx,
            tenant_id,
            &attempt,
            current_attempt.resource_version,
        )
        .await?;
        insert_event_tx(&mut tx, tenant_id, &event).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(TaskMutationOutcome {
            task,
            replayed: false,
        })
    }

    async fn complete_cancellation(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
        expected_resource_version: ResourceVersion,
        actor: TaskActor,
        now: UnixMillis,
    ) -> CentralResult<TaskMutationOutcome> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT tenant_id, task_id, intent_kind, purpose, primary_resource_kind, primary_resource_id, execution_id, execution_key_digest, state, current_stage_key, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?",
        )
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
        let current = decode_task_row(&row)?;
        if current.resource_version != expected_resource_version {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "operation task resource version changed",
            ));
        }
        if current.state == TaskState::Cancelled {
            tx.commit().await.map_err(storage_error)?;
            return Ok(TaskMutationOutcome {
                task: current,
                replayed: true,
            });
        }
        if current.state != TaskState::Cancelling {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "operation task must be cancelling before cancellation can complete",
            ));
        }

        let stage_rows = sqlx::query(
            "SELECT payload FROM task_stages WHERE tenant_id=? AND task_id=? ORDER BY LENGTH(ordinal), ordinal, stage_key",
        )
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(storage_error)?;
        if stage_rows.is_empty() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "operation task has no stage plan",
            ));
        }
        let mut stages = Vec::with_capacity(stage_rows.len());
        for row in &stage_rows {
            let mut stage: TaskStage = decode(
                &row.try_get::<Vec<u8>, _>("payload")
                    .map_err(storage_error)?,
            )?;
            if stage.state == neoengram_domain::protocol::StageState::Cancelling {
                let expected = stage.resource_version;
                stage
                    .transition_to(neoengram_domain::protocol::StageState::Cancelled, now)
                    .map_err(CentralError::from)?;
                write_stage(&mut tx, tenant_id, &stage, expected).await?;
            } else if !stage.state.is_terminal() {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "operation task stage has not reached the cancellation fence",
                ));
            }
            stages.push(stage);
        }
        neoengram_domain::protocol::validate_task_stages(&stages).map_err(CentralError::from)?;

        let attempt_row = sqlx::query(
            "SELECT tenant_id,task_id,attempt_id,attempt,state,current_stage_key,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt=?",
        )
        .bind(tenant_id.as_str())
        .bind(task_id.as_str())
        .bind(text_u64(current.attempt.get()))
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "operation task current attempt is missing",
            )
        })?;
        let current_attempt = decode_attempt_row(&attempt_row)?;
        if current_attempt.state != TaskState::Cancelling {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "operation task current attempt has not reached the cancellation fence",
            ));
        }
        let mut attempt = current_attempt.clone();
        attempt
            .transition_to(TaskState::Cancelled, now)
            .map_err(CentralError::from)?;

        let mut task = current.clone();
        task.complete_cancellation(now)
            .map_err(CentralError::from)?;
        task.stages = stages;
        let sequence = next_event_sequence(&mut tx, tenant_id, task_id).await?;
        let event = TaskEvent {
            event_id: neoengram_domain::protocol::TaskEventId::new(format!(
                "{}-event-{sequence}",
                task.task_id
            ))?,
            task_id: task.task_id.clone(),
            sequence: SequenceNumber::new(sequence),
            attempt: task.attempt,
            kind: neoengram_domain::protocol::TaskEventKind::Cancelled,
            state: TaskState::Cancelled,
            from_state: Some(current.state),
            to_state: Some(TaskState::Cancelled),
            actor,
            message: Some("cancellation convergence completed".to_owned()),
            issue: None,
            progress: Some(task.progress),
            occurred_at_unix_ms: now,
            resource_version: task.resource_version,
        };
        task.validate().map_err(CentralError::from)?;
        attempt.validate().map_err(CentralError::from)?;
        event.validate().map_err(CentralError::from)?;
        write_task(&mut tx, &task, expected_resource_version).await?;
        write_attempt(
            &mut tx,
            tenant_id,
            &attempt,
            current_attempt.resource_version,
        )
        .await?;
        insert_event_tx(&mut tx, tenant_id, &event).await?;
        tx.commit().await.map_err(storage_error)?;
        Ok(TaskMutationOutcome {
            task,
            replayed: false,
        })
    }
}
