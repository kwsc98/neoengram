//! SQLite mapper for the unified operation-task authority.
//!
//! Task rows keep a normalized query projection alongside a versioned JSON payload. The payload
//! is the canonical representation and is decoded/validated on every read; the projection only
//! exists to make the database schema auditable and to support indexed filters.

use async_trait::async_trait;
use neoengram_domain::protocol::{
    OperationTask, RequestId, ResourceVersion, SequenceNumber, TaskActor, TaskAttempt, TaskEvent,
    TaskEventKind, TaskId, TaskKind, TaskRelation, TaskRelationKind, TaskResourceKind,
    TaskResourceLink, TaskResourceRole, TaskState, TenantId, UnixMillis,
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
    sqlx::query("INSERT INTO task_attempts (tenant_id,task_id,attempt_id,attempt,state,phase,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,issue,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(tenant_id.as_str())
        .bind(attempt.task_id.as_str())
        .bind(attempt.attempt_id.as_str())
        .bind(text_u64(attempt.attempt.get()))
        .bind(state_name(attempt.state))
        .bind(&attempt.phase)
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
        TaskState::Cancelled => "cancelled",
    }
}

fn kind_name(kind: TaskKind) -> &'static str {
    kind.as_str()
}

fn origin_name(origin: neoengram_domain::protocol::TaskOrigin) -> &'static str {
    match origin {
        neoengram_domain::protocol::TaskOrigin::User => "user",
        neoengram_domain::protocol::TaskOrigin::System => "system",
        neoengram_domain::protocol::TaskOrigin::Legacy => "legacy",
    }
}

fn resource_kind_name(kind: TaskResourceKind) -> &'static str {
    match kind {
        TaskResourceKind::Tenant => "tenant",
        TaskResourceKind::Project => "project",
        TaskResourceKind::Artifact => "artifact",
        TaskResourceKind::ObjectNamespace => "object_namespace",
        TaskResourceKind::Commit => "commit",
        TaskResourceKind::Playground => "playground",
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
        TaskRelationKind::Parent => "parent",
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
    fn optional<T: PartialEq>(actual: &Option<T>, expected: &Option<T>) -> bool {
        expected
            .as_ref()
            .is_none_or(|value| actual.as_ref() == Some(value))
    }
    task.tenant_id == request.tenant_id
        && optional(&task.project_id, &request.project_id)
        && optional(&task.artifact_id, &request.artifact_id)
        && optional(&task.object_namespace_id, &request.object_namespace_id)
        && request
            .commit_id
            .is_none_or(|value| task.commit_id == Some(value))
        && optional(&task.playground_id, &request.playground_id)
        && optional(&task.snapshot_id, &request.snapshot_id)
        && optional(&task.storage_volume_id, &request.storage_volume_id)
        && (request.task_kinds.is_empty() || request.task_kinds.contains(&task.task_kind))
        && (request.states.is_empty() || request.states.contains(&task.state))
        && optional(&task.parent_task_id, &request.parent_task_id)
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
    task.validate().map_err(CentralError::from)?;
    if task.tenant_id != tenant || task.task_id != task_id {
        return Err(storage_corruption(
            "operation task relational identity differs from payload",
        ));
    }
    let state: String = row.try_get("state").map_err(storage_error)?;
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
        || row.try_get::<String, _>("phase").map_err(storage_error)? != attempt.phase
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
    let rows = sqlx::query("SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id = ? ORDER BY task_id")
        .bind(tenant_id.as_str()).fetch_all(&store.pool).await.map_err(storage_error)?;
    rows.iter().map(decode_task_row).collect()
}

async fn write_task<'a>(
    tx: &mut Transaction<'a, Sqlite>,
    task: &OperationTask,
    expected_resource_version: ResourceVersion,
) -> CentralResult<()> {
    let payload = encode(task)?;
    let actor = encode(&task.actor)?;
    let issue = task.issue.as_ref().map(encode).transpose()?;
    let result = sqlx::query("UPDATE operation_tasks SET task_kind=?, state=?, phase=?, project_id=?, artifact_id=?, object_namespace_id=?, commit_id=?, playground_id=?, snapshot_id=?, storage_volume_id=?, parent_task_id=?, request_id=?, request_digest=?, actor=?, attempt=?, progress_completed=?, progress_total=?, progress_completed_bytes=?, progress_total_bytes=?, detail_kind=?, detail_id=?, deadline_unix_ms=?, issue=?, created_at_unix_ms=?, updated_at_unix_ms=?, started_at_unix_ms=?, finished_at_unix_ms=?, resource_version=?, origin=?, executable=?, payload=? WHERE tenant_id=? AND task_id=? AND resource_version=?")
        .bind(kind_name(task.task_kind)).bind(state_name(task.state)).bind(&task.phase)
        .bind(task.project_id.as_ref().map(ToString::to_string)).bind(task.artifact_id.as_ref().map(ToString::to_string))
        .bind(task.object_namespace_id.as_ref().map(ToString::to_string)).bind(task.commit_id.map(|id| id.as_bytes().to_vec()))
        .bind(task.playground_id.as_ref().map(ToString::to_string)).bind(task.snapshot_id.as_ref().map(ToString::to_string))
        .bind(task.storage_volume_id.as_ref().map(ToString::to_string)).bind(task.parent_task_id.as_ref().map(ToString::to_string))
        .bind(task.request_id.as_str()).bind(task.request_digest.as_bytes().to_vec()).bind(actor)
        .bind(text_u64(task.attempt.get())).bind(text_u64(task.progress_summary.completed.get())).bind(text_u64(task.progress_summary.total.get()))
        .bind(text_u64(task.progress_summary.completed_bytes.get())).bind(text_u64(task.progress_summary.total_bytes.get()))
        .bind(task.detail_kind.clone()).bind(task.detail_id.clone()).bind(text_u64(task.deadline_unix_ms.get())).bind(issue)
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
    let result = sqlx::query("UPDATE task_attempts SET state=?,phase=?,created_at_unix_ms=?,updated_at_unix_ms=?,started_at_unix_ms=?,finished_at_unix_ms=?,issue=?,resource_version=?,payload=? WHERE tenant_id=? AND task_id=? AND attempt_id=? AND resource_version=?")
        .bind(state_name(attempt.state))
        .bind(&attempt.phase)
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

#[async_trait]
impl TaskRepository for SqliteAuthorityStore {
    async fn get(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Option<OperationTask>> {
        sqlx::query("SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
            .bind(tenant_id.as_str()).bind(task_id.as_str()).fetch_optional(&self.pool).await.map_err(storage_error)?.map(|row| decode_task_row(&row)).transpose()
    }

    async fn get_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<OperationTask>> {
        sqlx::query("SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND request_id=?")
            .bind(tenant_id.as_str()).bind(request_id.as_str()).fetch_optional(&self.pool).await.map_err(storage_error)?.map(|row| decode_task_row(&row)).transpose()
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
        let payload = encode(&task)?;
        let actor = encode(&task.actor)?;
        let issue = task.issue.as_ref().map(encode).transpose()?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let existing = sqlx::query("SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
            .bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).fetch_optional(&mut *tx).await.map_err(storage_error)?;
        if let Some(row) = existing {
            let current = decode_task_row(&row)?;
            if current.request_id == task.request_id
                && current.request_digest == task.request_digest
                && current.task_kind == task.task_kind
            {
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
        if let Some(parent_task_id) = &task.parent_task_id {
            if sqlx::query("SELECT 1 FROM operation_tasks WHERE tenant_id=? AND task_id=?")
                .bind(task.tenant_id.as_str())
                .bind(parent_task_id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?
                .is_none()
            {
                return Err(invalid(
                    CentralErrorCode::ResourceNotFound,
                    "parent operation task does not exist",
                ));
            }
        }
        sqlx::query("INSERT INTO operation_tasks (tenant_id,task_id,task_kind,state,phase,project_id,artifact_id,object_namespace_id,commit_id,playground_id,snapshot_id,storage_volume_id,parent_task_id,request_id,request_digest,actor,attempt,progress_completed,progress_total,progress_completed_bytes,progress_total_bytes,detail_kind,detail_id,deadline_unix_ms,issue,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,origin,executable,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).bind(kind_name(task.task_kind)).bind(state_name(task.state)).bind(&task.phase)
            .bind(task.project_id.as_ref().map(ToString::to_string)).bind(task.artifact_id.as_ref().map(ToString::to_string)).bind(task.object_namespace_id.as_ref().map(ToString::to_string)).bind(task.commit_id.map(|id| id.as_bytes().to_vec())).bind(task.playground_id.as_ref().map(ToString::to_string)).bind(task.snapshot_id.as_ref().map(ToString::to_string)).bind(task.storage_volume_id.as_ref().map(ToString::to_string)).bind(task.parent_task_id.as_ref().map(ToString::to_string)).bind(task.request_id.as_str()).bind(task.request_digest.as_bytes().to_vec()).bind(actor).bind(text_u64(task.attempt.get())).bind(text_u64(task.progress_summary.completed.get())).bind(text_u64(task.progress_summary.total.get())).bind(text_u64(task.progress_summary.completed_bytes.get())).bind(text_u64(task.progress_summary.total_bytes.get())).bind(task.detail_kind.clone()).bind(task.detail_id.clone()).bind(text_u64(task.deadline_unix_ms.get())).bind(issue).bind(text_u64(task.created_at_unix_ms.get())).bind(text_u64(task.updated_at_unix_ms.get())).bind(task.started_at_unix_ms.map(|value| text_u64(value.get()))).bind(task.finished_at_unix_ms.map(|value| text_u64(value.get()))).bind(text_u64(task.resource_version.get())).bind(origin_name(task.origin)).bind(i64::from(task.executable)).bind(payload).execute(&mut *tx).await.map_err(storage_error)?;
        if let Some(parent_task_id) = &task.parent_task_id {
            let relation = TaskRelation {
                task_id: task.task_id.clone(),
                related_task_id: parent_task_id.clone(),
                relation: TaskRelationKind::Parent,
            };
            relation.validate().map_err(CentralError::from)?;
            sqlx::query("INSERT INTO task_relations (tenant_id,task_id,related_task_id,relation,payload) VALUES (?,?,?,?,?)")
                .bind(task.tenant_id.as_str())
                .bind(task.task_id.as_str())
                .bind(parent_task_id.as_str())
                .bind(relation_name(relation.relation))
                .bind(encode(&relation)?)
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?;
        }
        if let Some(value) = attempt {
            sqlx::query("INSERT INTO task_attempts (tenant_id,task_id,attempt_id,attempt,state,phase,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,issue,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)").bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).bind(value.attempt_id.as_str()).bind(text_u64(value.attempt.get())).bind(state_name(value.state)).bind(&value.phase).bind(text_u64(value.created_at_unix_ms.get())).bind(text_u64(value.updated_at_unix_ms.get())).bind(value.started_at_unix_ms.map(|v| text_u64(v.get()))).bind(value.finished_at_unix_ms.map(|v| text_u64(v.get()))).bind(value.issue.as_ref().map(encode).transpose()?).bind(text_u64(value.resource_version.get())).bind(encode(&value)?).execute(&mut *tx).await.map_err(storage_error)?;
        }
        if let Some(value) = event {
            sqlx::query("INSERT INTO task_events (tenant_id,task_id,event_id,sequence,attempt,kind,state,occurred_at_unix_ms,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?)").bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).bind(value.event_id.as_str()).bind(text_u64(value.sequence.get())).bind(text_u64(value.attempt.get())).bind(event_kind_name(value.kind)).bind(state_name(value.state)).bind(text_u64(value.occurred_at_unix_ms.get())).bind(text_u64(value.resource_version.get())).bind(encode(&value)?).execute(&mut *tx).await.map_err(storage_error)?;
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(TaskInsertOutcome::Inserted(task))
    }

    async fn replace(
        &self,
        expected_resource_version: ResourceVersion,
        task: OperationTask,
    ) -> CentralResult<OperationTask> {
        task.validate().map_err(CentralError::from)?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query("SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?").bind(task.tenant_id.as_str()).bind(task.task_id.as_str()).fetch_optional(&mut *tx).await.map_err(storage_error)?.ok_or_else(|| invalid(CentralErrorCode::ResourceNotFound, "operation task not found"))?;
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
        let row = sqlx::query("SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
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
        if current.state == next {
            tx.commit().await.map_err(storage_error)?;
            return Ok(TaskMutationOutcome {
                task: current,
                replayed: true,
            });
        }

        let mut task = current.clone();
        if let Some(issue) = issue {
            task.issue = Some(issue);
        }
        task.transition_to(next, now).map_err(CentralError::from)?;
        let attempt_row = sqlx::query("SELECT tenant_id,task_id,attempt_id,attempt,state,phase,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt=?")
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
        event.progress = Some(task.progress_summary);
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
        let rows = sqlx::query("SELECT tenant_id,task_id,attempt_id,attempt,state,phase,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=?")
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
            "SELECT tenant_id,task_id,attempt_id,attempt,state,phase,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt_id=?",
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
        sqlx::query("INSERT INTO task_attempts (tenant_id,task_id,attempt_id,attempt,state,phase,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,issue,resource_version,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)").bind(tenant_id.as_str()).bind(attempt.task_id.as_str()).bind(attempt.attempt_id.as_str()).bind(text_u64(attempt.attempt.get())).bind(state_name(attempt.state)).bind(&attempt.phase).bind(text_u64(attempt.created_at_unix_ms.get())).bind(text_u64(attempt.updated_at_unix_ms.get())).bind(attempt.started_at_unix_ms.map(|v| text_u64(v.get()))).bind(attempt.finished_at_unix_ms.map(|v| text_u64(v.get()))).bind(attempt.issue.as_ref().map(encode).transpose()?).bind(text_u64(attempt.resource_version.get())).bind(encode(&attempt)?).execute(&self.pool).await.map_err(storage_error)?;
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
        let result = sqlx::query("UPDATE task_attempts SET state=?,phase=?,created_at_unix_ms=?,updated_at_unix_ms=?,started_at_unix_ms=?,finished_at_unix_ms=?,issue=?,resource_version=?,payload=? WHERE tenant_id=? AND task_id=? AND attempt_id=? AND resource_version=?")
            .bind(state_name(attempt.state)).bind(&attempt.phase).bind(text_u64(attempt.created_at_unix_ms.get())).bind(text_u64(attempt.updated_at_unix_ms.get())).bind(attempt.started_at_unix_ms.map(|v| text_u64(v.get()))).bind(attempt.finished_at_unix_ms.map(|v| text_u64(v.get()))).bind(attempt.issue.as_ref().map(encode).transpose()?).bind(text_u64(attempt.resource_version.get())).bind(encode(&attempt)?).bind(tenant_id.as_str()).bind(attempt.task_id.as_str()).bind(attempt.attempt_id.as_str()).bind(text_u64(expected_resource_version.get())).execute(&self.pool).await.map_err(storage_error)?;
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
        let task_row = sqlx::query("SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?")
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
            progress: Some(task.progress_summary),
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
            "SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?",
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
            progress: Some(task.progress_summary),
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
            "SELECT tenant_id, task_id, state, resource_version, payload FROM operation_tasks WHERE tenant_id=? AND task_id=?",
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
        if current.state == TaskState::Cancelled {
            tx.commit().await.map_err(storage_error)?;
            return Ok(TaskMutationOutcome {
                task: current,
                replayed: true,
            });
        }
        let mut task = current.clone();
        task.cancel(now).map_err(CentralError::from)?;
        let attempt_row = sqlx::query("SELECT tenant_id,task_id,attempt_id,attempt,state,phase,created_at_unix_ms,updated_at_unix_ms,started_at_unix_ms,finished_at_unix_ms,resource_version,payload FROM task_attempts WHERE tenant_id=? AND task_id=? AND attempt=?")
            .bind(tenant_id.as_str())
            .bind(task_id.as_str())
            .bind(text_u64(task.attempt.get()))
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "operation task current attempt is missing"))?;
        let current_attempt = decode_attempt_row(&attempt_row)?;
        let mut attempt = current_attempt.clone();
        attempt
            .transition_to(TaskState::Cancelled, now)
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
}
