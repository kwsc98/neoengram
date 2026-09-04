//! Public service for the unified operation-task and audit API.

use std::{str::FromStr, sync::Arc};

use neoengram_domain::{
    core::CommitId,
    protocol::{
        Generation, OperationTask, ResourceVersion, TaskActor, TaskAttempt, TaskEvent,
        TaskEventKind, TaskId, TaskIssue, TaskKind, TaskProgressSummary, TaskScope, TaskState,
        TenantId, UnixMillis,
    },
};

use crate::{
    dto::{
        CancelTaskRequest, QueryTaskEventListRequest, QueryTaskEventListResponse,
        QueryTaskListRequest, QueryTaskListResponse, QueryTaskRequest, QueryTaskResponse,
        QueryTaskSummaryRequest, QueryTaskSummaryResponse, RetryTaskRequest, TaskAttemptView,
        TaskEventView, TaskIssueView, TaskMutationResponse, TaskProgressView, TaskSummaryView,
        TaskView,
    },
    error::{invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy},
    CentralError, Clock, TaskEventListRequest, TaskInsertOutcome, TaskListRequest, TaskRepository,
    TaskSummary,
};

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 500;
const TASK_DEADLINE_MS: u64 = 24 * 60 * 60 * 1_000;

/// Central coordinator for the identity and coarse lifecycle of every mutating operation.
///
/// Domain services still own their detail records and invariants.  This component only creates
/// the stable task/attempt/audit envelope and performs CAS-guarded lifecycle transitions so all
/// write paths expose the same operational identity.
pub struct TaskCoordinator {
    repository: Arc<dyn TaskRepository>,
    clock: Arc<dyn Clock>,
}

impl TaskCoordinator {
    #[must_use]
    pub fn new(repository: Arc<dyn TaskRepository>, clock: Arc<dyn Clock>) -> Self {
        Self { repository, clock }
    }

    #[must_use]
    pub fn repository(&self) -> Arc<dyn TaskRepository> {
        self.repository.clone()
    }

    #[allow(clippy::too_many_arguments)]
    fn build_task<T: serde::Serialize>(
        &self,
        kind: TaskKind,
        scope: TaskScope,
        request_id: neoengram_domain::protocol::RequestId,
        request: &T,
        actor: TaskActor,
        parent_task_id: Option<TaskId>,
        detail_kind: Option<&str>,
        detail_id: Option<&str>,
    ) -> Result<(OperationTask, TaskAttempt, TaskEvent), CentralError> {
        let request_digest = neoengram_domain::jcs_blake3(request).map_err(CentralError::from)?;
        let task_id_digest = blake3::hash(format!("operation-task\0{request_id}").as_bytes());
        let task_id =
            TaskId::new(format!("task-{}", task_id_digest.to_hex())).map_err(CentralError::from)?;
        let now = self.clock.now();
        // InMemory test clocks intentionally start at zero, while task timestamps are strictly
        // positive by protocol. Keep that adapter detail local to task creation.
        let created_at = UnixMillis::new(now.get().max(1));
        let deadline = UnixMillis::new(
            created_at
                .get()
                .saturating_add(TASK_DEADLINE_MS)
                .max(created_at.get().saturating_add(1)),
        );
        let mut task = OperationTask::new(
            task_id.clone(),
            kind,
            scope,
            request_id,
            request_digest,
            actor.clone(),
            created_at,
            deadline,
        );
        task.parent_task_id = parent_task_id;
        task.detail_kind = detail_kind.map(str::to_owned);
        task.detail_id = detail_id.map(str::to_owned);
        let attempt_id =
            neoengram_domain::protocol::TaskAttemptId::new(format!("{}-attempt-1", task_id))
                .map_err(CentralError::from)?;
        let attempt = TaskAttempt::new(task_id.clone(), attempt_id, Generation::new(1), created_at);
        let event = TaskEvent {
            event_id: neoengram_domain::protocol::TaskEventId::new(format!("{}-event-1", task_id))
                .map_err(CentralError::from)?,
            task_id,
            sequence: neoengram_domain::protocol::SequenceNumber::new(1),
            attempt: Generation::new(1),
            kind: TaskEventKind::Created,
            state: TaskState::Queued,
            from_state: None,
            to_state: None,
            actor,
            message: None,
            issue: None,
            progress: Some(TaskProgressSummary::default()),
            occurred_at_unix_ms: created_at,
            resource_version: ResourceVersion::new(1),
        };
        Ok((task, attempt, event))
    }

    /// Creates an idempotent root task for one external request.  The task ID is derived from the
    /// request ID, while the digest covers the complete request payload; reusing an ID with a
    /// different payload is therefore rejected by the authority instead of silently replayed.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_root<T: serde::Serialize>(
        &self,
        kind: TaskKind,
        scope: TaskScope,
        request_id: neoengram_domain::protocol::RequestId,
        request: &T,
        actor: TaskActor,
        detail_kind: Option<&str>,
        detail_id: Option<&str>,
    ) -> Result<(OperationTask, bool), CentralError> {
        let (task, attempt, event) = self.build_task(
            kind,
            scope,
            request_id,
            request,
            actor,
            None,
            detail_kind,
            detail_id,
        )?;
        let outcome = self
            .repository
            .insert_with_history(task, Some(attempt), Some(event))
            .await?;
        Ok(match outcome {
            TaskInsertOutcome::Inserted(task) => (task, false),
            TaskInsertOutcome::Existing(task) => (task, true),
        })
    }

    /// Creates an executable child task while preserving the parent task identity.  The task
    /// row is first inserted with the same idempotency guarantees as a root request, then fenced
    /// with its parent link before it becomes visible to schedulers.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_child<T: serde::Serialize>(
        &self,
        parent: &OperationTask,
        kind: TaskKind,
        scope: TaskScope,
        request_id: neoengram_domain::protocol::RequestId,
        request: &T,
        actor: TaskActor,
        detail_kind: Option<&str>,
        detail_id: Option<&str>,
    ) -> Result<(OperationTask, bool), CentralError> {
        if parent.tenant_id != scope.tenant_id {
            return Err(CentralError::new(
                crate::CentralErrorCode::InvalidState,
                "child task tenant does not match parent",
            ));
        }
        let (task, attempt, event) = self.build_task(
            kind,
            scope,
            request_id,
            request,
            actor,
            Some(parent.task_id.clone()),
            detail_kind,
            detail_id,
        )?;
        let outcome = self
            .repository
            .insert_with_history(task, Some(attempt), Some(event))
            .await?;
        match outcome {
            TaskInsertOutcome::Inserted(task) => Ok((task, false)),
            TaskInsertOutcome::Existing(task)
                if task.parent_task_id.as_ref() == Some(&parent.task_id) =>
            {
                Ok((task, true))
            }
            TaskInsertOutcome::Existing(_) => Err(CentralError::new(
                crate::CentralErrorCode::InvalidState,
                "task request is already bound to a different parent",
            )),
        }
    }

    /// Transitions an existing task with a CAS fence and appends the corresponding immutable
    /// audit event.  A replay of an already-observed target state returns the current task.
    pub async fn transition(
        &self,
        task_id: &TaskId,
        tenant_id: &TenantId,
        next: TaskState,
        actor: TaskActor,
        message: Option<String>,
    ) -> Result<OperationTask, CentralError> {
        self.transition_with_issue(task_id, tenant_id, next, actor, None, message)
            .await
    }

    /// Transitions a task while preserving the authoritative failure/stall reason.  Domain
    /// executors use this variant when a materialization route, Agent, or source becomes
    /// unavailable; the issue is committed in the same repository CAS as the state event.
    pub async fn transition_with_issue(
        &self,
        task_id: &TaskId,
        tenant_id: &TenantId,
        next: TaskState,
        actor: TaskActor,
        issue: Option<TaskIssue>,
        message: Option<String>,
    ) -> Result<OperationTask, CentralError> {
        let current = self
            .repository
            .get(tenant_id, task_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    crate::CentralErrorCode::ResourceNotFound,
                    "operation task not found",
                )
            })?;
        if current.state == next && issue.as_ref() == current.issue.as_ref() {
            return Ok(current);
        }
        // A same-state update is useful for a repeated stall/failure observation whose message
        // changed, but TaskRepository::transition deliberately treats same-state requests as
        // idempotent. Update the issue through a normal optimistic replace so the new diagnosis
        // is visible without emitting duplicate state events.
        if current.state == next {
            let mut updated = current.clone();
            updated.issue = issue;
            updated.updated_at_unix_ms =
                UnixMillis::new(self.clock.now().get().max(updated.updated_at_unix_ms.get()));
            updated.resource_version =
                ResourceVersion::new(updated.resource_version.get().saturating_add(1));
            return self
                .repository
                .replace(current.resource_version, updated)
                .await;
        }
        let now = UnixMillis::new(self.clock.now().get().max(current.updated_at_unix_ms.get()));
        self.repository
            .transition(
                tenant_id,
                task_id,
                current.resource_version,
                next,
                actor,
                issue,
                message,
                now,
            )
            .await
            .map(|outcome| outcome.task)
    }

    /// Updates the high-frequency progress summary with an optimistic-concurrency fence. Byte
    /// checkpoints remain in the materialization detail tables; this row only carries the latest
    /// aggregate for task list/summary views and intentionally does not append one audit event per
    /// object.
    pub async fn update_progress(
        &self,
        task_id: &TaskId,
        tenant_id: &TenantId,
        progress: TaskProgressSummary,
    ) -> Result<OperationTask, CentralError> {
        progress.validate().map_err(CentralError::from)?;
        let current = self
            .repository
            .get(tenant_id, task_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    crate::CentralErrorCode::ResourceNotFound,
                    "operation task not found",
                )
            })?;
        if current.progress_summary == progress {
            return Ok(current);
        }
        let mut updated = current.clone();
        updated.progress_summary = progress;
        updated.updated_at_unix_ms =
            UnixMillis::new(self.clock.now().get().max(updated.updated_at_unix_ms.get()));
        updated.resource_version =
            ResourceVersion::new(updated.resource_version.get().saturating_add(1));
        self.repository
            .replace(current.resource_version, updated)
            .await
    }

    pub async fn complete_immediate(
        &self,
        task: &OperationTask,
        actor: TaskActor,
    ) -> Result<OperationTask, CentralError> {
        if task.state.is_terminal() {
            return Ok(task.clone());
        }
        let running = self
            .transition(
                &task.task_id,
                &task.tenant_id,
                TaskState::Running,
                actor.clone(),
                None,
            )
            .await?;
        self.transition(
            &running.task_id,
            &running.tenant_id,
            TaskState::Succeeded,
            actor,
            None,
        )
        .await
    }
}

/// Application service over the backend-neutral task authority port.
pub struct TaskService {
    repository: Arc<dyn TaskRepository>,
    policy: Arc<StaticRbacPolicy>,
    clock: Arc<dyn Clock>,
    coordinator: Arc<TaskCoordinator>,
    materialization: Option<Arc<super::CatalogService>>,
}

impl TaskService {
    #[must_use]
    pub fn new(
        repository: Arc<dyn TaskRepository>,
        policy: Arc<StaticRbacPolicy>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let coordinator = Arc::new(TaskCoordinator::new(repository.clone(), clock.clone()));
        Self {
            repository,
            policy,
            clock,
            coordinator,
            materialization: None,
        }
    }

    /// Installs the domain materialization service so the unified retry action can rebuild a
    /// Commit materialization plan instead of merely changing the task row.
    #[must_use]
    pub fn with_materialization_service(
        mut self,
        materialization: Arc<super::CatalogService>,
    ) -> Self {
        self.materialization = Some(materialization);
        self
    }

    #[must_use]
    pub fn repository(&self) -> Arc<dyn TaskRepository> {
        self.repository.clone()
    }

    pub async fn list_tasks(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTaskListRequest,
    ) -> Result<QueryTaskListResponse, fusen_rs::Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.authorize(identity, Permission::TaskRead, &tenant_id)?;
        let page = self
            .repository
            .list(&task_list_request_with_tenant(request, tenant_id)?)
            .await
            .map_err(map_central_error)?;
        Ok(QueryTaskListResponse {
            items: page.items.iter().map(task_view).collect(),
            next_cursor: page.next_cursor,
        })
    }

    pub async fn query_task(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTaskRequest,
    ) -> Result<QueryTaskResponse, fusen_rs::Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.authorize(identity, Permission::TaskRead, &tenant_id)?;
        let task_id = parse_task_id(&request.task_id)?;
        let task = self
            .repository
            .get(&tenant_id, &task_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(not_found)?;
        let attempts = self
            .repository
            .attempts(&tenant_id, &task_id)
            .await
            .map_err(map_central_error)?
            .iter()
            .map(attempt_view)
            .collect();
        let events = self
            .repository
            .list_events(&TaskEventListRequest {
                tenant_id: tenant_id.clone(),
                task_id: task_id.clone(),
                after_sequence: None,
                page_size: MAX_PAGE_SIZE,
            })
            .await
            .map_err(map_central_error)?
            .items
            .iter()
            .map(event_view)
            .collect();
        let children_request = TaskListRequest {
            parent_task_id: Some(task_id),
            page_size: MAX_PAGE_SIZE,
            ..TaskListRequest::for_tenant(tenant_id)
        };
        let children = self
            .repository
            .list(&children_request)
            .await
            .map_err(map_central_error)?
            .items
            .iter()
            .map(task_view)
            .collect();
        Ok(QueryTaskResponse {
            task: task_view(&task),
            attempts,
            events,
            children,
        })
    }

    pub async fn list_task_events(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTaskEventListRequest,
    ) -> Result<QueryTaskEventListResponse, fusen_rs::Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.authorize(identity, Permission::TaskRead, &tenant_id)?;
        let task_id = parse_task_id(&request.task_id)?;
        let page = self
            .repository
            .list_events(&TaskEventListRequest {
                tenant_id,
                task_id,
                after_sequence: request
                    .cursor
                    .as_deref()
                    .map(parse_u64)
                    .transpose()?
                    .map(neoengram_domain::protocol::SequenceNumber::new),
                page_size: page_size(request.page_size)?,
            })
            .await
            .map_err(map_central_error)?;
        Ok(QueryTaskEventListResponse {
            items: page.items.iter().map(event_view).collect(),
            next_cursor: page.next_cursor,
        })
    }

    pub async fn task_summary(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTaskSummaryRequest,
    ) -> Result<QueryTaskSummaryResponse, fusen_rs::Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.authorize(identity, Permission::TaskRead, &tenant_id)?;
        let summary = self
            .repository
            .summary(&task_list_summary_request(&request)?)
            .await
            .map_err(map_central_error)?;
        Ok(QueryTaskSummaryResponse {
            summary: summary_view(summary),
        })
    }

    pub async fn retry_task(
        &self,
        identity: &AuthenticatedIdentity,
        request: RetryTaskRequest,
    ) -> Result<TaskMutationResponse, fusen_rs::Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.authorize(identity, Permission::TaskManage, &tenant_id)?;
        let task_id = parse_task_id(&request.task_id)?;
        let current = self
            .repository
            .get(&tenant_id, &task_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(not_found)?;
        let expected = parse_expected_version(
            request.expected_resource_version.as_deref(),
            current.resource_version,
        )?;
        let result = self
            .repository
            .retry(
                &tenant_id,
                &task_id,
                expected,
                TaskActor::Principal(identity.principal().clone()),
                self.clock.now(),
            )
            .await
            .map_err(map_central_error)?;

        // Commit materialization retries are domain operations.  The task Attempt is advanced
        // first, then the planner is rerun against current Placement and route evidence.  This
        // keeps the public task API as the only retry entry point while retaining stable staging
        // keys and fencing stale data-plane reports.
        if current.task_kind == TaskKind::CommitMaterialize {
            if let Some(materialization) = &self.materialization {
                let retried = result.task.clone();
                let materialization_result = materialization
                    .retry_materialization_for_task(identity, &retried)
                    .await;
                match materialization_result {
                    Ok(response) => {
                        let desired = materialization_task_state(&response.materialization.state);
                        let progress = TaskProgressSummary::new(
                            response
                                .materialization
                                .verified_objects
                                .parse()
                                .map_err(|_| {
                                    invalid_request("materialization verified_objects is invalid")
                                })?,
                            response
                                .materialization
                                .total_objects
                                .parse()
                                .map_err(|_| {
                                    invalid_request("materialization total_objects is invalid")
                                })?,
                            response
                                .materialization
                                .verified_bytes
                                .parse()
                                .map_err(|_| {
                                    invalid_request("materialization verified_bytes is invalid")
                                })?,
                            response.materialization.total_bytes.parse().map_err(|_| {
                                invalid_request("materialization total_bytes is invalid")
                            })?,
                        );
                        let mut latest = self
                            .coordinator
                            .update_progress(&retried.task_id, &tenant_id, progress)
                            .await
                            .map_err(map_central_error)?;
                        if desired != TaskState::Queued && latest.state != desired {
                            // A complete replan may be immediately satisfied.  Walk through
                            // Running because the shared task state machine intentionally does
                            // not allow queued -> succeeded directly.
                            if desired == TaskState::Succeeded && latest.state == TaskState::Queued
                            {
                                latest = self
                                    .coordinator
                                    .transition(
                                        &latest.task_id,
                                        &latest.tenant_id,
                                        TaskState::Running,
                                        TaskActor::Principal(identity.principal().clone()),
                                        Some("materialization retry resumed".to_owned()),
                                    )
                                    .await
                                    .map_err(map_central_error)?;
                            }
                            let issue =
                                response
                                    .materialization
                                    .issue
                                    .as_ref()
                                    .map(|value| TaskIssue {
                                        code: value.code.clone(),
                                        message: value.message.clone(),
                                        retryable: value.retryable,
                                        detail: None,
                                    });
                            latest = self
                                .coordinator
                                .transition_with_issue(
                                    &latest.task_id,
                                    &latest.tenant_id,
                                    desired,
                                    TaskActor::Principal(identity.principal().clone()),
                                    issue,
                                    Some("materialization retry plan accepted".to_owned()),
                                )
                                .await
                                .map_err(map_central_error)?;
                        }
                        return Ok(TaskMutationResponse {
                            task: task_view(&latest),
                            replayed: result.replayed || response.replayed,
                        });
                    }
                    Err(error) => {
                        // Do not leave a retried task queued when planning itself failed.  Keep
                        // it actionable and retain the domain error for the caller.
                        let issue = TaskIssue {
                            code: "MATERIALIZATION_REPLAN_FAILED".to_owned(),
                            message: error.to_string(),
                            retryable: true,
                            detail: None,
                        };
                        let _ = self
                            .coordinator
                            .transition_with_issue(
                                &retried.task_id,
                                &retried.tenant_id,
                                TaskState::Stalled,
                                TaskActor::Principal(identity.principal().clone()),
                                Some(issue),
                                Some("materialization retry planning failed".to_owned()),
                            )
                            .await;
                        return Err(error);
                    }
                }
            }
        }
        Ok(TaskMutationResponse {
            task: task_view(&result.task),
            replayed: result.replayed,
        })
    }

    pub async fn cancel_task(
        &self,
        identity: &AuthenticatedIdentity,
        request: CancelTaskRequest,
    ) -> Result<TaskMutationResponse, fusen_rs::Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.authorize(identity, Permission::TaskManage, &tenant_id)?;
        let task_id = parse_task_id(&request.task_id)?;
        let expected = request
            .expected_resource_version
            .as_deref()
            .map(parse_u64)
            .transpose()?
            .map(ResourceVersion::new);
        let result = self
            .repository
            .cancel(
                &tenant_id,
                &task_id,
                expected,
                TaskActor::Principal(identity.principal().clone()),
                self.clock.now(),
            )
            .await
            .map_err(map_central_error)?;
        Ok(TaskMutationResponse {
            task: task_view(&result.task),
            replayed: result.replayed,
        })
    }

    fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        permission: Permission,
        tenant_id: &TenantId,
    ) -> Result<(), fusen_rs::Error> {
        self.policy
            .authorize_identity(identity, permission, tenant_id)
    }
}

fn task_list_request_with_tenant(
    request: QueryTaskListRequest,
    tenant_id: TenantId,
) -> Result<TaskListRequest, fusen_rs::Error> {
    let mut parsed = task_list_request(&request)?;
    parsed.tenant_id = tenant_id;
    Ok(parsed)
}

fn task_list_request(request: &QueryTaskListRequest) -> Result<TaskListRequest, fusen_rs::Error> {
    Ok(TaskListRequest {
        tenant_id: parse_tenant(&request.tenant_id)?,
        project_id: request
            .project_id
            .as_deref()
            .map(parse_project)
            .transpose()?,
        artifact_id: request
            .artifact_id
            .as_deref()
            .map(parse_artifact)
            .transpose()?,
        object_namespace_id: request
            .object_namespace_id
            .as_deref()
            .map(parse_namespace)
            .transpose()?,
        commit_id: request.commit_id.as_deref().map(parse_commit).transpose()?,
        playground_id: request
            .playground_id
            .as_deref()
            .map(parse_playground)
            .transpose()?,
        snapshot_id: request
            .snapshot_id
            .as_deref()
            .map(parse_snapshot)
            .transpose()?,
        storage_volume_id: request
            .storage_volume_id
            .as_deref()
            .map(parse_volume)
            .transpose()?,
        task_kinds: request
            .task_kind
            .iter()
            .map(|value| {
                TaskKind::from_str(value)
                    .map_err(|error| invalid_request(format!("task_kind: {error}")))
            })
            .collect::<Result<_, _>>()?,
        states: request
            .state
            .iter()
            .map(|value| parse_state(value))
            .collect::<Result<_, _>>()?,
        parent_task_id: request
            .parent_task_id
            .as_deref()
            .map(parse_task_id)
            .transpose()?,
        created_after_unix_ms: request
            .created_after_unix_ms
            .as_deref()
            .map(parse_unix_ms)
            .transpose()?,
        created_before_unix_ms: request
            .created_before_unix_ms
            .as_deref()
            .map(parse_unix_ms)
            .transpose()?,
        updated_after_unix_ms: request
            .updated_after_unix_ms
            .as_deref()
            .map(parse_unix_ms)
            .transpose()?,
        updated_before_unix_ms: request
            .updated_before_unix_ms
            .as_deref()
            .map(parse_unix_ms)
            .transpose()?,
        cursor: request.cursor.clone(),
        page_size: page_size(request.page_size)?,
    })
}

fn task_list_summary_request(
    request: &QueryTaskSummaryRequest,
) -> Result<TaskListRequest, fusen_rs::Error> {
    task_list_request(&QueryTaskListRequest {
        tenant_id: request.tenant_id.clone(),
        project_id: request.project_id.clone(),
        artifact_id: request.artifact_id.clone(),
        object_namespace_id: request.object_namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        playground_id: request.playground_id.clone(),
        snapshot_id: request.snapshot_id.clone(),
        storage_volume_id: request.storage_volume_id.clone(),
        task_kind: request.task_kind.clone(),
        state: request.state.clone(),
        ..Default::default()
    })
}

pub(crate) fn task_view(task: &OperationTask) -> TaskView {
    TaskView {
        task_id: task.task_id.to_string(),
        task_kind: task.task_kind.to_string(),
        state: state_name(task.state).to_owned(),
        phase: task.phase.clone(),
        tenant_id: task.tenant_id.to_string(),
        project_id: task.project_id.as_ref().map(ToString::to_string),
        artifact_id: task.artifact_id.as_ref().map(ToString::to_string),
        object_namespace_id: task.object_namespace_id.as_ref().map(ToString::to_string),
        commit_id: task.commit_id.map(|value| value.to_string()),
        playground_id: task.playground_id.as_ref().map(ToString::to_string),
        snapshot_id: task.snapshot_id.as_ref().map(ToString::to_string),
        storage_volume_id: task.storage_volume_id.as_ref().map(ToString::to_string),
        parent_task_id: task.parent_task_id.as_ref().map(ToString::to_string),
        request_id: task.request_id.to_string(),
        request_digest: task.request_digest.to_string(),
        actor: actor_name(&task.actor),
        attempt: task.attempt.to_string(),
        progress: progress_view(task.progress_summary),
        detail_kind: task.detail_kind.clone(),
        detail_id: task.detail_id.clone(),
        deadline_unix_ms: task.deadline_unix_ms.to_string(),
        issue: task.issue.as_ref().map(issue_view),
        created_at_unix_ms: task.created_at_unix_ms.to_string(),
        updated_at_unix_ms: task.updated_at_unix_ms.to_string(),
        started_at_unix_ms: task.started_at_unix_ms.map(|value| value.to_string()),
        finished_at_unix_ms: task.finished_at_unix_ms.map(|value| value.to_string()),
        resource_version: task.resource_version.to_string(),
        origin: origin_name(task.origin).to_owned(),
        executable: task.executable,
    }
}
fn progress_view(value: neoengram_domain::protocol::TaskProgressSummary) -> TaskProgressView {
    TaskProgressView {
        completed: value.completed.to_string(),
        total: value.total.to_string(),
        completed_bytes: value.completed_bytes.to_string(),
        total_bytes: value.total_bytes.to_string(),
    }
}
fn issue_view(value: &TaskIssue) -> TaskIssueView {
    TaskIssueView {
        code: value.code.clone(),
        message: value.message.clone(),
        retryable: value.retryable,
        detail: value.detail.clone(),
    }
}
fn attempt_view(value: &TaskAttempt) -> TaskAttemptView {
    TaskAttemptView {
        attempt_id: value.attempt_id.to_string(),
        task_id: value.task_id.to_string(),
        attempt: value.attempt.to_string(),
        state: state_name(value.state).to_owned(),
        phase: value.phase.clone(),
        created_at_unix_ms: value.created_at_unix_ms.to_string(),
        updated_at_unix_ms: value.updated_at_unix_ms.to_string(),
        started_at_unix_ms: value.started_at_unix_ms.map(|item| item.to_string()),
        finished_at_unix_ms: value.finished_at_unix_ms.map(|item| item.to_string()),
        issue: value.issue.as_ref().map(issue_view),
        resource_version: value.resource_version.to_string(),
    }
}
fn event_view(value: &TaskEvent) -> TaskEventView {
    TaskEventView {
        event_id: value.event_id.to_string(),
        task_id: value.task_id.to_string(),
        sequence: value.sequence.to_string(),
        attempt: value.attempt.to_string(),
        kind: event_kind_name(value.kind).to_owned(),
        state: state_name(value.state).to_owned(),
        from_state: value.from_state.map(|item| state_name(item).to_owned()),
        to_state: value.to_state.map(|item| state_name(item).to_owned()),
        actor: actor_name(&value.actor),
        message: value.message.clone(),
        issue: value.issue.as_ref().map(issue_view),
        progress: value.progress.map(progress_view),
        occurred_at_unix_ms: value.occurred_at_unix_ms.to_string(),
        resource_version: value.resource_version.to_string(),
    }
}
fn actor_name(value: &TaskActor) -> String {
    match value {
        TaskActor::Principal(actor) => actor.id.to_string(),
        TaskActor::Agent { agent_id } => format!("agent:{agent_id}"),
    }
}
fn summary_view(value: TaskSummary) -> TaskSummaryView {
    TaskSummaryView {
        total: value.total.to_string(),
        queued: value.queued.to_string(),
        running: value.running.to_string(),
        waiting: value.waiting.to_string(),
        verifying: value.verifying.to_string(),
        succeeded: value.succeeded.to_string(),
        stalled: value.stalled.to_string(),
        failed: value.failed.to_string(),
        cancelled: value.cancelled.to_string(),
    }
}
fn state_name(value: TaskState) -> &'static str {
    match value {
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

fn materialization_task_state(value: &str) -> TaskState {
    match value {
        "queued" => TaskState::Queued,
        "planning" | "materializing" => TaskState::Running,
        "waiting_for_sources" => TaskState::Waiting,
        "verifying" => TaskState::Verifying,
        "complete" => TaskState::Succeeded,
        "stalled" => TaskState::Stalled,
        "failed" => TaskState::Failed,
        "cancelled" => TaskState::Cancelled,
        // Materialization views are produced by the closed domain enum. Treat an unknown value
        // as stalled so a future state cannot accidentally be reported as successful.
        _ => TaskState::Stalled,
    }
}

fn origin_name(value: neoengram_domain::protocol::TaskOrigin) -> &'static str {
    match value {
        neoengram_domain::protocol::TaskOrigin::User => "user",
        neoengram_domain::protocol::TaskOrigin::System => "system",
        neoengram_domain::protocol::TaskOrigin::Legacy => "legacy",
    }
}
fn event_kind_name(value: TaskEventKind) -> &'static str {
    match value {
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
fn parse_tenant(value: &str) -> Result<TenantId, fusen_rs::Error> {
    TenantId::new(value.to_owned()).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}
fn parse_task_id(value: &str) -> Result<TaskId, fusen_rs::Error> {
    TaskId::new(value.to_owned()).map_err(|error| invalid_request(format!("task_id: {error}")))
}
fn parse_project(value: &str) -> Result<neoengram_domain::protocol::ProjectId, fusen_rs::Error> {
    neoengram_domain::protocol::ProjectId::new(value.to_owned())
        .map_err(|error| invalid_request(format!("project_id: {error}")))
}
fn parse_artifact(value: &str) -> Result<neoengram_domain::protocol::ArtifactId, fusen_rs::Error> {
    neoengram_domain::protocol::ArtifactId::new(value.to_owned())
        .map_err(|error| invalid_request(format!("artifact_id: {error}")))
}
fn parse_namespace(
    value: &str,
) -> Result<neoengram_domain::protocol::ObjectNamespaceId, fusen_rs::Error> {
    neoengram_domain::protocol::ObjectNamespaceId::new(value.to_owned())
        .map_err(|error| invalid_request(format!("object_namespace_id: {error}")))
}
fn parse_commit(value: &str) -> Result<CommitId, fusen_rs::Error> {
    CommitId::from_str(value).map_err(|error| invalid_request(format!("commit_id: {error}")))
}
fn parse_playground(
    value: &str,
) -> Result<neoengram_domain::protocol::PlaygroundId, fusen_rs::Error> {
    neoengram_domain::protocol::PlaygroundId::new(value.to_owned())
        .map_err(|error| invalid_request(format!("playground_id: {error}")))
}
fn parse_snapshot(value: &str) -> Result<neoengram_domain::protocol::SnapshotId, fusen_rs::Error> {
    neoengram_domain::protocol::SnapshotId::new(value.to_owned())
        .map_err(|error| invalid_request(format!("snapshot_id: {error}")))
}
fn parse_volume(
    value: &str,
) -> Result<neoengram_domain::protocol::StorageVolumeId, fusen_rs::Error> {
    neoengram_domain::protocol::StorageVolumeId::new(value.to_owned())
        .map_err(|error| invalid_request(format!("storage_volume_id: {error}")))
}
fn parse_state(value: &str) -> Result<TaskState, fusen_rs::Error> {
    match value {
        "queued" => Ok(TaskState::Queued),
        "running" => Ok(TaskState::Running),
        "waiting" => Ok(TaskState::Waiting),
        "verifying" => Ok(TaskState::Verifying),
        "succeeded" => Ok(TaskState::Succeeded),
        "stalled" => Ok(TaskState::Stalled),
        "failed" => Ok(TaskState::Failed),
        "cancelled" => Ok(TaskState::Cancelled),
        _ => Err(invalid_request(format!("state: unsupported value {value}"))),
    }
}
fn parse_unix_ms(value: &str) -> Result<UnixMillis, fusen_rs::Error> {
    parse_u64(value).map(UnixMillis::new)
}
fn parse_expected_version(
    value: Option<&str>,
    fallback: ResourceVersion,
) -> Result<ResourceVersion, fusen_rs::Error> {
    value
        .map(parse_u64)
        .transpose()?
        .map(ResourceVersion::new)
        .map_or(Ok(fallback), Ok)
}
fn parse_u64(value: &str) -> Result<u64, fusen_rs::Error> {
    value
        .parse::<u64>()
        .map_err(|_| invalid_request("numeric value must be an unsigned integer"))
}
fn page_size(value: Option<u16>) -> Result<usize, fusen_rs::Error> {
    let value = value.map(usize::from).unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&value) {
        return Err(invalid_request(format!(
            "page_size must be between 1 and {MAX_PAGE_SIZE}"
        )));
    }
    Ok(value)
}
fn not_found() -> fusen_rs::Error {
    crate::error::application_error(
        fusen_rs::ErrorCategory::NotFound,
        "task_not_found",
        "TASK_NOT_FOUND",
        "task not found",
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pagination_limits_are_bounded() {
        assert!(page_size(Some(500)).is_ok());
        assert!(page_size(Some(501)).is_err());
    }
    #[test]
    fn state_names_are_stable() {
        assert_eq!(state_name(TaskState::Verifying), "verifying");
    }
}
