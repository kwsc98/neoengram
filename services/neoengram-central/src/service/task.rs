//! Public service for the unified operation-task and audit API.

use std::{str::FromStr, sync::Arc};

use neoengram_domain::{
    core::CommitId,
    protocol::{
        Generation, OperationTask, ResourceVersion, StageState, TaskActor, TaskAttempt, TaskEvent,
        TaskEventKind, TaskId, TaskIntent, TaskIssue, TaskProgressSummary, TaskResourceKind,
        TaskResourceLink, TaskResourceRef, TaskResourceRole, TaskScope, TaskState, TenantId,
        UnixMillis,
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

/// Converts the typed task scope into public resource links. The scope fields are local query
/// projections and are not serialized in the task payload, so keeping these links on the
/// authority row is what makes SQLite task filtering and API consumers survive a restart.
pub(crate) fn scope_resource_links(task_id: &TaskId, scope: &TaskScope) -> Vec<TaskResourceLink> {
    let mut links = Vec::new();
    let mut add = |kind: TaskResourceKind, id: String, role: TaskResourceRole| {
        if !links
            .iter()
            .any(|link: &TaskResourceLink| link.resource_kind == kind && link.resource_id == id)
        {
            links.push(TaskResourceLink::new(task_id.clone(), kind, id, role));
        }
    };

    if let Some(project_id) = &scope.project_id {
        add(
            TaskResourceKind::Project,
            project_id.to_string(),
            TaskResourceRole::Related,
        );
    }
    if let Some(artifact_id) = &scope.artifact_id {
        add(
            TaskResourceKind::Artifact,
            artifact_id.to_string(),
            TaskResourceRole::Related,
        );
    }
    if let Some(namespace_id) = &scope.object_namespace_id {
        add(
            TaskResourceKind::ObjectNamespace,
            namespace_id.to_string(),
            TaskResourceRole::Related,
        );
    }
    if let Some(commit_id) = scope.commit_id {
        add(
            TaskResourceKind::Commit,
            commit_id.to_string(),
            TaskResourceRole::Source,
        );
    }
    if let Some(workspace_id) = &scope.workspace_id {
        add(
            TaskResourceKind::Workspace,
            workspace_id.to_string(),
            TaskResourceRole::Related,
        );
    }
    if let Some(snapshot_id) = &scope.snapshot_id {
        add(
            TaskResourceKind::Snapshot,
            snapshot_id.to_string(),
            TaskResourceRole::Related,
        );
    }
    if let Some(storage_volume_id) = &scope.storage_volume_id {
        add(
            TaskResourceKind::StorageVolume,
            storage_volume_id.to_string(),
            TaskResourceRole::Target,
        );
    }
    links
}

fn primary_resource_for(
    kind: TaskIntent,
    scope: &TaskScope,
    detail_kind: Option<&str>,
    detail_id: Option<&str>,
) -> TaskResourceRef {
    // The intent is the authoritative resource discriminator.  `detail_kind` is only a
    // secondary hint for infrastructure operations whose target is not part of TaskScope (for
    // example a Gateway replica or an S3 credential); it must never make a commit task point at
    // its materialization detail row.
    let detail_id = detail_id.map(str::to_owned);
    let preferred = match kind {
        TaskIntent::ProjectCreate | TaskIntent::ProjectDelete | TaskIntent::ProjectRestore => (
            TaskResourceKind::Project,
            scope.project_id.as_ref().map(ToString::to_string),
        ),
        TaskIntent::ArtifactCreate | TaskIntent::ArtifactDelete | TaskIntent::ArtifactRestore => (
            TaskResourceKind::Artifact,
            scope.artifact_id.as_ref().map(ToString::to_string),
        ),
        TaskIntent::WorkspaceCreate
        | TaskIntent::WorkspaceDelete
        | TaskIntent::WorkspaceRestore => (
            TaskResourceKind::Workspace,
            scope.workspace_id.as_ref().map(ToString::to_string),
        ),
        TaskIntent::SnapshotCreate | TaskIntent::SnapshotDelete | TaskIntent::SnapshotRestore => (
            TaskResourceKind::Snapshot,
            scope.snapshot_id.as_ref().map(ToString::to_string),
        ),
        TaskIntent::StorageVolumeCreate
        | TaskIntent::StorageVolumeDelete
        | TaskIntent::StorageVolumeRestore => (
            TaskResourceKind::StorageVolume,
            scope.storage_volume_id.as_ref().map(ToString::to_string),
        ),
        TaskIntent::CommitValidate | TaskIntent::CommitCreate | TaskIntent::CommitMaterialize => (
            TaskResourceKind::Commit,
            scope.commit_id.map(|id| id.to_string()),
        ),
        TaskIntent::S3AccessPointCreate
        | TaskIntent::S3AccessPointDelete
        | TaskIntent::S3AccessPointEnable
        | TaskIntent::S3AccessPointDisable => (TaskResourceKind::S3AccessPoint, detail_id.clone()),
        TaskIntent::AgentEnrollmentCreate
        | TaskIntent::AgentEnrollmentApprove
        | TaskIntent::AgentEnrollmentReject
        | TaskIntent::AgentEnrollmentRecover
        | TaskIntent::AgentEnrollmentDelete => (TaskResourceKind::Agent, detail_id.clone()),
        TaskIntent::GatewayPoolCreate
        | TaskIntent::GatewayPoolUpdate
        | TaskIntent::GatewayPoolDrain
        | TaskIntent::GatewayPoolDelete
        | TaskIntent::GatewayReplicaCreate
        | TaskIntent::GatewayReplicaActivate
        | TaskIntent::GatewayReplicaDrain
        | TaskIntent::GatewayReplicaRevoke
        | TaskIntent::GatewayReplicaDelete => (TaskResourceKind::Gateway, detail_id.clone()),
        TaskIntent::S3CredentialCreate | TaskIntent::S3CredentialRevoke => {
            (TaskResourceKind::S3Credential, detail_id.clone())
        }
    };
    if let Some(resource_id) = preferred.1 {
        return TaskResourceRef::new(preferred.0, resource_id);
    }

    // Some API operations are intentionally scoped to the parent (CommitCreate uses a Workspace
    // until the Commit ID exists). Preserve that relationship when the intent's direct target is
    // not known yet, then fall back to a typed detail hint and finally the tenant.
    if let Some(id) = scope.workspace_id.as_ref() {
        return TaskResourceRef::new(TaskResourceKind::Workspace, id.to_string());
    }
    if let Some(id) = scope.commit_id {
        return TaskResourceRef::new(TaskResourceKind::Commit, id.to_string());
    }
    if let Some(id) = scope.snapshot_id.as_ref() {
        return TaskResourceRef::new(TaskResourceKind::Snapshot, id.to_string());
    }
    if let Some(id) = scope.storage_volume_id.as_ref() {
        return TaskResourceRef::new(TaskResourceKind::StorageVolume, id.to_string());
    }
    if let Some(id) = scope.artifact_id.as_ref() {
        return TaskResourceRef::new(TaskResourceKind::Artifact, id.to_string());
    }
    if let Some(id) = scope.project_id.as_ref() {
        return TaskResourceRef::new(TaskResourceKind::Project, id.to_string());
    }
    if let (Some(detail_kind), Some(detail_id)) = (
        detail_kind.map(|value| value.to_ascii_lowercase()),
        detail_id,
    ) {
        let resource_kind = match detail_kind.as_str() {
            "tenant" => Some(TaskResourceKind::Tenant),
            "project" => Some(TaskResourceKind::Project),
            "artifact" => Some(TaskResourceKind::Artifact),
            "workspace" => Some(TaskResourceKind::Workspace),
            "snapshot" => Some(TaskResourceKind::Snapshot),
            "storage_volume" | "storage-volume" => Some(TaskResourceKind::StorageVolume),
            "s3_access_point" | "s3-access-point" => Some(TaskResourceKind::S3AccessPoint),
            "commit" => Some(TaskResourceKind::Commit),
            "materialization" => Some(TaskResourceKind::Materialization),
            "agent" | "agent_enrollment" => Some(TaskResourceKind::Agent),
            "gateway" | "gateway_pool" | "gateway_replica" => Some(TaskResourceKind::Gateway),
            "s3_credential" => Some(TaskResourceKind::S3Credential),
            _ => None,
        };
        if let Some(resource_kind) = resource_kind {
            return TaskResourceRef::new(resource_kind, detail_id);
        }
    }
    TaskResourceRef::new(TaskResourceKind::Tenant, scope.tenant_id.to_string())
}

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
        kind: TaskIntent,
        scope: TaskScope,
        request_id: neoengram_domain::protocol::RequestId,
        request: &T,
        actor: TaskActor,
        detail_kind: Option<&str>,
        detail_id: Option<&str>,
    ) -> Result<(OperationTask, TaskAttempt, TaskEvent), CentralError> {
        let request_digest = neoengram_domain::jcs_blake3(request).map_err(CentralError::from)?;
        // The request id is only unique within a tenant. Include that scope in the stable task
        // identifier so identical request ids from different tenants cannot collide before the
        // authority has a chance to apply its tenant-scoped uniqueness constraint.
        let task_id_digest =
            blake3::hash(format!("operation-task\0{}\0{request_id}", scope.tenant_id).as_bytes());
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
        let primary_resource = primary_resource_for(kind, &scope, detail_kind, detail_id);
        // Request identity and execution identity are separate.  Request IDs are transport
        // idempotency keys and must not make semantically identical operations execute twice.
        let mut semantic_payload = serde_json::to_value(request).map_err(|error| {
            CentralError::new(
                crate::CentralErrorCode::ProtocolInvalid,
                format!("task semantic payload: {error}"),
            )
        })?;
        if let serde_json::Value::Object(fields) = &mut semantic_payload {
            fields.retain(|key, _| {
                key != "request_id" && !key.ends_with("_request_id") && key != "idempotency_key"
            });
        }
        let purpose = if kind == TaskIntent::CommitMaterialize {
            serde_json::to_value(request)
                .ok()
                .and_then(|value| {
                    value
                        .get("purpose")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .and_then(|value| value.parse().ok())
        } else {
            None
        };
        // Execution identity is intentionally independent from request identity. Include every
        // target dimension (tenant, intent, purpose and the resolved primary resource) alongside
        // the canonical semantic payload so identical request bodies aimed at different resources
        // cannot collide in the execution index.
        let execution_identity = serde_json::json!({
            "protocol_version": neoengram_domain::protocol::OPERATION_TASK_PROTOCOL_VERSION,
            "tenant_id": scope.tenant_id.to_string(),
            "intent_kind": kind.as_str(),
            "purpose": purpose.map(|value: neoengram_domain::protocol::TaskPurpose| value.as_str()),
            "primary_resource": {
                "resource_kind": format!("{:?}", primary_resource.resource_kind).to_ascii_lowercase(),
                "resource_id": primary_resource.resource_id.clone(),
            },
            "detail_kind": detail_kind,
            "detail_id": detail_id,
            "scope": scope.clone(),
            "semantic_payload": semantic_payload,
        });
        let execution_key_digest =
            neoengram_domain::jcs_blake3(&execution_identity).map_err(CentralError::from)?;
        let execution_id = format!("execution-{}", execution_key_digest);
        let scope_links = scope_resource_links(&task_id, &scope);
        let mut task = OperationTask::new(
            task_id.clone(),
            kind,
            purpose,
            primary_resource,
            execution_id,
            execution_key_digest,
            scope,
            request_id,
            request_digest,
            actor.clone(),
            created_at,
            deadline,
        );
        task.resource_links = scope_links;
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
        kind: TaskIntent,
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
            detail_kind,
            detail_id,
        )?;
        let stages = neoengram_domain::protocol::TaskStage::plan_for_intent(
            task.task_id.clone(),
            task.intent_kind,
            task.created_at_unix_ms,
        );
        let outcome = self
            .repository
            .insert_with_history_and_stages(task, Some(attempt), Some(event), stages.clone())
            .await?;
        Ok(match outcome {
            TaskInsertOutcome::Inserted(mut task) => {
                task.stages = stages;
                (task, false)
            }
            TaskInsertOutcome::Existing(mut task) => {
                task.stages = self
                    .repository
                    .stages(&task.tenant_id, &task.task_id)
                    .await?;
                let replayed = task.request_replayed;
                (task, replayed)
            }
        })
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
            let mut current = current;
            current.stages = self.repository.stages(tenant_id, task_id).await?;
            return Ok(current);
        }
        if next == TaskState::Succeeded {
            let stages = self.repository.stages(tenant_id, task_id).await?;
            if stages.is_empty() || stages.iter().any(|stage| !stage.state.is_success()) {
                return Err(CentralError::new(
                    crate::CentralErrorCode::InvalidState,
                    "operation task cannot succeed before all required stages complete",
                ));
            }
        }
        let now = UnixMillis::new(self.clock.now().get().max(current.updated_at_unix_ms.get()));
        let mut updated = self
            .repository
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
            .map(|outcome| outcome.task)?;
        updated.stages = self.repository.stages(tenant_id, task_id).await?;
        Ok(updated)
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
        if matches!(current.state, TaskState::Cancelling | TaskState::Cancelled) {
            return Err(CentralError::new(
                crate::CentralErrorCode::InvalidState,
                "operation task is cancelling; progress updates are no longer accepted",
            ));
        }
        if current.progress == progress {
            return Ok(current);
        }
        let mut updated = current.clone();
        updated.progress = progress;
        updated.updated_at_unix_ms =
            UnixMillis::new(self.clock.now().get().max(updated.updated_at_unix_ms.get()));
        updated.resource_version =
            ResourceVersion::new(updated.resource_version.get().saturating_add(1));
        self.repository
            .replace(current.resource_version, updated)
            .await
    }

    /// Performs a CAS-guarded transition for one stage and enforces that all declared
    /// dependencies have reached a successful outcome before the stage becomes ready/running.
    /// Stage transitions are deliberately separate from root task transitions so independent DAG
    /// branches can progress concurrently without creating child tasks.
    pub async fn transition_stage(
        &self,
        task_id: &TaskId,
        tenant_id: &TenantId,
        stage_key: &str,
        next: StageState,
        issue: Option<TaskIssue>,
    ) -> Result<neoengram_domain::protocol::TaskStage, CentralError> {
        let task = self
            .repository
            .get(tenant_id, task_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    crate::CentralErrorCode::ResourceNotFound,
                    "operation task not found",
                )
            })?;
        // Once cancellation has been requested, no ordinary stage can be started or advanced.
        // Cancellation reconciliation is the only path allowed to move a stage through its
        // cancelling -> cancelled fence; this also rejects late Agent reports after the root is
        // already terminal.
        if matches!(task.state, TaskState::Cancelling | TaskState::Cancelled)
            && !matches!(next, StageState::Cancelling | StageState::Cancelled)
        {
            return Err(CentralError::new(
                crate::CentralErrorCode::InvalidState,
                "operation task is cancelling; only cancellation stage transitions are allowed",
            ));
        }
        let stages = self.repository.stages(tenant_id, task_id).await?;
        let mut stage = stages
            .iter()
            .find(|stage| stage.stage_key == stage_key)
            .cloned()
            .ok_or_else(|| {
                CentralError::new(
                    crate::CentralErrorCode::ResourceNotFound,
                    "operation task stage not found",
                )
            })?;
        if matches!(next, StageState::Ready | StageState::Running)
            && stage.dependencies.iter().any(|dependency| {
                stages
                    .iter()
                    .find(|candidate| candidate.stage_key == *dependency)
                    .is_none_or(|candidate| !candidate.state.is_success())
            })
        {
            return Err(CentralError::new(
                crate::CentralErrorCode::InvalidState,
                "stage dependencies have not completed successfully",
            ));
        }
        if let Some(issue) = issue {
            stage.issue = Some(issue);
        }
        if stage.state == next {
            return Ok(stage);
        }
        let expected = stage.resource_version;
        let now = UnixMillis::new(self.clock.now().get().max(stage.updated_at_unix_ms.get()));
        stage.transition_to(next, now).map_err(CentralError::from)?;
        let updated_stage = self
            .repository
            .replace_stage(tenant_id, expected, stage)
            .await?;
        // Keep the root's navigation pointer aligned with the furthest observed stage. The
        // pointer is a projection, not the source of truth; in particular, a late report from a
        // parallel/older branch must never move it backwards. Update it with a separate CAS so a
        // concurrent stage transition cannot overwrite a newer pointer.
        if matches!(
            next,
            StageState::Ready
                | StageState::Running
                | StageState::Waiting
                | StageState::Verifying
                | StageState::Succeeded
                | StageState::Skipped
                | StageState::NoOp
                | StageState::Failed
                | StageState::Stalled
                | StageState::Cancelled
        ) {
            let observed = self.repository.stages(tenant_id, task_id).await?;
            let candidate = observed
                .iter()
                .filter(|item| item.state != StageState::Pending)
                .max_by_key(|item| (item.ordinal, item.stage_key.as_str()));
            if let Some(candidate) = candidate {
                if let Some(mut task) = self.repository.get(tenant_id, task_id).await? {
                    let current_ordinal = observed
                        .iter()
                        .find(|item| item.stage_key == task.current_stage_key)
                        .map_or(0, |item| item.ordinal.get());
                    if candidate.ordinal.get() > current_ordinal
                        || (current_ordinal == 0 && task.current_stage_key != candidate.stage_key)
                    {
                        let expected = task.resource_version;
                        task.current_stage_key = candidate.stage_key.clone();
                        task.resource_version =
                            ResourceVersion::new(task.resource_version.get().saturating_add(1));
                        task.updated_at_unix_ms = UnixMillis::new(
                            self.clock.now().get().max(task.updated_at_unix_ms.get()),
                        );
                        match self.repository.replace(expected, task).await {
                            Ok(_) => {}
                            Err(error)
                                if error.code() == crate::CentralErrorCode::ConcurrentUpdate => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
        }
        Ok(updated_stage)
    }

    /// Marks a stage as successfully reused from a matching prior operation. The stage keeps its
    /// stable key and records `outcome=reused`, so the root task can complete without creating a
    /// second validation/scan task.
    pub async fn reuse_stage(
        &self,
        task_id: &TaskId,
        tenant_id: &TenantId,
        stage_key: &str,
    ) -> Result<neoengram_domain::protocol::TaskStage, CentralError> {
        let task = self
            .repository
            .get(tenant_id, task_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    crate::CentralErrorCode::ResourceNotFound,
                    "operation task not found",
                )
            })?;
        if matches!(task.state, TaskState::Cancelling | TaskState::Cancelled) {
            return Err(CentralError::new(
                crate::CentralErrorCode::InvalidState,
                "operation task is cancelling; stage reuse is no longer accepted",
            ));
        }
        let mut stage = self
            .repository
            .stages(tenant_id, task_id)
            .await?
            .into_iter()
            .find(|candidate| candidate.stage_key == stage_key)
            .ok_or_else(|| {
                CentralError::new(
                    crate::CentralErrorCode::ResourceNotFound,
                    "operation task stage not found",
                )
            })?;
        if stage.state.is_success()
            && stage.outcome == Some(neoengram_domain::protocol::StageOutcome::Reused)
        {
            return Ok(stage);
        }
        let stages = self.repository.stages(tenant_id, task_id).await?;
        if stage.dependencies.iter().any(|dependency| {
            stages
                .iter()
                .find(|candidate| candidate.stage_key == *dependency)
                .is_none_or(|candidate| !candidate.state.is_success())
        }) {
            return Err(CentralError::new(
                crate::CentralErrorCode::InvalidState,
                "stage dependencies have not completed successfully",
            ));
        }
        let expected = stage.resource_version;
        let now = UnixMillis::new(self.clock.now().get().max(stage.updated_at_unix_ms.get()));
        stage.mark_reused(now).map_err(CentralError::from)?;
        self.repository
            .replace_stage(tenant_id, expected, stage)
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
        // Complete stages through the coordinator so the dependency DAG is enforced even for
        // immediate catalog operations. Iterate until every stage is terminal-success; a custom
        // DAG may expose independent branches and therefore cannot rely on ordinal order alone.
        loop {
            let stages = self
                .repository
                .stages(&running.tenant_id, &running.task_id)
                .await?;
            let mut progressed = false;
            for stage in &stages {
                if stage.state.is_success() {
                    continue;
                }
                if !stage.dependencies.iter().all(|dependency| {
                    stages
                        .iter()
                        .find(|candidate| candidate.stage_key == *dependency)
                        .is_some_and(|candidate| candidate.state.is_success())
                }) {
                    continue;
                }
                let mut current = stage.clone();
                if current.state == StageState::Pending {
                    current = self
                        .transition_stage(
                            &running.task_id,
                            &running.tenant_id,
                            &current.stage_key,
                            StageState::Ready,
                            None,
                        )
                        .await?;
                }
                if matches!(
                    current.state,
                    StageState::Ready
                        | StageState::Waiting
                        | StageState::Verifying
                        | StageState::Stalled
                ) {
                    current = self
                        .transition_stage(
                            &running.task_id,
                            &running.tenant_id,
                            &current.stage_key,
                            StageState::Running,
                            None,
                        )
                        .await?;
                }
                if current.state == StageState::Running {
                    self.transition_stage(
                        &running.task_id,
                        &running.tenant_id,
                        &current.stage_key,
                        StageState::Succeeded,
                        None,
                    )
                    .await?;
                } else if !current.state.is_success() {
                    return Err(CentralError::new(
                        crate::CentralErrorCode::InvalidState,
                        format!(
                            "stage {} cannot be completed from {:?}",
                            current.stage_key, current.state
                        ),
                    ));
                }
                progressed = true;
            }
            let current = self
                .repository
                .stages(&running.tenant_id, &running.task_id)
                .await?;
            if current.iter().all(|stage| stage.state.is_success()) {
                break;
            }
            if !progressed {
                return Err(CentralError::new(
                    crate::CentralErrorCode::InvalidState,
                    "stage dependency graph cannot be completed",
                ));
            }
        }
        let completed = self
            .transition(
                &running.task_id,
                &running.tenant_id,
                TaskState::Succeeded,
                actor,
                None,
            )
            .await?;
        let mut completed = completed;
        completed.stages = self
            .repository
            .stages(&running.tenant_id, &running.task_id)
            .await?;
        Ok(completed)
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
        let mut task = self
            .repository
            .get(&tenant_id, &task_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(not_found)?;
        task.stages = self
            .repository
            .stages(&tenant_id, &task_id)
            .await
            .map_err(map_central_error)?;
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
        Ok(QueryTaskResponse {
            task: task_view(&task),
            attempts,
            events,
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
        if current.intent_kind == TaskIntent::CommitMaterialize {
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
                            request_replayed: result.replayed || response.request_replayed,
                            execution_reused: latest.execution_reused || response.execution_reused,
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
            request_replayed: result.replayed,
            execution_reused: result.task.execution_reused,
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
            request_replayed: result.replayed,
            execution_reused: result.task.execution_reused,
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
        workspace_id: request
            .workspace_id
            .as_deref()
            .map(|value| {
                neoengram_domain::protocol::WorkspaceId::new(value.to_owned())
                    .map_err(|error| invalid_request(format!("workspace_id: {error}")))
            })
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
        intent_kinds: request
            .intent_kind
            .iter()
            .map(|value| {
                TaskIntent::from_str(value)
                    .map_err(|error| invalid_request(format!("intent_kind: {error}")))
            })
            .collect::<Result<_, _>>()?,
        purpose: request
            .purpose
            .as_deref()
            .map(|value| {
                neoengram_domain::protocol::TaskPurpose::from_str(value)
                    .map_err(|error| invalid_request(format!("purpose: {error}")))
            })
            .transpose()?,
        states: request
            .state
            .iter()
            .map(|value| parse_state(value))
            .collect::<Result<_, _>>()?,
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
        workspace_id: request.workspace_id.clone(),
        snapshot_id: request.snapshot_id.clone(),
        storage_volume_id: request.storage_volume_id.clone(),
        intent_kind: request.intent_kind.clone(),
        purpose: request.purpose.clone(),
        state: request.state.clone(),
        ..Default::default()
    })
}

pub(crate) fn task_view(task: &OperationTask) -> TaskView {
    let stages = stage_views_for_task(task);
    let current_stage = stages
        .iter()
        .find(|stage| stage.stage_key == task.current_stage_key)
        .cloned()
        .unwrap_or_else(|| stage_view_for_task(task));
    TaskView {
        task_id: task.task_id.to_string(),
        intent_kind: task.intent_kind.to_string(),
        purpose: task.purpose.map(|value| value.to_string()),
        state: state_name(task.state).to_owned(),
        tenant_id: task.tenant_id.to_string(),
        primary_resource: crate::dto::TaskResourceRefView {
            resource_kind: resource_kind_name(task.primary_resource.resource_kind).to_owned(),
            resource_id: task.primary_resource.resource_id.clone(),
        },
        resource_links: task
            .resource_links
            .iter()
            .map(|link| crate::dto::TaskResourceLinkView {
                resource_kind: resource_kind_name(link.resource_kind).to_owned(),
                resource_id: link.resource_id.clone(),
                role: resource_role_name(link.role).to_owned(),
            })
            .collect(),
        execution_id: task.execution_id.clone(),
        execution_key_digest: task.execution_key_digest.to_string(),
        execution_reused: task.execution_reused,
        current_stage: current_stage.clone(),
        stages,
        completion: task
            .finished_at_unix_ms
            .map(|finished| crate::dto::TaskCompletionView {
                outcome: if task.state == TaskState::Succeeded {
                    "succeeded"
                } else if task.state == TaskState::Cancelled {
                    "cancelled"
                } else {
                    "failed"
                }
                .to_owned(),
                finished_at_unix_ms: Some(finished.to_string()),
            }),
        request_id: task.request_id.to_string(),
        request_digest: task.request_digest.to_string(),
        actor: actor_name(&task.actor),
        attempt: task.attempt.to_string(),
        progress: progress_view(task.progress),
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

fn resource_kind_name(value: TaskResourceKind) -> &'static str {
    match value {
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

fn resource_role_name(value: neoengram_domain::protocol::TaskResourceRole) -> &'static str {
    match value {
        neoengram_domain::protocol::TaskResourceRole::Primary => "primary",
        neoengram_domain::protocol::TaskResourceRole::Source => "source",
        neoengram_domain::protocol::TaskResourceRole::Target => "target",
        neoengram_domain::protocol::TaskResourceRole::Related => "related",
    }
}

fn stage_view_for_task(task: &OperationTask) -> crate::dto::TaskStageView {
    let state = state_name(task.state).to_owned();
    let outcome = match task.state {
        TaskState::Succeeded => Some("succeeded".to_owned()),
        TaskState::Cancelled => Some("cancelled".to_owned()),
        TaskState::Failed => Some("failed".to_owned()),
        _ => None,
    };
    crate::dto::TaskStageView {
        stage_key: task.current_stage_key.clone(),
        stage_kind: task.current_stage_key.clone(),
        ordinal: "1".to_owned(),
        dependencies: Vec::new(),
        state,
        stage_attempt: task.attempt.to_string(),
        outcome,
        progress: progress_view(task.progress),
        detail_kind: None,
        detail_id: None,
        issue: task.issue.as_ref().map(issue_view),
        created_at_unix_ms: task.created_at_unix_ms.to_string(),
        updated_at_unix_ms: task.updated_at_unix_ms.to_string(),
        started_at_unix_ms: task.started_at_unix_ms.map(|value| value.to_string()),
        finished_at_unix_ms: task.finished_at_unix_ms.map(|value| value.to_string()),
        resource_version: task.resource_version.to_string(),
    }
}

fn stage_views_for_task(task: &OperationTask) -> Vec<crate::dto::TaskStageView> {
    if task.stages.is_empty() {
        return Vec::new();
    }
    task.stages
        .iter()
        .map(|stage| crate::dto::TaskStageView {
            stage_key: stage.stage_key.clone(),
            stage_kind: stage.stage_kind.clone(),
            ordinal: stage.ordinal.to_string(),
            dependencies: stage.dependencies.clone(),
            state: format_stage_state(stage.state),
            stage_attempt: stage.stage_attempt.to_string(),
            outcome: stage.outcome.map(format_stage_outcome),
            progress: progress_view(stage.progress),
            detail_kind: stage.detail_kind.clone(),
            detail_id: stage.detail_id.clone(),
            issue: stage.issue.as_ref().map(issue_view),
            created_at_unix_ms: stage.created_at_unix_ms.to_string(),
            updated_at_unix_ms: stage.updated_at_unix_ms.to_string(),
            started_at_unix_ms: stage.started_at_unix_ms.map(|value| value.to_string()),
            finished_at_unix_ms: stage.finished_at_unix_ms.map(|value| value.to_string()),
            resource_version: stage.resource_version.to_string(),
        })
        .collect()
}

fn format_stage_state(value: neoengram_domain::protocol::StageState) -> String {
    match value {
        neoengram_domain::protocol::StageState::Pending => "pending",
        neoengram_domain::protocol::StageState::Ready => "ready",
        neoengram_domain::protocol::StageState::Running => "running",
        neoengram_domain::protocol::StageState::Waiting => "waiting",
        neoengram_domain::protocol::StageState::Verifying => "verifying",
        neoengram_domain::protocol::StageState::Succeeded => "succeeded",
        neoengram_domain::protocol::StageState::Skipped => "skipped",
        neoengram_domain::protocol::StageState::NoOp => "no_op",
        neoengram_domain::protocol::StageState::Stalled => "stalled",
        neoengram_domain::protocol::StageState::Failed => "failed",
        neoengram_domain::protocol::StageState::Cancelling => "cancelling",
        neoengram_domain::protocol::StageState::Cancelled => "cancelled",
    }
    .to_owned()
}

fn format_stage_outcome(value: neoengram_domain::protocol::StageOutcome) -> String {
    match value {
        neoengram_domain::protocol::StageOutcome::Succeeded => "succeeded",
        neoengram_domain::protocol::StageOutcome::Skipped => "skipped",
        neoengram_domain::protocol::StageOutcome::NoOp => "no_op",
        neoengram_domain::protocol::StageOutcome::Reused => "reused",
        neoengram_domain::protocol::StageOutcome::Failed => "failed",
        neoengram_domain::protocol::StageOutcome::Cancelled => "cancelled",
    }
    .to_owned()
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
        current_stage_key: value.current_stage_key.clone(),
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
        cancelling: value.cancelling.to_string(),
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
        TaskState::Cancelling => "cancelling",
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
        "cancelling" => Ok(TaskState::Cancelling),
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

    #[test]
    fn scope_links_expose_materialization_query_dimensions() {
        let task_id = TaskId::new("task-scope-links").unwrap();
        let scope = TaskScope {
            tenant_id: TenantId::new("tenant-scope-links").unwrap(),
            project_id: Some(neoengram_domain::protocol::ProjectId::new("project-scope").unwrap()),
            artifact_id: Some(
                neoengram_domain::protocol::ArtifactId::new("artifact-scope").unwrap(),
            ),
            object_namespace_id: Some(
                neoengram_domain::protocol::ObjectNamespaceId::new("namespace-scope").unwrap(),
            ),
            commit_id: Some(CommitId::from_bytes([7; 32])),
            workspace_id: None,
            snapshot_id: None,
            storage_volume_id: Some(
                neoengram_domain::protocol::StorageVolumeId::new("volume-scope").unwrap(),
            ),
        };

        let links = scope_resource_links(&task_id, &scope);
        assert!(links.iter().any(|link| {
            link.resource_kind == TaskResourceKind::ObjectNamespace
                && link.resource_id == "namespace-scope"
        }));
        assert!(links.iter().any(|link| {
            link.resource_kind == TaskResourceKind::Commit
                && link.resource_id == scope.commit_id.unwrap().to_string()
                && link.role == TaskResourceRole::Source
        }));
        assert!(links.iter().any(|link| {
            link.resource_kind == TaskResourceKind::StorageVolume
                && link.resource_id == "volume-scope"
                && link.role == TaskResourceRole::Target
        }));
    }

    #[tokio::test]
    async fn stage_transitions_are_fenced_after_root_cancellation() {
        let repository = Arc::new(crate::InMemoryTaskRepository::default());
        let clock = Arc::new(crate::InMemoryClock::new(100));
        let coordinator = TaskCoordinator::new(repository.clone(), clock);
        let tenant_id = TenantId::new("task-stage-cancellation-tenant").unwrap();
        let actor = TaskActor::Principal(neoengram_domain::protocol::PrincipalRef {
            kind: neoengram_domain::protocol::PrincipalKind::System,
            id: neoengram_domain::protocol::PrincipalId::new("task-stage-test").unwrap(),
            extensions: neoengram_domain::protocol::Extensions::new(),
        });
        let (task, _) = coordinator
            .create_root(
                TaskIntent::ProjectCreate,
                TaskScope::new(tenant_id.clone()),
                neoengram_domain::protocol::RequestId::new("stage-cancellation-request").unwrap(),
                &serde_json::json!({ "project_id": "project-stage-test" }),
                actor.clone(),
                Some("project"),
                Some("project-stage-test"),
            )
            .await
            .unwrap();

        let cancelling = repository
            .transition(
                &tenant_id,
                &task.task_id,
                task.resource_version,
                TaskState::Cancelling,
                actor.clone(),
                None,
                Some("cancellation requested".to_owned()),
                UnixMillis::new(101),
            )
            .await
            .unwrap()
            .task;
        let error = coordinator
            .transition_stage(
                &task.task_id,
                &tenant_id,
                "validate",
                StageState::Ready,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), crate::CentralErrorCode::InvalidState);

        // The cancellation reconciler is still allowed to close an individual stage.
        let stage = coordinator
            .transition_stage(
                &task.task_id,
                &tenant_id,
                "validate",
                StageState::Cancelling,
                None,
            )
            .await
            .unwrap();
        assert_eq!(stage.state, StageState::Cancelling);

        let cancelled = repository
            .complete_cancellation(
                &tenant_id,
                &task.task_id,
                cancelling.resource_version,
                actor,
                UnixMillis::new(102),
            )
            .await
            .unwrap()
            .task;
        assert_eq!(cancelled.state, TaskState::Cancelled);
        let error = coordinator
            .transition_stage(
                &task.task_id,
                &tenant_id,
                "validate",
                StageState::Ready,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), crate::CentralErrorCode::InvalidState);
    }
}
