use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use async_trait::async_trait;
use neoengram_domain::core::{
    CommitId, FileRecord, IndexVersion, LogicalPath, Manifest, ManifestId, ObjectId,
};
use neoengram_domain::protocol::materialization::{
    MaterializationBatch, MaterializationJob, MaterializationJobKey, MaterializationJobState,
    MaterializationLeaseState, MaterializationObject, MaterializationObjectReceipt,
    MaterializationObjectState, ObjectPlacement as ObjectPlacementV2, ObjectReadLease,
    PlacementHealthObservation, PlacementHealthState, StagingLease, VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    object_read_lease_id, staging_lease_id, AgentId, ArtifactId, DecimalU64, Generation,
    JobAssignment, JobState, MaterializationBatchId, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchPage, ObjectReceiptId, OperationTask, PlacementGeneration, ReplicationId,
    ReplicationState, RequestId, ResourceRef, ResourceVersion, SequenceNumber, StorageVolumeId,
    TaskActor, TaskAttempt, TaskAttemptId, TaskEvent, TaskEventId, TaskEventKind, TaskId,
    TaskRelation, TaskResourceLink, TaskState, TenantId, UnixMillis, WireIndexVersion, WorkspaceId,
};

use crate::{
    apply_cancel, apply_commit, apply_head_publication_ack, apply_job_sync, apply_restart,
    assignment_identity, build_started, same_cancel_request, same_commit_request,
    same_restart_request, same_start_request,
    validation::{invalid, same_index_version, validate_initial_index_snapshot},
    AgentEnrollmentAuditEvent, AssignmentOutbox, AssignmentPublishOutcome,
    AssignmentReserveOutcome, AssignmentRetireOutcome, AuditEvent, AuditSink,
    AuthorityCapabilities, AuthorityLifecycleAction, AuthorityLifecycleImpact,
    AuthorityLifecycleMutationOutcome, AuthorityLifecycleRecord, AuthorityLifecycleRepository,
    AuthorityLifecycleRequest, AuthorityStore, AuthorizationRequest, Authorizer, CentralError,
    CentralErrorCode, CentralResult, Clock, ControlPlane, InMemoryAgentRegistry, IndexKey,
    IndexPublishOutcome, IndexPublishRejection, IndexPublishRequest, IndexPublisher,
    InitializeIndexSnapshotRequest, JobInsertOutcome, JobKey, JobOperation, JobRecord,
    JobRepository, MetadataBatchStager, ObjectCatalog, ObjectPlacementEvidence,
    PreCommitCancelRequest, PreCommitCommitOutcome, PreCommitCommitRequest,
    PreCommitCommitSnapshot, PreCommitKey, PreCommitMutationOutcome, PreCommitPhase,
    PreCommitRecord, PreCommitRepository, PreCommitRestartRequest, PreCommitStartRequest,
    PreCommitState, PublishedIndex, StagedMetadataBatch, TaskEventListPage, TaskEventListRequest,
    TaskInsertOutcome, TaskListPage, TaskListRequest, TaskMutationOutcome, TaskRelationRecord,
    TaskRepository, TaskResourceLinkRecord, TaskSummary,
};

use crate::{
    same_retry_request, valid_replication_transition, validate_replication_checkpoints,
    validate_replication_publication, validate_replication_record, CancelReplicationRequest,
    CommitAvailabilityRecord, FinalizeReplicationRequest, FinalizeReplicationResult,
    MaterializationBatchCasRequest, MaterializationObjectCasRequest, MaterializationPlan,
    MaterializationPlanInsertOutcome, MaterializationPlanReplacement,
    MaterializationReceiptRequest, PlacementRepository, RefreshReplicationRoutesRequest,
    ReplicationRecord, ReplicationRouteBinding, ReplicationStateTransitionRequest,
    RetryReplicationRequest, RetryReplicationResult, WorkspaceRecord,
};

#[derive(Debug, Default)]
pub struct AllowAllAuthorizer;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    async fn authorize(&self, _request: &AuthorizationRequest) -> CentralResult<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DenyAllAuthorizer;

#[async_trait]
impl Authorizer for DenyAllAuthorizer {
    async fn authorize(&self, _request: &AuthorizationRequest) -> CentralResult<()> {
        Err(invalid(
            CentralErrorCode::Unauthorized,
            "the actor is not authorized for this managed Add operation",
        ))
    }
}

type MaterializationPlacementKey = (
    TenantId,
    neoengram_domain::protocol::ObjectNamespaceId,
    ObjectId,
    StorageVolumeId,
    PlacementGeneration,
);
type MaterializationPlacementMap = BTreeMap<MaterializationPlacementKey, ObjectPlacementV2>;
type VolumeCoverageKey = (
    TenantId,
    neoengram_domain::protocol::ObjectNamespaceId,
    CommitId,
    StorageVolumeId,
    PlacementGeneration,
);
type VolumeCoverageMap = BTreeMap<VolumeCoverageKey, VolumeCommitCoverage>;
type MaterializationReceiptKey = (
    TenantId,
    neoengram_domain::protocol::ObjectNamespaceId,
    ObjectReceiptId,
);
type MaterializationReceiptMap =
    BTreeMap<MaterializationReceiptKey, (MaterializationObjectReceipt, ObjectPlacementV2)>;
type PlacementHealthKey = (
    TenantId,
    neoengram_domain::protocol::ObjectNamespaceId,
    neoengram_domain::protocol::PlacementId,
    PlacementGeneration,
);

#[derive(Debug, Default)]
pub struct InMemoryJobRepository {
    jobs: Mutex<BTreeMap<JobKey, JobRecord>>,
}

impl InMemoryJobRepository {
    pub fn all(&self) -> CentralResult<Vec<JobRecord>> {
        Ok(lock(&self.jobs)?.values().cloned().collect())
    }
}

#[async_trait]
impl JobRepository for InMemoryJobRepository {
    async fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        Ok(lock(&self.jobs)?.get(key).cloned())
    }

    async fn list_recoverable(
        &self,
        after: Option<&JobKey>,
        now: UnixMillis,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>> {
        Ok(lock(&self.jobs)?
            .values()
            .filter(|job| after.is_none_or(|after| job.key() > *after))
            .filter(|job| crate::mapper_recovery_predicate(job, now))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_pending_decisions_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>> {
        Ok(lock(&self.jobs)?
            .values()
            .filter(|job| pending_decision_for_agent(job, agent_id))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        let key = job.key();
        let mut jobs = lock(&self.jobs)?;
        if let Some(existing) = jobs.get(&key) {
            return Ok(JobInsertOutcome::Existing(existing.clone()));
        }
        jobs.insert(key, job.clone());
        Ok(JobInsertOutcome::Inserted(job))
    }

    async fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        let key = job.key();
        let mut jobs = lock(&self.jobs)?;
        let persisted = jobs.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::JobNotFound,
                format!("job {} disappeared during update", key.job_id),
            )
        })?;
        if persisted.resource_version.get() != expected
            || job.resource_version.get() != expected.saturating_add(1)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                format!("job {} ResourceVersion changed", key.job_id),
            ));
        }
        jobs.insert(key, job.clone());
        Ok(job)
    }
}

type TaskMapKey = (TenantId, TaskId);
const MAX_TASK_PAGE_SIZE: usize = 500;

#[derive(Debug, Default)]
struct InMemoryTaskState {
    tasks: BTreeMap<TaskMapKey, OperationTask>,
    attempts: BTreeMap<TaskMapKey, Vec<TaskAttempt>>,
    events: BTreeMap<TaskMapKey, Vec<TaskEvent>>,
    links: BTreeMap<(TenantId, TaskId), Vec<TaskResourceLink>>,
    relations: BTreeMap<(TenantId, TaskId), Vec<TaskRelation>>,
}

/// In-memory implementation of the unified operation-task authority. A single mutex protects
/// the task row and all of its child audit/relationship records so mutation helpers have the same
/// atomic visibility semantics as the SQLite transaction implementation.
#[derive(Debug, Default)]
pub struct InMemoryTaskRepository {
    state: Mutex<InMemoryTaskState>,
}

impl InMemoryTaskRepository {
    pub fn all(&self) -> CentralResult<Vec<OperationTask>> {
        Ok(lock(&self.state)?.tasks.values().cloned().collect())
    }

    fn task_key(tenant_id: &TenantId, task_id: &TaskId) -> TaskMapKey {
        (tenant_id.clone(), task_id.clone())
    }

    fn task_or_not_found<'a>(
        state: &'a InMemoryTaskState,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<&'a OperationTask> {
        state
            .tasks
            .get(&Self::task_key(tenant_id, task_id))
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "operation task not found",
                )
            })
    }
}

#[async_trait]
impl TaskRepository for InMemoryTaskRepository {
    async fn get(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Option<OperationTask>> {
        Ok(lock(&self.state)?
            .tasks
            .get(&Self::task_key(tenant_id, task_id))
            .cloned())
    }

    async fn get_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<OperationTask>> {
        Ok(lock(&self.state)?
            .tasks
            .values()
            .find(|task| &task.tenant_id == tenant_id && &task.request_id == request_id)
            .cloned())
    }

    async fn list(&self, request: &TaskListRequest) -> CentralResult<TaskListPage> {
        if request.page_size > MAX_TASK_PAGE_SIZE {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "task page_size must be at most 500",
            ));
        }
        if request.page_size == 0 {
            return Ok(TaskListPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let cursor = request.cursor.as_deref().unwrap_or_default();
        let state = lock(&self.state)?;
        let mut items = state
            .tasks
            .values()
            .filter(|task| task_matches_request(task, request))
            .filter(|task| task.task_id.as_str() > cursor)
            .take(request.page_size.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let next_cursor = if items.len() > request.page_size {
            items.pop().map(|task| task.task_id.to_string())
        } else {
            None
        };
        Ok(TaskListPage { items, next_cursor })
    }

    async fn list_events(
        &self,
        request: &TaskEventListRequest,
    ) -> CentralResult<TaskEventListPage> {
        if request.page_size > MAX_TASK_PAGE_SIZE {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "task page_size must be at most 500",
            ));
        }
        if request.page_size == 0 {
            return Ok(TaskEventListPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let state = lock(&self.state)?;
        Self::task_or_not_found(&state, &request.tenant_id, &request.task_id)?;
        let after = request.after_sequence.map_or(0, SequenceNumber::get);
        let mut items = state
            .events
            .get(&Self::task_key(&request.tenant_id, &request.task_id))
            .into_iter()
            .flat_map(|events| events.iter())
            .filter(|event| event.sequence.get() > after)
            .take(request.page_size.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let next_cursor = if items.len() > request.page_size {
            items.pop().map(|event| event.sequence.to_string())
        } else {
            None
        };
        Ok(TaskEventListPage { items, next_cursor })
    }

    async fn summary(&self, request: &TaskListRequest) -> CentralResult<TaskSummary> {
        let state = lock(&self.state)?;
        let mut summary = TaskSummary::default();
        for task in state
            .tasks
            .values()
            .filter(|task| task_matches_request(task, request))
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
        if let Some(attempt) = &attempt {
            attempt.validate().map_err(CentralError::from)?;
            if attempt.task_id != task.task_id || attempt.attempt != task.attempt {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "initial task attempt does not match task identity",
                ));
            }
        }
        if let Some(event) = &event {
            event.validate().map_err(CentralError::from)?;
            if event.task_id != task.task_id || event.attempt != task.attempt {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "initial task event does not match task identity",
                ));
            }
        }
        let mut state = lock(&self.state)?;
        let key = Self::task_key(&task.tenant_id, &task.task_id);
        if let Some(existing) = state.tasks.get(&key) {
            if existing.request_digest != task.request_digest
                || existing.request_id != task.request_id
                || existing.task_kind != task.task_kind
            {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "operation task identity is already bound to a different request",
                ));
            }
            return Ok(TaskInsertOutcome::Existing(existing.clone()));
        }
        if state.tasks.values().any(|existing| {
            existing.tenant_id == task.tenant_id && existing.request_id == task.request_id
        }) {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "request ID is already bound to another operation task",
            ));
        }
        if let Some(parent) = &task.parent_task_id {
            if !state
                .tasks
                .contains_key(&(task.tenant_id.clone(), parent.clone()))
            {
                return Err(invalid(
                    CentralErrorCode::ResourceNotFound,
                    "parent operation task does not exist",
                ));
            }
        }
        if let Some(event) = &event {
            if event.sequence.get() != 1 {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "the first task event must use sequence 1",
                ));
            }
        }
        state.tasks.insert(key.clone(), task.clone());
        if let Some(parent_task_id) = &task.parent_task_id {
            state.relations.insert(
                key.clone(),
                vec![TaskRelation {
                    task_id: task.task_id.clone(),
                    related_task_id: parent_task_id.clone(),
                    relation: neoengram_domain::protocol::TaskRelationKind::Parent,
                }],
            );
        }
        if let Some(attempt) = attempt {
            state.attempts.insert(key.clone(), vec![attempt]);
        }
        if let Some(event) = event {
            state.events.insert(key, vec![event]);
        }
        Ok(TaskInsertOutcome::Inserted(task))
    }

    async fn replace(
        &self,
        expected_resource_version: ResourceVersion,
        task: OperationTask,
    ) -> CentralResult<OperationTask> {
        task.validate().map_err(CentralError::from)?;
        let mut state = lock(&self.state)?;
        let key = Self::task_key(&task.tenant_id, &task.task_id);
        let current = state.tasks.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
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
        state.tasks.insert(key, task.clone());
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
        let mut state = lock(&self.state)?;
        let key = Self::task_key(tenant_id, task_id);
        let current = state.tasks.get(&key).cloned().ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
        if current.resource_version != expected_resource_version {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "operation task resource version changed",
            ));
        }
        if current.state == next {
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
        let attempts = state.attempts.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "operation task has no current attempt",
            )
        })?;
        let attempt_index = attempts
            .iter()
            .position(|attempt| attempt.attempt == task.attempt)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "operation task current attempt is missing",
                )
            })?;
        let mut attempt = attempts[attempt_index].clone();
        attempt.issue = task.issue.clone();
        attempt
            .transition_to(next, now)
            .map_err(CentralError::from)?;
        let sequence = state
            .events
            .get(&key)
            .and_then(|events| events.last())
            .map_or(1, |event| event.sequence.get().saturating_add(1));
        let mut event = TaskEvent::state_change(
            TaskEventId::new(format!("{}-event-{sequence}", task.task_id))
                .map_err(CentralError::from)?,
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

        state.tasks.insert(key.clone(), task.clone());
        state
            .attempts
            .get_mut(&key)
            .expect("attempt collection was validated above")[attempt_index] = attempt;
        state.events.entry(key).or_default().push(event);
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
        let state = lock(&self.state)?;
        Self::task_or_not_found(&state, tenant_id, task_id)?;
        Ok(state
            .attempts
            .get(&Self::task_key(tenant_id, task_id))
            .cloned()
            .unwrap_or_default())
    }

    async fn insert_attempt(
        &self,
        tenant_id: &TenantId,
        attempt: TaskAttempt,
    ) -> CentralResult<TaskAttempt> {
        attempt.validate().map_err(CentralError::from)?;
        let mut state = lock(&self.state)?;
        let key = Self::task_key(tenant_id, &attempt.task_id);
        let task = state.tasks.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
        if attempt.attempt > task.attempt {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "task attempt exceeds current task attempt",
            ));
        }
        let attempts = state.attempts.entry(key).or_default();
        if let Some(existing) = attempts
            .iter()
            .find(|candidate| candidate.attempt_id == attempt.attempt_id)
        {
            if existing == &attempt {
                return Ok(existing.clone());
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task attempt ID was reused",
            ));
        }
        if attempts
            .iter()
            .any(|candidate| candidate.attempt == attempt.attempt)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task attempt number was reused",
            ));
        }
        attempts.push(attempt.clone());
        attempts.sort_by_key(|candidate| candidate.attempt);
        Ok(attempt)
    }

    async fn replace_attempt(
        &self,
        tenant_id: &TenantId,
        expected_resource_version: ResourceVersion,
        attempt: TaskAttempt,
    ) -> CentralResult<TaskAttempt> {
        attempt.validate().map_err(CentralError::from)?;
        let mut state = lock(&self.state)?;
        let key = Self::task_key(tenant_id, &attempt.task_id);
        if !state.tasks.contains_key(&key) {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        if attempt.attempt
            > state
                .tasks
                .get(&key)
                .map_or(Generation::new(0), |task| task.attempt)
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "task attempt exceeds current task attempt",
            ));
        }
        let attempts = state
            .attempts
            .get_mut(&key)
            .ok_or_else(|| invalid(CentralErrorCode::ResourceNotFound, "task attempt not found"))?;
        let current = attempts
            .iter_mut()
            .find(|candidate| candidate.attempt_id == attempt.attempt_id)
            .ok_or_else(|| invalid(CentralErrorCode::ResourceNotFound, "task attempt not found"))?;
        if current.resource_version != expected_resource_version
            || attempt.resource_version.get() != expected_resource_version.get().saturating_add(1)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task attempt resource version changed",
            ));
        }
        if current.task_id != attempt.task_id || current.attempt != attempt.attempt {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "immutable task attempt identity changed",
            ));
        }
        *current = attempt.clone();
        Ok(attempt)
    }

    async fn append_event(
        &self,
        tenant_id: &TenantId,
        event: TaskEvent,
    ) -> CentralResult<TaskEvent> {
        event.validate().map_err(CentralError::from)?;
        let mut state = lock(&self.state)?;
        let key = Self::task_key(tenant_id, &event.task_id);
        if !state.tasks.contains_key(&key) {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            ));
        }
        if event.attempt
            > state
                .tasks
                .get(&key)
                .map_or(Generation::new(0), |task| task.attempt)
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "task event attempt exceeds current task attempt",
            ));
        }
        let events = state.events.entry(key).or_default();
        if let Some(existing) = events
            .iter()
            .find(|candidate| candidate.event_id == event.event_id)
        {
            if existing == &event {
                return Ok(existing.clone());
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "task event ID was reused",
            ));
        }
        let expected = events
            .last()
            .map_or(1, |last| last.sequence.get().saturating_add(1));
        if event.sequence.get() != expected {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                format!("task event sequence must be {expected}"),
            ));
        }
        events.push(event.clone());
        Ok(event)
    }

    async fn link_resource(&self, record: TaskResourceLinkRecord) -> CentralResult<bool> {
        record.link.validate().map_err(CentralError::from)?;
        let mut state = lock(&self.state)?;
        let key = Self::task_key(&record.tenant_id, &record.link.task_id);
        let task =
            Self::task_or_not_found(&state, &record.tenant_id, &record.link.task_id)?.clone();
        if state
            .links
            .get(&key)
            .is_some_and(|links| links.iter().any(|candidate| candidate == &record.link))
        {
            return Ok(true);
        }
        let sequence = state
            .events
            .get(&key)
            .and_then(|events| events.last())
            .map_or(1, |event| event.sequence.get().saturating_add(1));
        let event = TaskEvent {
            event_id: TaskEventId::new(format!("{}-event-{sequence}", task.task_id))
                .map_err(CentralError::from)?,
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
        let links = state.links.entry(key.clone()).or_default();
        links.push(record.link);
        links.sort_by(|left, right| {
            (left.resource_kind, &left.resource_id, left.role).cmp(&(
                right.resource_kind,
                &right.resource_id,
                right.role,
            ))
        });
        state.events.entry(key).or_default().push(event);
        Ok(false)
    }

    async fn resources(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Vec<TaskResourceLink>> {
        let state = lock(&self.state)?;
        Self::task_or_not_found(&state, tenant_id, task_id)?;
        Ok(state
            .links
            .get(&Self::task_key(tenant_id, task_id))
            .cloned()
            .unwrap_or_default())
    }

    async fn add_relation(&self, record: TaskRelationRecord) -> CentralResult<bool> {
        record.relation.validate().map_err(CentralError::from)?;
        let mut state = lock(&self.state)?;
        Self::task_or_not_found(&state, &record.tenant_id, &record.relation.task_id)?;
        Self::task_or_not_found(&state, &record.tenant_id, &record.relation.related_task_id)?;
        let key = Self::task_key(&record.tenant_id, &record.relation.task_id);
        if state.relations.get(&key).is_some_and(|relations| {
            relations
                .iter()
                .any(|existing| existing == &record.relation)
        }) {
            return Ok(true);
        }
        let mut all = state
            .relations
            .iter()
            .filter(|((tenant, _), _)| tenant == &record.tenant_id)
            .flat_map(|(_, relations)| relations.iter())
            .cloned()
            .collect::<Vec<_>>();
        all.push(record.relation.clone());
        neoengram_domain::protocol::validate_task_relations(&all).map_err(CentralError::from)?;
        state
            .relations
            .entry(key)
            .or_default()
            .push(record.relation);
        Ok(false)
    }

    async fn relations(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
    ) -> CentralResult<Vec<TaskRelation>> {
        let state = lock(&self.state)?;
        Self::task_or_not_found(&state, tenant_id, task_id)?;
        Ok(state
            .relations
            .get(&Self::task_key(tenant_id, task_id))
            .cloned()
            .unwrap_or_default())
    }

    async fn retry(
        &self,
        tenant_id: &TenantId,
        task_id: &TaskId,
        expected_resource_version: ResourceVersion,
        actor: TaskActor,
        now: UnixMillis,
    ) -> CentralResult<TaskMutationOutcome> {
        let mut state = lock(&self.state)?;
        let key = Self::task_key(tenant_id, task_id);
        let current = state.tasks.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
        if current.resource_version != expected_resource_version {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "operation task resource version changed",
            ));
        }
        let mut task = current.clone();
        task.retry(now).map_err(CentralError::from)?;
        let attempt_id = TaskAttemptId::new(format!("{}-attempt-{}", task.task_id, task.attempt))
            .map_err(CentralError::from)?;
        let new_attempt = TaskAttempt::new(task.task_id.clone(), attempt_id, task.attempt, now);
        let sequence = state
            .events
            .get(&key)
            .and_then(|events| events.last())
            .map_or(1, |event| event.sequence.get().saturating_add(1));
        let event = TaskEvent {
            event_id: TaskEventId::new(format!("{}-event-{}", task.task_id, sequence))
                .map_err(CentralError::from)?,
            task_id: task.task_id.clone(),
            sequence: SequenceNumber::new(sequence),
            attempt: task.attempt,
            kind: TaskEventKind::Retried,
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
        event.validate().map_err(CentralError::from)?;
        state.tasks.insert(key.clone(), task.clone());
        state
            .attempts
            .entry(key.clone())
            .or_default()
            .push(new_attempt);
        state.events.entry(key).or_default().push(event);
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
        let mut state = lock(&self.state)?;
        let key = Self::task_key(tenant_id, task_id);
        let current = state.tasks.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "operation task not found",
            )
        })?;
        if let Some(expected) = expected_resource_version {
            if current.resource_version != expected {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "operation task resource version changed",
                ));
            }
        }
        if current.state == TaskState::Cancelled {
            return Ok(TaskMutationOutcome {
                task: current.clone(),
                replayed: true,
            });
        }
        let mut task = current.clone();
        task.cancel(now).map_err(CentralError::from)?;
        let attempts = state.attempts.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "operation task has no current attempt",
            )
        })?;
        let attempt_index = attempts
            .iter()
            .position(|attempt| attempt.attempt == task.attempt)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "operation task current attempt is missing",
                )
            })?;
        let mut attempt = attempts[attempt_index].clone();
        attempt
            .transition_to(TaskState::Cancelled, now)
            .map_err(CentralError::from)?;
        let sequence = state
            .events
            .get(&key)
            .and_then(|events| events.last())
            .map_or(1, |event| event.sequence.get().saturating_add(1));
        let event = TaskEvent {
            event_id: TaskEventId::new(format!("{}-event-{}", task.task_id, sequence))
                .map_err(CentralError::from)?,
            task_id: task.task_id.clone(),
            sequence: SequenceNumber::new(sequence),
            attempt: task.attempt,
            kind: TaskEventKind::Cancelled,
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
        state.tasks.insert(key.clone(), task.clone());
        state
            .attempts
            .get_mut(&key)
            .expect("attempt collection was validated above")[attempt_index] = attempt;
        state.events.entry(key).or_default().push(event);
        Ok(TaskMutationOutcome {
            task,
            replayed: false,
        })
    }
}

fn task_matches_request(task: &OperationTask, request: &TaskListRequest) -> bool {
    task.tenant_id == request.tenant_id
        && request
            .project_id
            .as_ref()
            .is_none_or(|value| task.project_id.as_ref() == Some(value))
        && request
            .artifact_id
            .as_ref()
            .is_none_or(|value| task.artifact_id.as_ref() == Some(value))
        && request
            .object_namespace_id
            .as_ref()
            .is_none_or(|value| task.object_namespace_id.as_ref() == Some(value))
        && request
            .commit_id
            .is_none_or(|value| task.commit_id == Some(value))
        && request
            .playground_id
            .as_ref()
            .is_none_or(|value| task.playground_id.as_ref() == Some(value))
        && request
            .snapshot_id
            .as_ref()
            .is_none_or(|value| task.snapshot_id.as_ref() == Some(value))
        && request
            .storage_volume_id
            .as_ref()
            .is_none_or(|value| task.storage_volume_id.as_ref() == Some(value))
        && (request.task_kinds.is_empty() || request.task_kinds.contains(&task.task_kind))
        && (request.states.is_empty() || request.states.contains(&task.state))
        && request
            .parent_task_id
            .as_ref()
            .is_none_or(|value| task.parent_task_id.as_ref() == Some(value))
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

/// In-memory counterpart of the Placement authority used by service tests and local runs.
///
/// Request IDs are indexed separately from resource IDs so retries return the exact original
/// record.  Object bytes are deliberately absent: this repository models only immutable
/// placement/transfer metadata, just like the SQLite authority.
#[derive(Debug, Default)]
pub struct InMemoryPlacementRepository {
    commit_object_sets: Mutex<
        BTreeMap<
            (TenantId, neoengram_domain::core::ContentDigest),
            neoengram_domain::protocol::CommitObjectSet,
        >,
    >,
    placement_sets: Mutex<
        BTreeMap<
            (
                TenantId,
                neoengram_domain::core::ContentDigest,
                neoengram_domain::protocol::BackendId,
            ),
            neoengram_domain::protocol::CommitPlacementSet,
        >,
    >,
    object_placements: Mutex<
        BTreeMap<
            (TenantId, neoengram_domain::core::ObjectId),
            Vec<neoengram_domain::protocol::ObjectPlacement>,
        >,
    >,
    replications: Mutex<BTreeMap<(TenantId, ReplicationId), ReplicationRecord>>,
    replication_objects:
        Mutex<BTreeMap<(TenantId, ReplicationId, ObjectId), crate::ReplicationObjectRecord>>,
    replication_requests: Mutex<BTreeMap<(TenantId, RequestId), ReplicationId>>,
    replication_retry_mutations:
        Mutex<BTreeMap<(TenantId, RequestId), (RetryReplicationRequest, ReplicationRecord)>>,
    workspaces: Mutex<BTreeMap<(TenantId, WorkspaceId), WorkspaceRecord>>,
    workspace_requests: Mutex<BTreeMap<(TenantId, RequestId), WorkspaceId>>,
    materialization_placements: Mutex<MaterializationPlacementMap>,
    volume_commit_coverages: Mutex<VolumeCoverageMap>,
    materializations: Mutex<
        BTreeMap<
            (
                TenantId,
                neoengram_domain::protocol::ObjectNamespaceId,
                neoengram_domain::protocol::MaterializationId,
            ),
            MaterializationJob,
        >,
    >,
    materialization_keys: Mutex<BTreeMap<MaterializationJobKey, MaterializationJob>>,
    materialization_batches: Mutex<
        BTreeMap<
            (
                TenantId,
                neoengram_domain::protocol::ObjectNamespaceId,
                neoengram_domain::protocol::MaterializationBatchId,
            ),
            MaterializationBatch,
        >,
    >,
    materialization_objects: Mutex<
        BTreeMap<
            (
                TenantId,
                neoengram_domain::protocol::MaterializationId,
                neoengram_domain::protocol::ObjectNamespaceId,
                ObjectId,
            ),
            MaterializationObject,
        >,
    >,
    object_read_leases: Mutex<
        BTreeMap<
            (
                TenantId,
                neoengram_domain::protocol::ObjectNamespaceId,
                neoengram_domain::protocol::LeaseId,
            ),
            ObjectReadLease,
        >,
    >,
    staging_leases: Mutex<
        BTreeMap<
            (
                TenantId,
                neoengram_domain::protocol::ObjectNamespaceId,
                neoengram_domain::protocol::LeaseId,
            ),
            StagingLease,
        >,
    >,
    materialization_receipts: Mutex<MaterializationReceiptMap>,
    placement_health: Mutex<BTreeMap<PlacementHealthKey, PlacementHealthObservation>>,
    /// Serializes the multi-record receipt publication boundary.  The individual maps remain
    /// independently queryable, while receipt writers observe one deterministic state transition.
    materialization_receipt_gate: tokio::sync::Mutex<()>,
}

impl InMemoryPlacementRepository {
    fn materialization_for_namespace(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Option<MaterializationJob>> {
        Ok(lock(&self.materializations)?
            .get(&(
                tenant_id.clone(),
                object_namespace_id.clone(),
                materialization_id.clone(),
            ))
            .cloned())
    }

    async fn release_receipt_leases(
        &self,
        receipt: &MaterializationObjectReceipt,
        _batch: &MaterializationBatch,
        task: &MaterializationObject,
    ) -> CentralResult<()> {
        let source_ids = task
            .primary_source
            .iter()
            .chain(task.fallback_sources.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        for source_id in source_ids {
            let lease_id = object_read_lease_id(
                &receipt.materialization_id,
                &receipt.batch_id,
                receipt.plan_revision,
                receipt.batch_attempt,
                &receipt.object_namespace_id,
                receipt.object_id,
                &source_id,
            )
            .map_err(CentralError::from)?;
            self.release_object_read_lease(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &lease_id,
            )
            .await?;
        }
        let lease_id = staging_lease_id(
            &receipt.materialization_id,
            receipt.plan_revision,
            &receipt.object_namespace_id,
            receipt.object_id,
        )
        .map_err(CentralError::from)?;
        self.release_staging_lease(&receipt.tenant_id, &receipt.object_namespace_id, &lease_id)
            .await?;
        Ok(())
    }

    // Receipt publication already owns `materialization_receipt_gate`; keep the actual CAS
    // mutation in a separate helper so the public mutation path can take the same gate without
    // recursively locking the non-reentrant Tokio mutex.
    async fn replace_materialization_unlocked(
        &self,
        tenant_id: &TenantId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
        expected_plan_revision: neoengram_domain::protocol::Generation,
        job: MaterializationJob,
    ) -> CentralResult<MaterializationJob> {
        job.validate().map_err(CentralError::from)?;
        if &job.key.tenant_id != tenant_id || &job.materialization_id != materialization_id {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization replacement identity does not match its key",
            ));
        }
        if job.plan_revision < expected_plan_revision {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision moved backwards",
            ));
        }
        let mut jobs = lock(&self.materializations)?;
        let current = jobs
            .get(&(
                tenant_id.clone(),
                job.key.object_namespace_id.clone(),
                materialization_id.clone(),
            ))
            .cloned()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if current.key != job.key {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization replacement cannot change its immutable target key",
            ));
        }
        if current.plan_revision != expected_plan_revision {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision changed",
            ));
        }
        if !current.state.can_transition_to(job.state) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization state transition is not allowed",
            ));
        }
        if job.plan_revision > Generation::new(expected_plan_revision.get().saturating_add(1)) {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision advanced by more than one",
            ));
        }
        jobs.insert(
            (
                tenant_id.clone(),
                job.key.object_namespace_id.clone(),
                materialization_id.clone(),
            ),
            job.clone(),
        );
        lock(&self.materialization_keys)?.insert(job.key.clone(), job.clone());
        Ok(job)
    }
}

#[async_trait]
impl PlacementRepository for InMemoryPlacementRepository {
    async fn insert_object_placement_v2(
        &self,
        placement: ObjectPlacementV2,
    ) -> CentralResult<ObjectPlacementV2> {
        placement.validate().map_err(CentralError::from)?;
        let volume = placement.storage_volume_id.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::ProtocolInvalid,
                "v2 object placements currently require a StorageVolume",
            )
        })?;
        let key = (
            placement.tenant_id.clone(),
            placement.object_namespace_id.clone(),
            placement.object_id,
            volume,
            placement.placement_generation,
        );
        let mut values = lock(&self.materialization_placements)?;
        if let Some((_, existing)) =
            values
                .iter()
                .find(|((tenant, namespace, _, _, _), existing)| {
                    tenant == &placement.tenant_id
                        && namespace == &placement.object_namespace_id
                        && existing.placement_id == placement.placement_id
                })
        {
            return if existing == &placement {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "v2 placement ID is already bound to different metadata",
                ))
            };
        }
        if let Some(existing) = values.get(&key) {
            return if existing == &placement {
                Ok(existing.clone())
            } else if existing.tenant_id == placement.tenant_id
                && existing.object_namespace_id == placement.object_namespace_id
                && existing.object_id == placement.object_id
                && existing.size == placement.size
                && existing.encoding == placement.encoding
                && existing.verified_digest == placement.verified_digest
                && existing.storage_volume_id == placement.storage_volume_id
                && existing.archive_id == placement.archive_id
                && existing.placement_generation == placement.placement_generation
                && existing.state == placement.state
                && existing.failure_domain == placement.failure_domain
            {
                // Two concurrent Agents may acknowledge the same durable object with different
                // receipt IDs. The physical identity is the namespace/object/Volume/generation;
                // retain the first Placement identity and make the loser an exact replay.
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "v2 object placement identity is already bound to different metadata",
                ))
            };
        }
        values.insert(key, placement.clone());
        Ok(placement)
    }

    async fn object_placements_v2(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        object_id: &ObjectId,
    ) -> CentralResult<Vec<ObjectPlacementV2>> {
        Ok(lock(&self.materialization_placements)?
            .iter()
            .filter(|((tenant, namespace, object, _, _), _)| {
                tenant == tenant_id && namespace == object_namespace_id && object == object_id
            })
            .map(|(_, placement)| placement.clone())
            .collect())
    }

    async fn record_placement_health_observation(
        &self,
        observation: PlacementHealthObservation,
    ) -> CentralResult<PlacementHealthObservation> {
        observation.validate().map_err(CentralError::from)?;
        let placement = lock(&self.materialization_placements)?
            .values()
            .find(|placement| {
                placement.tenant_id == observation.tenant_id
                    && placement.object_namespace_id == observation.object_namespace_id
                    && placement.placement_id == observation.placement_id
                    && placement.object_id == observation.object_id
                    && placement.storage_volume_id.as_ref() == Some(&observation.storage_volume_id)
                    && placement.placement_generation == observation.placement_generation
            })
            .cloned()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "integrity observation references an unknown placement",
                )
            })?;
        if placement.size != observation.observed_size
            && observation.state == PlacementHealthState::Healthy
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "healthy integrity observation size differs from placement evidence",
            ));
        }
        let key = (
            observation.tenant_id.clone(),
            observation.object_namespace_id.clone(),
            observation.placement_id.clone(),
            observation.placement_generation,
        );
        let mut health = lock(&self.placement_health)?;
        if let Some(existing) = health.get(&key) {
            if existing == &observation {
                return Ok(existing.clone());
            }
            if observation.observed_at_unix_ms < existing.observed_at_unix_ms {
                return Ok(existing.clone());
            }
            if observation.observed_at_unix_ms == existing.observed_at_unix_ms {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "integrity observations have conflicting timestamps",
                ));
            }
        }
        health.insert(key, observation.clone());
        Ok(observation)
    }

    async fn latest_placement_health(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        placement_id: &neoengram_domain::protocol::PlacementId,
        placement_generation: PlacementGeneration,
    ) -> CentralResult<Option<PlacementHealthObservation>> {
        Ok(lock(&self.placement_health)?
            .get(&(
                tenant_id.clone(),
                object_namespace_id.clone(),
                placement_id.clone(),
                placement_generation,
            ))
            .cloned())
    }

    async fn upsert_volume_commit_coverage(
        &self,
        coverage: VolumeCommitCoverage,
    ) -> CentralResult<VolumeCommitCoverage> {
        coverage.validate().map_err(CentralError::from)?;
        let object_set = self
            .get_commit_object_set(&coverage.tenant_id, &coverage.commit_id.digest())
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "coverage references an unknown Commit ObjectSet",
                )
            })?;
        coverage
            .validate_against(&object_set.object_set)
            .map_err(CentralError::from)?;
        // Health observations are the latest evidence about whether a Placement is physically
        // readable. Coverage is derived from healthy evidence, not from the durable Placement
        // row alone; otherwise a scrub-reported missing object could not demote a cached summary.
        let health = lock(&self.placement_health)?;
        let placements = lock(&self.materialization_placements)?
            .values()
            .filter(|placement| {
                placement.tenant_id == coverage.tenant_id
                    && placement.object_namespace_id == coverage.object_namespace_id
                    && placement.storage_volume_id.as_ref() == Some(&coverage.storage_volume_id)
                    && placement.placement_generation == coverage.placement_generation
            })
            .filter(|placement| {
                let key = (
                    placement.tenant_id.clone(),
                    placement.object_namespace_id.clone(),
                    placement.placement_id.clone(),
                    placement.placement_generation,
                );
                !health.get(&key).is_some_and(|observation| {
                    matches!(
                        observation.state,
                        PlacementHealthState::Missing | PlacementHealthState::Corrupt
                    )
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let recomputed = VolumeCommitCoverage::from_placements(
            coverage.tenant_id.clone(),
            coverage.object_namespace_id.clone(),
            coverage.commit_id,
            coverage.storage_volume_id.clone(),
            coverage.placement_generation,
            &object_set.object_set,
            &placements,
        )
        .map_err(CentralError::from)?;
        if coverage.object_set_digest != recomputed.object_set_digest
            || coverage.object_count != recomputed.object_count
            || coverage.verified_object_count != recomputed.verified_object_count
            || coverage.total_bytes != recomputed.total_bytes
            || coverage.verified_bytes != recomputed.verified_bytes
            || (matches!(
                coverage.state,
                neoengram_domain::protocol::materialization::CoverageState::Partial
                    | neoengram_domain::protocol::materialization::CoverageState::Complete
            ) && coverage.state != recomputed.state)
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "coverage does not match verified object placement evidence",
            ));
        }
        let key = (
            coverage.tenant_id.clone(),
            coverage.object_namespace_id.clone(),
            coverage.commit_id,
            coverage.storage_volume_id.clone(),
            coverage.placement_generation,
        );
        let mut values = lock(&self.volume_commit_coverages)?;
        if let Some(existing) = values.get(&key) {
            // Coverage is recomputable, so replacing a summary is allowed. In particular, a
            // newer integrity observation may demote `complete` to `partial` for this generation.
            if existing.object_set_digest != coverage.object_set_digest
                || existing.object_count != coverage.object_count
                || existing.total_bytes != coverage.total_bytes
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "coverage identity is already bound to different Commit metadata",
                ));
            }
        }
        values.insert(key, coverage.clone());
        Ok(coverage)
    }

    async fn volume_commit_coverages(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<VolumeCommitCoverage>> {
        Ok(lock(&self.volume_commit_coverages)?
            .iter()
            .filter(|((tenant, namespace, commit, _, _), _)| {
                tenant == tenant_id
                    && namespace == object_namespace_id
                    && commit.digest() == *commit_id
            })
            .map(|(_, coverage)| coverage.clone())
            .collect())
    }

    async fn insert_materialization(
        &self,
        job: MaterializationJob,
    ) -> CentralResult<MaterializationJob> {
        job.validate().map_err(CentralError::from)?;
        let tenant_id = job.key.tenant_id.clone();
        let namespace = job.key.object_namespace_id.clone();
        let id = job.materialization_id.clone();
        let key = job.key.clone();
        let mut jobs = lock(&self.materializations)?;
        // Materialization IDs are scoped by tenant and object namespace. This mirrors the
        // authority primary key and prevents a caller-reused ID from crossing namespace fences.
        let id_key = (tenant_id.clone(), namespace, id.clone());
        if let Some(existing) = jobs.get(&id_key) {
            return if existing == &job {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization ID is already bound to different metadata",
                ))
            };
        }
        if let Some(existing) = lock(&self.materialization_keys)?.get(&key) {
            if existing == &job {
                return Ok(existing.clone());
            }
            return Err(invalid(
                if existing.state.terminal() {
                    CentralErrorCode::InvalidState
                } else {
                    CentralErrorCode::ReplicationAlreadyActive
                },
                "materialization idempotency key is already bound to different metadata",
            ));
        }
        // Coverage thresholds are request policy, not a second target identity.  A target must
        // have at most one recoverable Job for a Commit regardless of whether the caller asked
        // for Complete, ObjectCount, or ByteCount coverage.  Keep this check under the same
        // mutex as insertion so InMemory has the same CAS boundary as SQLite's partial index.
        if jobs.values().any(|existing| {
            !existing.state.terminal()
                && existing.key.tenant_id == job.key.tenant_id
                && existing.key.object_namespace_id == job.key.object_namespace_id
                && existing.key.commit_id == job.key.commit_id
                && existing.key.target_storage_volume_id == job.key.target_storage_volume_id
        }) {
            return Err(invalid(
                CentralErrorCode::ReplicationAlreadyActive,
                "a materialization for this Commit target is already active",
            ));
        }
        jobs.insert(id_key, job.clone());
        lock(&self.materialization_keys)?.insert(key, job.clone());
        Ok(job)
    }

    async fn insert_materialization_plan(
        &self,
        plan: MaterializationPlan,
    ) -> CentralResult<MaterializationPlanInsertOutcome> {
        // Keep plan publication and object-receipt publication on one serialization boundary.
        // Without this fence a planner could replace the parent after a receipt validated it but
        // before the receipt advanced its Object/Job rows, leaving an orphan Placement behind.
        let _gate = self.materialization_receipt_gate.lock().await;
        let MaterializationPlan {
            job,
            batches,
            objects,
            object_read_leases,
            staging_leases,
            coverage,
        } = plan;
        job.validate().map_err(CentralError::from)?;
        coverage.validate().map_err(CentralError::from)?;
        if coverage.tenant_id != job.key.tenant_id
            || coverage.object_namespace_id != job.key.object_namespace_id
            || coverage.commit_id != job.key.commit_id
            || coverage.storage_volume_id != job.key.target_storage_volume_id
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Coverage identity does not match its Job",
            ));
        }
        let object_set = self
            .get_commit_object_set(&job.key.tenant_id, &job.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        coverage
            .validate_against(&object_set.object_set)
            .map_err(CentralError::from)?;

        // Validate the complete aggregate before taking any mutable lock. This keeps all
        // rejection paths side-effect free and mirrors the SQLite transaction below.
        let expected_objects = object_set
            .object_set
            .objects
            .iter()
            .map(|object| (object.object_id, object))
            .collect::<BTreeMap<_, _>>();
        let health = lock(&self.placement_health)?.clone();
        let relevant_placements = lock(&self.materialization_placements)?
            .values()
            .filter(|placement| {
                let key = (
                    placement.tenant_id.clone(),
                    placement.object_namespace_id.clone(),
                    placement.placement_id.clone(),
                    placement.placement_generation,
                );
                !health.get(&key).is_some_and(|observation| {
                    matches!(
                        observation.state,
                        PlacementHealthState::Missing | PlacementHealthState::Corrupt
                    )
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut object_ids = BTreeSet::new();
        for object in &objects {
            object.validate().map_err(CentralError::from)?;
            if object.materialization_id != job.materialization_id
                || object.plan_revision != job.plan_revision
                || object.object.object_namespace_id != job.key.object_namespace_id
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization Object does not match its Job fence",
                ));
            }
            let Some(expected) = expected_objects.get(&object.object.object_id) else {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization Object is not part of the Commit ObjectSet",
                ));
            };
            if object.object.object_id != expected.object_id
                || object.object.size != expected.size
                || object.object.encoding != expected.encoding
                || object.object.ordinal != expected.ordinal
                || !object_ids.insert(object.object.object_id)
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization Object metadata or identity is duplicated",
                ));
            }
        }
        if object_ids.len() != expected_objects.len() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization plan must include every Commit Object",
            ));
        }

        let mut batch_ids = BTreeSet::new();
        let mut assigned_objects = BTreeMap::<ObjectId, MaterializationBatchId>::new();
        for batch in &batches {
            batch.validate().map_err(CentralError::from)?;
            if batch.materialization_id != job.materialization_id
                || batch.plan_revision != job.plan_revision
                || batch.target.tenant_id != job.key.tenant_id
                || batch.target.object_namespace_id != job.key.object_namespace_id
                || batch.target.storage_volume_id != job.key.target_storage_volume_id
                || !batch_ids.insert(batch.batch_id.clone())
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization Batch does not match its Job fence or is duplicated",
                ));
            }
            for object_id in &batch.object_ids {
                if !object_ids.contains(object_id)
                    || assigned_objects
                        .insert(*object_id, batch.batch_id.clone())
                        .is_some()
                {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "materialization Batch object list is invalid or overlaps another Batch",
                    ));
                }
            }
        }
        for object in &objects {
            if let Some(batch_id) = &object.current_batch_id {
                if assigned_objects.get(&object.object.object_id) != Some(batch_id) {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "materialization Object current Batch does not match its plan",
                    ));
                }
            }
        }

        let mut read_lease_ids = BTreeSet::new();
        for lease in &object_read_leases {
            lease
                .validate_for_acquisition()
                .map_err(CentralError::from)?;
            if lease.materialization_id != job.materialization_id
                || lease.plan_revision != job.plan_revision
                || lease.tenant_id != job.key.tenant_id
                || lease.object_namespace_id != job.key.object_namespace_id
                || !batch_ids.contains(&lease.batch_id)
                || !object_ids.contains(&lease.object_id)
                || !read_lease_ids.insert(lease.lease_id.clone())
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "object read lease does not match its materialization plan",
                ));
            }
            let batch = batches
                .iter()
                .find(|batch| batch.batch_id == lease.batch_id)
                .expect("batch ID was checked above");
            let Some(object) = objects.iter().find(|object| {
                object.object.object_id == lease.object_id
                    && object.current_batch_id.as_ref() == Some(&lease.batch_id)
            }) else {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "object read lease is not assigned to its Batch",
                ));
            };
            let source_selected = object.primary_source.as_ref() == Some(&lease.placement_id)
                || object.fallback_sources.contains(&lease.placement_id);
            if !batch.object_ids.contains(&lease.object_id) || !source_selected {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "object read lease placement is not selected for its object task",
                ));
            }
            let placement = relevant_placements.iter().find(|placement| {
                placement.tenant_id == lease.tenant_id
                    && placement.object_namespace_id == lease.object_namespace_id
                    && placement.object_id == lease.object_id
                    && placement.placement_id == lease.placement_id
                    && placement.placement_generation == lease.placement_generation
                    && placement.readable()
            });
            if placement.is_none_or(|placement| {
                placement.size != expected_objects[&lease.object_id].size
                    || placement.encoding != expected_objects[&lease.object_id].encoding
                    || placement.storage_volume_id.is_none()
                    || (object.primary_source.as_ref() == Some(&lease.placement_id)
                        && (placement.storage_volume_id != batch.source.storage_volume_id
                            || placement.archive_id != batch.source.archive_id
                            || placement.placement_generation != batch.source.placement_generation))
            }) {
                return Err(invalid(
                    CentralErrorCode::ResourceNotFound,
                    "object read lease placement is not a readable source Placement",
                ));
            }
        }
        let mut staging_lease_ids = BTreeSet::new();
        for lease in &staging_leases {
            lease
                .validate_for_acquisition()
                .map_err(CentralError::from)?;
            if lease.materialization_id != job.materialization_id
                || lease.plan_revision != job.plan_revision
                || lease.tenant_id != job.key.tenant_id
                || lease.object_namespace_id != job.key.object_namespace_id
                || lease.target_storage_volume_id != job.key.target_storage_volume_id
                || !object_ids.contains(&lease.object_id)
                || !staging_lease_ids.insert(lease.lease_id.clone())
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "staging lease does not match its materialization plan",
                ));
            }
            let object = objects
                .iter()
                .find(|object| object.object.object_id == lease.object_id)
                .expect("object ID was checked above");
            lease
                .validate_against_object(object)
                .map_err(CentralError::from)?;
        }
        for object in &objects {
            if object.complete() {
                continue;
            }
            let Some(batch_id) = &object.current_batch_id else {
                continue;
            };
            if !object_read_leases.iter().any(|lease| {
                lease.batch_id == *batch_id && lease.object_id == object.object.object_id
            }) || !staging_leases
                .iter()
                .any(|lease| lease.object_id == object.object.object_id)
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "every assigned materialization Object requires source and staging leases",
                ));
            }
        }

        // Lock order matches the coverage/receipt paths: placements first, then materialization
        // rows, and Coverage last. No lock is held across an await, so publication is atomic to
        // readers of the in-memory repository as well.
        let recomputed = VolumeCommitCoverage::from_placements(
            coverage.tenant_id.clone(),
            coverage.object_namespace_id.clone(),
            coverage.commit_id,
            coverage.storage_volume_id.clone(),
            coverage.placement_generation,
            &object_set.object_set,
            &relevant_placements,
        )
        .map_err(CentralError::from)?;
        if coverage.object_set_digest != recomputed.object_set_digest
            || coverage.object_count != recomputed.object_count
            || coverage.verified_object_count != recomputed.verified_object_count
            || coverage.total_bytes != recomputed.total_bytes
            || coverage.verified_bytes != recomputed.verified_bytes
            || (matches!(
                coverage.state,
                neoengram_domain::protocol::materialization::CoverageState::Partial
                    | neoengram_domain::protocol::materialization::CoverageState::Complete
            ) && coverage.state != recomputed.state)
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Coverage does not match Placement evidence",
            ));
        }

        let mut jobs = lock(&self.materializations)?;
        let mut keys = lock(&self.materialization_keys)?;
        let mut stored_batches = lock(&self.materialization_batches)?;
        let mut stored_objects = lock(&self.materialization_objects)?;
        let mut stored_read_leases = lock(&self.object_read_leases)?;
        let mut stored_staging_leases = lock(&self.staging_leases)?;
        let mut stored_coverages = lock(&self.volume_commit_coverages)?;
        let job_key = (
            job.key.tenant_id.clone(),
            job.key.object_namespace_id.clone(),
            job.materialization_id.clone(),
        );
        let existing_job = jobs.get(&job_key).cloned();
        if let Some(existing) = &existing_job {
            if existing != &job {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization ID is already bound to different metadata",
                ));
            }
        } else if let Some(existing) = keys.get(&job.key) {
            return Err(invalid(
                if existing.state.terminal() {
                    CentralErrorCode::InvalidState
                } else {
                    CentralErrorCode::ReplicationAlreadyActive
                },
                "materialization idempotency key is already bound to different metadata",
            ));
        } else if jobs.values().any(|existing| {
            !existing.state.terminal()
                && existing.key.tenant_id == job.key.tenant_id
                && existing.key.object_namespace_id == job.key.object_namespace_id
                && existing.key.commit_id == job.key.commit_id
                && existing.key.target_storage_volume_id == job.key.target_storage_volume_id
        }) {
            return Err(invalid(
                CentralErrorCode::ReplicationAlreadyActive,
                "a materialization for this Commit target is already active",
            ));
        }

        for batch in &batches {
            let key = (
                job.key.tenant_id.clone(),
                job.key.object_namespace_id.clone(),
                batch.batch_id.clone(),
            );
            if let Some(existing) = stored_batches.get(&key) {
                if existing != batch {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "materialization Batch ID is already bound to different metadata",
                    ));
                }
            }
        }
        for object in &objects {
            let key = (
                job.key.tenant_id.clone(),
                job.materialization_id.clone(),
                job.key.object_namespace_id.clone(),
                object.object.object_id,
            );
            if let Some(existing) = stored_objects.get(&key) {
                if existing != object {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "materialization Object ID is already bound to different metadata",
                    ));
                }
            }
        }
        for lease in &object_read_leases {
            let key = (
                job.key.tenant_id.clone(),
                job.key.object_namespace_id.clone(),
                lease.lease_id.clone(),
            );
            if let Some(existing) = stored_read_leases.get(&key) {
                if existing != lease {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "object read lease ID is already in use",
                    ));
                }
            }
        }
        for lease in &staging_leases {
            let key = (
                job.key.tenant_id.clone(),
                job.key.object_namespace_id.clone(),
                lease.lease_id.clone(),
            );
            if let Some(existing) = stored_staging_leases.get(&key) {
                if existing != lease {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "staging lease ID is already in use",
                    ));
                }
            }
        }
        let coverage_key = (
            coverage.tenant_id.clone(),
            coverage.object_namespace_id.clone(),
            coverage.commit_id,
            coverage.storage_volume_id.clone(),
            coverage.placement_generation,
        );
        if let Some(existing) = stored_coverages.get(&coverage_key) {
            if existing.object_set_digest != coverage.object_set_digest
                || existing.object_count != coverage.object_count
                || existing.total_bytes != coverage.total_bytes
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "coverage identity is already bound to different Commit metadata",
                ));
            }
        }

        let inserted = existing_job.is_none();
        if inserted {
            jobs.insert(job_key, job.clone());
            keys.insert(job.key.clone(), job.clone());
        }
        for batch in batches {
            stored_batches.insert(
                (
                    job.key.tenant_id.clone(),
                    job.key.object_namespace_id.clone(),
                    batch.batch_id.clone(),
                ),
                batch,
            );
        }
        for object in objects {
            stored_objects.insert(
                (
                    job.key.tenant_id.clone(),
                    job.materialization_id.clone(),
                    job.key.object_namespace_id.clone(),
                    object.object.object_id,
                ),
                object,
            );
        }
        for lease in object_read_leases {
            stored_read_leases.insert(
                (
                    job.key.tenant_id.clone(),
                    job.key.object_namespace_id.clone(),
                    lease.lease_id.clone(),
                ),
                lease,
            );
        }
        for lease in staging_leases {
            stored_staging_leases.insert(
                (
                    job.key.tenant_id.clone(),
                    job.key.object_namespace_id.clone(),
                    lease.lease_id.clone(),
                ),
                lease,
            );
        }
        stored_coverages.insert(coverage_key, coverage);
        Ok(if inserted {
            MaterializationPlanInsertOutcome::Inserted(job)
        } else {
            MaterializationPlanInsertOutcome::Existing(job)
        })
    }

    async fn replace_materialization_plan(
        &self,
        request: MaterializationPlanReplacement,
    ) -> CentralResult<MaterializationPlanInsertOutcome> {
        // See `insert_materialization_plan`: replacements retire the previous batch/lease rows and
        // must not interleave with a receipt's Placement + checkpoint publication.
        let _gate = self.materialization_receipt_gate.lock().await;
        let expected_revision = request.expected_plan_revision;
        let plan = request.plan;
        let next_revision = expected_revision
            .get()
            .checked_add(1)
            .map(Generation::new)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization plan revision is exhausted",
                )
            })?;
        if plan.job.plan_revision != next_revision {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replacement materialization plan must advance exactly one revision",
            ));
        }
        let tenant_id = plan.job.key.tenant_id.clone();
        let materialization_id = plan.job.materialization_id.clone();
        let namespace = plan.job.key.object_namespace_id.clone();
        let current = self
            .materialization_for_namespace(&tenant_id, &namespace, &materialization_id)?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if current.plan_revision != expected_revision {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision changed",
            ));
        }
        if current.key != plan.job.key || current.materialization_id != materialization_id {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "replacement materialization plan changes its immutable identity",
            ));
        }
        if !materialization_state_transition_allowed(current.state, plan.job.state) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization state transition is not allowed",
            ));
        }

        // Run the same complete aggregate validation used by initial creation against a detached
        // validator. This keeps all rejection paths side-effect free before touching live rows.
        let object_set = lock(&self.commit_object_sets)?
            .get(&(tenant_id.clone(), current.key.commit_id.digest()))
            .cloned()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        let validator = InMemoryPlacementRepository::default();
        validator.insert_commit_object_set(object_set).await?;
        let health = lock(&self.placement_health)?.clone();
        let placements = lock(&self.materialization_placements)?
            .values()
            .filter(|placement| {
                let key = (
                    placement.tenant_id.clone(),
                    placement.object_namespace_id.clone(),
                    placement.placement_id.clone(),
                    placement.placement_generation,
                );
                !health.get(&key).is_some_and(|observation| {
                    matches!(
                        observation.state,
                        PlacementHealthState::Missing | PlacementHealthState::Corrupt
                    )
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        for placement in placements {
            validator.insert_object_placement_v2(placement).await?;
        }
        validator.insert_materialization_plan(plan.clone()).await?;

        // Recheck the CAS fence while holding every mutable map lock, retire old protection
        // records, and publish the replacement aggregate as one in-memory visibility boundary.
        let mut jobs = lock(&self.materializations)?;
        let mut keys = lock(&self.materialization_keys)?;
        let mut batches = lock(&self.materialization_batches)?;
        let mut objects = lock(&self.materialization_objects)?;
        let mut read_leases = lock(&self.object_read_leases)?;
        let mut staging_leases = lock(&self.staging_leases)?;
        let mut coverages = lock(&self.volume_commit_coverages)?;
        let job_key = (
            tenant_id.clone(),
            namespace.clone(),
            materialization_id.clone(),
        );
        let persisted = jobs.get(&job_key).cloned().ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "materialization disappeared during replacement",
            )
        })?;
        if persisted.plan_revision != expected_revision {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision changed",
            ));
        }
        // Validate each replacement object against its previous revision before retiring any
        // active child rows. A completed object may be reset only for an integrity repair plan:
        // the parent must reopen through Planning, and the new task must start at byte zero.
        for object in &plan.objects {
            let key = (
                tenant_id.clone(),
                materialization_id.clone(),
                namespace.clone(),
                object.object.object_id,
            );
            let previous = objects.get(&key).ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization replacement is missing an existing Object row",
                )
            })?;
            if previous.plan_revision != expected_revision {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object belongs to a different plan revision",
                ));
            }
            if previous.object != object.object || previous.staging_key != object.staging_key {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization Object identity or staging key cannot change",
                ));
            }
            let integrity_repair_reset = persisted.state == MaterializationJobState::Complete
                && plan.job.state != MaterializationJobState::Complete
                && previous.complete()
                && object.confirmed_offset.get() == 0
                && matches!(
                    object.state,
                    MaterializationObjectState::Missing | MaterializationObjectState::Reserved
                );
            if object.confirmed_offset < previous.confirmed_offset && !integrity_repair_reset {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object confirmed offset cannot move backwards",
                ));
            }
            if previous.complete() && !object.complete() && !integrity_repair_reset {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "completed materialization Object cannot regress",
                ));
            }
            let expected_attempt = previous.attempt.get().checked_add(1).ok_or_else(|| {
                invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object attempt is exhausted",
                )
            })?;
            if object.attempt.get() != expected_attempt {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object attempt must advance exactly one step",
                ));
            }
        }
        // Object rows are keyed by stable materialization/object identity. Overwrite them below
        // so re-planning preserves the staging key and durable checkpoint selected by the plan.
        for ((tenant, ns, batch_id), batch) in batches.iter_mut() {
            if tenant == &tenant_id
                && ns == &namespace
                && batch.materialization_id == materialization_id
                && !matches!(
                    batch.state,
                    neoengram_domain::protocol::materialization::MaterializationBatchState::Succeeded
                        | neoengram_domain::protocol::materialization::MaterializationBatchState::Failed
                )
            {
                batch.state =
                    neoengram_domain::protocol::materialization::MaterializationBatchState::Failed;
                let _ = batch_id;
            }
        }
        for ((tenant, ns, _), lease) in read_leases.iter_mut() {
            if tenant == &tenant_id
                && ns == &namespace
                && lease.materialization_id == materialization_id
                && lease.state
                    == neoengram_domain::protocol::materialization::MaterializationLeaseState::Active
            {
                lease.state =
                    neoengram_domain::protocol::materialization::MaterializationLeaseState::Released;
            }
        }
        for ((tenant, ns, _), lease) in staging_leases.iter_mut() {
            if tenant == &tenant_id
                && ns == &namespace
                && lease.materialization_id == materialization_id
                && lease.state
                    == neoengram_domain::protocol::materialization::MaterializationLeaseState::Active
            {
                lease.state =
                    neoengram_domain::protocol::materialization::MaterializationLeaseState::Released;
            }
        }
        jobs.insert(job_key, plan.job.clone());
        keys.insert(plan.job.key.clone(), plan.job.clone());
        for batch in plan.batches {
            batches.insert(
                (tenant_id.clone(), namespace.clone(), batch.batch_id.clone()),
                batch,
            );
        }
        for object in plan.objects {
            objects.insert(
                (
                    tenant_id.clone(),
                    materialization_id.clone(),
                    namespace.clone(),
                    object.object.object_id,
                ),
                object,
            );
        }
        for lease in plan.object_read_leases {
            read_leases.insert(
                (tenant_id.clone(), namespace.clone(), lease.lease_id.clone()),
                lease,
            );
        }
        for lease in plan.staging_leases {
            staging_leases.insert(
                (tenant_id.clone(), namespace.clone(), lease.lease_id.clone()),
                lease,
            );
        }
        coverages.insert(
            (
                tenant_id,
                namespace,
                plan.coverage.commit_id,
                plan.coverage.storage_volume_id.clone(),
                plan.coverage.placement_generation,
            ),
            plan.coverage,
        );
        Ok(MaterializationPlanInsertOutcome::Inserted(plan.job))
    }

    async fn get_materialization(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Option<MaterializationJob>> {
        let jobs = lock(&self.materializations)?;
        Ok(jobs
            .get(&(
                tenant_id.clone(),
                object_namespace_id.clone(),
                materialization_id.clone(),
            ))
            .cloned())
    }

    async fn get_materialization_by_key(
        &self,
        key: &MaterializationJobKey,
    ) -> CentralResult<Option<MaterializationJob>> {
        Ok(lock(&self.materialization_keys)?.get(key).cloned())
    }

    async fn list_materializations(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        commit_id: &neoengram_domain::core::ContentDigest,
        target_storage_volume_id: Option<&StorageVolumeId>,
    ) -> CentralResult<Vec<MaterializationJob>> {
        Ok(lock(&self.materializations)?
            .values()
            .filter(|job| {
                &job.key.tenant_id == tenant_id
                    && &job.key.object_namespace_id == object_namespace_id
                    && job.key.commit_id.digest() == *commit_id
                    && target_storage_volume_id
                        .is_none_or(|target| &job.key.target_storage_volume_id == target)
            })
            .cloned()
            .collect())
    }

    async fn replace_materialization(
        &self,
        tenant_id: &TenantId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
        expected_plan_revision: neoengram_domain::protocol::Generation,
        job: MaterializationJob,
    ) -> CentralResult<MaterializationJob> {
        let _gate = self.materialization_receipt_gate.lock().await;
        self.replace_materialization_unlocked(
            tenant_id,
            materialization_id,
            expected_plan_revision,
            job,
        )
        .await
    }

    async fn insert_materialization_batch(
        &self,
        batch: MaterializationBatch,
    ) -> CentralResult<MaterializationBatch> {
        batch.validate().map_err(CentralError::from)?;
        let tenant_id = batch.target.tenant_id.clone();
        let parent = self
            .materialization_for_namespace(
                &tenant_id,
                &batch.target.object_namespace_id,
                &batch.materialization_id,
            )?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != batch.target.object_namespace_id
            || parent.key.target_storage_volume_id != batch.target.storage_volume_id
            || parent.plan_revision != batch.plan_revision
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "batch target does not match its materialization",
            ));
        }
        let key = (
            tenant_id,
            batch.target.object_namespace_id.clone(),
            batch.batch_id.clone(),
        );
        let mut values = lock(&self.materialization_batches)?;
        if let Some(existing) = values.get(&key) {
            return if existing == &batch {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization batch ID is already bound to different metadata",
                ))
            };
        }
        values.insert(key, batch.clone());
        Ok(batch)
    }

    async fn replace_materialization_batch(
        &self,
        request: MaterializationBatchCasRequest,
    ) -> CentralResult<MaterializationBatch> {
        request.batch.validate().map_err(CentralError::from)?;
        if request.batch.materialization_id != request.materialization_id
            || request.batch.batch_id != request.batch_id
            || request.batch.target.tenant_id != request.tenant_id
            || request.batch.target.object_namespace_id != request.object_namespace_id
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Batch replacement identity does not match its key",
            ));
        }
        let parent = self
            .materialization_for_namespace(
                &request.tenant_id,
                &request.object_namespace_id,
                &request.materialization_id,
            )?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != request.object_namespace_id
            || parent.key.target_storage_volume_id != request.batch.target.storage_volume_id
            || parent.plan_revision != request.expected_plan_revision
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Batch target does not match its parent",
            ));
        }
        let key = (
            request.tenant_id.clone(),
            request.object_namespace_id.clone(),
            request.batch_id.clone(),
        );
        let mut values = lock(&self.materialization_batches)?;
        let current = values.get(&key).cloned().ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "materialization Batch not found",
            )
        })?;
        if current.plan_revision != request.expected_plan_revision
            || current.batch_attempt != request.expected_batch_attempt
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Batch revision or attempt changed",
            ));
        }
        if request.batch.plan_revision != request.expected_plan_revision
            || request.batch.batch_attempt < request.expected_batch_attempt
            || request.batch.batch_attempt
                > Generation::new(request.expected_batch_attempt.get().saturating_add(1))
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Batch advanced by more than one attempt",
            ));
        }
        if !current.state.can_transition_to(request.batch.state) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Batch state transition is not allowed",
            ));
        }
        values.insert(key, request.batch.clone());
        Ok(request.batch)
    }

    async fn list_materialization_batches(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Vec<MaterializationBatch>> {
        let batches = lock(&self.materialization_batches)?
            .iter()
            .filter(|((tenant, namespace, _), batch)| {
                tenant == tenant_id
                    && namespace == object_namespace_id
                    && batch.materialization_id == *materialization_id
                    && batch.target.object_namespace_id == *object_namespace_id
            })
            .map(|(_, batch)| batch.clone())
            .collect::<Vec<_>>();
        Ok(batches)
    }

    async fn list_active_materialization_batches_for_agent(
        &self,
        tenant_id: &TenantId,
        agent_id: &AgentId,
    ) -> CentralResult<Vec<MaterializationBatch>> {
        let mut batches = lock(&self.materialization_batches)?
            .values()
            .filter(|batch| {
                batch.target.tenant_id == *tenant_id
                    && batch.target.agent_id == *agent_id
                    && matches!(
                        batch.state,
                        neoengram_domain::protocol::materialization::MaterializationBatchState::Queued
                            | neoengram_domain::protocol::materialization::MaterializationBatchState::Assigned
                            | neoengram_domain::protocol::materialization::MaterializationBatchState::Transferring
                            | neoengram_domain::protocol::materialization::MaterializationBatchState::Verifying
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        batches.sort_by_key(|batch| {
            (
                batch.materialization_id.clone(),
                batch.plan_revision,
                batch.batch_id.clone(),
            )
        });
        Ok(batches)
    }

    async fn insert_materialization_object(
        &self,
        tenant_id: &TenantId,
        object: MaterializationObject,
    ) -> CentralResult<MaterializationObject> {
        object.validate().map_err(CentralError::from)?;
        let parent = self
            .materialization_for_namespace(
                tenant_id,
                &object.object.object_namespace_id,
                &object.materialization_id,
            )?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != object.object.object_namespace_id {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization object namespace does not match its parent",
            ));
        }
        if parent.plan_revision != object.plan_revision {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization object plan revision does not match its parent",
            ));
        }
        let object_set = self
            .get_commit_object_set(tenant_id, &parent.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        let expected = object_set
            .object_set
            .objects
            .iter()
            .find(|candidate| candidate.object_id == object.object.object_id)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "materialization object is not part of the Commit ObjectSet",
                )
            })?;
        if expected.size != object.object.size
            || expected.encoding != object.object.encoding
            || expected.ordinal != object.object.ordinal
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization object metadata disagrees with the Commit ObjectSet",
            ));
        }
        let key = (
            tenant_id.clone(),
            object.materialization_id.clone(),
            object.object.object_namespace_id.clone(),
            object.object.object_id,
        );
        let mut values = lock(&self.materialization_objects)?;
        if let Some(existing) = values.get(&key) {
            if existing == &object {
                return Ok(existing.clone());
            }
            // Replanning advances the parent Job revision and rewrites the same durable object
            // row. The staging key and ObjectRef are immutable, while a checkpoint may only move
            // forward. This keeps a reconnect/failover from starting the object at offset zero.
            if object.plan_revision.get()
                != existing.plan_revision.get().checked_add(1).ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ConcurrentUpdate,
                        "materialization object plan revision is exhausted",
                    )
                })?
                || object.object != existing.object
                || object.staging_key != existing.staging_key
            {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization object identity or plan revision changed",
                ));
            }
            if object.confirmed_offset < existing.confirmed_offset {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization object confirmed offset cannot move backwards",
                ));
            }
            if existing.complete()
                && (!object.complete() || object.confirmed_offset != existing.confirmed_offset)
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "a completed materialization object cannot regress during replanning",
                ));
            }
            if object.attempt.get()
                != existing.attempt.get().checked_add(1).ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ConcurrentUpdate,
                        "materialization object attempt is exhausted",
                    )
                })?
            {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization object attempt must advance by one during replanning",
                ));
            }
            values.insert(key, object.clone());
            return Ok(object);
        }
        values.insert(key, object.clone());
        Ok(object)
    }

    async fn replace_materialization_object(
        &self,
        request: MaterializationObjectCasRequest,
    ) -> CentralResult<MaterializationObject> {
        request.object.validate().map_err(CentralError::from)?;
        if request.object.materialization_id != request.materialization_id
            || request.object.object.object_namespace_id != request.object_namespace_id
            || request.object.object.object_id != request.object_id
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Object replacement identity does not match its key",
            ));
        }
        let parent = self
            .materialization_for_namespace(
                &request.tenant_id,
                &request.object_namespace_id,
                &request.materialization_id,
            )?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != request.object_namespace_id
            || parent.plan_revision != request.expected_plan_revision
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Object parent revision does not match",
            ));
        }
        let key = (
            request.tenant_id.clone(),
            request.materialization_id.clone(),
            request.object_namespace_id.clone(),
            request.object_id,
        );
        let mut values = lock(&self.materialization_objects)?;
        let current = values.get(&key).cloned().ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "materialization Object not found",
            )
        })?;
        if current.plan_revision != request.expected_plan_revision
            || current.attempt != request.expected_attempt
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object revision or attempt changed",
            ));
        }
        if current.object != request.object.object
            || current.staging_key != request.object.staging_key
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Object immutable metadata cannot change",
            ));
        }
        if request.object.confirmed_offset < current.confirmed_offset {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object confirmed offset cannot move backwards",
            ));
        }
        if request.object.plan_revision != request.expected_plan_revision
            || request.object.attempt < request.expected_attempt
            || request.object.attempt
                > Generation::new(request.expected_attempt.get().saturating_add(1))
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object advanced by more than one attempt",
            ));
        }
        if !current.state.can_transition_to(request.object.state) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Object state transition is not allowed",
            ));
        }
        values.insert(key, request.object.clone());
        Ok(request.object)
    }

    async fn list_materialization_objects(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Vec<MaterializationObject>> {
        let objects = lock(&self.materialization_objects)?
            .iter()
            .filter(|((tenant, materialization, namespace, _), _)| {
                tenant == tenant_id
                    && materialization == materialization_id
                    && namespace == object_namespace_id
            })
            .map(|(_, object)| object.clone())
            .collect::<Vec<_>>();
        Ok(objects)
    }

    async fn get_materialization_receipt(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        receipt_id: &neoengram_domain::protocol::ObjectReceiptId,
    ) -> CentralResult<Option<MaterializationObjectReceipt>> {
        Ok(lock(&self.materialization_receipts)?
            .get(&(
                tenant_id.clone(),
                object_namespace_id.clone(),
                receipt_id.clone(),
            ))
            .map(|(receipt, _)| receipt.clone()))
    }

    async fn record_materialization_receipt(
        &self,
        request: MaterializationReceiptRequest,
    ) -> CentralResult<ObjectPlacementV2> {
        let receipt = request.receipt;
        receipt
            .validate_against(&request.object)
            .map_err(CentralError::from)?;

        // A receipt publication updates several authority records.  Serialize that boundary so
        // two concurrent reports cannot both advance the same object or Job from one checkpoint.
        let _gate = self.materialization_receipt_gate.lock().await;
        let receipt_key = (
            receipt.tenant_id.clone(),
            receipt.object_namespace_id.clone(),
            receipt.receipt_id.clone(),
        );
        let existing_replay = {
            let receipts = lock(&self.materialization_receipts)?;
            receipts.get(&receipt_key).cloned()
        };
        if let Some((existing, placement)) = existing_replay {
            if existing != receipt {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "materialization receipt ID is already in use",
                ));
            }
            // A previous publication may have committed before its lease cleanup completed.
            // Replays are therefore also a repair point for source and staging leases.
            let batch = self
                .list_materialization_batches(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &receipt.materialization_id,
                )
                .await?
                .into_iter()
                .find(|batch| batch.batch_id == receipt.batch_id)
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ResourceNotFound,
                        "materialization batch not found during receipt replay",
                    )
                })?;
            let task = self
                .list_materialization_objects(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &receipt.materialization_id,
                )
                .await?
                .into_iter()
                .find(|task| {
                    task.object.object_namespace_id == receipt.object_namespace_id
                        && task.object.object_id == receipt.object_id
                })
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ResourceNotFound,
                        "materialization object not found during receipt replay",
                    )
                })?;
            let job = self
                .materialization_for_namespace(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &receipt.materialization_id,
                )?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ResourceNotFound,
                        "materialization not found during receipt replay",
                    )
                })?;
            // A prior process may have committed the receipt and Placement but been interrupted
            // before the derived Coverage write. Replays are a repair point: recompute Coverage
            // solely from the durable Placement evidence before releasing the remaining leases.
            let object_set = self
                .get_commit_object_set(&receipt.tenant_id, &job.key.commit_id.digest())
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ResourceNotFound,
                        "materialization Commit ObjectSet not found during receipt replay",
                    )
                })?;
            let mut placements = Vec::new();
            for object in &object_set.object_set.objects {
                placements.extend(
                    self.object_placements_v2(
                        &receipt.tenant_id,
                        &receipt.object_namespace_id,
                        &object.object_id,
                    )
                    .await?
                    .into_iter()
                    .filter(|candidate| {
                        candidate.storage_volume_id.as_ref()
                            == Some(&receipt.target_storage_volume_id)
                            && candidate.placement_generation == receipt.target_placement_generation
                    }),
                );
            }
            let coverage = VolumeCommitCoverage::from_placements(
                receipt.tenant_id.clone(),
                receipt.object_namespace_id.clone(),
                job.key.commit_id,
                receipt.target_storage_volume_id.clone(),
                receipt.target_placement_generation,
                &object_set.object_set,
                &placements,
            )
            .map_err(CentralError::from)?;
            self.upsert_volume_commit_coverage(coverage).await?;
            self.release_receipt_leases(&receipt, &batch, &task).await?;
            return Ok(placement.clone());
        }
        let conflicting_receipt = {
            let receipts = lock(&self.materialization_receipts)?;
            receipts
                .values()
                .find(|(existing, _)| {
                    existing.tenant_id == receipt.tenant_id
                        && existing.object_namespace_id == receipt.object_namespace_id
                        && existing.materialization_id == receipt.materialization_id
                        && existing.batch_id == receipt.batch_id
                        && existing.object_id == receipt.object_id
                })
                .map(|(existing, _)| existing.clone())
        };
        if let Some(existing) = conflicting_receipt {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!(
                    "materialization object already has receipt {} for this Batch",
                    existing.receipt_id
                ),
            ));
        }

        let job = self
            .materialization_for_namespace(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if job.plan_revision != receipt.plan_revision
            || job.key.object_namespace_id != receipt.object_namespace_id
            || job.key.target_storage_volume_id != receipt.target_storage_volume_id
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt is stale or scoped to another target",
            ));
        }
        let batch = self
            .list_materialization_batches(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .into_iter()
            .find(|batch| batch.batch_id == receipt.batch_id)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization batch not found",
                )
            })?;
        if batch.plan_revision != receipt.plan_revision
            || batch.batch_attempt != receipt.batch_attempt
            || batch.target.tenant_id != receipt.tenant_id
            || batch.target.object_namespace_id != receipt.object_namespace_id
            || batch.target.storage_volume_id != receipt.target_storage_volume_id
            || batch.target.placement_generation != receipt.target_placement_generation
            || !batch.object_ids.contains(&receipt.object_id)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt does not match the active batch fence",
            ));
        }
        if receipt.verified_at_unix_ms.get() >= batch.deadline_unix_ms.get() {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "receipt verification must occur before the materialization batch deadline",
            ));
        }
        let object_set = self
            .get_commit_object_set(&receipt.tenant_id, &job.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization Commit ObjectSet not found",
                )
            })?;
        let expected = object_set
            .object_set
            .objects
            .iter()
            .find(|object| object.object_id == receipt.object_id)
            .copied()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "receipt object is not part of the Commit ObjectSet",
                )
            })?;
        if expected.object_id != request.object.object_id
            || expected.size != request.object.size
            || expected.encoding != request.object.encoding
            || expected.ordinal != request.object.ordinal
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "receipt ObjectRef disagrees with the Commit ObjectSet",
            ));
        }
        let current = self
            .list_materialization_objects(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .into_iter()
            .find(|task| {
                task.object.object_namespace_id == receipt.object_namespace_id
                    && task.object.object_id == receipt.object_id
            })
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization object not found",
                )
            })?;
        if current.plan_revision != receipt.plan_revision {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization object belongs to an obsolete batch or plan",
            ));
        }
        if current.current_batch_id.as_ref() != Some(&receipt.batch_id) {
            // Two source-grouped Batches may race for one object after a retry or scheduler
            // replay. Once the target has a complete, matching Placement for this exact plan and
            // attempt, the losing receipt is safe to converge on that durable copy. Older
            // attempts still fail closed, even if their bytes happen to match.
            let target_has_evidence = self
                .object_placements_v2(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &receipt.object_id,
                )
                .await?
                .into_iter()
                .any(|candidate| {
                    candidate.readable()
                        && candidate.storage_volume_id.as_ref()
                            == Some(&receipt.target_storage_volume_id)
                        && candidate.placement_generation == receipt.target_placement_generation
                        && candidate.matches_ref(&request.object)
                });
            if !target_has_evidence || receipt.batch_attempt != current.attempt {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization object belongs to an obsolete batch or attempt",
                ));
            }
        }
        if current.object != request.object {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization object metadata disagrees with the receipt ObjectRef",
            ));
        }
        if !current.complete()
            && !current.state.can_transition_to(
                neoengram_domain::protocol::materialization::MaterializationObjectState::Verified,
            )
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "materialization Object cannot be verified from its current state",
            ));
        }
        if receipt.batch_attempt < current.attempt
            || receipt.batch_attempt > Generation::new(current.attempt.get().saturating_add(1))
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt attempt is stale or skipped",
            ));
        }

        let receipt_placement_id =
            crate::placement_authority::materialization_target_placement_id(&receipt)?;
        let placement = ObjectPlacementV2 {
            placement_id: receipt_placement_id.clone(),
            tenant_id: receipt.tenant_id.clone(),
            object_namespace_id: receipt.object_namespace_id.clone(),
            object_id: receipt.object_id,
            size: receipt.size,
            encoding: receipt.encoding,
            verified_digest: receipt.verified_digest,
            storage_volume_id: Some(receipt.target_storage_volume_id.clone()),
            archive_id: None,
            placement_generation: receipt.target_placement_generation,
            state: neoengram_domain::protocol::materialization::ObjectPlacementState::Verified,
            failure_domain: format!("volume:{}", receipt.target_storage_volume_id),
        };

        // If a process was interrupted after placement insertion, a completed task is a replay and
        // must not be forced through the one-way Published -> Verified transition.
        let existing_target = self
            .object_placements_v2(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.object_id,
            )
            .await?
            .into_iter()
            .find(|candidate| {
                candidate.storage_volume_id.as_ref() == Some(&receipt.target_storage_volume_id)
                    && candidate.placement_generation == receipt.target_placement_generation
                    && candidate.size == receipt.size
                    && candidate.encoding == receipt.encoding
                    && candidate.verified_digest == receipt.verified_digest
                    && candidate.readable()
            });
        if current.complete() {
            let stored = existing_target.ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "completed materialization object has no matching placement",
                )
            })?;
            // Placement publication may have succeeded immediately before a process crash. A
            // retry that observes the completed task must repair the derived Coverage just like
            // the normal publication and replay paths do.
            let mut placements = Vec::new();
            for object in &object_set.object_set.objects {
                placements.extend(
                    self.object_placements_v2(
                        &receipt.tenant_id,
                        &receipt.object_namespace_id,
                        &object.object_id,
                    )
                    .await?
                    .into_iter()
                    .filter(|candidate| {
                        candidate.storage_volume_id.as_ref()
                            == Some(&receipt.target_storage_volume_id)
                            && candidate.placement_generation == receipt.target_placement_generation
                    }),
                );
            }
            let coverage = VolumeCommitCoverage::from_placements(
                receipt.tenant_id.clone(),
                receipt.object_namespace_id.clone(),
                job.key.commit_id,
                receipt.target_storage_volume_id.clone(),
                receipt.target_placement_generation,
                &object_set.object_set,
                &placements,
            )
            .map_err(CentralError::from)?;
            self.upsert_volume_commit_coverage(coverage).await?;
            self.release_receipt_leases(&receipt, &batch, &current)
                .await?;
            lock(&self.materialization_receipts)?.insert(receipt_key, (receipt, stored.clone()));
            return Ok(stored);
        }

        let stored_placement = self.insert_object_placement_v2(placement).await?;
        let mut next_object = current.clone();
        next_object.confirmed_offset = DecimalU64::new(
            current
                .confirmed_offset
                .get()
                .max(receipt.committed_offset.get()),
        );
        next_object.state =
            neoengram_domain::protocol::materialization::MaterializationObjectState::Verified;
        next_object.attempt = current.attempt.max(receipt.batch_attempt);
        self.replace_materialization_object(MaterializationObjectCasRequest {
            tenant_id: receipt.tenant_id.clone(),
            object_namespace_id: receipt.object_namespace_id.clone(),
            materialization_id: receipt.materialization_id.clone(),
            object_id: receipt.object_id,
            expected_plan_revision: receipt.plan_revision,
            expected_attempt: current.attempt,
            object: next_object,
        })
        .await?;

        let tasks = self
            .list_materialization_objects(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?;
        let verified_objects = tasks.iter().filter(|task| task.complete()).count() as u64;
        let verified_bytes = tasks
            .iter()
            .filter(|task| task.complete())
            .map(|task| task.object.size.get())
            .sum::<u64>();
        let mut next_job = job.clone();
        next_job.verified_object_count = DecimalU64::new(verified_objects);
        next_job.verified_bytes = DecimalU64::new(verified_bytes);
        next_job.missing_object_count =
            DecimalU64::new(job.object_count.get().saturating_sub(verified_objects));
        next_job.missing_bytes =
            DecimalU64::new(job.total_bytes.get().saturating_sub(verified_bytes));
        next_job.state = if next_job.key.coverage_goal.satisfied_by(
            next_job.verified_object_count.get(),
            next_job.verified_bytes.get(),
            next_job.object_count.get(),
            next_job.total_bytes.get(),
        ) {
            neoengram_domain::protocol::materialization::MaterializationJobState::Complete
        } else {
            match job.state {
                neoengram_domain::protocol::materialization::MaterializationJobState::Queued
                | neoengram_domain::protocol::materialization::MaterializationJobState::Planning
                | neoengram_domain::protocol::materialization::MaterializationJobState::WaitingForSources
                | neoengram_domain::protocol::materialization::MaterializationJobState::Materializing
                | neoengram_domain::protocol::materialization::MaterializationJobState::Verifying =>
                    neoengram_domain::protocol::materialization::MaterializationJobState::Verifying,
                state => state,
            }
        };
        next_job.updated_at_unix_ms = UnixMillis::new(
            job.updated_at_unix_ms
                .get()
                .max(receipt.verified_at_unix_ms.get()),
        );
        self.replace_materialization_unlocked(
            &receipt.tenant_id,
            &receipt.materialization_id,
            receipt.plan_revision,
            next_job,
        )
        .await?;

        let mut placements = Vec::new();
        for object in &object_set.object_set.objects {
            placements.extend(
                self.object_placements_v2(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &object.object_id,
                )
                .await?
                .into_iter()
                .filter(|candidate| {
                    candidate.storage_volume_id.as_ref() == Some(&receipt.target_storage_volume_id)
                        && candidate.placement_generation == receipt.target_placement_generation
                }),
            );
        }
        let coverage = VolumeCommitCoverage::from_placements(
            receipt.tenant_id.clone(),
            receipt.object_namespace_id.clone(),
            job.key.commit_id,
            receipt.target_storage_volume_id.clone(),
            receipt.target_placement_generation,
            &object_set.object_set,
            &placements,
        )
        .map_err(CentralError::from)?;
        self.upsert_volume_commit_coverage(coverage).await?;
        self.release_receipt_leases(&receipt, &batch, &current)
            .await?;
        lock(&self.materialization_receipts)?
            .insert(receipt_key, (receipt, stored_placement.clone()));
        Ok(stored_placement)
    }

    async fn insert_object_read_lease(
        &self,
        lease: ObjectReadLease,
    ) -> CentralResult<ObjectReadLease> {
        lease
            .validate_for_acquisition()
            .map_err(CentralError::from)?;
        let parent = self
            .materialization_for_namespace(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != lease.object_namespace_id {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "read lease namespace mismatch",
            ));
        }
        if parent.plan_revision != lease.plan_revision {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "read lease plan revision does not match its parent",
            ));
        }
        let batches = self
            .list_materialization_batches(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )
            .await?;
        let batch = batches
            .iter()
            .find(|batch| {
                batch.batch_id == lease.batch_id
                    && batch.plan_revision == lease.plan_revision
                    && batch.target.object_namespace_id == lease.object_namespace_id
                    && batch.target.storage_volume_id == parent.key.target_storage_volume_id
            })
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "read lease batch is not registered for this materialization",
                )
            })?;
        if !batch.object_ids.contains(&lease.object_id) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "read lease object is not included in its batch manifest",
            ));
        }
        let objects = self
            .list_materialization_objects(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )
            .await?;
        let Some(object) = objects.iter().find(|object| {
            object.object.object_id == lease.object_id
                && object.current_batch_id.as_ref() == Some(&lease.batch_id)
        }) else {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "object read lease is not assigned to its Batch",
            ));
        };
        let source_selected = batch.source.placement_id == lease.placement_id
            || object.fallback_sources.contains(&lease.placement_id)
            || object.primary_source.as_ref() == Some(&lease.placement_id);
        if !source_selected {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "object read lease placement is not selected for its object task",
            ));
        }
        if object.primary_source.as_ref() == Some(&lease.placement_id)
            && batch.source.placement_generation != lease.placement_generation
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "read lease source generation differs from its Batch fence",
            ));
        }
        let object_set = self
            .get_commit_object_set(&parent.key.tenant_id, &parent.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        let expected = object_set
            .object_set
            .objects
            .iter()
            .find(|candidate| candidate.object_id == lease.object_id)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "read lease object is not part of the Commit ObjectSet",
                )
            })?;
        if expected.object_id != lease.object_id {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "read lease object identity mismatch",
            ));
        }
        let placements = lock(&self.materialization_placements)?;
        let placement = placements.values().find(|placement| {
            placement.tenant_id == lease.tenant_id
                && placement.object_namespace_id == lease.object_namespace_id
                && placement.object_id == lease.object_id
                && placement.placement_id == lease.placement_id
                && placement.placement_generation == lease.placement_generation
                && placement.readable()
        });
        if placement.is_none_or(|placement| {
            placement.size.get() != expected.size.get() || placement.encoding != expected.encoding
        }) {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "read lease placement is not a readable v2 placement",
            ));
        }
        if placement.is_none_or(|value| value.storage_volume_id.is_none()) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "read lease placement must reference a StorageVolume",
            ));
        }
        if placement.is_none_or(|value| {
            object.primary_source.as_ref() == Some(&lease.placement_id)
                && (value.storage_volume_id != batch.source.storage_volume_id
                    || value.archive_id != batch.source.archive_id)
        }) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "read lease placement Volume does not match its Batch source fence",
            ));
        }
        let key = (
            lease.tenant_id.clone(),
            lease.object_namespace_id.clone(),
            lease.lease_id.clone(),
        );
        let mut values = lock(&self.object_read_leases)?;
        if let Some(existing) = values.get(&key) {
            return if existing == &lease {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "read lease ID is already in use",
                ))
            };
        }
        values.insert(key, lease.clone());
        Ok(lease)
    }

    async fn insert_staging_lease(&self, lease: StagingLease) -> CentralResult<StagingLease> {
        lease
            .validate_for_acquisition()
            .map_err(CentralError::from)?;
        let parent = self
            .materialization_for_namespace(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != lease.object_namespace_id
            || parent.key.target_storage_volume_id != lease.target_storage_volume_id
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "staging lease target mismatch",
            ));
        }
        let objects = self
            .list_materialization_objects(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )
            .await?;
        let object = objects
            .iter()
            .find(|object| {
                object.object.object_namespace_id == lease.object_namespace_id
                    && object.object.object_id == lease.object_id
            })
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "staging lease object is not registered",
                )
            })?;
        if object.staging_key != lease.staging_key || object.plan_revision != lease.plan_revision {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "staging lease does not match materialization object",
            ));
        }
        let batch_id = object.current_batch_id.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "staging lease object has no current batch",
            )
        })?;
        let batch = self
            .list_materialization_batches(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )
            .await?
            .into_iter()
            .find(|batch| {
                batch.batch_id == *batch_id
                    && batch.plan_revision == lease.plan_revision
                    && batch.target.object_namespace_id == lease.object_namespace_id
                    && batch.target.storage_volume_id == lease.target_storage_volume_id
            })
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "staging lease object current batch is not registered",
                )
            })?;
        if !batch.object_ids.contains(&lease.object_id) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "staging lease object is not included in its batch manifest",
            ));
        }
        if batch.target.placement_generation != lease.target_placement_generation {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "staging lease target generation does not match its batch target fence",
            ));
        }
        let key = (
            lease.tenant_id.clone(),
            lease.object_namespace_id.clone(),
            lease.lease_id.clone(),
        );
        let mut values = lock(&self.staging_leases)?;
        if let Some(existing) = values.get(&key) {
            return if existing == &lease {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "staging lease ID is already in use",
                ))
            };
        }
        values.insert(key, lease.clone());
        Ok(lease)
    }

    async fn reconcile_materialization_leases(
        &self,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<crate::MaterializationLeaseExpiryReconciliation> {
        let mut result = crate::MaterializationLeaseExpiryReconciliation::default();
        {
            let mut values = lock(&self.object_read_leases)?;
            for lease in values.values_mut() {
                if lease.state == MaterializationLeaseState::Active
                    && lease.expires_at_unix_ms.get() <= now_unix_ms.get()
                {
                    lease.state = MaterializationLeaseState::Expired;
                    result.expired_object_read_leases += 1;
                }
            }
        }
        {
            let mut values = lock(&self.staging_leases)?;
            for lease in values.values_mut() {
                if lease.state == MaterializationLeaseState::Active
                    && lease.expires_at_unix_ms.get() <= now_unix_ms.get()
                {
                    lease.state = MaterializationLeaseState::Expired;
                    result.expired_staging_leases += 1;
                }
            }
        }
        Ok(result)
    }

    async fn release_object_read_lease(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        lease_id: &neoengram_domain::protocol::LeaseId,
    ) -> CentralResult<Option<ObjectReadLease>> {
        let mut values = lock(&self.object_read_leases)?;
        let Some((key, mut lease)) = values
            .iter()
            .find(|((tenant, namespace, id), _)| {
                tenant == tenant_id && namespace == object_namespace_id && id == lease_id
            })
            .map(|(key, lease)| (key.clone(), lease.clone()))
        else {
            return Ok(None);
        };
        if lease.state == MaterializationLeaseState::Active {
            lease.state = MaterializationLeaseState::Released;
        }
        values.insert(key, lease.clone());
        Ok(Some(lease))
    }

    async fn release_staging_lease(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        lease_id: &neoengram_domain::protocol::LeaseId,
    ) -> CentralResult<Option<StagingLease>> {
        let mut values = lock(&self.staging_leases)?;
        let Some((key, mut lease)) = values
            .iter()
            .find(|((tenant, namespace, id), _)| {
                tenant == tenant_id && namespace == object_namespace_id && id == lease_id
            })
            .map(|(key, lease)| (key.clone(), lease.clone()))
        else {
            return Ok(None);
        };
        if lease.state == MaterializationLeaseState::Active {
            lease.state = MaterializationLeaseState::Released;
        }
        values.insert(key, lease.clone());
        Ok(Some(lease))
    }

    async fn get_commit_object_set(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Option<neoengram_domain::protocol::CommitObjectSet>> {
        Ok(lock(&self.commit_object_sets)?
            .get(&(tenant_id.clone(), *commit_id))
            .cloned())
    }

    async fn insert_commit_object_set(
        &self,
        object_set: neoengram_domain::protocol::CommitObjectSet,
    ) -> CentralResult<neoengram_domain::protocol::CommitObjectSet> {
        object_set.validate().map_err(CentralError::from)?;
        let key = (object_set.tenant_id.clone(), object_set.commit_id.into());
        let mut values = lock(&self.commit_object_sets)?;
        if let Some(existing) = values.get(&key) {
            return if existing == &object_set {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Commit object set is already bound to another payload",
                ))
            };
        }
        values.insert(key, object_set.clone());
        Ok(object_set)
    }

    async fn get_placement_set(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
        backend_id: &neoengram_domain::protocol::BackendId,
    ) -> CentralResult<Option<neoengram_domain::protocol::CommitPlacementSet>> {
        Ok(lock(&self.placement_sets)?
            .get(&(tenant_id.clone(), *commit_id, backend_id.clone()))
            .cloned())
    }

    async fn published_placement_sets(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<neoengram_domain::protocol::CommitPlacementSet>> {
        Ok(lock(&self.placement_sets)?
            .iter()
            .filter(|((tenant, digest, _), set)| {
                tenant == tenant_id
                    && digest == commit_id
                    && set.state == neoengram_domain::protocol::CommitPlacementSetState::Published
            })
            .map(|(_, set)| set.clone())
            .collect())
    }

    async fn commit_placement_sets(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<neoengram_domain::protocol::CommitPlacementSet>> {
        Ok(lock(&self.placement_sets)?
            .iter()
            .filter(|((tenant, digest, _), _)| tenant == tenant_id && digest == commit_id)
            .map(|(_, set)| set.clone())
            .collect())
    }

    async fn insert_placement_set(
        &self,
        placement_set: neoengram_domain::protocol::CommitPlacementSet,
    ) -> CentralResult<neoengram_domain::protocol::CommitPlacementSet> {
        placement_set.validate().map_err(CentralError::from)?;
        let key = (
            placement_set.tenant_id.clone(),
            placement_set.commit_id.into(),
            placement_set.backend_id.clone(),
        );
        let mut values = lock(&self.placement_sets)?;
        if let Some(existing) = values.get(&key) {
            return if existing == &placement_set {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Commit placement set is already bound to another payload",
                ))
            };
        }
        values.insert(key, placement_set.clone());
        Ok(placement_set)
    }

    async fn publish_initial_placement(
        &self,
        object_set: neoengram_domain::protocol::CommitObjectSet,
        placements: Vec<neoengram_domain::protocol::ObjectPlacement>,
        placement_set: neoengram_domain::protocol::CommitPlacementSet,
    ) -> CentralResult<(
        neoengram_domain::protocol::CommitObjectSet,
        neoengram_domain::protocol::CommitPlacementSet,
    )> {
        object_set.validate().map_err(CentralError::from)?;
        placement_set.validate().map_err(CentralError::from)?;
        if !placement_set.published()
            || object_set.tenant_id != placement_set.tenant_id
            || object_set.commit_id != placement_set.commit_id
            || object_set.object_set.object_set_digest != placement_set.object_set_digest
            || object_set.object_set.object_count() as u64 != placement_set.object_count.get()
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "initial PlacementSet does not match the Commit ObjectSet",
            ));
        }
        let by_object = placements
            .iter()
            .map(|placement| (placement.object_id, placement))
            .collect::<BTreeMap<_, _>>();
        if by_object.len() != placements.len()
            || by_object.len() != object_set.object_set.object_count()
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "initial PlacementSet must contain exactly one copy of every Commit object",
            ));
        }
        for object in &object_set.object_set.objects {
            let placement = by_object.get(&object.object_id).ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "initial PlacementSet is missing a Commit object",
                )
            })?;
            placement.validate().map_err(CentralError::from)?;
            if placement.tenant_id != object_set.tenant_id
                || placement.backend_id != placement_set.backend_id
                || placement.storage_volume_id != placement_set.storage_volume_id
                || placement.archive_id != placement_set.archive_id
                || placement.placement_generation != placement_set.placement_generation
                || placement.state != neoengram_domain::protocol::PlacementState::Verified
                || placement.verified_size != object.size
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "initial object Placement does not match its publication fence",
                ));
            }
        }

        // Hold all three maps while validating and applying the batch. The in-memory adapter is
        // used by service tests, so this mirrors SQLite's all-or-nothing publication boundary.
        let commit_key = (object_set.tenant_id.clone(), object_set.commit_id.into());
        let placement_key = (
            placement_set.tenant_id.clone(),
            placement_set.commit_id.into(),
            placement_set.backend_id.clone(),
        );
        let mut commit_sets = lock(&self.commit_object_sets)?;
        let mut placement_sets = lock(&self.placement_sets)?;
        let mut object_placements = lock(&self.object_placements)?;
        if let Some(existing) = placement_sets.values().find(|existing| {
            existing.tenant_id == object_set.tenant_id
                && existing.commit_id == object_set.commit_id
                && existing.state == neoengram_domain::protocol::CommitPlacementSetState::Published
        }) {
            let stored_object_set = commit_sets.get(&commit_key).ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    "published PlacementSet is missing its Commit ObjectSet",
                )
            })?;
            if stored_object_set != &object_set {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Commit object set is already bound to another payload",
                ));
            }
            return Ok((stored_object_set.clone(), existing.clone()));
        }
        if let Some(existing) = commit_sets.get(&commit_key) {
            if existing != &object_set {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Commit object set is already bound to another payload",
                ));
            }
        }
        if let Some(existing) = placement_sets.get(&placement_key) {
            if existing != &placement_set {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Commit placement set is already bound to another payload",
                ));
            }
        }
        for placement in &placements {
            let entries =
                object_placements.get(&(placement.tenant_id.clone(), placement.object_id));
            if let Some(existing) = entries.and_then(|entries| {
                entries.iter().find(|existing| {
                    existing.backend_id == placement.backend_id
                        && existing.placement_generation == placement.placement_generation
                })
            }) {
                if existing != placement {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "Object placement is already bound to another payload",
                    ));
                }
            }
        }
        commit_sets.insert(commit_key, object_set.clone());
        placement_sets.insert(placement_key, placement_set.clone());
        for placement in placements {
            object_placements
                .entry((placement.tenant_id.clone(), placement.object_id))
                .or_default()
                .push(placement);
        }
        Ok((object_set, placement_set))
    }

    async fn insert_object_placement(
        &self,
        placement: neoengram_domain::protocol::ObjectPlacement,
    ) -> CentralResult<neoengram_domain::protocol::ObjectPlacement> {
        placement.validate().map_err(CentralError::from)?;
        let key = (placement.tenant_id.clone(), placement.object_id);
        let mut values = lock(&self.object_placements)?;
        let entries = values.entry(key).or_default();
        if let Some(existing) = entries.iter().find(|existing| {
            existing.backend_id == placement.backend_id
                && existing.placement_generation == placement.placement_generation
        }) {
            return if existing == &placement {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Object placement is already bound to another payload",
                ))
            };
        }
        entries.push(placement.clone());
        Ok(placement)
    }

    async fn set_object_placement_state(
        &self,
        tenant_id: &TenantId,
        object_id: &neoengram_domain::core::ObjectId,
        backend_id: &neoengram_domain::protocol::BackendId,
        placement_generation: neoengram_domain::protocol::PlacementGeneration,
        state: neoengram_domain::protocol::PlacementState,
    ) -> CentralResult<neoengram_domain::protocol::ObjectPlacement> {
        let key = (tenant_id.clone(), *object_id);
        let mut values = lock(&self.object_placements)?;
        let entries = values.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "object placement does not exist",
            )
        })?;
        let placement = entries
            .iter_mut()
            .find(|placement| {
                placement.backend_id == *backend_id
                    && placement.placement_generation == placement_generation
            })
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "object placement does not exist",
                )
            })?;
        if placement.state == state {
            return Ok(placement.clone());
        }
        if matches!(
            placement.state,
            neoengram_domain::protocol::PlacementState::Deleted
        ) && !matches!(state, neoengram_domain::protocol::PlacementState::Deleted)
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "deleted object placement cannot be revived",
            ));
        }
        placement.state = state;
        Ok(placement.clone())
    }

    async fn object_placements(
        &self,
        tenant_id: &TenantId,
        object_id: &neoengram_domain::core::ObjectId,
    ) -> CentralResult<Vec<neoengram_domain::protocol::ObjectPlacement>> {
        Ok(lock(&self.object_placements)?
            .get(&(tenant_id.clone(), *object_id))
            .cloned()
            .unwrap_or_default())
    }

    async fn get_replication(
        &self,
        tenant_id: &TenantId,
        replication_id: &ReplicationId,
    ) -> CentralResult<Option<ReplicationRecord>> {
        Ok(lock(&self.replications)?
            .get(&(tenant_id.clone(), replication_id.clone()))
            .cloned())
    }

    async fn get_replication_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<ReplicationRecord>> {
        let Some(replication_id) = lock(&self.replication_requests)?
            .get(&(tenant_id.clone(), request_id.clone()))
            .cloned()
        else {
            return Ok(None);
        };
        self.get_replication(tenant_id, &replication_id).await
    }

    async fn list_replications_for_commit(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<ReplicationRecord>> {
        Ok(lock(&self.replications)?
            .values()
            .filter(|record| &record.tenant_id == tenant_id && &record.commit_id == commit_id)
            .cloned()
            .collect())
    }

    async fn list_replications_for_agent(
        &self,
        tenant_id: &TenantId,
        agent_id: &neoengram_domain::protocol::AgentId,
    ) -> CentralResult<Vec<ReplicationRecord>> {
        Ok(lock(&self.replications)?
            .values()
            .filter(|record| {
                &record.tenant_id == tenant_id && record.target_agent_id.as_ref() == Some(agent_id)
            })
            .cloned()
            .collect())
    }

    async fn refresh_replication_routes(
        &self,
        request: RefreshReplicationRoutesRequest,
    ) -> CentralResult<ReplicationRecord> {
        let key = (request.tenant_id.clone(), request.replication_id.clone());
        let mut records = lock(&self.replications)?;
        let record = records.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "replication does not exist",
            )
        })?;
        if record.attempt != request.expected_attempt {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            ));
        }
        if !matches!(
            record.state,
            ReplicationState::Queued
                | ReplicationState::Planning
                | ReplicationState::Transferring
                | ReplicationState::Verifying
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "terminal replication cannot refresh its route",
            ));
        }
        let current_source = route_binding_from_record(record, true)?;
        let current_target = route_binding_from_record(record, false)?;
        if request.source.session_generation.get() == 0
            || request.source.mount_generation.get() == 0
            || request.source.route_generation.get() == 0
            || request.target.session_generation.get() == 0
            || request.target.mount_generation.get() == 0
            || request.target.route_generation.get() == 0
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "replication route generations must be positive",
            ));
        }
        if current_source != request.expected_source || current_target != request.expected_target {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication route changed concurrently",
            ));
        }
        if request.source.agent_id != current_source.agent_id
            || request.target.agent_id != current_target.agent_id
            || request.source.edge_cluster_id != current_source.edge_cluster_id
            || request.target.edge_cluster_id != current_target.edge_cluster_id
            || request.source.gateway_pool_id != current_source.gateway_pool_id
            || request.target.gateway_pool_id != current_target.gateway_pool_id
            || request.source.mount_generation != current_source.mount_generation
            || request.target.mount_generation != current_target.mount_generation
            || request.source.session_generation.get() < current_source.session_generation.get()
            || request.target.session_generation.get() < current_target.session_generation.get()
            || request.source.route_generation.get() < current_source.route_generation.get()
            || request.target.route_generation.get() < current_target.route_generation.get()
        {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "replication route refresh cannot change Agent or mount identity",
            ));
        }
        if request.updated_at_unix_ms < record.updated_at_unix_ms {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            ));
        }
        record.source_edge_cluster_id = Some(request.source.edge_cluster_id);
        record.source_gateway_pool_id = Some(request.source.gateway_pool_id);
        record.source_session_generation = Some(request.source.session_generation);
        record.source_mount_generation = Some(request.source.mount_generation);
        record.source_route_generation = Some(request.source.route_generation);
        record.target_edge_cluster_id = Some(request.target.edge_cluster_id);
        record.target_gateway_pool_id = Some(request.target.gateway_pool_id);
        record.target_session_generation = Some(request.target.session_generation);
        record.target_mount_generation = Some(request.target.mount_generation);
        record.target_route_generation = Some(request.target.route_generation);
        record.updated_at_unix_ms = request.updated_at_unix_ms;
        Ok(record.clone())
    }

    async fn insert_replication(
        &self,
        record: ReplicationRecord,
    ) -> CentralResult<ReplicationRecord> {
        validate_replication_record(&record)?;
        let key = (record.tenant_id.clone(), record.replication_id.clone());
        let request_key = (record.tenant_id.clone(), record.request_id.clone());
        let mut records = lock(&self.replications)?;
        if let Some(existing) = records.get(&key) {
            return if existing == &record {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "replication ID is already bound to another record",
                ))
            };
        }
        if let Some(existing_id) = lock(&self.replication_requests)?.get(&request_key) {
            let existing = records
                .get(&(record.tenant_id.clone(), existing_id.clone()))
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::Internal,
                        "replication request index is inconsistent",
                    )
                })?;
            return if existing.commit_id == record.commit_id
                && existing.target_storage_volume_id == record.target_storage_volume_id
                && existing.object_set_digest == record.object_set_digest
            {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "replication request ID is already bound to another payload",
                ))
            };
        }
        // The service performs this check as an early validation, but the authority must repeat
        // it while holding the replication/placement locks.  Otherwise a concurrent finalize can
        // publish the target PlacementSet between the service pre-check and this insert.
        let placement_sets = lock(&self.placement_sets)?;
        if matches!(
            record.state,
            ReplicationState::Queued
                | ReplicationState::Planning
                | ReplicationState::Transferring
                | ReplicationState::Verifying
        ) && placement_sets.keys().any(|(tenant, commit, backend)| {
            tenant == &record.tenant_id
                && *commit == record.commit_id
                && backend.as_str() == record.target_backend_id
        }) {
            return Err(invalid(
                CentralErrorCode::ReplicationAlreadyActive,
                "a Commit PlacementSet already targets this backend",
            ));
        }
        if records.values().any(|existing| {
            existing.tenant_id == record.tenant_id
                && existing.commit_id == record.commit_id
                && existing.target_backend_id == record.target_backend_id
                && matches!(
                    existing.state,
                    ReplicationState::Queued
                        | ReplicationState::Planning
                        | ReplicationState::Transferring
                        | ReplicationState::Verifying
                )
        }) {
            return Err(invalid(
                CentralErrorCode::ReplicationAlreadyActive,
                "an active replication already targets this Commit and backend",
            ));
        }
        records.insert(key, record.clone());
        lock(&self.replication_requests)?.insert(request_key, record.replication_id.clone());
        Ok(record)
    }

    async fn transition_replication(
        &self,
        request: ReplicationStateTransitionRequest,
    ) -> CentralResult<ReplicationRecord> {
        if !valid_replication_transition(request.expected_state, request.next_state) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "invalid Replication state transition",
            ));
        }
        let key = (request.tenant_id.clone(), request.replication_id.clone());
        let mut records = lock(&self.replications)?;
        let record = records.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "replication does not exist",
            )
        })?;
        if record.attempt != request.expected_attempt || record.state != request.expected_state {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication state or attempt changed concurrently",
            ));
        }
        if request.completed_objects < record.completed_objects
            || request.completed_bytes < record.completed_bytes
            || request.completed_objects > record.total_objects
            || request.completed_bytes > record.total_bytes
            || request.updated_at_unix_ms < record.updated_at_unix_ms
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication progress cannot move backwards or exceed its frozen total",
            ));
        }
        record.state = request.next_state;
        record.completed_objects = request.completed_objects;
        record.completed_bytes = request.completed_bytes;
        record.issue_code = request.issue_code;
        record.issue_message = request.issue_message;
        record.updated_at_unix_ms = request.updated_at_unix_ms;
        Ok(record.clone())
    }

    async fn retry_replication(
        &self,
        request: RetryReplicationRequest,
    ) -> CentralResult<RetryReplicationResult> {
        let request_key = (request.tenant_id.clone(), request.request_id.clone());
        let mut retry_mutations = lock(&self.replication_retry_mutations)?;
        if let Some((stored_request, stored_result)) = retry_mutations.get(&request_key) {
            return if same_retry_request(stored_request, &request) {
                Ok(RetryReplicationResult {
                    replication: stored_result.clone(),
                    replayed: true,
                })
            } else {
                Err(invalid(
                    CentralErrorCode::ReplicationRetryRequestReused,
                    "replication retry request ID is already bound to another payload",
                ))
            };
        }
        let key = (request.tenant_id.clone(), request.replication_id.clone());
        let mut records = lock(&self.replications)?;
        let current = records.get(&key).cloned().ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "replication does not exist",
            )
        })?;
        if matches!(current.state, ReplicationState::Queued)
            && current.attempt == request.expected_attempt.saturating_add(1)
        {
            // A queued row at the next attempt can only be an unrecorded legacy retry.  The
            // request receipt is still required for new calls; treat this as a fenced conflict
            // instead of guessing which earlier request caused the transition.
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            ));
        }
        if current.attempt != request.expected_attempt {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            ));
        }
        if !matches!(
            current.state,
            ReplicationState::Failed | ReplicationState::Cancelled
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "only failed or cancelled Replications can be retried",
            ));
        }
        if request.updated_at_unix_ms < current.updated_at_unix_ms {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            ));
        }
        if records.values().any(|existing| {
            existing.replication_id != current.replication_id
                && existing.tenant_id == current.tenant_id
                && existing.commit_id == current.commit_id
                && existing.target_backend_id == current.target_backend_id
                && matches!(
                    existing.state,
                    ReplicationState::Queued
                        | ReplicationState::Planning
                        | ReplicationState::Transferring
                        | ReplicationState::Verifying
                )
        }) {
            return Err(invalid(
                CentralErrorCode::ReplicationAlreadyActive,
                "an active replication already targets this Commit and backend",
            ));
        }
        let placement_sets = lock(&self.placement_sets)?;
        if placement_sets.keys().any(|(tenant, commit, backend)| {
            tenant == &current.tenant_id
                && *commit == current.commit_id
                && backend.as_str() == current.target_backend_id
        }) {
            return Err(invalid(
                CentralErrorCode::ReplicationAlreadyActive,
                "a Commit PlacementSet already targets this backend",
            ));
        }
        let next_attempt = current.attempt.checked_add(1).ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "replication attempt exhausted",
            )
        })?;
        let record = records
            .get_mut(&key)
            .expect("replication was read while holding the same lock");
        record.attempt = next_attempt;
        record.state = ReplicationState::Queued;
        record.issue_code = None;
        record.issue_message = None;
        record.updated_at_unix_ms = request.updated_at_unix_ms;
        let result = record.clone();
        retry_mutations.insert(request_key, (request, result.clone()));
        Ok(RetryReplicationResult {
            replication: result,
            replayed: false,
        })
    }

    async fn cancel_replication(
        &self,
        request: CancelReplicationRequest,
    ) -> CentralResult<ReplicationRecord> {
        let key = (request.tenant_id.clone(), request.replication_id.clone());
        let mut records = lock(&self.replications)?;
        let record = records.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "replication does not exist",
            )
        })?;
        if record.attempt != request.expected_attempt {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            ));
        }
        if matches!(record.state, ReplicationState::Cancelled) {
            return Ok(record.clone());
        }
        if matches!(
            record.state,
            ReplicationState::Published | ReplicationState::Failed
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "terminal Replication cannot be cancelled",
            ));
        }
        if request.updated_at_unix_ms < record.updated_at_unix_ms {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            ));
        }
        record.state = ReplicationState::Cancelled;
        record.issue_code = Some("REPLICATION_CANCELLED".to_owned());
        record.issue_message = Some("replication was cancelled by the caller".to_owned());
        record.updated_at_unix_ms = request.updated_at_unix_ms;
        Ok(record.clone())
    }

    async fn finalize_replication(
        &self,
        request: FinalizeReplicationRequest,
    ) -> CentralResult<FinalizeReplicationResult> {
        let replication_key = (request.tenant_id.clone(), request.replication_id.clone());
        let record = lock(&self.replications)?
            .get(&replication_key)
            .cloned()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "replication does not exist",
                )
            })?;
        if record.attempt != request.expected_attempt {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            ));
        }
        if matches!(record.state, ReplicationState::Published) {
            let stored = lock(&self.placement_sets)?
                .get(&(
                    request.tenant_id.clone(),
                    record.commit_id,
                    request.placement_set.backend_id.clone(),
                ))
                .cloned();
            if record.target_placement_set_id.as_ref()
                == Some(&request.placement_set.placement_set_id)
                && stored.as_ref() == Some(&request.placement_set)
            {
                return Ok(FinalizeReplicationResult {
                    replication: record,
                    placement_set: request.placement_set,
                    replayed: true,
                });
            }
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "published Replication is bound to another PlacementSet",
            ));
        }
        if !matches!(record.state, ReplicationState::Verifying) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "only verifying Replications can be finalized",
            ));
        }
        if record.completed_objects != record.total_objects
            || record.completed_bytes != record.total_bytes
        {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "all replication objects must be verified before finalize",
            ));
        }
        if request.finalized_at_unix_ms < record.updated_at_unix_ms {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            ));
        }
        let object_set = lock(&self.commit_object_sets)?
            .get(&(request.tenant_id.clone(), record.commit_id))
            .cloned()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "Commit ObjectSet is missing",
                )
            })?;
        let replication_objects = lock(&self.replication_objects)?;
        let checkpoints = replication_objects
            .iter()
            .filter(|((tenant, replication, _), _)| {
                tenant == &request.tenant_id && replication == &request.replication_id
            })
            .map(|(_, checkpoint)| checkpoint.clone())
            .collect::<Vec<_>>();
        validate_replication_checkpoints(&record, &object_set, &checkpoints)?;
        validate_replication_publication(
            &record,
            &object_set,
            &request.placements,
            &request.placement_set,
        )?;
        let placement_key = (
            request.placement_set.tenant_id.clone(),
            request.placement_set.commit_id.digest(),
            request.placement_set.backend_id.clone(),
        );
        let mut records = lock(&self.replications)?;
        let mut placement_sets = lock(&self.placement_sets)?;
        let mut object_placements = lock(&self.object_placements)?;
        if records.values().any(|existing| {
            existing.replication_id != request.replication_id
                && existing.tenant_id == request.tenant_id
                && existing.commit_id == record.commit_id
                && existing.target_backend_id == request.placement_set.backend_id.as_str()
                && matches!(
                    existing.state,
                    ReplicationState::Queued
                        | ReplicationState::Planning
                        | ReplicationState::Transferring
                        | ReplicationState::Verifying
                )
        }) {
            return Err(invalid(
                CentralErrorCode::ReplicationAlreadyActive,
                "an active replication already targets this Commit and backend",
            ));
        }
        if let Some(existing) = placement_sets.get(&placement_key) {
            if existing != &request.placement_set {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "target PlacementSet is already bound to different metadata",
                ));
            }
        }
        for placement in &request.placements {
            if let Some(existing) = object_placements
                .get(&(placement.tenant_id.clone(), placement.object_id))
                .and_then(|entries| {
                    entries.iter().find(|existing| {
                        existing.backend_id == placement.backend_id
                            && existing.placement_generation == placement.placement_generation
                    })
                })
            {
                if existing != placement {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "target ObjectPlacement is already bound to different metadata",
                    ));
                }
            }
        }
        let current = records.get_mut(&replication_key).ok_or_else(|| {
            invalid(
                CentralErrorCode::ResourceNotFound,
                "replication does not exist",
            )
        })?;
        if current.attempt != request.expected_attempt
            || current.state != ReplicationState::Verifying
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication state or attempt changed concurrently",
            ));
        }
        for placement in request.placements {
            let entries = object_placements
                .entry((placement.tenant_id.clone(), placement.object_id))
                .or_default();
            if !entries.iter().any(|existing| {
                existing.backend_id == placement.backend_id
                    && existing.placement_generation == placement.placement_generation
            }) {
                entries.push(placement);
            }
        }
        placement_sets.insert(placement_key, request.placement_set.clone());
        current.state = ReplicationState::Published;
        current.target_placement_set_id = Some(request.placement_set.placement_set_id.clone());
        current.completed_objects = current.total_objects;
        current.completed_bytes = current.total_bytes;
        current.issue_code = None;
        current.issue_message = None;
        current.updated_at_unix_ms = request.finalized_at_unix_ms;
        Ok(FinalizeReplicationResult {
            replication: current.clone(),
            placement_set: request.placement_set,
            replayed: false,
        })
    }

    async fn upsert_replication_object(
        &self,
        record: crate::ReplicationObjectRecord,
    ) -> CentralResult<crate::ReplicationObjectRecord> {
        // Keep the checkpoint and Replication state locks together. Finalization takes them in
        // this same order, so a terminal-state transition cannot race a checkpoint write.
        let mut checkpoints = lock(&self.replication_objects)?;
        let replications = lock(&self.replications)?;
        let replication = replications
            .get(&(record.tenant_id.clone(), record.replication_id.clone()))
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "replication object references a missing Replication",
                )
            })?;
        if matches!(
            replication.state,
            ReplicationState::Published | ReplicationState::Failed | ReplicationState::Cancelled
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "terminal Replication cannot accept object checkpoints",
            ));
        }
        if record.retry_count != replication.attempt {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication object checkpoint attempt is stale",
            ));
        }
        let key = (
            record.tenant_id.clone(),
            record.replication_id.clone(),
            record.object_id,
        );
        if let Some(existing) = checkpoints.get(&key) {
            if existing == &record {
                return Ok(existing.clone());
            }
            if record.offset < existing.offset
                || record.retry_count < existing.retry_count
                || record.updated_at_unix_ms < existing.updated_at_unix_ms
            {
                return Err(invalid(
                    CentralErrorCode::ConcurrentUpdate,
                    "replication object checkpoint cannot move backwards",
                ));
            }
        }
        checkpoints.insert(key, record.clone());
        Ok(record)
    }

    async fn list_replication_objects(
        &self,
        tenant_id: &TenantId,
        replication_id: &ReplicationId,
    ) -> CentralResult<Vec<crate::ReplicationObjectRecord>> {
        Ok(lock(&self.replication_objects)?
            .iter()
            .filter(|((tenant, replication, _), _)| {
                tenant == tenant_id && replication == replication_id
            })
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn get_workspace(
        &self,
        tenant_id: &TenantId,
        workspace_id: &WorkspaceId,
    ) -> CentralResult<Option<WorkspaceRecord>> {
        Ok(lock(&self.workspaces)?
            .get(&(tenant_id.clone(), workspace_id.clone()))
            .cloned())
    }

    async fn get_workspace_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<WorkspaceRecord>> {
        let Some(workspace_id) = lock(&self.workspace_requests)?
            .get(&(tenant_id.clone(), request_id.clone()))
            .cloned()
        else {
            return Ok(None);
        };
        self.get_workspace(tenant_id, &workspace_id).await
    }

    async fn insert_workspace(&self, record: WorkspaceRecord) -> CentralResult<WorkspaceRecord> {
        let key = (record.tenant_id.clone(), record.workspace_id.clone());
        let request_key = (record.tenant_id.clone(), record.request_id.clone());
        let mut records = lock(&self.workspaces)?;
        if let Some(existing) = records.get(&key) {
            return if existing == &record {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "workspace ID is already bound to another record",
                ))
            };
        }
        if let Some(existing_id) = lock(&self.workspace_requests)?.get(&request_key) {
            let existing = records
                .get(&(record.tenant_id.clone(), existing_id.clone()))
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::Internal,
                        "workspace request index is inconsistent",
                    )
                })?;
            return if existing.project_id == record.project_id
                && existing.artifact_id == record.artifact_id
                && existing.base_commit_id == record.base_commit_id
                && existing.target_storage_volume_id == record.target_storage_volume_id
            {
                Ok(existing.clone())
            } else {
                Err(invalid(
                    CentralErrorCode::InvalidState,
                    "workspace request ID is already bound to another payload",
                ))
            };
        }
        records.insert(key, record.clone());
        lock(&self.workspace_requests)?.insert(request_key, record.workspace_id.clone());
        Ok(record)
    }

    async fn commit_availability(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<CommitAvailabilityRecord> {
        let Some(object_set) = lock(&self.commit_object_sets)?
            .get(&(tenant_id.clone(), *commit_id))
            .cloned()
        else {
            return Ok(CommitAvailabilityRecord {
                tenant_id: tenant_id.clone(),
                commit_id: *commit_id,
                data_health: neoengram_domain::protocol::DataHealth::Unavailable,
                verified_placements: 0,
                missing_objects: 0,
                verified_storage_volume_ids: Vec::new(),
            });
        };
        let published = lock(&self.placement_sets)?
            .iter()
            .filter(|((tenant, digest, _), set)| {
                tenant == tenant_id
                    && digest == commit_id
                    && set.state == neoengram_domain::protocol::CommitPlacementSetState::Published
            })
            .map(|((_, _, backend), set)| {
                (
                    backend.clone(),
                    set.placement_generation,
                    set.storage_volume_id.clone(),
                    set.archive_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        let placements = lock(&self.object_placements)?;
        let mut verified_storage_volume_ids = std::collections::BTreeSet::new();
        let mut complete_placements = 0_u64;
        let mut missing_objects = 0_u64;
        let mut degraded = false;
        for object in &object_set.object_set.objects {
            let copies = placements
                .get(&(tenant_id.clone(), object.object_id))
                .into_iter()
                .flatten()
                .filter(|copy| {
                    published
                        .iter()
                        .any(|(backend, generation, volume, archive)| {
                            backend == &copy.backend_id
                                && *generation == copy.placement_generation
                                && volume == &copy.storage_volume_id
                                && archive == &copy.archive_id
                        })
                })
                .collect::<Vec<_>>();
            let readable = copies
                .iter()
                .filter(|copy| {
                    copy.state == neoengram_domain::protocol::PlacementState::Verified
                        && copy.verified_size.get() == object.size.get()
                        && copy.verified_digest == object.object_id.digest()
                })
                .count();
            if readable == 0 {
                missing_objects = missing_objects.saturating_add(1);
            }
            if copies.iter().any(|copy| {
                copy.state != neoengram_domain::protocol::PlacementState::Verified
                    || copy.verified_size.get() != object.size.get()
                    || copy.verified_digest != object.object_id.digest()
            }) {
                degraded = true;
            }
        }
        for (backend, generation, volume, archive) in &published {
            let complete = object_set.object_set.objects.iter().all(|object| {
                placements
                    .get(&(tenant_id.clone(), object.object_id))
                    .into_iter()
                    .flatten()
                    .any(|copy| {
                        copy.backend_id == *backend
                            && copy.placement_generation == *generation
                            && copy.storage_volume_id == *volume
                            && copy.archive_id == *archive
                            && copy.state == neoengram_domain::protocol::PlacementState::Verified
                            && copy.verified_size.get() == object.size.get()
                            && copy.verified_digest == object.object_id.digest()
                    })
            });
            if complete {
                complete_placements = complete_placements.saturating_add(1);
                if let Some(volume) = volume {
                    verified_storage_volume_ids.insert(volume.clone());
                }
            }
        }
        let data_health = if missing_objects > 0 {
            neoengram_domain::protocol::DataHealth::Unavailable
        } else if degraded {
            neoengram_domain::protocol::DataHealth::Degraded
        } else {
            neoengram_domain::protocol::DataHealth::Available
        };
        Ok(CommitAvailabilityRecord {
            tenant_id: tenant_id.clone(),
            commit_id: *commit_id,
            data_health,
            verified_placements: complete_placements,
            missing_objects,
            verified_storage_volume_ids: verified_storage_volume_ids.into_iter().collect(),
        })
    }
}

/// A replan publishes the next revision in one operation, but its state is logically reached
/// through the durable `Planning` phase.  Accept that two-step state path at the aggregate
/// boundary so a retry can move a recoverable/failed job directly to its planned next state.
fn materialization_state_transition_allowed(
    current: MaterializationJobState,
    next: MaterializationJobState,
) -> bool {
    // A completed Job is normally terminal, but a later integrity observation can make its
    // derived target Coverage partial. The explicit retry path then reopens it for one fenced
    // planning revision; all other terminal transitions remain rejected by the domain state
    // machine.
    (current == MaterializationJobState::Complete && next == MaterializationJobState::Planning)
        || current.can_transition_to(next)
        || (current.can_transition_to(MaterializationJobState::Planning)
            && MaterializationJobState::Planning.can_transition_to(next))
}

#[derive(Debug, Default)]
pub struct InMemoryPreCommitRepository {
    state: Mutex<InMemoryPreCommitState>,
}

#[derive(Debug, Default)]
struct InMemoryPreCommitState {
    precommits: BTreeMap<PreCommitKey, PreCommitRecord>,
    commits: BTreeMap<
        (
            TenantId,
            neoengram_domain::protocol::ProjectId,
            ArtifactId,
            CommitId,
        ),
        crate::CommitRecord,
    >,
    mutations:
        BTreeMap<(TenantId, neoengram_domain::protocol::RequestId), InMemoryPreCommitMutation>,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum InMemoryPreCommitMutation {
    Start {
        request: PreCommitStartRequest,
        result: PreCommitRecord,
    },
    Restart {
        request: PreCommitRestartRequest,
        result: PreCommitRecord,
    },
    Cancel {
        request: PreCommitCancelRequest,
        result: PreCommitRecord,
    },
    Commit {
        request: PreCommitCommitRequest,
        result: PreCommitCommitSnapshot,
    },
}

#[async_trait]
impl PreCommitRepository for InMemoryPreCommitRepository {
    async fn start(
        &self,
        request: PreCommitStartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.tenant_id.clone(),
            request.precommit_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Start {
                    request: stored,
                    result,
                } if same_start_request(stored, &request) => Ok(PreCommitMutationOutcome {
                    precommit: result.clone(),
                    replayed: true,
                }),
                _ => Err(precommit_request_conflict()),
            };
        }
        let record = build_started(&request)?;
        if state.precommits.contains_key(&record.key()) {
            return Err(precommit_request_conflict());
        }
        ensure_active_available(&state, &record, None)?;
        ensure_job_identity_available(&state, &record)?;
        state.precommits.insert(record.key(), record.clone());
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Start {
                request,
                result: record.clone(),
            },
        );
        Ok(PreCommitMutationOutcome {
            precommit: record,
            replayed: false,
        })
    }

    async fn get(&self, key: &PreCommitKey) -> CentralResult<Option<PreCommitRecord>> {
        Ok(lock(&self.state)?.precommits.get(key).cloned())
    }

    async fn get_active(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &neoengram_domain::protocol::PlaygroundId,
    ) -> CentralResult<Option<PreCommitRecord>> {
        let state = lock(&self.state)?;
        let mut matches = state.precommits.values().filter(|record| {
            &record.tenant_id == tenant_id
                && &record.project_id == project_id
                && &record.artifact_id == artifact_id
                && &record.playground_id == playground_id
                && precommit_is_active(record)
        });
        let result = matches.next().cloned();
        if matches.next().is_some() {
            return Err(crate::CentralError::new(
                CentralErrorCode::Internal,
                "more than one active Pre-commit exists for a Playground",
            ));
        }
        Ok(result)
    }

    async fn list_running(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        Ok(lock(&self.state)?
            .precommits
            .iter()
            .filter(|(key, record)| {
                after.is_none_or(|after| *key > after)
                    && record.state == crate::PreCommitState::Running
            })
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn list_unpublished_commits(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        Ok(lock(&self.state)?
            .precommits
            .iter()
            .filter(|(key, record)| {
                after.is_none_or(|after| *key > after)
                    && record.state == crate::PreCommitState::Committed
                    && record.head_published_at_unix_ms.is_none()
            })
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn find_restart_result(
        &self,
        tenant_id: &TenantId,
        restart_request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<PreCommitRecord>> {
        Ok(
            match lock(&self.state)?
                .mutations
                .get(&(tenant_id.clone(), restart_request_id.clone()))
            {
                Some(InMemoryPreCommitMutation::Restart { result, .. }) => Some(result.clone()),
                _ => None,
            },
        )
    }

    async fn restart(
        &self,
        request: PreCommitRestartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.key.tenant_id.clone(),
            request.restart_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Restart {
                    request: stored,
                    result,
                } if same_restart_request(stored, &request) => Ok(PreCommitMutationOutcome {
                    precommit: result.clone(),
                    replayed: true,
                }),
                _ => Err(precommit_request_conflict()),
            };
        }
        let stored = state
            .precommits
            .get(&request.key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let restarted = apply_restart(stored, &request)?;
        ensure_active_available(&state, &restarted, Some(&restarted.key()))?;
        ensure_job_identity_available(&state, &restarted)?;
        state.precommits.insert(restarted.key(), restarted.clone());
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Restart {
                request,
                result: restarted.clone(),
            },
        );
        Ok(PreCommitMutationOutcome {
            precommit: restarted,
            replayed: false,
        })
    }

    async fn cancel(
        &self,
        request: PreCommitCancelRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.key.tenant_id.clone(),
            request.cancel_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Cancel {
                    request: stored,
                    result,
                } if same_cancel_request(stored, &request) => Ok(PreCommitMutationOutcome {
                    precommit: result.clone(),
                    replayed: true,
                }),
                _ => Err(precommit_request_conflict()),
            };
        }
        let stored = state
            .precommits
            .get(&request.key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let cancelled = apply_cancel(stored, &request)?;
        state.precommits.insert(cancelled.key(), cancelled.clone());
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Cancel {
                request,
                result: cancelled.clone(),
            },
        );
        Ok(PreCommitMutationOutcome {
            precommit: cancelled,
            replayed: false,
        })
    }

    async fn sync_job(
        &self,
        job: JobRecord,
        published_index: Option<PublishedIndex>,
        observed_at_unix_ms: UnixMillis,
    ) -> CentralResult<Option<PreCommitRecord>> {
        let mut state = lock(&self.state)?;
        let found = state
            .precommits
            .iter()
            .find(|(_, precommit)| {
                precommit.tenant_id == job.spec.tenant_id && precommit.job_id == job.spec.job_id
            })
            .map(|(key, precommit)| (key.clone(), precommit.clone()));
        let Some((key, stored)) = found else {
            return Ok(None);
        };
        let base_records = match stored.frozen_head_commit_id {
            None => Some(Vec::new()),
            Some(commit_id) => state
                .commits
                .get(&(
                    stored.tenant_id.clone(),
                    stored.project_id.clone(),
                    stored.artifact_id.clone(),
                    commit_id,
                ))
                .map(|commit| commit.records.clone()),
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
            state.precommits.insert(key, synchronized.clone());
        }
        Ok(Some(synchronized))
    }

    async fn commit(
        &self,
        request: PreCommitCommitRequest,
    ) -> CentralResult<PreCommitCommitOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.key.tenant_id.clone(),
            request.commit.commit_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Commit {
                    request: stored,
                    result,
                } if same_commit_request(stored, &request) => {
                    Ok(PreCommitCommitOutcome::from_snapshot(result.clone(), true))
                }
                _ => Err(precommit_request_conflict()),
            };
        }
        let stored = state
            .precommits
            .get(&request.key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let consumed = apply_commit(stored, &request)?;
        let commit_key = (
            consumed.commit.tenant_id.clone(),
            consumed.commit.project_id.clone(),
            consumed.commit.artifact_id.clone(),
            consumed.commit.commit_id,
        );
        if state.commits.contains_key(&commit_key) {
            return Err(precommit_request_conflict());
        }
        state.commits.insert(commit_key, consumed.commit.clone());
        state.precommits.insert(
            consumed.consumed_precommit.key(),
            consumed.consumed_precommit.clone(),
        );
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Commit {
                request,
                result: consumed.clone(),
            },
        );
        Ok(PreCommitCommitOutcome::from_snapshot(consumed, false))
    }

    async fn get_commit(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &ArtifactId,
        commit_id: CommitId,
    ) -> CentralResult<Option<crate::CommitRecord>> {
        Ok(lock(&self.state)?
            .commits
            .get(&(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                commit_id,
            ))
            .cloned())
    }

    async fn list_published_commits(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Vec<crate::CommitRecord>> {
        let state = lock(&self.state)?;
        Ok(state
            .commits
            .iter()
            .filter(|((commit_tenant, commit_project, commit_artifact, _), _)| {
                commit_tenant == tenant_id
                    && commit_project == project_id
                    && commit_artifact == artifact_id
            })
            .filter(|(_, commit)| {
                state
                    .precommits
                    .get(&PreCommitKey::new(
                        commit.tenant_id.clone(),
                        commit.source_precommit_id.clone(),
                    ))
                    .is_some_and(|precommit| {
                        precommit.committed_commit_id == Some(commit.commit_id)
                            && precommit.head_published_at_unix_ms.is_some()
                    })
            })
            .map(|(_, commit)| commit.clone())
            .collect())
    }

    async fn acknowledge_head_publication(
        &self,
        key: &PreCommitKey,
        commit_id: CommitId,
        published_at_unix_ms: UnixMillis,
    ) -> CentralResult<PreCommitRecord> {
        let mut state = lock(&self.state)?;
        let stored = state
            .precommits
            .get(key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let acknowledged =
            apply_head_publication_ack(stored.clone(), commit_id, published_at_unix_ms)?;
        if acknowledged != stored {
            state.precommits.insert(key.clone(), acknowledged.clone());
        }
        Ok(acknowledged)
    }
}

fn ensure_job_identity_available(
    state: &InMemoryPreCommitState,
    candidate: &PreCommitRecord,
) -> CentralResult<()> {
    if state.precommits.values().any(|stored| {
        stored.key() != candidate.key()
            && stored.tenant_id == candidate.tenant_id
            && stored.job_id == candidate.job_id
    }) {
        return Err(precommit_request_conflict());
    }
    Ok(())
}

fn ensure_active_available(
    state: &InMemoryPreCommitState,
    candidate: &PreCommitRecord,
    except: Option<&PreCommitKey>,
) -> CentralResult<()> {
    if state.precommits.values().any(|stored| {
        except.is_none_or(|except| stored.key() != *except)
            && stored.tenant_id == candidate.tenant_id
            && stored.project_id == candidate.project_id
            && stored.artifact_id == candidate.artifact_id
            && stored.playground_id == candidate.playground_id
            && precommit_is_active(stored)
    }) {
        return Err(precommit_request_conflict());
    }
    Ok(())
}

fn precommit_is_active(record: &PreCommitRecord) -> bool {
    matches!(
        record.state,
        crate::PreCommitState::Running
            | crate::PreCommitState::Ready
            | crate::PreCommitState::Abnormal
    )
}

fn precommit_not_found() -> crate::CentralError {
    crate::CentralError::new(
        CentralErrorCode::JobNotFound,
        "tenant-scoped Pre-commit was not found",
    )
    .with_retryable(false)
}

fn precommit_request_conflict() -> crate::CentralError {
    crate::CentralError::new(
        CentralErrorCode::ConcurrentUpdate,
        "Pre-commit mutation identity was reused with another payload",
    )
    .with_retryable(false)
}

#[derive(Debug, Default)]
pub struct InMemoryAssignmentOutbox {
    assignments: Mutex<
        BTreeMap<
            (TenantId, neoengram_domain::protocol::AssignmentId),
            InMemoryAssignmentReservation,
        >,
    >,
}

#[derive(Debug, Clone)]
struct InMemoryAssignmentReservation {
    assignment: JobAssignment,
    published: bool,
    retired: bool,
}

impl InMemoryAssignmentOutbox {
    pub fn messages(&self) -> CentralResult<Vec<JobAssignment>> {
        Ok(lock(&self.assignments)?
            .values()
            .filter(|reservation| reservation.published)
            .map(|reservation| reservation.assignment.clone())
            .collect())
    }
}

#[async_trait]
impl AssignmentOutbox for InMemoryAssignmentOutbox {
    async fn reserve(&self, assignment: JobAssignment) -> CentralResult<AssignmentReserveOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut assignments = lock(&self.assignments)?;
        let key = (tenant_id.clone(), assignment_id.clone());
        if let Some(existing) = assignments.get(&key) {
            if existing.assignment == assignment {
                return Ok(AssignmentReserveOutcome::Existing);
            }
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "assignment ID {} was reused with another payload",
                    assignment_id
                ),
            ));
        }
        assignments.insert(
            key,
            InMemoryAssignmentReservation {
                assignment,
                published: false,
                retired: false,
            },
        );
        Ok(AssignmentReserveOutcome::Reserved)
    }

    async fn publish(&self, assignment: JobAssignment) -> CentralResult<AssignmentPublishOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut assignments = lock(&self.assignments)?;
        let key = (tenant_id.clone(), assignment_id.clone());
        let existing = assignments.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                format!(
                    "assignment {} must be reserved before publication",
                    assignment_id
                ),
            )
        })?;
        if existing.assignment != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "assignment ID {} was reused with another payload",
                    assignment_id
                ),
            ));
        }
        if existing.published {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        existing.published = true;
        Ok(AssignmentPublishOutcome::Published)
    }

    async fn reactivate(
        &self,
        assignment: JobAssignment,
    ) -> CentralResult<AssignmentPublishOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut assignments = lock(&self.assignments)?;
        let existing = assignments
            .get_mut(&(tenant_id.clone(), assignment_id.clone()))
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    format!("assignment {assignment_id} must be reserved before reactivation"),
                )
            })?;
        if existing.assignment != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!("assignment ID {assignment_id} was reused with another payload"),
            ));
        }
        if existing.published && !existing.retired {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        existing.published = true;
        existing.retired = false;
        Ok(AssignmentPublishOutcome::Published)
    }

    async fn retire(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::AssignmentId,
    ) -> CentralResult<AssignmentRetireOutcome> {
        let mut assignments = lock(&self.assignments)?;
        let reservation = assignments
            .get_mut(&(tenant_id.clone(), assignment_id.clone()))
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    format!("assignment {assignment_id} is not reserved"),
                )
            })?;
        if !reservation.published {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("assignment {assignment_id} is not published"),
            ));
        }
        if reservation.retired {
            return Ok(AssignmentRetireOutcome::AlreadyRetired);
        }
        reservation.retired = true;
        Ok(AssignmentRetireOutcome::Retired)
    }

    async fn pending_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobAssignment>> {
        Ok(lock(&self.assignments)?
            .values()
            .filter(|reservation| reservation.published && !reservation.retired)
            .filter(|reservation| assignment_identity(&reservation.assignment).3 == agent_id)
            .take(limit)
            .map(|reservation| reservation.assignment.clone())
            .collect())
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

#[derive(Debug, Default)]
pub struct InMemoryMetadataBatchStager {
    batches: Mutex<BTreeMap<(TenantId, MetadataBatchId), StagedMetadataBatch>>,
}

impl InMemoryMetadataBatchStager {
    pub fn batches(&self) -> CentralResult<Vec<StagedMetadataBatch>> {
        Ok(lock(&self.batches)?.values().cloned().collect())
    }

    /// Removes transient staging material to exercise post-publication catalog independence.
    pub fn clear(&self) -> CentralResult<()> {
        lock(&self.batches)?.clear();
        Ok(())
    }
}

#[async_trait]
impl MetadataBatchStager for InMemoryMetadataBatchStager {
    async fn stage_descriptor(&self, descriptor: MetadataBatchDescriptor) -> CentralResult<bool> {
        descriptor.validate()?;
        let mut batches = lock(&self.batches)?;
        let key = (
            descriptor.scope.tenant_id.clone(),
            descriptor.batch_id.clone(),
        );
        if let Some(existing) = batches.get(&key) {
            if existing.descriptor == descriptor {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!("metadata batch {} descriptor changed", descriptor.batch_id),
            ));
        }
        batches.insert(
            key,
            StagedMetadataBatch {
                descriptor,
                pages: BTreeMap::new(),
            },
        );
        Ok(false)
    }

    async fn stage_page(
        &self,
        descriptor: &MetadataBatchDescriptor,
        page: MetadataBatchPage,
    ) -> CentralResult<bool> {
        descriptor.validate_page(&page)?;
        let mut batches = lock(&self.batches)?;
        let key = (
            descriptor.scope.tenant_id.clone(),
            descriptor.batch_id.clone(),
        );
        let staged = batches.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::BatchIncomplete,
                format!(
                    "metadata batch {} descriptor must be staged before its pages",
                    descriptor.batch_id
                ),
            )
        })?;
        if staged.descriptor != *descriptor {
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!(
                    "metadata batch {} descriptor differs from staging",
                    descriptor.batch_id
                ),
            ));
        }
        if let Some(existing) = staged.pages.get(&page.page_number) {
            if existing == &page {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!(
                    "metadata batch {} page {} changed",
                    descriptor.batch_id, page.page_number
                ),
            ));
        }
        staged.pages.insert(page.page_number, page);
        Ok(false)
    }

    async fn get(
        &self,
        tenant_id: &TenantId,
        batch_id: &MetadataBatchId,
    ) -> CentralResult<Option<StagedMetadataBatch>> {
        Ok(lock(&self.batches)?
            .get(&(tenant_id.clone(), batch_id.clone()))
            .cloned())
    }
}

type PlacementReceiptKey = (TenantId, ObjectReceiptId);

#[derive(Debug, Default)]
pub struct InMemoryObjectCatalog {
    placements: Mutex<BTreeMap<PlacementReceiptKey, ObjectPlacementEvidence>>,
}

#[async_trait]
impl ObjectCatalog for InMemoryObjectCatalog {
    async fn record_placement(&self, evidence: &ObjectPlacementEvidence) -> CentralResult<()> {
        let receipt = &evidence.receipt;
        let key = (receipt.tenant_id.clone(), receipt.receipt_id.clone());
        let mut placements = lock(&self.placements)?;
        if let Some(existing) = placements.get(&key) {
            return if existing == evidence {
                Ok(())
            } else {
                Err(invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!(
                        "object placement receipt {} has conflicting evidence",
                        receipt.receipt_id
                    ),
                ))
            };
        }
        if placements.values().any(|existing| {
            existing.receipt.tenant_id == receipt.tenant_id
                && existing.receipt.artifact_id == receipt.artifact_id
                && existing.receipt.storage_volume_id == receipt.storage_volume_id
                && existing.receipt.artifact_placement_id == receipt.artifact_placement_id
                && existing.placement_generation == evidence.placement_generation
                && existing.receipt.object_id == receipt.object_id
                && existing.receipt.size != receipt.size
        }) {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                format!(
                    "object {} has conflicting placement sizes",
                    receipt.object_id
                ),
            ));
        }
        placements.insert(key, evidence.clone());
        Ok(())
    }

    async fn object_placement(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        storage_volume_id: &StorageVolumeId,
        artifact_placement_id: &neoengram_domain::protocol::ArtifactPlacementId,
        placement_generation: PlacementGeneration,
        object_id: ObjectId,
    ) -> CentralResult<Option<ObjectPlacementEvidence>> {
        Ok(lock(&self.placements)?
            .values()
            .filter(|evidence| {
                let receipt = &evidence.receipt;
                &receipt.tenant_id == tenant_id
                    && &receipt.artifact_id == artifact_id
                    && &receipt.storage_volume_id == storage_volume_id
                    && &receipt.artifact_placement_id == artifact_placement_id
                    && evidence.placement_generation == placement_generation
                    && receipt.object_id == object_id
            })
            .max_by_key(|evidence| evidence.receipt.verified_at_unix_ms)
            .cloned())
    }

    async fn artifact_placement_volumes(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Vec<StorageVolumeId>> {
        Ok(lock(&self.placements)?
            .values()
            .filter(|evidence| {
                &evidence.receipt.tenant_id == tenant_id
                    && &evidence.receipt.artifact_id == artifact_id
            })
            .map(|evidence| evidence.receipt.storage_volume_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    async fn volume_unique_artifact_replicas(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Vec<ArtifactId>> {
        let placements = lock(&self.placements)?;
        Ok(placements
            .values()
            .filter(|candidate| {
                &candidate.receipt.tenant_id == tenant_id
                    && &candidate.receipt.storage_volume_id == storage_volume_id
                    && !placements.values().any(|other| {
                        other.receipt.tenant_id == candidate.receipt.tenant_id
                            && other.receipt.artifact_id == candidate.receipt.artifact_id
                            && other.receipt.object_id == candidate.receipt.object_id
                            && other.receipt.size == candidate.receipt.size
                            && other.receipt.storage_volume_id != *storage_volume_id
                    })
            })
            .map(|evidence| evidence.receipt.artifact_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct IndexSnapshot {
    version: WireIndexVersion,
    records: BTreeMap<LogicalPath, FileRecord>,
}

#[derive(Debug, Default)]
struct PublisherState {
    indexes: BTreeMap<IndexKey, IndexSnapshot>,
    manifests: BTreeMap<(TenantId, ArtifactId, ManifestId), Manifest>,
    publications: BTreeMap<JobKey, (IndexPublishRequest, IndexPublishOutcome)>,
}

#[derive(Debug, Default)]
pub struct InMemoryIndexPublisher {
    state: Mutex<PublisherState>,
}

impl InMemoryIndexPublisher {
    fn reject(
        state: &mut PublisherState,
        request: IndexPublishRequest,
        rejection: IndexPublishRejection,
    ) -> IndexPublishOutcome {
        let outcome = IndexPublishOutcome::Rejected(rejection);
        state
            .publications
            .insert(request.job_key.clone(), (request, outcome.clone()));
        outcome
    }

    pub fn seed(
        &self,
        key: IndexKey,
        revision: u64,
        records: Vec<FileRecord>,
    ) -> CentralResult<WireIndexVersion> {
        let version = WireIndexVersion::from(
            IndexVersion::from_snapshot(revision, &records).map_err(|error| {
                invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("invalid seeded Index: {error}"),
                )
            })?,
        );
        let mapped = records
            .into_iter()
            .map(|record| (record.path.clone(), record))
            .collect();
        lock(&self.state)?.indexes.insert(
            key,
            IndexSnapshot {
                version: version.clone(),
                records: mapped,
            },
        );
        Ok(version)
    }

    pub fn snapshot(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        let state = lock(&self.state)?;
        let snapshot = state.indexes.get(key).cloned().unwrap_or(empty_snapshot()?);
        Ok(PublishedIndex {
            version: snapshot.version,
            records: snapshot.records.into_values().collect(),
        })
    }

    pub fn manifest(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        manifest_id: ManifestId,
    ) -> CentralResult<Option<Manifest>> {
        Ok(lock(&self.state)?
            .manifests
            .get(&(tenant_id.clone(), artifact_id.clone(), manifest_id))
            .cloned())
    }
}

#[async_trait]
impl IndexPublisher for InMemoryIndexPublisher {
    async fn initialize_snapshot(
        &self,
        request: InitializeIndexSnapshotRequest,
    ) -> CentralResult<WireIndexVersion> {
        validate_initial_index_snapshot(&request.version, &request.records)?;
        let records = request
            .records
            .into_iter()
            .map(|record| (record.path.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let snapshot = IndexSnapshot {
            version: request.version,
            records,
        };
        let mut state = lock(&self.state)?;
        if let Some(existing) = state.indexes.get(&request.index_key) {
            if same_index_version(&existing.version, &snapshot.version)
                && existing.records == snapshot.records
            {
                return Ok(existing.version.clone());
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Index for Playground {} is already initialized with a different snapshot",
                    request.index_key.playground_id
                ),
            ));
        }
        let version = snapshot.version.clone();
        state.indexes.insert(request.index_key, snapshot);
        Ok(version)
    }

    async fn compare_and_swap(
        &self,
        request: IndexPublishRequest,
    ) -> CentralResult<IndexPublishOutcome> {
        if request.job_key.tenant_id != request.index_key.tenant_id {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "Index publication Job and Index tenants differ",
            ));
        }
        let mut state = lock(&self.state)?;
        if let Some((existing_request, outcome)) = state.publications.get(&request.job_key) {
            if existing_request == &request {
                return Ok(outcome.clone());
            }
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "job {} attempted a different Index publication",
                    request.job_key.job_id
                ),
            ));
        }

        let current = state
            .indexes
            .get(&request.index_key)
            .cloned()
            .unwrap_or(empty_snapshot()?);
        if !same_index_version(&current.version, &request.expected_index_version) {
            let outcome = IndexPublishOutcome::Conflict(current.version);
            state
                .publications
                .insert(request.job_key.clone(), (request, outcome.clone()));
            return Ok(outcome);
        }

        let mut manifests = BTreeMap::new();
        for manifest in &request.manifests {
            let manifest_id = match manifest.canonical_id() {
                Ok(manifest_id) => manifest_id,
                Err(error) => {
                    return Ok(Self::reject(
                        &mut state,
                        request,
                        IndexPublishRejection::InvalidMetadata {
                            message: format!("invalid publication Manifest: {error}"),
                        },
                    ));
                }
            };
            if manifests.insert(manifest_id, manifest.clone()).is_some() {
                return Ok(Self::reject(
                    &mut state,
                    request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!("publication repeats Manifest {manifest_id}"),
                    },
                ));
            }
        }
        let mut referenced_manifests = BTreeSet::new();
        let mut manifest_metadata_error = None;
        for mutation in &request.mutations {
            let neoengram_domain::protocol::IndexDeltaRecord::Upsert {
                manifest_id,
                total_size,
                chunk_count,
                ..
            } = mutation
            else {
                continue;
            };
            referenced_manifests.insert(*manifest_id);
            if let Some(manifest) = manifests.get(manifest_id) {
                match manifest.chunk_count() {
                    Ok(observed_chunk_count)
                        if manifest.total_size == total_size.get()
                            && observed_chunk_count == chunk_count.get() => {}
                    Ok(_) => {
                        manifest_metadata_error = Some(format!(
                            "Index upsert metadata differs from Manifest {manifest_id}"
                        ));
                        break;
                    }
                    Err(error) => {
                        manifest_metadata_error =
                            Some(format!("invalid publication Manifest: {error}"));
                        break;
                    }
                }
            }
        }
        if let Some(message) = manifest_metadata_error {
            return Ok(Self::reject(
                &mut state,
                request,
                IndexPublishRejection::InvalidMetadata { message },
            ));
        }
        if manifests.keys().copied().collect::<BTreeSet<_>>() != referenced_manifests {
            return Ok(Self::reject(
                &mut state,
                request,
                IndexPublishRejection::InvalidMetadata {
                    message: "publication Manifests are not the exact Index upsert set".to_owned(),
                },
            ));
        }
        for (manifest_id, manifest) in &manifests {
            let key = (
                request.index_key.tenant_id.clone(),
                request.index_key.artifact_id.clone(),
                *manifest_id,
            );
            if state
                .manifests
                .get(&key)
                .is_some_and(|existing| existing != manifest)
            {
                return Ok(Self::reject(
                    &mut state,
                    request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!(
                            "immutable catalog already contains different content for Manifest {manifest_id}"
                        ),
                    },
                ));
            }
        }

        let mut records = current.records;
        for mutation in &request.mutations {
            match mutation {
                neoengram_domain::protocol::IndexDeltaRecord::Upsert {
                    path,
                    manifest_id,
                    total_size,
                    chunk_count,
                    ..
                } => {
                    let record = match FileRecord::new(
                        path.clone(),
                        *manifest_id,
                        total_size.get(),
                        chunk_count.get(),
                    ) {
                        Ok(record) => record,
                        Err(error) => {
                            return Ok(Self::reject(
                                &mut state,
                                request,
                                IndexPublishRejection::InvalidMetadata {
                                    message: format!("invalid Index upsert: {error}"),
                                },
                            ));
                        }
                    };
                    records.insert(path.clone(), record);
                }
                neoengram_domain::protocol::IndexDeltaRecord::Delete { path, .. } => {
                    records.remove(path);
                }
            }
        }
        let Some(revision) = current.version.revision.get().checked_add(1) else {
            return Ok(Self::reject(
                &mut state,
                request,
                IndexPublishRejection::RevisionExhausted,
            ));
        };
        let ordered = records.values().cloned().collect::<Vec<_>>();
        let version = match IndexVersion::from_snapshot(revision, &ordered) {
            Ok(version) => WireIndexVersion::from(version),
            Err(error) => {
                return Ok(Self::reject(
                    &mut state,
                    request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!("published Index is invalid: {error}"),
                    },
                ));
            }
        };
        if version.digest != request.expected_result_digest {
            let rejection = IndexPublishRejection::ResultDigestMismatch {
                expected_digest: request.expected_result_digest,
                observed_digest: version.digest,
            };
            return Ok(Self::reject(&mut state, request, rejection));
        }
        let outcome = IndexPublishOutcome::Published(version.clone());
        state.indexes.insert(
            request.index_key.clone(),
            IndexSnapshot { version, records },
        );
        for (manifest_id, manifest) in manifests {
            state
                .manifests
                .entry((
                    request.index_key.tenant_id.clone(),
                    request.index_key.artifact_id.clone(),
                    manifest_id,
                ))
                .or_insert(manifest);
        }
        state
            .publications
            .insert(request.job_key.clone(), (request, outcome.clone()));
        Ok(outcome)
    }

    async fn current_version(&self, key: &IndexKey) -> CentralResult<WireIndexVersion> {
        Ok(lock(&self.state)?
            .indexes
            .get(key)
            .map(|snapshot| snapshot.version.clone())
            .unwrap_or(empty_snapshot()?.version))
    }

    async fn published_index(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        self.snapshot(key)
    }

    async fn manifest(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        manifest_id: ManifestId,
    ) -> CentralResult<Option<Manifest>> {
        InMemoryIndexPublisher::manifest(self, tenant_id, artifact_id, manifest_id)
    }
}

type AuthorityLifecycleKey = (
    TenantId,
    neoengram_domain::protocol::DeletionId,
    String,
    &'static str,
);

/// In-process implementation of the Authority-owned lifecycle Saga boundary.
///
/// The component shares the exact backing stores exposed through `InMemoryComponents`, so tests
/// exercise the same metadata effects as the SQLite implementation.
#[derive(Debug)]
pub struct InMemoryAuthorityLifecycle {
    jobs: Arc<InMemoryJobRepository>,
    outbox: Arc<InMemoryAssignmentOutbox>,
    metadata: Arc<InMemoryMetadataBatchStager>,
    objects: Arc<InMemoryObjectCatalog>,
    publisher: Arc<InMemoryIndexPublisher>,
    precommits: Arc<InMemoryPreCommitRepository>,
    records: Mutex<BTreeMap<AuthorityLifecycleKey, AuthorityLifecycleRecord>>,
}

impl InMemoryAuthorityLifecycle {
    #[must_use]
    pub fn new(
        jobs: Arc<InMemoryJobRepository>,
        outbox: Arc<InMemoryAssignmentOutbox>,
        metadata: Arc<InMemoryMetadataBatchStager>,
        objects: Arc<InMemoryObjectCatalog>,
        publisher: Arc<InMemoryIndexPublisher>,
        precommits: Arc<InMemoryPreCommitRepository>,
    ) -> Self {
        Self {
            jobs,
            outbox,
            metadata,
            objects,
            publisher,
            precommits,
            records: Mutex::new(BTreeMap::new()),
        }
    }

    fn apply(
        &self,
        request: AuthorityLifecycleRequest,
        action: AuthorityLifecycleAction,
    ) -> CentralResult<AuthorityLifecycleMutationOutcome> {
        let key = authority_lifecycle_key(&request, action);
        let mut records = lock(&self.records)?;
        if let Some(existing) = records.get(&key) {
            let expected = AuthorityLifecycleRecord {
                tenant_id: request.tenant_id,
                deletion_id: request.deletion_id,
                target: request.target,
                action,
                lifecycle_generation: request.lifecycle_generation,
                request_digest: request.request_digest,
                completed_at_unix_ms: existing.completed_at_unix_ms,
            };
            if existing != &expected {
                return Err(authority_lifecycle_conflict(
                    "Authority lifecycle identity is already bound to another request",
                ));
            }
            return Ok(AuthorityLifecycleMutationOutcome {
                record: existing.clone(),
                replayed: true,
            });
        }

        match action {
            AuthorityLifecycleAction::Quiesce => self.quiesce_metadata(&request)?,
            AuthorityLifecycleAction::Finalize => self.finalize_metadata(&request)?,
        }
        let record = AuthorityLifecycleRecord {
            tenant_id: request.tenant_id,
            deletion_id: request.deletion_id,
            target: request.target,
            action,
            lifecycle_generation: request.lifecycle_generation,
            request_digest: request.request_digest,
            completed_at_unix_ms: request.occurred_at_unix_ms,
        };
        records.insert(key, record.clone());
        Ok(AuthorityLifecycleMutationOutcome {
            record,
            replayed: false,
        })
    }

    fn quiesce_metadata(&self, request: &AuthorityLifecycleRequest) -> CentralResult<()> {
        let mut jobs = lock(&self.jobs.jobs)?;
        let matching_keys = jobs
            .iter()
            .filter(|(_, job)| {
                job.spec.tenant_id == request.tenant_id
                    && authority_job_matches_target(job, &request.target)
                    && !job.state.is_terminal()
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in &matching_keys {
            let job = jobs.get(key).expect("matching Job remains locked");
            if job.resource_version.get() == u64::MAX {
                return Err(authority_lifecycle_conflict(
                    "Job resource version exhausted during lifecycle quiesce",
                ));
            }
        }

        let mut assignments = lock(&self.outbox.assignments)?;
        let mut precommits = lock(&self.precommits.state)?;
        let matching_precommits = precommits
            .precommits
            .iter()
            .filter(|(_, record)| {
                record.tenant_id == request.tenant_id
                    && authority_precommit_matches_target(record, &request.target)
                    && matches!(
                        record.state,
                        PreCommitState::Running | PreCommitState::Ready | PreCommitState::Abnormal
                    )
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in &matching_precommits {
            let record = precommits
                .precommits
                .get(key)
                .expect("matching Pre-commit remains locked");
            if record.resource_version.get() == u64::MAX {
                return Err(authority_lifecycle_conflict(
                    "Pre-commit resource version exhausted during lifecycle quiesce",
                ));
            }
        }

        for key in &matching_keys {
            let job = jobs.get_mut(key).expect("matching Job remains locked");
            job.resource_version = ResourceVersion::new(job.resource_version.get() + 1);
            job.state = JobState::Cancelled;
        }
        let matching_job_ids = matching_keys
            .iter()
            .map(|key| key.job_id.clone())
            .collect::<BTreeSet<_>>();
        for reservation in assignments.values_mut() {
            let (tenant_id, job_id, _, _) = assignment_identity(&reservation.assignment);
            if tenant_id == &request.tenant_id && matching_job_ids.contains(job_id) {
                reservation.published = true;
                reservation.retired = true;
            }
        }
        for key in matching_precommits {
            let record = precommits
                .precommits
                .get_mut(&key)
                .expect("matching Pre-commit remains locked");
            record.resource_version = ResourceVersion::new(record.resource_version.get() + 1);
            record.state = PreCommitState::Cancelled;
            record.phase = PreCommitPhase::Idle;
            record.updated_at_unix_ms = request.occurred_at_unix_ms;
        }
        Ok(())
    }

    fn finalize_metadata(&self, request: &AuthorityLifecycleRequest) -> CentralResult<()> {
        if let ResourceRef::StorageVolume { storage_volume_id } = &request.target {
            let placements = lock(&self.objects.placements)?;
            let blockers = in_memory_volume_unique_artifact_replicas(
                &placements,
                &request.tenant_id,
                storage_volume_id,
            );
            if !blockers.is_empty() {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    format!(
                        "StorageVolume contains unique object replicas for retained Artifacts: {}",
                        blockers
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                )
                .with_retryable(false));
            }
        }

        let mut jobs = lock(&self.jobs.jobs)?;
        let matching_keys = jobs
            .iter()
            .filter(|(_, job)| {
                job.spec.tenant_id == request.tenant_id
                    && authority_job_matches_target(job, &request.target)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let matching_job_ids = matching_keys
            .iter()
            .map(|key| key.job_id.clone())
            .collect::<BTreeSet<_>>();
        for key in matching_keys {
            jobs.remove(&key);
        }
        lock(&self.outbox.assignments)?.retain(|_, reservation| {
            let (tenant_id, job_id, _, _) = assignment_identity(&reservation.assignment);
            tenant_id != &request.tenant_id || !matching_job_ids.contains(job_id)
        });
        lock(&self.metadata.batches)?.retain(|_, batch| {
            batch.descriptor.scope.tenant_id != request.tenant_id
                || !matching_job_ids.contains(&batch.descriptor.scope.job_id)
        });
        lock(&self.publisher.state)?
            .publications
            .retain(|job_key, _| {
                job_key.tenant_id != request.tenant_id
                    || !matching_job_ids.contains(&job_key.job_id)
            });

        match &request.target {
            ResourceRef::Snapshot { .. } => {}
            ResourceRef::Playground {
                project_id,
                artifact_id,
                playground_id,
            } => {
                let mut state = lock(&self.precommits.state)?;
                let removed = state
                    .precommits
                    .iter()
                    .filter(|(_, record)| {
                        record.tenant_id == request.tenant_id
                            && &record.project_id == project_id
                            && &record.artifact_id == artifact_id
                            && &record.playground_id == playground_id
                            && record.state != PreCommitState::Committed
                    })
                    .map(|(key, _)| key.clone())
                    .collect::<BTreeSet<_>>();
                state.precommits.retain(|key, _| !removed.contains(key));
                state.mutations.retain(|_, mutation| {
                    !authority_mutation_matches_precommits(mutation, &removed)
                });
                lock(&self.publisher.state)?.indexes.remove(&IndexKey {
                    tenant_id: request.tenant_id.clone(),
                    project_id: project_id.clone(),
                    artifact_id: artifact_id.clone(),
                    playground_id: playground_id.clone(),
                });
            }
            ResourceRef::Artifact {
                project_id,
                artifact_id,
            } => {
                let mut state = lock(&self.precommits.state)?;
                let removed = state
                    .precommits
                    .iter()
                    .filter(|(_, record)| {
                        record.tenant_id == request.tenant_id
                            && &record.project_id == project_id
                            && &record.artifact_id == artifact_id
                    })
                    .map(|(key, _)| key.clone())
                    .collect::<BTreeSet<_>>();
                state.precommits.retain(|key, _| !removed.contains(key));
                state.mutations.retain(|_, mutation| {
                    !authority_mutation_matches_precommits(mutation, &removed)
                });
                state
                    .commits
                    .retain(|(tenant_id, commit_project_id, commit_artifact_id, _), _| {
                        tenant_id != &request.tenant_id
                            || commit_project_id != project_id
                            || commit_artifact_id != artifact_id
                    });

                let mut publisher = lock(&self.publisher.state)?;
                publisher.indexes.retain(|key, _| {
                    key.tenant_id != request.tenant_id
                        || &key.project_id != project_id
                        || &key.artifact_id != artifact_id
                });
                publisher
                    .manifests
                    .retain(|(tenant_id, stored_artifact_id, _), _| {
                        tenant_id != &request.tenant_id || stored_artifact_id != artifact_id
                    });
                publisher.publications.retain(|_, (publication, _)| {
                    publication.index_key.tenant_id != request.tenant_id
                        || &publication.index_key.project_id != project_id
                        || &publication.index_key.artifact_id != artifact_id
                });
                lock(&self.objects.placements)?.retain(|_, evidence| {
                    evidence.receipt.tenant_id != request.tenant_id
                        || &evidence.receipt.artifact_id != artifact_id
                });
            }
            ResourceRef::StorageVolume { storage_volume_id } => {
                lock(&self.objects.placements)?.retain(|_, evidence| {
                    evidence.receipt.tenant_id != request.tenant_id
                        || &evidence.receipt.storage_volume_id != storage_volume_id
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AuthorityLifecycleRepository for InMemoryAuthorityLifecycle {
    async fn impact(
        &self,
        tenant_id: &TenantId,
        target: &ResourceRef,
    ) -> CentralResult<AuthorityLifecycleImpact> {
        let active_job_count = lock(&self.jobs.jobs)?
            .values()
            .filter(|job| {
                &job.spec.tenant_id == tenant_id
                    && authority_job_matches_target(job, target)
                    && !job.state.is_terminal()
            })
            .count();
        let placements = lock(&self.objects.placements)?;
        let mut objects = BTreeSet::<Vec<u8>>::new();
        let mut bytes = 0_u64;
        for evidence in placements.values().filter(|evidence| {
            let receipt = &evidence.receipt;
            if &receipt.tenant_id != tenant_id {
                return false;
            }
            match target {
                ResourceRef::StorageVolume { storage_volume_id } => {
                    &receipt.storage_volume_id == storage_volume_id
                }
                ResourceRef::Artifact { artifact_id, .. } => &receipt.artifact_id == artifact_id,
                ResourceRef::Playground { .. } | ResourceRef::Snapshot { .. } => false,
            }
        }) {
            if objects.insert(evidence.receipt.object_id.as_bytes().to_vec()) {
                bytes = bytes
                    .checked_add(evidence.receipt.size.get())
                    .ok_or_else(|| {
                        authority_lifecycle_conflict("lifecycle byte estimate overflow")
                    })?;
            }
        }
        Ok(AuthorityLifecycleImpact {
            active_job_count: DecimalU64::new(
                u64::try_from(active_job_count)
                    .map_err(|_| authority_lifecycle_conflict("active Job count overflow"))?,
            ),
            estimated_file_count: DecimalU64::new(
                u64::try_from(objects.len())
                    .map_err(|_| authority_lifecycle_conflict("file estimate overflow"))?,
            ),
            estimated_bytes: DecimalU64::new(bytes),
        })
    }

    async fn quiesce(
        &self,
        request: AuthorityLifecycleRequest,
    ) -> CentralResult<AuthorityLifecycleMutationOutcome> {
        self.apply(request, AuthorityLifecycleAction::Quiesce)
    }

    async fn finalize(
        &self,
        request: AuthorityLifecycleRequest,
    ) -> CentralResult<AuthorityLifecycleMutationOutcome> {
        self.apply(request, AuthorityLifecycleAction::Finalize)
    }

    async fn get(
        &self,
        tenant_id: &TenantId,
        deletion_id: &neoengram_domain::protocol::DeletionId,
        target: &ResourceRef,
        action: AuthorityLifecycleAction,
    ) -> CentralResult<Option<AuthorityLifecycleRecord>> {
        Ok(lock(&self.records)?
            .get(&(
                tenant_id.clone(),
                deletion_id.clone(),
                authority_target_id(target),
                authority_action_name(action),
            ))
            .cloned())
    }
}

fn authority_lifecycle_key(
    request: &AuthorityLifecycleRequest,
    action: AuthorityLifecycleAction,
) -> AuthorityLifecycleKey {
    (
        request.tenant_id.clone(),
        request.deletion_id.clone(),
        authority_target_id(&request.target),
        authority_action_name(action),
    )
}

fn authority_target_id(target: &ResourceRef) -> String {
    match target {
        ResourceRef::StorageVolume { storage_volume_id } => storage_volume_id.to_string(),
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => format!("{project_id}/{artifact_id}"),
        ResourceRef::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => format!("{project_id}/{artifact_id}/{playground_id}"),
        ResourceRef::Snapshot { snapshot_id } => snapshot_id.to_string(),
    }
}

const fn authority_action_name(action: AuthorityLifecycleAction) -> &'static str {
    match action {
        AuthorityLifecycleAction::Quiesce => "quiesce",
        AuthorityLifecycleAction::Finalize => "finalize",
    }
}

fn authority_job_matches_target(job: &JobRecord, target: &ResourceRef) -> bool {
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

fn authority_precommit_matches_target(record: &PreCommitRecord, target: &ResourceRef) -> bool {
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

fn authority_mutation_matches_precommits(
    mutation: &InMemoryPreCommitMutation,
    keys: &BTreeSet<PreCommitKey>,
) -> bool {
    match mutation {
        InMemoryPreCommitMutation::Start { result, .. }
        | InMemoryPreCommitMutation::Restart { result, .. }
        | InMemoryPreCommitMutation::Cancel { result, .. } => keys.contains(&result.key()),
        InMemoryPreCommitMutation::Commit { result, .. } => {
            keys.contains(&result.consumed_precommit.key())
        }
    }
}

fn in_memory_volume_unique_artifact_replicas(
    placements: &BTreeMap<PlacementReceiptKey, ObjectPlacementEvidence>,
    tenant_id: &TenantId,
    storage_volume_id: &StorageVolumeId,
) -> BTreeSet<ArtifactId> {
    placements
        .values()
        .filter(|candidate| {
            &candidate.receipt.tenant_id == tenant_id
                && &candidate.receipt.storage_volume_id == storage_volume_id
                && !placements.values().any(|other| {
                    other.receipt.tenant_id == candidate.receipt.tenant_id
                        && other.receipt.artifact_id == candidate.receipt.artifact_id
                        && other.receipt.object_id == candidate.receipt.object_id
                        && other.receipt.size == candidate.receipt.size
                        && other.receipt.storage_volume_id != *storage_volume_id
                })
        })
        .map(|evidence| evidence.receipt.artifact_id.clone())
        .collect()
}

fn authority_lifecycle_conflict(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::ConcurrentUpdate, message).with_retryable(false)
}

#[derive(Debug, Default)]
pub struct InMemoryAuditSink {
    events: Mutex<BTreeMap<(TenantId, String), AuditEvent>>,
    enrollment_events: Mutex<BTreeMap<(TenantId, String), AgentEnrollmentAuditEvent>>,
}

impl InMemoryAuditSink {
    pub fn events(&self) -> CentralResult<Vec<AuditEvent>> {
        Ok(lock(&self.events)?.values().cloned().collect())
    }

    pub fn enrollment_events(&self) -> CentralResult<Vec<AgentEnrollmentAuditEvent>> {
        Ok(lock(&self.enrollment_events)?.values().cloned().collect())
    }
}

#[async_trait]
impl AuditSink for InMemoryAuditSink {
    async fn record(&self, event: AuditEvent) -> CentralResult<bool> {
        let mut events = lock(&self.events)?;
        let key = (event.job_key.tenant_id.clone(), event.event_id.clone());
        if let Some(existing) = events.get(&key) {
            if existing.kind == event.kind
                && existing.job_key == event.job_key
                && existing.state == event.state
            {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::Internal,
                format!("audit event ID {} was reused", event.event_id),
            ));
        }
        events.insert(key, event);
        Ok(false)
    }

    async fn record_enrollment_decision(
        &self,
        event: AgentEnrollmentAuditEvent,
    ) -> CentralResult<bool> {
        let mut events = lock(&self.enrollment_events)?;
        let key = (event.tenant_id.clone(), event.event_id.clone());
        if let Some(existing) = events.get(&key) {
            if existing.kind == event.kind
                && existing.enrollment_id == event.enrollment_id
                && existing.storage_volume_id == event.storage_volume_id
                && existing.decision_request_id == event.decision_request_id
                && existing.resource_version == event.resource_version
                && existing.actor == event.actor
            {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::Internal,
                format!("enrollment audit event ID {} was reused", event.event_id),
            ));
        }
        events.insert(key, event);
        Ok(false)
    }
}

#[derive(Debug)]
pub struct InMemoryClock {
    now_ms: AtomicU64,
}

impl InMemoryClock {
    #[must_use]
    pub const fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    pub fn set(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }

    pub fn advance(&self, delta_ms: u64) -> CentralResult<u64> {
        self.now_ms
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(delta_ms)
            })
            .map(|previous| previous + delta_ms)
            .map_err(|_| invalid(CentralErrorCode::Internal, "clock overflow"))
    }
}

impl Default for InMemoryClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Clock for InMemoryClock {
    fn now(&self) -> UnixMillis {
        UnixMillis::new(self.now_ms.load(Ordering::SeqCst))
    }
}

/// Convenient composition root for deterministic component and state-machine tests.
pub struct InMemoryComponents {
    pub authorizer: Arc<AllowAllAuthorizer>,
    pub jobs: Arc<InMemoryJobRepository>,
    pub tasks: Arc<InMemoryTaskRepository>,
    pub outbox: Arc<InMemoryAssignmentOutbox>,
    pub metadata: Arc<InMemoryMetadataBatchStager>,
    pub objects: Arc<InMemoryObjectCatalog>,
    pub publisher: Arc<InMemoryIndexPublisher>,
    pub audit: Arc<InMemoryAuditSink>,
    pub precommits: Arc<InMemoryPreCommitRepository>,
    pub authority_lifecycle: Arc<InMemoryAuthorityLifecycle>,
    pub agent_registry: Arc<InMemoryAgentRegistry>,
    pub gateway_registry: Arc<crate::InMemoryGatewayRegistry>,
    pub control_catalog: Arc<crate::InMemoryControlCatalog>,
    pub placement: Arc<InMemoryPlacementRepository>,
    pub clock: Arc<InMemoryClock>,
}

impl InMemoryComponents {
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        let agent_registry = Arc::new(InMemoryAgentRegistry::new());
        let gateway_registry = Arc::new(crate::InMemoryGatewayRegistry::with_agent_registry(
            agent_registry.clone(),
        ));
        let jobs = Arc::new(InMemoryJobRepository::default());
        let tasks = Arc::new(InMemoryTaskRepository::default());
        let outbox = Arc::new(InMemoryAssignmentOutbox::default());
        let metadata = Arc::new(InMemoryMetadataBatchStager::default());
        let objects = Arc::new(InMemoryObjectCatalog::default());
        let publisher = Arc::new(InMemoryIndexPublisher::default());
        let precommits = Arc::new(InMemoryPreCommitRepository::default());
        let authority_lifecycle = Arc::new(InMemoryAuthorityLifecycle::new(
            jobs.clone(),
            outbox.clone(),
            metadata.clone(),
            objects.clone(),
            publisher.clone(),
            precommits.clone(),
        ));
        let placement = Arc::new(InMemoryPlacementRepository::default());
        Self {
            authorizer: Arc::new(AllowAllAuthorizer),
            jobs,
            tasks,
            outbox,
            metadata,
            objects,
            publisher,
            audit: Arc::new(InMemoryAuditSink::default()),
            precommits,
            authority_lifecycle,
            agent_registry,
            gateway_registry,
            control_catalog: Arc::new(crate::InMemoryControlCatalog::default()),
            placement,
            clock: Arc::new(InMemoryClock::new(now_ms)),
        }
    }

    #[must_use]
    pub fn control_plane(&self) -> ControlPlane {
        ControlPlane::new(
            self.authorizer.clone(),
            self.authority_store(),
            self.clock.clone(),
        )
        .with_task_coordinator(Arc::new(crate::service::TaskCoordinator::new(
            self.tasks.clone(),
            self.clock.clone(),
        )))
        .with_placement_repository(self.placement.clone())
    }

    #[must_use]
    pub fn authority_store(&self) -> AuthorityStore {
        AuthorityStore::from_parts(
            self.jobs.clone(),
            self.outbox.clone(),
            self.metadata.clone(),
            self.objects.clone(),
            self.publisher.clone(),
            self.audit.clone(),
            AuthorityCapabilities::IN_MEMORY,
        )
        .with_tasks(self.tasks.clone())
        .with_precommits(self.precommits.clone())
        .with_agent_registry(self.agent_registry.clone())
        .with_gateway_registry(self.gateway_registry.clone())
        .with_control_catalog(self.control_catalog.clone())
        .with_placement(self.placement.clone())
        .with_authority_lifecycle(self.authority_lifecycle.clone())
    }
}

fn empty_snapshot() -> CentralResult<IndexSnapshot> {
    let version = WireIndexVersion::from(IndexVersion::from_snapshot(0, &[]).map_err(|error| {
        invalid(
            CentralErrorCode::Internal,
            format!("failed to construct empty Index: {error}"),
        )
    })?);
    Ok(IndexSnapshot {
        version,
        records: BTreeMap::new(),
    })
}

fn lock<T>(mutex: &Mutex<T>) -> CentralResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        invalid(
            CentralErrorCode::Internal,
            "in-memory adapter lock poisoned",
        )
    })
}

fn route_binding_from_record(
    record: &ReplicationRecord,
    source: bool,
) -> CentralResult<ReplicationRouteBinding> {
    let (
        edge_cluster_id,
        gateway_pool_id,
        agent_id,
        session_generation,
        mount_generation,
        route_generation,
    ) = if source {
        (
            record.source_edge_cluster_id.clone(),
            record.source_gateway_pool_id.clone(),
            record.source_agent_id.clone(),
            record.source_session_generation,
            record.source_mount_generation,
            record.source_route_generation,
        )
    } else {
        (
            record.target_edge_cluster_id.clone(),
            record.target_gateway_pool_id.clone(),
            record.target_agent_id.clone(),
            record.target_session_generation,
            record.target_mount_generation,
            record.target_route_generation,
        )
    };
    Ok(ReplicationRouteBinding {
        edge_cluster_id: edge_cluster_id.ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "replication route binding is incomplete",
            )
        })?,
        gateway_pool_id: gateway_pool_id.ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "replication route binding is incomplete",
            )
        })?,
        agent_id: agent_id.ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "replication route binding is incomplete",
            )
        })?,
        session_generation: session_generation.ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "replication route binding is incomplete",
            )
        })?,
        mount_generation: mount_generation.ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "replication route binding is incomplete",
            )
        })?,
        route_generation: route_generation.ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "replication route binding is incomplete",
            )
        })?,
    })
}
