use std::sync::{Arc, Mutex};

use neoengram_core::{CommitId, IndexVersion};
use neoengram_protocol::{
    AddOperation, AgentId, AgentMountId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    Extensions, JobId, JobState, MountGeneration, OwnerGeneration, PlacementGeneration,
    PrincipalId, PrincipalKind, PrincipalRef, SnapshotMountAssignment, SnapshotMountOperation,
    UnixMillis, WorkspaceMaterializeAssignment, WorkspaceMaterializeOperation,
};
use neoengramd::{
    AddJobSpec, AdvancePlaygroundCommitRequest, AgentRegistryRepository, AssignJobRequest,
    AssignSnapshotMountRequest, AssignWorkspaceMaterializationRequest, AssignmentTarget,
    CentralError, CentralErrorCode, CentralResult, Clock, ControlCatalogRepository, ControlPlane,
    CreateSnapshotMountRequest, CreateWorkspaceMaterializationRequest, ExpireAddJobRequest,
    IndexKey, IndexPublisher, InitializeIndexSnapshotRequest, JobKey, JobOperation, JobRecord,
    JobRepository, PlaygroundListRequest, PlaygroundRecord, PlaygroundState, PreCommitKey,
    PreCommitRecord, PreCommitRepository, PreCommitState, ResumePublicationRequest,
    SnapshotListRequest, SnapshotMountSpec, SnapshotMountTarget, SnapshotPhase, SnapshotRecord,
    SnapshotState, TenantListRequest, WorkspaceMaterializeSpec, WorkspaceMaterializeTarget,
};

const DEFAULT_RECOVERY_BATCH_SIZE: usize = 1_024;
const WORKSPACE_MATERIALIZE_DEADLINE_MS: u64 = 24 * 60 * 60 * 1_000;
const PRECOMMIT_JOB_DEADLINE_MS: u64 = 24 * 60 * 60 * 1_000;
const SNAPSHOT_RECOVERY_BACKOFF_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoordinatorRun {
    pub examined: usize,
    pub assigned: usize,
    pub expired: usize,
    pub finalized: usize,
}

/// Single-process development scheduler and publication recovery loop.
pub struct JobCoordinator {
    control: Arc<ControlPlane>,
    jobs: Arc<dyn JobRepository>,
    catalog: Arc<dyn ControlCatalogRepository>,
    registry: Arc<dyn AgentRegistryRepository>,
    indexes: Arc<dyn IndexPublisher>,
    precommits: Option<Arc<dyn PreCommitRepository>>,
    clock: Arc<dyn Clock>,
    heartbeat_timeout_ms: u64,
    recovery_cursor: Mutex<Option<JobKey>>,
    precommit_recovery_cursor: Mutex<Option<PreCommitKey>>,
    commit_recovery_cursor: Mutex<Option<PreCommitKey>>,
}

impl JobCoordinator {
    pub fn from_authority(
        control: Arc<ControlPlane>,
        authority: &neoengramd::AuthorityStore,
        clock: Arc<dyn Clock>,
        heartbeat_timeout_ms: u64,
    ) -> CentralResult<Self> {
        let catalog = authority.control_catalog().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no control catalog composition",
            )
        })?;
        let registry = authority.agent_registry().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no Agent registry composition",
            )
        })?;
        Ok(Self {
            control,
            jobs: authority.jobs(),
            catalog,
            registry,
            indexes: authority.publisher(),
            precommits: authority.precommits(),
            clock,
            heartbeat_timeout_ms,
            recovery_cursor: Mutex::new(None),
            precommit_recovery_cursor: Mutex::new(None),
            commit_recovery_cursor: Mutex::new(None),
        })
    }

    /// Rejects fabricated Playground scope and stale client Index defaults before Job creation.
    pub async fn validate_spec(&self, spec: &neoengramd::AddJobSpec) -> CentralResult<()> {
        validate_job_spec(
            self.jobs.as_ref(),
            self.catalog.as_ref(),
            self.indexes.as_ref(),
            spec,
        )
        .await
    }

    /// Performs one immediate scheduling attempt. Lack of a Ready owner leaves the Job queued.
    pub async fn schedule(&self, job: &JobRecord) -> CentralResult<Option<JobRecord>> {
        if job.operation == JobOperation::SnapshotMount {
            return self.schedule_snapshot_mount(job).await;
        }
        if job.state != JobState::Queued {
            return Ok(None);
        }
        if job.operation == JobOperation::WorkspaceMaterialize {
            return self.schedule_workspace_materialization(job).await;
        }
        let playground = self
            .catalog
            .get_playground(
                &job.spec.tenant_id,
                &job.spec.project_id,
                &job.spec.artifact_id,
                &job.spec.playground_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "the Job's Playground no longer exists",
                )
            })?;
        let current = self.indexes.current_version(&job.index_key()).await?;
        if !same_index_version(&current, &job.spec.expected_index_version) {
            return Err(CentralError::new(
                CentralErrorCode::MetadataInvalid,
                "the Job's expected IndexVersion no longer matches its Playground",
            ));
        }
        let Some(owner) = self
            .registry
            .get_current_by_volume(&job.spec.tenant_id, &playground.storage_volume_id)
            .await?
        else {
            return Ok(None);
        };
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != neoengramd::DerivedVolumeState::Ready
        {
            return Ok(None);
        }
        let Some(instance) = owner.instance.as_ref() else {
            return Ok(None);
        };
        if owner.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
            || owner.owner.active_agent_mount_id.as_ref() != Some(&owner.mount.agent_mount_id)
        {
            return Ok(None);
        }
        let target = AssignmentTarget {
            assignment_id: deterministic_assignment_id(job)?,
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: instance.agent_id.clone(),
            edge_cluster_id: owner.enrollment.edge_cluster_id.clone(),
            storage_volume_id: playground.storage_volume_id,
            artifact_placement_id: deterministic_placement_id(job, &owner.mount.storage_volume_id)?,
            placement_generation: PlacementGeneration::new(1),
            agent_mount_id: owner.mount.agent_mount_id.clone(),
            mount_generation: owner.mount.mount_generation,
            owner_generation: owner.owner.owner_generation,
            lease: None,
        };
        let result = self
            .control
            .assign_job(AssignJobRequest {
                actor: job.spec.principal.clone(),
                tenant_id: job.spec.tenant_id.clone(),
                job_id: job.spec.job_id.clone(),
                target,
            })
            .await?;
        Ok(Some(result.job))
    }

    /// Idempotently creates the durable materialization Job for one Creating Playground and
    /// performs its first owner-selection attempt.
    pub async fn ensure_workspace_materialization(
        &self,
        playground: &PlaygroundRecord,
    ) -> CentralResult<JobRecord> {
        if playground.state != PlaygroundState::Creating {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "only a Creating Playground can be materialized",
            ));
        }
        let job_id = deterministic_materialization_job_id(playground)?;
        let relative_root = WorkspaceMaterializeAssignment::canonical_relative_root(
            &playground.project_id,
            &playground.artifact_id,
            &playground.playground_id,
        )?;
        if relative_root.as_str() != playground.relative_root {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Playground relative_root differs from the canonical materialization path",
            ));
        }
        let index_key = IndexKey {
            tenant_id: playground.tenant_id.clone(),
            project_id: playground.project_id.clone(),
            artifact_id: playground.artifact_id.clone(),
            playground_id: playground.playground_id.clone(),
        };
        let base_index_version = if let Some(base_commit_id) = playground.base_commit_id {
            let precommits = self.precommits.as_ref().ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Commit authority is unavailable for Workspace materialization",
                )
            })?;
            let commit = precommits
                .get_commit(
                    &playground.tenant_id,
                    &playground.project_id,
                    &playground.artifact_id,
                    CommitId::from_digest(base_commit_id),
                )
                .await?
                .ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::MetadataInvalid,
                        "the Playground base Commit is missing from authority",
                    )
                })?;
            let initialized = self
                .indexes
                .initialize_snapshot(InitializeIndexSnapshotRequest {
                    index_key,
                    version: commit.index_version.clone(),
                    records: commit.records,
                })
                .await?;
            Some(initialized)
        } else {
            let empty = IndexVersion::from_snapshot(0, &[]).map_err(|error| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    format!("cannot construct the empty Playground Index: {error}"),
                )
            })?;
            self.indexes
                .initialize_snapshot(InitializeIndexSnapshotRequest {
                    index_key,
                    version: empty.into(),
                    records: Vec::new(),
                })
                .await?;
            None
        };
        let deadline_unix_ms = UnixMillis::new(
            playground
                .created_at_unix_ms
                .get()
                .saturating_add(WORKSPACE_MATERIALIZE_DEADLINE_MS),
        );
        let principal = PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("workspace-materializer")?,
            extensions: Extensions::new(),
        };
        let operation = WorkspaceMaterializeOperation {
            job_id: job_id.clone(),
            principal: principal.clone(),
            tenant_id: playground.tenant_id.clone(),
            project_id: playground.project_id.clone(),
            artifact_id: playground.artifact_id.clone(),
            playground_id: playground.playground_id.clone(),
            storage_volume_id: playground.storage_volume_id.clone(),
            relative_root: relative_root.clone(),
            base_commit_id: playground.base_commit_id,
            base_index_version: base_index_version.clone(),
            deadline_unix_ms,
            extensions: Extensions::new(),
        };
        let request_digest = operation.request_digest()?;
        let created = self
            .control
            .create_workspace_materialization(CreateWorkspaceMaterializationRequest {
                spec: WorkspaceMaterializeSpec {
                    job_id,
                    principal,
                    tenant_id: playground.tenant_id.clone(),
                    project_id: playground.project_id.clone(),
                    artifact_id: playground.artifact_id.clone(),
                    playground_id: playground.playground_id.clone(),
                    storage_volume_id: playground.storage_volume_id.clone(),
                    relative_root,
                    base_commit_id: playground.base_commit_id,
                    base_index_version,
                    request_digest,
                    deadline_unix_ms,
                },
            })
            .await?;
        match self.schedule(&created.job).await? {
            Some(assigned) => Ok(assigned),
            None => Ok(created.job),
        }
    }

    /// Idempotently creates and dispatches the durable FUSE mount Job for one Snapshot.
    pub async fn ensure_snapshot_mount(
        &self,
        snapshot: &SnapshotRecord,
    ) -> CentralResult<JobRecord> {
        if !matches!(
            snapshot.state,
            SnapshotState::Creating | SnapshotState::Abnormal
        ) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "only a Creating or Abnormal Snapshot can be mounted",
            ));
        }
        let job_id = deterministic_snapshot_mount_job_id(snapshot)?;
        if let Some(existing) = self
            .jobs
            .get(&JobKey::new(snapshot.tenant_id.clone(), job_id.clone()))
            .await?
        {
            let job = if snapshot.state == SnapshotState::Abnormal {
                self.control
                    .recover_snapshot_mount(&snapshot.tenant_id, &job_id)
                    .await?
            } else {
                existing
            };
            return match self.schedule_snapshot_mount(&job).await? {
                Some(assigned) => Ok(assigned),
                None => Ok(job),
            };
        }
        let precommits = self.precommits.as_ref().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "Commit authority is unavailable for Snapshot mounting",
            )
        })?;
        let commit = precommits
            .get_commit(
                &snapshot.tenant_id,
                &snapshot.project_id,
                &snapshot.artifact_id,
                CommitId::from_digest(snapshot.commit_id),
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::MetadataInvalid,
                    "the Snapshot Commit is missing from authority",
                )
            })?;
        let relative_root = SnapshotMountAssignment::canonical_relative_root(
            &snapshot.project_id,
            &snapshot.artifact_id,
            &snapshot.snapshot_id,
        )?;
        if relative_root.as_str() != snapshot.relative_root {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot relative_root differs from the canonical mount path",
            ));
        }
        // A SnapshotMount is a durable serving responsibility, not a bounded materialization
        // attempt. The protocol currently requires a deadline, so use its maximal value until
        // the wire model can represent an explicitly unbounded lifetime.
        let deadline_unix_ms = UnixMillis::new(u64::MAX);
        let principal = PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("snapshot-mounter")?,
            extensions: Extensions::new(),
        };
        let operation = SnapshotMountOperation {
            job_id: job_id.clone(),
            principal: principal.clone(),
            tenant_id: snapshot.tenant_id.clone(),
            project_id: snapshot.project_id.clone(),
            artifact_id: snapshot.artifact_id.clone(),
            snapshot_id: snapshot.snapshot_id.clone(),
            storage_volume_id: snapshot.storage_volume_id.clone(),
            relative_root: relative_root.clone(),
            commit_id: snapshot.commit_id,
            index_version: commit.index_version.clone(),
            deadline_unix_ms,
            extensions: Extensions::new(),
        };
        let request_digest = operation.request_digest()?;
        let created = self
            .control
            .create_snapshot_mount(CreateSnapshotMountRequest {
                spec: SnapshotMountSpec {
                    job_id,
                    principal,
                    tenant_id: snapshot.tenant_id.clone(),
                    project_id: snapshot.project_id.clone(),
                    artifact_id: snapshot.artifact_id.clone(),
                    snapshot_id: snapshot.snapshot_id.clone(),
                    storage_volume_id: snapshot.storage_volume_id.clone(),
                    relative_root,
                    commit_id: snapshot.commit_id,
                    index_version: commit.index_version,
                    request_digest,
                    deadline_unix_ms,
                },
            })
            .await?;
        match self.schedule_snapshot_mount(&created.job).await? {
            Some(assigned) => Ok(assigned),
            None => Ok(created.job),
        }
    }

    /// Idempotently creates and dispatches the real Add Job for one durable Pre-commit attempt.
    pub async fn ensure_precommit_job(
        &self,
        precommit: &PreCommitRecord,
    ) -> CentralResult<JobRecord> {
        if precommit.state != PreCommitState::Running {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "only a running Pre-commit can ensure its Add Job",
            ));
        }
        let spec = precommit_add_job_spec(precommit)?;
        if self
            .jobs
            .get(&JobKey::new(spec.tenant_id.clone(), spec.job_id.clone()))
            .await?
            .is_none()
        {
            validate_job_spec(
                self.jobs.as_ref(),
                self.catalog.as_ref(),
                self.indexes.as_ref(),
                &spec,
            )
            .await?;
        }
        let created = self.control.create_precommit_add_job(spec).await?;
        let job = match self.schedule(&created.job).await? {
            Some(assigned) => assigned,
            None => created.job,
        };
        let _ = self.sync_precommit_for_job(&job).await?;
        Ok(job)
    }

    /// Synchronizes a public Pre-commit query with its associated authoritative Job.
    pub async fn synchronize_precommit(
        &self,
        precommit: &PreCommitRecord,
    ) -> CentralResult<PreCommitRecord> {
        if precommit.state != PreCommitState::Running {
            return Ok(precommit.clone());
        }
        let Some(job) = self
            .jobs
            .get(&JobKey::new(
                precommit.tenant_id.clone(),
                precommit.job_id.clone(),
            ))
            .await?
        else {
            return Ok(precommit.clone());
        };
        Ok(self
            .sync_precommit_for_job(&job)
            .await?
            .unwrap_or_else(|| precommit.clone()))
    }

    async fn schedule_workspace_materialization(
        &self,
        job: &JobRecord,
    ) -> CentralResult<Option<JobRecord>> {
        let spec = job.workspace_spec.as_ref().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::Internal,
                "WorkspaceMaterialize Job lost its immutable spec",
            )
        })?;
        let playground = self
            .catalog
            .get_playground(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.playground_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "materialization Playground no longer exists",
                )
            })?;
        if playground.state != PlaygroundState::Creating {
            return Ok(None);
        }
        let Some(owner) = self
            .registry
            .get_current_by_volume(&spec.tenant_id, &spec.storage_volume_id)
            .await?
        else {
            return Ok(None);
        };
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != neoengramd::DerivedVolumeState::Ready
        {
            return Ok(None);
        }
        let Some(instance) = owner.instance.as_ref() else {
            return Ok(None);
        };
        if owner.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
            || owner.owner.active_agent_mount_id.as_ref() != Some(&owner.mount.agent_mount_id)
        {
            return Ok(None);
        }
        let result = self
            .control
            .assign_workspace_materialization(AssignWorkspaceMaterializationRequest {
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: WorkspaceMaterializeTarget {
                    assignment_id: deterministic_assignment_id(job)?,
                    assignment_generation: AssignmentGeneration::new(1),
                    agent_id: instance.agent_id.clone(),
                    storage_volume_id: spec.storage_volume_id.clone(),
                    agent_mount_id: owner.mount.agent_mount_id.clone(),
                    mount_generation: owner.mount.mount_generation,
                    owner_generation: owner.owner.owner_generation,
                },
            })
            .await?;
        Ok(Some(result.job))
    }

    async fn schedule_snapshot_mount(&self, job: &JobRecord) -> CentralResult<Option<JobRecord>> {
        let spec = job.snapshot_spec.as_ref().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::Internal,
                "SnapshotMount Job lost its immutable spec",
            )
        })?;
        let snapshot = self
            .catalog
            .get_snapshot(&spec.tenant_id, &spec.snapshot_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(CentralErrorCode::JobNotFound, "Snapshot no longer exists")
            })?;
        if !matches!(
            snapshot.state,
            SnapshotState::Creating | SnapshotState::Abnormal
        ) {
            return Ok(None);
        }
        let Some(owner) = self
            .registry
            .get_current_by_volume(&spec.tenant_id, &spec.storage_volume_id)
            .await?
        else {
            return Ok(None);
        };
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != neoengramd::DerivedVolumeState::Ready
        {
            return Ok(None);
        }
        let Some(instance) = owner.instance.as_ref() else {
            return Ok(None);
        };
        if owner.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
            || owner.owner.active_agent_mount_id.as_ref() != Some(&owner.mount.agent_mount_id)
        {
            return Ok(None);
        }
        if !instance.capabilities.is_empty()
            && !instance.capabilities.contains("snapshot_fuse_mount_v1")
        {
            return Ok(None);
        }
        let (assignment_generation, assignment_id) = match job.snapshot_assignment.as_ref() {
            Some(existing)
                if existing.agent_id == instance.agent_id
                    && existing.storage_volume_id == spec.storage_volume_id
                    && existing.agent_mount_id == owner.mount.agent_mount_id
                    && existing.mount_generation == owner.mount.mount_generation
                    && existing.owner_generation == owner.owner.owner_generation
                    && (!job.state.is_terminal() || job.state == JobState::Succeeded) =>
            {
                (
                    existing.assignment_generation,
                    existing.assignment_id.clone(),
                )
            }
            Some(existing) => {
                let next = existing
                    .assignment_generation
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| {
                        CentralError::new(
                            CentralErrorCode::GenerationMismatch,
                            "Snapshot assignment generation exhausted",
                        )
                    })?;
                let generation = AssignmentGeneration::new(next);
                let assignment_id = deterministic_snapshot_assignment_id(
                    job,
                    generation,
                    &instance.agent_id,
                    &owner.mount.agent_mount_id,
                    owner.mount.mount_generation,
                    owner.owner.owner_generation,
                )?;
                (generation, assignment_id)
            }
            None => {
                let generation = AssignmentGeneration::new(1);
                let assignment_id = deterministic_snapshot_assignment_id(
                    job,
                    generation,
                    &instance.agent_id,
                    &owner.mount.agent_mount_id,
                    owner.mount.mount_generation,
                    owner.owner.owner_generation,
                )?;
                (generation, assignment_id)
            }
        };
        let result = self
            .control
            .assign_snapshot_mount(AssignSnapshotMountRequest {
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: SnapshotMountTarget {
                    assignment_id,
                    assignment_generation,
                    agent_id: instance.agent_id.clone(),
                    storage_volume_id: spec.storage_volume_id.clone(),
                    agent_mount_id: owner.mount.agent_mount_id.clone(),
                    mount_generation: owner.mount.mount_generation,
                    owner_generation: owner.owner.owner_generation,
                },
            })
            .await?;
        Ok(Some(result.job))
    }

    pub async fn reconcile_once(&self) -> CentralResult<CoordinatorRun> {
        self.reconcile(DEFAULT_RECOVERY_BATCH_SIZE).await
    }

    pub async fn reconcile(&self, limit: usize) -> CentralResult<CoordinatorRun> {
        if limit == 0 {
            return Ok(CoordinatorRun::default());
        }
        // A crash can occur after the catalog commits Creating but before its deterministic Job
        // is inserted. Re-deriving the Job from catalog identity closes that cross-SQLite window.
        self.recover_creating_playgrounds(limit).await?;
        self.recover_creating_snapshots(limit).await?;
        self.reconcile_ready_snapshot_owners(limit).await?;
        self.recover_abnormal_snapshots(limit).await?;
        // The Pre-commit aggregate and its Add Job share authority storage but use separate ports.
        // Re-derive a missing deterministic Job after either write-side response is lost.
        self.recover_running_precommits(limit).await?;
        self.recover_unpublished_commits(limit).await?;
        let now = self.clock.now();
        let after = self
            .recovery_cursor
            .lock()
            .map_err(|_| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "Job coordinator recovery cursor lock is poisoned",
                )
            })?
            .clone();
        let mut jobs = self
            .jobs
            .list_recoverable(after.as_ref(), now, limit)
            .await?;
        if jobs.is_empty() && after.is_some() {
            jobs = self.jobs.list_recoverable(None, now, limit).await?;
        }
        // Advance before side effects so one persistently poisoned Job cannot pin the page.
        *self.recovery_cursor.lock().map_err(|_| {
            CentralError::new(
                CentralErrorCode::Internal,
                "Job coordinator recovery cursor lock is poisoned",
            )
        })? = jobs.last().map(JobRecord::key);
        let mut run = CoordinatorRun {
            examined: jobs.len(),
            ..CoordinatorRun::default()
        };
        for job in jobs {
            if let Err(error) = self.reconcile_job(&job, now, &mut run).await {
                tracing::warn!(
                    tenant_id = %job.spec.tenant_id,
                    job_id = %job.spec.job_id,
                    state = ?job.state,
                    %error,
                    "Job coordinator could not recover one Job; continuing the page"
                );
            }
        }
        Ok(run)
    }

    async fn recover_running_precommits(&self, limit: usize) -> CentralResult<()> {
        let Some(precommits) = &self.precommits else {
            return Ok(());
        };
        let after = self
            .precommit_recovery_cursor
            .lock()
            .map_err(|_| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "Pre-commit recovery cursor lock is poisoned",
                )
            })?
            .clone();
        let mut records = precommits.list_running(after.as_ref(), limit).await?;
        if records.is_empty() && after.is_some() {
            records = precommits.list_running(None, limit).await?;
        }
        *self.precommit_recovery_cursor.lock().map_err(|_| {
            CentralError::new(
                CentralErrorCode::Internal,
                "Pre-commit recovery cursor lock is poisoned",
            )
        })? = records.last().map(PreCommitRecord::key);
        for precommit in records {
            if let Err(error) = self.ensure_precommit_job(&precommit).await {
                tracing::warn!(
                    tenant_id = %precommit.tenant_id,
                    precommit_id = %precommit.precommit_id,
                    job_id = %precommit.job_id,
                    %error,
                    "Job coordinator could not recover one Pre-commit Add Job"
                );
            }
        }
        Ok(())
    }

    async fn recover_unpublished_commits(&self, limit: usize) -> CentralResult<()> {
        let Some(precommits) = &self.precommits else {
            return Ok(());
        };
        let after = self
            .commit_recovery_cursor
            .lock()
            .map_err(|_| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "Commit recovery cursor lock is poisoned",
                )
            })?
            .clone();
        let mut records = precommits
            .list_unpublished_commits(after.as_ref(), limit)
            .await?;
        if records.is_empty() && after.is_some() {
            records = precommits.list_unpublished_commits(None, limit).await?;
        }
        *self.commit_recovery_cursor.lock().map_err(|_| {
            CentralError::new(
                CentralErrorCode::Internal,
                "Commit recovery cursor lock is poisoned",
            )
        })? = records.last().map(PreCommitRecord::key);
        for precommit in records {
            if let Err(error) = self
                .publish_committed_heads(precommits.as_ref(), &precommit)
                .await
            {
                tracing::warn!(
                    tenant_id = %precommit.tenant_id,
                    precommit_id = %precommit.precommit_id,
                    %error,
                    "Job coordinator could not recover committed Head publication"
                );
            }
        }
        Ok(())
    }

    async fn publish_committed_heads(
        &self,
        precommits: &dyn PreCommitRepository,
        precommit: &PreCommitRecord,
    ) -> CentralResult<()> {
        let commit_id = precommit.committed_commit_id.ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::Internal,
                "committed Pre-commit lost its Commit identity",
            )
        })?;
        let commit = precommits
            .get_commit(
                &precommit.tenant_id,
                &precommit.project_id,
                &precommit.artifact_id,
                commit_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "committed Pre-commit lost its immutable Commit row",
                )
            })?;
        self.catalog
            .advance_playground_commit(AdvancePlaygroundCommitRequest {
                tenant_id: commit.tenant_id.clone(),
                project_id: commit.project_id.clone(),
                artifact_id: commit.artifact_id.clone(),
                playground_id: commit.source_playground_id.clone(),
                expected_head_commit_id: commit.parent_commit_id.map(Into::into),
                commit_id: commit.commit_id.into(),
                updated_at_unix_ms: commit.created_at_unix_ms,
            })
            .await?;
        precommits
            .acknowledge_head_publication(&precommit.key(), commit.commit_id, self.clock.now())
            .await?;
        Ok(())
    }

    async fn recover_creating_playgrounds(&self, limit: usize) -> CentralResult<()> {
        let mut remaining = limit;
        let mut tenant_cursor = None;
        while remaining != 0 {
            let page = self
                .catalog
                .list_tenants(&TenantListRequest {
                    visible_tenant_ids: None,
                    query: None,
                    after: tenant_cursor.clone(),
                    limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                })
                .await?;
            if page.records.is_empty() {
                break;
            }
            for tenant in page.records {
                let mut playground_cursor = None;
                loop {
                    let playgrounds = self
                        .catalog
                        .list_playgrounds(&PlaygroundListRequest {
                            tenant_id: tenant.tenant_id.clone(),
                            project_id: None,
                            artifact_id: None,
                            region: None,
                            state: Some(PlaygroundState::Creating),
                            query: None,
                            after: playground_cursor.clone(),
                            limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                        })
                        .await?;
                    for playground in &playgrounds.records {
                        if playground
                            .created_at_unix_ms
                            .get()
                            .saturating_add(WORKSPACE_MATERIALIZE_DEADLINE_MS)
                            <= self.clock.now().get()
                        {
                            let _ = self
                                .catalog
                                .transition_playground_state(
                                    &playground.tenant_id,
                                    &playground.project_id,
                                    &playground.artifact_id,
                                    &playground.playground_id,
                                    PlaygroundState::Creating,
                                    PlaygroundState::Abnormal,
                                    self.clock.now(),
                                )
                                .await?;
                            remaining = remaining.saturating_sub(1);
                            if remaining == 0 {
                                return Ok(());
                            }
                            continue;
                        }
                        let _ = self.ensure_workspace_materialization(playground).await?;
                        remaining = remaining.saturating_sub(1);
                        if remaining == 0 {
                            return Ok(());
                        }
                    }
                    playground_cursor = playgrounds.next;
                    if playground_cursor.is_none() {
                        break;
                    }
                }
            }
            tenant_cursor = page.next;
            if tenant_cursor.is_none() {
                break;
            }
        }
        Ok(())
    }

    async fn recover_creating_snapshots(&self, limit: usize) -> CentralResult<()> {
        let mut remaining = limit;
        let mut tenant_cursor = None;
        while remaining != 0 {
            let page = self
                .catalog
                .list_tenants(&TenantListRequest {
                    visible_tenant_ids: None,
                    query: None,
                    after: tenant_cursor.clone(),
                    limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                })
                .await?;
            if page.records.is_empty() {
                break;
            }
            for tenant in &page.records {
                let mut snapshot_cursor = None;
                loop {
                    let snapshots = self
                        .catalog
                        .list_snapshots(&SnapshotListRequest {
                            tenant_id: tenant.tenant_id.clone(),
                            project_id: None,
                            artifact_id: None,
                            commit_id: None,
                            storage_volume_id: None,
                            region: None,
                            state: Some(SnapshotState::Creating),
                            after: snapshot_cursor.clone(),
                            limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                        })
                        .await?;
                    for snapshot in &snapshots.records {
                        let _ = self.ensure_snapshot_mount(snapshot).await?;
                        remaining = remaining.saturating_sub(1);
                        if remaining == 0 {
                            return Ok(());
                        }
                    }
                    snapshot_cursor = snapshots.next;
                    if snapshot_cursor.is_none() {
                        break;
                    }
                }
            }
            tenant_cursor = page.next;
            if tenant_cursor.is_none() {
                break;
            }
        }
        Ok(())
    }

    /// Retries durable Snapshot serving after an owner heartbeat gap or fenced owner replacement.
    /// A matching owner receives the original assignment again; a changed owner is assigned the
    /// next generation by `schedule_snapshot_mount`.
    async fn recover_abnormal_snapshots(&self, limit: usize) -> CentralResult<()> {
        let mut remaining = limit;
        let mut tenant_cursor = None;
        while remaining != 0 {
            let page = self
                .catalog
                .list_tenants(&TenantListRequest {
                    visible_tenant_ids: None,
                    query: None,
                    after: tenant_cursor.clone(),
                    limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                })
                .await?;
            if page.records.is_empty() {
                break;
            }
            for tenant in &page.records {
                let mut snapshot_cursor = None;
                loop {
                    let snapshots = self
                        .catalog
                        .list_snapshots(&SnapshotListRequest {
                            tenant_id: tenant.tenant_id.clone(),
                            project_id: None,
                            artifact_id: None,
                            commit_id: None,
                            storage_volume_id: None,
                            region: None,
                            state: Some(SnapshotState::Abnormal),
                            after: snapshot_cursor.clone(),
                            limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                        })
                        .await?;
                    for snapshot in &snapshots.records {
                        let job_id = deterministic_snapshot_mount_job_id(snapshot)?;
                        let job = self
                            .jobs
                            .get(&JobKey::new(snapshot.tenant_id.clone(), job_id))
                            .await?;
                        let failed_attempt = job.as_ref().is_some_and(|job| {
                            job.state.is_terminal() && job.state != JobState::Succeeded
                        });
                        let replacement_ready = if let Some(job) = job.as_ref() {
                            self.ready_snapshot_owner_fence_changed(snapshot, job)
                                .await?
                        } else {
                            false
                        };
                        let snapshot = if failed_attempt && !replacement_ready {
                            let now = self.clock.now();
                            if now.get()
                                < snapshot
                                    .updated_at_unix_ms
                                    .get()
                                    .saturating_add(SNAPSHOT_RECOVERY_BACKOFF_MS)
                            {
                                continue;
                            }
                            match self
                                .catalog
                                .transition_snapshot_state(
                                    &snapshot.tenant_id,
                                    &snapshot.snapshot_id,
                                    SnapshotState::Abnormal,
                                    SnapshotState::Abnormal,
                                    snapshot.phase,
                                    now,
                                )
                                .await
                            {
                                Ok(claimed) => claimed,
                                Err(error)
                                    if error.code() == CentralErrorCode::ConcurrentUpdate =>
                                {
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                        } else {
                            snapshot.clone()
                        };
                        let _ = self.ensure_snapshot_mount(&snapshot).await?;
                        remaining = remaining.saturating_sub(1);
                        if remaining == 0 {
                            return Ok(());
                        }
                    }
                    snapshot_cursor = snapshots.next;
                    if snapshot_cursor.is_none() {
                        break;
                    }
                }
            }
            tenant_cursor = page.next;
            if tenant_cursor.is_none() {
                break;
            }
        }
        Ok(())
    }

    async fn ready_snapshot_owner_fence_changed(
        &self,
        snapshot: &SnapshotRecord,
        job: &JobRecord,
    ) -> CentralResult<bool> {
        let Some(assignment) = job.snapshot_assignment.as_ref() else {
            return Ok(false);
        };
        let Some(owner) = self
            .registry
            .get_current_by_volume(&snapshot.tenant_id, &snapshot.storage_volume_id)
            .await?
        else {
            return Ok(false);
        };
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != neoengramd::DerivedVolumeState::Ready
        {
            return Ok(false);
        }
        let Some(instance) = owner.instance.as_ref() else {
            return Ok(false);
        };
        if owner.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
            || owner.owner.active_agent_mount_id.as_ref() != Some(&owner.mount.agent_mount_id)
        {
            return Ok(false);
        }
        Ok(assignment.agent_id != instance.agent_id
            || assignment.agent_mount_id != owner.mount.agent_mount_id
            || assignment.mount_generation != owner.mount.mount_generation
            || assignment.owner_generation != owner.owner.owner_generation)
    }

    async fn reconcile_ready_snapshot_owners(&self, limit: usize) -> CentralResult<()> {
        let mut remaining = limit;
        let mut tenant_cursor = None;
        while remaining != 0 {
            let page = self
                .catalog
                .list_tenants(&TenantListRequest {
                    visible_tenant_ids: None,
                    query: None,
                    after: tenant_cursor.clone(),
                    limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                })
                .await?;
            if page.records.is_empty() {
                break;
            }
            for tenant in &page.records {
                let mut cursor = None;
                loop {
                    let snapshots = self
                        .catalog
                        .list_snapshots(&SnapshotListRequest {
                            tenant_id: tenant.tenant_id.clone(),
                            project_id: None,
                            artifact_id: None,
                            commit_id: None,
                            storage_volume_id: None,
                            region: None,
                            state: Some(SnapshotState::Ready),
                            after: cursor.clone(),
                            limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                        })
                        .await?;
                    for snapshot in &snapshots.records {
                        let job_id = deterministic_snapshot_mount_job_id(snapshot)?;
                        let job = self
                            .jobs
                            .get(&JobKey::new(snapshot.tenant_id.clone(), job_id))
                            .await?;
                        let owner = self
                            .registry
                            .get_current_by_volume(&snapshot.tenant_id, &snapshot.storage_volume_id)
                            .await?;
                        let healthy = match (job, owner) {
                            (Some(job), Some(owner))
                                if owner.derived_volume_state(
                                    self.clock.now(),
                                    self.heartbeat_timeout_ms,
                                ) == neoengramd::DerivedVolumeState::Ready =>
                            {
                                match (job.snapshot_assignment.as_ref(), owner.instance.as_ref()) {
                                    (Some(assignment), Some(instance)) => {
                                        assignment.agent_id == instance.agent_id
                                            && owner.owner.active_agent_id.as_ref()
                                                == Some(&assignment.agent_id)
                                            && owner.owner.active_agent_mount_id.as_ref()
                                                == Some(&assignment.agent_mount_id)
                                            && owner.mount.agent_mount_id
                                                == assignment.agent_mount_id
                                            && owner.mount.mount_generation
                                                == assignment.mount_generation
                                            && owner.owner.owner_generation
                                                == assignment.owner_generation
                                    }
                                    _ => false,
                                }
                            }
                            _ => false,
                        };
                        if !healthy {
                            let _ = self
                                .catalog
                                .transition_snapshot_state(
                                    &snapshot.tenant_id,
                                    &snapshot.snapshot_id,
                                    SnapshotState::Ready,
                                    SnapshotState::Abnormal,
                                    SnapshotPhase::Idle,
                                    self.clock.now(),
                                )
                                .await?;
                        }
                        remaining = remaining.saturating_sub(1);
                        if remaining == 0 {
                            return Ok(());
                        }
                    }
                    cursor = snapshots.next;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
            tenant_cursor = page.next;
            if tenant_cursor.is_none() {
                break;
            }
        }
        Ok(())
    }

    async fn reconcile_job(
        &self,
        job: &JobRecord,
        now: neoengram_protocol::UnixMillis,
        run: &mut CoordinatorRun,
    ) -> CentralResult<()> {
        if job.operation == JobOperation::SnapshotMount {
            let recovered = self
                .control
                .recover_snapshot_mount(&job.spec.tenant_id, &job.spec.job_id)
                .await?;
            if recovered.state == JobState::RecoveryRequired {
                match self.schedule_snapshot_mount(&recovered).await {
                    Ok(Some(_)) => run.assigned += 1,
                    Ok(None) => {}
                    Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                    Err(error) => return Err(error),
                }
                return Ok(());
            }
            if recovered.state.is_terminal() {
                return Ok(());
            }
            if recovered.state == JobState::Queued {
                match self.schedule(&recovered).await {
                    Ok(Some(_)) => run.assigned += 1,
                    Ok(None) => {}
                    Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                    Err(error) => return Err(error),
                }
            }
            return Ok(());
        }
        if job.operation == JobOperation::WorkspaceMaterialize {
            let recovered = self
                .control
                .recover_workspace_materialization(&job.spec.tenant_id, &job.spec.job_id)
                .await?;
            if recovered.state.is_terminal() {
                return Ok(());
            }
            if recovered.spec.deadline_unix_ms.get() <= now.get()
                && matches!(
                    recovered.state,
                    JobState::Queued | JobState::Assigned | JobState::Accepted | JobState::Running
                )
            {
                match self
                    .control
                    .expire_workspace_materialization(
                        &recovered.spec.tenant_id,
                        &recovered.spec.job_id,
                    )
                    .await
                {
                    Ok(_) => run.expired += 1,
                    Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                    Err(error) => return Err(error),
                }
            } else if recovered.state == JobState::Queued {
                match self.schedule(&recovered).await {
                    Ok(Some(_)) => run.assigned += 1,
                    Ok(None) => {}
                    Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                    Err(error) => return Err(error),
                }
            }
            return Ok(());
        }
        let _ = self.sync_precommit_for_job(job).await?;
        if job.spec.deadline_unix_ms.get() <= now.get()
            && matches!(
                job.state,
                JobState::Queued
                    | JobState::Assigned
                    | JobState::Accepted
                    | JobState::Running
                    | JobState::Prepared
                    | JobState::CancelRequested
            )
        {
            let expired = match self
                .control
                .expire_add_job(ExpireAddJobRequest {
                    actor: job.spec.principal.clone(),
                    tenant_id: job.spec.tenant_id.clone(),
                    job_id: job.spec.job_id.clone(),
                })
                .await
            {
                Ok(result) => {
                    run.expired += 1;
                    Some(result.job)
                }
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => None,
                Err(error) => return Err(error),
            };
            if let Some(expired) = expired {
                let _ = self.sync_precommit_for_job(&expired).await?;
            }
            return Ok(());
        }
        match job.state {
            JobState::Queued => match self.schedule(job).await {
                Ok(Some(_)) => run.assigned += 1,
                Ok(None) => {}
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                Err(error) => return Err(error),
            },
            JobState::Prepared => match self
                .control
                .finalize_prepared(ResumePublicationRequest {
                    tenant_id: job.spec.tenant_id.clone(),
                    job_id: job.spec.job_id.clone(),
                })
                .await
            {
                Ok(_) => run.finalized += 1,
                Err(error)
                    if matches!(
                        error.code(),
                        CentralErrorCode::BatchIncomplete
                            | CentralErrorCode::ObjectNotDurable
                            | CentralErrorCode::ConcurrentUpdate
                    ) => {}
                Err(error) => return Err(error),
            },
            JobState::Publishing => match self
                .control
                .resume_publication(ResumePublicationRequest {
                    tenant_id: job.spec.tenant_id.clone(),
                    job_id: job.spec.job_id.clone(),
                })
                .await
            {
                Ok(_) => run.finalized += 1,
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                Err(error) => return Err(error),
            },
            _ => {}
        }
        if let Some(updated) = self.jobs.get(&job.key()).await? {
            let _ = self.sync_precommit_for_job(&updated).await?;
        }
        Ok(())
    }

    async fn sync_precommit_for_job(
        &self,
        job: &JobRecord,
    ) -> CentralResult<Option<PreCommitRecord>> {
        let Some(precommits) = &self.precommits else {
            return Ok(None);
        };
        let published_index = if job.state == JobState::Succeeded {
            Some(self.indexes.published_index(&job.index_key()).await?)
        } else {
            None
        };
        precommits
            .sync_job(job.clone(), published_index, self.clock.now())
            .await
    }
}

fn precommit_add_job_spec(precommit: &PreCommitRecord) -> CentralResult<AddJobSpec> {
    let principal = PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new("precommit-scanner")?,
        extensions: Extensions::new(),
    };
    let deadline_unix_ms = UnixMillis::new(
        precommit
            .updated_at_unix_ms
            .get()
            .saturating_add(PRECOMMIT_JOB_DEADLINE_MS),
    );
    let operation = AddOperation {
        job_id: precommit.job_id.clone(),
        principal: principal.clone(),
        tenant_id: precommit.tenant_id.clone(),
        project_id: precommit.project_id.clone(),
        artifact_id: precommit.artifact_id.clone(),
        playground_id: precommit.playground_id.clone(),
        expected_index_version: precommit.source_index_version.clone(),
        deadline_unix_ms,
        paths: Vec::new(),
        all: true,
        extensions: Extensions::new(),
    };
    Ok(AddJobSpec {
        job_id: precommit.job_id.clone(),
        principal,
        tenant_id: precommit.tenant_id.clone(),
        project_id: precommit.project_id.clone(),
        artifact_id: precommit.artifact_id.clone(),
        playground_id: precommit.playground_id.clone(),
        expected_index_version: precommit.source_index_version.clone(),
        request_digest: operation.request_digest()?,
        deadline_unix_ms,
        paths: Vec::new(),
        all: true,
        extensions: Extensions::new(),
    })
}

pub(crate) async fn validate_job_spec(
    jobs: &dyn JobRepository,
    catalog: &dyn ControlCatalogRepository,
    indexes: &dyn IndexPublisher,
    spec: &neoengramd::AddJobSpec,
) -> CentralResult<()> {
    if jobs
        .get(&neoengramd::JobKey::new(
            spec.tenant_id.clone(),
            spec.job_id.clone(),
        ))
        .await?
        .is_some()
    {
        // Exact replay and JobId reuse are decided against the immutable persisted spec.
        return Ok(());
    }
    catalog
        .get_playground(
            &spec.tenant_id,
            &spec.project_id,
            &spec.artifact_id,
            &spec.playground_id,
        )
        .await?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::JobNotFound,
                "the requested Playground is not visible or does not exist",
            )
            .with_retryable(false)
        })?;
    let current = indexes
        .current_version(&IndexKey {
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id: spec.playground_id.clone(),
        })
        .await?;
    if !same_index_version(&current, &spec.expected_index_version) {
        return Err(CentralError::new(
            CentralErrorCode::MetadataInvalid,
            "expected_index_version differs from the authoritative Playground IndexVersion",
        )
        .with_retryable(false));
    }
    Ok(())
}

fn same_index_version(
    left: &neoengram_protocol::WireIndexVersion,
    right: &neoengram_protocol::WireIndexVersion,
) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn deterministic_assignment_id(job: &JobRecord) -> CentralResult<AssignmentId> {
    let input = format!("{}\0{}", job.spec.tenant_id, job.spec.job_id);
    AssignmentId::new(format!(
        "assignment-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_materialization_job_id(playground: &PlaygroundRecord) -> CentralResult<JobId> {
    let input = format!(
        "{}\0{}\0{}\0{}",
        playground.tenant_id,
        playground.project_id,
        playground.artifact_id,
        playground.playground_id
    );
    JobId::new(format!(
        "materialize-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_snapshot_mount_job_id(snapshot: &SnapshotRecord) -> CentralResult<JobId> {
    let input = format!(
        "{}\0{}\0{}\0{}",
        snapshot.tenant_id, snapshot.project_id, snapshot.artifact_id, snapshot.snapshot_id
    );
    JobId::new(format!(
        "snapshot-mount-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_snapshot_assignment_id(
    job: &JobRecord,
    assignment_generation: AssignmentGeneration,
    agent_id: &AgentId,
    agent_mount_id: &AgentMountId,
    mount_generation: MountGeneration,
    owner_generation: OwnerGeneration,
) -> CentralResult<AssignmentId> {
    let input = format!(
        "{}\0{}\0{}\0{}\0{}\0{}\0{}",
        job.spec.tenant_id,
        job.spec.job_id,
        assignment_generation.get(),
        agent_id,
        agent_mount_id,
        mount_generation.get(),
        owner_generation.get(),
    );
    AssignmentId::new(format!(
        "snapshot-assignment-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_placement_id(
    job: &JobRecord,
    volume_id: &neoengram_protocol::StorageVolumeId,
) -> CentralResult<ArtifactPlacementId> {
    let input = format!(
        "{}\0{}\0{}",
        job.spec.tenant_id, job.spec.artifact_id, volume_id
    );
    ArtifactPlacementId::new(format!(
        "placement-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use async_trait::async_trait;
    use neoengram_core::{
        ChunkingStrategy, ContentDigest, FileRecord, IndexVersion, LogicalPath, Manifest,
    };
    use neoengram_protocol::{
        AgentBootId, AgentEnrollmentId, AgentEnrollmentState, AgentEnrollmentTokenId, AgentId,
        AgentInstallationId, AgentMountId, AgentMountIdentityDigest, ArtifactId,
        AssignmentGeneration, AssignmentId, ControlError, ControlMessage, DecimalU64,
        EdgeClusterId, ErrorCode, Extensions, IndexDeltaRecord, JobAccepted, JobFailed,
        JobFailureStage, JobId, JobPrepared, JobProgress, ManifestRecord, MetadataBatchDescriptor,
        MetadataBatchId, MetadataBatchPage, MetadataBatchRecords, MetadataBatchScope,
        MetadataPublication, MountAccessMode, MountGeneration, OwnerGeneration,
        PlacementGeneration, PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef, ProjectId,
        ProtocolVersion, PvcIdentityDigest, RequestId, ResourceHealth, ResourceVersion,
        SequenceNumber, SessionGeneration, SessionId, SnapshotId, StorageVolumeId, TenantId,
        UnixMillis, VolumeMarkerId, WireChunkingStrategy, PROTOCOL_VERSION_V1,
    };
    use neoengramd::{
        Action, AddJobSpec, AgentEnrollmentRecord, AgentInstanceRecord, AgentInstanceState,
        AgentMountRecord, AgentRegistryRecord, AgentReport, ArtifactHeadExpectation,
        ArtifactInitialization, ArtifactRecord, AuthorityCapabilities, AuthorityStore,
        AuthorizationRequest, Authorizer, CatalogPvcReference, CreateAddJobRequest,
        FinalizeAddRequest, InMemoryComponents, InMemoryJobRepository, JobInsertOutcome,
        MetadataBatchSubmission, ReceiveReportRequest, SnapshotInsertRequest,
        StageMetadataBatchRequest, StorageAccessMode, StorageBackendType,
        StorageEnrollmentMetadata, StorageVolumeRecord, StorageVolumeState, TenantRecord,
        VolumeOwnerRecord, VolumeOwnerState,
    };

    use super::*;

    #[tokio::test]
    async fn snapshot_mount_recovers_same_owner_failure_and_owner_replacement() {
        let components = InMemoryComponents::new(100);
        let fixture = insert_snapshot_fixture(&components).await;
        let registry = Arc::new(TestSnapshotOwnerRegistry::new(snapshot_owner(
            &fixture,
            "agent-old",
            "mount-old",
            1,
            1,
            100,
        )));
        let (control, coordinator) = test_coordinator_with_registry(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
            registry.clone(),
        );
        let created = control
            .create_snapshot_mount(CreateSnapshotMountRequest {
                spec: snapshot_mount_spec(&fixture),
            })
            .await
            .unwrap();
        let first = coordinator.schedule(&created.job).await.unwrap().unwrap();
        let first_assignment = first.snapshot_assignment.clone().unwrap();
        complete_snapshot_mount(&control, &first_assignment).await;
        assert_snapshot_state(&components, &fixture, SnapshotState::Ready).await;

        // A heartbeat gap demotes the serving view. Once the same fenced owner reconnects, the
        // original terminal assignment is reactivated so its durable descriptor can be observed.
        components.clock.advance(30_001).unwrap();
        coordinator.reconcile(100).await.unwrap();
        assert_snapshot_state(&components, &fixture, SnapshotState::Abnormal).await;
        registry.set(snapshot_owner(
            &fixture,
            "agent-old",
            "mount-old",
            1,
            1,
            components.clock.now().get(),
        ));
        coordinator.reconcile(100).await.unwrap();
        let redelivered = control
            .poll_agent_messages(&first_assignment.agent_id, SessionGeneration::new(1), 10)
            .await
            .unwrap();
        assert!(redelivered.iter().any(|message| matches!(
            &message.message,
            ControlMessage::Assignment(assignment)
                if assignment.assignment == neoengram_protocol::AssignmentOperation::SnapshotMount {
                    input: first_assignment.clone(),
                    extensions: Extensions::new(),
                }
        )));
        report_snapshot_mounted(&control, &first_assignment).await;
        assert_snapshot_state(&components, &fixture, SnapshotState::Ready).await;

        // A descriptor recovery failure is valid even though the original mount Job succeeded.
        // It must create a fresh attempt, not replay a terminal failed execution record.
        let failed = control
            .receive_report(ReceiveReportRequest {
                tenant_id: fixture.tenant_id.clone(),
                agent_id: first_assignment.agent_id.clone(),
                report: AgentReport::Failed(snapshot_recovery_failure(&first_assignment)),
            })
            .await
            .unwrap();
        assert_eq!(failed.job.state, JobState::RecoveryRequired);
        assert_snapshot_state(&components, &fixture, SnapshotState::Abnormal).await;
        coordinator.reconcile(100).await.unwrap();
        let deferred = components.jobs.get(&first.key()).await.unwrap().unwrap();
        assert_eq!(deferred.state, JobState::RecoveryRequired);
        assert_eq!(
            deferred
                .snapshot_assignment
                .as_ref()
                .unwrap()
                .assignment_generation,
            AssignmentGeneration::new(1)
        );
        components
            .clock
            .advance(SNAPSHOT_RECOVERY_BACKOFF_MS)
            .unwrap();
        coordinator.reconcile(100).await.unwrap();
        let second = components.jobs.get(&first.key()).await.unwrap().unwrap();
        let second_assignment = second.snapshot_assignment.clone().unwrap();
        assert_eq!(second_assignment.assignment_generation.get(), 2);
        assert_ne!(
            second_assignment.assignment_id,
            first_assignment.assignment_id
        );
        complete_snapshot_mount(&control, &second_assignment).await;
        assert_snapshot_state(&components, &fixture, SnapshotState::Ready).await;

        // Replacing the Volume owner advances both registry fences and the assignment generation.
        components.clock.advance(30_001).unwrap();
        coordinator.reconcile(100).await.unwrap();
        assert_snapshot_state(&components, &fixture, SnapshotState::Abnormal).await;
        registry.set(snapshot_owner(
            &fixture,
            "agent-new",
            "mount-new",
            2,
            2,
            components.clock.now().get(),
        ));
        coordinator.reconcile(100).await.unwrap();
        let replacement = components.jobs.get(&first.key()).await.unwrap().unwrap();
        let replacement_assignment = replacement.snapshot_assignment.clone().unwrap();
        assert_eq!(replacement.state, JobState::Assigned);
        assert_eq!(replacement_assignment.assignment_generation.get(), 3);
        assert_ne!(
            replacement_assignment.assignment_id,
            second_assignment.assignment_id
        );
        assert_eq!(replacement_assignment.agent_id.as_str(), "agent-new");
        assert_eq!(replacement_assignment.agent_mount_id.as_str(), "mount-new");
        assert_eq!(replacement_assignment.mount_generation.get(), 2);
        assert_eq!(replacement_assignment.owner_generation.get(), 2);

        let stale = control
            .receive_report(ReceiveReportRequest {
                tenant_id: fixture.tenant_id.clone(),
                agent_id: second_assignment.agent_id.clone(),
                report: AgentReport::Progress(mounted_progress(&second_assignment)),
            })
            .await
            .unwrap_err();
        assert_eq!(stale.code(), CentralErrorCode::AssignmentMismatch);
        complete_snapshot_mount(&control, &replacement_assignment).await;
        assert_snapshot_state(&components, &fixture, SnapshotState::Ready).await;
    }

    #[tokio::test]
    async fn failed_snapshot_recovery_uses_persisted_backoff() {
        let components = InMemoryComponents::new(100);
        let fixture = insert_snapshot_fixture(&components).await;
        let registry = Arc::new(TestSnapshotOwnerRegistry::new(snapshot_owner(
            &fixture,
            "agent-backoff",
            "mount-backoff",
            1,
            1,
            100,
        )));
        let (control, coordinator) = test_coordinator_with_registry(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
            registry,
        );
        let created = control
            .create_snapshot_mount(CreateSnapshotMountRequest {
                spec: snapshot_mount_spec(&fixture),
            })
            .await
            .unwrap();
        let first = coordinator.schedule(&created.job).await.unwrap().unwrap();
        let first_assignment = first.snapshot_assignment.clone().unwrap();
        let mut failure = snapshot_recovery_failure(&first_assignment);
        failure.extensions.clear();
        let failed = control
            .receive_report(ReceiveReportRequest {
                tenant_id: fixture.tenant_id.clone(),
                agent_id: first_assignment.agent_id.clone(),
                report: AgentReport::Failed(failure),
            })
            .await
            .unwrap()
            .job;
        assert_eq!(failed.state, JobState::Failed);
        let failed_version = failed.resource_version;

        for _ in 0..3 {
            coordinator.reconcile(100).await.unwrap();
            let unchanged = components.jobs.get(&failed.key()).await.unwrap().unwrap();
            assert_eq!(unchanged.resource_version, failed_version);
            assert_eq!(
                unchanged
                    .snapshot_assignment
                    .as_ref()
                    .unwrap()
                    .assignment_generation,
                AssignmentGeneration::new(1)
            );
        }
        components
            .clock
            .advance(SNAPSHOT_RECOVERY_BACKOFF_MS - 1)
            .unwrap();
        coordinator.reconcile(100).await.unwrap();
        assert_eq!(
            components
                .jobs
                .get(&failed.key())
                .await
                .unwrap()
                .unwrap()
                .resource_version,
            failed_version
        );

        components.clock.advance(1).unwrap();
        coordinator.reconcile(100).await.unwrap();
        let retry = components.jobs.get(&failed.key()).await.unwrap().unwrap();
        assert_eq!(retry.state, JobState::Assigned);
        assert_eq!(
            retry
                .snapshot_assignment
                .as_ref()
                .unwrap()
                .assignment_generation,
            AssignmentGeneration::new(2)
        );
        assert!(retry.resource_version.get() > failed_version.get());
        assert_eq!(
            components
                .control_catalog
                .get_snapshot(&fixture.tenant_id, &fixture.snapshot_id)
                .await
                .unwrap()
                .unwrap()
                .updated_at_unix_ms,
            components.clock.now()
        );

        let retry_version = retry.resource_version;
        for _ in 0..3 {
            coordinator.reconcile(100).await.unwrap();
            assert_eq!(
                components
                    .jobs
                    .get(&failed.key())
                    .await
                    .unwrap()
                    .unwrap()
                    .resource_version,
                retry_version
            );
        }
    }

    #[tokio::test]
    async fn empty_workspace_materialization_creates_a_real_empty_index_marker() {
        let components = InMemoryComponents::new(100);
        let tenant_id = TenantId::new("tenant-empty-materialize").unwrap();
        let project_id = ProjectId::new("project-empty-materialize").unwrap();
        let artifact_id = ArtifactId::new("artifact-empty-materialize").unwrap();
        let playground_id = PlaygroundId::new("playground-empty-materialize").unwrap();
        let storage_volume_id = StorageVolumeId::new("volume-empty-materialize").unwrap();
        components
            .control_catalog
            .insert_tenant(TenantRecord {
                tenant_id: tenant_id.clone(),
                display_name: "Tenant".to_owned(),
                description: None,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_artifact(ArtifactRecord {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                display_name: "Artifact".to_owned(),
                description: None,
                initialization: ArtifactInitialization::Empty,
                head_commit_id: None,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id: tenant_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                display_name: "Volume".to_owned(),
                edge_cluster_id: EdgeClusterId::new("cluster-empty-materialize").unwrap(),
                region: "local".to_owned(),
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteMany,
                pvc_reference: Some(CatalogPvcReference {
                    namespace: "default".to_owned(),
                    claim_name: "empty-materialize".to_owned(),
                }),
                nfs_reference: None,
                state: StorageVolumeState::Ready,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            })
            .await
            .unwrap();
        let playground = PlaygroundRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            storage_volume_id,
            region: "local".to_owned(),
            display_name: "Playground".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: PlaygroundState::Creating,
            relative_root: format!("playgrounds/{project_id}/{artifact_id}/{playground_id}"),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
        };
        components
            .control_catalog
            .insert_playground(playground.clone())
            .await
            .unwrap();
        let (_, coordinator) = test_coordinator(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
        );

        let job = coordinator
            .ensure_workspace_materialization(&playground)
            .await
            .unwrap();
        let workspace_spec = job.workspace_spec.as_ref().unwrap();
        assert!(workspace_spec.base_commit_id.is_none());
        assert!(workspace_spec.base_index_version.is_none());

        let conflict = components
            .publisher
            .initialize_snapshot(InitializeIndexSnapshotRequest {
                index_key: IndexKey {
                    tenant_id,
                    project_id,
                    artifact_id,
                    playground_id,
                },
                version: IndexVersion::from_snapshot(1, &[]).unwrap().into(),
                records: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(conflict.code(), CentralErrorCode::ConcurrentUpdate);
    }

    #[tokio::test]
    async fn recovery_cursor_eventually_processes_more_jobs_than_one_page() {
        let components = InMemoryComponents::new(100);
        for index in 0..5 {
            components
                .jobs
                .insert_or_load(queued_job(&components, &format!("job-{index:02}"), 99).await)
                .await
                .unwrap();
        }
        let (_, coordinator) = test_coordinator(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
        );

        let first = coordinator.reconcile(2).await.unwrap();
        let second = coordinator.reconcile(2).await.unwrap();
        let third = coordinator.reconcile(2).await.unwrap();

        assert_eq!([first.examined, second.examined, third.examined], [2, 2, 1]);
        assert_eq!(first.expired + second.expired + third.expired, 5);
        assert!(components
            .jobs
            .all()
            .unwrap()
            .iter()
            .all(|job| job.state == JobState::TimedOut));
    }

    #[tokio::test]
    async fn poisoned_job_does_not_starve_later_work_with_single_item_pages() {
        let components = InMemoryComponents::new(100);
        let poisoned = queued_job(&components, "job-a-poisoned", 99).await;
        let later = queued_job(&components, "job-b-later", 99).await;
        components
            .jobs
            .insert_or_load(poisoned.clone())
            .await
            .unwrap();
        components.jobs.insert_or_load(later.clone()).await.unwrap();
        let jobs = Arc::new(PoisonReplaceRepository {
            inner: components.jobs.clone(),
            poisoned: poisoned.key(),
        });
        let (_, coordinator) =
            test_coordinator(&components, jobs.clone(), components.authorizer.clone());

        let first = coordinator.reconcile(1).await.unwrap();

        assert_eq!(first.examined, 1);
        assert_eq!(first.expired, 0);
        assert_eq!(
            components
                .jobs
                .get(&poisoned.key())
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::Queued
        );
        assert_eq!(
            components
                .jobs
                .get(&later.key())
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::Queued
        );

        let second = coordinator.reconcile(1).await.unwrap();

        assert_eq!(second.examined, 1);
        assert_eq!(second.expired, 1);
        assert_eq!(
            components
                .jobs
                .get(&later.key())
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::TimedOut
        );
        // The poison remains retryable after wraparound without turning the round into an error.
        assert_eq!(coordinator.reconcile(1).await.unwrap().examined, 1);
    }

    #[tokio::test]
    async fn prepared_recovery_bypasses_mutable_principal_finalize_permission() {
        let components = InMemoryComponents::new(100);
        let authorizer = Arc::new(RevocableFinalizeAuthorizer::default());
        let (control, coordinator) =
            test_coordinator(&components, components.jobs.clone(), authorizer.clone());
        let queued = queued_job(&components, "job-prepared", 10_000).await;
        let spec = queued.spec.clone();
        let actor = spec.principal.clone();
        let target = assignment_target();
        control
            .create_add_job(CreateAddJobRequest {
                actor: actor.clone(),
                spec: spec.clone(),
            })
            .await
            .unwrap();
        control
            .assign_job(AssignJobRequest {
                actor: actor.clone(),
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: target.clone(),
            })
            .await
            .unwrap();
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: spec.tenant_id.clone(),
                agent_id: target.agent_id.clone(),
                report: AgentReport::Accepted(JobAccepted {
                    job_id: spec.job_id.clone(),
                    assignment_id: target.assignment_id.clone(),
                    assignment_generation: target.assignment_generation,
                    accepted_at_unix_ms: UnixMillis::new(101),
                    request_digest: spec.request_digest,
                    extensions: Extensions::new(),
                }),
            })
            .await
            .unwrap();
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: spec.tenant_id.clone(),
                agent_id: target.agent_id.clone(),
                report: AgentReport::Progress(JobProgress {
                    job_id: spec.job_id.clone(),
                    assignment_id: target.assignment_id.clone(),
                    assignment_generation: target.assignment_generation,
                    state: JobState::Running,
                    phase: "prepare".to_owned(),
                    files_completed: DecimalU64::new(0),
                    bytes_completed: DecimalU64::new(0),
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                }),
            })
            .await
            .unwrap();
        let (prepared, pages) = prepared_metadata(&spec, &target);
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: spec.tenant_id.clone(),
                agent_id: target.agent_id.clone(),
                report: AgentReport::Prepared(prepared.clone()),
            })
            .await
            .unwrap();
        for descriptor in &prepared.metadata_batches {
            control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: spec.tenant_id.clone(),
                    job_id: spec.job_id.clone(),
                    agent_id: target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Descriptor(descriptor.clone()),
                })
                .await
                .unwrap();
        }
        for page in pages {
            control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: spec.tenant_id.clone(),
                    job_id: spec.job_id.clone(),
                    agent_id: target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Page(page),
                })
                .await
                .unwrap();
        }

        authorizer.revoke();
        let denied = control
            .finalize_add(FinalizeAddRequest {
                actor,
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(denied.code(), CentralErrorCode::Unauthorized);

        let run = coordinator.reconcile(10).await.unwrap();
        assert_eq!(run.finalized, 1);
        assert_eq!(
            components
                .jobs
                .get(&spec_key(&spec))
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::Succeeded
        );
    }

    #[derive(Clone)]
    struct SnapshotFixture {
        tenant_id: TenantId,
        project_id: ProjectId,
        artifact_id: ArtifactId,
        snapshot_id: SnapshotId,
        storage_volume_id: StorageVolumeId,
        commit_id: ContentDigest,
        relative_root: LogicalPath,
    }

    impl SnapshotFixture {
        fn record(&self) -> SnapshotRecord {
            SnapshotRecord {
                tenant_id: self.tenant_id.clone(),
                project_id: self.project_id.clone(),
                artifact_id: self.artifact_id.clone(),
                snapshot_id: self.snapshot_id.clone(),
                snapshot_request_id: RequestId::new("request-snapshot-recovery").unwrap(),
                commit_id: self.commit_id,
                storage_volume_id: self.storage_volume_id.clone(),
                region: "local".to_owned(),
                state: SnapshotState::Creating,
                phase: SnapshotPhase::Planning,
                relative_root: self.relative_root.as_str().to_owned(),
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            }
        }
    }

    async fn insert_snapshot_fixture(components: &InMemoryComponents) -> SnapshotFixture {
        let project_id = ProjectId::new("project-snapshot-recovery").unwrap();
        let artifact_id = ArtifactId::new("artifact-snapshot-recovery").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-recovery").unwrap();
        let fixture = SnapshotFixture {
            tenant_id: TenantId::new("tenant-snapshot-recovery").unwrap(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            snapshot_id: snapshot_id.clone(),
            storage_volume_id: StorageVolumeId::new("volume-snapshot-recovery").unwrap(),
            commit_id: ContentDigest::hash(b"snapshot-recovery-commit"),
            relative_root: SnapshotMountAssignment::canonical_relative_root(
                &project_id,
                &artifact_id,
                &snapshot_id,
            )
            .unwrap(),
        };
        components
            .control_catalog
            .insert_tenant(TenantRecord {
                tenant_id: fixture.tenant_id.clone(),
                display_name: "Snapshot recovery tenant".to_owned(),
                description: None,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_artifact(ArtifactRecord {
                tenant_id: fixture.tenant_id.clone(),
                project_id: fixture.project_id.clone(),
                artifact_id: fixture.artifact_id.clone(),
                display_name: "Artifact".to_owned(),
                description: None,
                initialization: ArtifactInitialization::Empty,
                head_commit_id: Some(fixture.commit_id),
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id: fixture.tenant_id.clone(),
                storage_volume_id: fixture.storage_volume_id.clone(),
                display_name: "Snapshot volume".to_owned(),
                edge_cluster_id: EdgeClusterId::new("cluster-snapshot-recovery").unwrap(),
                region: "local".to_owned(),
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteMany,
                pvc_reference: Some(CatalogPvcReference {
                    namespace: "default".to_owned(),
                    claim_name: "snapshot-recovery".to_owned(),
                }),
                nfs_reference: None,
                state: StorageVolumeState::Ready,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_snapshot_fenced(SnapshotInsertRequest {
                record: fixture.record(),
                artifact_head: ArtifactHeadExpectation::Any,
            })
            .await
            .unwrap();
        fixture
    }

    fn snapshot_mount_spec(fixture: &SnapshotFixture) -> SnapshotMountSpec {
        let principal = PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("snapshot-recovery-test").unwrap(),
            extensions: Extensions::new(),
        };
        let job_id = deterministic_snapshot_mount_job_id(&fixture.record()).unwrap();
        let index_version: neoengram_protocol::WireIndexVersion =
            IndexVersion::from_snapshot(1, &[]).unwrap().into();
        let operation = SnapshotMountOperation {
            job_id: job_id.clone(),
            principal: principal.clone(),
            tenant_id: fixture.tenant_id.clone(),
            project_id: fixture.project_id.clone(),
            artifact_id: fixture.artifact_id.clone(),
            snapshot_id: fixture.snapshot_id.clone(),
            storage_volume_id: fixture.storage_volume_id.clone(),
            relative_root: fixture.relative_root.clone(),
            commit_id: fixture.commit_id,
            index_version: index_version.clone(),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            extensions: Extensions::new(),
        };
        SnapshotMountSpec {
            job_id,
            principal,
            tenant_id: fixture.tenant_id.clone(),
            project_id: fixture.project_id.clone(),
            artifact_id: fixture.artifact_id.clone(),
            snapshot_id: fixture.snapshot_id.clone(),
            storage_volume_id: fixture.storage_volume_id.clone(),
            relative_root: fixture.relative_root.clone(),
            commit_id: fixture.commit_id,
            index_version,
            request_digest: operation.request_digest().unwrap(),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
        }
    }

    fn snapshot_owner(
        fixture: &SnapshotFixture,
        agent: &str,
        mount: &str,
        mount_generation: u64,
        owner_generation: u64,
        heartbeat_at: u64,
    ) -> AgentRegistryRecord {
        let agent_id = AgentId::new(agent).unwrap();
        let agent_mount_id = AgentMountId::new(mount).unwrap();
        let marker = VolumeMarkerId::new("snapshot-recovery-volume-marker").unwrap();
        AgentRegistryRecord {
            resource_version: ResourceVersion::new(1),
            enrollment: AgentEnrollmentRecord {
                token_id: AgentEnrollmentTokenId::new(format!("token-{agent}")).unwrap(),
                token_request_id: RequestId::new(format!("token-request-{agent}")).unwrap(),
                enrollment_id: AgentEnrollmentId::new(format!("enrollment-{agent}")).unwrap(),
                tenant_id: fixture.tenant_id.clone(),
                edge_cluster_id: EdgeClusterId::new("cluster-snapshot-recovery").unwrap(),
                storage_volume_id: fixture.storage_volume_id.clone(),
                volume_descriptor_digest: ContentDigest::hash(b"snapshot-volume-descriptor"),
                pvc_identity_digest: PvcIdentityDigest::derive("default", "snapshot-recovery")
                    .unwrap(),
                reserved_agent_id: agent_id.clone(),
                reserved_agent_mount_id: agent_mount_id.clone(),
                bootstrap_token_digest: ContentDigest::hash(
                    format!("bootstrap-{agent}").as_bytes(),
                ),
                state: AgentEnrollmentState::Approved,
                created_at_unix_ms: UnixMillis::new(1),
                expires_at_unix_ms: UnixMillis::new(u64::MAX),
                bootstrapped_at_unix_ms: Some(UnixMillis::new(2)),
                bootstrap_request_id: Some(
                    RequestId::new(format!("bootstrap-request-{agent}")).unwrap(),
                ),
                review_expires_at_unix_ms: Some(UnixMillis::new(u64::MAX)),
                decided_at_unix_ms: Some(UnixMillis::new(3)),
                decided_by: None,
                decision_request: None,
                replaces_enrollment_id: None,
                replaced_by_enrollment_id: None,
                extensions: Extensions::new(),
            },
            candidate: None,
            instance: Some(AgentInstanceRecord {
                agent_id: agent_id.clone(),
                installation_id: AgentInstallationId::new(format!("installation-{agent}")).unwrap(),
                public_key_fingerprint: ContentDigest::hash(format!("key-{agent}").as_bytes()),
                agent_version: "test".to_owned(),
                supported_protocol_versions: Vec::<ProtocolVersion>::from([PROTOCOL_VERSION_V1]),
                capabilities: BTreeSet::from(["snapshot_fuse_mount_v1".to_owned()]),
                state: AgentInstanceState::Active,
                session_generation: Some(SessionGeneration::new(1)),
                active_boot_id: Some(AgentBootId::new(format!("boot-{agent}")).unwrap()),
                active_session_id: Some(SessionId::new(format!("session-{agent}")).unwrap()),
                session_open_expected_resource_version: Some(ResourceVersion::new(1)),
                session_opened_at_unix_ms: Some(UnixMillis::new(heartbeat_at)),
                last_heartbeat_at_unix_ms: Some(UnixMillis::new(heartbeat_at)),
                last_sequence: Some(SequenceNumber::new(1)),
            }),
            mount: AgentMountRecord {
                agent_mount_id: agent_mount_id.clone(),
                storage_volume_id: fixture.storage_volume_id.clone(),
                mount_generation: MountGeneration::new(mount_generation),
                expected_volume_marker: marker.clone(),
                desired_access_mode: MountAccessMode::ReadWrite,
                mount_identity_digest: Some(AgentMountIdentityDigest::new(ContentDigest::hash(
                    format!("mount-identity-{mount}").as_bytes(),
                ))),
                observed_volume_marker: Some(marker),
                observed_access_mode: Some(MountAccessMode::ReadWrite),
                reported_health: Some(ResourceHealth::Ready),
                health: ResourceHealth::Ready,
                observed_at_unix_ms: Some(UnixMillis::new(heartbeat_at)),
            },
            owner: VolumeOwnerRecord {
                tenant_id: fixture.tenant_id.clone(),
                storage_volume_id: fixture.storage_volume_id.clone(),
                active_agent_id: Some(agent_id),
                active_agent_mount_id: Some(agent_mount_id),
                owner_generation: OwnerGeneration::new(owner_generation),
                state: VolumeOwnerState::Active,
            },
            decision_audit_event: None,
            storage_enrollment: StorageEnrollmentMetadata::default(),
        }
    }

    async fn complete_snapshot_mount(control: &ControlPlane, assignment: &SnapshotMountAssignment) {
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: assignment.tenant_id.clone(),
                agent_id: assignment.agent_id.clone(),
                report: AgentReport::Accepted(JobAccepted {
                    job_id: assignment.job_id.clone(),
                    assignment_id: assignment.assignment_id.clone(),
                    assignment_generation: assignment.assignment_generation,
                    accepted_at_unix_ms: UnixMillis::new(101),
                    request_digest: assignment.request_digest,
                    extensions: Extensions::new(),
                }),
            })
            .await
            .unwrap();
        report_snapshot_mounted(control, assignment).await;
    }

    async fn report_snapshot_mounted(control: &ControlPlane, assignment: &SnapshotMountAssignment) {
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: assignment.tenant_id.clone(),
                agent_id: assignment.agent_id.clone(),
                report: AgentReport::Progress(mounted_progress(assignment)),
            })
            .await
            .unwrap();
    }

    fn mounted_progress(assignment: &SnapshotMountAssignment) -> JobProgress {
        JobProgress {
            job_id: assignment.job_id.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            state: JobState::Succeeded,
            phase: "mounted".to_owned(),
            files_completed: DecimalU64::new(0),
            bytes_completed: DecimalU64::new(0),
            retry_after_ms: None,
            extensions: Extensions::new(),
        }
    }

    fn snapshot_recovery_failure(assignment: &SnapshotMountAssignment) -> JobFailed {
        let mut extensions = Extensions::new();
        extensions.insert(
            "snapshot_mount_recovery".to_owned(),
            serde_json::Value::Bool(true),
        );
        JobFailed {
            tenant_id: assignment.tenant_id.clone(),
            job_id: assignment.job_id.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            final_state: JobState::Failed,
            failed_at_unix_ms: UnixMillis::new(102),
            stage: JobFailureStage::Execution,
            error: ControlError {
                code: ErrorCode::new("SNAPSHOT_OBJECT_UNAVAILABLE").unwrap(),
                message: "snapshot object is unavailable".to_owned(),
                retryable: true,
                retry_after_ms: None,
                extensions: Extensions::new(),
            },
            extensions,
        }
    }

    async fn assert_snapshot_state(
        components: &InMemoryComponents,
        fixture: &SnapshotFixture,
        expected: SnapshotState,
    ) {
        assert_eq!(
            components
                .control_catalog
                .get_snapshot(&fixture.tenant_id, &fixture.snapshot_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            expected
        );
    }

    fn test_coordinator(
        components: &InMemoryComponents,
        jobs: Arc<dyn JobRepository>,
        authorizer: Arc<dyn Authorizer>,
    ) -> (Arc<ControlPlane>, JobCoordinator) {
        test_coordinator_with_registry(
            components,
            jobs,
            authorizer,
            components.agent_registry.clone(),
        )
    }

    fn test_coordinator_with_registry(
        components: &InMemoryComponents,
        jobs: Arc<dyn JobRepository>,
        authorizer: Arc<dyn Authorizer>,
        registry: Arc<dyn AgentRegistryRepository>,
    ) -> (Arc<ControlPlane>, JobCoordinator) {
        let authority = AuthorityStore::from_parts(
            jobs.clone(),
            components.outbox.clone(),
            components.metadata.clone(),
            components.objects.clone(),
            components.publisher.clone(),
            components.audit.clone(),
            AuthorityCapabilities::IN_MEMORY,
        )
        .with_precommits(components.precommits.clone())
        .with_agent_registry(registry.clone())
        .with_control_catalog(components.control_catalog.clone());
        let control = Arc::new(ControlPlane::new(
            authorizer,
            authority,
            components.clock.clone(),
        ));
        let coordinator = JobCoordinator {
            control: control.clone(),
            jobs,
            catalog: components.control_catalog.clone(),
            registry,
            indexes: components.publisher.clone(),
            precommits: Some(components.precommits.clone()),
            clock: components.clock.clone(),
            heartbeat_timeout_ms: 30_000,
            recovery_cursor: Mutex::new(None),
            precommit_recovery_cursor: Mutex::new(None),
            commit_recovery_cursor: Mutex::new(None),
        };
        (control, coordinator)
    }

    async fn queued_job(
        components: &InMemoryComponents,
        job_id: &str,
        deadline_unix_ms: u64,
    ) -> JobRecord {
        let tenant_id = TenantId::new("tenant-a").unwrap();
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let playground_id = PlaygroundId::new("playground-a").unwrap();
        let expected_index_version = components
            .publisher
            .current_version(&IndexKey {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: playground_id.clone(),
            })
            .await
            .unwrap();
        let principal = PrincipalRef {
            kind: PrincipalKind::User,
            id: PrincipalId::new("user-a").unwrap(),
            extensions: Extensions::new(),
        };
        let mut spec = AddJobSpec {
            job_id: JobId::new(job_id).unwrap(),
            principal,
            tenant_id,
            project_id,
            artifact_id,
            playground_id,
            expected_index_version,
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(deadline_unix_ms),
            paths: vec![LogicalPath::parse("dataset/file.bin").unwrap()],
            all: false,
            extensions: Extensions::new(),
        };
        spec.request_digest = spec.computed_request_digest().unwrap();
        JobRecord {
            spec,
            operation: neoengramd::JobOperation::Add,
            workspace_spec: None,
            snapshot_spec: None,
            state: JobState::Queued,
            resource_version: ResourceVersion::new(1),
            assignment: None,
            workspace_assignment: None,
            snapshot_assignment: None,
            accepted: None,
            progress: None,
            prepared: None,
            publication_candidate: None,
            decision: None,
            finalized: None,
            finalized_ack: None,
            failure: None,
        }
    }

    fn assignment_target() -> AssignmentTarget {
        AssignmentTarget {
            assignment_id: AssignmentId::new("assignment-prepared").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            lease: None,
        }
    }

    fn prepared_metadata(
        spec: &AddJobSpec,
        target: &AssignmentTarget,
    ) -> (JobPrepared, Vec<MetadataBatchPage>) {
        let manifest = Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new()).unwrap();
        let manifest_id = manifest.canonical_id().unwrap();
        let path = LogicalPath::parse("dataset/file.bin").unwrap();
        let result_record = FileRecord::from_manifest(path.clone(), &manifest).unwrap();
        let result_index_digest = IndexVersion::from_snapshot(1, &[result_record])
            .unwrap()
            .digest;
        let scope = MetadataBatchScope {
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id: spec.playground_id.clone(),
            job_id: spec.job_id.clone(),
            base_index_version: spec.expected_index_version.clone(),
            extensions: Extensions::new(),
        };
        let manifest_page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-manifest").unwrap(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::Manifest(vec![ManifestRecord {
                manifest_id,
                total_size: DecimalU64::new(0),
                chunking: WireChunkingStrategy::FastCdc,
                chunk_start: DecimalU64::new(0),
                chunks: Vec::new(),
                extensions: Extensions::new(),
            }]),
            Extensions::new(),
        )
        .unwrap();
        let index_page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-index").unwrap(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::IndexDelta(vec![IndexDeltaRecord::Upsert {
                path,
                manifest_id,
                total_size: DecimalU64::new(0),
                chunk_count: DecimalU64::new(0),
                extensions: Extensions::new(),
            }]),
            Extensions::new(),
        )
        .unwrap();
        let receipt_page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-receipt").unwrap(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::ObjectReceipt(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let pages = vec![manifest_page, index_page, receipt_page];
        let descriptors = pages
            .iter()
            .map(|page| {
                MetadataBatchDescriptor::from_pages(
                    page.batch_id.clone(),
                    scope.clone(),
                    std::slice::from_ref(page),
                    Extensions::new(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let publication_digest = MetadataPublication::from_pages(&pages)
            .unwrap()
            .publication_digest(&scope, result_index_digest)
            .unwrap();
        let prepared = JobPrepared::new(
            spec.job_id.clone(),
            target.assignment_id.clone(),
            target.assignment_generation,
            spec.expected_index_version.clone(),
            result_index_digest,
            publication_digest,
            descriptors,
            Extensions::new(),
        )
        .unwrap();
        (prepared, pages)
    }

    fn spec_key(spec: &AddJobSpec) -> JobKey {
        JobKey::new(spec.tenant_id.clone(), spec.job_id.clone())
    }

    struct TestSnapshotOwnerRegistry {
        current: Mutex<Option<AgentRegistryRecord>>,
    }

    impl TestSnapshotOwnerRegistry {
        fn new(current: AgentRegistryRecord) -> Self {
            Self {
                current: Mutex::new(Some(current)),
            }
        }

        fn set(&self, current: AgentRegistryRecord) {
            *self.current.lock().unwrap() = Some(current);
        }

        fn unsupported<T>() -> CentralResult<T> {
            Err(CentralError::new(
                CentralErrorCode::Internal,
                "unsupported TestSnapshotOwnerRegistry operation",
            ))
        }
    }

    #[async_trait]
    impl AgentRegistryRepository for TestSnapshotOwnerRegistry {
        async fn get(
            &self,
            _enrollment_id: &AgentEnrollmentId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn get_for_tenant(
            &self,
            _tenant_id: &TenantId,
            _enrollment_id: &AgentEnrollmentId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn list_for_tenant(
            &self,
            _request: &neoengramd::AgentEnrollmentListRequest,
        ) -> CentralResult<neoengramd::AgentEnrollmentListPage> {
            Ok(neoengramd::AgentEnrollmentListPage {
                records: Vec::new(),
                next: None,
            })
        }

        async fn get_by_agent(
            &self,
            agent_id: &AgentId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(self
                .current
                .lock()
                .unwrap()
                .as_ref()
                .filter(|record| {
                    record
                        .instance
                        .as_ref()
                        .is_some_and(|instance| &instance.agent_id == agent_id)
                })
                .cloned())
        }

        async fn get_by_token_request_id(
            &self,
            _tenant_id: &TenantId,
            _token_request_id: &RequestId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn get_by_token_digest(
            &self,
            _token_digest: &ContentDigest,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn get_by_bootstrap_request_id(
            &self,
            _tenant_id: &TenantId,
            _bootstrap_request_id: &RequestId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn get_by_decision_request_id(
            &self,
            _tenant_id: &TenantId,
            _decision_request_id: &RequestId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn get_by_installation_id(
            &self,
            _installation_id: &AgentInstallationId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn get_by_public_key_fingerprint(
            &self,
            _public_key_fingerprint: &ContentDigest,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(None)
        }

        async fn get_current_by_volume(
            &self,
            tenant_id: &TenantId,
            storage_volume_id: &StorageVolumeId,
        ) -> CentralResult<Option<AgentRegistryRecord>> {
            Ok(self
                .current
                .lock()
                .unwrap()
                .as_ref()
                .filter(|record| {
                    &record.enrollment.tenant_id == tenant_id
                        && &record.enrollment.storage_volume_id == storage_volume_id
                })
                .cloned())
        }

        async fn get_pvc_binding(
            &self,
            _edge_cluster_id: &EdgeClusterId,
            _pvc_identity_digest: &PvcIdentityDigest,
        ) -> CentralResult<Option<neoengramd::PvcVolumeBinding>> {
            Ok(None)
        }

        async fn expire_stale_token_intents(
            &self,
            _tenant_id: &TenantId,
            _storage_volume_id: &StorageVolumeId,
            _edge_cluster_id: &EdgeClusterId,
            _pvc_identity_digest: &PvcIdentityDigest,
            _now_unix_ms: UnixMillis,
        ) -> CentralResult<usize> {
            Ok(0)
        }

        async fn expire_stale_review_enrollments(
            &self,
            _tenant_id: &TenantId,
            _now_unix_ms: UnixMillis,
        ) -> CentralResult<usize> {
            Ok(0)
        }

        async fn reconcile_expired_enrollments(
            &self,
            _now_unix_ms: UnixMillis,
        ) -> CentralResult<neoengramd::AgentEnrollmentExpiryReconciliation> {
            Ok(neoengramd::AgentEnrollmentExpiryReconciliation::default())
        }

        async fn enrollment_audit_events(
            &self,
        ) -> CentralResult<Vec<neoengramd::AgentEnrollmentAuditEvent>> {
            Ok(Vec::new())
        }

        async fn enrollment_lifecycle_audit_events(
            &self,
            _tenant_id: &TenantId,
        ) -> CentralResult<Vec<neoengramd::AgentEnrollmentLifecycleAuditEvent>> {
            Ok(Vec::new())
        }

        async fn consume_bootstrap_status_signed_at(
            &self,
            _enrollment_id: &AgentEnrollmentId,
            _signed_at_unix_ms: UnixMillis,
        ) -> CentralResult<()> {
            Ok(())
        }

        async fn insert_or_load(
            &self,
            _record: AgentRegistryRecord,
        ) -> CentralResult<neoengramd::AgentRegistryInsertOutcome> {
            Self::unsupported()
        }

        async fn replace(
            &self,
            _expected_resource_version: u64,
            _record: AgentRegistryRecord,
        ) -> CentralResult<AgentRegistryRecord> {
            Self::unsupported()
        }

        async fn activate_replacement(
            &self,
            _expected_previous_resource_version: u64,
            _revoked: AgentRegistryRecord,
            _expected_replacement_resource_version: u64,
            _replacement: AgentRegistryRecord,
        ) -> CentralResult<neoengramd::AgentRegistryReplacementRecords> {
            Self::unsupported()
        }
    }

    struct PoisonReplaceRepository {
        inner: Arc<InMemoryJobRepository>,
        poisoned: JobKey,
    }

    #[async_trait]
    impl JobRepository for PoisonReplaceRepository {
        async fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
            self.inner.get(key).await
        }

        async fn list_recoverable(
            &self,
            after: Option<&JobKey>,
            now: UnixMillis,
            limit: usize,
        ) -> CentralResult<Vec<JobRecord>> {
            self.inner.list_recoverable(after, now, limit).await
        }

        async fn list_pending_decisions_for_agent(
            &self,
            agent_id: &AgentId,
            limit: usize,
        ) -> CentralResult<Vec<JobRecord>> {
            self.inner
                .list_pending_decisions_for_agent(agent_id, limit)
                .await
        }

        async fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
            self.inner.insert_or_load(job).await
        }

        async fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
            if job.key() == self.poisoned {
                return Err(CentralError::new(
                    CentralErrorCode::StorageFailure,
                    "injected poisoned Job write",
                ));
            }
            self.inner.replace(expected, job).await
        }
    }

    #[derive(Default)]
    struct RevocableFinalizeAuthorizer {
        revoked: std::sync::atomic::AtomicBool,
    }

    impl RevocableFinalizeAuthorizer {
        fn revoke(&self) {
            self.revoked
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Authorizer for RevocableFinalizeAuthorizer {
        async fn authorize(&self, request: &AuthorizationRequest) -> CentralResult<()> {
            if request.action == Action::FinalizeAdd
                && self.revoked.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CentralError::new(
                    CentralErrorCode::Unauthorized,
                    "principal finalize permission was revoked",
                ));
            }
            Ok(())
        }
    }
}
