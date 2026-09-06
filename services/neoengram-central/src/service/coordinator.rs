use std::sync::{Arc, Mutex};

use crate::{
    AddJobSpec, AgentRegistryRepository, AssignJobRequest, AssignSnapshotDeliveryRequest,
    AssignWorkspaceMaterializationRequest, AssignmentTarget, CentralError, CentralErrorCode,
    CentralResult, Clock, ControlCatalogRepository, ControlPlane,
    CreateWorkspaceMaterializationRequest, ExpireAddJobRequest, IndexKey, IndexPublisher,
    InitializeIndexSnapshotRequest, JobKey, JobOperation, JobRecord, JobRepository, PreCommitKey,
    PreCommitRecord, PreCommitRepository, PreCommitState, ResumePublicationRequest,
    SnapshotDeliveryRecord, SnapshotDeliverySpec, SnapshotDeliveryTarget, SnapshotState,
    TenantListRequest, WorkspaceListRequest, WorkspaceMaterializeSpec, WorkspaceMaterializeTarget,
    WorkspaceRecord, WorkspaceState,
};
use neoengram_domain::core::{CommitId, IndexVersion};
use neoengram_domain::protocol::{
    AddOperation, AgentId, AgentMountId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    Extensions, JobId, JobState, MountGeneration, OwnerGeneration, PlacementGeneration,
    PrincipalId, PrincipalKind, PrincipalRef, SnapshotDeliveryAction, SnapshotDeliveryMode,
    SnapshotDeliveryOperation, TaskId, UnixMillis, WorkspaceMaterializeAssignment,
    WorkspaceMaterializeOperation,
};

const DEFAULT_RECOVERY_BATCH_SIZE: usize = 1_024;
const WORKSPACE_MATERIALIZE_DEADLINE_MS: u64 = 24 * 60 * 60 * 1_000;
const PRECOMMIT_JOB_DEADLINE_MS: u64 = 24 * 60 * 60 * 1_000;
const SNAPSHOT_DELIVERY_DEADLINE_MS: u64 = 5 * 60 * 1_000;

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
        authority: &crate::AuthorityStore,
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

    /// Rejects fabricated Workspace scope and stale client Index defaults before Job creation.
    pub async fn validate_spec(&self, spec: &crate::AddJobSpec) -> CentralResult<()> {
        if self
            .jobs
            .get(&JobKey::new(spec.tenant_id.clone(), spec.job_id.clone()))
            .await?
            .is_some()
        {
            // Preserve idempotency when the runtime owner later disappears. The control plane
            // still compares the immutable persisted payload before returning the replay.
            return Ok(());
        }
        validate_job_spec(
            self.jobs.as_ref(),
            self.catalog.as_ref(),
            self.indexes.as_ref(),
            spec,
        )
        .await?;
        self.validate_live_storage(spec).await
    }

    /// Rejects a new Agent-backed operation before it creates a durable queued Job.
    pub async fn validate_live_storage(&self, spec: &crate::AddJobSpec) -> CentralResult<()> {
        let workspace = self
            .catalog
            .get_workspace(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.workspace_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "the requested Workspace is not visible or does not exist",
                )
                .with_retryable(false)
            })?;
        if !workspace.lifecycle.is_active() {
            return Err(resource_lifecycle_fenced());
        }
        let Some(owner) = self
            .registry
            .get_current_by_volume(&spec.tenant_id, &workspace.storage_volume_id)
            .await?
        else {
            return Err(storage_volume_unavailable());
        };
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != crate::DerivedVolumeState::Ready
        {
            return Err(storage_volume_unavailable());
        }
        Ok(())
    }

    /// Performs one immediate scheduling attempt. Lack of a Ready owner leaves the Job queued.
    pub async fn schedule(&self, job: &JobRecord) -> CentralResult<Option<JobRecord>> {
        if job.operation == JobOperation::SnapshotDelivery {
            return self.schedule_snapshot_delivery(job).await;
        }
        if job.state != JobState::Queued {
            return Ok(None);
        }
        if job.operation == JobOperation::WorkspaceMaterialize {
            return self.schedule_workspace_materialization(job).await;
        }
        let workspace = self
            .catalog
            .get_workspace(
                &job.spec.tenant_id,
                &job.spec.project_id,
                &job.spec.artifact_id,
                &job.spec.workspace_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "the Job's Workspace no longer exists",
                )
            })?;
        if !workspace.lifecycle.is_active() {
            return Ok(None);
        }
        let volume = self
            .catalog
            .get_storage_volume(&job.spec.tenant_id, &workspace.storage_volume_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::StorageVolumeNotFound,
                    "the Job's StorageVolume no longer exists",
                )
            })?;
        if !volume.lifecycle.is_active() {
            return Ok(None);
        }
        let current = self.indexes.current_version(&job.index_key()).await?;
        if !same_index_version(&current, &job.spec.expected_index_version) {
            return Err(CentralError::new(
                CentralErrorCode::MetadataInvalid,
                "the Job's expected IndexVersion no longer matches its Workspace",
            ));
        }
        let Some(owner) = self
            .registry
            .get_current_by_volume(&job.spec.tenant_id, &workspace.storage_volume_id)
            .await?
        else {
            return Ok(None);
        };
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != crate::DerivedVolumeState::Ready
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
            storage_volume_id: workspace.storage_volume_id,
            artifact_placement_id: deterministic_placement_id(job, &owner.mount.storage_volume_id)?,
            placement_generation: PlacementGeneration::new(1),
            agent_mount_id: owner.mount.agent_mount_id.clone(),
            mount_generation: owner.mount.mount_generation,
            owner_generation: owner.owner.owner_generation,
            max_whole_file_bytes: volume.max_whole_file_bytes,
            lease: None,
        };
        let result = Box::pin(self.control.assign_job(AssignJobRequest {
            actor: job.spec.principal.clone(),
            tenant_id: job.spec.tenant_id.clone(),
            job_id: job.spec.job_id.clone(),
            target,
        }))
        .await?;
        Ok(Some(result.job))
    }

    /// Idempotently creates the durable materialization Job for one Creating Workspace and
    /// performs its first owner-selection attempt.
    pub async fn ensure_workspace_materialization(
        &self,
        workspace: &WorkspaceRecord,
    ) -> CentralResult<JobRecord> {
        if !workspace.lifecycle.is_active() {
            return Err(resource_lifecycle_fenced());
        }
        if workspace.state != WorkspaceState::Creating {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "only a Creating Workspace can be materialized",
            ));
        }
        let job_id = deterministic_materialization_job_id(workspace)?;
        let relative_root = WorkspaceMaterializeAssignment::canonical_relative_root(
            &workspace.project_id,
            &workspace.artifact_id,
            &workspace.workspace_id,
        )?;
        if relative_root.as_str() != workspace.relative_root {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Workspace relative_root differs from the canonical materialization path",
            ));
        }
        let index_key = IndexKey {
            tenant_id: workspace.tenant_id.clone(),
            project_id: workspace.project_id.clone(),
            artifact_id: workspace.artifact_id.clone(),
            workspace_id: workspace.workspace_id.clone(),
        };
        let base_index_version = if let Some(base_commit_id) = workspace.base_commit_id {
            let precommits = self.precommits.as_ref().ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Commit authority is unavailable for Workspace materialization",
                )
            })?;
            let commit = precommits
                .get_commit(
                    &workspace.tenant_id,
                    &workspace.project_id,
                    &workspace.artifact_id,
                    CommitId::from_digest(base_commit_id),
                )
                .await?
                .ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::MetadataInvalid,
                        "the Workspace base Commit is missing from authority",
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
                    format!("cannot construct the empty Workspace Index: {error}"),
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
            workspace
                .created_at_unix_ms
                .get()
                .saturating_add(WORKSPACE_MATERIALIZE_DEADLINE_MS),
        );
        let operation_request_id = neoengram_domain::protocol::RequestId::new(format!(
            "workspace-create-{}",
            workspace.workspace_id
        ))?;
        let operation_task_id = task_id_for_request(&workspace.tenant_id, &operation_request_id)?;
        let principal = PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("workspace-materializer")?,
            extensions: Extensions::new(),
        };
        let operation = WorkspaceMaterializeOperation {
            job_id: job_id.clone(),
            principal: principal.clone(),
            tenant_id: workspace.tenant_id.clone(),
            project_id: workspace.project_id.clone(),
            artifact_id: workspace.artifact_id.clone(),
            workspace_id: workspace.workspace_id.clone(),
            storage_volume_id: workspace.storage_volume_id.clone(),
            relative_root: relative_root.clone(),
            base_commit_id: workspace.base_commit_id,
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
                    tenant_id: workspace.tenant_id.clone(),
                    project_id: workspace.project_id.clone(),
                    artifact_id: workspace.artifact_id.clone(),
                    workspace_id: workspace.workspace_id.clone(),
                    storage_volume_id: workspace.storage_volume_id.clone(),
                    relative_root,
                    base_commit_id: workspace.base_commit_id,
                    base_index_version,
                    request_digest,
                    deadline_unix_ms,
                    operation_task_id: Some(operation_task_id),
                },
            })
            .await?;
        match self.schedule(&created.job).await? {
            Some(assigned) => Ok(assigned),
            None => Ok(created.job),
        }
    }

    /// Idempotently creates and dispatches the Agent-backed Job for one SnapshotDelivery.
    /// Delivery creation is committed in the Catalog first; this method closes the separate
    /// Authority write window by deterministically re-deriving the Job from the Delivery record.
    pub async fn ensure_snapshot_delivery(
        &self,
        delivery: &SnapshotDeliveryRecord,
    ) -> CentralResult<JobRecord> {
        if matches!(
            delivery.state,
            neoengram_domain::protocol::SnapshotDeliveryState::Deleted
        ) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "deleted SnapshotDelivery cannot be scheduled",
            ));
        }
        let snapshot = self
            .catalog
            .get_snapshot(&delivery.tenant_id, &delivery.snapshot_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(CentralErrorCode::JobNotFound, "Snapshot no longer exists")
            })?;
        // A Delivery is an immutable child of exactly one Snapshot.  Do not derive a Job from
        // an otherwise valid-looking Delivery until every identity field has been checked; a
        // stale or corrupted child must fail closed instead of being dispatched against the
        // wrong Commit, Volume, or mode.
        if snapshot.tenant_id != delivery.tenant_id
            || snapshot.snapshot_id != delivery.snapshot_id
            || snapshot.delivery_id != delivery.delivery_id
            || snapshot.commit_id != delivery.commit_id
            || snapshot.storage_volume_id != delivery.storage_volume_id
            || snapshot.delivery_mode != delivery.mode
            || snapshot.snapshot_request_id != delivery.create_request_id
        {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot and Delivery immutable identities do not match",
            )
            .with_retryable(false));
        }
        if !snapshot.lifecycle.is_active()
            || !matches!(
                snapshot.state,
                SnapshotState::Creating | SnapshotState::Ready
            )
        {
            return Err(resource_lifecycle_fenced());
        }
        let volume = self
            .catalog
            .get_storage_volume(&delivery.tenant_id, &delivery.storage_volume_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "StorageVolume no longer exists",
                )
            })?;
        if !volume.lifecycle.is_active() || volume.state != crate::StorageVolumeState::Ready {
            return Err(resource_lifecycle_fenced());
        }
        if snapshot.edge_cluster_id != volume.edge_cluster_id {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Snapshot target EdgeCluster does not match its StorageVolume",
            )
            .with_retryable(false));
        }
        let job_id = deterministic_snapshot_delivery_job_id(delivery)?;
        if let Some(existing) = self
            .jobs
            .get(&JobKey::new(delivery.tenant_id.clone(), job_id.clone()))
            .await?
        {
            return match self.schedule_snapshot_delivery(&existing).await? {
                Some(assigned) => Ok(assigned),
                None => Ok(existing),
            };
        }
        let deadline_unix_ms = UnixMillis::new(
            self.clock
                .now()
                .get()
                .saturating_add(SNAPSHOT_DELIVERY_DEADLINE_MS),
        );
        let principal = PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("snapshot-delivery")?,
            extensions: Extensions::new(),
        };
        let action =
            if delivery.state == neoengram_domain::protocol::SnapshotDeliveryState::Deleting {
                SnapshotDeliveryAction::Delete
            } else {
                SnapshotDeliveryAction::Materialize
            };
        let operation = SnapshotDeliveryOperation {
            job_id: job_id.clone(),
            action,
            principal: principal.clone(),
            tenant_id: delivery.tenant_id.clone(),
            project_id: snapshot.project_id.clone(),
            artifact_id: snapshot.artifact_id.clone(),
            snapshot_id: delivery.snapshot_id.clone(),
            delivery_id: delivery.delivery_id.clone(),
            commit_id: delivery.commit_id,
            storage_volume_id: delivery.storage_volume_id.clone(),
            mode: delivery.mode,
            snapshot_size_bytes: neoengram_domain::protocol::DecimalU64::new(delivery.size_bytes),
            copy_reserve_bytes: volume.copy_reserve_bytes,
            hardlink_policy: volume.hardlink_policy,
            target_relative_root: delivery.target_relative_root.clone(),
            source_index_digest: delivery.source_index_digest,
            deadline_unix_ms,
            extensions: Extensions::new(),
        };
        let request_digest = operation.request_digest()?;
        let operation_task_id =
            task_id_for_request(&delivery.tenant_id, &delivery.create_request_id)?;
        let created = self
            .control
            .create_snapshot_delivery(crate::CreateSnapshotDeliveryRequest {
                spec: SnapshotDeliverySpec {
                    job_id,
                    action,
                    principal,
                    tenant_id: delivery.tenant_id.clone(),
                    project_id: snapshot.project_id,
                    artifact_id: snapshot.artifact_id,
                    snapshot_id: delivery.snapshot_id.clone(),
                    delivery_id: delivery.delivery_id.clone(),
                    commit_id: delivery.commit_id,
                    storage_volume_id: delivery.storage_volume_id.clone(),
                    mode: delivery.mode,
                    snapshot_size_bytes: neoengram_domain::protocol::DecimalU64::new(
                        delivery.size_bytes,
                    ),
                    copy_reserve_bytes: volume.copy_reserve_bytes,
                    hardlink_policy: volume.hardlink_policy,
                    target_relative_root: delivery.target_relative_root.clone(),
                    source_index_digest: delivery.source_index_digest,
                    delivery_generation: delivery.delivery_generation,
                    request_digest,
                    deadline_unix_ms,
                    operation_task_id: Some(operation_task_id),
                },
            })
            .await?;
        match self.schedule_snapshot_delivery(&created.job).await? {
            Some(assigned) => Ok(assigned),
            None => Ok(created.job),
        }
    }

    /// Performs the synchronous availability decision required before a Delivery is persisted.
    /// A queued Delivery without a current owner/capability would otherwise remain requested
    /// forever and would make an unsupported mode look accepted to callers.
    pub async fn preflight_snapshot_delivery(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
        mode: SnapshotDeliveryMode,
    ) -> CentralResult<()> {
        let owner = self
            .registry
            .get_current_by_volume(tenant_id, storage_volume_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::SnapshotDeliveryUnavailable,
                    "no current Agent owner is available for the StorageVolume",
                )
                .with_retryable(true)
            })?;
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != crate::DerivedVolumeState::Ready
        {
            return Err(CentralError::new(
                CentralErrorCode::SnapshotDeliveryUnavailable,
                "StorageVolume owner is not ready",
            )
            .with_retryable(true));
        }
        let instance = owner.instance.as_ref().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::SnapshotDeliveryUnavailable,
                "StorageVolume has no active Agent instance",
            )
            .with_retryable(true)
        })?;
        if owner.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
            || owner.owner.active_agent_mount_id.as_ref() != Some(&owner.mount.agent_mount_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::SnapshotDeliveryUnavailable,
                "StorageVolume owner and mount generations are not ready",
            )
            .with_retryable(true));
        }
        let capability = match mode {
            SnapshotDeliveryMode::Fuse => "snapshot_delivery_fuse_v2",
            SnapshotDeliveryMode::Copy => "snapshot_delivery_copy_v2",
            SnapshotDeliveryMode::Hardlink => "snapshot_delivery_hardlink_v2",
        };
        if !instance.capabilities.contains(capability) {
            return Err(CentralError::new(
                CentralErrorCode::SnapshotDeliveryUnsupported,
                format!("Agent does not advertise required capability {capability}"),
            )
            .with_retryable(false));
        }
        Ok(())
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
        let job = match Box::pin(self.schedule(&created.job)).await? {
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
        let workspace = self
            .catalog
            .get_workspace(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.workspace_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "materialization Workspace no longer exists",
                )
            })?;
        if !workspace.lifecycle.is_active() {
            return Ok(None);
        }
        if workspace.state != WorkspaceState::Creating {
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
            != crate::DerivedVolumeState::Ready
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

    async fn schedule_snapshot_delivery(
        &self,
        job: &JobRecord,
    ) -> CentralResult<Option<JobRecord>> {
        let spec = job.delivery_spec.as_ref().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::Internal,
                "SnapshotDelivery Job lost its immutable spec",
            )
        })?;
        let delivery = self
            .catalog
            .get_snapshot_delivery(&spec.tenant_id, &spec.delivery_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "SnapshotDelivery record no longer exists",
                )
            })?;
        let expected_action = match delivery.state {
            neoengram_domain::protocol::SnapshotDeliveryState::Requested
            | neoengram_domain::protocol::SnapshotDeliveryState::Validating
            | neoengram_domain::protocol::SnapshotDeliveryState::Materializing => {
                SnapshotDeliveryAction::Materialize
            }
            neoengram_domain::protocol::SnapshotDeliveryState::Deleting => {
                SnapshotDeliveryAction::Delete
            }
            neoengram_domain::protocol::SnapshotDeliveryState::Ready
            | neoengram_domain::protocol::SnapshotDeliveryState::Failed
            | neoengram_domain::protocol::SnapshotDeliveryState::Deleted => return Ok(None),
        };
        if delivery.delivery_generation != spec.delivery_generation
            || spec.action != expected_action
        {
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
            != crate::DerivedVolumeState::Ready
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
        let capability = match spec.mode {
            SnapshotDeliveryMode::Fuse => "snapshot_delivery_fuse_v2",
            SnapshotDeliveryMode::Copy => "snapshot_delivery_copy_v2",
            SnapshotDeliveryMode::Hardlink => "snapshot_delivery_hardlink_v2",
        };
        if !instance.capabilities.contains(capability) {
            return Ok(None);
        }
        let (assignment_generation, assignment_id) = match job.delivery_assignment.as_ref() {
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
                            "SnapshotDelivery assignment generation exhausted",
                        )
                    })?;
                let generation = AssignmentGeneration::new(next);
                let assignment_id = deterministic_snapshot_delivery_assignment_id(
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
                let assignment_id = deterministic_snapshot_delivery_assignment_id(
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
            .assign_snapshot_delivery(AssignSnapshotDeliveryRequest {
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: SnapshotDeliveryTarget {
                    assignment_id,
                    assignment_generation,
                    agent_id: instance.agent_id.clone(),
                    storage_volume_id: spec.storage_volume_id.clone(),
                    agent_mount_id: owner.mount.agent_mount_id.clone(),
                    mount_generation: owner.mount.mount_generation,
                    owner_generation: owner.owner.owner_generation,
                    placement_generation: PlacementGeneration::new(1),
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
        // A crash can occur after the catalog commits a Creating Workspace but before its
        // deterministic materialization Job is inserted. Re-deriving the Job from catalog
        // identity closes that cross-SQLite window.
        self.recover_creating_workspaces(limit).await?;
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
        super::workspace_commit::publish_committed_workspace_head(
            self.catalog.as_ref(),
            precommits,
            &commit,
        )
        .await?;
        precommits
            .acknowledge_head_publication(&precommit.key(), commit.commit_id, self.clock.now())
            .await?;
        Ok(())
    }

    async fn recover_creating_workspaces(&self, limit: usize) -> CentralResult<()> {
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
                let mut workspace_cursor = None;
                loop {
                    let workspaces = self
                        .catalog
                        .list_workspaces(&WorkspaceListRequest {
                            tenant_id: tenant.tenant_id.clone(),
                            project_id: None,
                            artifact_id: None,
                            region: None,
                            state: Some(WorkspaceState::Creating),
                            query: None,
                            after: workspace_cursor.clone(),
                            limit: u16::try_from(remaining.min(100)).unwrap_or(100),
                        })
                        .await?;
                    for workspace in &workspaces.records {
                        if workspace
                            .created_at_unix_ms
                            .get()
                            .saturating_add(WORKSPACE_MATERIALIZE_DEADLINE_MS)
                            <= self.clock.now().get()
                        {
                            let _ = self
                                .catalog
                                .transition_workspace_state(
                                    &workspace.tenant_id,
                                    &workspace.project_id,
                                    &workspace.artifact_id,
                                    &workspace.workspace_id,
                                    WorkspaceState::Creating,
                                    WorkspaceState::Abnormal,
                                    self.clock.now(),
                                )
                                .await?;
                            remaining = remaining.saturating_sub(1);
                            if remaining == 0 {
                                return Ok(());
                            }
                            continue;
                        }
                        let _ = self.ensure_workspace_materialization(workspace).await?;
                        remaining = remaining.saturating_sub(1);
                        if remaining == 0 {
                            return Ok(());
                        }
                    }
                    workspace_cursor = workspaces.next;
                    if workspace_cursor.is_none() {
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
        now: neoengram_domain::protocol::UnixMillis,
        run: &mut CoordinatorRun,
    ) -> CentralResult<()> {
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
        // SnapshotDelivery uses its own immutable Delivery spec and Agent report state. It is
        // intentionally excluded from the generic Add/PreCommit recovery path, which expects a
        // Workspace-backed Index and would otherwise recurse through the wrong synchronizer.
        if job.operation == JobOperation::SnapshotDelivery {
            if job.state.is_terminal() {
                return Ok(());
            }
            if job.state == JobState::Queued {
                match self.schedule_snapshot_delivery(job).await {
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
        workspace_id: precommit.workspace_id.clone(),
        expected_index_version: precommit.source_index_version.clone(),
        data_layout: precommit.data_layout,
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
        workspace_id: precommit.workspace_id.clone(),
        expected_index_version: precommit.source_index_version.clone(),
        data_layout: precommit.data_layout,
        request_digest: operation.request_digest()?,
        deadline_unix_ms,
        paths: Vec::new(),
        all: true,
        operation_task_id: Some(task_id_for_request(
            &precommit.tenant_id,
            &precommit.precommit_request_id,
        )?),
        extensions: Extensions::new(),
    })
}

fn task_id_for_request(
    tenant_id: &neoengram_domain::protocol::TenantId,
    request_id: &neoengram_domain::protocol::RequestId,
) -> CentralResult<TaskId> {
    let digest = blake3::hash(format!("operation-task\0{tenant_id}\0{request_id}").as_bytes());
    TaskId::new(format!("task-{digest}")).map_err(Into::into)
}

pub(crate) async fn validate_job_spec(
    jobs: &dyn JobRepository,
    catalog: &dyn ControlCatalogRepository,
    indexes: &dyn IndexPublisher,
    spec: &crate::AddJobSpec,
) -> CentralResult<()> {
    if jobs
        .get(&crate::JobKey::new(
            spec.tenant_id.clone(),
            spec.job_id.clone(),
        ))
        .await?
        .is_some()
    {
        // Exact replay and JobId reuse are decided against the immutable persisted spec.
        return Ok(());
    }
    let workspace = catalog
        .get_workspace(
            &spec.tenant_id,
            &spec.project_id,
            &spec.artifact_id,
            &spec.workspace_id,
        )
        .await?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::JobNotFound,
                "the requested Workspace is not visible or does not exist",
            )
            .with_retryable(false)
        })?;
    if !workspace.lifecycle.is_active() {
        return Err(resource_lifecycle_fenced());
    }
    let current = indexes
        .current_version(&IndexKey {
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            workspace_id: spec.workspace_id.clone(),
        })
        .await?;
    if !same_index_version(&current, &spec.expected_index_version) {
        return Err(CentralError::new(
            CentralErrorCode::MetadataInvalid,
            "expected_index_version differs from the authoritative Workspace IndexVersion",
        )
        .with_retryable(false));
    }
    Ok(())
}

fn same_index_version(
    left: &neoengram_domain::protocol::WireIndexVersion,
    right: &neoengram_domain::protocol::WireIndexVersion,
) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn storage_volume_unavailable() -> CentralError {
    CentralError::new(
        CentralErrorCode::StorageVolumeNotReady,
        "the Workspace StorageVolume has no reachable Ready Agent owner",
    )
    .with_retryable(true)
}

fn resource_lifecycle_fenced() -> CentralError {
    CentralError::new(
        CentralErrorCode::InvalidState,
        "the resource is not active and cannot accept or schedule jobs",
    )
    .with_retryable(false)
}

fn deterministic_assignment_id(job: &JobRecord) -> CentralResult<AssignmentId> {
    let input = format!("{}\0{}", job.spec.tenant_id, job.spec.job_id);
    AssignmentId::new(format!(
        "assignment-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_materialization_job_id(workspace: &WorkspaceRecord) -> CentralResult<JobId> {
    let input = format!(
        "{}\0{}\0{}\0{}",
        workspace.tenant_id, workspace.project_id, workspace.artifact_id, workspace.workspace_id
    );
    JobId::new(format!(
        "materialize-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_snapshot_delivery_job_id(
    delivery: &SnapshotDeliveryRecord,
) -> CentralResult<JobId> {
    let input = format!(
        "{}\0{}\0{}\0{}",
        delivery.tenant_id,
        delivery.snapshot_id,
        delivery.delivery_id,
        delivery.delivery_generation
    );
    JobId::new(format!(
        "snapshot-delivery-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_snapshot_delivery_assignment_id(
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
        "snapshot-delivery-assignment-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_placement_id(
    job: &JobRecord,
    volume_id: &neoengram_domain::protocol::StorageVolumeId,
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

    use crate::{
        Action, AddJobSpec, AgentEnrollmentRecord, AgentInstanceRecord, AgentInstanceState,
        AgentMountRecord, AgentRegistryRecord, AgentReport, ArtifactHeadExpectation,
        ArtifactInitialization, ArtifactRecord, AssignmentOutbox, AuthorityCapabilities,
        AuthorityStore, AuthorizationRequest, Authorizer, CatalogPvcReference, CreateAddJobRequest,
        FinalizeAddRequest, InMemoryComponents, InMemoryJobRepository, JobInsertOutcome,
        MetadataBatchSubmission, PlacementRepository, ReceiveReportRequest,
        SnapshotDeliveryInsertRequest, SnapshotInsertRequest, SnapshotRecord,
        SnapshotWithDeliveryInsertRequest, StageMetadataBatchRequest, StorageAccessMode,
        StorageBackendType, StorageEnrollmentMetadata, StorageVolumeRecord, StorageVolumeState,
        TenantRecord, VolumeOwnerRecord, VolumeOwnerState,
    };
    use async_trait::async_trait;
    use neoengram_domain::core::{
        ChunkingStrategy, ContentDigest, FileRecord, IndexVersion, LogicalPath, Manifest,
    };
    use neoengram_domain::protocol::{
        AgentBootId, AgentEnrollmentId, AgentEnrollmentState, AgentEnrollmentTokenId, AgentId,
        AgentInstallationId, AgentMountId, AgentMountIdentityDigest, ArtifactId,
        AssignmentGeneration, AssignmentId, DecimalU64, DeliveryGeneration, EdgeClusterId,
        Extensions, Generation, IndexDeltaRecord, JobAccepted, JobId, JobPrepared, JobProgress,
        ManifestRecord, MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage,
        MetadataBatchRecords, MetadataBatchScope, MetadataPublication, MountAccessMode,
        MountGeneration, OwnerGeneration, PlacementGeneration, PrincipalId, PrincipalKind,
        PrincipalRef, ProjectId, PvcIdentityDigest, RequestId, ResourceHealth, ResourceLifecycle,
        ResourceVersion, SequenceNumber, SessionGeneration, SessionId, SnapshotDeliveryId,
        SnapshotDeliveryState, SnapshotId, StorageVolumeId, TaskExecutionFence, TaskId, TenantId,
        UnixMillis, VolumeMarkerId, WireChunkingStrategy, WorkspaceId, CURRENT_WIRE_VERSION,
    };

    use super::*;

    fn test_task_fence(job_id: &JobId, stage_key: &str) -> TaskExecutionFence {
        TaskExecutionFence::new(
            TaskId::new(format!("task-{job_id}")).unwrap(),
            Generation::new(1),
            stage_key,
            Generation::new(1),
            Generation::new(1),
        )
    }

    #[tokio::test]
    async fn empty_workspace_materialization_creates_a_real_empty_index_marker() {
        let components = InMemoryComponents::new(100);
        let tenant_id = TenantId::new("tenant-empty-materialize").unwrap();
        let project_id = ProjectId::new("project-empty-materialize").unwrap();
        let artifact_id = ArtifactId::new("artifact-empty-materialize").unwrap();
        let workspace_id = WorkspaceId::new("workspace-empty-materialize").unwrap();
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
                lifecycle: ResourceLifecycle::active(),
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
                allowed_delivery_modes: vec![
                    neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
                    neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
                ],
                hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
                max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
                copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
                pvc_reference: Some(CatalogPvcReference {
                    namespace: "default".to_owned(),
                    claim_name: "empty-materialize".to_owned(),
                }),
                nfs_reference: None,
                state: StorageVolumeState::Ready,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: UnixMillis::new(100),
                updated_at_unix_ms: UnixMillis::new(100),
            })
            .await
            .unwrap();
        let workspace = WorkspaceRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            workspace_id: workspace_id.clone(),
            storage_volume_id,
            region: "local".to_owned(),
            display_name: "Workspace".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: WorkspaceState::Creating,
            resource_version: 1,
            lifecycle: ResourceLifecycle::active(),
            relative_root: format!("workspaces/{project_id}/{artifact_id}/{workspace_id}"),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
        };
        components
            .control_catalog
            .insert_workspace(workspace.clone())
            .await
            .unwrap();
        let (_, coordinator) = test_coordinator(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
        );

        let job = coordinator
            .ensure_workspace_materialization(&workspace)
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
                    workspace_id,
                },
                version: IndexVersion::from_snapshot(1, &[]).unwrap().into(),
                records: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(conflict.code(), CentralErrorCode::ConcurrentUpdate);
    }

    #[tokio::test]
    async fn snapshot_delivery_preflight_requires_the_current_agent_capability() {
        let components = InMemoryComponents::new(100);
        let registry = Arc::new(TestSnapshotOwnerRegistry::new(snapshot_owner_record(
            BTreeSet::new(),
        )));
        let (_, coordinator) = test_coordinator_with_registry(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
            registry.clone(),
        );
        let tenant_id = TenantId::new("tenant-delivery-preflight").unwrap();
        let volume_id = StorageVolumeId::new("volume-delivery-preflight").unwrap();

        let unsupported = coordinator
            .preflight_snapshot_delivery(&tenant_id, &volume_id, SnapshotDeliveryMode::Copy)
            .await
            .unwrap_err();
        assert_eq!(
            unsupported.code(),
            CentralErrorCode::SnapshotDeliveryUnsupported
        );
        assert!(!unsupported.retryable());

        registry.set(snapshot_owner_record(BTreeSet::from([
            "snapshot_delivery_copy_v2".to_owned(),
        ])));
        coordinator
            .preflight_snapshot_delivery(&tenant_id, &volume_id, SnapshotDeliveryMode::Copy)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn succeeded_snapshot_delivery_is_not_reactivated_by_scheduling_replay() {
        let components = InMemoryComponents::new(100);
        let delivery = seed_snapshot_delivery(&components).await;
        let owner = snapshot_owner_record(BTreeSet::from(["snapshot_delivery_copy_v2".to_owned()]));
        let agent_id = owner.instance.as_ref().unwrap().agent_id.clone();
        let registry = Arc::new(TestSnapshotOwnerRegistry::new(owner));
        let (control, coordinator) = test_coordinator_with_registry(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
            registry,
        );
        let assigned = coordinator
            .ensure_snapshot_delivery(&delivery)
            .await
            .unwrap();
        let assignment = assigned.delivery_assignment.clone().unwrap();
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: delivery.tenant_id.clone(),
                agent_id: agent_id.clone(),
                report: AgentReport::Accepted(JobAccepted {
                    job_id: assignment.job_id.clone(),
                    task_fence: assignment.task_fence.clone(),
                    assignment_id: assignment.assignment_id.clone(),
                    assignment_generation: assignment.assignment_generation,
                    accepted_at_unix_ms: UnixMillis::new(101),
                    request_digest: assignment.request_digest,
                    extensions: Extensions::new(),
                }),
            })
            .await
            .unwrap();
        let succeeded = control
            .receive_report(ReceiveReportRequest {
                tenant_id: delivery.tenant_id.clone(),
                agent_id: agent_id.clone(),
                report: AgentReport::Progress(JobProgress {
                    job_id: assignment.job_id,
                    task_fence: assignment.task_fence.clone(),
                    assignment_id: assignment.assignment_id,
                    assignment_generation: assignment.assignment_generation,
                    state: JobState::Succeeded,
                    phase: "materialized".to_owned(),
                    files_completed: DecimalU64::new(delivery.file_count),
                    bytes_completed: DecimalU64::new(delivery.size_bytes),
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                }),
            })
            .await
            .unwrap()
            .job;

        assert!(components
            .outbox
            .pending_for_agent(&agent_id, 10)
            .await
            .unwrap()
            .is_empty());
        assert!(coordinator
            .schedule_snapshot_delivery(&succeeded)
            .await
            .unwrap()
            .is_none());
        assert!(components
            .outbox
            .pending_for_agent(&agent_id, 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn stale_snapshot_delivery_generation_cannot_reactivate_materialization() {
        let components = InMemoryComponents::new(100);
        let delivery = seed_snapshot_delivery(&components).await;
        let owner = snapshot_owner_record(BTreeSet::from(["snapshot_delivery_copy_v2".to_owned()]));
        let agent_id = owner.instance.as_ref().unwrap().agent_id.clone();
        let registry = Arc::new(TestSnapshotOwnerRegistry::new(owner));
        let (_, coordinator) = test_coordinator_with_registry(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
            registry,
        );
        let assigned = coordinator
            .ensure_snapshot_delivery(&delivery)
            .await
            .unwrap();
        let assignment_id = assigned
            .delivery_assignment
            .as_ref()
            .unwrap()
            .assignment_id
            .clone();
        components
            .outbox
            .retire(&delivery.tenant_id, &assignment_id)
            .await
            .unwrap();
        let mut deleting = delivery.clone();
        deleting.state = SnapshotDeliveryState::Deleting;
        deleting.delivery_generation = DeliveryGeneration::new(2);
        deleting.updated_at_unix_ms = UnixMillis::new(102);
        components
            .control_catalog
            .replace_snapshot_delivery(delivery.resource_version, deleting)
            .await
            .unwrap();

        assert!(coordinator
            .schedule_snapshot_delivery(&assigned)
            .await
            .unwrap()
            .is_none());
        assert!(components
            .outbox
            .pending_for_agent(&agent_id, 10)
            .await
            .unwrap()
            .is_empty());
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
                    task_fence: test_task_fence(&spec.job_id, "scan_changes"),
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
                    task_fence: test_task_fence(&spec.job_id, "scan_changes"),
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
        let control = Arc::new(
            ControlPlane::new(authorizer, authority, components.clock.clone())
                .with_placement_repository(components.placement.clone()),
        );
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
        let workspace_id = WorkspaceId::new("workspace-a").unwrap();
        let expected_index_version = components
            .publisher
            .current_version(&IndexKey {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                workspace_id: workspace_id.clone(),
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
            workspace_id,
            expected_index_version,
            data_layout: neoengram_domain::protocol::CommitDataLayout::FastCdc,
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(deadline_unix_ms),
            paths: vec![LogicalPath::parse("dataset/file.bin").unwrap()],
            all: false,
            operation_task_id: None,
            extensions: Extensions::new(),
        };
        spec.request_digest = spec.computed_request_digest().unwrap();
        JobRecord {
            spec,
            operation: crate::JobOperation::Add,
            workspace_spec: None,
            delivery_spec: None,
            state: JobState::Queued,
            resource_version: ResourceVersion::new(1),
            assignment: None,
            workspace_assignment: None,
            delivery_assignment: None,
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
            max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
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
            workspace_id: spec.workspace_id.clone(),
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
            test_task_fence(&spec.job_id, "scan_changes"),
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

    async fn seed_snapshot_delivery(components: &InMemoryComponents) -> SnapshotDeliveryRecord {
        let tenant_id = TenantId::new("tenant-delivery-preflight").unwrap();
        let project_id = ProjectId::new("project-delivery-replay").unwrap();
        let artifact_id = ArtifactId::new("artifact-delivery-replay").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-delivery-replay").unwrap();
        let delivery_id = SnapshotDeliveryId::new("delivery-replay").unwrap();
        let storage_volume_id = StorageVolumeId::new("volume-delivery-preflight").unwrap();
        let now = UnixMillis::new(100);
        components
            .control_catalog
            .insert_tenant(TenantRecord {
                tenant_id: tenant_id.clone(),
                display_name: "Tenant".to_owned(),
                description: None,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
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
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id: tenant_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                display_name: "Volume".to_owned(),
                edge_cluster_id: EdgeClusterId::new("edge-delivery-preflight").unwrap(),
                region: "local".to_owned(),
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteMany,
                allowed_delivery_modes: vec![SnapshotDeliveryMode::Copy],
                hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
                max_whole_file_bytes: DecimalU64::new(u64::MAX),
                copy_reserve_bytes: DecimalU64::new(0),
                pvc_reference: Some(CatalogPvcReference {
                    namespace: "default".to_owned(),
                    claim_name: "delivery-replay".to_owned(),
                }),
                nfs_reference: None,
                state: StorageVolumeState::Ready,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .unwrap();
        let commit_id = ContentDigest::from_bytes([7; 32]);
        components
            .control_catalog
            .insert_snapshot_with_delivery(SnapshotWithDeliveryInsertRequest {
                snapshot: SnapshotInsertRequest {
                    record: SnapshotRecord {
                        tenant_id: tenant_id.clone(),
                        project_id: project_id.clone(),
                        artifact_id: artifact_id.clone(),
                        snapshot_id: snapshot_id.clone(),
                        snapshot_request_id: RequestId::new("snapshot-request-delivery-replay")
                            .unwrap(),
                        commit_id,
                        delivery_id: SnapshotDeliveryId::new("delivery-replay").unwrap(),
                        edge_cluster_id: EdgeClusterId::new("edge-delivery-preflight").unwrap(),
                        storage_volume_id: StorageVolumeId::new("volume-delivery-preflight")
                            .unwrap(),
                        delivery_mode: SnapshotDeliveryMode::Copy,
                        state: SnapshotState::Creating,
                        resource_version: 1,
                        lifecycle: ResourceLifecycle::active(),
                        created_at_unix_ms: now,
                        updated_at_unix_ms: now,
                    },
                    artifact_head: ArtifactHeadExpectation::Any,
                },
                delivery: SnapshotDeliveryInsertRequest {
                    record: SnapshotDeliveryRecord {
                        tenant_id: tenant_id.clone(),
                        delivery_id: SnapshotDeliveryId::new("delivery-replay").unwrap(),
                        create_request_id: RequestId::new("snapshot-request-delivery-replay")
                            .unwrap(),
                        snapshot_id: snapshot_id.clone(),
                        commit_id,
                        storage_volume_id: StorageVolumeId::new("volume-delivery-preflight")
                            .unwrap(),
                        mode: SnapshotDeliveryMode::Copy,
                        target_relative_root:
                            SnapshotDeliveryOperation::canonical_target_relative_root(
                                &project_id,
                                &artifact_id,
                                &snapshot_id,
                                &SnapshotDeliveryId::new("delivery-replay").unwrap(),
                            )
                            .unwrap(),
                        state: SnapshotDeliveryState::Requested,
                        source_index_digest: ContentDigest::from_bytes([8; 32]),
                        delivery_generation: DeliveryGeneration::new(1),
                        file_count: 1,
                        size_bytes: 11,
                        object_set_digest: ContentDigest::from_bytes([0; 32]),
                        resource_version: 1,
                        issue_code: None,
                        issue_message: None,
                        issue_retryable: false,
                        created_at_unix_ms: now,
                        updated_at_unix_ms: now,
                    },
                    request_id: RequestId::new("snapshot-request-delivery-replay").unwrap(),
                    retention_roots: Vec::new(),
                },
            })
            .await
            .unwrap();
        // A successful SnapshotDelivery must be backed by the v2 placement authority.  Seed the
        // exact Commit ObjectSet and its target-volume object evidence used by the readiness
        // gate; the test remains focused on replaying an already-completed delivery.
        let object_id = neoengram_domain::core::ObjectId::from_bytes([1; 32]);
        let object_set = neoengram_domain::protocol::ObjectSet::new(vec![
            neoengram_domain::protocol::CommitObject::new(
                object_id,
                11,
                neoengram_domain::protocol::ObjectEncoding::Raw,
                0,
            ),
        ])
        .unwrap();
        components
            .placement
            .insert_commit_object_set(neoengram_domain::protocol::CommitObjectSet {
                tenant_id: tenant_id.clone(),
                commit_id: neoengram_domain::core::CommitId::from_digest(commit_id),
                object_set: object_set.clone(),
            })
            .await
            .unwrap();
        let namespace =
            neoengram_domain::protocol::ObjectNamespaceId::new(artifact_id.to_string()).unwrap();
        components
            .placement
            .insert_object_placement_v2(
                neoengram_domain::protocol::materialization::ObjectPlacement {
                    placement_id: neoengram_domain::protocol::PlacementId::new(
                        "placement-delivery-replay",
                    )
                    .unwrap(),
                    tenant_id: tenant_id.clone(),
                    object_namespace_id: namespace,
                    object_id,
                    size: DecimalU64::new(11),
                    encoding: neoengram_domain::protocol::ObjectEncoding::Raw,
                    verified_digest: object_id.digest(),
                    storage_volume_id: Some(storage_volume_id.clone()),
                    archive_id: None,
                    placement_generation: neoengram_domain::protocol::PlacementGeneration::new(1),
                    state:
                        neoengram_domain::protocol::materialization::ObjectPlacementState::Verified,
                    failure_domain: "host-delivery-replay".to_owned(),
                },
            )
            .await
            .unwrap();
        SnapshotDeliveryRecord {
            tenant_id: tenant_id.clone(),
            delivery_id: delivery_id.clone(),
            create_request_id: RequestId::new("snapshot-request-delivery-replay").unwrap(),
            snapshot_id: snapshot_id.clone(),
            commit_id,
            storage_volume_id,
            mode: SnapshotDeliveryMode::Copy,
            target_relative_root: SnapshotDeliveryOperation::canonical_target_relative_root(
                &project_id,
                &artifact_id,
                &snapshot_id,
                &delivery_id,
            )
            .unwrap(),
            state: SnapshotDeliveryState::Requested,
            source_index_digest: ContentDigest::from_bytes([8; 32]),
            delivery_generation: DeliveryGeneration::new(1),
            file_count: 1,
            size_bytes: 11,
            object_set_digest: ContentDigest::from_bytes([0; 32]),
            resource_version: 1,
            issue_code: None,
            issue_message: None,
            issue_retryable: false,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        }
    }

    fn snapshot_owner_record(capabilities: BTreeSet<String>) -> AgentRegistryRecord {
        let tenant_id = TenantId::new("tenant-delivery-preflight").unwrap();
        let volume_id = StorageVolumeId::new("volume-delivery-preflight").unwrap();
        let agent_id = AgentId::new("agent-delivery-preflight").unwrap();
        let mount_id = AgentMountId::new("mount-delivery-preflight").unwrap();
        let marker_id = VolumeMarkerId::new("marker-delivery-preflight").unwrap();
        AgentRegistryRecord {
            resource_version: ResourceVersion::new(1),
            enrollment: AgentEnrollmentRecord {
                token_id: AgentEnrollmentTokenId::new("token-delivery-preflight").unwrap(),
                token_request_id: RequestId::new("token-request-delivery-preflight").unwrap(),
                enrollment_id: AgentEnrollmentId::new("enrollment-delivery-preflight").unwrap(),
                tenant_id: tenant_id.clone(),
                edge_cluster_id: EdgeClusterId::new("edge-delivery-preflight").unwrap(),
                storage_volume_id: volume_id.clone(),
                volume_descriptor_digest: ContentDigest::hash(b"delivery-preflight-volume"),
                pvc_identity_digest: PvcIdentityDigest::derive("delivery-preflight", "volume")
                    .unwrap(),
                reserved_agent_id: agent_id.clone(),
                reserved_agent_mount_id: mount_id.clone(),
                bootstrap_token_digest: ContentDigest::hash(b"delivery-preflight-token"),
                state: AgentEnrollmentState::Approved,
                created_at_unix_ms: UnixMillis::new(1),
                expires_at_unix_ms: UnixMillis::new(10_000),
                bootstrapped_at_unix_ms: Some(UnixMillis::new(2)),
                bootstrap_request_id: Some(RequestId::new("bootstrap-delivery-preflight").unwrap()),
                review_expires_at_unix_ms: None,
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
                installation_id: AgentInstallationId::new("installation-delivery-preflight")
                    .unwrap(),
                public_key_fingerprint: ContentDigest::hash(b"delivery-preflight-key"),
                agent_version: "0.2.0".to_owned(),
                wire_version: CURRENT_WIRE_VERSION,
                capabilities,
                state: AgentInstanceState::Active,
                session_generation: Some(SessionGeneration::new(1)),
                active_boot_id: Some(AgentBootId::new("boot-delivery-preflight").unwrap()),
                active_session_id: Some(SessionId::new("session-delivery-preflight").unwrap()),
                session_open_expected_resource_version: Some(ResourceVersion::new(1)),
                session_opened_at_unix_ms: Some(UnixMillis::new(90)),
                last_heartbeat_at_unix_ms: Some(UnixMillis::new(100)),
                last_sequence: Some(SequenceNumber::new(1)),
            }),
            mount: AgentMountRecord {
                agent_mount_id: mount_id.clone(),
                storage_volume_id: volume_id.clone(),
                mount_generation: MountGeneration::new(1),
                expected_volume_marker: marker_id.clone(),
                desired_access_mode: MountAccessMode::ReadWrite,
                mount_identity_digest: Some(AgentMountIdentityDigest::new(ContentDigest::hash(
                    b"delivery-preflight-mount",
                ))),
                observed_volume_marker: Some(marker_id),
                observed_access_mode: Some(MountAccessMode::ReadWrite),
                reported_health: Some(ResourceHealth::Ready),
                available_bytes: Some(u64::MAX),
                health: ResourceHealth::Ready,
                observed_at_unix_ms: Some(UnixMillis::new(100)),
            },
            owner: VolumeOwnerRecord {
                tenant_id,
                storage_volume_id: volume_id,
                active_agent_id: Some(agent_id),
                active_agent_mount_id: Some(mount_id),
                owner_generation: OwnerGeneration::new(1),
                state: VolumeOwnerState::Active,
            },
            decision_audit_event: None,
            storage_enrollment: StorageEnrollmentMetadata::default(),
            workload_certificate: None,
            volume_lifecycle_revocation: None,
        }
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
            _request: &crate::AgentEnrollmentListRequest,
        ) -> CentralResult<crate::AgentEnrollmentListPage> {
            Ok(crate::AgentEnrollmentListPage {
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
        ) -> CentralResult<Option<crate::PvcVolumeBinding>> {
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
        ) -> CentralResult<crate::AgentEnrollmentExpiryReconciliation> {
            Ok(crate::AgentEnrollmentExpiryReconciliation::default())
        }

        async fn enrollment_audit_events(
            &self,
        ) -> CentralResult<Vec<crate::AgentEnrollmentAuditEvent>> {
            Ok(Vec::new())
        }

        async fn enrollment_lifecycle_audit_events(
            &self,
            _tenant_id: &TenantId,
        ) -> CentralResult<Vec<crate::AgentEnrollmentLifecycleAuditEvent>> {
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
        ) -> CentralResult<crate::AgentRegistryInsertOutcome> {
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
        ) -> CentralResult<crate::AgentRegistryReplacementRecords> {
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
