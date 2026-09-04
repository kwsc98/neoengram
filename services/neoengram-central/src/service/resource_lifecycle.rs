use std::{collections::BTreeSet, sync::Arc};

use serde::Serialize;

use crate::{
    AgentRegistryRepository, AuthorityLifecycleAction, AuthorityLifecycleRepository,
    AuthorityLifecycleRequest, CentralError, CentralErrorCode, CentralResult, Clock,
    ControlCatalogRepository, DeletionListRequest, DeletionTransitionRequest,
    LifecycleAssignmentOutboxRecord, ObjectCatalog, RevokeVolumeForLifecycleRequest,
    TenantListRequest,
};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    AgentResourceLifecycleAssignment, AgentResourceLifecycleScope, ArtifactId, ArtifactPlacementId,
    DeletionId, DeletionOperation, DeletionOperationState, Extensions, LifecycleAssignmentId,
    OperationTask, PlacementGeneration, PrincipalId, PrincipalKind, PrincipalRef, RequestId,
    ResourceLifecycleAction, ResourceRef, RetentionHoldState, TaskActor, TaskKind,
    TaskResourceKind, TaskResourceLink, TaskResourceRole, TaskScope, TaskState, UnixMillis,
};

const RECONCILE_PAGE_SIZE: u16 = 100;
const ASSIGNMENT_DEADLINE_MS: u64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceLifecycleReconcileRun {
    pub examined: usize,
    pub transitioned: usize,
    pub assignments_published: usize,
    pub blocked: usize,
}

/// Drives the durable deletion state machine one idempotent step at a time.
pub struct ResourceLifecycleCoordinator {
    catalog: Arc<dyn ControlCatalogRepository>,
    agents: Arc<dyn AgentRegistryRepository>,
    authority_lifecycle: Arc<dyn AuthorityLifecycleRepository>,
    objects: Arc<dyn ObjectCatalog>,
    clock: Arc<dyn Clock>,
    task_coordinator: Option<Arc<super::TaskCoordinator>>,
}

impl ResourceLifecycleCoordinator {
    #[must_use]
    pub fn new(
        catalog: Arc<dyn ControlCatalogRepository>,
        agents: Arc<dyn AgentRegistryRepository>,
        authority_lifecycle: Arc<dyn AuthorityLifecycleRepository>,
        objects: Arc<dyn ObjectCatalog>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            catalog,
            agents,
            authority_lifecycle,
            objects,
            clock,
            task_coordinator: None,
        }
    }

    /// Installs the unified task coordinator used to expose background lifecycle work in the
    /// central task/audit view. Standalone compositions may omit it; the deletion saga itself is
    /// unchanged in that mode.
    #[must_use]
    pub fn with_task_coordinator(mut self, coordinator: Arc<super::TaskCoordinator>) -> Self {
        self.task_coordinator = Some(coordinator);
        self
    }

    async fn ensure_operation_task(
        &self,
        operation: &DeletionOperation,
    ) -> CentralResult<Option<OperationTask>> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok(None);
        };
        // A public deletion request may already have created its root task. Reuse that identity
        // when present so the reconciler acts as its executor rather than creating a duplicate
        // audit root. The fallback below keeps standalone/background operations observable too.
        if let Some(task) = coordinator
            .repository()
            .get_by_request_id(&operation.tenant_id, &operation.request_id)
            .await?
        {
            coordinator
                .repository()
                .link_resource(crate::TaskResourceLinkRecord {
                    tenant_id: operation.tenant_id.clone(),
                    link: TaskResourceLink::new(
                        task.task_id.clone(),
                        TaskResourceKind::Deletion,
                        operation.deletion_id.to_string(),
                        TaskResourceRole::Primary,
                    ),
                })
                .await?;
            return Ok(Some(task));
        }
        // The deletion operation is mutable as it advances through the saga. Bind the task to
        // immutable identity only, so every reconciliation pass replays the same task instead of
        // changing its request digest after a state transition.
        let request = LifecycleTaskRequest {
            deletion_id: &operation.deletion_id,
            request_digest: &operation.request_digest,
        };
        let digest = blake3::hash(
            format!(
                "resource-lifecycle\0{}\0{}",
                operation.tenant_id, operation.deletion_id
            )
            .as_bytes(),
        );
        let request_id = RequestId::new(format!("resource-lifecycle-{}", digest.to_hex()))
            .map_err(CentralError::from)?;
        let (task, _) = coordinator
            .create_root(
                TaskKind::CatalogLifecycle,
                lifecycle_task_scope(operation),
                request_id,
                &request,
                lifecycle_task_actor(),
                Some("deletion"),
                Some(operation.deletion_id.as_str()),
            )
            .await?;
        coordinator
            .repository()
            .link_resource(crate::TaskResourceLinkRecord {
                tenant_id: operation.tenant_id.clone(),
                link: TaskResourceLink::new(
                    task.task_id.clone(),
                    TaskResourceKind::Deletion,
                    operation.deletion_id.to_string(),
                    TaskResourceRole::Primary,
                ),
            })
            .await?;
        Ok(Some(task))
    }

    async fn transition_task(
        &self,
        task: &OperationTask,
        next: TaskState,
        message: impl Into<String>,
    ) -> CentralResult<OperationTask> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok(task.clone());
        };
        let current = coordinator
            .repository()
            .get(&task.tenant_id, &task.task_id)
            .await?
            .ok_or_else(|| invalid("resource lifecycle task disappeared"))?;
        if current.state == next || current.state.is_terminal() {
            return Ok(current);
        }
        coordinator
            .transition(
                &current.task_id,
                &current.tenant_id,
                next,
                lifecycle_task_actor(),
                Some(message.into()),
            )
            .await
    }

    async fn finish_operation_task(
        &self,
        operation: &DeletionOperation,
        task: Option<&OperationTask>,
    ) -> CentralResult<()> {
        let Some(task) = task else {
            return Ok(());
        };
        let latest = self
            .catalog
            .get_deletion_operation(&operation.tenant_id, &operation.deletion_id)
            .await?
            .unwrap_or_else(|| operation.clone());
        let (next, message) = match latest.state {
            DeletionOperationState::Completed => (
                TaskState::Succeeded,
                "resource lifecycle reconciliation completed",
            ),
            DeletionOperationState::Blocked | DeletionOperationState::Failed => (
                TaskState::Stalled,
                "resource lifecycle reconciliation is waiting for retry",
            ),
            _ => (
                TaskState::Running,
                "resource lifecycle reconciliation remains active",
            ),
        };
        self.transition_task(task, next, message).await.map(|_| ())
    }

    pub async fn reconcile_once(
        &self,
        limit: usize,
    ) -> CentralResult<ResourceLifecycleReconcileRun> {
        let mut run = ResourceLifecycleReconcileRun::default();
        if limit == 0 {
            return Ok(run);
        }
        let mut tenant_after = None;
        while run.examined < limit {
            let tenants = self
                .catalog
                .list_tenants(&TenantListRequest {
                    visible_tenant_ids: None,
                    query: None,
                    after: tenant_after.clone(),
                    limit: RECONCILE_PAGE_SIZE,
                })
                .await?;
            if tenants.records.is_empty() {
                break;
            }
            for tenant in &tenants.records {
                let mut deletion_after = None;
                while run.examined < limit {
                    let page = self
                        .catalog
                        .list_deletion_operations(&DeletionListRequest {
                            tenant_id: tenant.tenant_id.clone(),
                            states: None,
                            after: deletion_after.clone(),
                            limit: RECONCILE_PAGE_SIZE,
                        })
                        .await?;
                    if page.records.is_empty() {
                        break;
                    }
                    for operation in &page.records {
                        if run.examined == limit {
                            break;
                        }
                        if matches!(operation.state, DeletionOperationState::Completed) {
                            continue;
                        }
                        run.examined += 1;
                        let task = self.ensure_operation_task(operation).await?;
                        if !matches!(
                            operation.state,
                            DeletionOperationState::Blocked | DeletionOperationState::Failed
                        ) {
                            if let Some(task) = task.as_ref() {
                                self.transition_task(
                                    task,
                                    TaskState::Running,
                                    "resource lifecycle reconciliation started",
                                )
                                .await?;
                            }
                        }
                        match self.reconcile_operation(operation).await {
                            Ok(StepOutcome::Transitioned) => {
                                run.transitioned += 1;
                                self.finish_operation_task(operation, task.as_ref()).await?;
                            }
                            Ok(StepOutcome::AssignmentsPublished(count)) => {
                                run.assignments_published += count;
                                self.finish_operation_task(operation, task.as_ref()).await?;
                            }
                            Ok(StepOutcome::Idle) => {
                                self.finish_operation_task(operation, task.as_ref()).await?;
                            }
                            Err(error) if should_block(&error) => {
                                if self.block_operation(operation, &error).await.is_ok() {
                                    run.blocked += 1;
                                }
                                if let Some(task) = task.as_ref() {
                                    self.transition_task(task, TaskState::Stalled, error.message())
                                        .await?;
                                }
                            }
                            Err(error) => {
                                if let Some(task) = task.as_ref() {
                                    self.transition_task(task, TaskState::Failed, error.message())
                                        .await?;
                                }
                                return Err(error);
                            }
                        }
                    }
                    deletion_after = page.next;
                    if deletion_after.is_none() {
                        break;
                    }
                }
            }
            tenant_after = tenants.next;
            if tenant_after.is_none() {
                break;
            }
        }
        Ok(run)
    }

    async fn reconcile_operation(
        &self,
        operation: &DeletionOperation,
    ) -> CentralResult<StepOutcome> {
        match operation.state {
            DeletionOperationState::Requested => {
                self.transition(operation, DeletionOperationState::Quiescing, None)
                    .await?;
                Ok(StepOutcome::Transitioned)
            }
            DeletionOperationState::Quiescing => {
                self.ensure_authority_action(operation, AuthorityLifecycleAction::Quiesce)
                    .await?;
                self.transition(operation, DeletionOperationState::Quarantining, None)
                    .await?;
                Ok(StepOutcome::Transitioned)
            }
            DeletionOperationState::Quarantining => {
                let published = self
                    .ensure_assignments(operation, ResourceLifecycleAction::Quarantine)
                    .await?;
                if self
                    .assignments_complete(operation, ResourceLifecycleAction::Quarantine)
                    .await?
                {
                    self.transition(operation, DeletionOperationState::Recoverable, None)
                        .await?;
                    Ok(StepOutcome::Transitioned)
                } else if published != 0 {
                    Ok(StepOutcome::AssignmentsPublished(published))
                } else {
                    Ok(StepOutcome::Idle)
                }
            }
            DeletionOperationState::Recoverable => {
                if self.clock.now() < operation.purge_after_unix_ms
                    || self.has_active_hold(operation).await?
                {
                    return Ok(StepOutcome::Idle);
                }
                self.transition(operation, DeletionOperationState::Purging, None)
                    .await?;
                Ok(StepOutcome::Transitioned)
            }
            DeletionOperationState::Purging => {
                let published = self
                    .ensure_assignments(operation, ResourceLifecycleAction::Purge)
                    .await?;
                if self
                    .assignments_complete(operation, ResourceLifecycleAction::Purge)
                    .await?
                {
                    self.transition(operation, DeletionOperationState::Finalizing, None)
                        .await?;
                    Ok(StepOutcome::Transitioned)
                } else if published == 0 {
                    Ok(StepOutcome::Idle)
                } else {
                    Ok(StepOutcome::AssignmentsPublished(published))
                }
            }
            DeletionOperationState::Finalizing => {
                self.ensure_authority_action(operation, AuthorityLifecycleAction::Finalize)
                    .await?;
                self.ensure_volume_enrollment_revoked(operation).await?;
                self.transition(operation, DeletionOperationState::Completed, None)
                    .await?;
                Ok(StepOutcome::Transitioned)
            }
            DeletionOperationState::Restoring => {
                let published = self
                    .ensure_assignments(operation, ResourceLifecycleAction::Restore)
                    .await?;
                if self
                    .assignments_complete(operation, ResourceLifecycleAction::Restore)
                    .await?
                {
                    self.transition(operation, DeletionOperationState::Completed, None)
                        .await?;
                    Ok(StepOutcome::Transitioned)
                } else if published == 0 {
                    Ok(StepOutcome::Idle)
                } else {
                    Ok(StepOutcome::AssignmentsPublished(published))
                }
            }
            DeletionOperationState::Blocked
            | DeletionOperationState::Failed
            | DeletionOperationState::Completed => Ok(StepOutcome::Idle),
        }
    }

    async fn has_active_hold(&self, operation: &DeletionOperation) -> CentralResult<bool> {
        let now = self.clock.now();
        Ok(self
            .catalog
            .list_retention_holds(&operation.tenant_id, &operation.deletion_id)
            .await?
            .iter()
            .any(|hold| {
                hold.state == RetentionHoldState::Active
                    && hold
                        .expires_at_unix_ms
                        .is_none_or(|expires_at| expires_at > now)
            }))
    }

    async fn ensure_assignments(
        &self,
        operation: &DeletionOperation,
        action: ResourceLifecycleAction,
    ) -> CentralResult<usize> {
        let commands = self.commands(operation, action).await?;
        let mut published = 0;
        for command in commands {
            let assignment_id = command.assignment.assignment_id.clone();
            let existing = self
                .catalog
                .get_lifecycle_assignment(&operation.tenant_id, &assignment_id)
                .await?;
            let record = match existing {
                Some(existing) => existing,
                None => {
                    let _ = self
                        .catalog
                        .enqueue_lifecycle_assignment(LifecycleAssignmentOutboxRecord {
                            assignment: command,
                            published: false,
                            retired: false,
                            terminal_report_digest: None,
                        })
                        .await?;
                    self.catalog
                        .get_lifecycle_assignment(&operation.tenant_id, &assignment_id)
                        .await?
                        .ok_or_else(|| internal("lifecycle assignment disappeared after enqueue"))?
                }
            };
            if !record.published {
                self.catalog
                    .publish_lifecycle_assignment(&operation.tenant_id, &assignment_id)
                    .await?;
                published += 1;
            }
        }
        Ok(published)
    }

    async fn assignments_complete(
        &self,
        operation: &DeletionOperation,
        action: ResourceLifecycleAction,
    ) -> CentralResult<bool> {
        let commands = self.commands(operation, action).await?;
        if commands.is_empty() {
            return Ok(true);
        }
        for command in commands {
            let Some(record) = self
                .catalog
                .get_lifecycle_assignment(&operation.tenant_id, &command.assignment.assignment_id)
                .await?
            else {
                return Ok(false);
            };
            if !record.retired && record.assignment.assignment.deadline_unix_ms < self.clock.now() {
                return Err(invalid(
                    "lifecycle assignment expired before an Agent terminal report",
                ));
            }
            if !record.retired {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn commands(
        &self,
        operation: &DeletionOperation,
        action: ResourceLifecycleAction,
    ) -> CentralResult<Vec<AgentResourceLifecycleAssignment>> {
        if matches!(
            (&operation.root, action),
            (
                ResourceRef::StorageVolume { .. },
                ResourceLifecycleAction::Quarantine | ResourceLifecycleAction::Purge
            )
        ) {
            self.ensure_volume_has_no_unique_replicas(operation).await?;
        }
        let volume_ids = self.operation_volume_ids(operation).await?;
        if matches!(operation.root, ResourceRef::Artifact { .. }) && volume_ids.is_empty() {
            return Err(invalid(
                "Artifact physical placement inventory is empty; refusing metadata-only cleanup",
            ));
        }
        let target = operation
            .targets
            .iter()
            .find(|target| target.resource == operation.root)
            .ok_or_else(|| internal("deletion root is absent from its target batch"))?;
        if !target.requires_agent_cleanup {
            return Ok(Vec::new());
        }
        let mut commands = Vec::with_capacity(volume_ids.len());
        for volume_id in volume_ids {
            let owner = self
                .agents
                .get_current_by_volume(&operation.tenant_id, &volume_id)
                .await?
                .ok_or_else(|| invalid("deletion target Volume has no enrolled Agent"))?;
            let instance = owner
                .instance
                .as_ref()
                .ok_or_else(|| invalid("deletion target Agent has no active instance"))?;
            let session_generation = instance
                .session_generation
                .ok_or_else(|| invalid("deletion target Agent has no active session"))?;
            if owner.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
                || owner.owner.active_agent_mount_id.as_ref() != Some(&owner.mount.agent_mount_id)
            {
                return Err(invalid("deletion target Volume owner fence is unavailable"));
            }
            let scope = self.lifecycle_scope(operation, &volume_id).await?;
            let assignment_id = lifecycle_assignment_id(
                operation,
                action,
                &volume_id,
                target.lifecycle_generation,
            )?;
            let command = AgentResourceLifecycleAssignment {
                assignment: neoengram_domain::protocol::ResourceLifecycleAssignment {
                    assignment_id,
                    tenant_id: operation.tenant_id.clone(),
                    deletion_id: operation.deletion_id.clone(),
                    resource: operation.root.clone(),
                    action,
                    lifecycle_generation: target.lifecycle_generation,
                    request_digest: operation.request_digest,
                    deadline_unix_ms: UnixMillis::new(
                        self.clock
                            .now()
                            .get()
                            .checked_add(ASSIGNMENT_DEADLINE_MS)
                            .ok_or_else(|| internal("lifecycle assignment deadline overflow"))?,
                    ),
                },
                resource_scope: scope,
                agent_id: instance.agent_id.clone(),
                edge_cluster_id: owner.enrollment.edge_cluster_id.clone(),
                agent_mount_id: owner.mount.agent_mount_id.clone(),
                volume_marker_id: owner.mount.expected_volume_marker.clone(),
                session_generation,
                mount_generation: owner.mount.mount_generation,
                owner_generation: owner.owner.owner_generation,
                extensions: Default::default(),
            };
            command.validate()?;
            commands.push(command);
        }
        Ok(commands)
    }

    async fn operation_volume_ids(
        &self,
        operation: &DeletionOperation,
    ) -> CentralResult<Vec<neoengram_domain::protocol::StorageVolumeId>> {
        let mut volumes = BTreeSet::new();
        match &operation.root {
            ResourceRef::StorageVolume { storage_volume_id } => {
                volumes.insert(storage_volume_id.clone());
            }
            ResourceRef::Snapshot { snapshot_id } => {
                // A Snapshot owns exactly one immutable physical Delivery. Resolve its target
                // Volume so the lifecycle saga can fence and purge that Delivery's directory.
                let snapshot = self
                    .catalog
                    .get_snapshot_for_lifecycle(&operation.tenant_id, snapshot_id)
                    .await?
                    .ok_or_else(|| invalid("Snapshot deletion target no longer exists"))?;
                volumes.insert(snapshot.storage_volume_id);
            }
            ResourceRef::Playground {
                project_id,
                artifact_id,
                playground_id,
            } => {
                let playground = self
                    .catalog
                    .get_playground_for_lifecycle(
                        &operation.tenant_id,
                        project_id,
                        artifact_id,
                        playground_id,
                    )
                    .await?
                    .ok_or_else(|| invalid("Playground deletion target no longer exists"))?;
                volumes.insert(playground.storage_volume_id);
            }
            ResourceRef::Artifact { artifact_id, .. } => {
                volumes.extend(
                    self.objects
                        .artifact_placement_volumes(&operation.tenant_id, artifact_id)
                        .await?,
                );
                for target in &operation.targets {
                    match &target.resource {
                        ResourceRef::Snapshot { snapshot_id } => {
                            if let Some(snapshot) = self
                                .catalog
                                .get_snapshot_for_lifecycle(&operation.tenant_id, snapshot_id)
                                .await?
                            {
                                volumes.insert(snapshot.storage_volume_id);
                            }
                        }
                        ResourceRef::Playground {
                            project_id,
                            artifact_id,
                            playground_id,
                        } => {
                            if let Some(playground) = self
                                .catalog
                                .get_playground_for_lifecycle(
                                    &operation.tenant_id,
                                    project_id,
                                    artifact_id,
                                    playground_id,
                                )
                                .await?
                            {
                                volumes.insert(playground.storage_volume_id);
                            }
                        }
                        ResourceRef::StorageVolume { .. } | ResourceRef::Artifact { .. } => {}
                    }
                }
            }
        }
        Ok(volumes.into_iter().collect())
    }

    async fn ensure_volume_has_no_unique_replicas(
        &self,
        operation: &DeletionOperation,
    ) -> CentralResult<()> {
        let ResourceRef::StorageVolume { storage_volume_id } = &operation.root else {
            return Ok(());
        };
        let blockers = self
            .objects
            .volume_unique_artifact_replicas(&operation.tenant_id, storage_volume_id)
            .await?;
        if blockers.is_empty() {
            return Ok(());
        }
        Err(invalid(format!(
            "StorageVolume contains unique object replicas for retained Artifacts: {}",
            blockers
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    async fn ensure_authority_action(
        &self,
        operation: &DeletionOperation,
        action: AuthorityLifecycleAction,
    ) -> CentralResult<()> {
        let mut targets = operation.targets.iter().collect::<Vec<_>>();
        targets.sort_by_key(|target| authority_target_priority(&target.resource));
        for target in targets {
            if let Some(existing) = self
                .authority_lifecycle
                .get(
                    &operation.tenant_id,
                    &operation.deletion_id,
                    &target.resource,
                    action,
                )
                .await?
            {
                if existing.request_digest != operation.request_digest {
                    return Err(internal(
                        "Authority lifecycle record is bound to another deletion request",
                    ));
                }
                continue;
            }
            let request = AuthorityLifecycleRequest {
                tenant_id: operation.tenant_id.clone(),
                deletion_id: operation.deletion_id.clone(),
                target: target.resource.clone(),
                lifecycle_generation: target.lifecycle_generation,
                request_digest: operation.request_digest,
                occurred_at_unix_ms: self.clock.now(),
            };
            match action {
                AuthorityLifecycleAction::Quiesce => {
                    self.authority_lifecycle.quiesce(request).await?;
                }
                AuthorityLifecycleAction::Finalize => {
                    self.authority_lifecycle.finalize(request).await?;
                }
            }
        }
        Ok(())
    }

    async fn ensure_volume_enrollment_revoked(
        &self,
        operation: &DeletionOperation,
    ) -> CentralResult<()> {
        let ResourceRef::StorageVolume { storage_volume_id } = &operation.root else {
            return Ok(());
        };
        let target = operation
            .targets
            .iter()
            .find(|target| target.resource == operation.root)
            .ok_or_else(|| internal("Volume deletion root is absent from its target batch"))?;
        self.agents
            .revoke_volume_for_lifecycle(RevokeVolumeForLifecycleRequest {
                tenant_id: operation.tenant_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                deletion_id: operation.deletion_id.clone(),
                lifecycle_generation: target.lifecycle_generation,
                request_digest: operation.request_digest,
                occurred_at_unix_ms: self.clock.now(),
            })
            .await?;
        Ok(())
    }

    async fn lifecycle_scope(
        &self,
        operation: &DeletionOperation,
        storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
    ) -> CentralResult<AgentResourceLifecycleScope> {
        match &operation.root {
            ResourceRef::StorageVolume {
                storage_volume_id: expected,
            } => {
                if expected != storage_volume_id {
                    return Err(internal(
                        "StorageVolume lifecycle command resolved an unrelated Volume",
                    ));
                }
                Ok(AgentResourceLifecycleScope::StorageVolume {
                    storage_volume_id: storage_volume_id.clone(),
                })
            }
            ResourceRef::Artifact {
                project_id,
                artifact_id,
            } => Ok(AgentResourceLifecycleScope::Artifact {
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                artifact_placement_id: placement_id(
                    &operation.tenant_id,
                    artifact_id,
                    storage_volume_id,
                )?,
                placement_generation: PlacementGeneration::new(1),
            }),
            ResourceRef::Playground {
                project_id,
                artifact_id,
                playground_id,
            } => {
                let playground = self
                    .catalog
                    .get_playground_for_lifecycle(
                        &operation.tenant_id,
                        project_id,
                        artifact_id,
                        playground_id,
                    )
                    .await?
                    .ok_or_else(|| invalid("Playground deletion target no longer exists"))?;
                if &playground.storage_volume_id != storage_volume_id {
                    return Err(internal(
                        "Playground lifecycle command resolved an unrelated Volume",
                    ));
                }
                Ok(AgentResourceLifecycleScope::Playground {
                    project_id: project_id.clone(),
                    artifact_id: artifact_id.clone(),
                    playground_id: playground_id.clone(),
                    storage_volume_id: storage_volume_id.clone(),
                    artifact_placement_id: placement_id(
                        &operation.tenant_id,
                        artifact_id,
                        storage_volume_id,
                    )?,
                    placement_generation: PlacementGeneration::new(1),
                })
            }
            ResourceRef::Snapshot { snapshot_id } => {
                let snapshot = self
                    .catalog
                    .get_snapshot_for_lifecycle(&operation.tenant_id, snapshot_id)
                    .await?
                    .ok_or_else(|| invalid("Snapshot deletion target no longer exists"))?;
                if snapshot.storage_volume_id != *storage_volume_id {
                    return Err(internal(
                        "Snapshot lifecycle command resolved an unrelated Volume",
                    ));
                }
                Ok(AgentResourceLifecycleScope::Snapshot {
                    project_id: snapshot.project_id,
                    artifact_id: snapshot.artifact_id.clone(),
                    snapshot_id: snapshot.snapshot_id,
                    storage_volume_id: storage_volume_id.clone(),
                    artifact_placement_id: placement_id(
                        &operation.tenant_id,
                        &snapshot.artifact_id,
                        storage_volume_id,
                    )?,
                    placement_generation: PlacementGeneration::new(1),
                })
            }
        }
    }

    async fn transition(
        &self,
        operation: &DeletionOperation,
        next_state: DeletionOperationState,
        last_error: Option<String>,
    ) -> CentralResult<DeletionOperation> {
        self.catalog
            .transition_deletion_state(DeletionTransitionRequest {
                tenant_id: operation.tenant_id.clone(),
                deletion_id: operation.deletion_id.clone(),
                expected_state: operation.state,
                next_state,
                expected_resource_version: operation.resource_version.get(),
                now_unix_ms: self.clock.now(),
                last_error,
            })
            .await
    }

    async fn block_operation(
        &self,
        operation: &DeletionOperation,
        error: &CentralError,
    ) -> CentralResult<()> {
        if matches!(
            operation.state,
            DeletionOperationState::Blocked
                | DeletionOperationState::Failed
                | DeletionOperationState::Completed
        ) {
            return Ok(());
        }
        self.transition(
            operation,
            DeletionOperationState::Blocked,
            Some(error.message().to_owned()),
        )
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepOutcome {
    Idle,
    Transitioned,
    AssignmentsPublished(usize),
}

#[derive(Serialize)]
struct LifecycleTaskRequest<'a> {
    deletion_id: &'a DeletionId,
    request_digest: &'a ContentDigest,
}

fn lifecycle_task_actor() -> TaskActor {
    TaskActor::Principal(PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new("resource-lifecycle-reconciler")
            .expect("static lifecycle reconciler principal is valid"),
        extensions: Extensions::new(),
    })
}

fn lifecycle_task_scope(operation: &DeletionOperation) -> TaskScope {
    let mut scope = TaskScope::new(operation.tenant_id.clone());
    match &operation.root {
        ResourceRef::StorageVolume { storage_volume_id } => {
            scope.storage_volume_id = Some(storage_volume_id.clone());
        }
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => {
            scope.project_id = Some(project_id.clone());
            scope.artifact_id = Some(artifact_id.clone());
        }
        ResourceRef::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => {
            scope.project_id = Some(project_id.clone());
            scope.artifact_id = Some(artifact_id.clone());
            scope.playground_id = Some(playground_id.clone());
        }
        ResourceRef::Snapshot { snapshot_id } => {
            scope.snapshot_id = Some(snapshot_id.clone());
        }
    }
    scope
}

fn placement_id(
    tenant_id: &neoengram_domain::protocol::TenantId,
    artifact_id: &ArtifactId,
    storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
) -> CentralResult<ArtifactPlacementId> {
    let digest =
        blake3::hash(format!("{tenant_id}\0{artifact_id}\0{storage_volume_id}").as_bytes());
    ArtifactPlacementId::new(format!("placement-{}", digest.to_hex())).map_err(Into::into)
}

fn lifecycle_assignment_id(
    operation: &DeletionOperation,
    action: ResourceLifecycleAction,
    storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
    lifecycle_generation: neoengram_domain::protocol::LifecycleGeneration,
) -> CentralResult<LifecycleAssignmentId> {
    let action = match action {
        ResourceLifecycleAction::Quarantine => "quarantine",
        ResourceLifecycleAction::Restore => "restore",
        ResourceLifecycleAction::Purge => "purge",
        ResourceLifecycleAction::CancelJobs => "cancel-jobs",
    };
    let digest = blake3::hash(
        format!(
            "{}\0{}\0{}\0{}\0{}",
            operation.deletion_id,
            action,
            storage_volume_id,
            lifecycle_generation.get(),
            operation.retry_count.get()
        )
        .as_bytes(),
    );
    LifecycleAssignmentId::new(format!("lifecycle-assignment-{digest}")).map_err(Into::into)
}

fn authority_target_priority(resource: &ResourceRef) -> u8 {
    match resource {
        ResourceRef::Snapshot { .. } => 0,
        ResourceRef::Playground { .. } => 1,
        ResourceRef::Artifact { .. } => 2,
        ResourceRef::StorageVolume { .. } => 3,
    }
}

fn should_block(error: &CentralError) -> bool {
    matches!(
        error.code(),
        CentralErrorCode::InvalidState
            | CentralErrorCode::EnrollmentNotFound
            | CentralErrorCode::ArtifactNotFound
            | CentralErrorCode::StorageVolumeNotFound
    )
}

fn invalid(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(true)
}

fn internal(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::Internal, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use crate::{
        ArtifactHeadExpectation, ArtifactInitialization, ArtifactRecord, CatalogInsertOutcome,
        ControlCatalogRepository, CreateDeletionRequest, DeletionImpactQuery,
        DeletionTransitionRequest, InMemoryComponents, RetryDeletionRequest,
        SnapshotDeliveryInsertRequest, SnapshotDeliveryRecord, SnapshotInsertRequest,
        SnapshotRecord, SnapshotState, SnapshotWithDeliveryInsertRequest, StorageAccessMode,
        StorageBackendType, StorageVolumeRecord, StorageVolumeState, TaskCoordinator, TenantRecord,
    };
    use neoengram_domain::core::ContentDigest;
    use neoengram_domain::protocol::{
        ArtifactId, DecimalU64, DeletionCompletion, DeletionId, DeliveryGeneration, EdgeClusterId,
        HardlinkPolicy, ProjectId, RequestId, ResourceLifecycle, ResourceVersion,
        SnapshotDeliveryId, SnapshotDeliveryMode, SnapshotDeliveryOperation, SnapshotDeliveryState,
        SnapshotId, StorageVolumeId, TenantId,
    };

    use super::*;

    #[tokio::test]
    async fn coordinator_continues_from_the_phase_restored_by_retry() {
        let components = InMemoryComponents::new(1_000);
        let tenant_id = TenantId::new("tenant-retry".to_owned()).unwrap();
        let project_id = ProjectId::new("project-retry".to_owned()).unwrap();
        let artifact_id = ArtifactId::new("artifact-retry".to_owned()).unwrap();
        components
            .control_catalog
            .insert_tenant(TenantRecord {
                tenant_id: tenant_id.clone(),
                display_name: "Retry tenant".to_owned(),
                description: None,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(1),
                updated_at_unix_ms: UnixMillis::new(1),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_artifact(ArtifactRecord {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                display_name: "Retry artifact".to_owned(),
                description: None,
                initialization: ArtifactInitialization::Empty,
                head_commit_id: None,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: UnixMillis::new(2),
                updated_at_unix_ms: UnixMillis::new(2),
            })
            .await
            .unwrap();
        let root = ResourceRef::Artifact {
            project_id,
            artifact_id,
        };
        let impact = components
            .control_catalog
            .query_deletion_impact(DeletionImpactQuery {
                tenant_id: tenant_id.clone(),
                root: root.clone(),
                cascade: false,
                confirm_managed_data_erase: false,
                additional_blockers: Vec::new(),
                authority_impact: None,
                now_unix_ms: UnixMillis::new(1_000),
            })
            .await
            .unwrap();
        let deletion_id = DeletionId::new("deletion-retry".to_owned()).unwrap();
        let mut operation = match components
            .control_catalog
            .create_deletion_idempotent(CreateDeletionRequest {
                deletion_id: deletion_id.clone(),
                tenant_id: tenant_id.clone(),
                root,
                cascade: false,
                confirm_managed_data_erase: false,
                request_id: RequestId::new("delete-retry".to_owned()).unwrap(),
                request_digest: ContentDigest::hash(b"delete-retry"),
                impact_digest: impact.impact_digest,
                expected_resource_version: 1,
                now_unix_ms: UnixMillis::new(1_001),
            })
            .await
            .unwrap()
        {
            CatalogInsertOutcome::Inserted(operation) => operation,
            CatalogInsertOutcome::Existing(_) => panic!("delete must be inserted"),
        };
        for next_state in [
            DeletionOperationState::Quiescing,
            DeletionOperationState::Quarantining,
            DeletionOperationState::Recoverable,
        ] {
            operation = transition(
                components.control_catalog.as_ref(),
                operation,
                next_state,
                1_010,
            )
            .await;
        }
        let purge_at = operation.purge_after_unix_ms.get();
        operation = transition(
            components.control_catalog.as_ref(),
            operation,
            DeletionOperationState::Purging,
            purge_at,
        )
        .await;
        operation = transition(
            components.control_catalog.as_ref(),
            operation,
            DeletionOperationState::Finalizing,
            purge_at + 1,
        )
        .await;
        operation = components
            .control_catalog
            .transition_deletion_state(DeletionTransitionRequest {
                tenant_id: tenant_id.clone(),
                deletion_id: deletion_id.clone(),
                expected_state: DeletionOperationState::Finalizing,
                next_state: DeletionOperationState::Blocked,
                expected_resource_version: operation.resource_version.get(),
                now_unix_ms: UnixMillis::new(purge_at + 2),
                last_error: Some("temporary authority failure".to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(
            operation.resume_state,
            Some(DeletionOperationState::Finalizing)
        );
        operation = match components
            .control_catalog
            .retry_deletion_idempotent(RetryDeletionRequest {
                tenant_id: tenant_id.clone(),
                deletion_id: deletion_id.clone(),
                request_id: RequestId::new("retry-finalizing".to_owned()).unwrap(),
                request_digest: ContentDigest::hash(b"retry-finalizing"),
                expected_resource_version: operation.resource_version.get(),
                now_unix_ms: UnixMillis::new(purge_at + 3),
            })
            .await
            .unwrap()
        {
            CatalogInsertOutcome::Inserted(operation) => operation,
            CatalogInsertOutcome::Existing(_) => panic!("retry must be inserted"),
        };
        assert_eq!(operation.state, DeletionOperationState::Finalizing);

        components.clock.set(purge_at + 4);
        let coordinator = ResourceLifecycleCoordinator::new(
            components.control_catalog.clone(),
            components.agent_registry.clone(),
            components.authority_lifecycle.clone(),
            components.objects.clone(),
            components.clock.clone(),
        )
        .with_task_coordinator(Arc::new(TaskCoordinator::new(
            components.tasks.clone(),
            components.clock.clone(),
        )));
        let run = coordinator.reconcile_once(10).await.unwrap();
        assert_eq!(run.transitioned, 1);
        let completed = components
            .control_catalog
            .get_deletion_operation(&tenant_id, &deletion_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, DeletionOperationState::Completed);
        assert_eq!(completed.completion, Some(DeletionCompletion::Purged));
        assert_eq!(completed.resume_state, None);
        let tasks = components.tasks.all().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_kind, TaskKind::CatalogLifecycle);
        assert_eq!(tasks[0].state, TaskState::Succeeded);
        assert_eq!(tasks[0].detail_kind.as_deref(), Some("deletion"));
        assert_eq!(tasks[0].detail_id.as_deref(), Some("deletion-retry"));
    }

    #[tokio::test]
    async fn snapshot_lifecycle_resolves_its_bound_delivery_volume() {
        let components = InMemoryComponents::new(1_000);
        let tenant_id = TenantId::new("tenant-snapshot-cleanup").unwrap();
        let project_id = ProjectId::new("project-snapshot-cleanup").unwrap();
        let artifact_id = ArtifactId::new("artifact-snapshot-cleanup").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-snapshot-cleanup").unwrap();
        let delivery_id = SnapshotDeliveryId::new("delivery-snapshot-cleanup").unwrap();
        let request_id = RequestId::new("request-snapshot-cleanup").unwrap();
        let volume_id = StorageVolumeId::new("volume-snapshot-cleanup").unwrap();
        let edge_cluster_id = EdgeClusterId::new("cluster-snapshot-cleanup").unwrap();
        let commit_id = ContentDigest::hash(b"snapshot-cleanup-commit");

        components
            .control_catalog
            .insert_tenant(TenantRecord {
                tenant_id: tenant_id.clone(),
                display_name: "Snapshot cleanup tenant".to_owned(),
                description: None,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(1),
                updated_at_unix_ms: UnixMillis::new(1),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_artifact(ArtifactRecord {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                display_name: "Snapshot cleanup artifact".to_owned(),
                description: None,
                initialization: ArtifactInitialization::Empty,
                head_commit_id: None,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: UnixMillis::new(1),
                updated_at_unix_ms: UnixMillis::new(1),
            })
            .await
            .unwrap();
        components
            .control_catalog
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id: tenant_id.clone(),
                storage_volume_id: volume_id.clone(),
                display_name: "Snapshot cleanup volume".to_owned(),
                edge_cluster_id: edge_cluster_id.clone(),
                region: "test".to_owned(),
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteMany,
                allowed_delivery_modes: vec![SnapshotDeliveryMode::Copy],
                hardlink_policy: HardlinkPolicy::Disabled,
                max_whole_file_bytes: DecimalU64::new(u64::MAX),
                copy_reserve_bytes: DecimalU64::new(0),
                pvc_reference: None,
                nfs_reference: None,
                state: StorageVolumeState::Ready,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: UnixMillis::new(1),
                updated_at_unix_ms: UnixMillis::new(1),
            })
            .await
            .unwrap();

        let target_relative_root = SnapshotDeliveryOperation::canonical_target_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
            &delivery_id,
        )
        .unwrap();
        components
            .control_catalog
            .insert_snapshot_with_delivery(SnapshotWithDeliveryInsertRequest {
                snapshot: SnapshotInsertRequest {
                    record: SnapshotRecord {
                        tenant_id: tenant_id.clone(),
                        project_id: project_id.clone(),
                        artifact_id: artifact_id.clone(),
                        snapshot_id: snapshot_id.clone(),
                        snapshot_request_id: request_id.clone(),
                        commit_id,
                        delivery_id: delivery_id.clone(),
                        edge_cluster_id,
                        storage_volume_id: volume_id.clone(),
                        delivery_mode: SnapshotDeliveryMode::Copy,
                        state: SnapshotState::Creating,
                        resource_version: 1,
                        lifecycle: ResourceLifecycle::active(),
                        created_at_unix_ms: UnixMillis::new(1),
                        updated_at_unix_ms: UnixMillis::new(1),
                    },
                    artifact_head: ArtifactHeadExpectation::Any,
                },
                delivery: SnapshotDeliveryInsertRequest {
                    record: SnapshotDeliveryRecord {
                        tenant_id: tenant_id.clone(),
                        delivery_id: delivery_id.clone(),
                        create_request_id: request_id.clone(),
                        snapshot_id: snapshot_id.clone(),
                        commit_id,
                        storage_volume_id: volume_id.clone(),
                        mode: SnapshotDeliveryMode::Copy,
                        target_relative_root,
                        state: SnapshotDeliveryState::Requested,
                        source_index_digest: ContentDigest::hash(b"snapshot-cleanup-index"),
                        delivery_generation: DeliveryGeneration::new(1),
                        file_count: 0,
                        size_bytes: 0,
                        object_set_digest: ContentDigest::hash(b"snapshot-cleanup-objects"),
                        resource_version: 1,
                        issue_code: None,
                        issue_message: None,
                        issue_retryable: false,
                        created_at_unix_ms: UnixMillis::new(1),
                        updated_at_unix_ms: UnixMillis::new(1),
                    },
                    request_id,
                    retention_roots: Vec::new(),
                },
            })
            .await
            .unwrap();

        let operation = DeletionOperation {
            deletion_id: DeletionId::new("deletion-snapshot-cleanup").unwrap(),
            tenant_id: tenant_id.clone(),
            root: ResourceRef::Snapshot {
                snapshot_id: snapshot_id.clone(),
            },
            state: DeletionOperationState::Quarantining,
            resource_version: ResourceVersion::new(1),
            targets: Vec::new(),
            request_id: RequestId::new("delete-snapshot-cleanup").unwrap(),
            request_digest: ContentDigest::hash(b"delete-snapshot-cleanup"),
            impact_digest: ContentDigest::hash(b"impact-snapshot-cleanup"),
            cascade: false,
            confirm_managed_data_erase: false,
            purge_after_unix_ms: UnixMillis::new(2_000),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
            completion: None,
            last_error: None,
            resume_state: None,
            retry_count: DecimalU64::new(0),
        };
        let coordinator = ResourceLifecycleCoordinator::new(
            components.control_catalog.clone(),
            components.agent_registry.clone(),
            components.authority_lifecycle.clone(),
            components.objects.clone(),
            components.clock.clone(),
        );

        assert_eq!(
            coordinator.operation_volume_ids(&operation).await.unwrap(),
            vec![volume_id.clone()]
        );
        assert_eq!(
            coordinator
                .lifecycle_scope(&operation, &volume_id)
                .await
                .unwrap(),
            AgentResourceLifecycleScope::Snapshot {
                project_id,
                artifact_id: artifact_id.clone(),
                snapshot_id,
                storage_volume_id: volume_id.clone(),
                artifact_placement_id: placement_id(&tenant_id, &artifact_id, &volume_id,).unwrap(),
                placement_generation: PlacementGeneration::new(1),
            }
        );
    }

    async fn transition(
        catalog: &dyn ControlCatalogRepository,
        operation: DeletionOperation,
        next_state: DeletionOperationState,
        now_unix_ms: u64,
    ) -> DeletionOperation {
        catalog
            .transition_deletion_state(DeletionTransitionRequest {
                tenant_id: operation.tenant_id.clone(),
                deletion_id: operation.deletion_id.clone(),
                expected_state: operation.state,
                next_state,
                expected_resource_version: operation.resource_version.get(),
                now_unix_ms: UnixMillis::new(now_unix_ms),
                last_error: None,
            })
            .await
            .unwrap()
    }

    #[test]
    fn retry_assignment_identity_changes_without_changing_the_action() {
        let mut operation = DeletionOperation {
            deletion_id: DeletionId::new("deletion-assignment".to_owned()).unwrap(),
            tenant_id: TenantId::new("tenant-assignment".to_owned()).unwrap(),
            root: ResourceRef::StorageVolume {
                storage_volume_id: neoengram_domain::protocol::StorageVolumeId::new(
                    "volume-assignment".to_owned(),
                )
                .unwrap(),
            },
            state: DeletionOperationState::Purging,
            resource_version: ResourceVersion::new(1),
            targets: Vec::new(),
            request_id: RequestId::new("request-assignment".to_owned()).unwrap(),
            request_digest: ContentDigest::hash(b"request-assignment"),
            impact_digest: ContentDigest::hash(b"impact-assignment"),
            cascade: false,
            confirm_managed_data_erase: true,
            purge_after_unix_ms: UnixMillis::new(1),
            created_at_unix_ms: UnixMillis::new(1),
            updated_at_unix_ms: UnixMillis::new(1),
            completion: None,
            last_error: None,
            resume_state: None,
            retry_count: neoengram_domain::protocol::DecimalU64::new(0),
        };
        let volume_id = match &operation.root {
            ResourceRef::StorageVolume { storage_volume_id } => storage_volume_id.clone(),
            _ => unreachable!(),
        };
        let first = lifecycle_assignment_id(
            &operation,
            ResourceLifecycleAction::Purge,
            &volume_id,
            neoengram_domain::protocol::LifecycleGeneration::new(1),
        )
        .unwrap();
        operation.retry_count = neoengram_domain::protocol::DecimalU64::new(1);
        let retried = lifecycle_assignment_id(
            &operation,
            ResourceLifecycleAction::Purge,
            &volume_id,
            neoengram_domain::protocol::LifecycleGeneration::new(1),
        )
        .unwrap();
        assert_ne!(first, retried);
    }
}
