use std::{collections::BTreeSet, sync::Arc};

use neoengram_domain::protocol::materialization::{
    BatchManifest, CoverageState, IntegrityScanReport, MaterializationBatch,
    MaterializationBatchState, MaterializationBatchTicket, MaterializationJob,
    MaterializationJobState, MaterializationObjectState, MaterializationReport,
    VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    object_read_lease_id, staging_lease_id, AgentId, AssignmentOperation, CommitObject,
    ControlError, ControlMessage, DecisionGeneration, DeletionOperationState, DeletionProof,
    DeletionProofId, DeletionProofResult, Envelope, EnvelopeHeader, ErrorCode, Extensions,
    Generation, IndexRevision, JobAssignment, JobDecision, JobFinalized, JobState, LifecycleEvent,
    LifecycleEventId, LifecycleEventKind, MessageId, MountGeneration, ObjectNamespaceId, ObjectSet,
    ObjectTicketId, PlacementGeneration, PrincipalId, PrincipalKind, PrincipalRef, PublishDecision,
    ReplicationAssignment, ReplicationObjectState, ReplicationProgressReport, ReplicationState,
    RequestId, ResourceLifecycleReport, ResourceLifecycleReportState, ResourceVersion,
    RouteGeneration, SessionGeneration, SignedTransferTicket, SnapshotDeliveryAssignment,
    SnapshotDeliveryState, StageState, TaskActor, TaskExecutionFence, TaskId, TaskIssue,
    TaskProgressSummary, TaskState, TenantId, TraceId, TransferEndpoint, TransferTicket,
    UnixMillis, WireIndexVersion, WorkspaceMaterializeAssignment, AGENT_JOB_ASSIGNMENT_ACTION,
    AGENT_JOB_DECISION_ACTION, AGENT_LIFECYCLE_ASSIGNMENT_ACTION,
    AGENT_MATERIALIZATION_ASSIGNMENT_ACTION, CURRENT_WIRE_VERSION,
};

use crate::{
    validation::{
        invalid, validate_assignment_target, validate_descriptor_scope, validate_job_spec,
        validate_prepared, validate_report_identity, validate_staged_metadata,
        validate_terminal_state,
    },
    Action, Actor, AddJobSpec, AgentRegistryRepository, AgentReport, AssignJobRequest,
    AssignJobResult, AssignSnapshotDeliveryRequest, AssignSnapshotDeliveryResult,
    AssignWorkspaceMaterializationRequest, AssignWorkspaceMaterializationResult, AssignmentOutbox,
    AuditEvent, AuditKind, AuditSink, AuthorityStore, AuthorizationRequest, Authorizer,
    CentralError, CentralErrorCode, CentralResult, Clock, ControlCatalogRepository,
    CreateAddJobRequest, CreateAddJobResult, CreateSnapshotDeliveryRequest,
    CreateSnapshotDeliveryResult, CreateWorkspaceMaterializationRequest,
    CreateWorkspaceMaterializationResult, ExpireAddJobRequest, ExpireAddJobResult,
    FinalizeAddRequest, FinalizeAddResult, FinalizeReplicationRequest, GatewayRegistryRepository,
    IndexPublishOutcome, IndexPublishRejection, IndexPublishRequest, IndexPublisher,
    JobInsertOutcome, JobOperation, JobRecord, JobRepository, MetadataBatchStager,
    MetadataBatchSubmission, ObjectCatalog, PlacementRepository, PublicationCandidate,
    QueryJobRequest, QueryJobResult, ReceiveReportRequest, ReceiveReportResult,
    RefreshReplicationRoutesRequest, ReplicationObjectRecord, ReplicationRecord,
    ReplicationRouteBinding, ReplicationStateTransitionRequest, ResumePublicationRequest,
    StageMetadataBatchRequest, StageMetadataBatchResult,
};

use crate::service::{
    CentralCommandKeyring, TaskCoordinator, DEFAULT_CENTRAL_COMMAND_TTL_MS,
    MAX_CENTRAL_COMMAND_TTL_MS,
};

const CONTROL_ERROR_MESSAGE_LIMIT: usize = 4096;

/// Derive the stable execution fence used by the legacy Job ledger while all deliveries are
/// being migrated to the unified OperationTask protocol. The Job records do not yet persist a
/// separate task identity, so the deterministic Job/Replication identity is used as the root
/// task key. Every report and decision copies the fence from its persisted assignment.
fn execution_fence(
    source_id: &str,
    attempt: u64,
    stage_key: &'static str,
) -> CentralResult<TaskExecutionFence> {
    Ok(TaskExecutionFence::new(
        TaskId::new(format!("task-{source_id}"))?,
        Generation::new(attempt.max(1)),
        stage_key,
        Generation::new(1),
        Generation::new(1),
    ))
}

fn operation_task_fence_fallback(
    operation_task_id: Option<&TaskId>,
    source_id: &str,
    attempt: u64,
    stage_key: &'static str,
) -> CentralResult<TaskExecutionFence> {
    let task_id = operation_task_id
        .cloned()
        .unwrap_or(TaskId::new(format!("task-{source_id}"))?);
    Ok(TaskExecutionFence::new(
        task_id,
        Generation::new(attempt.max(1)),
        stage_key,
        Generation::new(1),
        Generation::new(1),
    ))
}

/// Loads the authoritative root-task and stage generations for a control assignment. The
/// operation task is the fencing source of truth after a retry; the legacy Job row must never
/// manufacture a new `attempt` or `stage_attempt` from a hard-coded value. Focused Job-only
/// compositions may omit the coordinator, in which case the deterministic fallback remains
/// available for tests that deliberately exercise the pre-task control surface.
async fn operation_task_fence(
    coordinator: Option<&TaskCoordinator>,
    operation_task_id: Option<&TaskId>,
    tenant_id: &TenantId,
    source_id: &str,
    fallback_attempt: u64,
    stage_key: &'static str,
) -> CentralResult<TaskExecutionFence> {
    let Some(operation_task_id) = operation_task_id else {
        // A Job can still be exercised through the lower-level managed-Add port without a
        // public operation root (for example while validating the delivery ledger in isolation).
        // Once a root is attached, the branch below is intentionally strict and reads every
        // generation from that root.  The deterministic fallback keeps these internal jobs
        // replay-safe without pretending they are user-visible operation tasks.
        return operation_task_fence_fallback(None, source_id, fallback_attempt, stage_key);
    };
    let Some(coordinator) = coordinator else {
        return operation_task_fence_fallback(
            Some(operation_task_id),
            source_id,
            fallback_attempt,
            stage_key,
        );
    };
    let repository = coordinator.repository();
    let Some(task) = repository.get(tenant_id, operation_task_id).await? else {
        return Err(invalid(
            CentralErrorCode::ResourceNotFound,
            format!(
                "operation task {operation_task_id} for {source_id} is not present in the authority"
            ),
        ));
    };
    let stage = task
        .stages
        .iter()
        .find(|stage| stage.stage_key == stage_key)
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                format!(
                    "operation task {} has no stage {}",
                    operation_task_id, stage_key
                ),
            )
        })?;
    Ok(TaskExecutionFence::new(
        task.task_id,
        task.attempt,
        stage.stage_key.clone(),
        stage.stage_attempt,
        Generation::new(1),
    ))
}

fn task_attempt_generation(value: &neoengram_domain::protocol::TaskAttemptId) -> Generation {
    value
        .as_str()
        .rsplit_once("-attempt-")
        .and_then(|(_, suffix)| suffix.parse::<u64>().ok())
        .map(Generation::new)
        .unwrap_or_else(|| Generation::new(1))
}

fn task_text(value: &str) -> String {
    const LIMIT: usize = neoengram_domain::protocol::MAX_TASK_TEXT_BYTES;
    if value.len() <= LIMIT {
        return value.to_owned();
    }
    let mut truncated = String::new();
    for character in value.chars() {
        if truncated.len().saturating_add(character.len_utf8()) > LIMIT.saturating_sub(3) {
            break;
        }
        truncated.push(character);
    }
    truncated.push_str("...");
    truncated
}

fn action_envelope(
    action: &'static str,
    request_id: MessageId,
    tenant_scope: neoengram_domain::protocol::TenantId,
    session_generation: SessionGeneration,
    deadline: UnixMillis,
    body: ControlMessage,
) -> CentralResult<Envelope<ControlMessage>> {
    let request_id = RequestId::new(request_id.as_str())?;
    let envelope = Envelope {
        header: EnvelopeHeader {
            wire_version: CURRENT_WIRE_VERSION,
            action: action.to_owned(),
            request_id: request_id.clone(),
            trace_id: TraceId::new(request_id.as_str())?,
            tenant_scope: Some(tenant_scope),
            actor: None,
            session_generation: Some(session_generation),
            route_generation: None,
            deadline,
        },
        body,
    };
    envelope.validate()?;
    Ok(envelope)
}

fn assignment_deadline(assignment: &JobAssignment) -> UnixMillis {
    match &assignment.assignment {
        AssignmentOperation::Add { input, .. } => input.deadline_unix_ms,
        AssignmentOperation::WorkspaceMaterialize { input, .. } => input.deadline_unix_ms,
        AssignmentOperation::SnapshotDelivery { input, .. } => input.deadline_unix_ms,
    }
}

/// Build the durable request ID for a materialization assignment.
///
/// Resource IDs are intentionally allowed to be fairly long, while MessageId has the same
/// 128-byte limit as every other wire identifier.  Hashing the full assignment identity keeps the
/// ID deterministic for replay/deduplication without allowing a long materialization or batch ID
/// to make an otherwise valid assignment fail envelope validation.
fn materialization_assignment_message_id(
    materialization_id: &neoengram_domain::protocol::MaterializationId,
    batch_id: &neoengram_domain::protocol::MaterializationBatchId,
    plan_revision: Generation,
    batch_attempt: Generation,
) -> CentralResult<MessageId> {
    let digest = blake3::hash(
        format!(
            "materialization-assignment-message\\0{}\\0{}\\0{}\\0{}",
            materialization_id, batch_id, plan_revision, batch_attempt
        )
        .as_bytes(),
    );
    MessageId::new(format!("materialization-{}", &digest.to_hex()[..64])).map_err(Into::into)
}

async fn target_volume_coverage_complete(
    placement: &dyn PlacementRepository,
    tenant_id: &neoengram_domain::protocol::TenantId,
    artifact_id: &neoengram_domain::protocol::ArtifactId,
    commit_id: neoengram_domain::core::ContentDigest,
    volume_id: &neoengram_domain::protocol::StorageVolumeId,
) -> CentralResult<bool> {
    let Some(stored) = placement
        .get_commit_object_set(tenant_id, &commit_id)
        .await?
    else {
        return Ok(false);
    };
    if stored.tenant_id != *tenant_id || stored.commit_id.digest() != commit_id {
        return Ok(false);
    }
    let namespace = ObjectNamespaceId::new(artifact_id.to_string())
        .map_err(|error| invalid(CentralErrorCode::InvalidState, error.to_string()))?;
    let object_set =
        neoengram_domain::protocol::materialization::NamespaceObjectSet::from_object_set(
            tenant_id.clone(),
            namespace.clone(),
            stored.commit_id,
            &stored.object_set,
        )?;
    let mut placements = Vec::new();
    for object in &object_set.objects {
        placements.extend(
            placement
                .object_placements_v2(tenant_id, &namespace, &object.object_id)
                .await?
                .into_iter()
                .filter(|item| {
                    item.readable()
                        && item.object_namespace_id == namespace
                        && item.object_id == object.object_id
                        && item.size == object.size
                        && item.encoding == object.encoding
                }),
        );
    }
    let Some(generation) = placements
        .iter()
        .filter(|item| item.storage_volume_id.as_ref() == Some(volume_id))
        .map(|item| item.placement_generation)
        .max()
        .or_else(|| {
            object_set
                .objects
                .is_empty()
                .then_some(PlacementGeneration::new(1))
        })
    else {
        return Ok(false);
    };
    let legacy = ObjectSet::new(
        object_set
            .objects
            .iter()
            .map(|object| {
                CommitObject::new(
                    object.object_id,
                    object.size.get(),
                    object.encoding,
                    object.ordinal.get(),
                )
            })
            .collect(),
    )?;
    let coverage = VolumeCommitCoverage::from_placements(
        tenant_id.clone(),
        namespace,
        stored.commit_id,
        volume_id.clone(),
        generation,
        &legacy,
        &placements,
    )?;
    Ok(coverage.state == CoverageState::Complete)
}

#[allow(dead_code)]
fn replication_ticket_deadline(now: UnixMillis) -> CentralResult<UnixMillis> {
    now.get()
        .checked_add(DEFAULT_CENTRAL_COMMAND_TTL_MS)
        .map(UnixMillis::new)
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::DeadlineExceeded,
                "replication ticket deadline overflowed",
            )
        })
}

fn materialization_ticket_window(
    now: UnixMillis,
    batch_deadline: UnixMillis,
) -> CentralResult<(UnixMillis, u64)> {
    if batch_deadline.get() <= now.get() {
        return Err(invalid(
            CentralErrorCode::DeadlineExceeded,
            "materialization Batch deadline has elapsed",
        ));
    }
    let remaining_ms = batch_deadline.get().checked_sub(now.get()).ok_or_else(|| {
        invalid(
            CentralErrorCode::DeadlineExceeded,
            "materialization Batch deadline has elapsed",
        )
    })?;
    let ttl_ms = remaining_ms.min(MAX_CENTRAL_COMMAND_TTL_MS);
    let ticket_deadline = now
        .get()
        .checked_add(ttl_ms)
        .map(UnixMillis::new)
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::DeadlineExceeded,
                "materialization Ticket deadline overflowed",
            )
        })?;
    Ok((ticket_deadline, ttl_ms))
}

fn lifecycle_event_id(
    assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    report_digest: &neoengram_domain::protocol::ContentDigest,
) -> CentralResult<LifecycleEventId> {
    let digest = blake3::hash(format!("event\0{assignment_id}\0{report_digest}").as_bytes());
    LifecycleEventId::new(format!("lifecycle-event-{digest}")).map_err(Into::into)
}

fn lifecycle_proof_id(
    assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    report_digest: &neoengram_domain::protocol::ContentDigest,
) -> CentralResult<DeletionProofId> {
    let digest = blake3::hash(format!("proof\0{assignment_id}\0{report_digest}").as_bytes());
    DeletionProofId::new(format!("deletion-proof-{digest}")).map_err(Into::into)
}

/// Central managed-Add application service composed entirely from explicit ports.
pub struct ControlPlane {
    authorizer: Arc<dyn Authorizer>,
    jobs: Arc<dyn JobRepository>,
    outbox: Arc<dyn AssignmentOutbox>,
    metadata: Arc<dyn MetadataBatchStager>,
    objects: Arc<dyn ObjectCatalog>,
    publisher: Arc<dyn IndexPublisher>,
    audit: Arc<dyn AuditSink>,
    catalog: Option<Arc<dyn ControlCatalogRepository>>,
    agent_registry: Option<Arc<dyn AgentRegistryRepository>>,
    placement: Option<Arc<dyn PlacementRepository>>,
    gateway_registry: Option<Arc<dyn GatewayRegistryRepository>>,
    replication_ticket_keyring: Option<Arc<CentralCommandKeyring>>,
    task_coordinator: Option<Arc<TaskCoordinator>>,
    clock: Arc<dyn Clock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationReportResult {
    pub resource_version: ResourceVersion,
    pub replayed: bool,
}

/// Result of applying one object-level materialization report.  Materialization rows do not use
/// the Job resource-version aggregate, so the channel ACK carries a stable non-zero sentinel;
/// the actual fencing keys are the plan revision, batch attempt, and target route generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationReportResult {
    pub resource_version: ResourceVersion,
    pub replayed: bool,
}

/// Result of applying one authenticated Volume integrity scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrityReportResult {
    pub resource_version: ResourceVersion,
    pub replayed: bool,
}

async fn release_materialization_batch_leases(
    placement: &dyn PlacementRepository,
    job: &neoengram_domain::protocol::materialization::MaterializationJob,
    batch: &MaterializationBatch,
) -> CentralResult<()> {
    let tasks = placement
        .list_materialization_objects(
            &job.key.tenant_id,
            &job.key.object_namespace_id,
            &job.materialization_id,
        )
        .await?;
    for object_id in &batch.object_ids {
        release_materialization_object_leases(placement, job, batch, &tasks, *object_id).await?;
    }
    Ok(())
}

async fn release_materialization_object_leases(
    placement: &dyn PlacementRepository,
    job: &neoengram_domain::protocol::materialization::MaterializationJob,
    batch: &MaterializationBatch,
    tasks: &[neoengram_domain::protocol::materialization::MaterializationObject],
    object_id: neoengram_domain::core::ObjectId,
) -> CentralResult<()> {
    if let Some(task) = tasks.iter().find(|task| {
        task.object.object_namespace_id == job.key.object_namespace_id
            && task.object.object_id == object_id
    }) {
        // Keep every authority-selected source alive for the lifetime of the object attempt.
        // A source switch can happen after the primary route fails; releasing/protecting only
        // the primary would let GC reclaim a fallback placement while it is still valid work.
        let source_ids = task
            .primary_source
            .iter()
            .chain(task.fallback_sources.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        for source_id in source_ids {
            let lease_id = object_read_lease_id(
                &job.materialization_id,
                &batch.batch_id,
                batch.plan_revision,
                batch.batch_attempt,
                &job.key.object_namespace_id,
                object_id,
                &source_id,
            )?;
            placement
                .release_object_read_lease(
                    &job.key.tenant_id,
                    &job.key.object_namespace_id,
                    &lease_id,
                )
                .await?;
        }
    }
    let lease_id = staging_lease_id(
        &job.materialization_id,
        batch.plan_revision,
        &job.key.object_namespace_id,
        object_id,
    )?;
    placement
        .release_staging_lease(&job.key.tenant_id, &job.key.object_namespace_id, &lease_id)
        .await?;
    Ok(())
}

/// Revalidates the route fence captured in a durable Batch immediately before issuing an Agent
/// assignment.  A Batch can outlive a Gateway reconnect; checking only the target session (the
/// control channel's session) is insufficient because the source Gateway may have advanced its
/// route generation independently.  Tickets are therefore never signed for an old route tuple.
async fn validate_materialization_route_fence(
    registry: &dyn GatewayRegistryRepository,
    agent_id: &AgentId,
    edge_cluster_id: &neoengram_domain::protocol::EdgeClusterId,
    gateway_pool_id: &neoengram_domain::protocol::GatewayPoolId,
    session_generation: SessionGeneration,
    route_generation: RouteGeneration,
    now: UnixMillis,
) -> CentralResult<()> {
    let route = registry
        .get_agent_route(agent_id)
        .await?
        .filter(|route| route.is_active_at(now))
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::GatewayRouteUnavailable,
                "materialization route is unavailable",
            )
        })?;
    if route.edge_cluster_id != *edge_cluster_id
        || route.gateway_pool_id != *gateway_pool_id
        || route.session_generation != session_generation
        || route.route_generation != route_generation
    {
        return Err(invalid(
            CentralErrorCode::GatewayRouteFenced,
            "materialization route generation changed since the Batch was planned",
        ));
    }
    Ok(())
}

/// A Gateway route can remain alive briefly while Volume ownership is being fenced. Materialization
/// tickets must carry the owner generation as well, otherwise a reconnecting old Agent could
/// continue serving or receiving bytes through an otherwise healthy route.
async fn validate_materialization_owner_fence(
    registry: &dyn AgentRegistryRepository,
    agent_id: &AgentId,
    volume_id: &neoengram_domain::protocol::StorageVolumeId,
    placement_generation: PlacementGeneration,
) -> CentralResult<()> {
    let record = registry.get_by_agent(agent_id).await?.ok_or_else(|| {
        invalid(
            CentralErrorCode::GatewayRouteUnavailable,
            "materialization Agent enrollment is unavailable",
        )
    })?;
    if record.mount.storage_volume_id != *volume_id
        || record.owner.storage_volume_id != *volume_id
        || record.owner.active_agent_id.as_ref() != Some(agent_id)
        || record.owner.active_agent_mount_id.as_ref() != Some(&record.mount.agent_mount_id)
        || record.owner.owner_generation.get() != placement_generation.get()
    {
        return Err(invalid(
            CentralErrorCode::GatewayRouteFenced,
            "materialization Volume owner generation changed since the Batch was planned",
        ));
    }
    Ok(())
}

async fn validate_replication_report_binding(
    placement: &dyn PlacementRepository,
    tenant_id: &neoengram_domain::protocol::TenantId,
    current: &ReplicationRecord,
    report: &ReplicationProgressReport,
) -> CentralResult<()> {
    let expected = execution_fence(current.replication_id.as_str(), current.attempt, "transfer")?;
    if report.task_fence() != &expected {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "replication report carries a stale task execution fence",
        ));
    }
    match report {
        ReplicationProgressReport::State {
            state,
            completed_objects,
            completed_bytes,
            ..
        } => {
            if *completed_objects > current.total_objects || *completed_bytes > current.total_bytes
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "replication progress exceeds its frozen total",
                ));
            }
            if *state == ReplicationState::Published && current.state != ReplicationState::Published
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Published state requires a placement publication report",
                ));
            }
        }
        ReplicationProgressReport::Object {
            object_id,
            offset,
            state,
            ..
        } => {
            let object_set = placement
                .get_commit_object_set(tenant_id, &current.commit_id)
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::InvalidState,
                        "Commit ObjectSet is missing",
                    )
                })?;
            let expected = object_set
                .object_set
                .objects
                .iter()
                .find(|object| object.object_id == *object_id)
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "object is outside the frozen ObjectSet",
                    )
                })?;
            if *offset > expected.size.get()
                || (*state == ReplicationObjectState::Verified && *offset != expected.size.get())
            {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "replication object checkpoint has an invalid offset",
                ));
            }
        }
        ReplicationProgressReport::Published {
            commit_id,
            object_set_digest,
            ..
        } => {
            if commit_id.digest() != current.commit_id
                || *object_set_digest != current.object_set_digest
            {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "replication publication differs from its frozen Commit/ObjectSet",
                ));
            }
        }
    }
    Ok(())
}

fn replication_active_state_rank(state: ReplicationState) -> Option<u8> {
    match state {
        ReplicationState::Queued => Some(0),
        ReplicationState::Planning => Some(1),
        ReplicationState::Transferring => Some(2),
        ReplicationState::Verifying => Some(3),
        ReplicationState::Published | ReplicationState::Failed | ReplicationState::Cancelled => {
            None
        }
    }
}

fn replication_object_state_rank(state: ReplicationObjectState) -> u8 {
    match state {
        ReplicationObjectState::Queued => 0,
        ReplicationObjectState::Transferring => 1,
        ReplicationObjectState::Verified => 2,
        // A failed object is terminal for this attempt and must not be replaced by a stale
        // progress event from a worker that was still unwinding when the failure was recorded.
        ReplicationObjectState::Failed => 3,
    }
}

fn current_is_terminal_replication(state: ReplicationState) -> bool {
    matches!(
        state,
        ReplicationState::Published | ReplicationState::Failed | ReplicationState::Cancelled
    )
}

#[allow(dead_code)]
fn replication_delivery_can_wait_for_next_tick(error: &crate::CentralError) -> bool {
    match error.code() {
        CentralErrorCode::GatewayRouteUnavailable => error.retryable(),
        // Repository CAS adapters may mark the caller's exact stale request as non-retryable.
        // Delivery still retries by re-reading the authoritative replication on the next tick.
        CentralErrorCode::ConcurrentUpdate => true,
        _ => false,
    }
}

fn reconnected_replication_report_matches_route(
    stored_session: SessionGeneration,
    stored_mount: MountGeneration,
    stored_route: RouteGeneration,
    report_session: SessionGeneration,
    refreshed: ReplicationRouteGenerations,
) -> bool {
    refreshed.session == report_session
        && refreshed.session.get() > stored_session.get()
        && refreshed.mount == stored_mount
        && refreshed.route.get() >= stored_route.get()
}

#[derive(Debug, Clone, Copy)]
struct ReplicationRouteGenerations {
    session: SessionGeneration,
    mount: MountGeneration,
    route: RouteGeneration,
}

fn replication_route_binding(
    record: &ReplicationRecord,
    source: bool,
    session_generation: SessionGeneration,
    mount_generation: MountGeneration,
    route_generation: RouteGeneration,
) -> CentralResult<ReplicationRouteBinding> {
    let (edge_cluster_id, gateway_pool_id, agent_id) = if source {
        (
            record.source_edge_cluster_id.clone(),
            record.source_gateway_pool_id.clone(),
            record.source_agent_id.clone(),
        )
    } else {
        (
            record.target_edge_cluster_id.clone(),
            record.target_gateway_pool_id.clone(),
            record.target_agent_id.clone(),
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
        session_generation,
        mount_generation,
        route_generation,
    })
}

impl ControlPlane {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        authorizer: Arc<dyn Authorizer>,
        authority: AuthorityStore,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            authorizer,
            jobs: authority.jobs(),
            outbox: authority.outbox(),
            metadata: authority.metadata(),
            objects: authority.objects(),
            publisher: authority.publisher(),
            audit: authority.audit(),
            catalog: authority.control_catalog(),
            agent_registry: authority.agent_registry(),
            placement: authority.placement(),
            gateway_registry: authority.gateway_registry(),
            replication_ticket_keyring: None,
            task_coordinator: None,
            clock,
        }
    }

    /// Installs the Placement authority used to schedule and finalize Commit replication. The
    /// optional form keeps the Job-only control plane usable in focused unit tests.
    #[must_use]
    pub fn with_placement_repository(mut self, placement: Arc<dyn PlacementRepository>) -> Self {
        self.placement = Some(placement);
        self
    }

    /// Installs the Central signing keyring used for Agent replication assignments. Without a
    /// signer, replication remains durable and queryable but is intentionally not delivered to a
    /// data-plane Agent.
    #[must_use]
    pub fn with_replication_ticket_keyring(mut self, keyring: Arc<CentralCommandKeyring>) -> Self {
        self.replication_ticket_keyring = Some(keyring);
        self
    }

    /// Installs the unified operation-task coordinator used to mirror materialization state and
    /// progress into the task/audit authority. Focused control-plane tests may omit it; the
    /// production runtime always wires the coordinator from the SQLite authority.
    #[must_use]
    pub fn with_task_coordinator(mut self, coordinator: Arc<TaskCoordinator>) -> Self {
        self.task_coordinator = Some(coordinator);
        self
    }

    /// Installs the Gateway route registry used to refresh session/route generations when a
    /// control channel reconnects. The immutable Agent, Volume, cluster, and pool bindings stay
    /// on the Replication record; only the live transport generations are refreshed.
    #[must_use]
    pub fn with_gateway_registry(mut self, registry: Arc<dyn GatewayRegistryRepository>) -> Self {
        self.gateway_registry = Some(registry);
        self
    }

    fn materialization_task_state(state: MaterializationJobState) -> TaskState {
        match state {
            MaterializationJobState::Queued => TaskState::Queued,
            MaterializationJobState::Planning | MaterializationJobState::Materializing => {
                TaskState::Running
            }
            MaterializationJobState::WaitingForSources => TaskState::Waiting,
            MaterializationJobState::Verifying => TaskState::Verifying,
            MaterializationJobState::Complete => TaskState::Succeeded,
            MaterializationJobState::Stalled => TaskState::Stalled,
            MaterializationJobState::Failed => TaskState::Failed,
            MaterializationJobState::Cancelled => TaskState::Cancelled,
        }
    }

    /// Mirrors the materialization aggregate into the explicit v2 stage DAG. The Job remains the
    /// source of truth for batches and object receipts; these transitions only expose the coarse
    /// user-facing milestones on the single `commit.materialize` task.
    async fn sync_materialization_stages(
        &self,
        job: &MaterializationJob,
        issue: Option<&TaskIssue>,
    ) -> CentralResult<()> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok(());
        };
        let repository = coordinator.repository();
        let stages = repository
            .stages(&job.key.tenant_id, &job.operation_task_id)
            .await?;
        if stages.is_empty() {
            return Ok(());
        }

        // A successful Job has crossed every materialization barrier. Intermediate Job states
        // deliberately leave downstream stages pending, so a root task cannot complete early.
        let targets: &[(&str, StageState)] = match job.state {
            MaterializationJobState::Queued => &[],
            MaterializationJobState::Planning => &[
                ("validate", StageState::Succeeded),
                ("plan", StageState::Running),
            ],
            MaterializationJobState::WaitingForSources => &[
                ("validate", StageState::Succeeded),
                ("plan", StageState::Succeeded),
                ("transfer", StageState::Waiting),
            ],
            MaterializationJobState::Materializing => &[
                ("validate", StageState::Succeeded),
                ("plan", StageState::Succeeded),
                ("transfer", StageState::Running),
            ],
            MaterializationJobState::Verifying => &[
                ("validate", StageState::Succeeded),
                ("plan", StageState::Succeeded),
                ("transfer", StageState::Succeeded),
                ("verify", StageState::Verifying),
            ],
            MaterializationJobState::Complete => &[
                ("validate", StageState::Succeeded),
                ("plan", StageState::Succeeded),
                ("transfer", StageState::Succeeded),
                ("verify", StageState::Succeeded),
                ("publish_coverage", StageState::Succeeded),
                ("finalize", StageState::Succeeded),
            ],
            MaterializationJobState::Stalled => &[
                ("validate", StageState::Succeeded),
                ("plan", StageState::Succeeded),
                ("transfer", StageState::Stalled),
            ],
            MaterializationJobState::Failed => &[
                ("validate", StageState::Succeeded),
                ("plan", StageState::Succeeded),
                ("transfer", StageState::Failed),
            ],
            MaterializationJobState::Cancelled => &[],
        };

        for (stage_key, target) in targets {
            let current = repository
                .stages(&job.key.tenant_id, &job.operation_task_id)
                .await?
                .into_iter()
                .find(|stage| stage.stage_key == *stage_key);
            let Some(current) = current else {
                continue;
            };
            if current.state.is_success() && *target == StageState::Succeeded {
                continue;
            }
            // A later aggregate observation must never regress a stage that already crossed its
            // durability barrier. This can happen when a stale Job snapshot reports `stalled`
            // after the final receipt committed the transfer stage.
            if current.state.is_success() && *target != StageState::Succeeded {
                continue;
            }
            let stage_issue = if matches!(target, StageState::Stalled | StageState::Failed) {
                issue.cloned()
            } else {
                None
            };

            // The stage state machine intentionally requires active states before a terminal
            // success/failure. Drive each transition through the legal path and let the
            // repository CAS turn a concurrent report into an idempotent retry.
            if *target == StageState::Succeeded {
                if current.state == StageState::Pending {
                    coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            StageState::Ready,
                            None,
                        )
                        .await
                        .map(|_| ())?;
                }
                let current = repository
                    .stages(&job.key.tenant_id, &job.operation_task_id)
                    .await?
                    .into_iter()
                    .find(|stage| stage.stage_key == *stage_key);
                if let Some(current) = current {
                    if !current.state.is_success() && current.state != StageState::Running {
                        coordinator
                            .transition_stage(
                                &job.operation_task_id,
                                &job.key.tenant_id,
                                stage_key,
                                StageState::Running,
                                None,
                            )
                            .await
                            .map(|_| ())?;
                    }
                }
                let current = repository
                    .stages(&job.key.tenant_id, &job.operation_task_id)
                    .await?
                    .into_iter()
                    .find(|stage| stage.stage_key == *stage_key);
                if let Some(current) = current {
                    if !current.state.is_success() {
                        coordinator
                            .transition_stage(
                                &job.operation_task_id,
                                &job.key.tenant_id,
                                stage_key,
                                StageState::Succeeded,
                                None,
                            )
                            .await
                            .map(|_| ())?;
                    }
                }
            } else if *target == StageState::Waiting || *target == StageState::Verifying {
                let mut current = current;
                if current.state == StageState::Pending {
                    current = coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            StageState::Ready,
                            None,
                        )
                        .await?;
                }
                if current.state == StageState::Ready || current.state == StageState::Stalled {
                    current = coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            StageState::Running,
                            None,
                        )
                        .await?;
                }
                if current.state != *target {
                    coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            *target,
                            stage_issue,
                        )
                        .await
                        .map(|_| ())?;
                }
            } else if *target == StageState::Running {
                let mut current = current;
                if current.state == StageState::Pending {
                    current = coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            StageState::Ready,
                            None,
                        )
                        .await?;
                }
                if current.state != StageState::Running && !current.state.is_success() {
                    coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            StageState::Running,
                            None,
                        )
                        .await
                        .map(|_| ())?;
                }
            } else if *target == StageState::Stalled || *target == StageState::Failed {
                let mut current = current;
                if current.state == StageState::Pending {
                    current = coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            StageState::Ready,
                            None,
                        )
                        .await?;
                }
                if current.state == StageState::Ready {
                    current = coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            StageState::Running,
                            None,
                        )
                        .await?;
                }
                if current.state != *target {
                    coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            stage_key,
                            *target,
                            stage_issue,
                        )
                        .await
                        .map(|_| ())?;
                }
            }
        }

        if job.state == MaterializationJobState::Cancelled {
            // Cancellation is a convergence point. Mark every still-active stage cancelled only
            // after the durable Job has reached Cancelled; no new stage is started afterwards.
            for current in repository
                .stages(&job.key.tenant_id, &job.operation_task_id)
                .await?
            {
                if current.state.is_success() || current.state == StageState::Cancelled {
                    continue;
                }
                if current.state != StageState::Cancelling {
                    coordinator
                        .transition_stage(
                            &job.operation_task_id,
                            &job.key.tenant_id,
                            &current.stage_key,
                            StageState::Cancelling,
                            None,
                        )
                        .await?;
                }
                coordinator
                    .transition_stage(
                        &job.operation_task_id,
                        &job.key.tenant_id,
                        &current.stage_key,
                        StageState::Cancelled,
                        None,
                    )
                    .await
                    .map(|_| ())?;
            }
        }
        Ok(())
    }

    /// Projects one control-plane Job used by Workspace/Snapshot flows onto its owning root task.
    /// The Job remains the source of truth for assignment and Agent observations; this projection
    /// only advances the user-visible stage DAG and root state. A missing task is allowed for
    /// standalone Job compositions, while a present task is never replaced by a second task.
    async fn sync_control_job_task(
        &self,
        job: &JobRecord,
        issue: Option<TaskIssue>,
    ) -> CentralResult<()> {
        let Some(operation_task_id) = job.spec.operation_task_id.as_ref() else {
            return Ok(());
        };
        let Some(coordinator) = &self.task_coordinator else {
            return Ok(());
        };
        let Some(task) = coordinator
            .repository()
            .get(&job.spec.tenant_id, operation_task_id)
            .await?
        else {
            return Ok(());
        };

        // A SnapshotDelivery Job can also be used for a physical delete. That operation is
        // owned by a deletion saga and has a different five-stage plan, so leave it to the
        // ResourceLifecycleCoordinator rather than trying to force it into SnapshotCreate's DAG.
        let (stage_keys, root_state) = match job.operation {
            JobOperation::WorkspaceMaterialize => {
                let keys = match job.state {
                    JobState::Queued => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                    ],
                    JobState::Assigned | JobState::Accepted | JobState::Running => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("materialize", StageState::Running),
                    ],
                    JobState::Succeeded => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("materialize", StageState::Succeeded),
                        ("verify", StageState::Succeeded),
                        ("publish", StageState::Succeeded),
                    ],
                    JobState::RecoveryRequired => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("materialize", StageState::Stalled),
                    ],
                    JobState::Failed | JobState::Rejected | JobState::TimedOut => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("materialize", StageState::Failed),
                    ],
                    JobState::CancelRequested | JobState::Cancelled => Vec::new(),
                    JobState::Prepared | JobState::Publishing | JobState::Conflicted => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("materialize", StageState::Running),
                    ],
                    JobState::Unknown => Vec::new(),
                };
                (keys, control_job_root_state(job.state))
            }
            JobOperation::SnapshotDelivery
                if task.intent_kind == neoengram_domain::protocol::TaskIntent::SnapshotCreate =>
            {
                let keys = match job.state {
                    JobState::Queued => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                    ],
                    JobState::Assigned | JobState::Accepted | JobState::Running => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("delivery_materialize", StageState::Running),
                    ],
                    JobState::Succeeded => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("delivery_materialize", StageState::Succeeded),
                        ("verify", StageState::Succeeded),
                        ("publish", StageState::Succeeded),
                    ],
                    JobState::RecoveryRequired => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("delivery_materialize", StageState::Stalled),
                    ],
                    JobState::Failed | JobState::Rejected | JobState::TimedOut => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("delivery_materialize", StageState::Failed),
                    ],
                    JobState::CancelRequested | JobState::Cancelled => Vec::new(),
                    JobState::Prepared | JobState::Publishing | JobState::Conflicted => vec![
                        ("validate", StageState::Succeeded),
                        ("persist", StageState::Succeeded),
                        ("delivery_materialize", StageState::Running),
                    ],
                    JobState::Unknown => Vec::new(),
                };
                (keys, control_job_root_state(job.state))
            }
            _ => return Ok(()),
        };

        for (stage_key, desired) in stage_keys {
            drive_control_job_stage(
                coordinator,
                &job.spec.tenant_id,
                operation_task_id,
                stage_key,
                desired,
                issue.clone(),
            )
            .await?;
        }

        let current = coordinator
            .repository()
            .get(&job.spec.tenant_id, operation_task_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "operation task disappeared",
                )
            })?;

        if matches!(job.state, JobState::CancelRequested | JobState::Cancelled) {
            if current.state.is_terminal() {
                return Ok(());
            }
            if current.state != TaskState::Cancelling {
                coordinator
                    .transition(
                        operation_task_id,
                        &job.spec.tenant_id,
                        TaskState::Cancelling,
                        control_job_actor(),
                        Some("control Job cancellation converged".to_owned()),
                    )
                    .await?;
            }
            for stage in coordinator
                .repository()
                .stages(&job.spec.tenant_id, operation_task_id)
                .await?
            {
                if stage.state.is_success() || stage.state == StageState::Cancelled {
                    continue;
                }
                drive_control_job_stage(
                    coordinator,
                    &job.spec.tenant_id,
                    operation_task_id,
                    &stage.stage_key,
                    StageState::Cancelled,
                    None,
                )
                .await?;
            }
            let latest = coordinator
                .repository()
                .get(&job.spec.tenant_id, operation_task_id)
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ResourceNotFound,
                        "operation task disappeared",
                    )
                })?;
            if latest.state == TaskState::Cancelling {
                coordinator
                    .repository()
                    .complete_cancellation(
                        &job.spec.tenant_id,
                        operation_task_id,
                        latest.resource_version,
                        control_job_actor(),
                        self.clock.now(),
                    )
                    .await?;
            }
            return Ok(());
        }

        if current.state.is_terminal() {
            return Ok(());
        }
        let desired = root_state;
        if desired == TaskState::Succeeded && current.state == TaskState::Queued {
            coordinator
                .transition(
                    operation_task_id,
                    &job.spec.tenant_id,
                    TaskState::Running,
                    control_job_actor(),
                    Some("control Job completed".to_owned()),
                )
                .await?;
        }
        let latest = coordinator
            .repository()
            .get(&job.spec.tenant_id, operation_task_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "operation task disappeared",
                )
            })?;
        if latest.state != desired {
            coordinator
                .transition_with_issue(
                    operation_task_id,
                    &job.spec.tenant_id,
                    desired,
                    control_job_actor(),
                    issue,
                    Some(format!("control Job state: {:?}", job.state)),
                )
                .await?;
        }
        Ok(())
    }

    /// Mirrors the durable materialization aggregate into its unified operation task. The
    /// materialization repository remains authoritative for object/Batch facts; this helper only
    /// updates the coarse task state and latest progress summary used by operations screens.
    async fn sync_materialization_task(
        &self,
        job: &MaterializationJob,
        actor: TaskActor,
        issue: Option<TaskIssue>,
    ) -> CentralResult<()> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok(());
        };
        let repository = coordinator.repository();
        let Some(current) = repository
            .get(&job.key.tenant_id, &job.operation_task_id)
            .await?
        else {
            // Standalone/legacy materializations can predate the unified task row. They remain
            // queryable through the domain API and are deliberately not mutated here.
            tracing::debug!(
                materialization_id = %job.materialization_id,
                task_id = %job.operation_task_id,
                "materialization has no operation task to synchronize"
            );
            return Ok(());
        };
        self.sync_materialization_stages(job, issue.as_ref())
            .await?;
        let desired = Self::materialization_task_state(job.state);
        if current.state.is_terminal() && current.state != desired {
            // A cancelled/succeeded task is an explicit operator decision or completed history;
            // a late data-plane report must not resurrect or regress it.
            tracing::debug!(
                materialization_id = %job.materialization_id,
                task_id = %job.operation_task_id,
                task_state = ?current.state,
                materialization_state = ?job.state,
                "ignoring materialization state that would regress a terminal operation task"
            );
            return Ok(());
        }

        if desired == TaskState::Cancelled {
            // A cancelled materialization is only terminal after the root task has entered its
            // explicit convergence state. The Job's durable Cancelled state is the evidence that
            // Agent work and leases have drained, so it is safe to close the root now.
            // Re-read after stage projection: stage transitions update the root navigation
            // pointer under their own CAS and may therefore advance the task resource version.
            let latest = repository
                .get(&job.key.tenant_id, &job.operation_task_id)
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ResourceNotFound,
                        "operation task disappeared",
                    )
                })?;
            if latest.state.is_terminal() {
                return Ok(());
            }
            if latest.state != TaskState::Cancelling {
                match coordinator
                    .transition_with_issue(
                        &job.operation_task_id,
                        &job.key.tenant_id,
                        TaskState::Cancelling,
                        actor.clone(),
                        None,
                        Some("materialization cancellation converged".to_owned()),
                    )
                    .await
                {
                    Ok(updated) if updated.state.is_terminal() => return Ok(()),
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.code(),
                            CentralErrorCode::ConcurrentUpdate
                                | CentralErrorCode::ResourceNotFound
                                | CentralErrorCode::InvalidState
                        ) =>
                    {
                        return Ok(())
                    }
                    Err(error) => return Err(error),
                }
            }
            // The cancellation completion API owns the final fence. It verifies the current
            // task Attempt and every stage are already at the cancellation barrier, then updates
            // all three projections atomically. Never use a normal state transition here.
            let latest = repository
                .get(&job.key.tenant_id, &job.operation_task_id)
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::ResourceNotFound,
                        "operation task disappeared",
                    )
                })?;
            if latest.state == TaskState::Cancelling {
                match repository
                    .complete_cancellation(
                        &job.key.tenant_id,
                        &job.operation_task_id,
                        latest.resource_version,
                        actor,
                        self.clock.now(),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.code(),
                            CentralErrorCode::ConcurrentUpdate
                                | CentralErrorCode::ResourceNotFound
                                | CentralErrorCode::InvalidState
                        ) => {}
                    Err(error) => return Err(error),
                }
            }
            return Ok(());
        }
        let mut task_state = current.state;
        // The domain state machine intentionally requires an active state before success or
        // verification. A reconnected Agent may deliver the final receipt while the task still
        // says queued/stalled, so advance through Running before the terminal/verification state.
        let requires_running = (matches!(
            desired,
            TaskState::Succeeded | TaskState::Verifying | TaskState::Failed | TaskState::Stalled
        ) && matches!(
            task_state,
            TaskState::Queued | TaskState::Waiting | TaskState::Stalled
        )) || (desired == TaskState::Waiting
            && task_state == TaskState::Stalled);
        if requires_running {
            let resumed = match coordinator
                .transition_with_issue(
                    &job.operation_task_id,
                    &job.key.tenant_id,
                    TaskState::Running,
                    actor.clone(),
                    None,
                    Some("materialization resumed".to_owned()),
                )
                .await
            {
                Ok(updated) => updated,
                Err(error)
                    if matches!(
                        error.code(),
                        CentralErrorCode::ConcurrentUpdate
                            | CentralErrorCode::ResourceNotFound
                            | CentralErrorCode::InvalidState
                    ) =>
                {
                    return Ok(())
                }
                Err(error) => return Err(error),
            };
            task_state = resumed.state;
        }
        if task_state != desired || current.issue != issue {
            match coordinator
                .transition_with_issue(
                    &job.operation_task_id,
                    &job.key.tenant_id,
                    desired,
                    actor.clone(),
                    issue,
                    Some(format!("materialization state: {:?}", job.state)),
                )
                .await
            {
                Ok(updated) => task_state = updated.state,
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => return Ok(()),
                Err(error) if error.code() == CentralErrorCode::ResourceNotFound => return Ok(()),
                Err(error) if error.code() == CentralErrorCode::InvalidState => {
                    tracing::debug!(
                        materialization_id = %job.materialization_id,
                        task_id = %job.operation_task_id,
                        %error,
                        "operation task state changed before materialization mirror"
                    );
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        if task_state != TaskState::Cancelled {
            let progress = TaskProgressSummary::new(
                job.verified_object_count.get(),
                job.object_count.get(),
                job.verified_bytes.get(),
                job.total_bytes.get(),
            );
            match coordinator
                .update_progress(&job.operation_task_id, &job.key.tenant_id, progress)
                .await
            {
                Ok(_) => {}
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                Err(error) if error.code() == CentralErrorCode::ResourceNotFound => {}
                Err(error) if error.code() == CentralErrorCode::InvalidState => {
                    tracing::debug!(
                        materialization_id = %job.materialization_id,
                        task_id = %job.operation_task_id,
                        %error,
                        "operation task progress changed before materialization mirror"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Refreshes only the ephemeral transport generations of an active materialization Batch.
    ///
    /// Agent reconnects advance the session and route fences without changing the Volume owner
    /// or the selected object Placements. Reusing the durable Batch in that case preserves its
    /// staging checkpoint and avoids making a reconnect look like a permanently unavailable
    /// source. Owner/mount changes remain hard fences and are handled by the recovery path below.
    async fn refresh_materialization_batch_routes(
        &self,
        placement: &dyn PlacementRepository,
        batch: &MaterializationBatch,
    ) -> CentralResult<MaterializationBatch> {
        let Some(registry) = &self.gateway_registry else {
            return Ok(batch.clone());
        };
        let now = self.clock.now();
        let mut refreshed = batch.clone();
        for (agent_id, edge_cluster_id, gateway_pool_id, session, route) in [
            (
                &batch.target.agent_id,
                &batch.target.edge_cluster_id,
                &batch.target.gateway_pool_id,
                batch.target.session_generation,
                batch.target.route_generation,
            ),
            (
                &batch.source.agent_id,
                &batch.source.edge_cluster_id,
                &batch.source.gateway_pool_id,
                batch.source.session_generation,
                batch.source.route_generation,
            ),
        ] {
            let current = registry
                .get_agent_route(agent_id)
                .await?
                .filter(|candidate| candidate.is_active_at(now))
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::GatewayRouteUnavailable,
                        "materialization route is unavailable after Agent reconnect",
                    )
                })?;
            if current.edge_cluster_id != *edge_cluster_id
                || current.gateway_pool_id != *gateway_pool_id
                || current.session_generation < session
                || current.route_generation < route
            {
                return Err(invalid(
                    CentralErrorCode::GatewayRouteFenced,
                    "materialization route identity or generation moved backwards",
                ));
            }
            if agent_id == &batch.target.agent_id {
                refreshed.target.session_generation = current.session_generation;
                refreshed.target.route_generation = current.route_generation;
            } else {
                refreshed.source.session_generation = current.session_generation;
                refreshed.source.route_generation = current.route_generation;
            }
        }
        if refreshed == *batch {
            return Ok(refreshed);
        }
        placement
            .replace_materialization_batch(crate::MaterializationBatchCasRequest {
                tenant_id: batch.target.tenant_id.clone(),
                object_namespace_id: batch.target.object_namespace_id.clone(),
                materialization_id: batch.materialization_id.clone(),
                batch_id: batch.batch_id.clone(),
                expected_plan_revision: batch.plan_revision,
                expected_batch_attempt: batch.batch_attempt,
                batch: refreshed,
            })
            .await
    }

    /// Converts a delivery-time route/deadline failure into durable recovery state. This is
    /// deliberately idempotent and fenced: a newer plan or a terminal Batch wins the race and is
    /// left untouched. A caller can then invoke the normal retry planner with the next revision.
    async fn stall_materialization_batch(
        &self,
        batch: &MaterializationBatch,
        issue: &'static str,
    ) -> CentralResult<()> {
        let Some(placement) = &self.placement else {
            return Ok(());
        };
        let Some(job) = placement
            .get_materialization(
                &batch.target.tenant_id,
                &batch.target.object_namespace_id,
                &batch.materialization_id,
            )
            .await?
        else {
            return Ok(());
        };
        if job.plan_revision != batch.plan_revision {
            return Ok(());
        }
        let Some(current_batch) = placement
            .list_materialization_batches(
                &batch.target.tenant_id,
                &batch.target.object_namespace_id,
                &batch.materialization_id,
            )
            .await?
            .into_iter()
            .find(|candidate| candidate.batch_id == batch.batch_id)
        else {
            return Ok(());
        };
        if matches!(
            current_batch.state,
            MaterializationBatchState::Succeeded | MaterializationBatchState::Failed
        ) {
            return Ok(());
        }
        let mut failed_batch = current_batch.clone();
        failed_batch.state = MaterializationBatchState::Failed;
        match placement
            .replace_materialization_batch(crate::MaterializationBatchCasRequest {
                tenant_id: job.key.tenant_id.clone(),
                object_namespace_id: job.key.object_namespace_id.clone(),
                materialization_id: job.materialization_id.clone(),
                batch_id: current_batch.batch_id.clone(),
                expected_plan_revision: current_batch.plan_revision,
                expected_batch_attempt: current_batch.batch_attempt,
                batch: failed_batch,
            })
            .await
        {
            Ok(_) => {}
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => return Ok(()),
            Err(error) => return Err(error),
        }
        // Release protection only after the Batch CAS succeeds. If a retry won the race first,
        // the CAS above fails and cannot accidentally release the new plan's leases.
        release_materialization_batch_leases(placement.as_ref(), &job, &current_batch).await?;

        let Some(latest) = placement
            .get_materialization(
                &job.key.tenant_id,
                &job.key.object_namespace_id,
                &job.materialization_id,
            )
            .await?
        else {
            return Ok(());
        };
        if latest.plan_revision != job.plan_revision || latest.state.terminal() {
            return Ok(());
        }
        if !latest
            .state
            .can_transition_to(MaterializationJobState::Stalled)
        {
            return Ok(());
        }
        let mut stalled = latest.clone();
        stalled.state = MaterializationJobState::Stalled;
        stalled.issue = Some(issue.to_owned());
        stalled.updated_at_unix_ms = self.clock.now();
        match placement
            .replace_materialization(
                &latest.key.tenant_id,
                &latest.materialization_id,
                latest.plan_revision,
                stalled,
            )
            .await
        {
            Ok(updated) => {
                let task_issue = Some(TaskIssue {
                    code: issue.to_owned(),
                    message: format!("materialization batch stalled: {issue}"),
                    retryable: true,
                    detail: None,
                });
                self.sync_materialization_task(
                    &updated,
                    TaskActor::Agent {
                        agent_id: batch.target.agent_id.clone(),
                    },
                    task_issue,
                )
                .await
            }
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn current_replication_route_generations(
        &self,
        agent_id: &AgentId,
        expected_edge_cluster_id: &neoengram_domain::protocol::EdgeClusterId,
        expected_gateway_pool_id: &neoengram_domain::protocol::GatewayPoolId,
        expected_session_generation: SessionGeneration,
        expected_mount_generation: MountGeneration,
        expected_route_generation: RouteGeneration,
    ) -> CentralResult<ReplicationRouteGenerations> {
        let Some(registry) = &self.gateway_registry else {
            // Focused Job-only adapters do not have a Gateway registry. Their replication tests
            // still use the immutable route snapshot stored on the record.
            return Ok(ReplicationRouteGenerations {
                session: expected_session_generation,
                mount: expected_mount_generation,
                route: expected_route_generation,
            });
        };
        let route = registry
            .get_agent_route(agent_id)
            .await?
            .filter(|route| route.is_active_at(self.clock.now()))
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::GatewayRouteUnavailable,
                    "replication Agent route is temporarily unavailable",
                )
            })?;
        // A reconnect may advance session/route generations, but it must not silently move a
        // transfer to a different Agent, EdgeCluster, or GatewayPool.
        if route.agent_id != *agent_id
            || route.edge_cluster_id != *expected_edge_cluster_id
            || route.gateway_pool_id != *expected_gateway_pool_id
        {
            return Err(invalid(
                CentralErrorCode::GatewayRouteUnavailable,
                "replication Agent route no longer matches its frozen placement scope",
            ));
        }
        let mount = if let Some(agent_registry) = &self.agent_registry {
            let record = agent_registry
                .get_by_agent(agent_id)
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::GatewayRouteUnavailable,
                        "replication Agent enrollment is temporarily unavailable",
                    )
                })?;
            if record.mount.mount_generation != expected_mount_generation
                || record.enrollment.edge_cluster_id != *expected_edge_cluster_id
                || record.owner.active_agent_id.as_ref() != Some(agent_id)
                || record.owner.active_agent_mount_id.as_ref() != Some(&record.mount.agent_mount_id)
            {
                return Err(invalid(
                    CentralErrorCode::GatewayRouteUnavailable,
                    "replication Agent owner or mount generation changed",
                ));
            }
            record.mount.mount_generation
        } else {
            expected_mount_generation
        };
        Ok(ReplicationRouteGenerations {
            session: route.session_generation,
            mount,
            route: route.route_generation,
        })
    }

    /// Derives the current Agent delivery set from the durable assignment outbox and Job CAS.
    /// Add assignments disappear after Accepted because their execution ledger is Agent-durable.
    /// Workspace materialization assignments remain deliverable through Accepted/Running and
    /// disappear only at a terminal outcome, allowing an Agent restart to resume physical checkout.
    /// Decisions disappear only after the matching Finalized acknowledgement is persisted.
    pub async fn deliverable_agent_messages(
        &self,
        agent_id: &AgentId,
        session_generation: SessionGeneration,
        limit: usize,
    ) -> CentralResult<Vec<Envelope<ControlMessage>>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut messages = Vec::with_capacity(limit);
        for assignment in self.outbox.pending_for_agent(agent_id, limit).await? {
            let (tenant_id, job_id, assignment_id, valid) = match &assignment.assignment {
                AssignmentOperation::Add { input, .. } => {
                    let job = self
                        .jobs
                        .get(&crate::JobKey::new(
                            input.tenant_id.clone(),
                            input.job_id.clone(),
                        ))
                        .await?;
                    (
                        input.tenant_id.clone(),
                        input.job_id.clone(),
                        input.assignment_id.clone(),
                        job.is_some_and(|job| {
                            job.operation == JobOperation::Add
                                && job.state == JobState::Assigned
                                && job.assignment.as_ref() == Some(input)
                        }),
                    )
                }
                AssignmentOperation::WorkspaceMaterialize { input, .. } => {
                    let job = self
                        .jobs
                        .get(&crate::JobKey::new(
                            input.tenant_id.clone(),
                            input.job_id.clone(),
                        ))
                        .await?;
                    (
                        input.tenant_id.clone(),
                        input.job_id.clone(),
                        input.assignment_id.clone(),
                        job.is_some_and(|job| {
                            job.operation == JobOperation::WorkspaceMaterialize
                                && matches!(
                                    job.state,
                                    JobState::Assigned
                                        | JobState::Accepted
                                        | JobState::Running
                                        | JobState::Succeeded
                                        | JobState::RecoveryRequired
                                )
                                && job.workspace_assignment.as_ref() == Some(input)
                        }),
                    )
                }
                AssignmentOperation::SnapshotDelivery { input, .. } => {
                    let job = self
                        .jobs
                        .get(&crate::JobKey::new(
                            input.tenant_id.clone(),
                            input.job_id.clone(),
                        ))
                        .await?;
                    (
                        input.tenant_id.clone(),
                        input.job_id.clone(),
                        input.assignment_id.clone(),
                        job.is_some_and(|job| {
                            job.operation == JobOperation::SnapshotDelivery
                                && matches!(
                                    job.state,
                                    JobState::Assigned
                                        | JobState::Accepted
                                        | JobState::Running
                                        | JobState::Succeeded
                                        | JobState::RecoveryRequired
                                )
                                && job.delivery_assignment.as_ref() == Some(input)
                        }),
                    )
                }
            };
            if !valid {
                continue;
            }
            self.jobs
                .get(&crate::JobKey::new(tenant_id.clone(), job_id.clone()))
                .await?
                .ok_or_else(|| {
                    invalid(CentralErrorCode::JobNotFound, "assignment Job disappeared")
                })?;
            let envelope = action_envelope(
                AGENT_JOB_ASSIGNMENT_ACTION,
                MessageId::new(format!("assignment-{assignment_id}"))?,
                tenant_id,
                session_generation,
                assignment_deadline(&assignment),
                ControlMessage::Assignment(Box::new(assignment)),
            )?;
            messages.push(envelope);
            if messages.len() == limit {
                return Ok(messages);
            }
        }

        let remaining = limit.saturating_sub(messages.len());
        for job in self
            .jobs
            .list_pending_decisions_for_agent(agent_id, remaining)
            .await?
        {
            let Some(assignment) = &job.assignment else {
                continue;
            };
            let Some(decision) = &job.decision else {
                continue;
            };
            if assignment.agent_id != *agent_id || job.finalized_ack.is_some() {
                continue;
            }
            let envelope = action_envelope(
                AGENT_JOB_DECISION_ACTION,
                MessageId::new(format!(
                    "decision-{}-{}",
                    decision.job_id, decision.decision_generation
                ))?,
                assignment.tenant_id.clone(),
                session_generation,
                UnixMillis::new(self.clock.now().get().max(1)),
                ControlMessage::Decision(decision.clone()),
            )?;
            messages.push(envelope);
            if messages.len() == limit {
                break;
            }
        }
        if messages.len() == limit {
            return Ok(messages);
        }
        if let Some(catalog) = &self.catalog {
            for record in catalog
                .pending_lifecycle_assignments_for_agent(
                    agent_id,
                    limit.saturating_sub(messages.len()),
                )
                .await?
            {
                let assignment = &record.assignment;
                if assignment.session_generation != session_generation {
                    continue;
                }
                catalog
                    .get_deletion_operation(
                        &assignment.assignment.tenant_id,
                        &assignment.assignment.deletion_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::Internal,
                            "lifecycle outbox references a missing deletion operation",
                        )
                    })?;
                let envelope = action_envelope(
                    AGENT_LIFECYCLE_ASSIGNMENT_ACTION,
                    MessageId::new(format!("lifecycle-{}", assignment.assignment.assignment_id))?,
                    assignment.assignment.tenant_id.clone(),
                    session_generation,
                    assignment.assignment.deadline_unix_ms,
                    ControlMessage::LifecycleAssignment(Box::new(assignment.clone())),
                )?;
                messages.push(envelope);
                if messages.len() == limit {
                    break;
                }
            }
        }

        // v2 materialization commands are derived from namespace-scoped object authority. They
        // remain deliverable while a Batch is active so a reconnect can resume the same stable
        // staging keys and offsets. Legacy whole-Commit ReplicationRecords are intentionally not
        // delivered here; they remain readable only to support explicit reset/inventory tooling.
        if messages.len() < limit {
            if let Some(placement) = &self.placement {
                let tenant_id = if let Some(registry) = &self.agent_registry {
                    let Some(record) = registry.get_by_agent(agent_id).await? else {
                        return Ok(messages);
                    };
                    record.enrollment.tenant_id
                } else {
                    return Ok(messages);
                };
                for batch in placement
                    .list_active_materialization_batches_for_agent(&tenant_id, agent_id)
                    .await?
                {
                    if messages.len() == limit
                        || matches!(
                            batch.state,
                            MaterializationBatchState::Succeeded
                                | MaterializationBatchState::Failed
                        )
                    {
                        continue;
                    }
                    let assignment = match self
                        .materialization_assignment(&batch, session_generation)
                        .await
                    {
                        Ok(assignment) => assignment,
                        Err(error)
                            if matches!(
                                error.code(),
                                CentralErrorCode::GatewayRouteUnavailable
                                    | CentralErrorCode::GatewayRouteFenced
                                    | CentralErrorCode::DeadlineExceeded
                            ) =>
                        {
                            let issue = match error.code() {
                                CentralErrorCode::DeadlineExceeded => {
                                    "MATERIALIZATION_DEADLINE_EXPIRED"
                                }
                                _ => "MATERIALIZATION_ROUTE_UNAVAILABLE",
                            };
                            if let Err(recovery_error) =
                                self.stall_materialization_batch(&batch, issue).await
                            {
                                tracing::warn!(
                                    agent_id = %agent_id,
                                    batch_id = %batch.batch_id,
                                    %recovery_error,
                                    "could not persist materialization recovery state"
                                );
                            }
                            tracing::debug!(
                                agent_id = %agent_id,
                                batch_id = %batch.batch_id,
                                code = error.stable_code(),
                                "materialization batch requires route/deadline recovery"
                            );
                            continue;
                        }
                        Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                            tracing::debug!(
                                agent_id = %agent_id,
                                batch_id = %batch.batch_id,
                                code = error.stable_code(),
                                "materialization batch changed while preparing delivery"
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let Some(assignment) = assignment else {
                        continue;
                    };
                    let deadline = assignment.signed_ticket.ticket.deadline_unix_ms;
                    let envelope = action_envelope(
                        AGENT_MATERIALIZATION_ASSIGNMENT_ACTION,
                        materialization_assignment_message_id(
                            &assignment.batch.materialization_id,
                            &assignment.batch.batch_id,
                            assignment.batch.plan_revision,
                            assignment.batch.batch_attempt,
                        )?,
                        assignment.batch.target.tenant_id.clone(),
                        session_generation,
                        deadline,
                        ControlMessage::MaterializationAssignment(Box::new(assignment)),
                    )?;
                    messages.push(envelope);
                }
            }
        }
        Ok(messages)
    }

    /// Builds one Central-signed object-level materialization assignment from the durable Batch.
    /// Manifest pages are reconstructed from the namespace-scoped object tasks; they are not
    /// accepted from an Agent and therefore cannot drift from the authority's plan revision.
    async fn materialization_assignment(
        &self,
        batch: &MaterializationBatch,
        session_generation: SessionGeneration,
    ) -> CentralResult<Option<neoengram_domain::protocol::MaterializationAssignment>> {
        let Some(placement) = &self.placement else {
            return Ok(None);
        };
        let Some(keyring) = &self.replication_ticket_keyring else {
            return Ok(None);
        };
        let Some(job) = placement
            .get_materialization(
                &batch.target.tenant_id,
                &batch.target.object_namespace_id,
                &batch.materialization_id,
            )
            .await?
        else {
            return Err(invalid(
                CentralErrorCode::ResourceNotFound,
                "materialization Batch references a missing Job",
            ));
        };
        if job.plan_revision != batch.plan_revision
            || job.key.object_namespace_id != batch.target.object_namespace_id
            || job.key.target_storage_volume_id != batch.target.storage_volume_id
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Batch is stale relative to its Job",
            ));
        }
        // Session/route generations are transport fences, not object identity. Refresh them
        // after a reconnect while retaining the same Batch attempt and staging checkpoint.
        let batch = self
            .refresh_materialization_batch_routes(placement.as_ref(), batch)
            .await?;
        if batch.target.session_generation != session_generation {
            return Ok(None);
        }
        // Route generations are part of the signed Batch fence. Recheck both hops at delivery
        // time so a source or target reconnect cannot receive a ticket for an obsolete route.
        if let Some(gateway_registry) = &self.gateway_registry {
            let now = self.clock.now();
            validate_materialization_route_fence(
                gateway_registry.as_ref(),
                &batch.target.agent_id,
                &batch.target.edge_cluster_id,
                &batch.target.gateway_pool_id,
                batch.target.session_generation,
                batch.target.route_generation,
                now,
            )
            .await?;
            validate_materialization_route_fence(
                gateway_registry.as_ref(),
                &batch.source.agent_id,
                &batch.source.edge_cluster_id,
                &batch.source.gateway_pool_id,
                batch.source.session_generation,
                batch.source.route_generation,
                now,
            )
            .await?;
        }
        if let Some(agent_registry) = &self.agent_registry {
            validate_materialization_owner_fence(
                agent_registry.as_ref(),
                &batch.target.agent_id,
                &batch.target.storage_volume_id,
                batch.target.placement_generation,
            )
            .await?;
            if let Some(source_volume_id) = &batch.source.storage_volume_id {
                validate_materialization_owner_fence(
                    agent_registry.as_ref(),
                    &batch.source.agent_id,
                    source_volume_id,
                    batch.source.placement_generation,
                )
                .await?;
            }
        }
        let mut batch = batch;
        // Claim a queued Batch before exposing it on the reverse channel. This gives receipt
        // handling a monotonic state path (Assigned -> Transferring -> Verifying -> Succeeded)
        // while preserving the durable Batch identity across reconnects.
        if batch.state == MaterializationBatchState::Queued {
            let mut claimed = batch.clone();
            claimed.state = MaterializationBatchState::Assigned;
            batch = placement
                .replace_materialization_batch(crate::MaterializationBatchCasRequest {
                    tenant_id: batch.target.tenant_id.clone(),
                    object_namespace_id: batch.target.object_namespace_id.clone(),
                    materialization_id: batch.materialization_id.clone(),
                    batch_id: batch.batch_id.clone(),
                    expected_plan_revision: batch.plan_revision,
                    expected_batch_attempt: batch.batch_attempt,
                    batch: claimed,
                })
                .await?;
        }
        let mut tasks = placement
            .list_materialization_objects(
                &batch.target.tenant_id,
                &batch.target.object_namespace_id,
                &batch.materialization_id,
            )
            .await?
            .into_iter()
            .filter(|task| {
                task.plan_revision == batch.plan_revision
                    && task.object.object_namespace_id == batch.target.object_namespace_id
                    && batch.object_ids.contains(&task.object.object_id)
            })
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| task.object.ordinal);
        if tasks.len() != batch.object_ids.len() {
            return Err(invalid(
                CentralErrorCode::BatchIncomplete,
                "materialization Batch does not have all namespace object tasks",
            ));
        }
        let objects = tasks
            .iter()
            .map(|task| task.object.clone())
            .collect::<Vec<_>>();
        let source_bindings = tasks
            .iter()
            .map(|task| {
                let placement_id = task.primary_source.clone().ok_or_else(|| {
                    invalid(
                        CentralErrorCode::BatchIncomplete,
                        "materialization object task has no selected source Placement",
                    )
                })?;
                Ok(neoengram_domain::protocol::MaterializationManifestSource {
                    object_id: task.object.object_id,
                    placement_id,
                    placement_generation: batch.source.placement_generation,
                })
            })
            .collect::<CentralResult<Vec<_>>>()?;
        let (manifest, pages) = BatchManifest::paginate_with_sources(
            batch.materialization_id.clone(),
            batch.batch_id.clone(),
            batch.plan_revision,
            batch.batch_attempt,
            batch.target.object_namespace_id.clone(),
            objects,
            source_bindings,
            4096,
        )
        .map_err(CentralError::from)?;
        if manifest.manifest_digest != batch.manifest_digest {
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                "materialization Batch manifest digest differs from its object tasks",
            ));
        }
        let now = self.clock.now();
        if batch.deadline_unix_ms.get() <= now.get() {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "materialization Batch deadline has elapsed",
            ));
        }
        let ticket_digest = blake3::hash(
            format!(
                "materialization-ticket\0{}\0{}\0{}\0{}",
                batch.materialization_id, batch.batch_id, batch.plan_revision, batch.batch_attempt
            )
            .as_bytes(),
        );
        let ticket_id = ObjectTicketId::new(format!("ticket-{}", &ticket_digest.to_hex()[..32]))
            .map_err(CentralError::from)?;
        // A materialization Job/Batch may live for a day, while a Central command signature is
        // deliberately capped at the short command TTL.  The signed Ticket deadline must be the
        // exact expiry produced by the signer; otherwise strict Ticket validation rejects every
        // assignment before it can reach the Agent.  Keep the long Batch deadline for scheduling
        // and use the bounded ticket deadline only for this delivery capability.
        let (ticket_deadline, ticket_ttl_ms) =
            materialization_ticket_window(now, batch.deadline_unix_ms)?;
        let ticket = MaterializationBatchTicket {
            ticket_id,
            operation_task_id: job.operation_task_id.clone(),
            task_attempt_id: job.task_attempt_id.clone(),
            task_attempt: task_attempt_generation(&job.task_attempt_id),
            stage_key: "transfer".to_owned(),
            stage_attempt: batch.batch_attempt,
            materialization_id: batch.materialization_id.clone(),
            batch_id: batch.batch_id.clone(),
            plan_revision: batch.plan_revision,
            batch_attempt: batch.batch_attempt,
            tenant_id: batch.target.tenant_id.clone(),
            artifact_id: job.artifact_id,
            object_namespace_id: batch.target.object_namespace_id.clone(),
            commit_id: job.key.commit_id,
            manifest_digest: batch.manifest_digest,
            source: batch.source.clone(),
            target: batch.target.clone(),
            max_bytes: batch.max_bytes,
            deadline_unix_ms: ticket_deadline,
            capability:
                neoengram_domain::protocol::materialization::COMMIT_MATERIALIZATION_CAPABILITY_V2
                    .to_owned(),
        };
        let signed_ticket = keyring
            .sign_materialization_batch_ticket(ticket, now, ticket_ttl_ms)
            .await
            .map_err(|error| invalid(CentralErrorCode::Internal, error.to_string()))?;
        let assignment = neoengram_domain::protocol::MaterializationAssignment {
            operation_task_id: job.operation_task_id.clone(),
            task_attempt_id: job.task_attempt_id.clone(),
            task_attempt: task_attempt_generation(&job.task_attempt_id),
            stage_key: "transfer".to_owned(),
            stage_attempt: batch.batch_attempt,
            signed_ticket,
            batch: batch.clone(),
            manifest,
            pages,
            extensions: Extensions::new(),
        };
        assignment.validate()?;
        Ok(Some(assignment))
    }

    /// Applies one authenticated object-level materialization report. Receipt publication is
    /// delegated to the PlacementRepository's idempotent durability barrier; failure reports
    /// fence the matching Batch/Job without ever creating a Placement.
    pub async fn receive_materialization_report(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        agent_id: &AgentId,
        session_generation: SessionGeneration,
        report: MaterializationReport,
    ) -> CentralResult<MaterializationReportResult> {
        report.validate()?;
        if report.tenant_id() != tenant_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "materialization report tenant differs from its authenticated Agent session",
            ));
        }
        let placement = self.placement.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::StorageFailure,
                "materialization Placement authority is unavailable",
            )
        })?;
        let materialization_id = report.materialization_id().clone();
        let job = placement
            .get_materialization(tenant_id, report.object_namespace_id(), &materialization_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization report references an unknown Job",
                )
            })?;
        if job.operation_task_id != *report.operation_task_id()
            || job.task_attempt_id != *report.task_attempt_id()
        {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "materialization report task identity differs from the owning operation task",
            ));
        }
        if job.key.object_namespace_id != *report.object_namespace_id() {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization report namespace or plan revision is stale",
            ));
        }
        let batch = placement
            .list_materialization_batches(
                tenant_id,
                report.object_namespace_id(),
                &materialization_id,
            )
            .await?
            .into_iter()
            .find(|batch| batch.batch_id == *report.batch_id())
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "materialization report references an unknown Batch",
                )
            })?;
        if batch.target.agent_id != *agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "materialization report is not bound to the target Agent",
            ));
        }
        // Every v2 report carries the exact target fence copied from its signed assignment.
        // Compare it with the durable Batch before consulting live route state so a delayed
        // report cannot be retargeted merely because the Agent/Gateway has since reconnected.
        if report.target() != &batch.target {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "materialization report target fence differs from the durable Batch",
            ));
        }
        // Reports must match every identity fence copied from the assignment. In particular,
        // `task_attempt_id` alone is insufficient: an implementation could forge an ID with the
        // same textual suffix while targeting an older stage execution. The transfer stage is the
        // only data-plane stage that emits object reports, and its stage attempt is the Batch
        // attempt that was signed into the ticket.
        if report.task_attempt() != task_attempt_generation(&job.task_attempt_id)
            || report.stage_key() != "transfer"
            || report.stage_attempt() != batch.batch_attempt
            || batch.plan_revision != report.plan_revision()
            || batch.batch_attempt != report.batch_attempt()
            || batch.target.object_namespace_id != *report.object_namespace_id()
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization report does not match the active task/stage Batch fence",
            ));
        }

        // A receipt is the durable idempotency point. Probe its exact identity before checking
        // live session, route, and owner generations: an Agent can reconnect after the original
        // publication succeeded, while the queued report still carries the old signed target
        // fence. Unknown receipts do not get this exception and continue through all live fences
        // below, so an authenticated Agent cannot use the replay path to submit a never-committed
        // stale report.
        if let MaterializationReport::Receipt { receipt, .. } = &report {
            if let Some(existing) = placement
                .get_materialization_receipt(
                    tenant_id,
                    &receipt.object_namespace_id,
                    &receipt.receipt_id,
                )
                .await?
            {
                if existing != receipt.clone() {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "materialization receipt ID is already in use",
                    ));
                }
                let object = placement
                    .list_materialization_objects(
                        tenant_id,
                        &receipt.object_namespace_id,
                        &materialization_id,
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
                            "materialization receipt object task is missing",
                        )
                    })?;
                placement
                    .record_materialization_receipt(crate::MaterializationReceiptRequest {
                        receipt: receipt.clone(),
                        object: object.object,
                    })
                    .await?;
                return Ok(MaterializationReportResult {
                    resource_version: ResourceVersion::new(1),
                    replayed: true,
                });
            }
        }

        if batch.target.session_generation != session_generation {
            return Err(invalid(
                CentralErrorCode::GenerationMismatch,
                "materialization report carries a stale target session generation",
            ));
        }

        // If route authority is installed, bind the report to the current Agent/Gateway route
        // and mount generation as well. Focused in-memory Job tests intentionally omit this
        // registry and rely on the signed Batch route snapshot.
        if let Some(gateway_registry) = &self.gateway_registry {
            let route = gateway_registry
                .get_agent_route(agent_id)
                .await?
                .filter(|route| route.is_active_at(self.clock.now()))
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::GatewayRouteUnavailable,
                        "materialization target route is unavailable",
                    )
                })?;
            if route.session_generation != session_generation
                || route.edge_cluster_id != batch.target.edge_cluster_id
                || route.gateway_pool_id != batch.target.gateway_pool_id
                || route.route_generation != batch.target.route_generation
            {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "materialization report is not bound to the current Gateway route",
                ));
            }
        }
        if let Some(agent_registry) = &self.agent_registry {
            let record = agent_registry
                .get_by_agent(agent_id)
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::GatewayRouteUnavailable,
                        "materialization target Agent enrollment is unavailable",
                    )
                })?;
            if record.mount.storage_volume_id != batch.target.storage_volume_id
                || record.mount.mount_generation != batch.target.mount_generation
                || record.owner.storage_volume_id != batch.target.storage_volume_id
                || record.owner.active_agent_id.as_ref() != Some(agent_id)
                || record.owner.active_agent_mount_id.as_ref() != Some(&record.mount.agent_mount_id)
                || record.owner.owner_generation.get() != batch.target.placement_generation.get()
            {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "materialization report mount or owner generation is stale",
                ));
            }
        }

        // A receipt is a durable idempotency point.  Replanning advances the live Job and
        // rewrites the object task, but it intentionally keeps the old Batch/receipt evidence so
        // an Agent retry can be acknowledged after the response was lost.  Let the repository
        // perform its durable receipt/Placement replay lookup before applying the live Job plan
        // fence.  Non-receipt reports never get this exception and remain fenced to the active
        // plan revision.
        if job.plan_revision != report.plan_revision() {
            if let MaterializationReport::Receipt { receipt, .. } = &report {
                let object = placement
                    .list_materialization_objects(
                        tenant_id,
                        &receipt.object_namespace_id,
                        &materialization_id,
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
                            "materialization receipt object task is missing",
                        )
                    })?;
                placement
                    .record_materialization_receipt(crate::MaterializationReceiptRequest {
                        receipt: receipt.clone(),
                        object: object.object,
                    })
                    .await?;
                return Ok(MaterializationReportResult {
                    resource_version: ResourceVersion::new(1),
                    replayed: true,
                });
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "materialization report namespace or plan revision is stale",
            ));
        }

        match report {
            MaterializationReport::Receipt { receipt, .. } => {
                let task = placement
                    .list_materialization_objects(
                        tenant_id,
                        &receipt.object_namespace_id,
                        &materialization_id,
                    )
                    .await?
                    .into_iter()
                    .find(|task| {
                        task.object.object_namespace_id == receipt.object_namespace_id
                            && task.object.object_id == receipt.object_id
                            && task.current_batch_id.as_ref() == Some(&receipt.batch_id)
                    })
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::ResourceNotFound,
                            "materialization receipt object task is missing",
                        )
                    })?;
                let replayed = task.complete();
                let receipt_object_id = receipt.object_id;
                placement
                    .record_materialization_receipt(crate::MaterializationReceiptRequest {
                        receipt,
                        object: task.object.clone(),
                    })
                    .await?;
                // The receipt has crossed the target durability barrier. Release only this
                // object's source/staging roots; a different object in the same Batch may still
                // be transferring and must remain protected.
                release_materialization_object_leases(
                    placement.as_ref(),
                    &job,
                    &batch,
                    std::slice::from_ref(&task),
                    receipt_object_id,
                )
                .await?;
                // A receipt is the object durability point. Once every object in the Batch has a
                // durable receipt, advance the Batch through the explicit verification states so
                // it is removed from reconnect delivery. Each step is fenced by the same plan
                // revision and attempt; a concurrent planner/report will make the caller retry.
                let current_batch = placement
                    .list_materialization_batches(
                        tenant_id,
                        &batch.target.object_namespace_id,
                        &materialization_id,
                    )
                    .await?
                    .into_iter()
                    .find(|candidate| candidate.batch_id == batch.batch_id)
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::ResourceNotFound,
                            "materialization Batch disappeared after receipt",
                        )
                    })?;
                let tasks = placement
                    .list_materialization_objects(
                        tenant_id,
                        &batch.target.object_namespace_id,
                        &materialization_id,
                    )
                    .await?;
                let complete = current_batch.object_ids.iter().all(|object_id| {
                    tasks
                        .iter()
                        .any(|task| task.object.object_id == *object_id && task.complete())
                });
                if complete && current_batch.state != MaterializationBatchState::Succeeded {
                    let mut state = current_batch.state;
                    for next_state in [
                        MaterializationBatchState::Transferring,
                        MaterializationBatchState::Verifying,
                        MaterializationBatchState::Succeeded,
                    ] {
                        if state == MaterializationBatchState::Succeeded {
                            break;
                        }
                        if state == next_state {
                            continue;
                        }
                        // Queued is only possible for a report racing the first delivery claim.
                        // Move it through Assigned before the normal transfer states.
                        if state == MaterializationBatchState::Queued {
                            let mut assigned = current_batch.clone();
                            assigned.state = MaterializationBatchState::Assigned;
                            placement
                                .replace_materialization_batch(
                                    crate::MaterializationBatchCasRequest {
                                        tenant_id: tenant_id.clone(),
                                        object_namespace_id: batch
                                            .target
                                            .object_namespace_id
                                            .clone(),
                                        materialization_id: materialization_id.clone(),
                                        batch_id: batch.batch_id.clone(),
                                        expected_plan_revision: current_batch.plan_revision,
                                        expected_batch_attempt: current_batch.batch_attempt,
                                        batch: assigned,
                                    },
                                )
                                .await?;
                            state = MaterializationBatchState::Assigned;
                        }
                        if !state.can_transition_to(next_state) {
                            continue;
                        }
                        let mut next = placement
                            .list_materialization_batches(
                                tenant_id,
                                &batch.target.object_namespace_id,
                                &materialization_id,
                            )
                            .await?
                            .into_iter()
                            .find(|candidate| candidate.batch_id == batch.batch_id)
                            .ok_or_else(|| {
                                invalid(
                                    CentralErrorCode::ResourceNotFound,
                                    "materialization Batch disappeared while completing",
                                )
                            })?;
                        next.state = next_state;
                        placement
                            .replace_materialization_batch(crate::MaterializationBatchCasRequest {
                                tenant_id: tenant_id.clone(),
                                object_namespace_id: batch.target.object_namespace_id.clone(),
                                materialization_id: materialization_id.clone(),
                                batch_id: batch.batch_id.clone(),
                                expected_plan_revision: next.plan_revision,
                                expected_batch_attempt: next.batch_attempt,
                                batch: next.clone(),
                            })
                            .await?;
                        state = next_state;
                    }
                }
                // The Placement authority updates the Job counters/state as part of the receipt
                // durability barrier. Mirror that latest aggregate into the operation task only
                // after the receipt and Batch CAS have succeeded, so task progress never gets
                // ahead of durable object evidence.
                if let Some(latest_job) = placement
                    .get_materialization(
                        tenant_id,
                        &batch.target.object_namespace_id,
                        &materialization_id,
                    )
                    .await?
                {
                    self.sync_materialization_task(
                        &latest_job,
                        TaskActor::Agent {
                            agent_id: agent_id.clone(),
                        },
                        None,
                    )
                    .await?;
                }
                Ok(MaterializationReportResult {
                    resource_version: ResourceVersion::new(1),
                    replayed,
                })
            }
            MaterializationReport::Failed {
                object_id,
                object_namespace_id,
                issue_code,
                issue_message,
                ..
            } => {
                let mut replayed = false;
                let issue = format!("{issue_code}: {issue_message}");
                let task_issue = Some(TaskIssue {
                    code: task_text(&issue_code),
                    message: task_text(&issue_message),
                    retryable: true,
                    detail: None,
                });
                // Check the Batch terminal fence before mutating an object task. A second
                // failure report for an already-failed Batch must be an exact replay or a hard
                // conflict; updating `last_error` first would leave a partially applied state
                // even though the report is rejected below.
                let initial_batch = placement
                    .list_materialization_batches(
                        tenant_id,
                        &batch.target.object_namespace_id,
                        &materialization_id,
                    )
                    .await?
                    .into_iter()
                    .find(|candidate| candidate.batch_id == batch.batch_id)
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::ResourceNotFound,
                            "materialization Batch disappeared before applying failure",
                        )
                    })?;
                if initial_batch.state == MaterializationBatchState::Succeeded {
                    return Err(invalid(
                        CentralErrorCode::ConcurrentUpdate,
                        "materialization failure report arrived after Batch success",
                    ));
                }
                if initial_batch.state == MaterializationBatchState::Failed {
                    let existing_job = placement
                        .get_materialization(
                            tenant_id,
                            &batch.target.object_namespace_id,
                            &materialization_id,
                        )
                        .await?
                        .ok_or_else(|| {
                            invalid(
                                CentralErrorCode::ResourceNotFound,
                                "materialization Job disappeared while checking failure replay",
                            )
                        })?;
                    if existing_job.issue.as_deref() == Some(issue.as_str()) {
                        return Ok(MaterializationReportResult {
                            resource_version: ResourceVersion::new(1),
                            replayed: true,
                        });
                    }
                    return Err(invalid(
                        CentralErrorCode::ConcurrentUpdate,
                        "materialization Batch already has a different failure report",
                    ));
                }
                if let Some(object_id) = object_id {
                    let task = placement
                        .list_materialization_objects(
                            tenant_id,
                            &object_namespace_id,
                            &materialization_id,
                        )
                        .await?
                        .into_iter()
                        .find(|task| {
                            task.object.object_namespace_id == object_namespace_id
                                && task.object.object_id == object_id
                                && task.current_batch_id.as_ref() == Some(&batch.batch_id)
                        })
                        .ok_or_else(|| {
                            invalid(
                                CentralErrorCode::ResourceNotFound,
                                "materialization failure object task is missing",
                            )
                        })?;
                    if task.state == MaterializationObjectState::Failed
                        && task.last_error.as_deref() == Some(issue.as_str())
                    {
                        replayed = true;
                    } else {
                        let mut next = task.clone();
                        next.state = MaterializationObjectState::Failed;
                        next.last_error = Some(issue.clone());
                        placement
                            .replace_materialization_object(
                                crate::MaterializationObjectCasRequest {
                                    tenant_id: tenant_id.clone(),
                                    object_namespace_id: object_namespace_id.clone(),
                                    materialization_id: materialization_id.clone(),
                                    object_id,
                                    expected_plan_revision: batch.plan_revision,
                                    expected_attempt: task.attempt,
                                    object: next,
                                },
                            )
                            .await?;
                    }
                }
                let current_batch = placement
                    .list_materialization_batches(
                        tenant_id,
                        &batch.target.object_namespace_id,
                        &materialization_id,
                    )
                    .await?
                    .into_iter()
                    .find(|candidate| candidate.batch_id == batch.batch_id)
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::ResourceNotFound,
                            "materialization Batch disappeared while applying failure",
                        )
                    })?;
                if current_batch.state == MaterializationBatchState::Failed {
                    let existing_job = placement
                        .get_materialization(
                            tenant_id,
                            &batch.target.object_namespace_id,
                            &materialization_id,
                        )
                        .await?
                        .ok_or_else(|| {
                            invalid(
                                CentralErrorCode::ResourceNotFound,
                                "materialization Job disappeared while checking failure replay",
                            )
                        })?;
                    if existing_job.issue.as_deref() == Some(issue.as_str()) {
                        replayed = true;
                    } else {
                        return Err(invalid(
                            CentralErrorCode::ConcurrentUpdate,
                            "materialization Batch already has a different failure report",
                        ));
                    }
                } else if current_batch.state != MaterializationBatchState::Succeeded {
                    release_materialization_batch_leases(placement.as_ref(), &job, &current_batch)
                        .await?;
                    let mut next = current_batch.clone();
                    next.state = MaterializationBatchState::Failed;
                    placement
                        .replace_materialization_batch(crate::MaterializationBatchCasRequest {
                            tenant_id: tenant_id.clone(),
                            object_namespace_id,
                            materialization_id: materialization_id.clone(),
                            batch_id: current_batch.batch_id.clone(),
                            expected_plan_revision: current_batch.plan_revision,
                            expected_batch_attempt: current_batch.batch_attempt,
                            batch: next,
                        })
                        .await?;
                } else {
                    return Err(invalid(
                        CentralErrorCode::ConcurrentUpdate,
                        "materialization failure report arrived after Batch success",
                    ));
                }
                let latest_job = placement
                    .get_materialization(
                        tenant_id,
                        &batch.target.object_namespace_id,
                        &materialization_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::ResourceNotFound,
                            "materialization Job disappeared while applying failure",
                        )
                    })?;
                let final_job = if latest_job.state.terminal() {
                    replayed = true;
                    latest_job
                } else {
                    let mut next_job = latest_job.clone();
                    next_job.state = neoengram_domain::protocol::materialization::MaterializationJobState::Stalled;
                    next_job.issue = Some(issue);
                    next_job.updated_at_unix_ms = self.clock.now();
                    placement
                        .replace_materialization(
                            tenant_id,
                            &materialization_id,
                            latest_job.plan_revision,
                            next_job,
                        )
                        .await?
                };
                self.sync_materialization_task(
                    &final_job,
                    TaskActor::Agent {
                        agent_id: agent_id.clone(),
                    },
                    task_issue,
                )
                .await?;
                Ok(MaterializationReportResult {
                    resource_version: ResourceVersion::new(1),
                    replayed,
                })
            }
        }
    }

    /// Persists a complete Agent Volume scrub after validating its authenticated Volume/session
    /// binding. Health observations are append-only by scan identity and latest-state lookups are
    /// used by the planner to exclude confirmed missing/corrupt source objects.
    pub async fn receive_integrity_report(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        agent_id: &AgentId,
        session_generation: SessionGeneration,
        report: IntegrityScanReport,
    ) -> CentralResult<IntegrityReportResult> {
        report.validate().map_err(CentralError::from)?;
        if &report.scan.tenant_id != tenant_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "integrity report tenant differs from its authenticated Agent session",
            ));
        }
        let placement = self.placement.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::StorageFailure,
                "placement health authority is unavailable",
            )
        })?;
        if let Some(agent_registry) = &self.agent_registry {
            let record = agent_registry
                .get_by_agent(agent_id)
                .await?
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::GatewayRouteUnavailable,
                        "integrity report Agent enrollment is unavailable",
                    )
                })?;
            if record.mount.storage_volume_id != report.scan.storage_volume_id
                || record.mount.mount_generation != report.mount_generation
                || record.owner.storage_volume_id != report.scan.storage_volume_id
                || record.owner.active_agent_id.as_ref() != Some(agent_id)
                || record
                    .instance
                    .as_ref()
                    .and_then(|instance| instance.session_generation)
                    != Some(session_generation)
            {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "integrity report mount, owner, or session generation is stale",
                ));
            }
        }
        for observation in report.observations {
            if observation.tenant_id != *tenant_id
                || observation.storage_volume_id != report.scan.storage_volume_id
            {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "integrity observation is outside the authenticated Volume scope",
                ));
            }
            placement
                .record_placement_health_observation(observation)
                .await?;
        }
        Ok(IntegrityReportResult {
            resource_version: ResourceVersion::new(1),
            replayed: false,
        })
    }

    #[allow(dead_code)]
    async fn replication_assignment(
        &self,
        current: &ReplicationRecord,
    ) -> CentralResult<Option<ReplicationAssignment>> {
        let (Some(placement), Some(keyring)) = (&self.placement, &self.replication_ticket_keyring)
        else {
            return Ok(None);
        };
        let mut record = current.clone();
        if record.state == ReplicationState::Queued {
            record = placement
                .transition_replication(ReplicationStateTransitionRequest {
                    tenant_id: record.tenant_id.clone(),
                    replication_id: record.replication_id.clone(),
                    expected_state: ReplicationState::Queued,
                    expected_attempt: record.attempt,
                    next_state: ReplicationState::Planning,
                    completed_objects: record.completed_objects,
                    completed_bytes: record.completed_bytes,
                    issue_code: None,
                    issue_message: None,
                    updated_at_unix_ms: self.clock.now(),
                })
                .await?;
        }
        let object_set = placement
            .get_commit_object_set(&record.tenant_id, &record.commit_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "Commit ObjectSet disappeared",
                )
            })?;
        if object_set.object_set.object_set_digest != record.object_set_digest {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "replication ObjectSet differs from its frozen authority record",
            ));
        }
        let source_placement_id = record
            .source_placement_set_id
            .as_ref()
            .map(|value| neoengram_domain::protocol::PlacementId::new(value.to_string()))
            .transpose()?;
        let target_placement_id = record
            .target_placement_set_id
            .as_ref()
            .map(|value| neoengram_domain::protocol::PlacementId::new(value.to_string()))
            .transpose()?;
        let (
            Some(artifact_id),
            Some(source_placement_id),
            Some(target_placement_id),
            Some(source_volume_id),
            Some(source_edge_cluster_id),
            Some(source_gateway_pool_id),
            Some(source_agent_id),
            Some(source_session_generation),
            Some(source_mount_generation),
            Some(source_route_generation),
            Some(target_edge_cluster_id),
            Some(target_gateway_pool_id),
            Some(target_agent_id),
            Some(target_session_generation),
            Some(target_mount_generation),
            Some(target_route_generation),
            Some(transfer_id),
        ) = (
            record.artifact_id.clone(),
            source_placement_id,
            target_placement_id,
            record.source_storage_volume_id.clone(),
            record.source_edge_cluster_id.clone(),
            record.source_gateway_pool_id.clone(),
            record.source_agent_id.clone(),
            record.source_session_generation,
            record.source_mount_generation,
            record.source_route_generation,
            record.target_edge_cluster_id.clone(),
            record.target_gateway_pool_id.clone(),
            record.target_agent_id.clone(),
            record.target_session_generation,
            record.target_mount_generation,
            record.target_route_generation,
            record.transfer_id.clone(),
        )
        else {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "replication route binding is incomplete",
            ));
        };
        let source_generations = self
            .current_replication_route_generations(
                &source_agent_id,
                &source_edge_cluster_id,
                &source_gateway_pool_id,
                source_session_generation,
                source_mount_generation,
                source_route_generation,
            )
            .await?;
        let target_generations = self
            .current_replication_route_generations(
                &target_agent_id,
                &target_edge_cluster_id,
                &target_gateway_pool_id,
                target_session_generation,
                target_mount_generation,
                target_route_generation,
            )
            .await?;
        let expected_source = replication_route_binding(
            &record,
            true,
            source_session_generation,
            source_mount_generation,
            source_route_generation,
        )?;
        let expected_target = replication_route_binding(
            &record,
            false,
            target_session_generation,
            target_mount_generation,
            target_route_generation,
        )?;
        let refreshed_source = replication_route_binding(
            &record,
            true,
            source_generations.session,
            source_generations.mount,
            source_generations.route,
        )?;
        let refreshed_target = replication_route_binding(
            &record,
            false,
            target_generations.session,
            target_generations.mount,
            target_generations.route,
        )?;
        if refreshed_source != expected_source || refreshed_target != expected_target {
            record = placement
                .refresh_replication_routes(RefreshReplicationRoutesRequest {
                    tenant_id: record.tenant_id.clone(),
                    replication_id: record.replication_id.clone(),
                    expected_attempt: record.attempt,
                    expected_source,
                    expected_target,
                    source: refreshed_source,
                    target: refreshed_target,
                    updated_at_unix_ms: self.clock.now(),
                })
                .await?;
        }
        let mut allowed_objects = object_set
            .object_set
            .objects
            .iter()
            .map(|object| object.object_id)
            .collect::<Vec<_>>();
        allowed_objects.sort_unstable();
        let ticket = TransferTicket {
            transfer_id,
            tenant_id: record.tenant_id.clone(),
            artifact_id: artifact_id.clone(),
            commit_id: neoengram_domain::CommitId::from_digest(record.commit_id),
            object_set_digest: record.object_set_digest,
            source: TransferEndpoint {
                placement_id: source_placement_id,
                agent_id: source_agent_id,
                gateway_pool_id: source_gateway_pool_id,
                edge_cluster_id: source_edge_cluster_id,
                storage_volume_id: Some(source_volume_id),
            },
            target: TransferEndpoint {
                placement_id: target_placement_id,
                agent_id: target_agent_id,
                gateway_pool_id: target_gateway_pool_id,
                edge_cluster_id: target_edge_cluster_id,
                storage_volume_id: Some(record.target_storage_volume_id.clone()),
            },
            source_session_generation: source_generations.session,
            source_mount_generation: source_generations.mount,
            source_route_generation: source_generations.route,
            session_generation: target_generations.session,
            mount_generation: target_generations.mount,
            route_generation: target_generations.route,
            deadline_unix_ms: replication_ticket_deadline(self.clock.now())?,
            max_bytes: neoengram_domain::protocol::DecimalU64::new(record.total_bytes),
            allowed_objects,
        };
        let signed_ticket = keyring
            .sign_transfer_ticket(ticket, self.clock.now(), DEFAULT_CENTRAL_COMMAND_TTL_MS)
            .await
            .map_err(|error| invalid(CentralErrorCode::Internal, error.to_string()))?;
        let assignment = ReplicationAssignment {
            replication_id: record.replication_id.clone(),
            task_fence: execution_fence(
                record.replication_id.as_str(),
                record.attempt,
                "transfer",
            )?,
            tenant_id: record.tenant_id,
            artifact_id,
            commit_id: neoengram_domain::CommitId::from_digest(record.commit_id),
            attempt: record.attempt,
            signed_ticket: SignedTransferTicket {
                ticket: signed_ticket.ticket,
                central_signature: signed_ticket.central_signature,
            },
            object_set: object_set.object_set,
            extensions: Extensions::new(),
        };
        assignment.validate()?;
        Ok(Some(assignment))
    }

    /// Applies one authenticated Agent replication checkpoint. Object reports are persisted before
    /// the final publication report; only `Published` invokes the PlacementRepository atomic
    /// finalize operation that makes the target copy visible to availability/S3 consumers.
    pub async fn receive_replication_report(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        agent_id: &AgentId,
        session_generation: SessionGeneration,
        report: ReplicationProgressReport,
    ) -> CentralResult<ReplicationReportResult> {
        report.validate()?;
        if report.tenant_id() != tenant_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "replication report tenant differs from its authenticated session",
            ));
        }
        let placement = self.placement.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "placement authority is unavailable for replication reports",
            )
        })?;
        let replication_id = report.replication_id().clone();
        let mut current = placement
            .get_replication(tenant_id, &replication_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    "replication does not exist",
                )
            })?;
        if current.target_agent_id.as_ref() != Some(agent_id) {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "replication report is not bound to the target Agent",
            ));
        }
        if report.attempt() < current.attempt {
            // A cancelled/failed attempt can still have durable Agent reports in flight when the
            // caller starts its successor. They are authenticated as belonging to the same target
            // Agent, but can no longer mutate authority. Acknowledge them as replays so the Agent
            // outbox can drain and deliver reports for the current attempt.
            return Ok(ReplicationReportResult {
                resource_version: ResourceVersion::new(1),
                replayed: true,
            });
        }
        if report.attempt() > current.attempt {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                "replication report attempt is ahead of authority",
            ));
        }
        validate_replication_report_binding(placement.as_ref(), tenant_id, &current, &report)
            .await?;
        // Cancellation is authoritative for its fenced attempt. An Agent may reconnect with
        // reports that were durably queued before it observed the cancellation; acknowledge and
        // discard those reports so its outbox can drain without changing checkpoints or making a
        // target Placement visible.
        if current.state == ReplicationState::Cancelled {
            return Ok(ReplicationReportResult {
                resource_version: ResourceVersion::new(1),
                replayed: true,
            });
        }
        if current.target_session_generation != Some(session_generation) {
            // A process restart legitimately advances the session generation while the
            // Replication attempt remains active. Accept the report only after re-reading the
            // current route and mount fence for the same Agent; a replacement Agent or Volume
            // mount still fails closed.
            let (
                Some(target_edge_cluster_id),
                Some(target_gateway_pool_id),
                Some(target_agent_id),
                Some(target_session_generation),
                Some(target_mount_generation),
                Some(target_route_generation),
            ) = (
                current.target_edge_cluster_id.clone(),
                current.target_gateway_pool_id.clone(),
                current.target_agent_id.clone(),
                current.target_session_generation,
                current.target_mount_generation,
                current.target_route_generation,
            )
            else {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "replication report target route binding is incomplete",
                ));
            };
            let refreshed = self
                .current_replication_route_generations(
                    &target_agent_id,
                    &target_edge_cluster_id,
                    &target_gateway_pool_id,
                    target_session_generation,
                    target_mount_generation,
                    target_route_generation,
                )
                .await?;
            if !reconnected_replication_report_matches_route(
                target_session_generation,
                target_mount_generation,
                target_route_generation,
                session_generation,
                refreshed,
            ) {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "replication report is not bound to the current target route and mount",
                ));
            }
            // Persist the advanced target fence before applying the report. This keeps the
            // durable attempt aligned with the session that authenticated the replay and avoids
            // validating every later outbox item against a permanently stale route snapshot.
            // The source does not need to be live here: its frozen binding is carried forward
            // unchanged, so a target can drain durable reports while the source is offline.
            if !current_is_terminal_replication(current.state) {
                if let (
                    Some(source_session_generation),
                    Some(source_mount_generation),
                    Some(source_route_generation),
                ) = (
                    current.source_session_generation,
                    current.source_mount_generation,
                    current.source_route_generation,
                ) {
                    let expected_source = replication_route_binding(
                        &current,
                        true,
                        source_session_generation,
                        source_mount_generation,
                        source_route_generation,
                    )?;
                    let expected_target = replication_route_binding(
                        &current,
                        false,
                        target_session_generation,
                        target_mount_generation,
                        target_route_generation,
                    )?;
                    let refreshed_target = replication_route_binding(
                        &current,
                        false,
                        refreshed.session,
                        refreshed.mount,
                        refreshed.route,
                    )?;
                    current = placement
                        .refresh_replication_routes(RefreshReplicationRoutesRequest {
                            tenant_id: tenant_id.clone(),
                            replication_id: replication_id.clone(),
                            expected_attempt: current.attempt,
                            source: expected_source.clone(),
                            target: refreshed_target,
                            expected_source,
                            expected_target,
                            updated_at_unix_ms: self.clock.now(),
                        })
                        .await?;
                }
            }
        }
        let now = self.clock.now();
        let mut replayed = false;
        match report {
            ReplicationProgressReport::State {
                state,
                completed_objects,
                completed_bytes,
                issue_code,
                issue_message,
                ..
            } => {
                let state_is_stale = match (
                    replication_active_state_rank(current.state),
                    replication_active_state_rank(state),
                ) {
                    (Some(current_rank), Some(next_rank)) => {
                        next_rank < current_rank
                            || completed_objects < current.completed_objects
                            || completed_bytes < current.completed_bytes
                            || (next_rank == current_rank
                                && completed_objects == current.completed_objects
                                && completed_bytes == current.completed_bytes)
                    }
                    _ => false,
                };
                if current_is_terminal_replication(current.state)
                    || state == ReplicationState::Published
                    || state_is_stale
                {
                    replayed = true;
                } else {
                    // Failed/Cancelled are explicit terminal transitions. They are accepted even
                    // when the last progress counters were higher because the counters are only
                    // informational and a failed transfer must not be resurrected by a replay.
                    placement
                        .transition_replication(ReplicationStateTransitionRequest {
                            tenant_id: tenant_id.clone(),
                            replication_id,
                            expected_state: current.state,
                            expected_attempt: current.attempt,
                            next_state: state,
                            completed_objects,
                            completed_bytes,
                            issue_code,
                            issue_message,
                            updated_at_unix_ms: now,
                        })
                        .await?;
                }
            }
            ReplicationProgressReport::Object {
                object_id,
                offset,
                state,
                ..
            } => {
                if current_is_terminal_replication(current.state) {
                    replayed = true;
                } else {
                    let existing = placement
                        .list_replication_objects(tenant_id, &replication_id)
                        .await?
                        .into_iter()
                        .find(|checkpoint| checkpoint.object_id == object_id);
                    let state_is_stale = existing.as_ref().is_some_and(|checkpoint| {
                        offset < checkpoint.offset
                            || replication_object_state_rank(state)
                                < replication_object_state_rank(checkpoint.state)
                            || (offset == checkpoint.offset && state == checkpoint.state)
                    });
                    if state_is_stale {
                        replayed = true;
                    } else {
                        let checkpoint = ReplicationObjectRecord {
                            tenant_id: tenant_id.clone(),
                            replication_id,
                            object_id,
                            offset,
                            state,
                            retry_count: current.attempt,
                            updated_at_unix_ms: now,
                        };
                        placement.upsert_replication_object(checkpoint).await?;
                    }
                }
            }
            ReplicationProgressReport::Published {
                commit_id,
                object_set_digest,
                ..
            } => {
                if current_is_terminal_replication(current.state) {
                    replayed = true;
                } else {
                    let object_set = placement
                        .get_commit_object_set(tenant_id, &current.commit_id)
                        .await?
                        .ok_or_else(|| {
                            invalid(
                                CentralErrorCode::InvalidState,
                                "Commit ObjectSet is missing",
                            )
                        })?;
                    let backend_id = neoengram_domain::protocol::BackendId::new(
                        current.target_backend_id.clone(),
                    )?;
                    let placement_generation =
                        current.target_placement_generation.ok_or_else(|| {
                            invalid(
                                CentralErrorCode::InvalidState,
                                "target placement generation is missing",
                            )
                        })?;
                    let placements = object_set
                        .object_set
                        .objects
                        .iter()
                        .map(|object| neoengram_domain::protocol::ObjectPlacement {
                            tenant_id: tenant_id.clone(),
                            object_id: object.object_id,
                            backend_id: backend_id.clone(),
                            storage_volume_id: Some(current.target_storage_volume_id.clone()),
                            archive_id: None,
                            edge_cluster_id: current.target_edge_cluster_id.clone(),
                            gateway_pool_id: current.target_gateway_pool_id.clone(),
                            region: None,
                            placement_generation,
                            state: neoengram_domain::protocol::PlacementState::Verified,
                            verified_size: object.size,
                            verified_digest: object.object_id.digest(),
                            failure_domain: format!(
                                "cluster/{}/pool/{}/volume/{}",
                                current
                                    .target_edge_cluster_id
                                    .as_ref()
                                    .map(ToString::to_string)
                                    .unwrap_or_default(),
                                current
                                    .target_gateway_pool_id
                                    .as_ref()
                                    .map(ToString::to_string)
                                    .unwrap_or_default(),
                                current.target_storage_volume_id,
                            ),
                        })
                        .collect::<Vec<_>>();
                    let placement_set_id =
                        current.target_placement_set_id.clone().ok_or_else(|| {
                            invalid(
                                CentralErrorCode::InvalidState,
                                "target PlacementSet ID is missing",
                            )
                        })?;
                    let placement_set = neoengram_domain::protocol::CommitPlacementSet {
                        placement_set_id,
                        tenant_id: tenant_id.clone(),
                        commit_id,
                        backend_id,
                        storage_volume_id: Some(current.target_storage_volume_id.clone()),
                        archive_id: None,
                        object_set_digest,
                        object_count: neoengram_domain::protocol::DecimalU64::new(
                            current.total_objects,
                        ),
                        verified_object_count: neoengram_domain::protocol::DecimalU64::new(
                            current.total_objects,
                        ),
                        placement_generation,
                        state: neoengram_domain::protocol::CommitPlacementSetState::Published,
                    };
                    placement
                        .finalize_replication(FinalizeReplicationRequest {
                            tenant_id: tenant_id.clone(),
                            replication_id,
                            expected_attempt: current.attempt,
                            placements,
                            placement_set,
                            finalized_at_unix_ms: now,
                        })
                        .await?;
                }
            }
        }
        Ok(ReplicationReportResult {
            resource_version: ResourceVersion::new(1),
            replayed,
        })
    }

    /// Applies one lifecycle report against its exact durable command and generation fences.
    /// Terminal reports are evidenced before the outbox row is retired, so a crash at any point
    /// is repaired by replay without accepting a different terminal payload.
    pub async fn receive_lifecycle_report(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        agent_id: &AgentId,
        session_generation: SessionGeneration,
        report: ResourceLifecycleReport,
    ) -> CentralResult<(ResourceVersion, bool)> {
        report.validate()?;
        if &report.tenant_id != tenant_id
            || &report.agent_id != agent_id
            || report.session_generation != session_generation
        {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "lifecycle report differs from its authenticated Agent session",
            ));
        }
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::StorageFailure,
                "resource lifecycle catalog is unavailable",
            )
        })?;
        let record = catalog
            .get_lifecycle_assignment(tenant_id, &report.assignment_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "lifecycle report has no durable Central assignment",
                )
            })?;
        report.validate_for_assignment(&record.assignment)?;
        if !record.published {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "lifecycle report arrived before its assignment was published",
            ));
        }
        let operation = catalog
            .get_deletion_operation(tenant_id, &report.deletion_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "lifecycle report deletion operation no longer exists",
                )
            })?;
        if record.retired {
            let replay_digest = neoengram_domain::protocol::jcs_blake3(&report)?;
            if record.terminal_report_digest.as_ref() == Some(&replay_digest) {
                return Ok((operation.resource_version, true));
            }
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "lifecycle assignment already has a different terminal report",
            ));
        }
        let registry = self.agent_registry.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::StorageFailure,
                "resource lifecycle Agent registry is unavailable",
            )
        })?;
        let current = registry
            .get_current_by_volume(tenant_id, &report.storage_volume_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "lifecycle report Volume no longer has an enrolled Agent",
                )
            })?;
        let current_instance = current.instance.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::AssignmentMismatch,
                "lifecycle report Agent no longer has an active instance",
            )
        })?;
        if current_instance.agent_id != report.agent_id
            || current_instance.session_generation != Some(report.session_generation)
            || current.mount.mount_generation != report.mount_generation
            || current.owner.owner_generation != report.owner_generation
            || current.owner.active_agent_id.as_ref() != Some(&report.agent_id)
            || current.owner.active_agent_mount_id.as_ref() != Some(&current.mount.agent_mount_id)
        {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "lifecycle report is fenced by the current Agent owner generation",
            ));
        }
        if matches!(report.state, ResourceLifecycleReportState::Accepted) {
            return Ok((operation.resource_version, false));
        }

        let report_digest = neoengram_domain::protocol::jcs_blake3(&report)?;
        catalog
            .record_lifecycle_report(tenant_id, &report.assignment_id, &report_digest)
            .await?;
        let event_id = lifecycle_event_id(&report.assignment_id, &report_digest)?;
        let proof = matches!(report.state, ResourceLifecycleReportState::Purged)
            .then(|| {
                report
                    .evidence
                    .as_ref()
                    .expect("purged report validation requires evidence")
            })
            .map(|evidence| -> CentralResult<DeletionProof> {
                Ok(DeletionProof {
                    proof_id: lifecycle_proof_id(&report.assignment_id, &report_digest)?,
                    tenant_id: report.tenant_id.clone(),
                    deletion_id: report.deletion_id.clone(),
                    resource: report.resource.clone(),
                    lifecycle_generation: report.lifecycle_generation,
                    agent_id: report.agent_id.clone(),
                    result: DeletionProofResult::Complete,
                    file_count: evidence.file_count,
                    object_count: evidence.object_count,
                    byte_count: evidence.byte_count,
                    object_set_digest: evidence.object_set_digest,
                    report_digest,
                    completed_at_unix_ms: report.reported_at_unix_ms,
                })
            })
            .transpose()?;
        catalog
            .append_lifecycle_evidence(
                tenant_id,
                &report.deletion_id,
                crate::LifecycleEvidenceBatch {
                    event: Some(LifecycleEvent {
                        event_id,
                        tenant_id: report.tenant_id.clone(),
                        deletion_id: report.deletion_id.clone(),
                        kind: if proof.is_some() {
                            LifecycleEventKind::ProofAccepted
                        } else {
                            LifecycleEventKind::StateChanged
                        },
                        occurred_at_unix_ms: report.reported_at_unix_ms,
                        payload_digest: report_digest,
                    }),
                    proof,
                },
            )
            .await?;

        let failed_state = match report.state {
            ResourceLifecycleReportState::Blocked => Some(DeletionOperationState::Blocked),
            ResourceLifecycleReportState::Failed => Some(DeletionOperationState::Failed),
            _ => None,
        };
        let operation = if let Some(next_state) = failed_state {
            catalog
                .transition_deletion_state(crate::DeletionTransitionRequest {
                    tenant_id: tenant_id.clone(),
                    deletion_id: report.deletion_id.clone(),
                    expected_state: operation.state,
                    next_state,
                    expected_resource_version: operation.resource_version.get(),
                    now_unix_ms: report.reported_at_unix_ms,
                    last_error: report.error.as_ref().map(|error| error.message.clone()),
                })
                .await?
        } else {
            operation
        };
        let retired = catalog
            .retire_lifecycle_assignment(tenant_id, &report.assignment_id)
            .await?;
        Ok((operation.resource_version, retired.retired))
    }

    /// Checks Create Add Job authorization without reading or mutating Job authority state.
    pub async fn preauthorize_create_add_job(
        &self,
        actor: &neoengram_domain::protocol::PrincipalRef,
        spec: &AddJobSpec,
    ) -> CentralResult<()> {
        self.authorize(Actor::Principal(actor.clone()), Action::CreateAddJob, spec)
            .await
    }

    /// Creates the authoritative queued job. Reusing a JobId with another digest/spec is rejected.
    pub async fn create_add_job(
        &self,
        request: CreateAddJobRequest,
    ) -> CentralResult<CreateAddJobResult> {
        self.authorize(
            Actor::Principal(request.actor),
            Action::CreateAddJob,
            &request.spec,
        )
        .await?;
        self.persist_add_job(request.spec, "create").await
    }

    /// Creates the internal Add Job associated with a durable Pre-commit attempt.
    ///
    /// Public authorization occurs before the Pre-commit aggregate is persisted. Recovery must
    /// not depend on that principal retaining mutable RBAC grants, so only this fixed system
    /// principal may enter the trusted continuation path.
    pub async fn create_precommit_add_job(
        &self,
        spec: AddJobSpec,
    ) -> CentralResult<CreateAddJobResult> {
        if spec.principal.kind != PrincipalKind::System
            || spec.principal.id.as_str() != "precommit-scanner"
        {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "Pre-commit Add Job requires the fixed system principal",
            ));
        }
        self.persist_add_job(spec, "precommit-create").await
    }

    async fn persist_add_job(
        &self,
        spec: AddJobSpec,
        audit_action: &'static str,
    ) -> CentralResult<CreateAddJobResult> {
        let key = crate::JobKey::new(spec.tenant_id.clone(), spec.job_id.clone());
        if let Some(existing) = self.jobs.get(&key).await? {
            if existing.spec != spec {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    "JobId already belongs to a different managed Add request",
                ));
            }
            self.audit(&existing, AuditKind::JobCreated, audit_action)
                .await?;
            return Ok(CreateAddJobResult {
                job: existing,
                replayed: true,
            });
        }
        validate_job_spec(&spec, self.clock.now().get())?;

        let job = JobRecord {
            spec: spec.clone(),
            operation: JobOperation::Add,
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
        };
        let (job, replayed) = match self.jobs.insert_or_load(job).await? {
            JobInsertOutcome::Inserted(job) => (job, false),
            JobInsertOutcome::Existing(existing) => {
                if existing.spec != spec {
                    return Err(invalid(
                        CentralErrorCode::JobIdReused,
                        "JobId already belongs to a different managed Add request",
                    ));
                }
                (existing, true)
            }
        };
        self.audit(&job, AuditKind::JobCreated, audit_action)
            .await?;
        Ok(CreateAddJobResult { job, replayed })
    }

    /// Creates the durable infrastructure Job used to materialize a Workspace directory.
    ///
    /// The record is intentionally stored in the same Job table as managed Add so the existing
    /// assignment outbox foreign key and recovery scanner cover both operations. Its operation
    /// discriminant and immutable WorkspaceMaterializeSpec make replay identity explicit.
    pub async fn create_workspace_materialization(
        &self,
        request: CreateWorkspaceMaterializationRequest,
    ) -> CentralResult<CreateWorkspaceMaterializationResult> {
        let spec = request.spec;
        let canonical = WorkspaceMaterializeAssignment::canonical_relative_root(
            &spec.project_id,
            &spec.artifact_id,
            &spec.workspace_id,
        )?;
        if spec.relative_root != canonical {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "WorkspaceMaterialize relative_root is not server-derived",
            ));
        }
        if spec.request_digest != spec.computed_request_digest()? {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "WorkspaceMaterialize request digest does not bind its immutable spec",
            ));
        }
        if spec.deadline_unix_ms.get() <= self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "WorkspaceMaterialize deadline has elapsed",
            ));
        }

        // JobRecord keeps a common tenant/artifact identity for authorization and audit. The
        // operation-specific spec remains authoritative for materialization validation.
        let mut job_scope = AddJobSpec {
            job_id: spec.job_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            workspace_id: spec.workspace_id.clone(),
            expected_index_version: WireIndexVersion {
                revision: IndexRevision::new(0),
                digest: neoengram_domain::core::ContentDigest::from_bytes([0; 32]),
                extensions: Extensions::new(),
            },
            data_layout: neoengram_domain::protocol::CommitDataLayout::FastCdc,
            request_digest: neoengram_domain::core::ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: spec.deadline_unix_ms,
            paths: Vec::new(),
            all: true,
            operation_task_id: spec.operation_task_id.clone(),
            extensions: Extensions::new(),
        };
        job_scope.request_digest = job_scope.computed_request_digest()?;
        let job = JobRecord {
            spec: job_scope,
            operation: JobOperation::WorkspaceMaterialize,
            workspace_spec: Some(spec.clone()),
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
        };
        let (job, replayed) = match self.jobs.insert_or_load(job).await? {
            JobInsertOutcome::Inserted(job) => (job, false),
            JobInsertOutcome::Existing(existing) => {
                if existing.operation != JobOperation::WorkspaceMaterialize
                    || existing.workspace_spec.as_ref() != Some(&spec)
                {
                    return Err(invalid(
                        CentralErrorCode::JobIdReused,
                        "materialization JobId is already bound to another operation",
                    ));
                }
                (existing, true)
            }
        };
        self.sync_control_job_task(&job, None).await?;
        self.audit(&job, AuditKind::JobCreated, "materialize-create")
            .await?;
        Ok(CreateWorkspaceMaterializationResult { job, replayed })
    }

    /// Persists and publishes a server-selected WorkspaceMaterialize assignment.
    pub async fn assign_workspace_materialization(
        &self,
        request: AssignWorkspaceMaterializationRequest,
    ) -> CentralResult<AssignWorkspaceMaterializationResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.operation != JobOperation::WorkspaceMaterialize {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "Job is not a WorkspaceMaterialize operation",
            ));
        }
        let spec = job.workspace_spec.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "WorkspaceMaterialize Job has no immutable operation spec",
            )
        })?;
        if spec.storage_volume_id != request.target.storage_volume_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "materialization assignment selected a different StorageVolume",
            ));
        }
        let relative_root = spec.relative_root.clone();
        let assignment = WorkspaceMaterializeAssignment {
            job_id: spec.job_id.clone(),
            task_fence: operation_task_fence(
                self.task_coordinator.as_deref(),
                spec.operation_task_id.as_ref(),
                &spec.tenant_id,
                spec.job_id.as_str(),
                1,
                "materialize",
            )
            .await?,
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            workspace_id: spec.workspace_id.clone(),
            storage_volume_id: spec.storage_volume_id.clone(),
            agent_mount_id: request.target.agent_mount_id.clone(),
            mount_generation: request.target.mount_generation,
            owner_generation: request.target.owner_generation,
            relative_root,
            base_commit_id: spec.base_commit_id,
            base_index_version: spec.base_index_version.clone(),
            request_digest: spec.request_digest,
            deadline_unix_ms: spec.deadline_unix_ms,
            extensions: Extensions::new(),
        };
        assignment.validate()?;
        let envelope = JobAssignment {
            assignment: AssignmentOperation::WorkspaceMaterialize {
                input: assignment.clone(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
        envelope.validate()?;

        if let Some(existing) = &job.workspace_assignment {
            if existing != &assignment {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    "materialization Job already has a different assignment",
                ));
            }
            let _ = self.outbox.reserve(envelope.clone()).await?;
            let _ = self.outbox.publish(envelope.clone()).await?;
            self.sync_control_job_task(&job, None).await?;
            return Ok(AssignWorkspaceMaterializationResult {
                job,
                assignment: envelope,
                replayed: true,
            });
        }
        if job.state != JobState::Queued {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot assign materialization Job in state {:?}", job.state),
            ));
        }
        let _ = self.outbox.reserve(envelope.clone()).await?;
        let previous = job.resource_version.get();
        job.workspace_assignment = Some(assignment.clone());
        job.state = JobState::Assigned;
        job.resource_version = ResourceVersion::new(previous.saturating_add(1));
        job = match self.replace(previous, job).await {
            Ok(job) => job,
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                let persisted = self.load(&request.tenant_id, &request.job_id).await?;
                if persisted.workspace_assignment.as_ref() != Some(&assignment) {
                    return Err(error);
                }
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.publish(envelope.clone()).await?;
                self.sync_control_job_task(&persisted, None).await?;
                return Ok(AssignWorkspaceMaterializationResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };
        let _ = self.outbox.publish(envelope.clone()).await?;
        self.sync_control_job_task(&job, None).await?;
        self.audit(&job, AuditKind::AssignmentQueued, "materialize-assignment")
            .await?;
        Ok(AssignWorkspaceMaterializationResult {
            job,
            assignment: envelope,
            replayed: false,
        })
    }

    /// Creates the durable infrastructure Job used to materialize a SnapshotDelivery.
    pub async fn create_snapshot_delivery(
        &self,
        request: CreateSnapshotDeliveryRequest,
    ) -> CentralResult<CreateSnapshotDeliveryResult> {
        let spec = request.spec;
        spec.operation().validate()?;
        if spec.request_digest != spec.computed_request_digest()? {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "SnapshotDelivery request digest does not bind its immutable spec",
            ));
        }
        if spec.deadline_unix_ms.get() <= self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "SnapshotDelivery deadline has elapsed",
            ));
        }
        let workspace_id =
            neoengram_domain::protocol::WorkspaceId::new(spec.snapshot_id.as_str().to_owned())?;
        let mut job_scope = AddJobSpec {
            job_id: spec.job_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            workspace_id,
            expected_index_version: WireIndexVersion {
                revision: neoengram_domain::protocol::IndexRevision::new(0),
                digest: neoengram_domain::core::ContentDigest::from_bytes([0; 32]),
                extensions: Extensions::new(),
            },
            data_layout: neoengram_domain::protocol::CommitDataLayout::FastCdc,
            request_digest: neoengram_domain::core::ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: spec.deadline_unix_ms,
            paths: Vec::new(),
            all: true,
            operation_task_id: spec.operation_task_id.clone(),
            extensions: Extensions::new(),
        };
        job_scope.request_digest = job_scope.computed_request_digest()?;
        let job = JobRecord {
            spec: job_scope,
            operation: JobOperation::SnapshotDelivery,
            workspace_spec: None,
            delivery_spec: Some(spec.clone()),
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
        };
        let (job, replayed) = match self.jobs.insert_or_load(job).await? {
            JobInsertOutcome::Inserted(job) => (job, false),
            JobInsertOutcome::Existing(existing) => {
                if existing.operation != JobOperation::SnapshotDelivery
                    || existing.delivery_spec.as_ref() != Some(&spec)
                {
                    return Err(invalid(
                        CentralErrorCode::JobIdReused,
                        "SnapshotDelivery JobId is already bound to another operation",
                    ));
                }
                (existing, true)
            }
        };
        self.sync_control_job_task(&job, None).await?;
        self.audit(&job, AuditKind::JobCreated, "snapshot-delivery-create")
            .await?;
        Ok(CreateSnapshotDeliveryResult { job, replayed })
    }

    /// Persists and publishes a server-selected SnapshotDelivery assignment.
    pub async fn assign_snapshot_delivery(
        &self,
        request: AssignSnapshotDeliveryRequest,
    ) -> CentralResult<AssignSnapshotDeliveryResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.operation != JobOperation::SnapshotDelivery {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "Job is not a SnapshotDelivery operation",
            ));
        }
        let spec = job.delivery_spec.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "SnapshotDelivery Job has no immutable operation spec",
            )
        })?;
        if spec.storage_volume_id != request.target.storage_volume_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "SnapshotDelivery selected a different StorageVolume",
            ));
        }
        let assignment = SnapshotDeliveryAssignment {
            job_id: spec.job_id.clone(),
            task_fence: operation_task_fence(
                self.task_coordinator.as_deref(),
                spec.operation_task_id.as_ref(),
                &spec.tenant_id,
                spec.job_id.as_str(),
                1,
                "delivery_materialize",
            )
            .await?,
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: spec.principal.clone(),
            action: spec.action,
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            snapshot_id: spec.snapshot_id.clone(),
            delivery_id: spec.delivery_id.clone(),
            commit_id: spec.commit_id,
            storage_volume_id: spec.storage_volume_id.clone(),
            snapshot_size_bytes: spec.snapshot_size_bytes,
            copy_reserve_bytes: spec.copy_reserve_bytes,
            hardlink_policy: spec.hardlink_policy,
            agent_mount_id: request.target.agent_mount_id.clone(),
            mount_generation: request.target.mount_generation,
            owner_generation: request.target.owner_generation,
            placement_generation: request.target.placement_generation,
            mode: spec.mode,
            target_relative_root: spec.target_relative_root.clone(),
            source_index_digest: spec.source_index_digest,
            request_digest: spec.request_digest,
            delivery_generation: spec.delivery_generation,
            deadline_unix_ms: spec.deadline_unix_ms,
            extensions: Extensions::new(),
        };
        assignment.validate()?;
        let envelope = JobAssignment {
            assignment: AssignmentOperation::SnapshotDelivery {
                input: assignment.clone(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
        envelope.validate()?;
        if let Some(existing) = &job.delivery_assignment {
            if existing == &assignment {
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.reactivate(envelope.clone()).await?;
                self.sync_control_job_task(&job, None).await?;
                return Ok(AssignSnapshotDeliveryResult {
                    job,
                    assignment: envelope,
                    replayed: true,
                });
            }
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "SnapshotDelivery Job already has a different persisted assignment",
            ));
        }
        if job.state != JobState::Queued {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!(
                    "cannot assign SnapshotDelivery Job in state {:?}",
                    job.state
                ),
            ));
        }
        let _ = self.outbox.reserve(envelope.clone()).await?;
        let previous = job.resource_version.get();
        job.delivery_assignment = Some(assignment);
        job.state = JobState::Assigned;
        job.resource_version = ResourceVersion::new(previous.saturating_add(1));
        job = match self.replace(previous, job).await {
            Ok(job) => job,
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                let persisted = self.load(&request.tenant_id, &request.job_id).await?;
                if persisted.delivery_assignment.as_ref()
                    != match &envelope.assignment {
                        AssignmentOperation::SnapshotDelivery { input, .. } => Some(input),
                        _ => None,
                    }
                {
                    return Err(error);
                }
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.publish(envelope.clone()).await?;
                self.sync_control_job_task(&persisted, None).await?;
                return Ok(AssignSnapshotDeliveryResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };
        let _ = self.outbox.publish(envelope.clone()).await?;
        self.sync_control_job_task(&job, None).await?;
        self.audit(
            &job,
            AuditKind::AssignmentQueued,
            "snapshot-delivery-assignment",
        )
        .await?;
        Ok(AssignSnapshotDeliveryResult {
            job,
            assignment: envelope,
            replayed: false,
        })
    }

    /// Marks an elapsed materialization terminal and exposes the failed lifecycle on Workspace.
    pub async fn expire_workspace_materialization(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        job_id: &neoengram_domain::protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let mut job = self.load(tenant_id, job_id).await?;
        if job.operation != JobOperation::WorkspaceMaterialize {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "Job is not a WorkspaceMaterialize operation",
            ));
        }
        if job.state == JobState::TimedOut {
            self.sync_control_job_task(&job, None).await?;
            return Ok(job);
        }
        if job.spec.deadline_unix_ms.get() > self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "cannot expire WorkspaceMaterialize before its deadline",
            ));
        }
        let spec = job.workspace_spec.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "WorkspaceMaterialize Job lost its immutable spec",
            )
        })?;
        self.catalog
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "ControlPlane has no control catalog for materialization state",
                )
            })?
            .transition_workspace_state(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.workspace_id,
                crate::WorkspaceState::Creating,
                crate::WorkspaceState::Abnormal,
                self.clock.now(),
            )
            .await?;
        let previous = job.resource_version.get();
        job.state = JobState::TimedOut;
        job = self.replace(previous, job).await?;
        self.sync_control_job_task(&job, None).await?;
        if let Some(assignment) = &job.workspace_assignment {
            let _ = self
                .outbox
                .retire(tenant_id, &assignment.assignment_id)
                .await?;
        }
        Ok(job)
    }

    /// Converges the Job side of the lifecycle publication after a crash between the catalog
    /// lifecycle CAS and the Job CAS. No success or failure is fabricated while the Workspace
    /// remains Creating.
    pub async fn recover_workspace_materialization(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        job_id: &neoengram_domain::protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let mut job = self.load(tenant_id, job_id).await?;
        if job.operation != JobOperation::WorkspaceMaterialize || job.state.is_terminal() {
            return Ok(job);
        }
        let spec = job.workspace_spec.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "WorkspaceMaterialize Job lost its immutable spec",
            )
        })?;
        let workspace = self
            .catalog
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "ControlPlane has no control catalog for materialization state",
                )
            })?
            .get_workspace(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.workspace_id,
            )
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::JobNotFound,
                    "materialization Workspace no longer exists",
                )
            })?;
        let recovered_state = match workspace.state {
            crate::WorkspaceState::Creating => return Ok(job),
            crate::WorkspaceState::Ready => JobState::Succeeded,
            crate::WorkspaceState::Abnormal => JobState::RecoveryRequired,
        };
        let previous = job.resource_version.get();
        job.state = recovered_state;
        job = self.replace(previous, job).await?;
        self.sync_control_job_task(&job, None).await?;
        if let Some(assignment) = &job.workspace_assignment {
            let _ = self
                .outbox
                .retire(tenant_id, &assignment.assignment_id)
                .await?;
        }
        Ok(job)
    }

    /// Returns the authoritative Job only when its persisted scope is visible to the actor.
    pub async fn query_job(&self, request: QueryJobRequest) -> CentralResult<QueryJobResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        if let Err(error) = self
            .authorize(Actor::Principal(request.actor), Action::QueryJob, &job.spec)
            .await
        {
            if error.code() == CentralErrorCode::Unauthorized {
                return Err(job_not_found(&request.job_id));
            }
            return Err(error);
        }
        Ok(QueryJobResult { job })
    }

    /// Reserves its delivery identity, persists the assignment, then exposes it in the outbox.
    pub async fn assign_job(&self, request: AssignJobRequest) -> CentralResult<AssignJobResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Principal(request.actor),
            Action::AssignJob,
            &job.spec,
        )
        .await?;
        let assignment = neoengram_domain::protocol::AddAssignment {
            job_id: job.spec.job_id.clone(),
            task_fence: operation_task_fence(
                self.task_coordinator.as_deref(),
                job.spec.operation_task_id.as_ref(),
                &job.spec.tenant_id,
                job.spec.job_id.as_str(),
                1,
                "scan_changes",
            )
            .await?,
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: job.spec.principal.clone(),
            tenant_id: job.spec.tenant_id.clone(),
            project_id: job.spec.project_id.clone(),
            artifact_id: job.spec.artifact_id.clone(),
            workspace_id: job.spec.workspace_id.clone(),
            edge_cluster_id: request.target.edge_cluster_id.clone(),
            storage_volume_id: request.target.storage_volume_id.clone(),
            artifact_placement_id: request.target.artifact_placement_id.clone(),
            placement_generation: request.target.placement_generation,
            agent_mount_id: request.target.agent_mount_id.clone(),
            mount_generation: request.target.mount_generation,
            owner_generation: request.target.owner_generation,
            expected_index_version: job.spec.expected_index_version.clone(),
            data_layout: job.spec.data_layout,
            max_whole_file_bytes: request.target.max_whole_file_bytes,
            lease: request.target.lease.clone(),
            request_digest: job.spec.request_digest,
            deadline_unix_ms: job.spec.deadline_unix_ms,
            paths: job.spec.paths.clone(),
            all: job.spec.all,
            extensions: job.spec.extensions.clone(),
        };
        let envelope = JobAssignment {
            assignment: AssignmentOperation::Add {
                input: assignment.clone(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };

        if let Some(existing) = &job.assignment {
            if existing != &assignment {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    "job already has a different persisted assignment",
                ));
            }
            let _ = self.outbox.reserve(envelope.clone()).await?;
            let _ = self.outbox.publish(envelope.clone()).await?;
            self.audit(&job, AuditKind::AssignmentQueued, "assignment")
                .await?;
            return Ok(AssignJobResult {
                job,
                assignment: envelope,
                replayed: true,
            });
        }
        validate_assignment_target(&request.target, self.clock.now().get())?;
        if job.spec.deadline_unix_ms.get() <= self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "managed Add deadline elapsed before assignment",
            ));
        }
        if job.state != JobState::Queued {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot assign a job in state {:?}", job.state),
            ));
        }

        // Reserve first so a tenant-scoped AssignmentId conflict cannot mutate the job. The
        // reservation remains delivery-invisible until the authoritative assignment is durable.
        let _ = self.outbox.reserve(envelope.clone()).await?;
        let previous = job.resource_version.get();
        job.assignment = Some(assignment.clone());
        job.state = JobState::Assigned;
        job = match self.replace(previous, job).await {
            Ok(job) => job,
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                let persisted = self.load(&request.tenant_id, &request.job_id).await?;
                if persisted.assignment.as_ref() != Some(&assignment) {
                    return Err(error);
                }
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.publish(envelope.clone()).await?;
                self.audit(&persisted, AuditKind::AssignmentQueued, "assignment")
                    .await?;
                return Ok(AssignJobResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };

        let _ = self.outbox.publish(envelope.clone()).await?;
        self.audit(&job, AuditKind::AssignmentQueued, "assignment")
            .await?;
        Ok(AssignJobResult {
            job,
            assignment: envelope,
            replayed: false,
        })
    }

    /// Applies one idempotent agent report to the persisted state machine.
    pub async fn receive_report(
        &self,
        request: ReceiveReportRequest,
    ) -> CentralResult<ReceiveReportResult> {
        Box::pin(self.receive_report_with_session(request, None)).await
    }

    /// Applies a report received over an authenticated Agent session.
    pub async fn receive_session_report(
        &self,
        request: ReceiveReportRequest,
        session_generation: SessionGeneration,
    ) -> CentralResult<ReceiveReportResult> {
        Box::pin(self.receive_report_with_session(request, Some(session_generation))).await
    }

    async fn receive_report_with_session(
        &self,
        request: ReceiveReportRequest,
        _session_generation: Option<SessionGeneration>,
    ) -> CentralResult<ReceiveReportResult> {
        let mut job = self
            .load(&request.tenant_id, request.report.job_id())
            .await?;
        if job.is_workspace_materialization() {
            return Box::pin(self.receive_workspace_materialization_report(job, request)).await;
        }
        if job.is_snapshot_delivery() {
            return Box::pin(self.receive_snapshot_delivery_report(job, request)).await;
        }
        self.authorize(
            Actor::Agent(request.agent_id.clone()),
            Action::ReceiveReport,
            &job.spec,
        )
        .await?;
        let assignment = job.assignment.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "cannot receive an agent report before assignment",
            )
        })?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "reporting agent does not own the persisted assignment",
            ));
        }

        let assignment_id = assignment.assignment_id.clone();
        let previous = job.resource_version.get();
        let replayed = match request.report {
            AgentReport::Accepted(report) => {
                report.validate()?;
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                if report.request_digest != assignment.request_digest {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "accepted report carries a different request digest",
                    ));
                }
                if let Some(existing) = &job.accepted {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::AssignmentMismatch,
                            "assignment was accepted with a different report payload",
                        ));
                    }
                } else {
                    if job.state != JobState::Assigned {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot accept a job in state {:?}", job.state),
                        ));
                    }
                    job.accepted = Some(report);
                    job.state = JobState::Accepted;
                    false
                }
            }
            AgentReport::Progress(report) => {
                report.validate()?;
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                if report.state != JobState::Running {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "progress reports must carry running state",
                    ));
                }
                if job.progress.as_ref() == Some(&report) {
                    true
                } else {
                    if !matches!(job.state, JobState::Accepted | JobState::Running) {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot apply progress in state {:?}", job.state),
                        ));
                    }
                    job.progress = Some(report);
                    job.state = JobState::Running;
                    false
                }
            }
            AgentReport::Prepared(report) => {
                validate_prepared(&job, &report)?;
                if let Some(existing) = &job.prepared {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::BatchTampered,
                            "job was prepared with different metadata descriptors",
                        ));
                    }
                } else {
                    if job.state != JobState::Running {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot prepare a job in state {:?}", job.state),
                        ));
                    }
                    job.prepared = Some(report);
                    job.state = JobState::Prepared;
                    false
                }
            }
            AgentReport::Finalized(report) => {
                report.validate()?;
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                let finalized = job.finalized.as_ref().ok_or_else(|| {
                    invalid(
                        CentralErrorCode::InvalidState,
                        "agent finalized before the central publish decision",
                    )
                })?;
                if report.decision_generation != finalized.decision_generation
                    || report.final_state != finalized.final_state
                {
                    return Err(invalid(
                        CentralErrorCode::GenerationMismatch,
                        "agent finalized acknowledgement differs from central decision",
                    ));
                }
                if let Some(existing) = &job.finalized_ack {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::AssignmentMismatch,
                            "agent replayed a different finalized acknowledgement",
                        ));
                    }
                } else {
                    job.finalized_ack = Some(report);
                    false
                }
            }
            AgentReport::Failed(report) => {
                report.validate()?;
                if report.tenant_id != request.tenant_id {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "job failure tenant does not match the report authority scope",
                    ));
                }
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                validate_terminal_state(report.final_state)?;
                if job.state == JobState::Publishing {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "cannot replace a publishing job with an agent terminal report",
                    ));
                }
                if let Some(existing) = &job.failure {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::AssignmentMismatch,
                            "agent replayed a different terminal report",
                        ));
                    }
                } else {
                    if job.state.is_terminal() {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            "job already has a different terminal outcome",
                        ));
                    }
                    let decision_generation = DecisionGeneration::new(1);
                    let decision = JobDecision {
                        job_id: assignment.job_id.clone(),
                        task_fence: assignment.task_fence.clone(),
                        assignment_id: assignment.assignment_id.clone(),
                        assignment_generation: assignment.assignment_generation,
                        decision_generation,
                        decision: PublishDecision::Reject {
                            error: report.error.clone(),
                            extensions: Extensions::new(),
                        },
                        final_state: report.final_state,
                        extensions: Extensions::new(),
                    };
                    let finalized = JobFinalized {
                        job_id: assignment.job_id.clone(),
                        task_fence: assignment.task_fence.clone(),
                        assignment_id: assignment.assignment_id.clone(),
                        assignment_generation: assignment.assignment_generation,
                        decision_generation,
                        final_state: report.final_state,
                        finalized_at_unix_ms: report.failed_at_unix_ms,
                        extensions: Extensions::new(),
                    };
                    decision.validate()?;
                    finalized.validate()?;
                    job.decision = Some(decision);
                    job.finalized = Some(finalized);
                    job.state = report.final_state;
                    job.failure = Some(report);
                    false
                }
            }
        };

        if !replayed {
            job = self.replace(previous, job).await?;
        }
        // Every valid report proves delivery. Accepted is the normal acknowledgement; retiring on
        // later reports also repairs a lost acknowledgement response without leaving a stale row.
        let _ = self
            .outbox
            .retire(&request.tenant_id, &assignment_id)
            .await?;
        self.audit(&job, AuditKind::ReportReceived, "report")
            .await?;
        Ok(ReceiveReportResult { job, replayed })
    }

    async fn receive_workspace_materialization_report(
        &self,
        mut job: JobRecord,
        request: ReceiveReportRequest,
    ) -> CentralResult<ReceiveReportResult> {
        let assignment = job.workspace_assignment.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "WorkspaceMaterialize Job has no persisted assignment",
            )
        })?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "reporting Agent does not own the WorkspaceMaterialize assignment",
            ));
        }
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "ControlPlane has no control catalog for materialization state",
            )
        })?;
        let assignment_id = assignment.assignment_id.clone();
        let mut replayed = false;
        match request.report {
            AgentReport::Accepted(report) => {
                report.validate()?;
                validate_workspace_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                if report.request_digest != assignment.request_digest {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "accepted materialization report carries a different request digest",
                    ));
                }
                // A restarted materializer no longer has the acknowledged report in its local
                // outbox. Identity and request_digest are the immutable acceptance facts; a new
                // observation timestamp is therefore a semantic replay, not a conflicting claim.
                if job.accepted.is_some() {
                    replayed = true;
                } else {
                    if job.state != JobState::Assigned {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot accept materialization Job in state {:?}", job.state),
                        ));
                    }
                    let previous = job.resource_version.get();
                    job.accepted = Some(report);
                    job.state = JobState::Accepted;
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Progress(report) => {
                report.validate()?;
                validate_workspace_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                if job.progress.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    match report.state {
                        JobState::Running => {
                            if !matches!(job.state, JobState::Accepted | JobState::Running) {
                                return Err(invalid(
                                    CentralErrorCode::InvalidState,
                                    format!(
                                        "cannot apply materialization progress in state {:?}",
                                        job.state
                                    ),
                                ));
                            }
                        }
                        JobState::Succeeded => {
                            if !matches!(
                                job.state,
                                JobState::Accepted | JobState::Running | JobState::Succeeded
                            ) {
                                return Err(invalid(
                                    CentralErrorCode::InvalidState,
                                    format!(
                                        "cannot complete materialization Job in state {:?}",
                                        job.state
                                    ),
                                ));
                            }
                            if let Some(base_commit_id) = assignment.base_commit_id {
                                let placement = self.placement.as_ref().ok_or_else(|| {
                                    invalid(
                                        CentralErrorCode::InvalidState,
                                        "v2 placement authority is required before Workspace becomes Ready",
                                    )
                                })?;
                                if !target_volume_coverage_complete(
                                    placement.as_ref(),
                                    &assignment.tenant_id,
                                    &assignment.artifact_id,
                                    base_commit_id,
                                    &assignment.storage_volume_id,
                                )
                                .await?
                                {
                                    return Err(invalid(
                                        CentralErrorCode::InvalidState,
                                        "Workspace materialization succeeded without complete v2 target Coverage",
                                    ));
                                }
                            }
                            catalog
                                .transition_workspace_state(
                                    &assignment.tenant_id,
                                    &assignment.project_id,
                                    &assignment.artifact_id,
                                    &assignment.workspace_id,
                                    crate::WorkspaceState::Creating,
                                    crate::WorkspaceState::Ready,
                                    self.clock.now(),
                                )
                                .await?;
                        }
                        state => {
                            return Err(invalid(
                                CentralErrorCode::InvalidState,
                                format!(
                                    "WorkspaceMaterialize progress cannot carry state {state:?}"
                                ),
                            ));
                        }
                    }
                    let previous = job.resource_version.get();
                    job.state = report.state;
                    job.progress = Some(report);
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Failed(report) => {
                report.validate()?;
                validate_workspace_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                validate_terminal_state(report.final_state)?;
                if job.failure.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    if job.state.is_terminal() && job.state != JobState::RecoveryRequired {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            "materialization Job already has another terminal outcome",
                        ));
                    }
                    catalog
                        .transition_workspace_state(
                            &assignment.tenant_id,
                            &assignment.project_id,
                            &assignment.artifact_id,
                            &assignment.workspace_id,
                            crate::WorkspaceState::Creating,
                            crate::WorkspaceState::Abnormal,
                            self.clock.now(),
                        )
                        .await?;
                    let previous = job.resource_version.get();
                    job.state = report.final_state;
                    job.failure = Some(report);
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Prepared(_) | AgentReport::Finalized(_) => {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "WorkspaceMaterialize does not publish metadata or await a decision",
                ));
            }
        }
        if job.state.is_terminal() {
            let _ = self
                .outbox
                .retire(&request.tenant_id, &assignment_id)
                .await?;
        }
        let task_issue = job.failure.as_ref().map(|failure| TaskIssue {
            code: failure.error.code.as_str().to_owned(),
            message: failure.error.message.clone(),
            retryable: failure.error.retryable,
            detail: None,
        });
        self.sync_control_job_task(&job, task_issue).await?;
        self.audit(&job, AuditKind::ReportReceived, "materialize-report")
            .await?;
        Ok(ReceiveReportResult { job, replayed })
    }

    #[allow(dead_code)]
    async fn receive_snapshot_delivery_report(
        &self,
        mut job: JobRecord,
        request: ReceiveReportRequest,
    ) -> CentralResult<ReceiveReportResult> {
        let assignment = job.delivery_assignment.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "SnapshotDelivery Job has no persisted assignment",
            )
        })?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "reporting Agent does not own the SnapshotDelivery assignment",
            ));
        }
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "ControlPlane has no control catalog for SnapshotDelivery state",
            )
        })?;
        let mut delivery = catalog
            .get_snapshot_delivery(&assignment.tenant_id, &assignment.delivery_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::JobNotFound,
                    "SnapshotDelivery no longer exists",
                )
            })?;
        if delivery.delivery_generation != assignment.delivery_generation {
            return Err(invalid(
                CentralErrorCode::GenerationMismatch,
                "SnapshotDelivery report belongs to a stale delivery generation",
            ));
        }
        let assignment_id = assignment.assignment_id.clone();
        let mut replayed = false;
        match request.report {
            AgentReport::Accepted(report) => {
                report.validate()?;
                validate_delivery_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                if report.request_digest != assignment.request_digest {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "accepted SnapshotDelivery report carries a different request digest",
                    ));
                }
                if job.accepted.is_some() {
                    replayed = true;
                } else {
                    if job.state != JobState::Assigned {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!(
                                "cannot accept SnapshotDelivery Job in state {:?}",
                                job.state
                            ),
                        ));
                    }
                    let previous = job.resource_version.get();
                    job.accepted = Some(report);
                    job.state = JobState::Accepted;
                    // Accepted means the Agent has fenced the assignment and is resolving the
                    // frozen Index/Manifest snapshot. Expose that durable phase rather than
                    // leaving an already-dispatched Delivery indistinguishable from requested.
                    if delivery.state == SnapshotDeliveryState::Requested {
                        delivery.state = SnapshotDeliveryState::Validating;
                        delivery.updated_at_unix_ms = self.clock.now();
                        let expected = delivery.resource_version;
                        delivery.resource_version = expected.saturating_add(1);
                        delivery = catalog
                            .replace_snapshot_delivery(expected, delivery)
                            .await?;
                    }
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Progress(report) => {
                report.validate()?;
                validate_delivery_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                if job.progress.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    match report.state {
                        JobState::Running => {
                            if !matches!(job.state, JobState::Accepted | JobState::Running) {
                                return Err(invalid(
                                    CentralErrorCode::InvalidState,
                                    format!(
                                        "cannot apply SnapshotDelivery progress in state {:?}",
                                        job.state
                                    ),
                                ));
                            }
                            if matches!(
                                delivery.state,
                                SnapshotDeliveryState::Requested
                                    | SnapshotDeliveryState::Validating
                            ) {
                                delivery.state = SnapshotDeliveryState::Materializing;
                                delivery.updated_at_unix_ms = self.clock.now();
                                let expected = delivery.resource_version;
                                delivery.resource_version = expected.saturating_add(1);
                                delivery = catalog
                                    .replace_snapshot_delivery(expected, delivery)
                                    .await?;
                            }
                        }
                        JobState::Succeeded => {
                            if !matches!(
                                job.state,
                                JobState::Accepted | JobState::Running | JobState::Succeeded
                            ) {
                                return Err(invalid(
                                    CentralErrorCode::InvalidState,
                                    format!(
                                        "cannot complete SnapshotDelivery Job in state {:?}",
                                        job.state
                                    ),
                                ));
                            }
                            if assignment.action
                                == neoengram_domain::protocol::SnapshotDeliveryAction::Delete
                            {
                                delivery.state = SnapshotDeliveryState::Deleted;
                            } else {
                                let placement = self.placement.as_ref().ok_or_else(|| {
                                    invalid(
                                        CentralErrorCode::InvalidState,
                                        "v2 placement authority is required before SnapshotDelivery becomes Ready",
                                    )
                                })?;
                                if !target_volume_coverage_complete(
                                    placement.as_ref(),
                                    &assignment.tenant_id,
                                    &assignment.artifact_id,
                                    assignment.commit_id,
                                    &assignment.storage_volume_id,
                                )
                                .await?
                                {
                                    return Err(invalid(
                                        CentralErrorCode::InvalidState,
                                        "SnapshotDelivery succeeded without complete v2 target Coverage",
                                    ));
                                }
                                delivery.state = SnapshotDeliveryState::Ready;
                                delivery.file_count = report.files_completed.get();
                                delivery.size_bytes = report.bytes_completed.get();
                            }
                            delivery.issue_code = None;
                            delivery.issue_message = None;
                            delivery.issue_retryable = false;
                            delivery.updated_at_unix_ms = self.clock.now();
                            let expected = delivery.resource_version;
                            delivery.resource_version = expected.saturating_add(1);
                            delivery = catalog
                                .replace_snapshot_delivery(expected, delivery)
                                .await?;
                            if assignment.action
                                == neoengram_domain::protocol::SnapshotDeliveryAction::Materialize
                            {
                                let snapshot = catalog
                                    .get_snapshot(&assignment.tenant_id, &assignment.snapshot_id)
                                    .await?;
                                if let Some(snapshot) = snapshot {
                                    if snapshot.delivery_id == assignment.delivery_id
                                        && snapshot.state == crate::SnapshotState::Creating
                                    {
                                        let _ = catalog
                                            .transition_snapshot_state(
                                                &assignment.tenant_id,
                                                &assignment.snapshot_id,
                                                crate::SnapshotState::Creating,
                                                crate::SnapshotState::Ready,
                                                self.clock.now(),
                                            )
                                            .await?;
                                    }
                                }
                            }
                        }
                        state => {
                            return Err(invalid(
                                CentralErrorCode::InvalidState,
                                format!("SnapshotDelivery progress cannot carry state {state:?}"),
                            ));
                        }
                    }
                    let previous = job.resource_version.get();
                    job.state = report.state;
                    job.progress = Some(report);
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Failed(report) => {
                report.validate()?;
                validate_delivery_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                    &report.task_fence,
                )?;
                validate_terminal_state(report.final_state)?;
                if job.failure.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    if job.state.is_terminal() && job.state != JobState::RecoveryRequired {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            "SnapshotDelivery Job already has another terminal outcome",
                        ));
                    }
                    delivery.state = SnapshotDeliveryState::Failed;
                    delivery.issue_code = Some(report.error.code.as_str().to_owned());
                    delivery.issue_message = Some(report.error.message.clone());
                    delivery.issue_retryable = report.error.retryable;
                    delivery.updated_at_unix_ms = self.clock.now();
                    let expected = delivery.resource_version;
                    delivery.resource_version = expected.saturating_add(1);
                    delivery = catalog
                        .replace_snapshot_delivery(expected, delivery)
                        .await?;
                    let snapshot = catalog
                        .get_snapshot(&assignment.tenant_id, &assignment.snapshot_id)
                        .await?;
                    if let Some(snapshot) = snapshot {
                        if snapshot.delivery_id == assignment.delivery_id
                            && snapshot.state == crate::SnapshotState::Creating
                        {
                            let _ = catalog
                                .transition_snapshot_state(
                                    &assignment.tenant_id,
                                    &assignment.snapshot_id,
                                    crate::SnapshotState::Creating,
                                    crate::SnapshotState::Abnormal,
                                    self.clock.now(),
                                )
                                .await?;
                        }
                    }
                    let previous = job.resource_version.get();
                    job.state = report.final_state;
                    job.failure = Some(report);
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Prepared(_) | AgentReport::Finalized(_) => {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "SnapshotDelivery does not publish metadata or await a decision",
                ));
            }
        }
        if job.state.is_terminal() {
            let _ = self
                .outbox
                .retire(&request.tenant_id, &assignment_id)
                .await?;
        }
        let task_issue = job.failure.as_ref().map(|failure| TaskIssue {
            code: failure.error.code.as_str().to_owned(),
            message: failure.error.message.clone(),
            retryable: failure.error.retryable,
            detail: None,
        });
        self.sync_control_job_task(&job, task_issue).await?;
        self.audit(&job, AuditKind::ReportReceived, "snapshot-delivery-report")
            .await?;
        let _ = delivery;
        Ok(ReceiveReportResult { job, replayed })
    }

    /// Stages an exact prepared descriptor or page after validating assignment and batch scope.
    pub async fn stage_metadata_batch(
        &self,
        request: StageMetadataBatchRequest,
    ) -> CentralResult<StageMetadataBatchResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Agent(request.agent_id.clone()),
            Action::StageMetadataBatch,
            &job.spec,
        )
        .await?;
        if !matches!(
            job.state,
            JobState::Prepared | JobState::Publishing | JobState::Succeeded | JobState::Conflicted
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot stage metadata in state {:?}", job.state),
            ));
        }
        let assignment = job
            .assignment
            .as_ref()
            .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "job has no assignment"))?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "metadata uploader does not own the assignment",
            ));
        }
        let prepared = job
            .prepared
            .as_ref()
            .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "job has no prepared report"))?;
        let batch_id = request.submission.batch_id().clone();
        let declared = prepared
            .metadata_batches
            .iter()
            .find(|descriptor| descriptor.batch_id == batch_id)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::BatchUndeclared,
                    format!("metadata batch {batch_id} was not declared by JobPrepared"),
                )
            })?;
        validate_descriptor_scope(assignment, declared)?;
        let audit_suffix = match &request.submission {
            MetadataBatchSubmission::Descriptor(_) => format!("batch-{batch_id}-descriptor"),
            MetadataBatchSubmission::Page(page) => {
                format!("batch-{batch_id}-page-{}", page.page_number)
            }
        };
        let replayed = match request.submission {
            MetadataBatchSubmission::Descriptor(descriptor) => {
                if descriptor != *declared {
                    return Err(invalid(
                        CentralErrorCode::BatchTampered,
                        "staged descriptor differs from JobPrepared",
                    ));
                }
                self.metadata.stage_descriptor(descriptor).await?
            }
            MetadataBatchSubmission::Page(page) => {
                declared.validate_page(&page)?;
                self.metadata.stage_page(declared, page).await?
            }
        };
        let complete = self
            .metadata
            .get(&assignment.tenant_id, &batch_id)
            .await?
            .is_some_and(|batch| batch.is_complete());
        self.audit(&job, AuditKind::MetadataStaged, &audit_suffix)
            .await?;
        Ok(StageMetadataBatchResult {
            batch_id,
            complete,
            replayed,
        })
    }

    /// Atomically marks an elapsed managed Add as TimedOut and creates a terminal decision when
    /// the job already has an assignment.
    pub async fn expire_add_job(
        &self,
        request: ExpireAddJobRequest,
    ) -> CentralResult<ExpireAddJobResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Principal(request.actor),
            Action::ExpireAddJob,
            &job.spec,
        )
        .await?;

        if job.state == JobState::TimedOut {
            let decision = job.decision.clone();
            let finalized = job.finalized.clone();
            match (&job.assignment, &decision, &finalized) {
                (None, None, None) => {}
                (Some(assignment), Some(decision), Some(finalized)) => {
                    decision.validate()?;
                    finalized.validate()?;
                    validate_report_identity(
                        assignment,
                        &decision.job_id,
                        &decision.assignment_id,
                        decision.assignment_generation,
                        &decision.task_fence,
                    )?;
                    if !matches!(decision.decision, PublishDecision::Reject { .. })
                        || decision.final_state != JobState::TimedOut
                        || finalized.job_id != decision.job_id
                        || finalized.assignment_id != decision.assignment_id
                        || finalized.assignment_generation != decision.assignment_generation
                        || finalized.decision_generation != decision.decision_generation
                        || finalized.final_state != JobState::TimedOut
                    {
                        return Err(invalid(
                            CentralErrorCode::Internal,
                            "timed-out job has an inconsistent durable decision",
                        ));
                    }
                }
                _ => {
                    return Err(invalid(
                        CentralErrorCode::Internal,
                        "timed-out job has inconsistent assignment or decision state",
                    ));
                }
            }
            if let Some(assignment) = &job.assignment {
                let _ = self
                    .outbox
                    .retire(&request.tenant_id, &assignment.assignment_id)
                    .await?;
            }
            self.audit(&job, AuditKind::AddExpired, "expire").await?;
            return Ok(ExpireAddJobResult {
                job,
                decision,
                finalized,
                replayed: true,
            });
        }

        if !matches!(
            job.state,
            JobState::Queued
                | JobState::Assigned
                | JobState::Accepted
                | JobState::Running
                | JobState::Prepared
                | JobState::CancelRequested
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot expire managed Add in state {:?}", job.state),
            ));
        }
        let now = self.clock.now();
        if job.spec.deadline_unix_ms.get() > now.get() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "cannot expire managed Add before its deadline",
            ));
        }
        if job.decision.is_some() || job.finalized.is_some() {
            return Err(invalid(
                CentralErrorCode::Internal,
                "non-terminal job already has a publish decision",
            ));
        }

        let (decision, finalized) = match &job.assignment {
            Some(assignment) if job.state != JobState::Queued => {
                let decision_generation = DecisionGeneration::new(1);
                let decision = JobDecision {
                    job_id: assignment.job_id.clone(),
                    task_fence: assignment.task_fence.clone(),
                    assignment_id: assignment.assignment_id.clone(),
                    assignment_generation: assignment.assignment_generation,
                    decision_generation,
                    decision: PublishDecision::Reject {
                        error: ControlError {
                            code: ErrorCode::new(CentralErrorCode::DeadlineExceeded.as_str())?,
                            message: "managed Add deadline elapsed before publication".to_owned(),
                            retryable: false,
                            retry_after_ms: None,
                            extensions: Extensions::new(),
                        },
                        extensions: Extensions::new(),
                    },
                    final_state: JobState::TimedOut,
                    extensions: Extensions::new(),
                };
                let finalized = JobFinalized {
                    job_id: assignment.job_id.clone(),
                    task_fence: assignment.task_fence.clone(),
                    assignment_id: assignment.assignment_id.clone(),
                    assignment_generation: assignment.assignment_generation,
                    decision_generation,
                    final_state: JobState::TimedOut,
                    finalized_at_unix_ms: now,
                    extensions: Extensions::new(),
                };
                (Some(decision), Some(finalized))
            }
            None if job.state == JobState::Queued => (None, None),
            _ => {
                return Err(invalid(
                    CentralErrorCode::Internal,
                    "job assignment is inconsistent with its pre-timeout state",
                ));
            }
        };

        let previous = job.resource_version.get();
        if let Some(decision) = &decision {
            decision.validate()?;
        }
        if let Some(finalized) = &finalized {
            finalized.validate()?;
        }
        job.state = JobState::TimedOut;
        job.decision.clone_from(&decision);
        job.finalized.clone_from(&finalized);
        job = self.replace(previous, job).await?;
        if let Some(assignment) = &job.assignment {
            let _ = self
                .outbox
                .retire(&request.tenant_id, &assignment.assignment_id)
                .await?;
        }
        self.audit(&job, AuditKind::AddExpired, "expire").await?;
        Ok(ExpireAddJobResult {
            job,
            decision,
            finalized,
            replayed: false,
        })
    }

    /// Validates complete staged metadata and assigned-Volume placement evidence, then performs
    /// one CAS.
    pub async fn finalize_add(
        &self,
        request: FinalizeAddRequest,
    ) -> CentralResult<FinalizeAddResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Principal(request.actor),
            Action::FinalizeAdd,
            &job.spec,
        )
        .await?;
        self.finalize_loaded(job).await
    }

    /// Finalizes a Prepared job under the server's recovery authority.
    ///
    /// Scheduler recovery must not depend on mutable user RBAC after job creation. Public callers
    /// continue to use [`Self::finalize_add`], which performs principal authorization.
    pub async fn finalize_prepared(
        &self,
        request: ResumePublicationRequest,
    ) -> CentralResult<FinalizeAddResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.state != JobState::Prepared {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot internally finalize job in state {:?}", job.state),
            ));
        }
        self.finalize_loaded(job).await
    }

    /// Resumes only a previously frozen Publishing job without re-entering mutable user policy.
    /// Transport adapters must keep this internal and expose [`Self::finalize_add`] to users.
    pub async fn resume_publication(
        &self,
        request: ResumePublicationRequest,
    ) -> CentralResult<FinalizeAddResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.state != JobState::Publishing {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot resume publication in state {:?}", job.state),
            ));
        }
        self.finalize_loaded(job).await
    }

    async fn finalize_loaded(&self, mut job: JobRecord) -> CentralResult<FinalizeAddResult> {
        let resumed_publication = job.state == JobState::Publishing;
        if let (Some(decision), Some(finalized)) = (&job.decision, &job.finalized) {
            if matches!(
                job.state,
                JobState::Succeeded | JobState::Conflicted | JobState::Failed
            ) {
                self.audit(&job, AuditKind::AddFinalized, "finalize")
                    .await?;
                return Ok(FinalizeAddResult {
                    job: job.clone(),
                    decision: decision.clone(),
                    finalized: finalized.clone(),
                    replayed: true,
                });
            }
        }
        if !matches!(job.state, JobState::Prepared | JobState::Publishing) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot finalize managed Add in state {:?}", job.state),
            ));
        }
        let assignment = job.assignment.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "publishing job lost its assignment",
            )
        })?;
        let publication_candidate = if resumed_publication {
            let candidate = job.publication_candidate.clone().ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    "publishing job lost its frozen publication candidate",
                )
            })?;
            validate_frozen_publication(&job, &candidate)?;
            candidate
        } else {
            if job.publication_candidate.is_some() {
                return Err(invalid(
                    CentralErrorCode::Internal,
                    "prepared job already contains a frozen publication candidate",
                ));
            }
            let metadata = validate_staged_metadata(&job, self.metadata.as_ref()).await?;
            for receipt in &metadata.placements {
                let evidence = crate::ObjectPlacementEvidence {
                    receipt: receipt.clone(),
                    placement_generation: assignment.placement_generation,
                };
                self.objects.record_placement(&evidence).await?;
                // Managed Add receipts are the first source of durable object evidence. Keep
                // them in the same namespace-scoped v2 Placement authority consumed by the
                // multi-source planner; the legacy ObjectCatalog remains as the Add recovery
                // ledger until its callers are retired. The Add data path currently stores raw
                // objects, matching the Commit ObjectSet builder's encoding contract.
                if let Some(placement) = &self.placement {
                    let object_namespace_id =
                        ObjectNamespaceId::from_artifact(&assignment.artifact_id);
                    let placement_digest = blake3::hash(
                        format!(
                            "managed-add-v2\0{}\0{}\0{}\0{}",
                            assignment.tenant_id,
                            assignment.artifact_id,
                            assignment.artifact_placement_id,
                            receipt.object_id
                        )
                        .as_bytes(),
                    );
                    let placement_id = neoengram_domain::protocol::PlacementId::new(format!(
                        "managed-add-v2-{}",
                        &placement_digest.to_hex()[..32]
                    ))
                    .map_err(|_| {
                        invalid(
                            CentralErrorCode::MetadataInvalid,
                            "managed Add generated an invalid v2 placement identity",
                        )
                    })?;
                    placement
                        .insert_object_placement_v2(
                            neoengram_domain::protocol::materialization::ObjectPlacement {
                                placement_id,
                                tenant_id: assignment.tenant_id.clone(),
                                object_namespace_id,
                                object_id: receipt.object_id,
                                size: receipt.size,
                                encoding: neoengram_domain::protocol::ObjectEncoding::Raw,
                                verified_digest: receipt.object_id.digest(),
                                storage_volume_id: Some(assignment.storage_volume_id.clone()),
                                archive_id: None,
                                placement_generation: assignment.placement_generation,
                                state: neoengram_domain::protocol::materialization::ObjectPlacementState::Verified,
                                failure_domain: format!(
                                    "volume:{}",
                                    assignment.storage_volume_id
                                ),
                            },
                        )
                        .await?;
                }
                let placed = self
                    .objects
                    .object_placement(
                        &assignment.tenant_id,
                        &assignment.artifact_id,
                        &assignment.storage_volume_id,
                        &assignment.artifact_placement_id,
                        assignment.placement_generation,
                        receipt.object_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::ObjectNotDurable,
                            format!(
                                "object {} has no evidence on the assigned Volume placement generation",
                                receipt.object_id
                            ),
                        )
                    })?;
                if placed.receipt.size != receipt.size {
                    return Err(invalid(
                        CentralErrorCode::ObjectNotDurable,
                        format!(
                            "object {} placement evidence differs from its declaration",
                            receipt.object_id
                        ),
                    ));
                }
            }
            // This is the last authority gate before Publishing becomes durable. Once that state
            // and its canonical candidate are persisted, recovery must converge a possibly
            // completed CAS without consulting mutable authority or transient staging again.
            let now = self.clock.now().get();
            if job.spec.deadline_unix_ms.get() <= now {
                return Err(invalid(
                    CentralErrorCode::DeadlineExceeded,
                    "managed Add deadline elapsed before publication",
                ));
            }
            if assignment
                .lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at_unix_ms.get() <= now)
            {
                return Err(invalid(
                    CentralErrorCode::DeadlineExceeded,
                    "managed Add assignment lease elapsed before publication",
                ));
            }

            let prepared = job.prepared.as_ref().ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    "prepared job lost its publication identity",
                )
            })?;
            let candidate = PublicationCandidate {
                expected_index_version: job.spec.expected_index_version.clone(),
                result_index_digest: prepared.result_index_digest,
                publication_digest: prepared.publication_digest,
                manifests: metadata.manifests,
                mutations: metadata.mutations,
            };
            let previous = job.resource_version.get();
            job.state = JobState::Publishing;
            job.publication_candidate = Some(candidate.clone());
            job = self.replace(previous, job).await?;
            candidate
        };

        let prepared_result_digest = publication_candidate.result_index_digest;
        let publish = self
            .publisher
            .compare_and_swap(IndexPublishRequest {
                job_key: job.key(),
                index_key: job.index_key(),
                expected_index_version: publication_candidate.expected_index_version,
                expected_result_digest: prepared_result_digest,
                manifests: publication_candidate.manifests,
                mutations: publication_candidate.mutations,
            })
            .await?;
        let decision_generation = DecisionGeneration::new(1);
        let (state, outcome) = match publish {
            IndexPublishOutcome::Published(version) => (
                JobState::Succeeded,
                PublishDecision::Publish {
                    published_index_version: version,
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Conflict(version) => (
                JobState::Conflicted,
                PublishDecision::Conflict {
                    current_index_version: version,
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Rejected(IndexPublishRejection::ResultDigestMismatch {
                expected_digest,
                observed_digest,
            }) => (
                JobState::Failed,
                PublishDecision::Reject {
                    error: ControlError {
                        code: ErrorCode::new("INDEX_RESULT_DIGEST_MISMATCH")?,
                        message: format!(
                            "published Index digest {observed_digest} differs from prepared result {expected_digest}"
                        ),
                        retryable: false,
                        retry_after_ms: None,
                        extensions: Extensions::new(),
                    },
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Rejected(IndexPublishRejection::InvalidMetadata {
                message,
            }) => (
                JobState::Failed,
                PublishDecision::Reject {
                    error: ControlError {
                        code: ErrorCode::new(CentralErrorCode::MetadataInvalid.as_str())?,
                        message: bounded_control_error_message(message),
                        retryable: false,
                        retry_after_ms: None,
                        extensions: Extensions::new(),
                    },
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Rejected(IndexPublishRejection::RevisionExhausted) => (
                JobState::Failed,
                PublishDecision::Reject {
                    error: ControlError {
                        code: ErrorCode::new("INDEX_REVISION_EXHAUSTED")?,
                        message: "Index revision reached its maximum value".to_owned(),
                        retryable: false,
                        retry_after_ms: None,
                        extensions: Extensions::new(),
                    },
                    extensions: Extensions::new(),
                },
            ),
        };
        let decision = JobDecision {
            job_id: assignment.job_id.clone(),
            task_fence: assignment.task_fence.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            decision_generation,
            decision: outcome,
            final_state: state,
            extensions: Extensions::new(),
        };
        let finalized = JobFinalized {
            job_id: assignment.job_id.clone(),
            task_fence: assignment.task_fence.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            decision_generation,
            final_state: state,
            finalized_at_unix_ms: self.clock.now(),
            extensions: Extensions::new(),
        };
        decision.validate()?;
        finalized.validate()?;
        let previous = job.resource_version.get();
        job.state = state;
        job.decision = Some(decision.clone());
        job.finalized = Some(finalized.clone());
        job = self.replace(previous, job).await?;
        self.audit(&job, AuditKind::AddFinalized, "finalize")
            .await?;
        Ok(FinalizeAddResult {
            job,
            decision,
            finalized,
            replayed: resumed_publication,
        })
    }

    async fn load(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        job_id: &neoengram_domain::protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let key = crate::JobKey::new(tenant_id.clone(), job_id.clone());
        self.jobs
            .get(&key)
            .await?
            .ok_or_else(|| job_not_found(job_id))
    }

    async fn replace(&self, expected: u64, mut job: JobRecord) -> CentralResult<JobRecord> {
        let next = expected
            .checked_add(1)
            .ok_or_else(|| invalid(CentralErrorCode::Internal, "job ResourceVersion overflow"))?;
        job.resource_version = ResourceVersion::new(next);
        self.jobs.replace(expected, job).await
    }

    async fn authorize(
        &self,
        actor: Actor,
        action: Action,
        spec: &AddJobSpec,
    ) -> CentralResult<()> {
        self.authorizer
            .authorize(&AuthorizationRequest {
                actor,
                action,
                tenant_id: spec.tenant_id.clone(),
                artifact_id: spec.artifact_id.clone(),
                workspace_id: spec.workspace_id.clone(),
                job_id: spec.job_id.clone(),
            })
            .await
    }

    async fn audit(&self, job: &JobRecord, kind: AuditKind, suffix: &str) -> CentralResult<()> {
        let event = AuditEvent {
            event_id: format!(
                "{}:{}:{}:{suffix}",
                job.spec.tenant_id, job.spec.job_id, job.resource_version
            ),
            kind,
            job_key: job.key(),
            state: job.state,
            occurred_at_unix_ms: self.clock.now(),
        };
        let _ = self.audit.record(event).await?;
        Ok(())
    }
}

fn control_job_root_state(state: JobState) -> TaskState {
    match state {
        JobState::Queued
        | JobState::Assigned
        | JobState::Accepted
        | JobState::Running
        | JobState::Prepared
        | JobState::Publishing
        | JobState::Conflicted
        | JobState::Unknown => TaskState::Running,
        JobState::Succeeded => TaskState::Succeeded,
        JobState::RecoveryRequired => TaskState::Stalled,
        JobState::Failed | JobState::Rejected | JobState::TimedOut => TaskState::Failed,
        JobState::CancelRequested | JobState::Cancelled => TaskState::Cancelled,
    }
}

fn control_job_actor() -> TaskActor {
    TaskActor::Principal(PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new("control-job-projector")
            .expect("static control Job projector principal is valid"),
        extensions: Extensions::new(),
    })
}

/// Drives one stage through the legal state-machine path to a desired projection. Every read is
/// followed by a fenced transition so concurrent Agent/recovery reports remain idempotent.
async fn drive_control_job_stage(
    coordinator: &TaskCoordinator,
    tenant_id: &TenantId,
    task_id: &TaskId,
    stage_key: &str,
    desired: StageState,
    issue: Option<TaskIssue>,
) -> CentralResult<()> {
    loop {
        let current = coordinator
            .repository()
            .stages(tenant_id, task_id)
            .await?
            .into_iter()
            .find(|stage| stage.stage_key == stage_key)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::ResourceNotFound,
                    format!("operation task stage {stage_key} not found"),
                )
            })?;
        if current.state == desired {
            return Ok(());
        }
        if current.state.is_success() {
            // A successful stage is an irreversible publication barrier for this attempt. A
            // stale Job observation may not regress it to a waiting/running projection.
            return Ok(());
        }
        let next = match desired {
            StageState::Succeeded => match current.state {
                StageState::Pending => StageState::Ready,
                StageState::Ready
                | StageState::Waiting
                | StageState::Verifying
                | StageState::Stalled => StageState::Running,
                StageState::Running => StageState::Succeeded,
                StageState::Failed | StageState::Cancelling | StageState::Cancelled => {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        format!(
                            "stage {stage_key} cannot reach succeeded from {:?}",
                            current.state
                        ),
                    ));
                }
                StageState::Skipped | StageState::NoOp | StageState::Succeeded => unreachable!(),
            },
            StageState::Running => match current.state {
                StageState::Pending => StageState::Ready,
                StageState::Ready
                | StageState::Waiting
                | StageState::Verifying
                | StageState::Stalled => StageState::Running,
                StageState::Running => return Ok(()),
                StageState::Failed | StageState::Cancelling | StageState::Cancelled => {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        format!(
                            "stage {stage_key} cannot reach running from {:?}",
                            current.state
                        ),
                    ));
                }
                StageState::Skipped | StageState::NoOp | StageState::Succeeded => return Ok(()),
            },
            StageState::Waiting | StageState::Verifying => match current.state {
                StageState::Pending => StageState::Ready,
                StageState::Ready
                | StageState::Stalled
                | StageState::Waiting
                | StageState::Verifying => StageState::Running,
                StageState::Running => desired,
                StageState::Failed | StageState::Cancelling | StageState::Cancelled => {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        format!(
                            "stage {stage_key} cannot reach {desired:?} from {:?}",
                            current.state
                        ),
                    ));
                }
                StageState::Skipped | StageState::NoOp | StageState::Succeeded => return Ok(()),
            },
            StageState::Stalled | StageState::Failed => match current.state {
                StageState::Pending => StageState::Ready,
                StageState::Ready => StageState::Running,
                StageState::Running | StageState::Waiting | StageState::Verifying => desired,
                StageState::Stalled if desired == StageState::Failed => StageState::Failed,
                StageState::Failed if desired == StageState::Stalled => StageState::Stalled,
                StageState::Stalled | StageState::Failed => return Ok(()),
                StageState::Cancelling | StageState::Cancelled => {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        format!(
                            "stage {stage_key} cannot reach {desired:?} from {:?}",
                            current.state
                        ),
                    ));
                }
                StageState::Skipped | StageState::NoOp | StageState::Succeeded => return Ok(()),
            },
            StageState::Cancelled => match current.state {
                StageState::Pending
                | StageState::Ready
                | StageState::Running
                | StageState::Waiting
                | StageState::Verifying
                | StageState::Stalled
                | StageState::Failed => StageState::Cancelling,
                StageState::Cancelling => StageState::Cancelled,
                StageState::Skipped | StageState::NoOp | StageState::Succeeded => return Ok(()),
                StageState::Cancelled => unreachable!(),
            },
            StageState::Pending
            | StageState::Ready
            | StageState::Skipped
            | StageState::NoOp
            | StageState::Cancelling => desired,
        };
        coordinator
            .transition_stage(task_id, tenant_id, stage_key, next, issue.clone())
            .await?;
    }
}

fn job_not_found(job_id: &neoengram_domain::protocol::JobId) -> crate::CentralError {
    invalid(
        CentralErrorCode::JobNotFound,
        format!("managed Add job {job_id} was not found"),
    )
}

fn validate_workspace_report_identity(
    assignment: &WorkspaceMaterializeAssignment,
    job_id: &neoengram_domain::protocol::JobId,
    assignment_id: &neoengram_domain::protocol::AssignmentId,
    generation: neoengram_domain::protocol::AssignmentGeneration,
    task_fence: &TaskExecutionFence,
) -> CentralResult<()> {
    if job_id != &assignment.job_id || assignment_id != &assignment.assignment_id {
        return Err(invalid(
            CentralErrorCode::AssignmentMismatch,
            "materialization report does not identify the persisted assignment",
        ));
    }
    if generation != assignment.assignment_generation {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "materialization report carries a stale assignment generation",
        ));
    }
    if task_fence != &assignment.task_fence {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "materialization report carries a stale task execution fence",
        ));
    }
    Ok(())
}

fn validate_delivery_report_identity(
    assignment: &SnapshotDeliveryAssignment,
    job_id: &neoengram_domain::protocol::JobId,
    assignment_id: &neoengram_domain::protocol::AssignmentId,
    generation: neoengram_domain::protocol::AssignmentGeneration,
    task_fence: &TaskExecutionFence,
) -> CentralResult<()> {
    if job_id != &assignment.job_id || assignment_id != &assignment.assignment_id {
        return Err(invalid(
            CentralErrorCode::AssignmentMismatch,
            "SnapshotDelivery report does not identify the persisted assignment",
        ));
    }
    if generation != assignment.assignment_generation {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "SnapshotDelivery report carries a stale assignment generation",
        ));
    }
    if task_fence != &assignment.task_fence {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "SnapshotDelivery report carries a stale task execution fence",
        ));
    }
    Ok(())
}

fn bounded_control_error_message(mut message: String) -> String {
    if message.trim().is_empty() {
        return "Index publication was rejected by the publisher".to_owned();
    }
    if message.len() <= CONTROL_ERROR_MESSAGE_LIMIT {
        return message;
    }

    const ELLIPSIS: &str = "...";
    let mut end = CONTROL_ERROR_MESSAGE_LIMIT - ELLIPSIS.len();
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message.push_str(ELLIPSIS);
    message
}

fn validate_frozen_publication(
    job: &JobRecord,
    candidate: &PublicationCandidate,
) -> CentralResult<()> {
    let prepared = job.prepared.as_ref().ok_or_else(|| {
        invalid(
            CentralErrorCode::Internal,
            "publishing job lost its prepared publication identity",
        )
    })?;
    if candidate.expected_index_version != job.spec.expected_index_version
        || candidate.result_index_digest != prepared.result_index_digest
        || candidate.publication_digest != prepared.publication_digest
    {
        return Err(invalid(
            CentralErrorCode::Internal,
            "frozen publication identity differs from the durable prepared report",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        action_envelope, bounded_control_error_message, execution_fence,
        materialization_assignment_message_id, materialization_ticket_window,
        reconnected_replication_report_matches_route, replication_delivery_can_wait_for_next_tick,
        target_volume_coverage_complete, ReplicationRouteGenerations, AGENT_JOB_ASSIGNMENT_ACTION,
        CONTROL_ERROR_MESSAGE_LIMIT, MAX_CENTRAL_COMMAND_TTL_MS,
    };
    use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
    use neoengram_domain::protocol::materialization::{ObjectPlacement, ObjectPlacementState};
    use neoengram_domain::protocol::{
        AgentId, ArtifactId, CommitObject, CommitObjectSet, ControlError, ControlMessage,
        DecimalU64, EdgeClusterId, ErrorCode, Extensions, GatewayPoolId, Generation, MessageId,
        MountGeneration, ObjectEncoding, ObjectNamespaceId, ObjectSet, PlacementGeneration,
        PlacementId, PlacementSetId, ReplicationId, ReplicationObjectState,
        ReplicationProgressReport, ReplicationState, RequestId, RouteGeneration, SessionGeneration,
        StorageVolumeId, TenantId, UnixMillis,
    };

    use crate::{
        CancelReplicationRequest, CentralError, CentralErrorCode, InMemoryComponents,
        InMemoryPlacementRepository, PlacementRepository, ReplicationObjectRecord,
        ReplicationRecord,
    };

    #[tokio::test]
    async fn target_coverage_requires_all_objects_on_one_volume() {
        let placement = Arc::new(InMemoryPlacementRepository::default());
        let tenant_id = TenantId::new("tenant-coverage-gate").unwrap();
        let artifact_id = ArtifactId::new("artifact-coverage-gate").unwrap();
        let namespace = ObjectNamespaceId::new(artifact_id.to_string()).unwrap();
        let volume_id = StorageVolumeId::new("volume-coverage-gate").unwrap();
        let commit_id = ContentDigest::from_bytes([31; 32]);
        let first_object = ObjectId::from_bytes([32; 32]);
        let second_object = ObjectId::from_bytes([33; 32]);
        let object_set = ObjectSet::new(vec![
            CommitObject::new(first_object, 4, ObjectEncoding::Raw, 0),
            CommitObject::new(second_object, 6, ObjectEncoding::Raw, 1),
        ])
        .unwrap();
        placement
            .insert_commit_object_set(CommitObjectSet {
                tenant_id: tenant_id.clone(),
                commit_id: CommitId::from_digest(commit_id),
                object_set,
            })
            .await
            .unwrap();

        let make_placement = |object_id, id| ObjectPlacement {
            placement_id: PlacementId::new(id).unwrap(),
            tenant_id: tenant_id.clone(),
            object_namespace_id: namespace.clone(),
            object_id,
            size: DecimalU64::new(if object_id == first_object { 4 } else { 6 }),
            encoding: ObjectEncoding::Raw,
            verified_digest: object_id.digest(),
            storage_volume_id: Some(volume_id.clone()),
            archive_id: None,
            placement_generation: PlacementGeneration::new(1),
            state: ObjectPlacementState::Verified,
            failure_domain: "host-coverage-gate".to_owned(),
        };
        placement
            .insert_object_placement_v2(make_placement(first_object, "placement-gate-first"))
            .await
            .unwrap();
        assert!(!target_volume_coverage_complete(
            placement.as_ref(),
            &tenant_id,
            &artifact_id,
            commit_id,
            &volume_id,
        )
        .await
        .unwrap());

        placement
            .insert_object_placement_v2(make_placement(second_object, "placement-gate-second"))
            .await
            .unwrap();
        assert!(target_volume_coverage_complete(
            placement.as_ref(),
            &tenant_id,
            &artifact_id,
            commit_id,
            &volume_id,
        )
        .await
        .unwrap());
    }

    #[test]
    fn only_temporary_replication_delivery_errors_leave_the_message_batch_usable() {
        for code in [
            CentralErrorCode::GatewayRouteUnavailable,
            CentralErrorCode::ConcurrentUpdate,
        ] {
            assert!(replication_delivery_can_wait_for_next_tick(
                &CentralError::new(code, "temporary delivery race")
            ));
        }
        assert!(replication_delivery_can_wait_for_next_tick(
            &CentralError::new(CentralErrorCode::ConcurrentUpdate, "SQLite route CAS lost")
                .with_retryable(false)
        ));
        for code in [
            CentralErrorCode::ProtocolInvalid,
            CentralErrorCode::InvalidState,
            CentralErrorCode::StorageFailure,
            CentralErrorCode::Internal,
        ] {
            assert!(!replication_delivery_can_wait_for_next_tick(
                &CentralError::new(code, "delivery must fail closed")
            ));
        }
    }

    #[test]
    fn materialization_ticket_window_matches_the_signed_expiry() {
        let now = UnixMillis::new(10_000);
        let long_batch_deadline = UnixMillis::new(
            now.get()
                .checked_add(MAX_CENTRAL_COMMAND_TTL_MS + 1_000)
                .unwrap(),
        );
        let (ticket_deadline, ttl_ms) =
            materialization_ticket_window(now, long_batch_deadline).unwrap();
        assert_eq!(ttl_ms, MAX_CENTRAL_COMMAND_TTL_MS);
        assert_eq!(ticket_deadline.get(), now.get() + ttl_ms);

        let short_batch_deadline = UnixMillis::new(now.get() + 1_000);
        let (ticket_deadline, ttl_ms) =
            materialization_ticket_window(now, short_batch_deadline).unwrap();
        assert_eq!(ttl_ms, 1_000);
        assert_eq!(ticket_deadline, short_batch_deadline);

        assert!(materialization_ticket_window(now, now).is_err());
    }

    #[test]
    fn materialization_assignment_message_id_is_bounded_and_replay_stable() {
        let materialization_id =
            neoengram_domain::protocol::MaterializationId::new("m".repeat(128)).unwrap();
        let batch_id =
            neoengram_domain::protocol::MaterializationBatchId::new("b".repeat(128)).unwrap();
        let first = materialization_assignment_message_id(
            &materialization_id,
            &batch_id,
            Generation::new(7),
            Generation::new(3),
        )
        .unwrap();
        let replay = materialization_assignment_message_id(
            &materialization_id,
            &batch_id,
            Generation::new(7),
            Generation::new(3),
        )
        .unwrap();
        let next_attempt = materialization_assignment_message_id(
            &materialization_id,
            &batch_id,
            Generation::new(7),
            Generation::new(4),
        )
        .unwrap();

        assert!(first.as_str().len() <= 128);
        assert_eq!(first, replay);
        assert_ne!(first, next_attempt);
    }

    #[test]
    fn reconnected_replication_report_accepts_only_an_advanced_live_route() {
        let stored_session = SessionGeneration::new(4);
        let stored_mount = MountGeneration::new(2);
        let stored_route = RouteGeneration::new(7);
        assert!(reconnected_replication_report_matches_route(
            stored_session,
            stored_mount,
            stored_route,
            SessionGeneration::new(5),
            ReplicationRouteGenerations {
                session: SessionGeneration::new(5),
                mount: stored_mount,
                route: RouteGeneration::new(8),
            },
        ));
        assert!(!reconnected_replication_report_matches_route(
            stored_session,
            stored_mount,
            stored_route,
            SessionGeneration::new(5),
            ReplicationRouteGenerations {
                session: SessionGeneration::new(5),
                mount: MountGeneration::new(3),
                route: RouteGeneration::new(8),
            },
        ));
        assert!(!reconnected_replication_report_matches_route(
            stored_session,
            stored_mount,
            stored_route,
            SessionGeneration::new(5),
            ReplicationRouteGenerations {
                session: SessionGeneration::new(5),
                mount: stored_mount,
                route: RouteGeneration::new(6),
            },
        ));
    }

    struct ReplicationFixture {
        tenant_id: TenantId,
        target_agent_id: AgentId,
        object_id: ObjectId,
        object_set: CommitObjectSet,
        replication: ReplicationRecord,
    }

    fn replication_fixture(state: ReplicationState, completed: bool) -> ReplicationFixture {
        let tenant_id = TenantId::new("tenant-cancelled-report").unwrap();
        let target_agent_id = AgentId::new("agent-cancelled-report").unwrap();
        let target_session_generation = SessionGeneration::new(7);
        let object_id = ObjectId::from_bytes([3; 32]);
        let commit_id = ContentDigest::from_bytes([4; 32]);
        let object_set = ObjectSet::new(vec![CommitObject::new(
            object_id,
            12,
            ObjectEncoding::Raw,
            0,
        )])
        .unwrap();
        let commit_object_set = CommitObjectSet {
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(commit_id),
            object_set,
        };
        let (completed_objects, completed_bytes) = if completed { (1, 12) } else { (0, 0) };
        let replication = ReplicationRecord {
            tenant_id: tenant_id.clone(),
            replication_id: ReplicationId::new("replication-cancelled-report").unwrap(),
            artifact_id: Some(ArtifactId::new("artifact-cancelled-report").unwrap()),
            commit_id,
            target_backend_id: "backend-cancelled-report".to_owned(),
            target_storage_volume_id: StorageVolumeId::new("volume-cancelled-report").unwrap(),
            source_placement_set_id: None,
            source_backend_id: None,
            source_storage_volume_id: None,
            source_edge_cluster_id: None,
            source_gateway_pool_id: None,
            source_placement_generation: None,
            source_agent_id: None,
            source_session_generation: None,
            source_mount_generation: None,
            source_route_generation: None,
            target_edge_cluster_id: Some(EdgeClusterId::new("cluster-cancelled-report").unwrap()),
            target_gateway_pool_id: Some(GatewayPoolId::new("pool-cancelled-report").unwrap()),
            target_placement_generation: Some(PlacementGeneration::new(1)),
            target_agent_id: Some(target_agent_id.clone()),
            target_session_generation: Some(target_session_generation),
            target_mount_generation: Some(MountGeneration::new(2)),
            target_route_generation: Some(RouteGeneration::new(3)),
            transfer_route_id: None,
            transfer_id: None,
            target_placement_set_id: Some(
                PlacementSetId::new("placement-set-cancelled-report").unwrap(),
            ),
            staging_id: Some("staging-cancelled-report".to_owned()),
            object_set_digest: commit_object_set.object_set.object_set_digest,
            state,
            request_id: RequestId::new("request-cancelled-report").unwrap(),
            attempt: 1,
            completed_objects,
            total_objects: 1,
            completed_bytes,
            total_bytes: 12,
            issue_code: None,
            issue_message: None,
            created_at_unix_ms: UnixMillis::new(10),
            updated_at_unix_ms: UnixMillis::new(10),
        };
        ReplicationFixture {
            tenant_id,
            target_agent_id,
            object_id,
            object_set: commit_object_set,
            replication,
        }
    }

    async fn insert_and_cancel(
        components: &InMemoryComponents,
        fixture: &ReplicationFixture,
    ) -> ReplicationRecord {
        components
            .placement
            .insert_commit_object_set(fixture.object_set.clone())
            .await
            .unwrap();
        components
            .placement
            .insert_replication(fixture.replication.clone())
            .await
            .unwrap();
        components
            .placement
            .cancel_replication(CancelReplicationRequest {
                tenant_id: fixture.tenant_id.clone(),
                replication_id: fixture.replication.replication_id.clone(),
                expected_attempt: fixture.replication.attempt,
                updated_at_unix_ms: UnixMillis::new(20),
            })
            .await
            .unwrap()
    }

    #[test]
    fn control_error_messages_are_bounded_on_utf8_boundaries() {
        let original = "界".repeat(CONTROL_ERROR_MESSAGE_LIMIT);
        let bounded = bounded_control_error_message(original);

        assert!(bounded.len() <= CONTROL_ERROR_MESSAGE_LIMIT);
        assert!(bounded.ends_with("..."));
        assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
        assert!(!bounded_control_error_message("   ".to_owned())
            .trim()
            .is_empty());
    }

    #[test]
    fn agent_delivery_uses_strict_action_envelope() {
        let envelope = action_envelope(
            AGENT_JOB_ASSIGNMENT_ACTION,
            MessageId::new("assignment-test").unwrap(),
            TenantId::new("tenant-test").unwrap(),
            SessionGeneration::new(1),
            UnixMillis::new(1_000),
            ControlMessage::Error(ControlError {
                code: ErrorCode::new("TEST").unwrap(),
                message: "test".to_owned(),
                retryable: false,
                retry_after_ms: None,
                extensions: Extensions::new(),
            }),
        )
        .unwrap();

        assert_eq!(envelope.header.action, AGENT_JOB_ASSIGNMENT_ACTION);
        assert_eq!(envelope.header.request_id.as_str(), "assignment-test");
        assert_eq!(envelope.header.trace_id.as_str(), "assignment-test");
        assert_eq!(
            envelope.header.session_generation,
            Some(SessionGeneration::new(1))
        );
    }

    #[tokio::test]
    async fn superseded_replication_report_is_acked_without_mutating_current_attempt() {
        let components = InMemoryComponents::new(30);
        let mut fixture = replication_fixture(ReplicationState::Queued, false);
        fixture.replication.attempt = 2;
        components
            .placement
            .insert_replication(fixture.replication.clone())
            .await
            .unwrap();
        let control = components.control_plane();

        let result = control
            .receive_replication_report(
                &fixture.tenant_id,
                &fixture.target_agent_id,
                SessionGeneration::new(8),
                ReplicationProgressReport::State {
                    replication_id: fixture.replication.replication_id.clone(),
                    tenant_id: fixture.tenant_id.clone(),
                    task_fence: execution_fence(
                        fixture.replication.replication_id.as_str(),
                        1,
                        "transfer",
                    )
                    .unwrap(),
                    attempt: 1,
                    state: ReplicationState::Failed,
                    completed_objects: 0,
                    completed_bytes: 0,
                    issue_code: Some("OLD_ATTEMPT_FAILED".to_owned()),
                    issue_message: Some("durable report from the previous attempt".to_owned()),
                    extensions: Extensions::new(),
                },
            )
            .await
            .unwrap();

        assert!(result.replayed);
        assert_eq!(
            components
                .placement
                .get_replication(&fixture.tenant_id, &fixture.replication.replication_id)
                .await
                .unwrap(),
            Some(fixture.replication)
        );
    }

    #[tokio::test]
    async fn future_replication_report_attempt_remains_fail_closed() {
        let components = InMemoryComponents::new(30);
        let fixture = replication_fixture(ReplicationState::Queued, false);
        components
            .placement
            .insert_replication(fixture.replication.clone())
            .await
            .unwrap();
        let control = components.control_plane();

        let error = control
            .receive_replication_report(
                &fixture.tenant_id,
                &fixture.target_agent_id,
                SessionGeneration::new(8),
                ReplicationProgressReport::State {
                    replication_id: fixture.replication.replication_id.clone(),
                    tenant_id: fixture.tenant_id.clone(),
                    task_fence: execution_fence(
                        fixture.replication.replication_id.as_str(),
                        2,
                        "transfer",
                    )
                    .unwrap(),
                    attempt: 2,
                    state: ReplicationState::Transferring,
                    completed_objects: 0,
                    completed_bytes: 0,
                    issue_code: None,
                    issue_message: None,
                    extensions: Extensions::new(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), CentralErrorCode::ConcurrentUpdate);
        assert_eq!(
            components
                .placement
                .get_replication(&fixture.tenant_id, &fixture.replication.replication_id)
                .await
                .unwrap(),
            Some(fixture.replication)
        );
    }

    #[tokio::test]
    async fn cancelled_replication_acks_queued_failure_without_changing_cancellation() {
        let components = InMemoryComponents::new(30);
        let fixture = replication_fixture(ReplicationState::Transferring, false);
        let cancelled = insert_and_cancel(&components, &fixture).await;
        let control = components.control_plane();

        let result = control
            .receive_replication_report(
                &fixture.tenant_id,
                &fixture.target_agent_id,
                SessionGeneration::new(8),
                ReplicationProgressReport::State {
                    replication_id: fixture.replication.replication_id.clone(),
                    tenant_id: fixture.tenant_id.clone(),
                    task_fence: execution_fence(
                        fixture.replication.replication_id.as_str(),
                        fixture.replication.attempt,
                        "transfer",
                    )
                    .unwrap(),
                    attempt: fixture.replication.attempt,
                    state: ReplicationState::Failed,
                    completed_objects: 0,
                    completed_bytes: 0,
                    issue_code: Some("SOURCE_UNAVAILABLE".to_owned()),
                    issue_message: Some("source disconnected before cancellation".to_owned()),
                    extensions: Extensions::new(),
                },
            )
            .await
            .unwrap();

        assert!(result.replayed);
        assert_eq!(
            components
                .placement
                .get_replication(&fixture.tenant_id, &fixture.replication.replication_id)
                .await
                .unwrap(),
            Some(cancelled)
        );
        assert!(components
            .placement
            .list_replication_objects(&fixture.tenant_id, &fixture.replication.replication_id)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn cancelled_replication_discards_stale_publication_without_publishing_placement() {
        let components = InMemoryComponents::new(30);
        let fixture = replication_fixture(ReplicationState::Verifying, true);
        components
            .placement
            .insert_commit_object_set(fixture.object_set.clone())
            .await
            .unwrap();
        components
            .placement
            .insert_replication(fixture.replication.clone())
            .await
            .unwrap();
        let checkpoint = ReplicationObjectRecord {
            tenant_id: fixture.tenant_id.clone(),
            replication_id: fixture.replication.replication_id.clone(),
            object_id: fixture.object_id,
            offset: 12,
            state: ReplicationObjectState::Verified,
            retry_count: fixture.replication.attempt,
            updated_at_unix_ms: UnixMillis::new(11),
        };
        components
            .placement
            .upsert_replication_object(checkpoint.clone())
            .await
            .unwrap();
        let cancelled = components
            .placement
            .cancel_replication(CancelReplicationRequest {
                tenant_id: fixture.tenant_id.clone(),
                replication_id: fixture.replication.replication_id.clone(),
                expected_attempt: fixture.replication.attempt,
                updated_at_unix_ms: UnixMillis::new(20),
            })
            .await
            .unwrap();
        let control = components.control_plane();

        let result = control
            .receive_replication_report(
                &fixture.tenant_id,
                &fixture.target_agent_id,
                SessionGeneration::new(8),
                ReplicationProgressReport::Published {
                    replication_id: fixture.replication.replication_id.clone(),
                    tenant_id: fixture.tenant_id.clone(),
                    task_fence: execution_fence(
                        fixture.replication.replication_id.as_str(),
                        fixture.replication.attempt,
                        "transfer",
                    )
                    .unwrap(),
                    attempt: fixture.replication.attempt,
                    commit_id: fixture.object_set.commit_id,
                    object_set_digest: fixture.object_set.object_set.object_set_digest,
                    extensions: Extensions::new(),
                },
            )
            .await
            .unwrap();

        assert!(result.replayed);
        assert_eq!(
            components
                .placement
                .get_replication(&fixture.tenant_id, &fixture.replication.replication_id)
                .await
                .unwrap(),
            Some(cancelled)
        );
        assert_eq!(
            components
                .placement
                .list_replication_objects(&fixture.tenant_id, &fixture.replication.replication_id)
                .await
                .unwrap(),
            vec![checkpoint]
        );
        assert!(components
            .placement
            .commit_placement_sets(&fixture.tenant_id, &fixture.replication.commit_id)
            .await
            .unwrap()
            .is_empty());
        assert!(components
            .placement
            .object_placements(&fixture.tenant_id, &fixture.object_id)
            .await
            .unwrap()
            .is_empty());
    }
}
