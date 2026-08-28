use std::sync::Arc;

use neoengram_domain::protocol::{
    AgentId, AssignmentOperation, ControlError, ControlMessage, DecisionGeneration,
    DeletionOperationState, DeletionProof, DeletionProofId, DeletionProofResult, Envelope,
    EnvelopeHeader, ErrorCode, Extensions, IndexRevision, JobAssignment, JobDecision, JobFinalized,
    JobState, LifecycleEvent, LifecycleEventId, LifecycleEventKind, MessageId, MountGeneration,
    PrincipalKind, PublishDecision, ReplicationAssignment, ReplicationObjectState,
    ReplicationProgressReport, ReplicationState, RequestId, ResourceLifecycleReport,
    ResourceLifecycleReportState, ResourceVersion, RouteGeneration, SessionGeneration,
    SignedTransferTicket, SnapshotDeliveryAssignment, SnapshotDeliveryState, TraceId,
    TransferEndpoint, TransferTicket, UnixMillis, WireIndexVersion, WorkspaceMaterializeAssignment,
    AGENT_JOB_ASSIGNMENT_ACTION, AGENT_JOB_DECISION_ACTION, AGENT_LIFECYCLE_ASSIGNMENT_ACTION,
    AGENT_REPLICATION_ASSIGNMENT_ACTION, CURRENT_WIRE_VERSION,
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
    CentralErrorCode, CentralResult, Clock, ControlCatalogRepository, CreateAddJobRequest,
    CreateAddJobResult, CreateSnapshotDeliveryRequest, CreateSnapshotDeliveryResult,
    CreateWorkspaceMaterializationRequest, CreateWorkspaceMaterializationResult,
    ExpireAddJobRequest, ExpireAddJobResult, FinalizeAddRequest, FinalizeAddResult,
    FinalizeReplicationRequest, GatewayRegistryRepository, IndexPublishOutcome,
    IndexPublishRejection, IndexPublishRequest, IndexPublisher, JobInsertOutcome, JobOperation,
    JobRecord, JobRepository, MetadataBatchStager, MetadataBatchSubmission, ObjectCatalog,
    PlacementRepository, PublicationCandidate, QueryJobRequest, QueryJobResult,
    ReceiveReportRequest, ReceiveReportResult, RefreshReplicationRoutesRequest,
    ReplicationObjectRecord, ReplicationRecord, ReplicationRouteBinding,
    ReplicationStateTransitionRequest, ResumePublicationRequest, StageMetadataBatchRequest,
    StageMetadataBatchResult,
};

use crate::service::{CentralCommandKeyring, DEFAULT_CENTRAL_COMMAND_TTL_MS};

const CONTROL_ERROR_MESSAGE_LIMIT: usize = 4096;

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
    clock: Arc<dyn Clock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationReportResult {
    pub resource_version: ResourceVersion,
    pub replayed: bool,
}

async fn validate_replication_report_binding(
    placement: &dyn PlacementRepository,
    tenant_id: &neoengram_domain::protocol::TenantId,
    current: &ReplicationRecord,
    report: &ReplicationProgressReport,
) -> CentralResult<()> {
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

    /// Installs the Gateway route registry used to refresh session/route generations when a
    /// control channel reconnects. The immutable Agent, Volume, cluster, and pool bindings stay
    /// on the Replication record; only the live transport generations are refreshed.
    #[must_use]
    pub fn with_gateway_registry(mut self, registry: Arc<dyn GatewayRegistryRepository>) -> Self {
        self.gateway_registry = Some(registry);
        self
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

        // Replication commands are derived from the Placement authority rather than the Job
        // outbox. They remain deliverable throughout an active attempt so a reconnect can resume
        // the same staging offsets, while the immutable attempt/object-set/route fences are
        // rechecked before every delivery.
        if messages.len() < limit {
            if let (Some(placement), Some(_keyring), Some(agent_registry)) = (
                &self.placement,
                &self.replication_ticket_keyring,
                &self.agent_registry,
            ) {
                let Some(agent_record) = agent_registry.get_by_agent(agent_id).await? else {
                    return Ok(messages);
                };
                let tenant_id = agent_record.enrollment.tenant_id;
                for replication in placement
                    .list_replications_for_agent(&tenant_id, agent_id)
                    .await?
                {
                    if messages.len() == limit
                        || !matches!(
                            replication.state,
                            ReplicationState::Queued
                                | ReplicationState::Planning
                                | ReplicationState::Transferring
                                | ReplicationState::Verifying
                        )
                    {
                        continue;
                    }
                    let assignment = match self.replication_assignment(&replication).await {
                        Ok(assignment) => assignment,
                        Err(error) if replication_delivery_can_wait_for_next_tick(&error) => {
                            // A single replication can race a Gateway lease refresh or a route
                            // CAS. Keep unrelated Job/decision/lifecycle messages flowing; the
                            // active replication remains in Placement and will be retried on the
                            // next delivery tick. Authority/protocol failures still fail closed.
                            tracing::debug!(
                                agent_id = %agent_id,
                                replication_id = %replication.replication_id,
                                code = error.stable_code(),
                                "skipping temporarily unavailable replication assignment"
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let Some(assignment) = assignment else {
                        continue;
                    };
                    // The assignment builder refreshes the live target route. Do not enqueue a
                    // command for a channel whose session was fenced while the delivery pass was
                    // reading the registry.
                    if assignment.signed_ticket.as_ticket().session_generation != session_generation
                    {
                        continue;
                    }
                    let deadline = assignment.signed_ticket.as_ticket().deadline_unix_ms;
                    let envelope = action_envelope(
                        AGENT_REPLICATION_ASSIGNMENT_ACTION,
                        MessageId::new(format!(
                            "replication-{}-{}",
                            assignment.replication_id, assignment.attempt
                        ))?,
                        assignment.tenant_id.clone(),
                        session_generation,
                        deadline,
                        ControlMessage::ReplicationAssignment(Box::new(assignment)),
                    )?;
                    messages.push(envelope);
                }
            }
        }
        Ok(messages)
    }

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
            replication_id: record.replication_id,
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

    /// Creates the durable infrastructure Job used to materialize a Playground directory.
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
            &spec.playground_id,
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
            playground_id: spec.playground_id.clone(),
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
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id: spec.playground_id.clone(),
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
                return Ok(AssignWorkspaceMaterializationResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };
        let _ = self.outbox.publish(envelope.clone()).await?;
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
        let playground_id =
            neoengram_domain::protocol::PlaygroundId::new(spec.snapshot_id.as_str().to_owned())?;
        let mut job_scope = AddJobSpec {
            job_id: spec.job_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id,
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
                return Ok(AssignSnapshotDeliveryResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };
        let _ = self.outbox.publish(envelope.clone()).await?;
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

    /// Marks an elapsed materialization terminal and exposes the failed lifecycle on Playground.
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
            .transition_playground_state(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.playground_id,
                crate::PlaygroundState::Creating,
                crate::PlaygroundState::Abnormal,
                self.clock.now(),
            )
            .await?;
        let previous = job.resource_version.get();
        job.state = JobState::TimedOut;
        job = self.replace(previous, job).await?;
        if let Some(assignment) = &job.workspace_assignment {
            let _ = self
                .outbox
                .retire(tenant_id, &assignment.assignment_id)
                .await?;
        }
        Ok(job)
    }

    /// Converges the Job side of the lifecycle publication after a crash between the catalog
    /// lifecycle CAS and the Job CAS. No success or failure is fabricated while the Playground
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
        let playground = self
            .catalog
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "ControlPlane has no control catalog for materialization state",
                )
            })?
            .get_playground(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.playground_id,
            )
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::JobNotFound,
                    "materialization Playground no longer exists",
                )
            })?;
        let recovered_state = match playground.state {
            crate::PlaygroundState::Creating => return Ok(job),
            crate::PlaygroundState::Ready => JobState::Succeeded,
            crate::PlaygroundState::Abnormal => JobState::RecoveryRequired,
        };
        let previous = job.resource_version.get();
        job.state = recovered_state;
        job = self.replace(previous, job).await?;
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
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: job.spec.principal.clone(),
            tenant_id: job.spec.tenant_id.clone(),
            project_id: job.spec.project_id.clone(),
            artifact_id: job.spec.artifact_id.clone(),
            playground_id: job.spec.playground_id.clone(),
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
                            catalog
                                .transition_playground_state(
                                    &assignment.tenant_id,
                                    &assignment.project_id,
                                    &assignment.artifact_id,
                                    &assignment.playground_id,
                                    crate::PlaygroundState::Creating,
                                    crate::PlaygroundState::Ready,
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
                        .transition_playground_state(
                            &assignment.tenant_id,
                            &assignment.project_id,
                            &assignment.artifact_id,
                            &assignment.playground_id,
                            crate::PlaygroundState::Creating,
                            crate::PlaygroundState::Abnormal,
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
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            decision_generation,
            decision: outcome,
            final_state: state,
            extensions: Extensions::new(),
        };
        let finalized = JobFinalized {
            job_id: assignment.job_id.clone(),
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
                playground_id: spec.playground_id.clone(),
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
    Ok(())
}

fn validate_delivery_report_identity(
    assignment: &SnapshotDeliveryAssignment,
    job_id: &neoengram_domain::protocol::JobId,
    assignment_id: &neoengram_domain::protocol::AssignmentId,
    generation: neoengram_domain::protocol::AssignmentGeneration,
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
    use super::{
        action_envelope, bounded_control_error_message,
        reconnected_replication_report_matches_route, replication_delivery_can_wait_for_next_tick,
        ReplicationRouteGenerations, AGENT_JOB_ASSIGNMENT_ACTION, CONTROL_ERROR_MESSAGE_LIMIT,
    };
    use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
    use neoengram_domain::protocol::{
        AgentId, ArtifactId, CommitObject, CommitObjectSet, ControlError, ControlMessage,
        EdgeClusterId, ErrorCode, Extensions, GatewayPoolId, MessageId, MountGeneration,
        ObjectEncoding, ObjectSet, PlacementGeneration, PlacementSetId, ReplicationId,
        ReplicationObjectState, ReplicationProgressReport, ReplicationState, RequestId,
        RouteGeneration, SessionGeneration, StorageVolumeId, TenantId, UnixMillis,
    };

    use crate::{
        CancelReplicationRequest, CentralError, CentralErrorCode, InMemoryComponents,
        PlacementRepository, ReplicationObjectRecord, ReplicationRecord,
    };

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
