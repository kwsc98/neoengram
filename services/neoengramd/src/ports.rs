use std::sync::Arc;

use async_trait::async_trait;
use neoengram_core::ObjectId;
use neoengram_protocol::{
    AgentId, ArtifactId, JobAssignment, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchPage, PlacementGeneration, StorageVolumeId, TenantId, UnixMillis,
    WireIndexVersion,
};

use crate::{
    AgentEnrollmentAuditEvent, AgentEnrollmentExpiryReconciliation,
    AgentEnrollmentLifecycleAuditEvent, AgentEnrollmentListPage, AgentEnrollmentListRequest,
    AgentRegistryRecord, AgentRegistryReplacementRecords, ArtifactListPage, ArtifactListRequest,
    ArtifactRecord, AuditEvent, AuthorizationRequest, CatalogInsertOutcome, CentralResult,
    IndexKey, IndexPublishOutcome, IndexPublishRequest, InitializeIndexSnapshotRequest, JobKey,
    JobRecord, ObjectPlacementEvidence, PlaygroundInsertRequest, PlaygroundListPage,
    PlaygroundListRequest, PlaygroundRecord, PreCommitCancelRequest, PreCommitCommitOutcome,
    PreCommitCommitRequest, PreCommitKey, PreCommitMutationOutcome, PreCommitRecord,
    PreCommitRestartRequest, PreCommitStartRequest, PublishedIndex, StagedMetadataBatch,
    StorageVolumeListPage, StorageVolumeListRequest, StorageVolumeRecord, TenantListPage,
    TenantListRequest, TenantRecord,
};

/// Result of atomically inserting a job or loading the record already stored at its key.
#[derive(Debug, Clone, PartialEq)]
pub enum JobInsertOutcome {
    Inserted(JobRecord),
    Existing(JobRecord),
}

/// Result of durably reserving an AssignmentId in the delivery outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentReserveOutcome {
    Reserved,
    Existing,
}

/// Result of making a reserved assignment visible to delivery workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentPublishOutcome {
    Published,
    AlreadyPublished,
}

/// Result of durably retiring an assignment after the authoritative Job accepted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentRetireOutcome {
    Retired,
    AlreadyRetired,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AgentRegistryInsertOutcome {
    Inserted(AgentRegistryRecord),
    Existing(AgentRegistryRecord),
}

/// Durable aggregate repository for the one-Volume enrollment vertical slice.
#[async_trait]
pub trait AgentRegistryRepository: Send + Sync {
    async fn get(
        &self,
        enrollment_id: &neoengram_protocol::AgentEnrollmentId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_for_tenant(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        enrollment_id: &neoengram_protocol::AgentEnrollmentId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn list_for_tenant(
        &self,
        request: &AgentEnrollmentListRequest,
    ) -> CentralResult<AgentEnrollmentListPage>;
    async fn get_by_agent(
        &self,
        agent_id: &neoengram_protocol::AgentId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_token_request_id(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        token_request_id: &neoengram_protocol::RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_token_digest(
        &self,
        token_digest: &neoengram_core::ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_bootstrap_request_id(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        bootstrap_request_id: &neoengram_protocol::RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_decision_request_id(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        decision_request_id: &neoengram_protocol::RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_installation_id(
        &self,
        installation_id: &neoengram_protocol::AgentInstallationId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_public_key_fingerprint(
        &self,
        public_key_fingerprint: &neoengram_core::ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_current_by_volume(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        storage_volume_id: &neoengram_protocol::StorageVolumeId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_pvc_binding(
        &self,
        edge_cluster_id: &neoengram_protocol::EdgeClusterId,
        pvc_identity_digest: &neoengram_protocol::PvcIdentityDigest,
    ) -> CentralResult<Option<crate::PvcVolumeBinding>>;
    async fn expire_stale_token_intents(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        storage_volume_id: &neoengram_protocol::StorageVolumeId,
        edge_cluster_id: &neoengram_protocol::EdgeClusterId,
        pvc_identity_digest: &neoengram_protocol::PvcIdentityDigest,
        now_unix_ms: neoengram_protocol::UnixMillis,
    ) -> CentralResult<usize>;
    async fn expire_stale_review_enrollments(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        now_unix_ms: neoengram_protocol::UnixMillis,
    ) -> CentralResult<usize>;
    /// Atomically expires every stale token intent and pending review in the registry.
    async fn reconcile_expired_enrollments(
        &self,
        now_unix_ms: neoengram_protocol::UnixMillis,
    ) -> CentralResult<AgentEnrollmentExpiryReconciliation>;
    /// Returns immutable decision audit events persisted in the same CAS aggregates.
    async fn enrollment_audit_events(&self) -> CentralResult<Vec<AgentEnrollmentAuditEvent>>;
    /// Returns lifecycle events persisted atomically with each enrollment state transition.
    async fn enrollment_lifecycle_audit_events(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
    ) -> CentralResult<Vec<AgentEnrollmentLifecycleAuditEvent>>;
    /// Atomically consumes a strictly increasing bootstrap-status signature timestamp.
    ///
    /// This is an authentication replay watermark, not an aggregate domain mutation: successful
    /// consumption must not advance the public ResourceVersion or enrollment update time.
    async fn consume_bootstrap_status_signed_at(
        &self,
        enrollment_id: &neoengram_protocol::AgentEnrollmentId,
        signed_at_unix_ms: neoengram_protocol::UnixMillis,
    ) -> CentralResult<()>;
    /// Inserts only a fresh `TokenIssued` intent; later states must use the CAS methods.
    async fn insert_or_load(
        &self,
        record: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryInsertOutcome>;
    async fn replace(
        &self,
        expected_resource_version: u64,
        record: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryRecord>;
    async fn activate_replacement(
        &self,
        expected_previous_resource_version: u64,
        revoked: AgentRegistryRecord,
        expected_replacement_resource_version: u64,
        replacement: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryReplacementRecords>;
}

/// Durable control-catalog repository. SQLite composes this with AgentRegistry in one database.
#[async_trait]
pub trait ControlCatalogRepository: Send + Sync {
    async fn get_tenant(&self, tenant_id: &TenantId) -> CentralResult<Option<TenantRecord>>;
    async fn list_tenants(&self, request: &TenantListRequest) -> CentralResult<TenantListPage>;
    async fn insert_tenant(
        &self,
        record: TenantRecord,
    ) -> CentralResult<CatalogInsertOutcome<TenantRecord>>;

    async fn get_artifact(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_protocol::ProjectId,
        artifact_id: &neoengram_protocol::ArtifactId,
    ) -> CentralResult<Option<ArtifactRecord>>;
    async fn list_artifacts(
        &self,
        request: &ArtifactListRequest,
    ) -> CentralResult<ArtifactListPage>;
    async fn insert_artifact(
        &self,
        record: ArtifactRecord,
    ) -> CentralResult<CatalogInsertOutcome<ArtifactRecord>>;

    async fn get_storage_volume(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &neoengram_protocol::StorageVolumeId,
    ) -> CentralResult<Option<StorageVolumeRecord>>;
    async fn list_storage_volumes(
        &self,
        request: &StorageVolumeListRequest,
    ) -> CentralResult<StorageVolumeListPage>;
    async fn insert_storage_volume(
        &self,
        record: StorageVolumeRecord,
    ) -> CentralResult<CatalogInsertOutcome<StorageVolumeRecord>>;

    async fn get_playground(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_protocol::ProjectId,
        artifact_id: &neoengram_protocol::ArtifactId,
        playground_id: &neoengram_protocol::PlaygroundId,
    ) -> CentralResult<Option<PlaygroundRecord>>;
    async fn list_playgrounds(
        &self,
        request: &PlaygroundListRequest,
    ) -> CentralResult<PlaygroundListPage>;
    async fn insert_playground(
        &self,
        record: PlaygroundRecord,
    ) -> CentralResult<CatalogInsertOutcome<PlaygroundRecord>> {
        self.insert_playground_fenced(PlaygroundInsertRequest {
            record,
            artifact_head: crate::ArtifactHeadExpectation::Any,
        })
        .await
    }

    /// Inserts a Playground while atomically fencing a Head observation made by the service.
    /// Implementations must resolve an existing idempotent record before evaluating the fence.
    async fn insert_playground_fenced(
        &self,
        request: PlaygroundInsertRequest,
    ) -> CentralResult<CatalogInsertOutcome<PlaygroundRecord>>;

    /// Atomically advances a Playground lifecycle state. Implementations must treat a replay
    /// which already observes `next` as idempotent, while rejecting a transition from another
    /// state. This is the fencing boundary used by asynchronous materialization reports.
    async fn transition_playground_state(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_protocol::ProjectId,
        artifact_id: &neoengram_protocol::ArtifactId,
        playground_id: &neoengram_protocol::PlaygroundId,
        expected: crate::PlaygroundState,
        next: crate::PlaygroundState,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<PlaygroundRecord>;

    /// Atomically advances both authority-derived Head pointers for one committed Playground.
    /// An exact replay which already observes `commit_id` on both resources succeeds unchanged.
    async fn advance_playground_commit(
        &self,
        request: crate::AdvancePlaygroundCommitRequest,
    ) -> CentralResult<crate::AdvancePlaygroundCommitOutcome>;

    async fn get_snapshot(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &neoengram_protocol::SnapshotId,
    ) -> CentralResult<Option<crate::SnapshotRecord>>;

    async fn list_snapshots(
        &self,
        request: &crate::SnapshotListRequest,
    ) -> CentralResult<crate::SnapshotListPage>;

    /// Idempotently inserts a Snapshot while atomically validating its Artifact Head fence,
    /// Ready Volume binding, request identity, and reusable Commit/Volume placement identity.
    async fn insert_snapshot_fenced(
        &self,
        request: crate::SnapshotInsertRequest,
    ) -> CentralResult<crate::SnapshotInsertOutcome>;

    /// Advances delivery lifecycle. An exact replay already at `next_state/next_phase` succeeds
    /// and may advance `updated_at_unix_ms` for a persisted recovery lease; any other state is
    /// rejected so stale Agent reports cannot overwrite a later result.
    #[allow(clippy::too_many_arguments)]
    async fn transition_snapshot_state(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &neoengram_protocol::SnapshotId,
        expected_state: crate::SnapshotState,
        next_state: crate::SnapshotState,
        next_phase: crate::SnapshotPhase,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<crate::SnapshotRecord>;
}

#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(&self, request: &AuthorizationRequest) -> CentralResult<()>;
}

#[async_trait]
pub trait JobRepository: Send + Sync {
    async fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>>;
    /// Lists jobs which still require scheduler, expiry, publication, or delivery recovery work.
    ///
    /// Implementations return records strictly after `after` in stable tenant/job ordering and
    /// apply `limit` after filtering against `now`.
    async fn list_recoverable(
        &self,
        after: Option<&JobKey>,
        now: UnixMillis,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>>;
    /// Lists unacknowledged publish decisions for one Agent, applying `limit` after Agent filtering.
    async fn list_pending_decisions_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>>;
    /// Atomically inserts `job`, or returns the complete record already stored at the same key.
    async fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome>;
    async fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord>;
}

/// Durable Pre-commit aggregate and immutable Commit repository.
///
/// `commit` consumes a candidate and inserts its Commit in one authority transaction. Updating
/// Artifact and Playground Head pointers remains a separate control-catalog recovery boundary.
#[async_trait]
pub trait PreCommitRepository: Send + Sync {
    async fn start(
        &self,
        request: PreCommitStartRequest,
    ) -> CentralResult<PreCommitMutationOutcome>;
    async fn get(&self, key: &PreCommitKey) -> CentralResult<Option<PreCommitRecord>>;
    /// Returns the current operation for one Playground. Abnormal sessions remain active so the
    /// user can inspect and restart them; cancelled and committed history is excluded.
    async fn get_active(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_protocol::ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &neoengram_protocol::PlaygroundId,
    ) -> CentralResult<Option<PreCommitRecord>>;
    /// Stable keyset scan used to recover attempts whose Pre-commit write committed before their
    /// associated Add Job was created or dispatched.
    async fn list_running(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>>;
    /// Lists committed Pre-commits whose immutable Commit is durable but whose Artifact and
    /// Playground Head publication has not yet been acknowledged.
    async fn list_unpublished_commits(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>>;
    async fn restart(
        &self,
        request: PreCommitRestartRequest,
    ) -> CentralResult<PreCommitMutationOutcome>;
    async fn cancel(
        &self,
        request: PreCommitCancelRequest,
    ) -> CentralResult<PreCommitMutationOutcome>;
    async fn sync_job(
        &self,
        job: JobRecord,
        published_index: Option<PublishedIndex>,
        observed_at_unix_ms: UnixMillis,
    ) -> CentralResult<Option<PreCommitRecord>>;
    async fn commit(
        &self,
        request: PreCommitCommitRequest,
    ) -> CentralResult<PreCommitCommitOutcome>;
    async fn get_commit(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_protocol::ProjectId,
        artifact_id: &ArtifactId,
        commit_id: neoengram_core::CommitId,
    ) -> CentralResult<Option<crate::CommitRecord>>;
    async fn acknowledge_head_publication(
        &self,
        key: &PreCommitKey,
        commit_id: neoengram_core::CommitId,
        published_at_unix_ms: UnixMillis,
    ) -> CentralResult<PreCommitRecord>;
}

#[async_trait]
pub trait AssignmentOutbox: Send + Sync {
    /// Durably claims the tenant-scoped AssignmentId without exposing it for delivery.
    ///
    /// An identical payload is idempotent. Reusing the ID for another payload must fail. A
    /// reservation deliberately survives a failed job write so callers can recover with the same
    /// assignment, while a different AssignmentId remains available for a fresh attempt.
    async fn reserve(&self, assignment: JobAssignment) -> CentralResult<AssignmentReserveOutcome>;

    /// Makes an exact, durable reservation visible after the authoritative job stores it.
    async fn publish(&self, assignment: JobAssignment) -> CentralResult<AssignmentPublishOutcome>;

    /// Makes an exact retired reservation visible again. Durable serving operations use this to
    /// request a fresh observation from the same fenced Agent without changing assignment identity.
    async fn reactivate(
        &self,
        assignment: JobAssignment,
    ) -> CentralResult<AssignmentPublishOutcome> {
        self.publish(assignment).await
    }

    /// Retires a published assignment after its operation-specific acknowledgement boundary.
    /// Managed Add retires at Accepted because the Agent ledger owns recovery; Workspace
    /// materialization retires only after a terminal report so Server redelivery survives restart.
    ///
    /// The reservation remains durable so the tenant-scoped AssignmentId cannot be reused.
    async fn retire(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_protocol::AssignmentId,
    ) -> CentralResult<AssignmentRetireOutcome>;

    /// Lists published, non-retired delivery candidates for one Agent in stable assignment order.
    ///
    /// The caller must pair these records with authoritative Job state. Add is deliverable only in
    /// `Assigned`; Workspace materialization remains deliverable in `Assigned/Accepted/Running`.
    /// This keeps acknowledgement and execution recovery grounded in the Job CAS.
    async fn pending_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobAssignment>>;
}

#[async_trait]
pub trait MetadataBatchStager: Send + Sync {
    /// Staged material for an active Prepared job must remain readable until the control plane
    /// atomically freezes its PublicationCandidate or the job reaches a terminal state.
    async fn stage_descriptor(&self, descriptor: MetadataBatchDescriptor) -> CentralResult<bool>;
    async fn stage_page(
        &self,
        descriptor: &MetadataBatchDescriptor,
        page: MetadataBatchPage,
    ) -> CentralResult<bool>;
    async fn get(
        &self,
        tenant_id: &TenantId,
        batch_id: &MetadataBatchId,
    ) -> CentralResult<Option<StagedMetadataBatch>>;
}

#[async_trait]
pub trait ObjectCatalog: Send + Sync {
    /// Persists authenticated Agent evidence for bytes held by one exact Volume placement
    /// generation. Replays are idempotent; reusing a receipt identity for different evidence is
    /// rejected.
    async fn record_placement(&self, evidence: &ObjectPlacementEvidence) -> CentralResult<()>;

    async fn object_placement(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        storage_volume_id: &StorageVolumeId,
        artifact_placement_id: &neoengram_protocol::ArtifactPlacementId,
        placement_generation: PlacementGeneration,
        object_id: ObjectId,
    ) -> CentralResult<Option<ObjectPlacementEvidence>>;
}

#[async_trait]
pub trait IndexPublisher: Send + Sync {
    /// Creates one authoritative Playground Index at the exact supplied version and records.
    ///
    /// Implementations must validate that `version.digest` is the canonical digest of `records`.
    /// The absent-to-present transition is atomic; an exact replay returns the stored version,
    /// while an existing different snapshot returns `ConcurrentUpdate` without mutation.
    async fn initialize_snapshot(
        &self,
        request: InitializeIndexSnapshotRequest,
    ) -> CentralResult<WireIndexVersion>;

    /// On success, atomically publishes the request's canonical Manifests and Index CAS as one
    /// idempotent boundary. Conflict and rejection must publish neither. Repeating an identical
    /// `job_key` request must return the original outcome, including after the CAS completed but
    /// before the control-plane terminal state was persisted.
    async fn compare_and_swap(
        &self,
        request: IndexPublishRequest,
    ) -> CentralResult<IndexPublishOutcome>;
    async fn current_version(&self, key: &IndexKey) -> CentralResult<WireIndexVersion>;

    /// Reads the last authoritative logical Index snapshot for a Playground.
    ///
    /// This is deliberately a read-only companion to publication. Implementations that do not
    /// retain logical records may return `InvalidState`; callers must never fall back to the
    /// Agent's physical worktree for public browsing.
    async fn published_index(&self, _key: &IndexKey) -> CentralResult<PublishedIndex> {
        Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "this authority backend does not expose logical Index snapshots",
        ))
    }
}

#[async_trait]
pub trait AuditSink: Send + Sync {
    /// Records one deterministic event ID. Replay retains the first observed timestamp.
    async fn record(&self, event: AuditEvent) -> CentralResult<bool>;
    /// Records one deterministic enrollment decision event across retry/replay.
    async fn record_enrollment_decision(
        &self,
        event: AgentEnrollmentAuditEvent,
    ) -> CentralResult<bool>;
}

pub trait Clock: Send + Sync {
    fn now(&self) -> UnixMillis;
}

/// Backend-neutral composition of all authoritative central-control storage ports.
#[derive(Clone)]
pub struct AuthorityStore {
    jobs: Arc<dyn JobRepository>,
    outbox: Arc<dyn AssignmentOutbox>,
    metadata: Arc<dyn MetadataBatchStager>,
    objects: Arc<dyn ObjectCatalog>,
    publisher: Arc<dyn IndexPublisher>,
    audit: Arc<dyn AuditSink>,
    precommits: Option<Arc<dyn PreCommitRepository>>,
    agent_registry: Option<Arc<dyn AgentRegistryRepository>>,
    control_catalog: Option<Arc<dyn ControlCatalogRepository>>,
    capabilities: AuthorityCapabilities,
}

impl AuthorityStore {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn from_parts(
        jobs: Arc<dyn JobRepository>,
        outbox: Arc<dyn AssignmentOutbox>,
        metadata: Arc<dyn MetadataBatchStager>,
        objects: Arc<dyn ObjectCatalog>,
        publisher: Arc<dyn IndexPublisher>,
        audit: Arc<dyn AuditSink>,
        capabilities: AuthorityCapabilities,
    ) -> Self {
        Self {
            jobs,
            outbox,
            metadata,
            objects,
            publisher,
            audit,
            precommits: None,
            agent_registry: None,
            control_catalog: None,
            capabilities,
        }
    }

    #[must_use]
    pub fn jobs(&self) -> Arc<dyn JobRepository> {
        self.jobs.clone()
    }

    #[must_use]
    pub fn outbox(&self) -> Arc<dyn AssignmentOutbox> {
        self.outbox.clone()
    }

    #[must_use]
    pub fn metadata(&self) -> Arc<dyn MetadataBatchStager> {
        self.metadata.clone()
    }

    #[must_use]
    pub fn objects(&self) -> Arc<dyn ObjectCatalog> {
        self.objects.clone()
    }

    #[must_use]
    pub fn publisher(&self) -> Arc<dyn IndexPublisher> {
        self.publisher.clone()
    }

    #[must_use]
    pub fn audit(&self) -> Arc<dyn AuditSink> {
        self.audit.clone()
    }

    #[must_use]
    pub fn with_precommits(mut self, repository: Arc<dyn PreCommitRepository>) -> Self {
        self.precommits = Some(repository);
        self
    }

    #[must_use]
    pub fn precommits(&self) -> Option<Arc<dyn PreCommitRepository>> {
        self.precommits.clone()
    }

    /// Adds the optional enrollment/Agent registry vertical slice to this composition root.
    #[must_use]
    pub fn with_agent_registry(mut self, registry: Arc<dyn AgentRegistryRepository>) -> Self {
        self.agent_registry = Some(registry);
        self
    }

    #[must_use]
    pub fn agent_registry(&self) -> Option<Arc<dyn AgentRegistryRepository>> {
        self.agent_registry.clone()
    }

    /// Adds the optional Tenant/Artifact/Volume/Playground control catalog.
    #[must_use]
    pub fn with_control_catalog(mut self, catalog: Arc<dyn ControlCatalogRepository>) -> Self {
        self.control_catalog = Some(catalog);
        self
    }

    #[must_use]
    pub fn control_catalog(&self) -> Option<Arc<dyn ControlCatalogRepository>> {
        self.control_catalog.clone()
    }

    #[must_use]
    pub const fn capabilities(&self) -> AuthorityCapabilities {
        self.capabilities
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityCapabilities {
    pub single_process: bool,
    pub database_enforced_tenant_isolation: bool,
    pub high_availability: bool,
    /// True when every enrollment decision and its immutable audit event share one CAS write.
    pub atomic_agent_registry_audit: bool,
}

impl AuthorityCapabilities {
    pub const IN_MEMORY: Self = Self {
        single_process: true,
        database_enforced_tenant_isolation: false,
        high_availability: false,
        atomic_agent_registry_audit: true,
    };

    pub const SQLITE: Self = Self {
        single_process: true,
        database_enforced_tenant_isolation: false,
        high_availability: false,
        atomic_agent_registry_audit: true,
    };
}
