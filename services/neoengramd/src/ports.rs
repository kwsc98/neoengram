use std::sync::Arc;

use async_trait::async_trait;
use neoengram_core::ObjectId;
use neoengram_protocol::{
    AgentId, ArtifactId, JobAssignment, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchPage, ObjectDurabilityReceipt, TenantId, UnixMillis, WireIndexVersion,
};

use crate::{
    AgentEnrollmentAuditEvent, AgentEnrollmentExpiryReconciliation,
    AgentEnrollmentLifecycleAuditEvent, AgentEnrollmentListPage, AgentEnrollmentListRequest,
    AgentRegistryRecord, AgentRegistryReplacementRecords, AuditEvent, AuthorizationRequest,
    CatalogInsertOutcome, CentralResult, DurableObject, IndexKey, IndexPublishOutcome,
    IndexPublishRequest, JobKey, JobRecord, PlaygroundListPage, PlaygroundListRequest,
    PlaygroundRecord, StagedMetadataBatch, StorageVolumeListPage, StorageVolumeListRequest,
    StorageVolumeRecord, TenantListPage, TenantListRequest, TenantRecord,
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
    ) -> CentralResult<CatalogInsertOutcome<PlaygroundRecord>>;
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

    /// Retires a published assignment after the Job CAS records acceptance.
    ///
    /// The reservation remains durable so the tenant-scoped AssignmentId cannot be reused.
    async fn retire(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_protocol::AssignmentId,
    ) -> CentralResult<AssignmentRetireOutcome>;

    /// Lists published, non-retired delivery candidates for one Agent in stable assignment order.
    ///
    /// The caller must pair these records with the authoritative Job state and deliver only jobs
    /// which are still `Assigned`. This keeps acknowledgement recovery grounded in the Job CAS.
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
    async fn record_durability(&self, receipt: &ObjectDurabilityReceipt) -> CentralResult<()>;

    async fn durable_object(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object_id: ObjectId,
    ) -> CentralResult<Option<DurableObject>>;
}

#[async_trait]
pub trait IndexPublisher: Send + Sync {
    /// On success, atomically publishes the request's canonical Manifests and Index CAS as one
    /// idempotent boundary. Conflict and rejection must publish neither. Repeating an identical
    /// `job_key` request must return the original outcome, including after the CAS completed but
    /// before the control-plane terminal state was persisted.
    async fn compare_and_swap(
        &self,
        request: IndexPublishRequest,
    ) -> CentralResult<IndexPublishOutcome>;
    async fn current_version(&self, key: &IndexKey) -> CentralResult<WireIndexVersion>;
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

    /// Adds the optional Tenant/Volume/Playground control catalog.
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
