use neoengram_core::ObjectId;
use neoengram_protocol::{
    ArtifactId, JobAssignment, MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage,
    TenantId, UnixMillis, WireIndexVersion,
};

use crate::{
    AuditEvent, AuthorizationRequest, CentralResult, DurableObject, IndexKey, IndexPublishOutcome,
    IndexPublishRequest, JobKey, JobRecord, StagedMetadataBatch,
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

pub trait Authorizer: Send + Sync {
    fn authorize(&self, request: &AuthorizationRequest) -> CentralResult<()>;
}

pub trait JobRepository: Send + Sync {
    fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>>;
    /// Atomically inserts `job`, or returns the complete record already stored at the same key.
    fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome>;
    fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord>;
}

pub trait AssignmentOutbox: Send + Sync {
    /// Durably claims the tenant-scoped AssignmentId without exposing it for delivery.
    ///
    /// An identical payload is idempotent. Reusing the ID for another payload must fail. A
    /// reservation deliberately survives a failed job write so callers can recover with the same
    /// assignment, while a different AssignmentId remains available for a fresh attempt.
    fn reserve(&self, assignment: JobAssignment) -> CentralResult<AssignmentReserveOutcome>;

    /// Makes an exact, durable reservation visible after the authoritative job stores it.
    fn publish(&self, assignment: JobAssignment) -> CentralResult<AssignmentPublishOutcome>;
}

pub trait MetadataBatchStager: Send + Sync {
    /// Staged material for an active Prepared job must remain readable until the control plane
    /// atomically freezes its PublicationCandidate or the job reaches a terminal state.
    fn stage_descriptor(&self, descriptor: MetadataBatchDescriptor) -> CentralResult<bool>;
    fn stage_page(
        &self,
        descriptor: &MetadataBatchDescriptor,
        page: MetadataBatchPage,
    ) -> CentralResult<bool>;
    fn get(
        &self,
        tenant_id: &TenantId,
        batch_id: &MetadataBatchId,
    ) -> CentralResult<Option<StagedMetadataBatch>>;
}

pub trait ObjectCatalog: Send + Sync {
    fn durable_object(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object_id: ObjectId,
    ) -> CentralResult<Option<DurableObject>>;
}

pub trait IndexPublisher: Send + Sync {
    /// On success, atomically publishes the request's canonical Manifests and Index CAS as one
    /// idempotent boundary. Conflict and rejection must publish neither. Repeating an identical
    /// `job_key` request must return the original outcome, including after the CAS completed but
    /// before the control-plane terminal state was persisted.
    fn compare_and_swap(&self, request: IndexPublishRequest) -> CentralResult<IndexPublishOutcome>;
    fn current_version(&self, key: &IndexKey) -> CentralResult<WireIndexVersion>;
}

pub trait AuditSink: Send + Sync {
    /// Records one deterministic event ID. Replay retains the first observed timestamp.
    fn record(&self, event: AuditEvent) -> CentralResult<bool>;
}

pub trait Clock: Send + Sync {
    fn now(&self) -> UnixMillis;
}
