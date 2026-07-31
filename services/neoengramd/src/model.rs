use std::collections::BTreeMap;

use neoengram_core::{ContentDigest, FileRecord, LogicalPath, Manifest, ObjectId};
use neoengram_protocol::{
    AddAssignment, AddOperation, AgentId, AgentMountId, ArtifactId, ArtifactPlacementId,
    AssignmentGeneration, AssignmentId, EdgeClusterId, Extensions, JobAccepted, JobAssignment,
    JobDecision, JobFailed, JobFinalized, JobId, JobPrepared, JobProgress, JobState, LeaseGrant,
    MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage, MountGeneration, OwnerGeneration,
    PlacementGeneration, PlaygroundId, PrincipalRef, ProjectId, ResourceVersion, StorageVolumeId,
    TenantId, UnixMillis, WireIndexVersion,
};
use serde::{Deserialize, Serialize};

/// Immutable tenant/artifact/workspace authority scope for one managed Add job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddJobSpec {
    pub job_id: JobId,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
    pub expected_index_version: WireIndexVersion,
    pub request_digest: ContentDigest,
    pub deadline_unix_ms: UnixMillis,
    pub paths: Vec<LogicalPath>,
    pub all: bool,
    /// Unknown user-operation members retained verbatim and bound by `request_digest`.
    pub extensions: Extensions,
}

impl AddJobSpec {
    #[must_use]
    pub fn operation(&self) -> AddOperation {
        AddOperation {
            job_id: self.job_id.clone(),
            principal: self.principal.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            playground_id: self.playground_id.clone(),
            expected_index_version: self.expected_index_version.clone(),
            deadline_unix_ms: self.deadline_unix_ms,
            paths: self.paths.clone(),
            all: self.all,
            extensions: self.extensions.clone(),
        }
    }

    pub fn computed_request_digest(&self) -> neoengram_protocol::ProtocolResult<ContentDigest> {
        self.operation().request_digest()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JobKey {
    pub tenant_id: TenantId,
    pub job_id: JobId,
}

impl JobKey {
    #[must_use]
    pub fn new(tenant_id: TenantId, job_id: JobId) -> Self {
        Self { tenant_id, job_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IndexKey {
    pub tenant_id: TenantId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
}

/// Server-selected execution placement and all generations required to fence stale agents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssignmentTarget {
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub agent_id: AgentId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    pub artifact_placement_id: ArtifactPlacementId,
    pub placement_generation: PlacementGeneration,
    pub agent_mount_id: AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
    pub lease: Option<LeaseGrant>,
}

/// Durable authoritative record. Assignment and prepared metadata are persisted before side effects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    pub spec: AddJobSpec,
    pub state: JobState,
    pub resource_version: ResourceVersion,
    pub assignment: Option<AddAssignment>,
    pub accepted: Option<JobAccepted>,
    pub progress: Option<JobProgress>,
    pub prepared: Option<JobPrepared>,
    pub publication_candidate: Option<PublicationCandidate>,
    pub decision: Option<JobDecision>,
    pub finalized: Option<JobFinalized>,
    pub finalized_ack: Option<JobFinalized>,
    pub failure: Option<JobFailed>,
}

/// Canonical publication material frozen atomically with `Prepared -> Publishing`.
///
/// Publishing recovery must use this record rather than transient metadata staging or current
/// object-durability observations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationCandidate {
    pub expected_index_version: WireIndexVersion,
    pub result_index_digest: ContentDigest,
    pub publication_digest: ContentDigest,
    pub manifests: Vec<Manifest>,
    pub mutations: Vec<neoengram_protocol::IndexDeltaRecord>,
}

impl JobRecord {
    #[must_use]
    pub fn key(&self) -> JobKey {
        JobKey::new(self.spec.tenant_id.clone(), self.spec.job_id.clone())
    }

    #[must_use]
    pub fn index_key(&self) -> IndexKey {
        IndexKey {
            tenant_id: self.spec.tenant_id.clone(),
            artifact_id: self.spec.artifact_id.clone(),
            playground_id: self.spec.playground_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Actor {
    Principal(PrincipalRef),
    Agent(AgentId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    CreateAddJob,
    AssignJob,
    ReceiveReport,
    StageMetadataBatch,
    ExpireAddJob,
    FinalizeAdd,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationRequest {
    pub actor: Actor,
    pub action: Action,
    pub tenant_id: TenantId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
    pub job_id: JobId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAddJobRequest {
    pub actor: PrincipalRef,
    pub spec: AddJobSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAddJobResult {
    pub job: JobRecord,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AssignJobRequest {
    pub actor: PrincipalRef,
    pub tenant_id: TenantId,
    pub job_id: JobId,
    pub target: AssignmentTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AssignJobResult {
    pub job: JobRecord,
    pub assignment: JobAssignment,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AgentReport {
    Accepted(JobAccepted),
    Progress(JobProgress),
    Prepared(JobPrepared),
    Finalized(JobFinalized),
    Failed(JobFailed),
}

impl AgentReport {
    #[must_use]
    pub const fn job_id(&self) -> &JobId {
        match self {
            Self::Accepted(report) => &report.job_id,
            Self::Progress(report) => &report.job_id,
            Self::Prepared(report) => &report.job_id,
            Self::Finalized(report) => &report.job_id,
            Self::Failed(report) => &report.job_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveReportRequest {
    pub tenant_id: TenantId,
    pub agent_id: AgentId,
    pub report: AgentReport,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveReportResult {
    pub job: JobRecord,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetadataBatchSubmission {
    Descriptor(MetadataBatchDescriptor),
    Page(MetadataBatchPage),
}

impl MetadataBatchSubmission {
    #[must_use]
    pub const fn batch_id(&self) -> &MetadataBatchId {
        match self {
            Self::Descriptor(descriptor) => &descriptor.batch_id,
            Self::Page(page) => &page.batch_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StageMetadataBatchRequest {
    pub tenant_id: TenantId,
    pub job_id: JobId,
    pub agent_id: AgentId,
    pub submission: MetadataBatchSubmission,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StageMetadataBatchResult {
    pub batch_id: MetadataBatchId,
    pub complete: bool,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpireAddJobRequest {
    pub actor: PrincipalRef,
    pub tenant_id: TenantId,
    pub job_id: JobId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpireAddJobResult {
    pub job: JobRecord,
    pub decision: Option<JobDecision>,
    pub finalized: Option<JobFinalized>,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FinalizeAddRequest {
    pub actor: PrincipalRef,
    pub tenant_id: TenantId,
    pub job_id: JobId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePublicationRequest {
    pub tenant_id: TenantId,
    pub job_id: JobId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FinalizeAddResult {
    pub job: JobRecord,
    pub decision: JobDecision,
    pub finalized: JobFinalized,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StagedMetadataBatch {
    pub descriptor: MetadataBatchDescriptor,
    pub pages: BTreeMap<u32, MetadataBatchPage>,
}

impl StagedMetadataBatch {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.pages.len() == self.descriptor.page_count as usize
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableObject {
    pub object_id: ObjectId,
    pub size: u64,
    pub verified_digest: ContentDigest,
    pub storage_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexPublishRequest {
    pub job_key: JobKey,
    pub index_key: IndexKey,
    pub expected_index_version: WireIndexVersion,
    pub expected_result_digest: ContentDigest,
    pub manifests: Vec<Manifest>,
    pub mutations: Vec<neoengram_protocol::IndexDeltaRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexPublishRejection {
    InvalidMetadata {
        message: String,
    },
    ResultDigestMismatch {
        expected_digest: ContentDigest,
        observed_digest: ContentDigest,
    },
    RevisionExhausted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexPublishOutcome {
    Published(WireIndexVersion),
    Conflict(WireIndexVersion),
    Rejected(IndexPublishRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AuditKind {
    JobCreated,
    AssignmentQueued,
    ReportReceived,
    MetadataStaged,
    AddExpired,
    AddFinalized,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    pub kind: AuditKind,
    pub job_key: JobKey,
    pub state: JobState,
    pub occurred_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AgentEnrollmentAuditKind {
    Approved,
    Rejected,
    ReplacementApproved,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentEnrollmentAuditEvent {
    pub event_id: String,
    pub kind: AgentEnrollmentAuditKind,
    pub tenant_id: neoengram_protocol::TenantId,
    pub enrollment_id: neoengram_protocol::AgentEnrollmentId,
    pub storage_volume_id: neoengram_protocol::StorageVolumeId,
    pub decision_request_id: neoengram_protocol::RequestId,
    pub resource_version: neoengram_protocol::ResourceVersion,
    pub actor: neoengram_protocol::PrincipalRef,
    pub occurred_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValidatedMetadata {
    pub manifests: Vec<Manifest>,
    pub mutations: Vec<neoengram_protocol::IndexDeltaRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishedIndex {
    pub version: WireIndexVersion,
    pub records: Vec<FileRecord>,
}
