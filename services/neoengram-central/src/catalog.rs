use std::collections::BTreeSet;

use neoengram_domain::core::{ContentDigest, LogicalPath};
use neoengram_domain::protocol::{
    AgentResourceLifecycleAssignment, ArtifactId, DecimalU64, DeletionId, DeletionImpact,
    DeletionOperation, DeletionOperationState, DeletionProof, EdgeClusterId, HardlinkPolicy,
    LifecycleEvent, PlaygroundId, ProjectId, RequestId, ResourceLifecycle, ResourceRef,
    RetentionHoldId, SnapshotDeliveryId, SnapshotDeliveryMode, SnapshotDeliveryPolicy,
    SnapshotDeliveryState, SnapshotId, StorageVolumeId, TenantId, UnixMillis,
};
use serde::{Deserialize, Serialize};

/// Publicly selectable storage backend family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendType {
    Pvc,
    Nfs,
}

/// Access semantics frozen when a Volume is registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageAccessMode {
    ReadWriteOnce,
    ReadWriteMany,
    ReadOnlyMany,
}

/// Placement health exposed by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageVolumeState {
    Ready,
    Degraded,
    Unavailable,
}

/// PVC locator which is safe to expose in administrative responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogPvcReference {
    pub namespace: String,
    pub claim_name: String,
}

/// Private NFS locator. It must never be projected by the public API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogNfsReference {
    pub server: String,
    pub export_path: String,
}

/// Authoritative Tenant catalog record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRecord {
    pub tenant_id: TenantId,
    pub display_name: String,
    pub description: Option<String>,
    pub resource_version: u64,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

/// Authoritative logical Project catalog record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRecord {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub display_name: String,
    pub description: Option<String>,
    pub resource_version: u64,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

/// Immutable creation provenance for a logical Artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactInitialization {
    Empty,
    Derived {
        source_project_id: ProjectId,
        source_artifact_id: ArtifactId,
        source_commit_id: ContentDigest,
    },
}

/// Authoritative logical data asset. Physical placement is represented by derived resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub display_name: String,
    pub description: Option<String>,
    pub initialization: ArtifactInitialization,
    pub head_commit_id: Option<ContentDigest>,
    pub resource_version: u64,
    pub lifecycle: ResourceLifecycle,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

/// Authoritative logical Volume catalog record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageVolumeRecord {
    pub tenant_id: TenantId,
    pub storage_volume_id: StorageVolumeId,
    pub display_name: String,
    pub edge_cluster_id: EdgeClusterId,
    pub region: String,
    pub backend_type: StorageBackendType,
    pub access_mode: StorageAccessMode,
    pub allowed_delivery_modes: Vec<SnapshotDeliveryMode>,
    pub hardlink_policy: HardlinkPolicy,
    pub max_whole_file_bytes: DecimalU64,
    pub copy_reserve_bytes: DecimalU64,
    pub pvc_reference: Option<CatalogPvcReference>,
    pub nfs_reference: Option<CatalogNfsReference>,
    pub state: StorageVolumeState,
    pub resource_version: u64,
    pub lifecycle: ResourceLifecycle,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

impl StorageVolumeRecord {
    #[must_use]
    pub fn delivery_policy(&self) -> SnapshotDeliveryPolicy {
        SnapshotDeliveryPolicy {
            allowed_modes: self.allowed_delivery_modes.clone(),
            hardlink_policy: self.hardlink_policy,
            max_whole_file_bytes: self.max_whole_file_bytes,
            copy_reserve_bytes: self.copy_reserve_bytes,
            extensions: neoengram_domain::protocol::Extensions::new(),
        }
    }
}

/// Minimal authoritative Playground placement used by scheduling and the Web flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaygroundRecord {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
    pub storage_volume_id: StorageVolumeId,
    pub region: String,
    pub display_name: String,
    pub base_commit_id: Option<ContentDigest>,
    pub head_commit_id: Option<ContentDigest>,
    pub state: PlaygroundState,
    pub resource_version: u64,
    pub lifecycle: ResourceLifecycle,
    /// Relative to the Agent's approved Volume mount; never an absolute host path.
    pub relative_root: String,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaygroundState {
    Creating,
    Ready,
    Abnormal,
}

/// Logical immutable reference to one published Artifact Commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub snapshot_id: SnapshotId,
    /// Public idempotency identity. It is never accepted as the Snapshot resource identity.
    pub snapshot_request_id: RequestId,
    pub commit_id: ContentDigest,
    /// The single immutable Delivery selected as part of Snapshot creation.
    pub delivery_id: SnapshotDeliveryId,
    /// Edge cluster selected together with the target StorageVolume.  The repository and service
    /// validate that this matches the Volume's authoritative cluster binding.
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    pub delivery_mode: SnapshotDeliveryMode,
    pub state: SnapshotState,
    pub resource_version: u64,
    pub lifecycle: ResourceLifecycle,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotState {
    Creating,
    Ready,
    Abnormal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantListRequest {
    /// `None` is an explicit wildcard grant; `Some([])` means no visibility.
    pub visible_tenant_ids: Option<Vec<TenantId>>,
    pub query: Option<String>,
    pub after: Option<TenantListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub tenant_id: TenantId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantListPage {
    pub records: Vec<TenantRecord>,
    pub next: Option<TenantListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectListRequest {
    pub tenant_id: TenantId,
    pub query: Option<String>,
    pub after: Option<ProjectListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub project_id: ProjectId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectListPage {
    pub records: Vec<ProjectRecord>,
    pub next: Option<ProjectListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactListRequest {
    pub tenant_id: TenantId,
    pub project_id: Option<ProjectId>,
    pub query: Option<String>,
    pub after: Option<ArtifactListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactListPage {
    pub records: Vec<ArtifactRecord>,
    pub next: Option<ArtifactListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageVolumeListRequest {
    pub tenant_id: TenantId,
    pub region: Option<String>,
    pub backend_type: Option<StorageBackendType>,
    pub query: Option<String>,
    pub after: Option<StorageVolumeListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageVolumeListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub storage_volume_id: StorageVolumeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageVolumeListPage {
    pub records: Vec<StorageVolumeRecord>,
    pub next: Option<StorageVolumeListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaygroundListRequest {
    pub tenant_id: TenantId,
    pub project_id: Option<ProjectId>,
    pub artifact_id: Option<ArtifactId>,
    pub region: Option<String>,
    pub state: Option<PlaygroundState>,
    pub query: Option<String>,
    pub after: Option<PlaygroundListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaygroundListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaygroundListPage {
    pub records: Vec<PlaygroundRecord>,
    pub next: Option<PlaygroundListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotListRequest {
    pub tenant_id: TenantId,
    pub project_id: Option<ProjectId>,
    pub artifact_id: Option<ArtifactId>,
    pub commit_id: Option<ContentDigest>,
    pub state: Option<SnapshotState>,
    pub after: Option<SnapshotListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub snapshot_id: SnapshotId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotListPage {
    pub records: Vec<SnapshotRecord>,
    pub next: Option<SnapshotListCursor>,
}

/// Read-only projection of an immutable Snapshot.  v2 creates exactly one Delivery alongside the
/// Snapshot, so `snapshot_id` is unique in the Delivery catalog and the target placement is
/// immutable for the lifetime of both records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotDeliveryRecord {
    pub tenant_id: TenantId,
    pub delivery_id: SnapshotDeliveryId,
    /// Durable idempotency identity for the create mutation. It is control-plane metadata and is
    /// never exposed as part of the public Delivery resource.
    pub create_request_id: RequestId,
    pub snapshot_id: SnapshotId,
    pub commit_id: ContentDigest,
    pub storage_volume_id: StorageVolumeId,
    pub mode: SnapshotDeliveryMode,
    pub target_relative_root: LogicalPath,
    pub state: SnapshotDeliveryState,
    pub source_index_digest: ContentDigest,
    pub delivery_generation: neoengram_domain::protocol::DeliveryGeneration,
    pub file_count: u64,
    pub size_bytes: u64,
    pub object_set_digest: ContentDigest,
    pub resource_version: u64,
    pub issue_code: Option<String>,
    pub issue_message: Option<String>,
    pub issue_retryable: bool,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

impl SnapshotDeliveryRecord {
    /// Compares only the immutable create payload. Runtime state, retry generation, progress,
    /// issues, resource version, and timestamps may legitimately differ when a create request is
    /// replayed after the Agent has already processed the Delivery.
    #[must_use]
    pub fn same_create_request(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
            && self.delivery_id == other.delivery_id
            && self.create_request_id == other.create_request_id
            && self.snapshot_id == other.snapshot_id
            && self.commit_id == other.commit_id
            && self.storage_volume_id == other.storage_volume_id
            && self.mode == other.mode
            && self.target_relative_root == other.target_relative_root
            && self.source_index_digest == other.source_index_digest
            && self.object_set_digest == other.object_set_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDeliveryInsertRequest {
    pub record: SnapshotDeliveryRecord,
    pub request_id: RequestId,
    pub retention_roots: Vec<SnapshotDeliveryRetentionRoot>,
}

pub(crate) fn validate_snapshot_delivery_retention_roots(
    request: &SnapshotDeliveryInsertRequest,
) -> crate::CentralResult<()> {
    if request.record.mode != SnapshotDeliveryMode::Hardlink && !request.retention_roots.is_empty()
    {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::ProtocolInvalid,
            "Only Hardlink SnapshotDeliveries may retain CAS objects",
        )
        .with_retryable(false));
    }

    let mut object_ids = BTreeSet::new();
    for root in &request.retention_roots {
        if root.tenant_id != request.record.tenant_id
            || root.delivery_id != request.record.delivery_id
        {
            return Err(crate::CentralError::new(
                crate::CentralErrorCode::ProtocolInvalid,
                "SnapshotDelivery retention root scope does not match the Delivery",
            )
            .with_retryable(false));
        }
        if !object_ids.insert(root.object_id) {
            return Err(crate::CentralError::new(
                crate::CentralErrorCode::ProtocolInvalid,
                "SnapshotDelivery retention roots contain a duplicate object",
            )
            .with_retryable(false));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotDeliveryInsertOutcome {
    Inserted(SnapshotDeliveryRecord),
    Existing(SnapshotDeliveryRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDeliveryListRequest {
    pub tenant_id: TenantId,
    pub snapshot_id: Option<SnapshotId>,
    pub mode: Option<SnapshotDeliveryMode>,
    pub state: Option<SnapshotDeliveryState>,
    pub limit: u16,
}

/// Exact CAS objects pinned by a Hardlink SnapshotDelivery.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotDeliveryRetentionRoot {
    pub tenant_id: TenantId,
    pub delivery_id: SnapshotDeliveryId,
    pub object_id: neoengram_domain::core::ObjectId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotDeliveryMutationKind {
    Retry,
    Delete,
}

/// Durable result receipt for a Delivery retry/delete mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotDeliveryMutationRecord {
    pub tenant_id: TenantId,
    pub request_id: RequestId,
    pub delivery_id: SnapshotDeliveryId,
    pub kind: SnapshotDeliveryMutationKind,
    pub request_digest: ContentDigest,
    pub delivery: SnapshotDeliveryRecord,
}

/// One idempotent SnapshotDelivery state mutation.
///
/// The repository compares `expected_resource_version`, applies `desired_delivery` when it
/// differs from the current record, and persists the resulting receipt in the same transaction.
/// Passing the unchanged current record intentionally creates a durable no-op receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDeliveryMutationRequest {
    pub tenant_id: TenantId,
    pub request_id: RequestId,
    pub delivery_id: SnapshotDeliveryId,
    pub kind: SnapshotDeliveryMutationKind,
    pub request_digest: ContentDigest,
    pub expected_resource_version: u64,
    pub desired_delivery: SnapshotDeliveryRecord,
}

pub(crate) fn validate_snapshot_delivery_mutation_request(
    request: &SnapshotDeliveryMutationRequest,
) -> crate::CentralResult<()> {
    if request.expected_resource_version == 0
        || request.desired_delivery.tenant_id != request.tenant_id
        || request.desired_delivery.delivery_id != request.delivery_id
    {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::ProtocolInvalid,
            "SnapshotDelivery mutation scope or ResourceVersion is invalid",
        )
        .with_retryable(false));
    }
    Ok(())
}

pub(crate) fn validate_snapshot_delivery_parents(
    delivery: &SnapshotDeliveryRecord,
    snapshot: &SnapshotRecord,
    volume: &StorageVolumeRecord,
) -> crate::CentralResult<()> {
    if !volume.lifecycle.is_active() {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "SnapshotDelivery StorageVolume is not in the active lifecycle state",
        )
        .with_retryable(false));
    }
    if volume.state != StorageVolumeState::Ready {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::StorageVolumeNotReady,
            "SnapshotDelivery StorageVolume is not ready",
        )
        .with_retryable(false));
    }
    if volume.tenant_id != delivery.tenant_id
        || volume.storage_volume_id != delivery.storage_volume_id
    {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "SnapshotDelivery StorageVolume binding changed before creation",
        )
        .with_retryable(false));
    }
    if !snapshot.lifecycle.is_active() {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "SnapshotDelivery Snapshot is not in the active lifecycle state",
        )
        .with_retryable(false));
    }
    // A Delivery is created in the same aggregate transaction as its Snapshot.  The only valid
    // aggregate insertion states are the queued pair (`Creating` + `Requested`) and a consistent
    // already-completed pair (`Ready` + `Ready`).
    // In particular, a `Ready` Snapshot can never be paired with a non-ready Delivery: that
    // would publish a readable logical resource whose physical view is not available.
    let valid_initial_state = matches!(
        (snapshot.state, delivery.state),
        (SnapshotState::Creating, SnapshotDeliveryState::Requested)
            | (SnapshotState::Ready, SnapshotDeliveryState::Ready)
    );
    if !valid_initial_state {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "Snapshot and SnapshotDelivery states are inconsistent",
        )
        .with_retryable(false));
    }
    if snapshot.tenant_id != delivery.tenant_id
        || snapshot.snapshot_id != delivery.snapshot_id
        || snapshot.commit_id != delivery.commit_id
        || snapshot.delivery_id != delivery.delivery_id
        || snapshot.storage_volume_id != delivery.storage_volume_id
        || snapshot.delivery_mode != delivery.mode
        || snapshot.edge_cluster_id != volume.edge_cluster_id
    {
        return Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "SnapshotDelivery Snapshot identity changed before creation",
        )
        .with_retryable(false));
    }
    Ok(())
}

/// A tenant-scoped, immutable S3 view over exactly one Ready Snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3AccessPointRecord {
    pub access_point_id: neoengram_domain::protocol::S3AccessPointId,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub snapshot_id: SnapshotId,
    pub commit_id: ContentDigest,
    /// The immutable SnapshotDelivery that materializes this Access Point's read view.
    pub delivery_id: SnapshotDeliveryId,
    /// The Delivery target is persisted so S3 never falls back to an arbitrary complete Volume.
    pub storage_volume_id: StorageVolumeId,
    pub edge_cluster_id: EdgeClusterId,
    pub bucket_name: String,
    pub state: S3AccessPointState,
    pub policy_generation: u64,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3AccessPointState {
    Active,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CredentialRecord {
    pub credential_id: neoengram_domain::protocol::S3CredentialId,
    pub access_point_id: neoengram_domain::protocol::S3AccessPointId,
    pub access_key_id: String,
    /// Envelope-encrypted secret material. It is never returned by repository reads.
    pub encrypted_secret: Vec<u8>,
    pub state: S3CredentialState,
    pub expires_at_unix_ms: UnixMillis,
    pub created_at_unix_ms: UnixMillis,
    pub last_used_at_unix_ms: Option<UnixMillis>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3CredentialState {
    Active,
    Revoked,
    Expired,
}

/// One successfully committed public S3 mutation identity.
///
/// The `(tenant_id, request_id)` pair is globally unique across all S3 mutation kinds. Reusing
/// the request identity for another operation or payload must fail closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3MutationRecord {
    pub tenant_id: TenantId,
    pub request_id: RequestId,
    pub operation: S3MutationKind,
    pub request_digest: ContentDigest,
    pub created_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3MutationKind {
    AccessPointCreate,
    AccessPointEnable,
    AccessPointDisable,
    CredentialCreate,
    CredentialRevoke,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3AccessPointCreateResult {
    pub access_point: S3AccessPointRecord,
    pub credential: S3CredentialRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3AccessPointListRequest {
    pub tenant_id: TenantId,
    pub after: Option<S3AccessPointListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3AccessPointListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub access_point_id: neoengram_domain::protocol::S3AccessPointId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3AccessPointListPage {
    pub records: Vec<S3AccessPointRecord>,
    pub next: Option<S3AccessPointListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3AccessPointInsertOutcome {
    Inserted(S3AccessPointRecord),
    Existing(S3AccessPointRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3CredentialInsertOutcome {
    Inserted(S3CredentialRecord),
    Existing(S3CredentialRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogInsertOutcome<T> {
    Inserted(T),
    Existing(T),
}

/// Artifact Head condition captured while resolving a Playground create request.
///
/// `Any` is used for an explicitly selected immutable Commit. `Exact` is used when the caller
/// omitted the base Commit and the service resolved it from the current Artifact Head; the
/// repository must compare that observation in the same atomic boundary as the first insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactHeadExpectation {
    Any,
    Exact(Option<ContentDigest>),
}

/// Internal, storage-independent request for an atomic Playground insertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaygroundInsertRequest {
    pub record: PlaygroundRecord,
    pub artifact_head: ArtifactHeadExpectation,
}

/// Atomic Snapshot insertion. `artifact_head` fences an omitted Commit selection in the same
/// transaction as the first insert; explicit immutable Commit selection uses `Any`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInsertRequest {
    pub record: SnapshotRecord,
    pub artifact_head: ArtifactHeadExpectation,
}

/// Atomic Snapshot + its one immutable Delivery.  The two records share the same public request
/// identity and are published in one repository transaction/critical section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotWithDeliveryInsertRequest {
    pub snapshot: SnapshotInsertRequest,
    pub delivery: SnapshotDeliveryInsertRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotWithDeliveryInsertResult {
    pub snapshot: SnapshotRecord,
    pub delivery: SnapshotDeliveryRecord,
    pub replayed: bool,
}

/// One atomic control-catalog publication of an immutable Commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvancePlaygroundCommitRequest {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
    /// Branch-local Playground Head frozen by the Pre-commit. Artifact Head is a convenience
    /// pointer and is deliberately not part of this compare-and-swap fence.
    pub expected_head_commit_id: Option<ContentDigest>,
    pub commit_id: ContentDigest,
    pub updated_at_unix_ms: UnixMillis,
}

/// Artifact and Playground heads observed after a successful branch-local publication CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvancePlaygroundCommitOutcome {
    pub artifact: ArtifactRecord,
    pub playground: PlaygroundRecord,
    pub replayed: bool,
}

/// Repository result for a five-minute, immutable deletion impact view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionImpactRecord {
    pub impact: DeletionImpact,
    pub impact_digest: ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionImpactQuery {
    pub tenant_id: TenantId,
    pub root: ResourceRef,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    /// Authority-owned blockers observed immediately before the Catalog impact transaction.
    pub additional_blockers: Vec<neoengram_domain::protocol::DeletionBlocker>,
    /// Authority-owned workload and placement facts captured with the impact snapshot.
    ///
    /// These values are advisory for the confirmation surface, but are included in the
    /// Catalog-computed impact digest so a replay cannot silently change what the user confirmed.
    pub authority_impact: Option<AuthorityLifecycleImpact>,
    pub now_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDeletionRequest {
    pub deletion_id: DeletionId,
    pub tenant_id: TenantId,
    pub root: ResourceRef,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub impact_digest: ContentDigest,
    pub expected_resource_version: u64,
    pub now_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreDeletionRequest {
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub expected_resource_version: u64,
    pub now_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryDeletionRequest {
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub expected_resource_version: u64,
    pub now_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionTransitionRequest {
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub expected_state: DeletionOperationState,
    pub next_state: DeletionOperationState,
    pub expected_resource_version: u64,
    pub now_unix_ms: UnixMillis,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionListRequest {
    pub tenant_id: TenantId,
    pub states: Option<Vec<DeletionOperationState>>,
    pub after: Option<DeletionListCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionListCursor {
    pub created_at_unix_ms: UnixMillis,
    pub deletion_id: DeletionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionListPage {
    pub records: Vec<DeletionOperation>,
    pub next: Option<DeletionListCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRetentionHoldRequest {
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub retention_hold_id: RetentionHoldId,
    pub reason: String,
    pub expires_at_unix_ms: Option<UnixMillis>,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub expected_resource_version: u64,
    pub now_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseRetentionHoldRequest {
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub retention_hold_id: RetentionHoldId,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub expected_resource_version: u64,
    pub now_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleAssignmentOutboxRecord {
    pub assignment: AgentResourceLifecycleAssignment,
    pub published: bool,
    pub retired: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_report_digest: Option<ContentDigest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleAssignmentInsertOutcome {
    Inserted(LifecycleAssignmentOutboxRecord),
    Existing(LifecycleAssignmentOutboxRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvidenceBatch {
    pub event: Option<LifecycleEvent>,
    pub proof: Option<DeletionProof>,
}

/// Authority-side mutation phase which is committed independently from the Catalog lifecycle
/// transaction. The deletion ID and target generation make retries safe across process restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityLifecycleAction {
    Quiesce,
    Finalize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLifecycleRequest {
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub target: ResourceRef,
    pub lifecycle_generation: neoengram_domain::protocol::LifecycleGeneration,
    pub request_digest: ContentDigest,
    pub occurred_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLifecycleRecord {
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub target: ResourceRef,
    pub action: AuthorityLifecycleAction,
    pub lifecycle_generation: neoengram_domain::protocol::LifecycleGeneration,
    pub request_digest: ContentDigest,
    pub completed_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityLifecycleMutationOutcome {
    pub record: AuthorityLifecycleRecord,
    pub replayed: bool,
}

/// Best-effort Authority-side facts captured alongside a Catalog deletion impact.
///
/// The Catalog still performs the authoritative resource-version fence. These values make the
/// confirmation dialog useful and are recomputed by the Authority Saga before destructive work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityLifecycleImpact {
    pub active_job_count: DecimalU64,
    pub estimated_file_count: DecimalU64,
    pub estimated_bytes: DecimalU64,
}
