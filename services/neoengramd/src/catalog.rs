use neoengram_core::ContentDigest;
use neoengram_protocol::{
    ArtifactId, EdgeClusterId, PlaygroundId, ProjectId, RequestId, SnapshotId, StorageVolumeId,
    TenantId, UnixMillis,
};

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
    pub pvc_reference: Option<CatalogPvcReference>,
    pub nfs_reference: Option<CatalogNfsReference>,
    pub state: StorageVolumeState,
    pub resource_version: u64,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
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

/// Authoritative placement of one immutable Artifact Commit as a read-only FUSE view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub snapshot_id: SnapshotId,
    /// Public idempotency identity. It is never accepted as the Snapshot resource identity.
    pub snapshot_request_id: RequestId,
    pub commit_id: ContentDigest,
    pub storage_volume_id: StorageVolumeId,
    pub region: String,
    pub state: SnapshotState,
    pub phase: SnapshotPhase,
    /// Relative to the approved Agent Volume mount; never an absolute host path.
    pub relative_root: String,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotState {
    Creating,
    Ready,
    Abnormal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPhase {
    Planning,
    Materializing,
    Verifying,
    Idle,
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
    pub storage_volume_id: Option<StorageVolumeId>,
    pub region: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotInsertOutcome {
    Inserted(SnapshotRecord),
    ExistingRequest(SnapshotRecord),
    ExistingPlacement(SnapshotRecord),
}

/// One atomic control-catalog publication of an immutable Commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvancePlaygroundCommitRequest {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
    pub expected_head_commit_id: Option<ContentDigest>,
    pub commit_id: ContentDigest,
    pub updated_at_unix_ms: UnixMillis,
}

/// Artifact and Playground heads observed after a successful publication CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvancePlaygroundCommitOutcome {
    pub artifact: ArtifactRecord,
    pub playground: PlaygroundRecord,
    pub replayed: bool,
}
