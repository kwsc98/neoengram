use std::{
    collections::{BTreeMap, BTreeSet},
    net::Ipv4Addr,
    str::FromStr,
    sync::Arc,
};

use crate::{
    diff_index_snapshots, summarize_index_changes, AgentRegistryService, ArtifactHeadExpectation,
    ArtifactInitialization as CatalogArtifactInitialization, ArtifactListCursor,
    ArtifactListRequest, ArtifactRecord, AuthorityLifecycleRepository, CatalogInsertOutcome,
    CatalogNfsReference, CatalogPvcReference, Clock, CommitRecord, ControlCatalogRepository,
    CreateDeletionRequest as DomainCreateDeletionRequest,
    CreateRetentionHoldRequest as DomainCreateRetentionHoldRequest, DeletionImpactQuery,
    DeletionListCursor, DeletionListRequest, GatewayCredentialState, GatewayPoolState,
    GatewayReplicaState, IndexKey, IndexPublisher, IndexSnapshotChangeKind,
    PlaygroundInsertRequest, PlaygroundListCursor, PlaygroundListRequest, PlaygroundRecord,
    PlaygroundState, PreCommitCancelRequest as DomainPreCommitCancelRequest, PreCommitId,
    PreCommitKey, PreCommitPhase, PreCommitRecord, PreCommitRepository,
    PreCommitRestartRequest as DomainPreCommitRestartRequest,
    PreCommitStartRequest as DomainPreCommitStartRequest, PreCommitState, ProjectListCursor,
    ProjectListRequest, ProjectRecord,
    ReleaseRetentionHoldRequest as DomainReleaseRetentionHoldRequest,
    RestoreDeletionRequest as DomainRestoreDeletionRequest,
    RetryDeletionRequest as DomainRetryDeletionRequest, S3AccessPointListRequest,
    S3AccessPointRecord, S3AccessPointState, S3CredentialRecord, S3CredentialState, S3MutationKind,
    S3MutationRecord, SnapshotDeliveryInsertRequest, SnapshotDeliveryRecord, SnapshotInsertRequest,
    SnapshotListCursor, SnapshotListRequest, SnapshotRecord, SnapshotState, StorageAccessMode,
    StorageBackendType, StorageVolumeListCursor, StorageVolumeListRequest, StorageVolumeRecord,
    StorageVolumeState, TenantListCursor, TenantListRequest, TenantRecord,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::{CommitId, ContentDigest, FileRecord, LogicalPath};
use neoengram_domain::protocol::materialization::{
    CoverageState, DurabilityPolicy, NamespaceObjectSet, ObjectPlacement, VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    presign_s3_get, AgentId, ArtifactId, CommitDataLayout, CommitObject, DataHealth,
    DeletionCompletion, DeletionId, DeletionOperation, DeletionOperationState, DeliveryGeneration,
    EdgeClusterId, GatewayOpaqueBytes, GatewayPoolId, GatewayS3ReadRevocation, HardlinkPolicy,
    IndexRevision, JobId, LifecycleGeneration, MountGeneration, ObjectNamespaceId, ObjectSet,
    OwnerGeneration, PlacementGeneration, PlaygroundId, ProjectId, PvcIdentityDigest, RequestId,
    ResourceLifecycle, ResourceLifecycleState, ResourceRef, ResourceVersion, RetentionHold,
    RetentionHoldId, RetentionHoldState, S3AccessPointId, S3AuthorizeOperation, S3AuthorizeRequest,
    S3AuthorizeResponse, S3AuthorizedObject, S3CredentialId, S3PresignRequest, S3ReadTicket,
    SessionGeneration, SnapshotDeliveryMode, SnapshotDeliveryOperation, SnapshotDeliveryPolicy,
    SnapshotDeliveryState, SnapshotId, StorageVolumeId, TaskActor, TaskKind, TaskResourceKind,
    TaskResourceLink, TaskResourceRole, TaskScope, TaskState, TenantId, UnixMillis,
    WireIndexVersion,
};
use ring::hmac;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    dto::{
        ArtifactInitialization as ArtifactInitializationBody, ArtifactView, CancelPreCommitRequest,
        CancelPreCommitResponse, CommitDiffEntry, CommitDiffSummary, CommitDiffView,
        CommitGraphView, CommitNodeView, CommitPlaygroundRequest, CommitPlaygroundResponse,
        CreateArtifactRequest, CreateArtifactResponse, CreateDeletionRequest,
        CreatePlaygroundRequest, CreatePlaygroundResponse, CreateProjectRequest,
        CreateProjectResponse, CreateRetentionHoldRequest, CreateRetentionHoldResponse,
        CreateS3AccessPointRequest, CreateS3AccessPointResponse, CreateS3CredentialRequest,
        CreateS3CredentialResponse, CreateS3DownloadUrlRequest, CreateS3DownloadUrlResponse,
        CreateSnapshotRequest, CreateSnapshotResponse, CreateStorageVolumeRequest,
        CreateStorageVolumeResponse, CreateTenantRequest, CreateTenantResponse,
        DatasetProfileSummary, DatasetProfileView, DeletionBlockerView, DeletionImpactView,
        DeletionMutationResponse, DeletionOperationView, DeletionTargetView, FileMetadataView,
        IndexVersionBody, LogicalFileEntry, PlaygroundChangeEntry, PlaygroundChangeSummary,
        PlaygroundView, PreCommitCheckView, PreCommitDiffSummaryView, PreCommitNoticeView,
        PreCommitProgressView, PreCommitView, ProjectView, PvcReference,
        QueryArtifactCommitDiffRequest, QueryArtifactCommitDiffResponse,
        QueryArtifactCommitGraphRequest, QueryArtifactCommitGraphResponse,
        QueryArtifactListRequest, QueryArtifactListResponse, QueryArtifactRequest,
        QueryArtifactResponse, QueryDeletionImpactRequest, QueryDeletionImpactResponse,
        QueryDeletionListRequest, QueryDeletionListResponse, QueryDeletionRequest,
        QueryDeletionResponse, QueryPlaygroundChangeListRequest, QueryPlaygroundChangeListResponse,
        QueryPlaygroundDatasetProfileRequest, QueryPlaygroundDatasetProfileResponse,
        QueryPlaygroundFileListRequest, QueryPlaygroundFileListResponse,
        QueryPlaygroundFileMetadataRequest, QueryPlaygroundFileMetadataResponse,
        QueryPlaygroundListRequest, QueryPlaygroundListResponse, QueryPlaygroundRequest,
        QueryPlaygroundResponse, QueryPreCommitRequest, QueryPreCommitResponse,
        QueryProjectListRequest, QueryProjectListResponse, QueryS3AccessPointListRequest,
        QueryS3AccessPointListResponse, QueryS3AccessPointRequest, QueryS3AccessPointResponse,
        QueryS3CredentialListRequest, QueryS3CredentialListResponse, QueryS3ObjectListRequest,
        QueryS3ObjectListResponse, QuerySnapshotListRequest, QuerySnapshotListResponse,
        QuerySnapshotRequest, QuerySnapshotResponse, QueryStorageVolumeListRequest,
        QueryStorageVolumeListResponse, QueryStorageVolumeRequest, QueryStorageVolumeResponse,
        QueryTenantListRequest, QueryTenantListResponse, QueryTenantRequest, QueryTenantResponse,
        ReleaseRetentionHoldRequest, ReleaseRetentionHoldResponse, ResourceIssueSummary,
        ResourceLifecycleView, ResourceRefBody, RestartPreCommitRequest, RestartPreCommitResponse,
        RetentionHoldView, RevokeS3CredentialRequest, S3AccessPointView, S3CredentialView,
        S3ObjectEntryView, SnapshotDeliveryMode as DeliveryModeBody, SnapshotIntegritySummary,
        SnapshotView, StartPreCommitRequest, StartPreCommitResponse, StorageVolumeView, TaskView,
        TenantView, UpdateDeletionRequest, UpdateS3AccessPointRequest, UpdateS3AccessPointResponse,
    },
    error::{application_error, invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy, TenantVisibility},
};

use super::{LocalS3SecretEnvelope, S3SecretEnvelope};

use super::CentralCommandKeyring;

const DEFAULT_PAGE_SIZE: u16 = 50;
const MAX_PAGE_SIZE: u16 = 100;
const CURSOR_PREFIX: &str = "ngcat_v1_";
const MAX_QUERY_CHARS: usize = 256;

/// Resolves live, Agent-derived availability for one Tenant-scoped StorageVolume.
///
/// Production composition uses [`AgentRegistryService`]. Keeping this boundary explicit makes a
/// missing Registry distinguishable from an unreachable Volume instead of silently treating both
/// as ready.
#[async_trait]
pub trait StorageAvailabilityProvider: Send + Sync {
    async fn current_volume_state(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> crate::CentralResult<crate::DerivedVolumeState>;

    /// Returns heartbeat-backed free space for the current ready mount.
    async fn current_available_bytes(
        &self,
        _tenant_id: &TenantId,
        _storage_volume_id: &StorageVolumeId,
    ) -> crate::CentralResult<Option<u64>> {
        Ok(None)
    }
}

#[cfg(test)]
mod v2_readiness_tests {
    use super::*;

    fn coverage(state: CoverageState) -> VolumeCommitCoverage {
        VolumeCommitCoverage {
            tenant_id: TenantId::new("tenant-v2").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-v2").unwrap(),
            commit_id: CommitId::from_bytes([1; 32]),
            storage_volume_id: StorageVolumeId::new("volume-v2").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            object_set_digest: ContentDigest::from_bytes([2; 32]),
            object_count: neoengram_domain::protocol::DecimalU64::new(2),
            verified_object_count: neoengram_domain::protocol::DecimalU64::new(
                if state == CoverageState::Complete {
                    2
                } else {
                    1
                },
            ),
            total_bytes: neoengram_domain::protocol::DecimalU64::new(10),
            verified_bytes: neoengram_domain::protocol::DecimalU64::new(
                if state == CoverageState::Complete {
                    10
                } else {
                    4
                },
            ),
            state,
        }
    }

    #[test]
    fn only_complete_v2_coverage_is_readable() {
        assert!(!CatalogService::v2_coverage_is_readable(None));
        assert!(!CatalogService::v2_coverage_is_readable(Some(&coverage(
            CoverageState::Partial
        ))));
        assert!(CatalogService::v2_coverage_is_readable(Some(&coverage(
            CoverageState::Complete
        ))));
    }
}

#[async_trait]
impl StorageAvailabilityProvider for AgentRegistryService {
    async fn current_volume_state(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> crate::CentralResult<crate::DerivedVolumeState> {
        AgentRegistryService::current_volume_state(self, tenant_id, storage_volume_id).await
    }

    async fn current_available_bytes(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> crate::CentralResult<Option<u64>> {
        Ok(self
            .current_ready_volume_record(tenant_id, storage_volume_id)
            .await?
            .and_then(|record| record.mount.available_bytes))
    }
}

#[cfg(test)]
mod commit_diff_tests {
    use super::*;

    fn file(path: &str, manifest: u8, size: u64) -> FileRecord {
        FileRecord::new(
            LogicalPath::parse(path).unwrap(),
            neoengram_domain::core::ManifestId::from_bytes([manifest; 32]),
            size,
            u64::from(size > 0),
        )
        .unwrap()
    }

    fn commit(
        id: u8,
        parent_commit_id: Option<CommitId>,
        records: Vec<FileRecord>,
    ) -> CommitRecord {
        CommitRecord {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            source_playground_id: PlaygroundId::new("playground-a").unwrap(),
            source_precommit_id: PreCommitId::new(format!("precommit-{id}")).unwrap(),
            commit_request_id: RequestId::new(format!("request-{id}")).unwrap(),
            commit_id: CommitId::from_bytes([id; 32]),
            object_set_digest: ContentDigest::from_bytes([id; 32]),
            root_directory_id: neoengram_domain::core::DirectoryId::from_bytes([id; 32]),
            parent_commit_id,
            index_version: WireIndexVersion {
                revision: IndexRevision::new(u64::from(id)),
                digest: ContentDigest::from_bytes([id; 32]),
                extensions: neoengram_domain::protocol::Extensions::new(),
            },
            data_layout: CommitDataLayout::FastCdc,
            records,
            message: format!("Commit {id}"),
            description: None,
            tag_names: Vec::new(),
            created_at_unix_ms: UnixMillis::new(u64::from(id)),
        }
    }

    #[test]
    fn commit_diff_projects_root_and_explicit_base_without_private_metadata() {
        let root = commit(1, None, vec![file("root.txt", 1, 5)]);
        let root_diff = build_commit_diff_view(None, &root).unwrap();
        assert!(root_diff.base_commit.is_none());
        assert_eq!(root_diff.summary.files_added, "1");
        assert_eq!(root_diff.summary.bytes_added, "5");
        assert_eq!(root_diff.changes[0].path, "root.txt");

        let parent = commit(
            2,
            None,
            vec![file("data.txt", 2, 10), file("old.txt", 3, 4)],
        );
        let target = commit(
            3,
            Some(parent.commit_id),
            vec![file("data.txt", 4, 13), file("new.txt", 3, 4)],
        );
        let explicit_diff = build_commit_diff_view(Some(&parent), &target).unwrap();
        assert_eq!(
            explicit_diff.base_commit.as_ref().unwrap().commit_id,
            parent.commit_id.to_string()
        );
        assert_eq!(
            explicit_diff.target_commit.commit_id,
            target.commit_id.to_string()
        );
        assert_eq!(explicit_diff.summary.files_modified, "1");
        assert_eq!(explicit_diff.summary.files_renamed, "1");
        assert_eq!(explicit_diff.summary.bytes_added, "3");
        assert_eq!(
            explicit_diff.changes[1].previous_path.as_deref(),
            Some("old.txt")
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3AgentPlacement {
    pub agent_id: AgentId,
    pub owner_generation: OwnerGeneration,
    pub mount_generation: MountGeneration,
    pub session_generation: SessionGeneration,
}

#[async_trait]
pub trait S3PlacementProvider: Send + Sync {
    async fn current_placement(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> crate::CentralResult<Option<S3AgentPlacement>>;
}

#[async_trait]
impl S3PlacementProvider for AgentRegistryService {
    async fn current_placement(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> crate::CentralResult<Option<S3AgentPlacement>> {
        let Some(record) = self
            .current_ready_volume_record(tenant_id, storage_volume_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(instance) = record.instance else {
            return Ok(None);
        };
        let Some(session_generation) = instance.session_generation else {
            return Ok(None);
        };
        if record.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
            || record.owner.active_agent_mount_id.as_ref() != Some(&record.mount.agent_mount_id)
        {
            return Ok(None);
        }
        Ok(Some(S3AgentPlacement {
            agent_id: instance.agent_id,
            owner_generation: record.owner.owner_generation,
            mount_generation: record.mount.mount_generation,
            session_generation,
        }))
    }
}

/// Publishes monotonic S3 authorization fences after the Catalog transaction has committed.
#[async_trait]
pub trait S3ReadRevocationPublisher: Send + Sync {
    async fn publish_s3_read_revocation(
        &self,
        gateway_pool_id: &GatewayPoolId,
        revocation: GatewayS3ReadRevocation,
    );
}

/// Public Tenant, StorageVolume, and minimal Playground application service.
pub struct CatalogService {
    pub(crate) repository: Arc<dyn ControlCatalogRepository>,
    pub(crate) indexes: Arc<dyn IndexPublisher>,
    pub(crate) policy: Arc<StaticRbacPolicy>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) coordinator: Option<Arc<super::JobCoordinator>>,
    pub(crate) precommits: Option<Arc<dyn PreCommitRepository>>,
    workspace_commits: Option<Arc<super::WorkspaceCommitService>>,
    pub(crate) storage_availability: Option<Arc<dyn StorageAvailabilityProvider>>,
    pub(crate) s3_placement: Option<Arc<dyn S3PlacementProvider>>,
    pub(crate) agent_registry: Option<Arc<AgentRegistryService>>,
    pub(crate) gateway_registry: Option<Arc<dyn crate::GatewayRegistryRepository>>,
    pub(crate) placement: Option<Arc<dyn crate::PlacementRepository>>,
    pub(crate) task_coordinator: Option<Arc<super::TaskCoordinator>>,
    /// Namespace-scoped durability policy overrides.  The default policy is one verified copy in
    /// one failure domain; callers can install a stricter policy at composition time without
    /// making durability an implicit property of a complete Volume.
    pub(crate) default_durability_policy: DurabilityPolicy,
    pub(crate) durability_policies: BTreeMap<(TenantId, ObjectNamespaceId), DurabilityPolicy>,
    pub(crate) replication_ticket_keyring: Option<Arc<CentralCommandKeyring>>,
    lifecycle_objects: Option<Arc<dyn crate::ObjectCatalog>>,
    lifecycle_authority: Option<Arc<dyn AuthorityLifecycleRepository>>,
    s3_read_revocations: Option<Arc<dyn S3ReadRevocationPublisher>>,
    s3_ticket_keyring: Option<Arc<CentralCommandKeyring>>,
    s3_secret_envelope: Arc<dyn S3SecretEnvelope>,
    s3_cursor_signing_key: Arc<[u8; 32]>,
}

impl CatalogService {
    pub(crate) fn v2_coverage_is_readable(coverage: Option<&VolumeCommitCoverage>) -> bool {
        coverage.is_some_and(|coverage| coverage.state == CoverageState::Complete)
    }

    #[must_use]
    pub fn new(
        repository: Arc<dyn ControlCatalogRepository>,
        indexes: Arc<dyn IndexPublisher>,
        policy: Arc<StaticRbacPolicy>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let local_envelope =
            LocalS3SecretEnvelope::new("development-v1", development_s3_envelope_key())
                .expect("static development S3 envelope key ID is valid");
        let cursor_signing_key = local_envelope.cursor_signing_key();
        Self {
            repository,
            indexes,
            policy,
            clock,
            coordinator: None,
            precommits: None,
            workspace_commits: None,
            storage_availability: None,
            s3_placement: None,
            agent_registry: None,
            gateway_registry: None,
            placement: None,
            task_coordinator: None,
            default_durability_policy: DurabilityPolicy::default(),
            durability_policies: BTreeMap::new(),
            replication_ticket_keyring: None,
            lifecycle_objects: None,
            lifecycle_authority: None,
            s3_read_revocations: None,
            s3_ticket_keyring: None,
            s3_secret_envelope: Arc::new(local_envelope),
            s3_cursor_signing_key: Arc::new(cursor_signing_key),
        }
    }

    #[must_use]
    pub fn with_coordinator(mut self, coordinator: Arc<super::JobCoordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
    }

    #[must_use]
    pub fn with_precommits(mut self, precommits: Arc<dyn PreCommitRepository>) -> Self {
        self.precommits = Some(precommits);
        self
    }

    #[must_use]
    pub fn with_lifecycle_objects(mut self, objects: Arc<dyn crate::ObjectCatalog>) -> Self {
        self.lifecycle_objects = Some(objects);
        self
    }

    #[must_use]
    pub fn with_lifecycle_authority(
        mut self,
        authority: Arc<dyn AuthorityLifecycleRepository>,
    ) -> Self {
        self.lifecycle_authority = Some(authority);
        self
    }

    #[must_use]
    pub fn with_workspace_commits(
        mut self,
        workspace_commits: Arc<super::WorkspaceCommitService>,
    ) -> Self {
        self.workspace_commits = Some(workspace_commits);
        self
    }

    #[must_use]
    pub fn with_agent_registry(mut self, agent_registry: Arc<AgentRegistryService>) -> Self {
        self.agent_registry = Some(agent_registry.clone());
        self.storage_availability = Some(agent_registry.clone());
        self.s3_placement = Some(agent_registry);
        self
    }

    #[must_use]
    pub fn with_storage_availability_provider(
        mut self,
        storage_availability: Arc<dyn StorageAvailabilityProvider>,
    ) -> Self {
        self.storage_availability = Some(storage_availability);
        self
    }

    #[must_use]
    pub fn with_gateway_registry(
        mut self,
        gateway_registry: Arc<dyn crate::GatewayRegistryRepository>,
    ) -> Self {
        self.gateway_registry = Some(gateway_registry);
        self
    }

    #[must_use]
    pub fn with_placement_repository(
        mut self,
        placement: Arc<dyn crate::PlacementRepository>,
    ) -> Self {
        self.placement = Some(placement);
        self
    }

    #[must_use]
    pub fn with_task_coordinator(mut self, task_coordinator: Arc<super::TaskCoordinator>) -> Self {
        self.task_coordinator = Some(task_coordinator);
        self
    }

    /// Creates the unified task envelope for a mutating request.  Standalone unit compositions
    /// may omit the task repository; production runtime always installs it from AuthorityStore.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn begin_operation_task<T: Serialize>(
        &self,
        kind: TaskKind,
        scope: TaskScope,
        request_id: RequestId,
        request: &T,
        identity: &AuthenticatedIdentity,
        detail_kind: Option<&str>,
        detail_id: Option<&str>,
    ) -> Result<(Option<TaskView>, bool), Error> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok((None, false));
        };
        let (task, replayed) = coordinator
            .create_root(
                kind,
                scope,
                request_id,
                request,
                TaskActor::Principal(identity.principal().clone()),
                detail_kind,
                detail_id,
            )
            .await
            .map_err(map_central_error)?;
        Ok((Some(super::task::task_view(&task)), replayed))
    }

    fn derived_task_request_id<T: Serialize>(
        kind: TaskKind,
        request: &T,
    ) -> Result<RequestId, Error> {
        let digest = neoengram_domain::jcs_blake3(request)
            .map_err(|error| invalid_request(format!("task request: {error}")))?;
        RequestId::new(format!(
            "{}-{}",
            kind.as_str().replace('.', "-"),
            &digest.to_hex()[..32]
        ))
        .map_err(|error| invalid_request(format!("task request_id: {error}")))
    }

    pub(crate) async fn link_operation_resource(
        &self,
        task: &Option<TaskView>,
        kind: TaskResourceKind,
        resource_id: impl Into<String>,
        role: TaskResourceRole,
    ) -> Result<(), Error> {
        let (Some(coordinator), Some(task)) = (&self.task_coordinator, task) else {
            return Ok(());
        };
        let task_id = neoengram_domain::protocol::TaskId::new(task.task_id.clone())
            .map_err(|error| invalid_request(format!("task_id: {error}")))?;
        coordinator
            .repository()
            .link_resource(crate::TaskResourceLinkRecord {
                tenant_id: TenantId::new(task.tenant_id.clone())
                    .map_err(|error| invalid_request(format!("tenant_id: {error}")))?,
                link: TaskResourceLink::new(task_id, kind, resource_id, role),
            })
            .await
            .map_err(map_central_error)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn begin_child_operation_task<T: Serialize>(
        &self,
        parent: &Option<TaskView>,
        kind: TaskKind,
        scope: TaskScope,
        request_id: RequestId,
        request: &T,
        identity: &AuthenticatedIdentity,
        detail_kind: Option<&str>,
        detail_id: Option<&str>,
    ) -> Result<Option<TaskView>, Error> {
        let (Some(coordinator), Some(parent)) = (&self.task_coordinator, parent) else {
            return Ok(None);
        };
        let parent_id = neoengram_domain::protocol::TaskId::new(parent.task_id.clone())
            .map_err(|error| invalid_request(format!("parent_task_id: {error}")))?;
        let parent_task = coordinator
            .repository()
            .get(
                &TenantId::new(parent.tenant_id.clone())
                    .map_err(|error| invalid_request(format!("tenant_id: {error}")))?,
                &parent_id,
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| invalid_request("parent operation task disappeared"))?;
        let (task, _) = coordinator
            .create_child(
                &parent_task,
                kind,
                scope,
                request_id,
                request,
                TaskActor::Principal(identity.principal().clone()),
                detail_kind,
                detail_id,
            )
            .await
            .map_err(map_central_error)?;
        Ok(Some(super::task::task_view(&task)))
    }

    pub(crate) async fn transition_operation_task(
        &self,
        task: Option<TaskView>,
        next: TaskState,
        identity: &AuthenticatedIdentity,
        message: Option<String>,
    ) -> Result<Option<TaskView>, Error> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok(task);
        };
        let Some(task) = task else {
            return Ok(None);
        };
        let task_id = neoengram_domain::protocol::TaskId::new(task.task_id.clone())
            .map_err(|error| invalid_request(format!("task_id: {error}")))?;
        let tenant_id = TenantId::new(task.tenant_id.clone())
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        let updated = coordinator
            .transition(
                &task_id,
                &tenant_id,
                next,
                TaskActor::Principal(identity.principal().clone()),
                message,
            )
            .await
            .map_err(map_central_error)?;
        Ok(Some(super::task::task_view(&updated)))
    }

    pub(crate) async fn complete_operation_task(
        &self,
        task: Option<TaskView>,
        identity: &AuthenticatedIdentity,
    ) -> Result<Option<TaskView>, Error> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok(task);
        };
        let Some(task) = task else {
            return Ok(None);
        };
        let task_id = neoengram_domain::protocol::TaskId::new(task.task_id.clone())
            .map_err(|error| invalid_request(format!("task_id: {error}")))?;
        let tenant_id = TenantId::new(task.tenant_id.clone())
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        let operation =
            neoengram_domain::protocol::TaskActor::Principal(identity.principal().clone());
        let current = coordinator
            .repository()
            .get(&tenant_id, &task_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| invalid_request("operation task disappeared"))?;
        let updated = coordinator
            .complete_immediate(&current, operation)
            .await
            .map_err(map_central_error)?;
        Ok(Some(super::task::task_view(&updated)))
    }

    /// Installs the policy used when a namespace has no explicit override.
    ///
    /// Policy validation is performed while composing the service so an invalid deployment
    /// cannot silently weaken an availability decision at request time.
    #[must_use]
    pub fn with_default_durability_policy(mut self, policy: DurabilityPolicy) -> Self {
        policy
            .validate()
            .expect("default durability policy must satisfy protocol constraints");
        self.default_durability_policy = policy;
        self
    }

    /// Installs a durability policy for one tenant/namespace pair.  Namespace IDs are not
    /// globally unique, so the tenant remains part of the lookup key.
    #[must_use]
    pub fn with_durability_policy(
        mut self,
        tenant_id: TenantId,
        object_namespace_id: ObjectNamespaceId,
        policy: DurabilityPolicy,
    ) -> Self {
        policy
            .validate()
            .expect("durability policy must satisfy protocol constraints");
        self.durability_policies
            .insert((tenant_id, object_namespace_id), policy);
        self
    }

    #[must_use]
    pub fn with_replication_ticket_keyring(mut self, keyring: Arc<CentralCommandKeyring>) -> Self {
        self.replication_ticket_keyring = Some(keyring);
        self
    }

    #[must_use]
    pub fn with_s3_read_revocation_publisher(
        mut self,
        publisher: Arc<dyn S3ReadRevocationPublisher>,
    ) -> Self {
        self.s3_read_revocations = Some(publisher);
        self
    }

    #[must_use]
    pub fn with_s3_placement_provider(mut self, placement: Arc<dyn S3PlacementProvider>) -> Self {
        self.s3_placement = Some(placement);
        self
    }

    #[must_use]
    pub fn with_s3_ticket_keyring(mut self, keyring: Arc<CentralCommandKeyring>) -> Self {
        self.s3_ticket_keyring = Some(keyring);
        self
    }

    /// Installs a local wrapping key for development and integration fixtures.
    #[must_use]
    pub fn with_s3_envelope_key(mut self, key: [u8; 32]) -> Self {
        let digest = blake3::hash(&key).to_hex().to_string();
        let envelope = LocalS3SecretEnvelope::new(format!("local-{}", &digest[..16]), key)
            .expect("derived local S3 envelope key ID is valid");
        self.s3_cursor_signing_key = Arc::new(envelope.cursor_signing_key());
        self.s3_secret_envelope = Arc::new(envelope);
        self
    }

    /// Installs the production KMS/HSM envelope provider and a separately managed cursor HMAC key.
    #[must_use]
    pub fn with_s3_secret_envelope(
        mut self,
        envelope: Arc<dyn S3SecretEnvelope>,
        cursor_signing_key: [u8; 32],
    ) -> Self {
        self.s3_secret_envelope = envelope;
        self.s3_cursor_signing_key = Arc::new(cursor_signing_key);
        self
    }

    pub async fn list_tenants(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTenantListRequest,
    ) -> Result<QueryTenantListResponse, Error> {
        let visible_tenant_ids = match self
            .policy
            .tenant_visibility(identity.principal(), Permission::TenantRead)
        {
            TenantVisibility::None => Some(Vec::new()),
            TenantVisibility::All => None,
            TenantVisibility::Explicit(ids) => Some(ids),
        };
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Tenant {
            visible: visible_tenant_ids
                .as_ref()
                .map(|ids| ids.iter().map(ToString::to_string).collect()),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_tenant_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_tenants(&TenantListRequest {
                visible_tenant_ids,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let items = page
            .records
            .iter()
            .map(|record| self.tenant_view(identity, record))
            .collect();
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_tenant_cursor(&scope, cursor))
            .transpose()?;
        let can_create_tenant = !matches!(
            self.policy
                .tenant_visibility(identity.principal(), Permission::TenantCreate),
            TenantVisibility::None
        );
        Ok(QueryTenantListResponse {
            items,
            next_cursor,
            can_create_tenant,
        })
    }

    pub async fn query_tenant(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTenantRequest,
    ) -> Result<QueryTenantResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::TenantRead, &tenant_id)
        {
            return Err(resource_not_found("tenant"));
        }
        let record = self
            .repository
            .get_tenant(&tenant_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("tenant"))?;
        Ok(QueryTenantResponse {
            tenant: self.tenant_view(identity, &record),
        })
    }

    pub async fn create_tenant(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateTenantRequest,
    ) -> Result<CreateTenantResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.policy
            .authorize_identity(identity, Permission::TenantCreate, &tenant_id)?;
        let display_name = validate_display_name(request.display_name.clone())?;
        let description = request.description.map(validate_description).transpose()?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                TaskScope::new(tenant_id.clone()),
                Self::derived_task_request_id(TaskKind::CatalogLifecycle, &task_request)?,
                &task_request,
                identity,
                Some("tenant"),
                Some(tenant_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Tenant,
            tenant_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_tenant(TenantRecord {
                tenant_id,
                display_name,
                description,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "tenant_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CreateTenantResponse {
            tenant: self.tenant_view(identity, &record),
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn list_projects(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryProjectListRequest,
    ) -> Result<QueryProjectListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ProjectRead, &tenant_id)
            .await?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Project {
            tenant_id: tenant_id.to_string(),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_project_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_projects(&ProjectListRequest {
                tenant_id,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_project_cursor(&scope, cursor))
            .transpose()?;
        Ok(QueryProjectListResponse {
            items: page.records.iter().map(project_view).collect(),
            next_cursor,
        })
    }

    pub async fn create_project(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateProjectRequest,
    ) -> Result<CreateProjectResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ProjectCreate, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let display_name = validate_display_name(request.display_name.clone())?;
        let description = request.description.map(validate_description).transpose()?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id.clone()),
                    artifact_id: None,
                    object_namespace_id: None,
                    commit_id: None,
                    playground_id: None,
                    snapshot_id: None,
                    storage_volume_id: None,
                },
                Self::derived_task_request_id(TaskKind::CatalogLifecycle, &task_request)?,
                &task_request,
                identity,
                Some("project"),
                Some(project_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Project,
            project_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_project(ProjectRecord {
                tenant_id,
                project_id,
                display_name,
                description,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "project_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CreateProjectResponse {
            project: project_view(&record),
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn list_storage_volumes(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryStorageVolumeListRequest,
    ) -> Result<QueryStorageVolumeListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        let can_read_storage =
            self.policy
                .is_allowed(identity.principal(), Permission::StorageRead, &tenant_id);
        let can_replicate = self.policy.is_allowed(
            identity.principal(),
            Permission::ArtifactCommitReplicate,
            &tenant_id,
        );
        // Snapshot creators must be able to choose a target Volume, but do not need the broader
        // storage administration permission.  The response is already sanitized below (for
        // example PVC/NFS locators are omitted unless `storage.read` is granted).
        let can_create_snapshot =
            self.policy
                .is_allowed(identity.principal(), Permission::SnapshotCreate, &tenant_id);
        if !can_read_storage && !can_replicate && !can_create_snapshot {
            return Err(resource_not_found("tenant"));
        }
        self.repository
            .get_tenant(&tenant_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("tenant"))?;
        let region = request.region.map(validate_region).transpose()?;
        let backend_type = request
            .backend_type
            .as_deref()
            .map(parse_backend_type)
            .transpose()?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Volume {
            tenant_id: tenant_id.to_string(),
            region: region.clone(),
            backend_type: backend_type.map(backend_name).map(str::to_owned),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_volume_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_storage_volumes(&StorageVolumeListRequest {
                tenant_id,
                region,
                backend_type,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_volume_cursor(&scope, cursor))
            .transpose()?;
        let mut items = Vec::with_capacity(page.records.len());
        for record in page
            .records
            .iter()
            .filter(|record| record.lifecycle.is_active())
        {
            let mut view = self.storage_volume_view_with_live_state(record).await?;
            if !can_read_storage {
                view.pvc_reference = None;
            }
            items.push(view);
        }
        Ok(QueryStorageVolumeListResponse { items, next_cursor })
    }

    pub async fn query_storage_volume(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryStorageVolumeRequest,
    ) -> Result<QueryStorageVolumeResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        let storage_volume_id = parse_volume_id(request.storage_volume_id)?;
        let can_read_storage =
            self.policy
                .is_allowed(identity.principal(), Permission::StorageRead, &tenant_id);
        if let Some(snapshot_id) = request.snapshot_id {
            let snapshot_id = parse_snapshot_id(snapshot_id)?;
            if !self
                .policy
                .is_allowed(identity.principal(), Permission::SnapshotRead, &tenant_id)
            {
                return Err(resource_not_found("storage volume"));
            }
            let snapshot = self
                .repository
                .get_snapshot(&tenant_id, &snapshot_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| resource_not_found("storage volume"))?;
            require_active_for_read(&snapshot.lifecycle, "snapshot")?;
            if snapshot.storage_volume_id != storage_volume_id {
                // Do not disclose whether the requested Volume exists when the Snapshot does not
                // bind it. The caller only receives the same not-found response as an unknown ID.
                return Err(resource_not_found("storage volume"));
            }
        } else if !can_read_storage {
            return Err(resource_not_found("storage volume"));
        }
        let record = self
            .repository
            .get_storage_volume(&tenant_id, &storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        require_active_for_read(&record.lifecycle, "storage volume")?;
        Ok(QueryStorageVolumeResponse {
            storage_volume: {
                let mut view = self.storage_volume_view_with_live_state(&record).await?;
                if !can_read_storage {
                    view.pvc_reference = None;
                }
                view
            },
        })
    }

    pub async fn create_storage_volume(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateStorageVolumeRequest,
    ) -> Result<CreateStorageVolumeResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::StorageCreate, &tenant_id)
            .await?;
        let storage_volume_id = parse_volume_id(request.storage_volume_id)?;
        let display_name = validate_display_name(request.display_name)?;
        let edge_cluster_id = EdgeClusterId::new(request.edge_cluster_id)
            .map_err(|error| invalid_request(format!("edge_cluster_id: {error}")))?;
        let region = validate_region(request.region)?;
        let backend_type = parse_backend_type(&request.backend_type)?;
        let access_mode = parse_access_mode(&request.access_mode)?;
        let allowed_delivery_modes = if request.allowed_delivery_modes.is_empty() {
            vec![SnapshotDeliveryMode::Fuse, SnapshotDeliveryMode::Copy]
        } else {
            request
                .allowed_delivery_modes
                .iter()
                .map(|mode| parse_delivery_mode(mode))
                .collect::<Result<Vec<_>, _>>()?
        };
        let hardlink_policy = parse_hardlink_policy_body(request.hardlink_policy)?;
        let max_whole_file_bytes = request
            .max_whole_file_bytes
            .as_deref()
            .unwrap_or("18446744073709551615")
            .parse::<neoengram_domain::protocol::DecimalU64>()
            .map_err(|error| invalid_request(format!("max_whole_file_bytes: {error}")))?;
        let copy_reserve_bytes = request
            .copy_reserve_bytes
            .as_deref()
            .unwrap_or("0")
            .parse::<neoengram_domain::protocol::DecimalU64>()
            .map_err(|error| invalid_request(format!("copy_reserve_bytes: {error}")))?;
        SnapshotDeliveryPolicy {
            allowed_modes: allowed_delivery_modes.clone(),
            hardlink_policy,
            max_whole_file_bytes,
            copy_reserve_bytes,
            extensions: neoengram_domain::protocol::Extensions::new(),
        }
        .validate()
        .map_err(|error| invalid_request(format!("delivery policy: {error}")))?;
        let (pvc_reference, nfs_reference) = match backend_type {
            StorageBackendType::Pvc => {
                if request.nfs_reference.is_some() {
                    return Err(invalid_request(
                        "nfs_reference is forbidden when backend_type is pvc",
                    ));
                }
                let reference = request.pvc_reference.ok_or_else(|| {
                    invalid_request("pvc_reference is required when backend_type is pvc")
                })?;
                PvcIdentityDigest::derive(&reference.namespace, &reference.claim_name)
                    .map_err(|error| invalid_request(error.to_string()))?;
                (
                    Some(CatalogPvcReference {
                        namespace: reference.namespace,
                        claim_name: reference.claim_name,
                    }),
                    None,
                )
            }
            StorageBackendType::Nfs => {
                if request.pvc_reference.is_some() {
                    return Err(invalid_request(
                        "pvc_reference is forbidden when backend_type is nfs",
                    ));
                }
                let reference = request.nfs_reference.ok_or_else(|| {
                    invalid_request("nfs_reference is required when backend_type is nfs")
                })?;
                let server = validate_nfs_server(reference.server)?;
                let export_path = validate_nfs_export_path(reference.export_path)?;
                (
                    None,
                    Some(CatalogNfsReference {
                        server,
                        export_path,
                    }),
                )
            }
        };
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::StorageLifecycle,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: None,
                    artifact_id: None,
                    object_namespace_id: None,
                    commit_id: None,
                    playground_id: None,
                    snapshot_id: None,
                    storage_volume_id: Some(storage_volume_id.clone()),
                },
                Self::derived_task_request_id(TaskKind::StorageLifecycle, &task_request)?,
                &task_request,
                identity,
                Some("storage_volume"),
                Some(storage_volume_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::StorageVolume,
            storage_volume_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id,
                storage_volume_id,
                display_name,
                edge_cluster_id,
                region,
                backend_type,
                access_mode,
                allowed_delivery_modes,
                hardlink_policy,
                max_whole_file_bytes,
                copy_reserve_bytes,
                pvc_reference,
                nfs_reference,
                state: StorageVolumeState::Unavailable,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "storage_volume_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CreateStorageVolumeResponse {
            storage_volume: storage_volume_view(&record),
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn list_artifacts(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryArtifactListRequest,
    ) -> Result<QueryArtifactListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactRead, &tenant_id)
            .await?;
        let project_id = request.project_id.map(parse_project_id).transpose()?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Artifact {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.as_ref().map(ToString::to_string),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_artifact_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_artifacts(&ArtifactListRequest {
                tenant_id,
                project_id,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_artifact_cursor(&scope, cursor))
            .transpose()?;
        Ok(QueryArtifactListResponse {
            items: page
                .records
                .iter()
                .filter(|record| record.lifecycle.is_active())
                .map(artifact_view)
                .collect(),
            next_cursor,
        })
    }

    pub async fn query_artifact(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryArtifactRequest,
    ) -> Result<QueryArtifactResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::ArtifactRead, &tenant_id)
        {
            return Err(resource_not_found("artifact"));
        }
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let record = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        require_active_for_read(&record.lifecycle, "artifact")?;
        Ok(QueryArtifactResponse {
            artifact: artifact_view(&record),
        })
    }

    pub async fn query_artifact_commit_graph(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryArtifactCommitGraphRequest,
    ) -> Result<QueryArtifactCommitGraphResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::ArtifactRead, &tenant_id)
        {
            return Err(resource_not_found("artifact"));
        }
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        require_active_for_read(&artifact.lifecycle, "artifact")?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::CommitGraph {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
            artifact_id: artifact_id.to_string(),
            graph_version: artifact.resource_version.to_string(),
            head_commit_id: artifact
                .head_commit_id
                .map(|commit_id| commit_id.to_string()),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_commit_cursor(cursor, &scope))
            .transpose()?;
        let precommits = self.precommit_repository()?;
        // The Artifact Head is only one tip. A historical Playground can publish a sibling while
        // another Playground advances that pointer, so walking parent links from the Head would
        // hide valid commits. Read the complete immutable scope and order the current Head first,
        // followed by newest commits; the UI can reconstruct the parent tree from each node.
        let mut all_commits = self
            .load_published_commit_graph(&artifact, precommits.as_ref())
            .await?;
        all_commits.sort_by(|left, right| {
            (Some(right.commit_id) == artifact.head_commit_id.map(CommitId::from_digest))
                .cmp(&(Some(left.commit_id) == artifact.head_commit_id.map(CommitId::from_digest)))
                .then_with(|| right.created_at_unix_ms.cmp(&left.created_at_unix_ms))
                .then_with(|| left.commit_id.cmp(&right.commit_id))
        });
        let start = if let Some(cursor) = &after {
            let Some(index) = all_commits.iter().position(|commit| {
                commit.commit_id == cursor.commit_id
                    && commit.created_at_unix_ms == cursor.created_at_unix_ms
            }) else {
                return Err(cursor_conflict());
            };
            index.saturating_add(1)
        } else {
            0
        };
        let page = all_commits
            .into_iter()
            .skip(start)
            .take(usize::from(page_size) + 1)
            .collect::<Vec<_>>();
        let has_more = page.len() > usize::from(page_size);
        let mut records = page;
        records.truncate(usize::from(page_size));
        let next = has_more.then(|| {
            let last = records.last().expect("non-empty Commit graph page");
            CommitGraphCursor {
                created_at_unix_ms: last.created_at_unix_ms,
                commit_id: last.commit_id,
            }
        });
        let next_cursor = next
            .as_ref()
            .map(|cursor| encode_commit_cursor(&scope, cursor))
            .transpose()?;
        let observed_artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        if observed_artifact.resource_version != artifact.resource_version
            || observed_artifact.head_commit_id != artifact.head_commit_id
        {
            return Err(if after.is_some() {
                cursor_conflict()
            } else {
                commit_graph_changed()
            });
        }
        Ok(QueryArtifactCommitGraphResponse {
            graph: CommitGraphView {
                graph_version: artifact.resource_version.to_string(),
                head_commit_id: artifact
                    .head_commit_id
                    .map(|commit_id| commit_id.to_string()),
                nodes: records.iter().map(commit_node_view).collect(),
                next_cursor,
            },
        })
    }

    pub async fn query_artifact_commit_diff(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryArtifactCommitDiffRequest,
    ) -> Result<QueryArtifactCommitDiffResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::ArtifactRead, &tenant_id)
        {
            return Err(resource_not_found("artifact"));
        }
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        require_active_for_read(&artifact.lifecycle, "artifact")?;

        let target_commit_id = parse_commit_id(request.commit_id)?;
        let requested_base_commit_id = request.base_commit_id.map(parse_commit_id).transpose()?;
        if requested_base_commit_id == Some(target_commit_id) {
            return Err(invalid_request("base_commit_id must differ from commit_id"));
        }
        let target = self
            .load_published_commit(&artifact, target_commit_id)
            .await?;
        let base = match requested_base_commit_id {
            Some(base_commit_id) => Some(
                self.load_published_commit(&artifact, base_commit_id)
                    .await?,
            ),
            None => match target.parent_commit_id {
                Some(parent_commit_id) => Some(
                    self.load_published_commit(&artifact, parent_commit_id)
                        .await?,
                ),
                None => None,
            },
        };
        Ok(QueryArtifactCommitDiffResponse {
            diff: build_commit_diff_view(base.as_ref(), &target)?,
        })
    }

    async fn load_published_commit_graph(
        &self,
        artifact: &ArtifactRecord,
        precommits: &dyn PreCommitRepository,
    ) -> Result<Vec<CommitRecord>, Error> {
        // Acknowledgements preserve old tips after their Playground moves. Current Head pointers
        // close the cross-store window where catalog publication committed but acknowledgement did
        // not. Walking every seed to its root also restores an unacknowledged published parent.
        let acknowledged = precommits
            .list_published_commits(
                &artifact.tenant_id,
                &artifact.project_id,
                &artifact.artifact_id,
            )
            .await
            .map_err(map_central_error)?;
        let mut commits = BTreeMap::new();
        let mut seeds = Vec::with_capacity(acknowledged.len().saturating_add(1));
        for commit in acknowledged {
            seeds.push(commit.commit_id);
            commits.insert(commit.commit_id, commit);
        }
        if let Some(head) = artifact.head_commit_id {
            seeds.push(CommitId::from_digest(head));
        }

        let mut after = None;
        loop {
            let page = self
                .repository
                .list_playgrounds(&PlaygroundListRequest {
                    tenant_id: artifact.tenant_id.clone(),
                    project_id: Some(artifact.project_id.clone()),
                    artifact_id: Some(artifact.artifact_id.clone()),
                    region: None,
                    state: None,
                    query: None,
                    after,
                    limit: MAX_PAGE_SIZE,
                })
                .await
                .map_err(map_central_error)?;
            seeds.extend(
                page.records
                    .iter()
                    .filter_map(|playground| playground.head_commit_id)
                    .map(CommitId::from_digest),
            );
            let Some(next) = page.next else {
                break;
            };
            after = Some(next);
        }

        let mut resolved = BTreeSet::new();
        for seed in seeds {
            if resolved.contains(&seed) {
                continue;
            }
            let mut current = Some(seed);
            let mut path = Vec::new();
            let mut path_ids = BTreeSet::new();
            while let Some(commit_id) = current {
                if resolved.contains(&commit_id) {
                    break;
                }
                if !path_ids.insert(commit_id) {
                    return Err(internal_catalog_error());
                }
                let parent = if let Some(commit) = commits.get(&commit_id) {
                    commit.parent_commit_id
                } else {
                    let commit = precommits
                        .get_commit(
                            &artifact.tenant_id,
                            &artifact.project_id,
                            &artifact.artifact_id,
                            commit_id,
                        )
                        .await
                        .map_err(map_central_error)?
                        .ok_or_else(internal_catalog_error)?;
                    let parent = commit.parent_commit_id;
                    commits.insert(commit.commit_id, commit);
                    parent
                };
                path.push(commit_id);
                current = parent;
            }
            resolved.extend(path);
        }
        Ok(commits.into_values().collect())
    }

    pub async fn create_artifact(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateArtifactRequest,
    ) -> Result<CreateArtifactResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCreate, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let display_name = validate_display_name(request.display_name)?;
        let description = request.description.map(validate_description).transpose()?;
        let initialization = match request.initialization {
            ArtifactInitializationBody::Empty => CatalogArtifactInitialization::Empty,
            ArtifactInitializationBody::Derived {
                source_project_id,
                source_artifact_id,
                source_commit_id,
            } => {
                parse_project_id(source_project_id)?;
                parse_artifact_id(source_artifact_id)?;
                ContentDigest::from_str(&source_commit_id).map_err(|_| {
                    invalid_request("initialization.source_commit_id must be a 64-character digest")
                })?;
                return Err(catalog_conflict(
                    "artifact_derived_initialization_unsupported",
                    "ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED",
                    "derived Artifact initialization requires authoritative Commit validation",
                ));
            }
        };
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id.clone()),
                    artifact_id: Some(artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(&artifact_id)),
                    commit_id: None,
                    playground_id: None,
                    snapshot_id: None,
                    storage_volume_id: None,
                },
                Self::derived_task_request_id(TaskKind::CatalogLifecycle, &task_request)?,
                &task_request,
                identity,
                Some("artifact"),
                Some(artifact_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Artifact,
            artifact_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_artifact(ArtifactRecord {
                tenant_id,
                project_id,
                artifact_id,
                display_name,
                description,
                initialization,
                head_commit_id: None,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "artifact_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CreateArtifactResponse {
            artifact: artifact_view(&record),
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn list_playgrounds(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundListRequest,
    ) -> Result<QueryPlaygroundListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundRead, &tenant_id)
            .await?;
        if request.artifact_id.is_some() && request.project_id.is_none() {
            return Err(invalid_request(
                "project_id is required when artifact_id is provided",
            ));
        }
        let project_id = request.project_id.map(parse_project_id).transpose()?;
        let artifact_id = request.artifact_id.map(parse_artifact_id).transpose()?;
        let region = request.region.map(validate_region).transpose()?;
        let state = request
            .state
            .as_deref()
            .map(parse_playground_state)
            .transpose()?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Playground {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.as_ref().map(ToString::to_string),
            artifact_id: artifact_id.as_ref().map(ToString::to_string),
            region: region.clone(),
            state: state.map(playground_state_name).map(str::to_owned),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_playground_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_playgrounds(&PlaygroundListRequest {
                tenant_id,
                project_id,
                artifact_id,
                region,
                state,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_playground_cursor(&scope, cursor))
            .transpose()?;
        let mut items = Vec::with_capacity(page.records.len());
        for record in &page.records {
            if !record.lifecycle.is_active() {
                continue;
            }
            items.push(self.playground_view(record).await?);
        }
        Ok(QueryPlaygroundListResponse { items, next_cursor })
    }

    pub async fn query_playground(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundRequest,
    ) -> Result<QueryPlaygroundResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::PlaygroundRead, &tenant_id)
        {
            return Err(resource_not_found("playground"));
        }
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let playground_id = parse_playground_id(request.playground_id)?;
        let record = self
            .repository
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("playground"))?;
        require_active_for_read(&record.lifecycle, "playground")?;
        Ok(QueryPlaygroundResponse {
            playground: self.playground_view(&record).await?,
        })
    }

    pub async fn create_playground(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreatePlaygroundRequest,
    ) -> Result<CreatePlaygroundResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let playground_id = parse_playground_id(request.playground_id)?;
        let storage_volume_id = parse_volume_id(request.storage_volume_id)?;
        let display_name = validate_display_name(request.display_name.clone())?;
        let requested_base_commit_id = request
            .base_commit_id
            .clone()
            .map(|value| {
                ContentDigest::from_str(&value)
                    .map_err(|_| invalid_request("base_commit_id must be a 64-character digest"))
            })
            .transpose()?;
        let operation_request_id = RequestId::new(format!("workspace-create-{}", playground_id))
            .map_err(|error| invalid_request(format!("task request_id: {error}")))?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::WorkspaceCreate,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id.clone()),
                    artifact_id: Some(artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(&artifact_id)),
                    commit_id: requested_base_commit_id.map(CommitId::from_digest),
                    playground_id: Some(playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: Some(storage_volume_id.clone()),
                },
                operation_request_id,
                &task_request,
                identity,
                Some("playground"),
                Some(playground_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Playground,
            playground_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let relative_root = format!(
            "playgrounds/{}/{}/{}",
            project_id, artifact_id, playground_id
        );
        if let Some(existing) = self
            .repository
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .map_err(map_central_error)?
        {
            require_active_for_mutation(&existing.lifecycle, "Playground")?;
            if existing.storage_volume_id == storage_volume_id
                && existing.display_name == display_name
                && existing.relative_root == relative_root
                && requested_base_commit_id
                    .as_ref()
                    .is_none_or(|requested| existing.base_commit_id.as_ref() == Some(requested))
            {
                self.require_storage_availability_configured()?;
                self.ensure_workspace_materialization(&existing).await;
                return Ok(CreatePlaygroundResponse {
                    playground: self.playground_view(&existing).await?,
                    replayed: true,
                    task,
                });
            }
            return Err(catalog_conflict(
                "playground_id_reused",
                "PLAYGROUND_ID_REUSED",
                "Playground ID is already bound to another create request",
            ));
        }
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        require_active_for_mutation(&artifact.lifecycle, "Artifact")?;
        // An explicit base may select any published immutable Commit owned by this exact Artifact
        // scope. Omitting it still means "freeze the current authoritative Head", not an empty
        // baseline.
        let (base_commit_id, artifact_head) = match requested_base_commit_id {
            Some(commit_id) => {
                // A Workspace may be created on any ready Volume. Central resolves a readable
                // source Placement for hydration; the target Volume is never required to be the
                // Commit's existing physical location.
                self.load_published_commit(&artifact, CommitId::from_digest(commit_id))
                    .await?;
                (Some(commit_id), ArtifactHeadExpectation::Any)
            }
            None => (
                artifact.head_commit_id,
                ArtifactHeadExpectation::Exact(artifact.head_commit_id),
            ),
        };
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        require_active_for_mutation(&volume.lifecycle, "StorageVolume")?;
        if volume.state != StorageVolumeState::Ready {
            return Err(catalog_conflict(
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "the selected StorageVolume is not ready for placement",
            ));
        }
        self.require_live_storage_ready(&tenant_id, &storage_volume_id, "Playground placement")
            .await?;
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_playground_fenced(PlaygroundInsertRequest {
                record: PlaygroundRecord {
                    tenant_id,
                    project_id,
                    artifact_id,
                    playground_id,
                    storage_volume_id,
                    region: volume.region,
                    display_name,
                    base_commit_id,
                    head_commit_id: base_commit_id,
                    // Physical materialization is asynchronous. The record must remain Creating
                    // until the selected owner Agent confirms the directory is ready.
                    state: PlaygroundState::Creating,
                    resource_version: 1,
                    lifecycle: ResourceLifecycle::active(),
                    relative_root,
                    created_at_unix_ms: now,
                    updated_at_unix_ms: now,
                },
                artifact_head,
            })
            .await
            .map_err(playground_mutation_error)?;
        let (record, replayed) = split_outcome(outcome);
        self.ensure_workspace_materialization(&record).await;
        let materialize_task = self
            .begin_child_operation_task(
                &task,
                TaskKind::WorkspaceMaterialize,
                TaskScope {
                    tenant_id: record.tenant_id.clone(),
                    project_id: Some(record.project_id.clone()),
                    artifact_id: Some(record.artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(
                        &record.artifact_id,
                    )),
                    commit_id: record.base_commit_id.map(CommitId::from_digest),
                    playground_id: Some(record.playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: Some(record.storage_volume_id.clone()),
                },
                RequestId::new(format!("{}-materialize", record.playground_id))
                    .map_err(|error| invalid_request(format!("task request_id: {error}")))?,
                &task_request,
                identity,
                Some("workspace_materialization"),
                Some(record.playground_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &materialize_task,
            TaskResourceKind::Playground,
            record.playground_id.to_string(),
            TaskResourceRole::Target,
        )
        .await?;
        let task = self
            .transition_operation_task(
                task,
                TaskState::Running,
                identity,
                Some("workspace created; materialization pending".to_owned()),
            )
            .await?;
        Ok(CreatePlaygroundResponse {
            playground: self.playground_view(&record).await?,
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn list_snapshots(
        &self,
        identity: &AuthenticatedIdentity,
        request: QuerySnapshotListRequest,
    ) -> Result<QuerySnapshotListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotRead, &tenant_id)
            .await?;
        if request.artifact_id.is_some() && request.project_id.is_none() {
            return Err(invalid_request(
                "project_id is required when artifact_id is provided",
            ));
        }
        let project_id = request.project_id.map(parse_project_id).transpose()?;
        let artifact_id = request.artifact_id.map(parse_artifact_id).transpose()?;
        let commit_id = request
            .commit_id
            .map(|value| {
                ContentDigest::from_str(&value)
                    .map_err(|_| invalid_request("commit_id must be a 64-character digest"))
            })
            .transpose()?;
        let state = request
            .state
            .as_deref()
            .map(parse_snapshot_state)
            .transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Snapshot {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.as_ref().map(ToString::to_string),
            artifact_id: artifact_id.as_ref().map(ToString::to_string),
            commit_id: commit_id.map(|id| id.to_string()),
            state: state.map(snapshot_state_name).map(str::to_owned),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_snapshot_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_snapshots(&SnapshotListRequest {
                tenant_id,
                project_id,
                artifact_id,
                commit_id,
                state,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_snapshot_cursor(&scope, cursor))
            .transpose()?;
        let mut items = Vec::with_capacity(page.records.len());
        for record in &page.records {
            if !record.lifecycle.is_active() {
                continue;
            }
            items.push(self.snapshot_view(record).await?);
        }
        Ok(QuerySnapshotListResponse { items, next_cursor })
    }

    pub async fn query_snapshot(
        &self,
        identity: &AuthenticatedIdentity,
        request: QuerySnapshotRequest,
    ) -> Result<QuerySnapshotResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::SnapshotRead, &tenant_id)
        {
            return Err(resource_not_found("snapshot"));
        }
        let snapshot_id = parse_snapshot_id(request.snapshot_id)?;
        let record = self
            .repository
            .get_snapshot(&tenant_id, &snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        require_active_for_read(&record.lifecycle, "snapshot")?;
        Ok(QuerySnapshotResponse {
            snapshot: self.snapshot_view(&record).await?,
        })
    }

    pub async fn create_snapshot(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateSnapshotRequest,
    ) -> Result<CreateSnapshotResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let snapshot_request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let commit_id = ContentDigest::from_str(&request.commit_id)
            .map_err(|_| invalid_request("commit_id must be a 64-character digest"))?;
        let target_storage_volume_id = parse_volume_id(request.target_storage_volume_id)?;
        let target_edge_cluster_id = EdgeClusterId::new(request.target_edge_cluster_id)
            .map_err(|error| invalid_request(format!("target_edge_cluster_id: {error}")))?;
        let delivery_mode = match request.delivery_mode {
            DeliveryModeBody::Fuse => SnapshotDeliveryMode::Fuse,
            DeliveryModeBody::Copy => SnapshotDeliveryMode::Copy,
            DeliveryModeBody::Hardlink => SnapshotDeliveryMode::Hardlink,
        };
        let snapshot_id = deterministic_snapshot_id(&tenant_id, &snapshot_request_id)?;
        let (mut task, _task_replayed) = self
            .begin_operation_task(
                TaskKind::SnapshotCreate,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id.clone()),
                    artifact_id: Some(artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(&artifact_id)),
                    commit_id: Some(CommitId::from_digest(commit_id)),
                    playground_id: None,
                    snapshot_id: Some(snapshot_id.clone()),
                    storage_volume_id: Some(target_storage_volume_id.clone()),
                },
                snapshot_request_id.clone(),
                &task_request,
                identity,
                Some("snapshot"),
                Some(snapshot_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Snapshot,
            snapshot_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        if let Some(existing) = self
            .repository
            .get_snapshot(&tenant_id, &snapshot_id)
            .await
            .map_err(map_central_error)?
        {
            require_active_for_mutation(&existing.lifecycle, "Snapshot")?;
            if existing.project_id != project_id
                || existing.artifact_id != artifact_id
                || existing.commit_id != commit_id
                || existing.snapshot_request_id != snapshot_request_id
                || existing.storage_volume_id != target_storage_volume_id
                || existing.edge_cluster_id != target_edge_cluster_id
                || existing.delivery_mode != delivery_mode
            {
                return Err(catalog_conflict(
                    "snapshot_request_id_reused",
                    "SNAPSHOT_REQUEST_ID_REUSED",
                    "Snapshot request ID is already bound to another create payload",
                ));
            }
            let delivery = self
                .repository
                .get_snapshot_delivery(&tenant_id, &existing.delivery_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(internal_catalog_error)?;
            if delivery.snapshot_id != existing.snapshot_id
                || delivery.commit_id != existing.commit_id
                || delivery.storage_volume_id != existing.storage_volume_id
                || delivery.mode != existing.delivery_mode
            {
                return Err(internal_catalog_error());
            }
            self.best_effort_schedule_snapshot_delivery(&delivery).await;
            return Ok(CreateSnapshotResponse {
                snapshot: self.snapshot_view(&existing).await?,
                replayed: true,
                task,
            });
        }
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        require_active_for_mutation(&artifact.lifecycle, "Artifact")?;
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &target_storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        if !volume.lifecycle.is_active() {
            return Err(catalog_conflict(
                "storage_volume_lifecycle_fenced",
                "STORAGE_VOLUME_LIFECYCLE_FENCED",
                "the target StorageVolume is not active",
            ));
        }
        if volume.state != StorageVolumeState::Ready {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "the target StorageVolume is not ready",
                true,
            ));
        }
        if volume.edge_cluster_id != target_edge_cluster_id {
            return Err(catalog_conflict(
                "storage_volume_edge_cluster_mismatch",
                "STORAGE_VOLUME_EDGE_CLUSTER_MISMATCH",
                "the target StorageVolume belongs to a different EdgeCluster",
            ));
        }
        if !volume.allowed_delivery_modes.contains(&delivery_mode) {
            return Err(catalog_conflict(
                "delivery_mode_not_allowed",
                "DELIVERY_MODE_NOT_ALLOWED",
                "the requested delivery mode is disabled by the StorageVolume policy",
            ));
        }
        let artifact_head = ArtifactHeadExpectation::Any;
        let commit = self
            .load_published_commit(&artifact, CommitId::from_digest(commit_id))
            .await?;
        let now = self.clock.now();
        let delivery_id = super::snapshot_delivery::deterministic_delivery_id(
            &tenant_id,
            &snapshot_id,
            delivery_mode,
            snapshot_request_id.as_str(),
        )?;
        let target_relative_root = SnapshotDeliveryOperation::canonical_target_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
            &delivery_id,
        )
        .map_err(|error| invalid_request(error.to_string()))?;
        let file_count = u64::try_from(commit.records.len())
            .map_err(|_| invalid_request("snapshot file count exceeds u64"))?;
        let size_bytes = commit
            .records
            .iter()
            .try_fold(0_u64, |total, record| total.checked_add(record.total_size))
            .ok_or_else(|| invalid_request("snapshot size exceeds u64"))?;
        if commit.data_layout == CommitDataLayout::WholeFile
            && commit
                .records
                .iter()
                .any(|record| record.total_size > volume.max_whole_file_bytes.get())
        {
            return Err(catalog_conflict(
                "whole_file_size_limit_exceeded",
                "WHOLE_FILE_SIZE_LIMIT_EXCEEDED",
                "the Commit contains a file that exceeds the StorageVolume WholeFile size policy",
            ));
        }
        if delivery_mode == SnapshotDeliveryMode::Hardlink
            && commit.data_layout != CommitDataLayout::WholeFile
        {
            return Err(catalog_conflict(
                "hardlink_requires_whole_file",
                "HARDLINK_REQUIRES_WHOLE_FILE",
                "Hardlink delivery requires a WholeFile Commit",
            ));
        }
        if delivery_mode == SnapshotDeliveryMode::Hardlink
            && matches!(volume.hardlink_policy, HardlinkPolicy::Disabled)
        {
            return Err(catalog_conflict(
                "hardlink_unsafe_volume",
                "HARDLINK_UNSAFE_VOLUME",
                "StorageVolume has not enabled a sealed hardlink policy",
            ));
        }
        // A live heartbeat-backed Volume is part of Snapshot creation, not merely a later
        // scheduling hint.  In the production composition the coordinator also performs the
        // capability/session preflight, so an Agent that has not completed the QUIC handshake
        // cannot leave behind an apparently accepted Delivery.
        self.require_live_storage_ready(&tenant_id, &target_storage_volume_id, "Snapshot delivery")
            .await?;
        if self.agent_registry.is_some() {
            let coordinator = self.coordinator.as_ref().ok_or_else(|| {
                application_error(
                    ErrorCategory::Unavailable,
                    "snapshot_delivery_execution_unavailable",
                    "SNAPSHOT_DELIVERY_EXECUTION_UNAVAILABLE",
                    "Snapshot delivery execution is not configured",
                    true,
                )
            })?;
            coordinator
                .preflight_snapshot_delivery(&tenant_id, &target_storage_volume_id, delivery_mode)
                .await
                .map_err(map_central_error)?;
        }
        let retention_roots = if delivery_mode == SnapshotDeliveryMode::Hardlink {
            self.hardlink_retention_roots(&tenant_id, &artifact_id, &delivery_id, &commit.records)
                .await?
        } else {
            Vec::new()
        };
        let outcome = self
            .repository
            .insert_snapshot_with_delivery(crate::SnapshotWithDeliveryInsertRequest {
                snapshot: SnapshotInsertRequest {
                    record: SnapshotRecord {
                        tenant_id: tenant_id.clone(),
                        project_id: project_id.clone(),
                        artifact_id: artifact_id.clone(),
                        snapshot_id: snapshot_id.clone(),
                        snapshot_request_id: snapshot_request_id.clone(),
                        commit_id,
                        delivery_id: delivery_id.clone(),
                        edge_cluster_id: target_edge_cluster_id.clone(),
                        storage_volume_id: target_storage_volume_id.clone(),
                        delivery_mode,
                        state: SnapshotState::Creating,
                        resource_version: 1,
                        lifecycle: ResourceLifecycle::active(),
                        created_at_unix_ms: now,
                        updated_at_unix_ms: now,
                    },
                    artifact_head,
                },
                delivery: SnapshotDeliveryInsertRequest {
                    record: SnapshotDeliveryRecord {
                        tenant_id: tenant_id.clone(),
                        delivery_id,
                        create_request_id: snapshot_request_id.clone(),
                        snapshot_id,
                        commit_id,
                        storage_volume_id: target_storage_volume_id,
                        mode: delivery_mode,
                        target_relative_root,
                        state: SnapshotDeliveryState::Requested,
                        source_index_digest: commit.index_version.digest,
                        delivery_generation: DeliveryGeneration::new(1),
                        file_count,
                        size_bytes,
                        object_set_digest: commit.object_set_digest,
                        resource_version: 1,
                        issue_code: None,
                        issue_message: None,
                        issue_retryable: false,
                        created_at_unix_ms: now,
                        updated_at_unix_ms: now,
                    },
                    request_id: snapshot_request_id,
                    retention_roots,
                },
            })
            .await
            .map_err(snapshot_mutation_error)?;
        let crate::SnapshotWithDeliveryInsertResult {
            snapshot: record,
            delivery,
            replayed,
        } = outcome;
        let delivery_task = self
            .begin_child_operation_task(
                &task,
                TaskKind::SnapshotDeliveryMaterialize,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id.clone()),
                    artifact_id: Some(artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(&artifact_id)),
                    commit_id: Some(CommitId::from_digest(record.commit_id)),
                    playground_id: None,
                    snapshot_id: Some(record.snapshot_id.clone()),
                    storage_volume_id: Some(record.storage_volume_id.clone()),
                },
                Self::derived_task_request_id(
                    TaskKind::SnapshotDeliveryMaterialize,
                    &task_request,
                )?,
                &task_request,
                identity,
                Some("snapshot_delivery"),
                Some(delivery.delivery_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &delivery_task,
            TaskResourceKind::SnapshotDelivery,
            delivery.delivery_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        task = self
            .transition_operation_task(
                task,
                TaskState::Running,
                identity,
                Some("snapshot created; delivery materialization pending".to_owned()),
            )
            .await?;
        let _ = self
            .transition_operation_task(
                delivery_task,
                TaskState::Running,
                identity,
                Some("snapshot delivery scheduled".to_owned()),
            )
            .await?;
        // Commit was loaded before the fenced insert. Retain this assertion so a corrupted
        // repository cannot dispatch a different immutable Index than the public response.
        if ContentDigest::from(commit.commit_id) != record.commit_id {
            return Err(internal_catalog_error());
        }
        self.best_effort_schedule_snapshot_delivery(&delivery).await;
        Ok(CreateSnapshotResponse {
            snapshot: self.snapshot_view(&record).await?,
            replayed,
            task,
        })
    }

    pub async fn query_deletion_impact(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryDeletionImpactRequest,
    ) -> Result<QueryDeletionImpactResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ResourceLifecycleRead, &tenant_id)
            .await?;
        let expected_resource_version = parse_lifecycle_resource_version(
            "expected_resource_version",
            &request.expected_resource_version,
        )?;
        let root = parse_resource_ref(request.resource)?;
        let additional_blockers = self.lifecycle_authority_blockers(&tenant_id, &root).await?;
        let authority_impact = match self.lifecycle_authority.as_ref() {
            Some(authority) => Some(
                authority
                    .impact(&tenant_id, &root)
                    .await
                    .map_err(map_central_error)?,
            ),
            None => None,
        };
        let record = self
            .repository
            .query_deletion_impact(DeletionImpactQuery {
                tenant_id,
                root: root.clone(),
                cascade: request.cascade,
                confirm_managed_data_erase: request.confirm_managed_data_erase,
                additional_blockers,
                authority_impact,
                now_unix_ms: self.clock.now(),
            })
            .await
            .map_err(map_central_error)?;
        let root_target = record
            .impact
            .targets
            .iter()
            .find(|target| target.resource == root)
            .ok_or_else(internal_catalog_error)?;
        if root_target.resource_version.get() != expected_resource_version {
            return Err(lifecycle_conflict(
                "resource_version_changed",
                "RESOURCE_VERSION_CHANGED",
                "the resource changed after the delete dialog was opened",
            ));
        }
        Ok(QueryDeletionImpactResponse {
            impact: deletion_impact_view(&record.impact),
            impact_digest: record.impact_digest.to_string(),
        })
    }

    pub async fn create_deletion(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateDeletionRequest,
    ) -> Result<DeletionMutationResponse, Error> {
        let task_request = request.clone();
        let request_digest = lifecycle_request_digest("deletion_create", &request)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ResourceLifecycleManage, &tenant_id)
            .await?;
        let request_id = parse_request_id(request.request_id)?;
        let expected_resource_version = parse_lifecycle_resource_version(
            "expected_resource_version",
            &request.expected_resource_version,
        )?;
        let impact_digest = parse_content_digest("impact_digest", &request.impact_digest)?;
        let root = parse_resource_ref(request.resource)?;
        let deletion_id = deletion_id_for_request(&tenant_id, &request_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                task_scope_for_resource(tenant_id.clone(), &root),
                request_id.clone(),
                &task_request,
                identity,
                Some("deletion"),
                Some(deletion_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Deletion,
            deletion_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let now = self.clock.now();
        if let Some(existing) = self
            .repository
            .get_deletion_operation(&tenant_id, &deletion_id)
            .await
            .map_err(map_central_error)?
        {
            if existing.request_id == request_id && existing.request_digest == request_digest {
                self.publish_deletion_s3_read_revocations(&existing, "deletion request replayed")
                    .await;
                let task = self.complete_operation_task(task, identity).await?;
                return Ok(DeletionMutationResponse {
                    deletion: deletion_operation_view(&existing),
                    replayed: true,
                    task,
                });
            }
            return Err(lifecycle_request_id_reused());
        }
        // The impact digest identifies the exact, short-lived snapshot returned by the
        // confirmation query. Rebuilding an impact here would change its issued/expiry timestamps
        // on every request and make a valid confirmation fail with IMPACT_CHANGED. The repository
        // transaction below re-loads that saved snapshot, checks its TTL and target versions, and
        // fences the current resources atomically before creating the deletion operation.
        let current_blockers = self.lifecycle_authority_blockers(&tenant_id, &root).await?;
        if !current_blockers.is_empty() {
            return Err(lifecycle_conflict(
                "unique_object_replica",
                "UNIQUE_OBJECT_REPLICA",
                "the StorageVolume now contains an object with no valid replica on another Volume",
            ));
        }
        let outcome = self
            .repository
            .create_deletion_idempotent(DomainCreateDeletionRequest {
                deletion_id,
                tenant_id,
                root,
                cascade: request.cascade,
                confirm_managed_data_erase: request.confirm_managed_data_erase,
                request_id,
                request_digest,
                impact_digest,
                expected_resource_version,
                now_unix_ms: now,
            })
            .await
            .map_err(map_lifecycle_mutation_error)?;
        let (deletion, replayed) = split_outcome(outcome);
        self.publish_deletion_s3_read_revocations(&deletion, "resource deletion requested")
            .await;
        Ok(DeletionMutationResponse {
            deletion: deletion_operation_view(&deletion),
            replayed: replayed || task_replayed,
            task: self.complete_operation_task(task, identity).await?,
        })
    }

    async fn lifecycle_authority_blockers(
        &self,
        tenant_id: &TenantId,
        resource: &ResourceRef,
    ) -> Result<Vec<neoengram_domain::protocol::DeletionBlocker>, Error> {
        let ResourceRef::StorageVolume { storage_volume_id } = resource else {
            return Ok(Vec::new());
        };
        let Some(objects) = &self.lifecycle_objects else {
            return Ok(Vec::new());
        };
        let artifacts = objects
            .volume_unique_artifact_replicas(tenant_id, storage_volume_id)
            .await
            .map_err(map_central_error)?;
        if artifacts.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![neoengram_domain::protocol::DeletionBlocker {
            code: "UNIQUE_OBJECT_REPLICA".to_owned(),
            resource: Some(resource.clone()),
            message: format!(
                "StorageVolume contains unique object replicas for retained Artifacts: {}",
                artifacts
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }])
    }

    pub async fn query_deletion(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryDeletionRequest,
    ) -> Result<QueryDeletionResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ResourceLifecycleRead, &tenant_id)
            .await?;
        let deletion_id = parse_deletion_id(request.deletion_id)?;
        let deletion = self
            .repository
            .get_deletion_operation(&tenant_id, &deletion_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("deletion operation"))?;
        let retention_holds = self
            .repository
            .list_retention_holds(&tenant_id, &deletion_id)
            .await
            .map_err(map_central_error)?
            .iter()
            .map(retention_hold_view)
            .collect();
        Ok(QueryDeletionResponse {
            deletion: deletion_operation_view(&deletion),
            retention_holds,
        })
    }

    pub async fn list_deletions(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryDeletionListRequest,
    ) -> Result<QueryDeletionListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ResourceLifecycleRead, &tenant_id)
            .await?;
        let states = request
            .states
            .map(|states| {
                states
                    .into_iter()
                    .map(|state| parse_deletion_state(&state))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Deletion {
            tenant_id: tenant_id.to_string(),
            states: states.as_ref().map(|states| {
                states
                    .iter()
                    .map(|state| deletion_state_name(*state).to_owned())
                    .collect()
            }),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_deletion_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_deletion_operations(&DeletionListRequest {
                tenant_id,
                states,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_deletion_cursor(&scope, cursor))
            .transpose()?;
        Ok(QueryDeletionListResponse {
            items: page.records.iter().map(deletion_operation_view).collect(),
            next_cursor,
        })
    }

    pub async fn restore_deletion(
        &self,
        identity: &AuthenticatedIdentity,
        request: UpdateDeletionRequest,
    ) -> Result<DeletionMutationResponse, Error> {
        let task_request = request.clone();
        let request_digest = lifecycle_request_digest("deletion_restore", &request)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ResourceLifecycleManage, &tenant_id)
            .await?;
        let deletion_id = parse_deletion_id(request.deletion_id)?;
        let request_id = parse_request_id(request.request_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                TaskScope::new(tenant_id.clone()),
                request_id.clone(),
                &task_request,
                identity,
                Some("deletion"),
                Some(deletion_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Deletion,
            deletion_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let outcome = self
            .repository
            .restore_deletion_idempotent(DomainRestoreDeletionRequest {
                tenant_id,
                deletion_id,
                request_id,
                request_digest,
                expected_resource_version: parse_lifecycle_resource_version(
                    "expected_resource_version",
                    &request.expected_resource_version,
                )?,
                now_unix_ms: self.clock.now(),
            })
            .await
            .map_err(map_lifecycle_mutation_error)?;
        let (deletion, replayed) = split_outcome(outcome);
        self.publish_deletion_s3_read_revocations(&deletion, "resource restore requested")
            .await;
        Ok(DeletionMutationResponse {
            deletion: deletion_operation_view(&deletion),
            replayed: replayed || task_replayed,
            task: self.complete_operation_task(task, identity).await?,
        })
    }

    pub async fn retry_deletion(
        &self,
        identity: &AuthenticatedIdentity,
        request: UpdateDeletionRequest,
    ) -> Result<DeletionMutationResponse, Error> {
        let task_request = request.clone();
        let request_digest = lifecycle_request_digest("deletion_retry", &request)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ResourceLifecycleManage, &tenant_id)
            .await?;
        let deletion_id = parse_deletion_id(request.deletion_id)?;
        let request_id = parse_request_id(request.request_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                TaskScope::new(tenant_id.clone()),
                request_id.clone(),
                &task_request,
                identity,
                Some("deletion"),
                Some(deletion_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Deletion,
            deletion_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let outcome = self
            .repository
            .retry_deletion_idempotent(DomainRetryDeletionRequest {
                tenant_id,
                deletion_id,
                request_id,
                request_digest,
                expected_resource_version: parse_lifecycle_resource_version(
                    "expected_resource_version",
                    &request.expected_resource_version,
                )?,
                now_unix_ms: self.clock.now(),
            })
            .await
            .map_err(map_lifecycle_mutation_error)?;
        let (deletion, replayed) = split_outcome(outcome);
        Ok(DeletionMutationResponse {
            deletion: deletion_operation_view(&deletion),
            replayed: replayed || task_replayed,
            task: self.complete_operation_task(task, identity).await?,
        })
    }

    pub async fn create_retention_hold(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateRetentionHoldRequest,
    ) -> Result<CreateRetentionHoldResponse, Error> {
        let task_request = request.clone();
        let request_digest = lifecycle_request_digest("retention_hold_create", &request)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::RetentionManage, &tenant_id)
            .await?;
        let deletion_id = parse_deletion_id(request.deletion_id)?;
        let request_id = parse_request_id(request.request_id)?;
        let retention_hold_id = retention_hold_id_for_request(&deletion_id, &request_id)?;
        let reason = validate_text("reason", request.reason, 1, 2_048)?;
        let expires_at_unix_ms = request
            .expires_at_unix_ms
            .as_deref()
            .map(|value| parse_unix_millis("expires_at_unix_ms", value))
            .transpose()?;
        let now = self.clock.now();
        if expires_at_unix_ms.is_some_and(|expires_at| expires_at.get() <= now.get()) {
            return Err(invalid_request("expires_at_unix_ms must be in the future"));
        }
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                TaskScope::new(tenant_id.clone()),
                request_id.clone(),
                &task_request,
                identity,
                Some("retention_hold"),
                Some(retention_hold_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::RetentionHold,
            retention_hold_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Deletion,
            deletion_id.to_string(),
            TaskResourceRole::Related,
        )
        .await?;
        let outcome = self
            .repository
            .create_retention_hold_idempotent(DomainCreateRetentionHoldRequest {
                tenant_id: tenant_id.clone(),
                deletion_id: deletion_id.clone(),
                retention_hold_id,
                reason,
                expires_at_unix_ms,
                request_id,
                request_digest,
                expected_resource_version: parse_lifecycle_resource_version(
                    "expected_resource_version",
                    &request.expected_resource_version,
                )?,
                now_unix_ms: now,
            })
            .await
            .map_err(map_lifecycle_mutation_error)?;
        let (retention_hold, replayed) = split_outcome(outcome);
        let deletion = self
            .repository
            .get_deletion_operation(&tenant_id, &deletion_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("deletion operation"))?;
        Ok(CreateRetentionHoldResponse {
            deletion: deletion_operation_view(&deletion),
            retention_hold: retention_hold_view(&retention_hold),
            replayed: replayed || task_replayed,
            task: self.complete_operation_task(task, identity).await?,
        })
    }

    pub async fn release_retention_hold(
        &self,
        identity: &AuthenticatedIdentity,
        request: ReleaseRetentionHoldRequest,
    ) -> Result<ReleaseRetentionHoldResponse, Error> {
        let task_request = request.clone();
        let request_digest = lifecycle_request_digest("retention_hold_release", &request)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::RetentionManage, &tenant_id)
            .await?;
        let deletion_id = parse_deletion_id(request.deletion_id)?;
        let retention_hold_id = parse_retention_hold_id(request.retention_hold_id)?;
        let request_id = parse_request_id(request.request_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::CatalogLifecycle,
                TaskScope::new(tenant_id.clone()),
                request_id.clone(),
                &task_request,
                identity,
                Some("retention_hold"),
                Some(retention_hold_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::RetentionHold,
            retention_hold_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Deletion,
            deletion_id.to_string(),
            TaskResourceRole::Related,
        )
        .await?;
        let outcome = self
            .repository
            .release_retention_hold_idempotent(DomainReleaseRetentionHoldRequest {
                tenant_id: tenant_id.clone(),
                deletion_id: deletion_id.clone(),
                retention_hold_id,
                request_id,
                request_digest,
                expected_resource_version: parse_lifecycle_resource_version(
                    "expected_resource_version",
                    &request.expected_resource_version,
                )?,
                now_unix_ms: self.clock.now(),
            })
            .await
            .map_err(map_lifecycle_mutation_error)?;
        let (retention_hold, replayed) = split_outcome(outcome);
        let deletion = self
            .repository
            .get_deletion_operation(&tenant_id, &deletion_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("deletion operation"))?;
        Ok(ReleaseRetentionHoldResponse {
            deletion: deletion_operation_view(&deletion),
            retention_hold: retention_hold_view(&retention_hold),
            replayed: replayed || task_replayed,
            task: self.complete_operation_task(task, identity).await?,
        })
    }

    pub async fn list_s3_access_points(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryS3AccessPointListRequest,
    ) -> Result<QueryS3AccessPointListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::S3AccessRead, &tenant_id)
            .await?;
        let page_size = page_size(request.page_size)?;
        let scope = format!("s3ap:{}", tenant_id);
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_s3_access_point_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_s3_access_points(&S3AccessPointListRequest {
                tenant_id: tenant_id.clone(),
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_s3_access_point_cursor(&scope, cursor))
            .transpose()?;
        let mut items = Vec::with_capacity(page.records.len());
        for record in &page.records {
            items.push(self.s3_access_point_view(record).await?);
        }
        Ok(QueryS3AccessPointListResponse { items, next_cursor })
    }

    pub async fn query_s3_access_point(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryS3AccessPointRequest,
    ) -> Result<QueryS3AccessPointResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::S3AccessRead, &tenant_id)
            .await?;
        let access_point_id = parse_s3_access_point_id(request.access_point_id)?;
        let record = self
            .repository
            .get_s3_access_point(&tenant_id, &access_point_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("S3 access point"))?;
        Ok(QueryS3AccessPointResponse {
            access_point: self.s3_access_point_view(&record).await?,
        })
    }

    pub async fn create_s3_access_point(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateS3AccessPointRequest,
    ) -> Result<CreateS3AccessPointResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id.clone())?;
        self.require_tenant(identity, Permission::S3AccessManage, &tenant_id)
            .await?;
        let snapshot_id = parse_snapshot_id(request.snapshot_id.clone())?;
        let bucket_name = validate_s3_bucket_name(&request.bucket_name)?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let mutation = s3_mutation_record(
            &tenant_id,
            &request_id,
            S3MutationKind::AccessPointCreate,
            &serde_json::json!({
                "tenant_id": tenant_id.as_str(),
                "snapshot_id": snapshot_id.as_str(),
                "bucket_name": bucket_name,
            }),
            self.clock.now(),
        )?;
        let request_access_point_id = s3_access_point_id_for_request(&tenant_id, &request_id)?;
        let request_credential_id =
            s3_credential_id_for_request(&request_access_point_id, &request_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::S3Lifecycle,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: None,
                    artifact_id: None,
                    object_namespace_id: None,
                    commit_id: None,
                    playground_id: None,
                    snapshot_id: Some(snapshot_id.clone()),
                    storage_volume_id: None,
                },
                request_id.clone(),
                &task_request,
                identity,
                Some("s3_access_point"),
                Some(request_access_point_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::S3AccessPoint,
            request_access_point_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        if let Some(existing_mutation) = self
            .repository
            .get_s3_mutation(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            ensure_s3_mutation_identity(&existing_mutation, &mutation)?;
            let access_point = self
                .repository
                .get_s3_access_point(&tenant_id, &request_access_point_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(internal_catalog_error)?;
            self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
                .await?;
            let credential = self
                .repository
                .list_s3_credentials(&request_access_point_id)
                .await
                .map_err(map_central_error)?
                .into_iter()
                .find(|credential| credential.credential_id == request_credential_id)
                .ok_or_else(internal_catalog_error)?;
            let task = self.complete_operation_task(task, identity).await?;
            return Ok(CreateS3AccessPointResponse {
                access_point: self.s3_access_point_view(&access_point).await?,
                access_key_id: credential.access_key_id,
                secret_access_key: String::new(),
                credential_expires_at_unix_ms: credential.expires_at_unix_ms.to_string(),
                replayed: true,
                task,
            });
        }
        let snapshot = self
            .repository
            .get_snapshot(&tenant_id, &snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        require_active_for_mutation(&snapshot.lifecycle, "Snapshot")?;
        if snapshot.state != SnapshotState::Ready {
            return Err(catalog_conflict(
                "snapshot_not_ready",
                "SNAPSHOT_NOT_READY",
                "S3 access can only be enabled for a Ready Snapshot",
            ));
        }
        // Access Point creation must be fenced by the Snapshot's bound Delivery even when an
        // older Access Point record already exists for this Snapshot.  Otherwise a new request
        // could rotate/bootstrap credentials while the physical read view is unavailable.
        self.ensure_snapshot_delivery_ready(&snapshot).await?;
        if let Some(existing) = self
            .repository
            .get_s3_access_point_by_snapshot(&tenant_id, &snapshot_id)
            .await
            .map_err(map_central_error)?
        {
            if existing.bucket_name != bucket_name {
                return Err(catalog_conflict(
                    "snapshot_s3_access_point_exists",
                    "SNAPSHOT_S3_ACCESS_POINT_EXISTS",
                    "Snapshot already has a different S3 bucket",
                ));
            }
            if existing.access_point_id != request_access_point_id {
                return Err(catalog_conflict(
                    "snapshot_s3_access_point_exists",
                    "SNAPSHOT_S3_ACCESS_POINT_EXISTS",
                    "Snapshot already has an S3 Access Point; use the credential API to rotate access keys",
                ));
            }
            let existing_credential = self
                .repository
                .list_s3_credentials(&existing.access_point_id)
                .await
                .map_err(map_central_error)?
                .into_iter()
                .find(|credential| credential.credential_id == request_credential_id);
            let (credential, secret) = match existing_credential {
                Some(credential) => (credential, String::new()),
                None if existing.state == S3AccessPointState::Active => {
                    self.build_s3_credential_record(
                        &existing,
                        None,
                        &request_id,
                        mutation.created_at_unix_ms,
                    )
                    .await?
                }
                None => return Err(internal_catalog_error()),
            };
            let outcome = self
                .repository
                .create_s3_access_point_idempotent(mutation, existing, credential)
                .await
                .map_err(map_s3_mutation_error)?;
            let result = match outcome {
                CatalogInsertOutcome::Inserted(result) | CatalogInsertOutcome::Existing(result) => {
                    result
                }
            };
            let task = self.complete_operation_task(task, identity).await?;
            return Ok(CreateS3AccessPointResponse {
                access_point: self.s3_access_point_view(&result.access_point).await?,
                access_key_id: result.credential.access_key_id,
                secret_access_key: secret,
                credential_expires_at_unix_ms: result.credential.expires_at_unix_ms.to_string(),
                replayed: true,
                task,
            });
        }
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &snapshot.project_id, &snapshot.artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let commit = self
            .load_published_commit(&artifact, CommitId::from_digest(snapshot.commit_id))
            .await?;
        // An Access Point is a physical read view, so it must be bound to the Snapshot's one
        // Ready Delivery.  Never select an unrelated complete Volume for the same Commit.
        let delivery = self.ensure_snapshot_delivery_ready(&snapshot).await?;
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &snapshot.storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(s3_snapshot_unavailable)?;
        if volume.edge_cluster_id != snapshot.edge_cluster_id
            || !volume.lifecycle.is_active()
            || volume.state != StorageVolumeState::Ready
        {
            return Err(s3_snapshot_unavailable());
        }
        let (_, _, _, _pool) = self
            .resolve_s3_route_for_volume(
                &tenant_id,
                &commit,
                &snapshot.storage_volume_id,
                self.clock.now(),
            )
            .await?;
        let now = mutation.created_at_unix_ms;
        let record = S3AccessPointRecord {
            access_point_id: request_access_point_id,
            tenant_id: tenant_id.clone(),
            project_id: snapshot.project_id.clone(),
            artifact_id: snapshot.artifact_id.clone(),
            snapshot_id: snapshot.snapshot_id.clone(),
            commit_id: snapshot.commit_id,
            delivery_id: delivery.delivery_id,
            storage_volume_id: delivery.storage_volume_id,
            edge_cluster_id: volume.edge_cluster_id,
            bucket_name,
            state: S3AccessPointState::Active,
            policy_generation: 1,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let (credential, secret) = self
            .build_s3_credential_record(&record, None, &request_id, now)
            .await?;
        let outcome = self
            .repository
            .create_s3_access_point_idempotent(mutation, record, credential)
            .await
            .map_err(map_s3_mutation_error)?;
        let (result, replayed) = match outcome {
            CatalogInsertOutcome::Inserted(result) => (result, false),
            CatalogInsertOutcome::Existing(result) => (result, true),
        };
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CreateS3AccessPointResponse {
            access_point: self.s3_access_point_view(&result.access_point).await?,
            access_key_id: result.credential.access_key_id,
            secret_access_key: if replayed { String::new() } else { secret },
            credential_expires_at_unix_ms: result.credential.expires_at_unix_ms.to_string(),
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn enable_s3_access_point(
        &self,
        identity: &AuthenticatedIdentity,
        request: UpdateS3AccessPointRequest,
    ) -> Result<UpdateS3AccessPointResponse, Error> {
        self.update_s3_access_point(identity, request, S3AccessPointState::Active)
            .await
    }

    pub async fn disable_s3_access_point(
        &self,
        identity: &AuthenticatedIdentity,
        request: UpdateS3AccessPointRequest,
    ) -> Result<UpdateS3AccessPointResponse, Error> {
        self.update_s3_access_point(identity, request, S3AccessPointState::Disabled)
            .await
    }

    async fn update_s3_access_point(
        &self,
        identity: &AuthenticatedIdentity,
        request: UpdateS3AccessPointRequest,
        state: S3AccessPointState,
    ) -> Result<UpdateS3AccessPointResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id.clone())?;
        self.require_tenant(identity, Permission::S3AccessManage, &tenant_id)
            .await?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let access_point_id = parse_s3_access_point_id(request.access_point_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::S3Lifecycle,
                TaskScope::new(tenant_id.clone()),
                request_id.clone(),
                &task_request,
                identity,
                Some("s3_access_point"),
                Some(access_point_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::S3AccessPoint,
            access_point_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        if state == S3AccessPointState::Active {
            let access_point = self
                .repository
                .get_s3_access_point(&tenant_id, &access_point_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| resource_not_found("S3 access point"))?;
            let snapshot = self
                .repository
                .get_snapshot(&tenant_id, &access_point.snapshot_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| resource_not_found("snapshot"))?;
            require_active_for_mutation(&snapshot.lifecycle, "Snapshot")?;
            self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
                .await?;
        }
        let operation = match state {
            S3AccessPointState::Active => S3MutationKind::AccessPointEnable,
            S3AccessPointState::Disabled => S3MutationKind::AccessPointDisable,
        };
        let mutation = s3_mutation_record(
            &tenant_id,
            &request_id,
            operation,
            &serde_json::json!({
                "tenant_id": tenant_id.as_str(),
                "access_point_id": access_point_id.as_str(),
                "state": s3_access_point_state_name(state),
            }),
            self.clock.now(),
        )?;
        let outcome = self
            .repository
            .update_s3_access_point_state_idempotent(
                mutation,
                &access_point_id,
                state,
                self.clock.now(),
            )
            .await
            .map_err(map_s3_mutation_error)?;
        let (updated, replayed) = match outcome {
            CatalogInsertOutcome::Inserted(updated) => (updated, false),
            CatalogInsertOutcome::Existing(updated) => (updated, true),
        };
        match self
            .repository
            .get_snapshot(&tenant_id, &updated.snapshot_id)
            .await
        {
            Ok(Some(snapshot)) => {
                self.publish_access_point_s3_read_revocation(
                    &updated,
                    snapshot.lifecycle.generation,
                    "S3 Access Point policy changed",
                )
                .await;
            }
            Ok(None) => tracing::error!(
                access_point_id = %updated.access_point_id,
                snapshot_id = %updated.snapshot_id,
                "cannot publish S3 read revocation for a missing Snapshot"
            ),
            Err(error) => tracing::error!(
                access_point_id = %updated.access_point_id,
                %error,
                "cannot load Snapshot for S3 read revocation"
            ),
        }
        let task = self.complete_operation_task(task, identity).await?;
        Ok(UpdateS3AccessPointResponse {
            access_point: self.s3_access_point_view(&updated).await?,
            replayed: replayed || task_replayed,
            task,
        })
    }

    async fn publish_deletion_s3_read_revocations(
        &self,
        deletion: &DeletionOperation,
        reason: &'static str,
    ) {
        if self.s3_read_revocations.is_none() {
            return;
        }
        for target in &deletion.targets {
            let ResourceRef::Snapshot { snapshot_id } = &target.resource else {
                continue;
            };
            match self
                .repository
                .get_s3_access_point_by_snapshot(&deletion.tenant_id, snapshot_id)
                .await
            {
                Ok(Some(access_point)) => {
                    self.publish_access_point_s3_read_revocation(
                        &access_point,
                        target.lifecycle_generation,
                        reason,
                    )
                    .await;
                }
                Ok(None) => {}
                Err(error) => tracing::error!(
                    tenant_id = %deletion.tenant_id,
                    snapshot_id = %snapshot_id,
                    %error,
                    "cannot load S3 Access Point for lifecycle revocation"
                ),
            }
        }
    }

    async fn publish_access_point_s3_read_revocation(
        &self,
        access_point: &S3AccessPointRecord,
        snapshot_lifecycle_generation: LifecycleGeneration,
        reason: &'static str,
    ) {
        let Some(publisher) = &self.s3_read_revocations else {
            return;
        };
        let Ok(commit) = self.load_s3_access_point_commit(access_point).await else {
            tracing::warn!(
                access_point_id = %access_point.access_point_id,
                "cannot resolve Access Point placement for S3 revocation"
            );
            return;
        };
        let Ok((_, _, route, _)) = self
            .resolve_s3_route_for_access_point(access_point, &commit, self.clock.now())
            .await
        else {
            tracing::warn!(
                access_point_id = %access_point.access_point_id,
                "cannot resolve Gateway route for S3 revocation"
            );
            return;
        };
        publisher
            .publish_s3_read_revocation(
                &route.gateway_pool_id,
                GatewayS3ReadRevocation {
                    tenant_id: access_point.tenant_id.clone(),
                    snapshot_id: access_point.snapshot_id.clone(),
                    minimum_snapshot_lifecycle_generation: snapshot_lifecycle_generation,
                    bucket: access_point.bucket_name.clone(),
                    minimum_access_point_policy_generation: ResourceVersion::new(
                        access_point.policy_generation,
                    ),
                    reason: reason.to_owned(),
                },
            )
            .await;
    }

    pub async fn list_s3_credentials(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryS3CredentialListRequest,
    ) -> Result<QueryS3CredentialListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::S3AccessManage, &tenant_id)
            .await?;
        let access_point_id = parse_s3_access_point_id(request.access_point_id)?;
        let access_point = self
            .repository
            .get_s3_access_point(&tenant_id, &access_point_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("S3 access point"))?;
        self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
            .await?;
        self.expire_s3_credentials(&access_point_id).await?;
        let items = self
            .repository
            .list_s3_credentials(&access_point_id)
            .await
            .map_err(map_central_error)?
            .iter()
            .map(s3_credential_view)
            .collect();
        Ok(QueryS3CredentialListResponse { items, task: None })
    }

    pub async fn create_s3_credential(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateS3CredentialRequest,
    ) -> Result<CreateS3CredentialResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id.clone())?;
        self.require_tenant(identity, Permission::S3AccessManage, &tenant_id)
            .await?;
        let access_point_id = parse_s3_access_point_id(request.access_point_id.clone())?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let expires_at = request
            .expires_at_unix_ms
            .clone()
            .map(|value| {
                value
                    .parse::<u64>()
                    .map(UnixMillis::new)
                    .map_err(|_| invalid_request("expires_at_unix_ms must be an unsigned integer"))
            })
            .transpose()?;
        if expires_at.is_some_and(|value| value.get() <= self.clock.now().get()) {
            return Err(invalid_request("expires_at_unix_ms must be in the future"));
        }
        let mutation = s3_mutation_record(
            &tenant_id,
            &request_id,
            S3MutationKind::CredentialCreate,
            &serde_json::json!({
                "tenant_id": tenant_id.as_str(),
                "access_point_id": access_point_id.as_str(),
                "expires_at_unix_ms": expires_at.map(|value| value.to_string()),
            }),
            self.clock.now(),
        )?;
        let credential_id = s3_credential_id_for_request(&access_point_id, &request_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::S3Lifecycle,
                TaskScope::new(tenant_id.clone()),
                request_id.clone(),
                &task_request,
                identity,
                Some("s3_credential"),
                Some(credential_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::S3AccessPoint,
            access_point_id.to_string(),
            TaskResourceRole::Related,
        )
        .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::S3Credential,
            credential_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        if let Some(existing_mutation) = self
            .repository
            .get_s3_mutation(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            ensure_s3_mutation_identity(&existing_mutation, &mutation)?;
            let access_point = self
                .repository
                .get_s3_access_point(&tenant_id, &access_point_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(internal_catalog_error)?;
            self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
                .await?;
            let credential = self
                .repository
                .list_s3_credentials(&access_point_id)
                .await
                .map_err(map_central_error)?
                .into_iter()
                .find(|credential| credential.credential_id == credential_id)
                .ok_or_else(internal_catalog_error)?;
            let task = self.complete_operation_task(task, identity).await?;
            return Ok(CreateS3CredentialResponse {
                credential: s3_credential_view(&credential),
                secret_access_key: String::new(),
                replayed: true,
                task,
            });
        }
        let access_point = self
            .repository
            .get_s3_access_point(&tenant_id, &access_point_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("S3 access point"))?;
        if access_point.state != S3AccessPointState::Active {
            return Err(catalog_conflict(
                "s3_access_point_disabled",
                "S3_ACCESS_POINT_DISABLED",
                "credentials cannot be created for a disabled Access Point",
            ));
        }
        let snapshot = self
            .repository
            .get_snapshot(&tenant_id, &access_point.snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        require_active_for_mutation(&snapshot.lifecycle, "Snapshot")?;
        self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
            .await?;
        self.expire_s3_credentials(&access_point_id).await?;
        let (credential, secret) = self
            .build_s3_credential_record(
                &access_point,
                expires_at,
                &request_id,
                mutation.created_at_unix_ms,
            )
            .await?;
        let outcome = self
            .repository
            .create_s3_credential_idempotent(mutation, credential)
            .await
            .map_err(map_s3_credential_mutation_error)?;
        let (credential, replayed) = match outcome {
            CatalogInsertOutcome::Inserted(credential) => (credential, false),
            CatalogInsertOutcome::Existing(credential) => (credential, true),
        };
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CreateS3CredentialResponse {
            credential: s3_credential_view(&credential),
            secret_access_key: if replayed { String::new() } else { secret },
            replayed: replayed || task_replayed,
            task,
        })
    }

    /// Authorizes one workload-authenticated Gateway S3 request. Central resolves credentials,
    /// verifies SigV4, checks the frozen Snapshot/route fences and signs a short-lived read ticket;
    /// object bytes never cross this boundary.
    pub async fn authorize_s3_request(
        &self,
        request: S3AuthorizeRequest,
    ) -> Result<S3AuthorizeResponse, Error> {
        request
            .validate_signed_operation_binding()
            .map_err(|_| s3_access_denied())?;
        let requested_gateway_pool_id =
            neoengram_domain::protocol::GatewayPoolId::new(request.gateway_pool_id)
                .map_err(|_| s3_access_denied())?;
        let access_point = self
            .repository
            .get_s3_access_point_by_bucket(&request.bucket)
            .await
            .map_err(map_central_error)?
            .ok_or_else(s3_access_denied)?;
        if access_point.state != S3AccessPointState::Active {
            return Err(s3_access_denied());
        }
        self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
            .await?;
        let snapshot = self
            .repository
            .get_snapshot(&access_point.tenant_id, &access_point.snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(s3_snapshot_unavailable)?;
        if snapshot.commit_id != access_point.commit_id {
            return Err(s3_snapshot_unavailable());
        }
        let commit = self.load_s3_access_point_commit(&access_point).await?;
        let (volume, _placement, route, _pool) = self
            .resolve_s3_route_for_access_point(&access_point, &commit, self.clock.now())
            .await?;
        if route.gateway_pool_id != requested_gateway_pool_id {
            return Err(s3_access_denied());
        }
        self.expire_s3_credentials(&access_point.access_point_id)
            .await?;
        let (access_key_id, credential_region, service) = request
            .sigv4
            .credential_scope()
            .map_err(|_| s3_access_denied())?;
        if service != "s3" || credential_region != volume.region {
            return Err(s3_access_denied());
        }
        let now = self.clock.now();
        let credential = self
            .repository
            .get_s3_credential_by_access_key(&access_key_id)
            .await
            .map_err(map_central_error)?
            .filter(|credential| {
                credential.access_point_id == access_point.access_point_id
                    && credential.state == S3CredentialState::Active
                    && credential.expires_at_unix_ms.get() > now.get()
            })
            .ok_or_else(s3_access_denied)?;
        let secret = Zeroizing::new(
            self.s3_secret_envelope
                .decrypt_secret(
                    &s3_secret_context(&credential),
                    &credential.encrypted_secret,
                )
                .await
                .map_err(|_| internal_catalog_error())?,
        );
        let claims = request
            .sigv4
            .verify_at(secret.as_slice(), now.get() / 1000)
            .map_err(|_| s3_access_denied())?;
        if claims.access_key_id != credential.access_key_id
            || claims.region != volume.region
            || claims.service != "s3"
        {
            return Err(s3_access_denied());
        }
        self.ensure_s3_gateway_pool_ready(&route.gateway_pool_id)
            .await?;
        let response = match request.operation {
            S3AuthorizeOperation::HeadBucket | S3AuthorizeOperation::GetBucketLocation => {
                Ok(S3AuthorizeResponse::Bucket {
                    region: volume.region,
                })
            }
            S3AuthorizeOperation::ListObjectsV2 {
                prefix,
                delimiter,
                continuation_token,
                start_after,
                max_keys,
            } => {
                let prefix = validate_s3_prefix(prefix)?;
                let delimiter = delimiter.unwrap_or_default();
                if !delimiter.is_empty() && delimiter != "/" {
                    return Err(invalid_request("delimiter must be '/' when provided"));
                }
                if max_keys > 1000 {
                    return Err(invalid_request("max_keys cannot exceed 1000"));
                }
                let scope = S3ObjectCursorScope {
                    access_point_id: access_point.access_point_id.to_string(),
                    policy_generation: access_point.policy_generation,
                    snapshot_id: access_point.snapshot_id.to_string(),
                    index_digest: commit.index_version.digest.to_string(),
                    prefix: prefix.clone(),
                    delimiter: delimiter.clone(),
                };
                let all_entries = s3_catalog_entries(&commit, &prefix, &delimiter);
                let start_index = if let Some(cursor) = continuation_token.as_deref() {
                    let cursor =
                        decode_s3_object_cursor(&self.s3_cursor_signing_key, cursor, &scope)?;
                    s3_object_cursor_start(&all_entries, &cursor)?
                } else if let Some(start_after) = start_after.filter(|value| !value.is_empty()) {
                    validate_s3_cursor_key(&start_after)?;
                    all_entries.partition_point(|(key, _)| key <= &start_after)
                } else {
                    0
                };
                let mut entries = all_entries
                    .into_iter()
                    .skip(start_index)
                    .take(usize::from(max_keys) + 1)
                    .collect::<Vec<_>>();
                let has_more = entries.len() > usize::from(max_keys);
                entries.truncate(usize::from(max_keys));
                let next_continuation_token = if has_more && max_keys > 0 {
                    entries
                        .last()
                        .map(|(key, _)| {
                            encode_s3_object_cursor(
                                &self.s3_cursor_signing_key,
                                &scope,
                                start_index + entries.len(),
                                key,
                            )
                        })
                        .transpose()?
                } else {
                    None
                };
                let common_prefixes = entries
                    .iter()
                    .filter_map(|(key, record)| record.is_none().then_some(key.clone()))
                    .collect::<Vec<_>>();
                let objects = entries
                    .into_iter()
                    .filter_map(|(key, record)| {
                        record.map(|record| authorized_object(&key, record, &commit))
                    })
                    .collect();
                Ok(S3AuthorizeResponse::ListObjectsV2 {
                    objects,
                    common_prefixes,
                    next_continuation_token,
                })
            }
            S3AuthorizeOperation::HeadObject {
                key,
                range,
                if_match,
                if_none_match,
            } => {
                let key = validate_s3_object_key(key)?;
                let record = commit
                    .records
                    .iter()
                    .find(|record| record.path.as_str() == key)
                    .ok_or_else(|| resource_not_found("S3 object"))?;
                let object = authorized_object(&key, record, &commit);
                let etag = object.etag.to_string();
                let (status, content_length, content_range) = if if_match
                    .as_deref()
                    .is_some_and(|value| !s3_if_match_matches(value, &etag))
                {
                    (412, 0, None)
                } else if if_none_match
                    .as_deref()
                    .is_some_and(|value| s3_if_none_match_matches(value, &etag))
                {
                    (304, 0, None)
                } else if let Some(range) = range.as_deref() {
                    match parse_s3_authorized_range(range, record.total_size) {
                        Ok((start, end_exclusive)) => (
                            206,
                            end_exclusive.saturating_sub(start),
                            Some(format!(
                                "bytes {start}-{}/{size}",
                                end_exclusive - 1,
                                size = record.total_size
                            )),
                        ),
                        Err(S3AuthorizedRangeError::Unsatisfiable) => {
                            (416, 0, Some(format!("bytes */{}", record.total_size)))
                        }
                        Err(S3AuthorizedRangeError::Invalid) => {
                            return Err(invalid_request("Range must contain one valid byte range"));
                        }
                    }
                } else {
                    (200, record.total_size, None)
                };
                Ok(S3AuthorizeResponse::Object {
                    object,
                    status,
                    content_length,
                    content_range,
                    ticket: None,
                })
            }
            S3AuthorizeOperation::GetObject {
                key,
                range,
                if_match,
                if_none_match,
            } => {
                let key = validate_s3_object_key(key)?;
                let record = commit
                    .records
                    .iter()
                    .find(|record| record.path.as_str() == key)
                    .ok_or_else(|| resource_not_found("S3 object"))?;
                let object = authorized_object(&key, record, &commit);
                let etag = object.etag.to_string();
                if if_match
                    .as_deref()
                    .is_some_and(|value| !s3_if_match_matches(value, &etag))
                {
                    self.mark_s3_credential_used(&credential.credential_id, now)
                        .await;
                    return Ok(S3AuthorizeResponse::Object {
                        object,
                        status: 412,
                        content_length: 0,
                        content_range: None,
                        ticket: None,
                    });
                }
                if if_none_match
                    .as_deref()
                    .is_some_and(|value| s3_if_none_match_matches(value, &etag))
                {
                    self.mark_s3_credential_used(&credential.credential_id, now)
                        .await;
                    return Ok(S3AuthorizeResponse::Object {
                        object,
                        status: 304,
                        content_length: 0,
                        content_range: None,
                        ticket: None,
                    });
                }
                let (start, end_exclusive, status, content_range) = match range.as_deref() {
                    Some(range) => match parse_s3_authorized_range(range, record.total_size) {
                        Ok((start, end_exclusive)) => (
                            start,
                            end_exclusive,
                            206,
                            Some(format!(
                                "bytes {start}-{}/{size}",
                                end_exclusive - 1,
                                size = record.total_size
                            )),
                        ),
                        Err(S3AuthorizedRangeError::Unsatisfiable) => {
                            self.mark_s3_credential_used(&credential.credential_id, now)
                                .await;
                            return Ok(S3AuthorizeResponse::Object {
                                object,
                                status: 416,
                                content_length: 0,
                                content_range: Some(format!("bytes */{}", record.total_size)),
                                ticket: None,
                            });
                        }
                        Err(S3AuthorizedRangeError::Invalid) => {
                            return Err(invalid_request("Range must contain one valid byte range"));
                        }
                    },
                    None => (0, record.total_size, 200, None),
                };
                let ticket = self
                    .issue_s3_read_ticket(
                        &access_point,
                        &snapshot,
                        &commit,
                        record,
                        &key,
                        start,
                        end_exclusive,
                        claims.expires_at_unix_seconds,
                        now,
                    )
                    .await?;
                Ok(S3AuthorizeResponse::Object {
                    object,
                    status,
                    content_length: end_exclusive.saturating_sub(start),
                    content_range,
                    ticket: Some(Box::new(ticket)),
                })
            }
        }?;
        self.mark_s3_credential_used(&credential.credential_id, now)
            .await;
        Ok(response)
    }

    #[allow(clippy::too_many_arguments)]
    async fn issue_s3_read_ticket(
        &self,
        access_point: &S3AccessPointRecord,
        snapshot: &SnapshotRecord,
        commit: &CommitRecord,
        record: &FileRecord,
        key: &str,
        start: u64,
        end_exclusive: u64,
        sigv4_expires_at_unix_seconds: u64,
        now: UnixMillis,
    ) -> Result<S3ReadTicket, Error> {
        let (_, placement, route, _) = self
            .resolve_s3_route_for_access_point(access_point, commit, now)
            .await?;
        let owner_replica = self
            .gateway_registry
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?
            .get_replica(&route.gateway_replica_id)
            .await
            .map_err(map_central_error)?
            .filter(|replica| {
                replica.state == GatewayReplicaState::Active
                    && replica.credential.state == GatewayCredentialState::Active
                    && replica.gateway_pool_id == route.gateway_pool_id
                    && replica.edge_cluster_id == route.edge_cluster_id
                    && replica
                        .credential
                        .certificate_not_after_unix_ms
                        .is_some_and(|not_after| not_after.get() > now.get())
            })
            .ok_or_else(s3_snapshot_unavailable)?;
        let keyring = self
            .s3_ticket_keyring
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?;
        let sigv4_expiry_ms = sigv4_expires_at_unix_seconds.saturating_mul(1000);
        let expires_at = UnixMillis::new(now.get().saturating_add(30_000).min(sigv4_expiry_ms));
        if expires_at.get() <= now.get() {
            return Err(s3_access_denied());
        }
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|_| internal_catalog_error())?;
        let unsigned = S3ReadTicket {
            ticket_id: format!("s3read-{}", URL_SAFE_NO_PAD.encode(random)),
            tenant_id: access_point.tenant_id.to_string(),
            project_id: access_point.project_id.to_string(),
            artifact_id: access_point.artifact_id.to_string(),
            snapshot_id: access_point.snapshot_id.to_string(),
            snapshot_lifecycle_generation: snapshot.lifecycle.generation,
            commit_id: access_point.commit_id,
            index_digest: commit.index_version.digest,
            bucket: access_point.bucket_name.clone(),
            access_point_policy_generation: ResourceVersion::new(access_point.policy_generation),
            logical_path: key.to_owned(),
            manifest_id: record.manifest_id.digest(),
            size_bytes: record.total_size,
            allowed_start: start,
            allowed_end_exclusive: end_exclusive,
            gateway_pool_id: route.gateway_pool_id.to_string(),
            owner_replica_id: route.gateway_replica_id,
            owner_peer_endpoint: owner_replica.peer_endpoint,
            agent_connection_id: route.connection_id,
            route_generation: route.route_generation,
            agent_id: placement.agent_id,
            owner_generation: placement.owner_generation,
            mount_generation: placement.mount_generation,
            session_generation: placement.session_generation,
            issued_at_unix_ms: now,
            expires_at_unix_ms: expires_at,
            signature: GatewayOpaqueBytes::new(Vec::new()).map_err(|_| internal_catalog_error())?,
        };
        let payload = GatewayOpaqueBytes::new(
            unsigned
                .signing_bytes()
                .map_err(|_| internal_catalog_error())?,
        )
        .map_err(|_| internal_catalog_error())?;
        let signature = keyring
            .sign_with_ttl_ms(payload, now, expires_at.get() - now.get())
            .await
            .map_err(|_| s3_snapshot_unavailable())?;
        unsigned
            .with_central_signature(&signature)
            .map_err(|_| internal_catalog_error())
    }

    /// Lists the frozen Commit index behind an Access Point. This is deliberately served from
    /// Central metadata: the Gateway never has to reconstruct a mutable workspace index.
    pub async fn list_s3_objects(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryS3ObjectListRequest,
    ) -> Result<QueryS3ObjectListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::S3AccessRead, &tenant_id)
            .await?;
        let access_point_id = parse_s3_access_point_id(request.access_point_id)?;
        let access_point = self
            .repository
            .get_s3_access_point(&tenant_id, &access_point_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("S3 access point"))?;
        if access_point.state != S3AccessPointState::Active {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "s3_access_point_disabled",
                "S3_ACCESS_POINT_DISABLED",
                "the S3 Access Point is disabled",
                false,
            ));
        }
        self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
            .await?;
        self.expire_s3_credentials(&access_point.access_point_id)
            .await?;
        let prefix = validate_s3_prefix(request.prefix.unwrap_or_default())?;
        let delimiter = request.delimiter.unwrap_or_default();
        if !delimiter.is_empty() && delimiter != "/" {
            return Err(invalid_request("delimiter must be '/' when provided"));
        }
        let limit = s3_page_size(request.page_size)?;
        let commit = self
            .precommit_repository()?
            .get_commit(
                &tenant_id,
                &access_point.project_id,
                &access_point.artifact_id,
                CommitId::from_digest(access_point.commit_id),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("commit"))?;
        // Listing is part of the S3 read view as well.  Resolve the current complete v2 target
        // Coverage and live route before exposing logical keys; a frozen Snapshot index alone is
        // not evidence that any Volume can serve the corresponding bytes.
        self.resolve_s3_route_for_access_point(&access_point, &commit, self.clock.now())
            .await?;
        let scope = S3ObjectCursorScope {
            access_point_id: access_point.access_point_id.to_string(),
            policy_generation: access_point.policy_generation,
            snapshot_id: access_point.snapshot_id.to_string(),
            index_digest: commit.index_version.digest.to_string(),
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
        };
        let mut catalog = BTreeMap::new();
        for record in commit
            .records
            .iter()
            .filter(|record| record.path.as_str().starts_with(&prefix))
        {
            let key = record.path.to_string();
            if !delimiter.is_empty() {
                let rest = &key[prefix.len()..];
                if let Some(index) = rest.find('/') {
                    catalog
                        .entry(format!("{}{}", prefix, &rest[..index + 1]))
                        .or_insert(None);
                    continue;
                }
            }
            catalog.insert(key, Some(record));
        }
        let all_entries = catalog.into_iter().collect::<Vec<_>>();
        let start_index = if let Some(cursor) = request.cursor.as_deref() {
            let cursor = decode_s3_object_cursor(&self.s3_cursor_signing_key, cursor, &scope)?;
            s3_object_cursor_start(&all_entries, &cursor)?
        } else {
            0
        };
        let mut page = all_entries
            .into_iter()
            .skip(start_index)
            .take(usize::from(limit) + 1)
            .collect::<Vec<_>>();
        let has_more = page.len() > usize::from(limit);
        page.truncate(usize::from(limit));
        let next_cursor = if has_more {
            page.last()
                .map(|(key, _)| {
                    encode_s3_object_cursor(
                        &self.s3_cursor_signing_key,
                        &scope,
                        start_index + page.len(),
                        key,
                    )
                })
                .transpose()?
        } else {
            None
        };
        let common_prefixes = page
            .iter()
            .filter_map(|(key, record)| record.is_none().then_some(key.clone()))
            .collect::<Vec<_>>();
        Ok(QueryS3ObjectListResponse {
            items: page
                .into_iter()
                .map(|(key, record)| S3ObjectEntryView {
                    key,
                    entry_type: if record.is_some() { "object" } else { "prefix" }.to_owned(),
                    size_bytes: record.map(|record| record.total_size.to_string()),
                    etag: record.map(|record| record.manifest_id.to_string()),
                    last_modified_unix_ms: record.map(|_| commit.created_at_unix_ms.to_string()),
                })
                .collect(),
            common_prefixes,
            next_cursor,
        })
    }

    /// Creates a short-lived browser download URL. The Gateway validates the ticket before
    /// routing bytes; no S3 Secret is ever sent to the browser.
    pub async fn create_s3_download_url(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateS3DownloadUrlRequest,
    ) -> Result<CreateS3DownloadUrlResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::S3AccessRead, &tenant_id)
            .await?;
        let access_point_id = parse_s3_access_point_id(request.access_point_id)?;
        let access_point = self
            .repository
            .get_s3_access_point(&tenant_id, &access_point_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("S3 access point"))?;
        if access_point.state != S3AccessPointState::Active {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "s3_access_point_disabled",
                "S3_ACCESS_POINT_DISABLED",
                "the S3 Access Point is disabled",
                false,
            ));
        }
        self.ensure_s3_access_point_snapshot_delivery_ready(&access_point)
            .await?;
        let key = validate_s3_object_key(request.key)?;
        let expires_seconds = request.expires_seconds.unwrap_or(300).clamp(1, 300);
        let snapshot = self
            .repository
            .get_snapshot(&tenant_id, &access_point.snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        if snapshot.commit_id != access_point.commit_id {
            return Err(s3_snapshot_unavailable());
        }
        let commit = self
            .precommit_repository()?
            .get_commit(
                &tenant_id,
                &access_point.project_id,
                &access_point.artifact_id,
                CommitId::from_digest(snapshot.commit_id),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("commit"))?;
        if !commit
            .records
            .iter()
            .any(|record| record.path.as_str() == key)
        {
            return Err(resource_not_found("S3 object"));
        }
        let now = self.clock.now();
        let credential = self
            .repository
            .list_s3_credentials(&access_point.access_point_id)
            .await
            .map_err(map_central_error)?
            .into_iter()
            .rev()
            .find(|credential| {
                credential.state == S3CredentialState::Active
                    && credential.expires_at_unix_ms.get() > now.get()
            })
            .ok_or_else(|| {
                application_error(
                    ErrorCategory::Unavailable,
                    "s3_credential_unavailable",
                    "S3_CREDENTIAL_UNAVAILABLE",
                    "the Access Point has no active credential for presigned downloads",
                    false,
                )
            })?;
        let secret = Zeroizing::new(
            self.s3_secret_envelope
                .decrypt_secret(
                    &s3_secret_context(&credential),
                    &credential.encrypted_secret,
                )
                .await
                .map_err(|_| internal_catalog_error())?,
        );
        let endpoint = self.s3_endpoint_for_access_point(&access_point).await?;
        let commit = self.load_s3_access_point_commit(&access_point).await?;
        let (volume, _, _, _) = self
            .resolve_s3_route_for_access_point(&access_point, &commit, now)
            .await?;
        let url = presign_s3_get(&S3PresignRequest {
            endpoint: &endpoint,
            bucket: &access_point.bucket_name,
            key: &key,
            region: &volume.region,
            access_key_id: &credential.access_key_id,
            secret: &secret,
            now_unix_seconds: now.get() / 1_000,
            expires_seconds,
        })
        .map_err(|_| internal_catalog_error())?;
        let expires_at =
            UnixMillis::new(now.get().saturating_add(u64::from(expires_seconds) * 1000));
        Ok(CreateS3DownloadUrlResponse {
            url,
            expires_at_unix_ms: expires_at.to_string(),
        })
    }

    pub async fn revoke_s3_credential(
        &self,
        identity: &AuthenticatedIdentity,
        request: RevokeS3CredentialRequest,
    ) -> Result<QueryS3CredentialListResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id.clone())?;
        self.require_tenant(identity, Permission::S3AccessManage, &tenant_id)
            .await?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let access_point_id = parse_s3_access_point_id(request.access_point_id)?;
        let credential_id = parse_s3_credential_id(request.credential_id)?;
        let (task, _task_replayed) = self
            .begin_operation_task(
                TaskKind::S3Lifecycle,
                TaskScope::new(tenant_id.clone()),
                request_id.clone(),
                &task_request,
                identity,
                Some("s3_credential"),
                Some(credential_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::S3AccessPoint,
            access_point_id.to_string(),
            TaskResourceRole::Related,
        )
        .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::S3Credential,
            credential_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let mutation = s3_mutation_record(
            &tenant_id,
            &request_id,
            S3MutationKind::CredentialRevoke,
            &serde_json::json!({
                "tenant_id": tenant_id.as_str(),
                "access_point_id": access_point_id.as_str(),
                "credential_id": credential_id.as_str(),
            }),
            self.clock.now(),
        )?;
        let outcome = self
            .repository
            .revoke_s3_credential_idempotent(mutation, &access_point_id, &credential_id)
            .await
            .map_err(map_s3_mutation_error)?;
        if matches!(outcome, CatalogInsertOutcome::Inserted(_)) {
            self.expire_s3_credentials(&access_point_id).await?;
        }
        let items = self
            .repository
            .list_s3_credentials(&access_point_id)
            .await
            .map_err(map_central_error)?
            .iter()
            .map(s3_credential_view)
            .collect();
        let task = self.complete_operation_task(task, identity).await?;
        Ok(QueryS3CredentialListResponse { items, task })
    }

    async fn build_s3_credential_record(
        &self,
        access_point: &S3AccessPointRecord,
        expires_at: Option<UnixMillis>,
        request_id: &RequestId,
        created_at_unix_ms: UnixMillis,
    ) -> Result<(S3CredentialRecord, String), Error> {
        let mut random = [0_u8; 32];
        getrandom::fill(&mut random).map_err(|_| internal_catalog_error())?;
        let secret = URL_SAFE_NO_PAD.encode(random);
        let id_bytes = format!("{}\0{}", access_point.access_point_id, request_id).into_bytes();
        let digest = blake3::hash(&id_bytes).to_hex().to_string();
        let credential_id = S3CredentialId::new(format!("s3cred-{}", &digest[..24]))
            .map_err(|error| invalid_request(format!("credential_id: {error}")))?;
        let access_key_id = format!("NGS3{}", &digest[..16]);
        let expires_at = expires_at.unwrap_or_else(|| {
            UnixMillis::new(
                created_at_unix_ms
                    .get()
                    .saturating_add(90 * 24 * 60 * 60 * 1000),
            )
        });
        let mut record = S3CredentialRecord {
            credential_id,
            access_point_id: access_point.access_point_id.clone(),
            access_key_id,
            encrypted_secret: Vec::new(),
            state: S3CredentialState::Active,
            expires_at_unix_ms: expires_at,
            created_at_unix_ms,
            last_used_at_unix_ms: None,
        };
        record.encrypted_secret = self
            .s3_secret_envelope
            .encrypt_secret(&s3_secret_context(&record), secret.as_bytes())
            .await
            .map_err(|_| internal_catalog_error())?;
        Ok((record, secret))
    }

    async fn expire_s3_credentials(&self, access_point_id: &S3AccessPointId) -> Result<(), Error> {
        self.repository
            .expire_s3_credentials(access_point_id, self.clock.now())
            .await
            .map(|_| ())
            .map_err(map_central_error)
    }

    async fn mark_s3_credential_used(
        &self,
        credential_id: &S3CredentialId,
        used_at_unix_ms: UnixMillis,
    ) {
        if let Err(error) = self
            .repository
            .update_s3_credential_last_used(credential_id, used_at_unix_ms)
            .await
        {
            tracing::warn!(
                credential_id = %credential_id,
                error = %error,
                "failed to persist S3 credential usage timestamp"
            );
        }
    }

    async fn ensure_s3_gateway_pool_ready(
        &self,
        gateway_pool_id: &neoengram_domain::protocol::GatewayPoolId,
    ) -> Result<(), Error> {
        let registry = self
            .gateway_registry
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?;
        let pool = registry
            .get_pool(gateway_pool_id)
            .await
            .map_err(map_central_error)?
            .filter(|pool| pool.state == GatewayPoolState::Ready)
            .ok_or_else(s3_snapshot_unavailable)?;
        if pool.gateway_pool_id != *gateway_pool_id {
            return Err(s3_snapshot_unavailable());
        }
        let now = self.clock.now();
        let minimum_ready = usize::from(pool.minimum_ready_replicas);
        let mut ready_replicas = 0_usize;
        let mut after = None;
        loop {
            let replicas = registry
                .list_replicas(&crate::GatewayReplicaListRequest {
                    gateway_pool_id: gateway_pool_id.clone(),
                    state: Some(GatewayReplicaState::Active),
                    after: after.clone(),
                    limit: crate::GATEWAY_REGISTRY_MAX_PAGE_SIZE,
                })
                .await
                .map_err(map_central_error)?;
            ready_replicas += replicas
                .iter()
                .filter(|replica| {
                    replica.gateway_pool_id == *gateway_pool_id
                        && replica.edge_cluster_id == pool.edge_cluster_id
                        && replica.state == GatewayReplicaState::Active
                        && replica.credential.state == GatewayCredentialState::Active
                        && replica.credential.certificate_generation.is_some()
                        && replica
                            .credential
                            .certificate_not_after_unix_ms
                            .is_some_and(|not_after| not_after.get() > now.get())
                        && replica.last_heartbeat_at_unix_ms.is_some_and(|heartbeat| {
                            heartbeat.get() <= now.get()
                                && now.get() - heartbeat.get()
                                    <= crate::AGENT_ROUTE_LEASE_MAX_TTL_MS
                        })
                })
                .count();
            if ready_replicas >= minimum_ready {
                return Ok(());
            }
            if replicas.len() < crate::GATEWAY_REGISTRY_MAX_PAGE_SIZE {
                return Err(s3_snapshot_unavailable());
            }
            after = replicas
                .last()
                .map(|replica| replica.gateway_replica_id.clone());
        }
    }

    /// Resolves the current data placement and Gateway route for a Commit. Access Point records
    /// intentionally do not cache this information: Volume failures, replica promotion and
    /// Gateway route changes must take effect without rewriting S3 metadata.
    pub(crate) async fn v2_commit_coverage_for_volume(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        commit: &CommitRecord,
        volume_id: &StorageVolumeId,
        expected_generation: Option<PlacementGeneration>,
    ) -> Result<Option<VolumeCommitCoverage>, Error> {
        let Some(placement_authority) = self.placement.as_ref() else {
            return Ok(None);
        };
        let stored = placement_authority
            .get_commit_object_set(tenant_id, &ContentDigest::from(commit.commit_id))
            .await
            .map_err(map_central_error)?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        if stored.tenant_id != *tenant_id
            || stored.commit_id.digest() != ContentDigest::from(commit.commit_id)
            || stored.object_set.object_set_digest != commit.object_set_digest
        {
            return Ok(None);
        }
        let namespace = ObjectNamespaceId::new(artifact_id.to_string())
            .map_err(|error| invalid_request(format!("object namespace: {error}")))?;
        let object_set =
            neoengram_domain::protocol::materialization::NamespaceObjectSet::from_object_set(
                tenant_id.clone(),
                namespace.clone(),
                stored.commit_id,
                &stored.object_set,
            )
            .map_err(|error| invalid_request(format!("commit object set: {error}")))?;
        let mut placements = Vec::<ObjectPlacement>::new();
        for object in &object_set.objects {
            for placement in placement_authority
                .object_placements_v2(tenant_id, &namespace, &object.object_id)
                .await
                .map_err(map_central_error)?
                .into_iter()
                .filter(|placement| {
                    placement.readable()
                        && placement.tenant_id == *tenant_id
                        && placement.object_namespace_id == namespace
                        && placement.matches_ref(object)
                })
            {
                let unhealthy = placement_authority
                    .latest_placement_health(
                        tenant_id,
                        &namespace,
                        &placement.placement_id,
                        placement.placement_generation,
                    )
                    .await
                    .map_err(map_central_error)?
                    .is_some_and(|observation| {
                        matches!(
                            observation.state,
                            neoengram_domain::protocol::PlacementHealthState::Missing
                                | neoengram_domain::protocol::PlacementHealthState::Corrupt
                        )
                    });
                if !unhealthy {
                    placements.push(placement);
                }
            }
        }
        // In a live composition the owner generation is the authoritative physical fence. Do
        // not select the newest historical Placement row: after a Volume takeover, an old
        // generation may still describe a complete object set that the current Agent cannot
        // serve. Metadata-only compositions have no owner provider, so retain the deterministic
        // historical fallback used by offline/catalog tests.
        let generation = if let Some(expected) = expected_generation {
            Some(expected)
        } else if let Some(provider) = &self.s3_placement {
            provider
                .current_placement(tenant_id, volume_id)
                .await
                .map_err(map_central_error)?
                .map(|placement| PlacementGeneration::new(placement.owner_generation.get()))
        } else {
            placements
                .iter()
                .filter(|placement| placement.storage_volume_id.as_ref() == Some(volume_id))
                .map(|placement| placement.placement_generation)
                .max()
                .or_else(|| {
                    object_set
                        .objects
                        .is_empty()
                        .then_some(PlacementGeneration::new(1))
                })
        };
        let Some(generation) = generation else {
            return Ok(None);
        };
        let legacy_object_set = ObjectSet::new(
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
        )
        .map_err(|error| invalid_request(format!("commit object set: {error}")))?;
        let coverage = VolumeCommitCoverage::from_placements(
            tenant_id.clone(),
            namespace,
            stored.commit_id,
            volume_id.clone(),
            generation,
            &legacy_object_set,
            &placements,
        )
        .map_err(|error| invalid_request(format!("coverage: {error}")))?;
        Ok(Some(coverage))
    }

    /// Computes Commit content health from namespace-scoped v2 object evidence.
    ///
    /// A Commit is not considered readable merely because a legacy PlacementSet exists.  Every
    /// object in the immutable ObjectSet must have at least one matching, verified v2 Placement;
    /// non-readable evidence is retained as a degraded signal when another copy is available.
    /// `None` means that placement authority is not configured, preserving metadata-only catalog
    /// compositions without reintroducing a legacy read fallback.
    pub(crate) async fn v2_commit_data_health(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        commit: &CommitRecord,
    ) -> Result<Option<DataHealth>, Error> {
        let Some(placement_authority) = self.placement.as_ref() else {
            return Ok(None);
        };
        let commit_digest = ContentDigest::from(commit.commit_id);
        let Some(stored) = placement_authority
            .get_commit_object_set(tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
        else {
            return Ok(Some(DataHealth::Unavailable));
        };
        if stored.tenant_id != *tenant_id
            || stored.commit_id.digest() != commit_digest
            || stored.object_set.object_set_digest != commit.object_set_digest
        {
            return Ok(Some(DataHealth::Unavailable));
        }
        let namespace = ObjectNamespaceId::new(artifact_id.to_string())
            .map_err(|error| invalid_request(format!("object namespace: {error}")))?;
        let object_set = NamespaceObjectSet::from_object_set(
            tenant_id.clone(),
            namespace.clone(),
            stored.commit_id,
            &stored.object_set,
        )
        .map_err(|error| invalid_request(format!("commit object set: {error}")))?;
        Ok(Some(
            self.v2_object_set_data_health(tenant_id, &namespace, &object_set)
                .await?,
        ))
    }

    /// Resolves v2 content health when only the authority Commit ObjectSet digest is available.
    /// This is used by placement/workspace actions that do not load the richer catalog
    /// `CommitRecord`; the namespace remains explicit so object evidence cannot cross Artifact
    /// boundaries.
    pub(crate) async fn v2_commit_data_health_for_digest(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &ObjectNamespaceId,
        commit_digest: &ContentDigest,
    ) -> Result<Option<DataHealth>, Error> {
        let Some(placement_authority) = self.placement.as_ref() else {
            return Ok(None);
        };
        let Some(stored) = placement_authority
            .get_commit_object_set(tenant_id, commit_digest)
            .await
            .map_err(map_central_error)?
        else {
            return Ok(Some(DataHealth::Unavailable));
        };
        if stored.tenant_id != *tenant_id || stored.commit_id.digest() != *commit_digest {
            return Ok(Some(DataHealth::Unavailable));
        }
        let object_set = NamespaceObjectSet::from_object_set(
            tenant_id.clone(),
            object_namespace_id.clone(),
            stored.commit_id,
            &stored.object_set,
        )
        .map_err(|error| invalid_request(format!("commit object set: {error}")))?;
        Ok(Some(
            self.v2_object_set_data_health(tenant_id, object_namespace_id, &object_set)
                .await?,
        ))
    }

    async fn v2_object_set_data_health(
        &self,
        tenant_id: &TenantId,
        namespace: &ObjectNamespaceId,
        object_set: &NamespaceObjectSet,
    ) -> Result<DataHealth, Error> {
        let placement_authority = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let mut degraded = false;
        for object in &object_set.objects {
            let placements = placement_authority
                .object_placements_v2(tenant_id, namespace, &object.object_id)
                .await
                .map_err(map_central_error)?;
            let mut has_readable = false;
            let mut has_unhealthy = false;
            let mut has_matching = false;
            for placement in placements.iter().filter(|placement| {
                placement.tenant_id == *tenant_id
                    && placement.object_namespace_id == *namespace
                    && placement.matches_ref(object)
            }) {
                has_matching = true;
                let unhealthy = placement_authority
                    .latest_placement_health(
                        tenant_id,
                        namespace,
                        &placement.placement_id,
                        placement.placement_generation,
                    )
                    .await
                    .map_err(map_central_error)?
                    .is_some_and(|observation| {
                        matches!(
                            observation.state,
                            neoengram_domain::protocol::PlacementHealthState::Missing
                                | neoengram_domain::protocol::PlacementHealthState::Corrupt
                        )
                    });
                if unhealthy {
                    has_unhealthy = true;
                } else if placement.readable() {
                    has_readable = true;
                }
            }
            if !has_matching || !has_readable {
                return Ok(DataHealth::Unavailable);
            }
            if has_unhealthy {
                degraded = true;
            }
        }
        Ok(if degraded {
            DataHealth::Degraded
        } else {
            DataHealth::Available
        })
    }

    #[allow(dead_code)]
    async fn resolve_s3_route_for_commit(
        &self,
        tenant_id: &TenantId,
        commit: &CommitRecord,
        now: UnixMillis,
    ) -> Result<
        (
            StorageVolumeRecord,
            S3AgentPlacement,
            crate::AgentRouteLease,
            crate::GatewayPoolRecord,
        ),
        Error,
    > {
        let placement_provider = self
            .s3_placement
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?;
        let registry = self
            .gateway_registry
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?;
        let placement_authority = self
            .placement
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?;
        // S3 is placement-first: only a complete v2 Coverage on a single Volume can make a
        // Commit readable. A global object union or legacy publication is insufficient.
        let commit_digest = ContentDigest::from(commit.commit_id);
        let mut volume_ids = Vec::new();
        let stored = placement_authority
            .get_commit_object_set(tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(s3_snapshot_unavailable)?;
        let namespace = ObjectNamespaceId::new(commit.artifact_id.to_string())
            .map_err(|_| s3_snapshot_unavailable())?;
        let mut candidate_volumes = BTreeSet::new();
        for object in &stored.object_set.objects {
            for placement in placement_authority
                .object_placements_v2(tenant_id, &namespace, &object.object_id)
                .await
                .map_err(map_central_error)?
            {
                let unhealthy = placement_authority
                    .latest_placement_health(
                        tenant_id,
                        &namespace,
                        &placement.placement_id,
                        placement.placement_generation,
                    )
                    .await
                    .map_err(map_central_error)?
                    .is_some_and(|observation| {
                        matches!(
                            observation.state,
                            neoengram_domain::protocol::PlacementHealthState::Missing
                                | neoengram_domain::protocol::PlacementHealthState::Corrupt
                        )
                    });
                if unhealthy {
                    continue;
                }
                if placement.readable()
                    && placement.tenant_id == *tenant_id
                    && placement.object_namespace_id == namespace
                    && placement.object_id == object.object_id
                    && placement.size == object.size
                    && placement.encoding == object.encoding
                    && placement.verified_digest == object.object_id.digest()
                {
                    if let Some(volume_id) = placement.storage_volume_id {
                        candidate_volumes.insert(volume_id);
                    }
                }
            }
        }
        for volume_id in candidate_volumes {
            if self
                .v2_commit_coverage_for_volume(
                    tenant_id,
                    &commit.artifact_id,
                    commit,
                    &volume_id,
                    None,
                )
                .await?
                .is_some_and(|coverage| Self::v2_coverage_is_readable(Some(&coverage)))
            {
                volume_ids.push(volume_id);
            }
        }
        volume_ids.sort();
        volume_ids.dedup();
        for volume_id in volume_ids {
            let Some(volume) = self
                .repository
                .get_storage_volume(tenant_id, &volume_id)
                .await
                .map_err(map_central_error)?
            else {
                continue;
            };
            let Some(placement) = placement_provider
                .current_placement(tenant_id, &volume_id)
                .await
                .map_err(map_central_error)?
            else {
                continue;
            };
            let Some(route) = registry
                .get_agent_route(&placement.agent_id)
                .await
                .map_err(map_central_error)?
                .filter(|route| {
                    route.is_active_at(now)
                        && route.session_generation == placement.session_generation
                })
            else {
                continue;
            };
            let Some(pool) = registry
                .get_pool(&route.gateway_pool_id)
                .await
                .map_err(map_central_error)?
                .filter(|pool| pool.state == GatewayPoolState::Ready)
            else {
                continue;
            };
            return Ok((volume, placement, route, pool));
        }
        Err(s3_snapshot_unavailable())
    }

    /// Resolves the live route for a specific SnapshotDelivery target.  This is intentionally
    /// separate from the legacy commit-wide resolver: an S3 Access Point must never fail over to
    /// another Volume merely because that Volume also has complete Commit coverage.
    async fn resolve_s3_route_for_volume(
        &self,
        tenant_id: &TenantId,
        commit: &CommitRecord,
        volume_id: &StorageVolumeId,
        now: UnixMillis,
    ) -> Result<
        (
            StorageVolumeRecord,
            S3AgentPlacement,
            crate::AgentRouteLease,
            crate::GatewayPoolRecord,
        ),
        Error,
    > {
        let placement_provider = self
            .s3_placement
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?;
        let registry = self
            .gateway_registry
            .as_ref()
            .ok_or_else(s3_snapshot_unavailable)?;
        let volume = self
            .repository
            .get_storage_volume(tenant_id, volume_id)
            .await
            .map_err(map_central_error)?
            .filter(|volume| {
                volume.lifecycle.is_active() && volume.state == StorageVolumeState::Ready
            })
            .ok_or_else(s3_snapshot_unavailable)?;
        let coverage = self
            .v2_commit_coverage_for_volume(tenant_id, &commit.artifact_id, commit, volume_id, None)
            .await?;
        if !Self::v2_coverage_is_readable(coverage.as_ref()) {
            return Err(s3_snapshot_unavailable());
        }
        let placement = placement_provider
            .current_placement(tenant_id, volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(s3_snapshot_unavailable)?;
        let route = registry
            .get_agent_route(&placement.agent_id)
            .await
            .map_err(map_central_error)?
            .filter(|route| {
                route.is_active_at(now)
                    && route.session_generation == placement.session_generation
                    && route.edge_cluster_id == volume.edge_cluster_id
            })
            .ok_or_else(s3_snapshot_unavailable)?;
        let pool = registry
            .get_pool(&route.gateway_pool_id)
            .await
            .map_err(map_central_error)?
            .filter(|pool| {
                pool.state == GatewayPoolState::Ready
                    && pool.edge_cluster_id == volume.edge_cluster_id
            })
            .ok_or_else(s3_snapshot_unavailable)?;
        Ok((volume, placement, route, pool))
    }

    /// Validates the logical Snapshot gate shared by S3 management and read paths.  A historical
    /// `SnapshotState::Ready` value is not sufficient: the one Delivery created with the Snapshot
    /// must still exist, remain bound to the same immutable fields, and be Ready now.
    async fn ensure_snapshot_delivery_ready(
        &self,
        snapshot: &SnapshotRecord,
    ) -> Result<SnapshotDeliveryRecord, Error> {
        if !snapshot.lifecycle.is_active() || snapshot.state != SnapshotState::Ready {
            return Err(s3_snapshot_unavailable());
        }
        let delivery = self
            .repository
            .get_snapshot_delivery(&snapshot.tenant_id, &snapshot.delivery_id)
            .await
            .map_err(map_central_error)?
            .filter(|delivery| {
                delivery.tenant_id == snapshot.tenant_id
                    && delivery.snapshot_id == snapshot.snapshot_id
                    && delivery.commit_id == snapshot.commit_id
                    && delivery.storage_volume_id == snapshot.storage_volume_id
                    && delivery.mode == snapshot.delivery_mode
                    && delivery.state == SnapshotDeliveryState::Ready
            })
            .ok_or_else(s3_snapshot_unavailable)?;
        Ok(delivery)
    }

    /// Validates that an Access Point still points at the Snapshot's sole physical Delivery.
    /// This is intentionally independent from route/coverage checks so credential management
    /// cannot resurrect access while the Delivery is pending, failed, or deleted.
    async fn ensure_s3_access_point_snapshot_delivery_ready(
        &self,
        access_point: &S3AccessPointRecord,
    ) -> Result<SnapshotDeliveryRecord, Error> {
        let snapshot = self
            .repository
            .get_snapshot(&access_point.tenant_id, &access_point.snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(s3_snapshot_unavailable)?;
        if snapshot.project_id != access_point.project_id
            || snapshot.artifact_id != access_point.artifact_id
            || snapshot.commit_id != access_point.commit_id
            || snapshot.delivery_id != access_point.delivery_id
            || snapshot.storage_volume_id != access_point.storage_volume_id
            || snapshot.edge_cluster_id != access_point.edge_cluster_id
        {
            return Err(s3_snapshot_unavailable());
        }
        let delivery = self.ensure_snapshot_delivery_ready(&snapshot).await?;
        if delivery.storage_volume_id != access_point.storage_volume_id {
            return Err(s3_snapshot_unavailable());
        }
        Ok(delivery)
    }

    async fn resolve_s3_route_for_access_point(
        &self,
        access_point: &S3AccessPointRecord,
        commit: &CommitRecord,
        now: UnixMillis,
    ) -> Result<
        (
            StorageVolumeRecord,
            S3AgentPlacement,
            crate::AgentRouteLease,
            crate::GatewayPoolRecord,
        ),
        Error,
    > {
        self.ensure_s3_access_point_snapshot_delivery_ready(access_point)
            .await?;
        let (volume, placement, route, pool) = self
            .resolve_s3_route_for_volume(
                &access_point.tenant_id,
                commit,
                &access_point.storage_volume_id,
                now,
            )
            .await?;
        if volume.edge_cluster_id != access_point.edge_cluster_id
            || route.edge_cluster_id != access_point.edge_cluster_id
        {
            return Err(s3_snapshot_unavailable());
        }
        Ok((volume, placement, route, pool))
    }

    async fn load_s3_access_point_commit(
        &self,
        access_point: &S3AccessPointRecord,
    ) -> Result<CommitRecord, Error> {
        let artifact = self
            .repository
            .get_artifact(
                &access_point.tenant_id,
                &access_point.project_id,
                &access_point.artifact_id,
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(s3_snapshot_unavailable)?;
        self.load_published_commit(&artifact, CommitId::from_digest(access_point.commit_id))
            .await
    }

    async fn s3_access_point_view(
        &self,
        record: &S3AccessPointRecord,
    ) -> Result<S3AccessPointView, Error> {
        let commit = self.load_s3_access_point_commit(record).await?;
        let (volume, _, _, pool) = self
            .resolve_s3_route_for_access_point(record, &commit, self.clock.now())
            .await?;
        Ok(S3AccessPointView {
            access_point_id: record.access_point_id.to_string(),
            tenant_id: record.tenant_id.to_string(),
            project_id: record.project_id.to_string(),
            artifact_id: record.artifact_id.to_string(),
            snapshot_id: record.snapshot_id.to_string(),
            commit_id: record.commit_id.to_string(),
            delivery_id: record.delivery_id.to_string(),
            storage_volume_id: record.storage_volume_id.to_string(),
            edge_cluster_id: record.edge_cluster_id.to_string(),
            bucket_name: record.bucket_name.clone(),
            endpoint: pool
                .s3_endpoint
                .map(|endpoint| endpoint.trim_end_matches('/').to_owned())
                .filter(|endpoint| !endpoint.is_empty())
                .ok_or_else(s3_snapshot_unavailable)?,
            region: volume.region,
            state: s3_access_point_state_name(record.state).to_owned(),
            policy_generation: record.policy_generation.to_string(),
            created_at_unix_ms: record.created_at_unix_ms.to_string(),
            updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
        })
    }

    async fn s3_endpoint_for_access_point(
        &self,
        record: &S3AccessPointRecord,
    ) -> Result<String, Error> {
        let commit = self.load_s3_access_point_commit(record).await?;
        let (_, _, _, pool) = self
            .resolve_s3_route_for_access_point(record, &commit, self.clock.now())
            .await?;
        pool.s3_endpoint
            .map(|endpoint| endpoint.trim_end_matches('/').to_owned())
            .filter(|endpoint| !endpoint.is_empty())
            .ok_or_else(|| {
                application_error(
                    ErrorCategory::Unavailable,
                    "gateway_s3_endpoint_unavailable",
                    "GATEWAY_S3_ENDPOINT_UNAVAILABLE",
                    "the GatewayPool S3 endpoint is not configured",
                    true,
                )
            })
    }

    async fn ensure_workspace_materialization(&self, playground: &PlaygroundRecord) {
        if playground.state != PlaygroundState::Creating {
            return;
        }
        let Some(coordinator) = &self.coordinator else {
            return;
        };
        if let Err(error) = coordinator
            .ensure_workspace_materialization(playground)
            .await
        {
            tracing::warn!(
                tenant_id = %playground.tenant_id,
                project_id = %playground.project_id,
                artifact_id = %playground.artifact_id,
                playground_id = %playground.playground_id,
                %error,
                "immediate Playground materialization dispatch failed; recovery will retry"
            );
        }
    }

    pub async fn start_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: StartPreCommitRequest,
    ) -> Result<StartPreCommitResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        self.require_tenant(identity, Permission::CreateAddJob, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let playground_id = parse_playground_id(request.playground_id)?;
        self.require_storage_availability_configured()?;
        let precommit_request_id = RequestId::new(request.precommit_request_id.clone())
            .map_err(|error| invalid_request(format!("precommit_request_id: {error}")))?;
        let source_index_version = parse_index_version(request.expected_index_version)?;
        let data_layout = match request.data_layout {
            crate::dto::DataLayout::FastCdc => CommitDataLayout::FastCdc,
            crate::dto::DataLayout::WholeFile => CommitDataLayout::WholeFile,
        };
        let precommit_id = deterministic_precommit_id(&tenant_id, &precommit_request_id)?;
        let key = PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
        let (mut task, _task_replayed) = self
            .begin_operation_task(
                TaskKind::PrecommitCheck,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id.clone()),
                    artifact_id: Some(artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(&artifact_id)),
                    commit_id: None,
                    playground_id: Some(playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: None,
                },
                precommit_request_id.clone(),
                &task_request,
                identity,
                Some("precommit"),
                Some(precommit_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Precommit,
            precommit_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let (precommits, coordinator) = self.precommit_execution()?;
        let now = self.clock.now();

        // The proposed ID is derived from the idempotency key. Resolve a persisted replay before
        // consulting mutable Playground/Index state, which may already have advanced.
        if let Some(existing) = precommits.get(&key).await.map_err(map_central_error)? {
            let outcome = precommits
                .start(DomainPreCommitStartRequest {
                    tenant_id,
                    project_id,
                    artifact_id,
                    playground_id,
                    precommit_id,
                    precommit_request_id,
                    source_index_version,
                    data_layout,
                    frozen_head_commit_id: existing.frozen_head_commit_id,
                    job_id: existing.job_id.clone(),
                    created_at_unix_ms: now,
                })
                .await
                .map_err(precommit_mutation_error)?;
            if outcome.precommit.state == PreCommitState::Running {
                coordinator
                    .ensure_precommit_job(&outcome.precommit)
                    .await
                    .map_err(map_central_error)?;
            }
            let playground = self
                .load_precommit_playground(identity, &outcome.precommit, Permission::PlaygroundRead)
                .await?;
            return Ok(StartPreCommitResponse {
                precommit: precommit_view(&outcome.precommit),
                playground: self.playground_view(&playground).await?,
                replayed: outcome.replayed,
                task,
            });
        }

        let playground = self
            .load_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await?;
        self.require_live_storage_ready(
            &playground.tenant_id,
            &playground.storage_volume_id,
            "Pre-commit start",
        )
        .await?;
        self.validate_precommit_source(&playground, &source_index_version)
            .await?;
        self.repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let job_id = deterministic_precommit_job_id(&tenant_id, &precommit_id, 1)?;
        let outcome = precommits
            .start(DomainPreCommitStartRequest {
                tenant_id,
                project_id,
                artifact_id,
                playground_id,
                precommit_id,
                precommit_request_id,
                source_index_version,
                data_layout,
                // A Playground is an independent line of development. Freeze its own Head as
                // the Commit parent; the Artifact Head is only the convenient current pointer.
                frozen_head_commit_id: playground.head_commit_id.map(Into::into),
                job_id,
                created_at_unix_ms: now,
            })
            .await
            .map_err(precommit_mutation_error)?;
        coordinator
            .ensure_precommit_job(&outcome.precommit)
            .await
            .map_err(map_central_error)?;
        let add_task = self
            .begin_child_operation_task(
                &task,
                TaskKind::AddScan,
                TaskScope {
                    tenant_id: outcome.precommit.tenant_id.clone(),
                    project_id: Some(outcome.precommit.project_id.clone()),
                    artifact_id: Some(outcome.precommit.artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(
                        &outcome.precommit.artifact_id,
                    )),
                    commit_id: None,
                    playground_id: Some(outcome.precommit.playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: Some(playground.storage_volume_id.clone()),
                },
                RequestId::new(format!("{}-add", outcome.precommit.precommit_request_id))
                    .map_err(|error| invalid_request(format!("task request_id: {error}")))?,
                &task_request,
                identity,
                Some("control_job"),
                Some(outcome.precommit.job_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &add_task,
            TaskResourceKind::ControlJob,
            outcome.precommit.job_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let _ = self
            .transition_operation_task(
                add_task,
                TaskState::Running,
                identity,
                Some("add scan scheduled".to_owned()),
            )
            .await?;
        task = self
            .transition_operation_task(
                task,
                TaskState::Running,
                identity,
                Some("pre-commit scan scheduled".to_owned()),
            )
            .await?;
        Ok(StartPreCommitResponse {
            precommit: precommit_view(&outcome.precommit),
            playground: self.playground_view(&playground).await?,
            replayed: outcome.replayed,
            task,
        })
    }

    pub async fn query_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPreCommitRequest,
    ) -> Result<QueryPreCommitResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundRead, &tenant_id)
            .await?;
        let precommit_id = PreCommitId::new(request.precommit_id)
            .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
        let precommits = self.precommit_repository()?;
        let stored = precommits
            .get(&PreCommitKey::new(tenant_id, precommit_id))
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        self.load_precommit_playground(identity, &stored, Permission::PlaygroundRead)
            .await?;
        let synchronized = match &self.coordinator {
            Some(coordinator) => coordinator
                .synchronize_precommit(&stored)
                .await
                .map_err(map_central_error)?,
            None => stored,
        };
        Ok(QueryPreCommitResponse {
            precommit: precommit_view(&synchronized),
        })
    }

    pub async fn restart_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: RestartPreCommitRequest,
    ) -> Result<RestartPreCommitResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        self.require_tenant(identity, Permission::CreateAddJob, &tenant_id)
            .await?;
        let precommit_id = PreCommitId::new(request.precommit_id)
            .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
        self.require_storage_availability_configured()?;
        let restart_request_id = RequestId::new(request.restart_request_id.clone())
            .map_err(|error| invalid_request(format!("restart_request_id: {error}")))?;
        let source_index_version = parse_index_version(request.expected_index_version)?;
        let key = PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
        let (precommits, coordinator) = self.precommit_execution()?;
        let stored_for_task = precommits
            .get(&key)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::PrecommitCheck,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(stored_for_task.project_id.clone()),
                    artifact_id: Some(stored_for_task.artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(
                        &stored_for_task.artifact_id,
                    )),
                    commit_id: None,
                    playground_id: Some(stored_for_task.playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: None,
                },
                restart_request_id.clone(),
                &task_request,
                identity,
                Some("precommit"),
                Some(precommit_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Precommit,
            precommit_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        if let Some(replayed) = precommits
            .find_restart_result(&tenant_id, &restart_request_id)
            .await
            .map_err(map_central_error)?
        {
            if replayed.precommit_id != precommit_id
                || !same_index_version(&replayed.source_index_version, &source_index_version)
            {
                return Err(precommit_conflict(
                    "restart_request_id is already bound to another request",
                ));
            }
            let playground = self
                .load_precommit_playground(identity, &replayed, Permission::PlaygroundRead)
                .await?;
            let task = match replayed.state {
                PreCommitState::Running => {
                    self.transition_operation_task(
                        task,
                        TaskState::Running,
                        identity,
                        Some("Pre-commit restart scheduled".to_owned()),
                    )
                    .await?
                }
                PreCommitState::Ready | PreCommitState::Committed => {
                    self.complete_operation_task(task, identity).await?
                }
                PreCommitState::Abnormal => {
                    self.transition_operation_task(
                        task,
                        TaskState::Failed,
                        identity,
                        Some("Pre-commit restart failed".to_owned()),
                    )
                    .await?
                }
                PreCommitState::Cancelled => {
                    self.transition_operation_task(
                        task,
                        TaskState::Cancelled,
                        identity,
                        Some("Pre-commit restart cancelled".to_owned()),
                    )
                    .await?
                }
            };
            return Ok(RestartPreCommitResponse {
                precommit: precommit_view(&replayed),
                playground: self.playground_view(&playground).await?,
                replayed: true,
                task,
            });
        }
        let stored = precommits
            .get(&key)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        let playground = self
            .load_precommit_playground(identity, &stored, Permission::PlaygroundCreate)
            .await?;
        self.require_live_storage_ready(
            &playground.tenant_id,
            &playground.storage_volume_id,
            "Pre-commit restart",
        )
        .await?;
        if matches!(
            stored.state,
            PreCommitState::Abnormal | PreCommitState::Cancelled
        ) {
            self.validate_precommit_source(&playground, &source_index_version)
                .await?;
        }
        self.repository
            .get_artifact(&stored.tenant_id, &stored.project_id, &stored.artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let next_attempt = stored
            .attempt
            .checked_add(1)
            .ok_or_else(|| precommit_conflict("Pre-commit attempt is exhausted"))?;
        let restart = DomainPreCommitRestartRequest {
            key,
            restart_request_id,
            source_index_version,
            frozen_head_commit_id: playground.head_commit_id.map(Into::into),
            job_id: deterministic_precommit_job_id(&tenant_id, &precommit_id, next_attempt)?,
            restarted_at_unix_ms: self.clock.now(),
        };
        let outcome = precommits
            .restart(restart)
            .await
            .map_err(precommit_mutation_error)?;
        if outcome.precommit.state == PreCommitState::Running {
            coordinator
                .ensure_precommit_job(&outcome.precommit)
                .await
                .map_err(map_central_error)?;
        }
        Ok(RestartPreCommitResponse {
            precommit: precommit_view(&outcome.precommit),
            playground: self.playground_view(&playground).await?,
            replayed: outcome.replayed || task_replayed,
            task: self
                .transition_operation_task(
                    task,
                    TaskState::Running,
                    identity,
                    Some("Pre-commit restart scheduled".to_owned()),
                )
                .await?,
        })
    }

    pub async fn cancel_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: CancelPreCommitRequest,
    ) -> Result<CancelPreCommitResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        let precommit_id = PreCommitId::new(request.precommit_id)
            .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
        let cancel_request_id = RequestId::new(request.cancel_request_id.clone())
            .map_err(|error| invalid_request(format!("cancel_request_id: {error}")))?;
        let precommits = self.precommit_repository()?;
        let key = PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
        let stored = precommits
            .get(&key)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        let playground = self
            .load_precommit_playground(identity, &stored, Permission::PlaygroundCreate)
            .await?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskKind::PrecommitCheck,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(stored.project_id.clone()),
                    artifact_id: Some(stored.artifact_id.clone()),
                    object_namespace_id: Some(ObjectNamespaceId::from_artifact(
                        &stored.artifact_id,
                    )),
                    commit_id: None,
                    playground_id: Some(stored.playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: Some(playground.storage_volume_id.clone()),
                },
                cancel_request_id.clone(),
                &task_request,
                identity,
                Some("precommit"),
                Some(precommit_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Precommit,
            precommit_id.to_string(),
            TaskResourceRole::Primary,
        )
        .await?;
        let outcome = precommits
            .cancel(DomainPreCommitCancelRequest {
                key,
                cancel_request_id,
                cancelled_at_unix_ms: self.clock.now(),
            })
            .await
            .map_err(precommit_mutation_error)?;
        Ok(CancelPreCommitResponse {
            precommit: precommit_view(&outcome.precommit),
            playground: self.playground_view(&playground).await?,
            replayed: outcome.replayed || task_replayed,
            task: self.complete_operation_task(task, identity).await?,
        })
    }

    pub async fn commit_playground(
        &self,
        identity: &AuthenticatedIdentity,
        request: CommitPlaygroundRequest,
    ) -> Result<CommitPlaygroundResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id.clone())?;
        let project_id = parse_project_id(request.project_id.clone())?;
        let artifact_id = parse_artifact_id(request.artifact_id.clone())?;
        let playground_id = parse_playground_id(request.playground_id.clone())?;
        let precommit_id = PreCommitId::new(request.precommit_id.clone())
            .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
        let commit_request_id = RequestId::new(request.commit_request_id.clone())
            .map_err(|error| invalid_request(format!("commit_request_id: {error}")))?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        let (task, _task_replayed) = self
            .begin_operation_task(
                TaskKind::CommitCreate,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: Some(project_id),
                    artifact_id: Some(artifact_id),
                    object_namespace_id: None,
                    commit_id: None,
                    playground_id: Some(playground_id),
                    snapshot_id: None,
                    storage_volume_id: None,
                },
                commit_request_id,
                &task_request,
                identity,
                Some("commit"),
                Some(precommit_id.as_str()),
            )
            .await?;
        self.link_operation_resource(
            &task,
            TaskResourceKind::Precommit,
            precommit_id.to_string(),
            TaskResourceRole::Related,
        )
        .await?;
        let service = self.workspace_commits.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "workspace_commit_unavailable",
                "WORKSPACE_COMMIT_UNAVAILABLE",
                "Workspace Commit authority is unavailable",
                true,
            )
        })?;
        let result = service.commit_playground(identity, request).await?;
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CommitPlaygroundResponse {
            commit: CommitNodeView {
                commit_id: result.commit.commit_id.to_string(),
                parent_commit_id: result.commit.parent_commit_id.map(|id| id.to_string()),
                message: result.commit.message,
                description: result.commit.description,
                tag_names: result.commit.tag_names,
                created_at_unix_ms: result.commit.created_at_unix_ms.to_string(),
                data_layout: match result.commit.data_layout {
                    CommitDataLayout::FastCdc => crate::dto::DataLayout::FastCdc,
                    CommitDataLayout::WholeFile => crate::dto::DataLayout::WholeFile,
                },
            },
            playground: self.playground_view(&result.playground).await?,
            consumed_precommit: precommit_view(&result.consumed_precommit),
            replayed: result.replayed,
            task,
        })
    }

    /// Lists the logical contents of the authoritative Playground Index. No Agent or mount
    /// locator participates in this read path.
    pub async fn query_playground_file_list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundFileListRequest,
    ) -> Result<QueryPlaygroundFileListResponse, Error> {
        let path_prefix = request
            .path_prefix
            .map(|value| parse_logical_path("path_prefix", value))
            .transpose()?;
        let format = request.format.map(validate_file_format).transpose()?;
        let page_size = page_size(request.page_size)?;
        let (_, index) = self
            .load_browse_index(
                identity,
                request.tenant_id,
                request.project_id,
                request.artifact_id,
                request.playground_id,
            )
            .await?;
        let scope = BrowseCursorScope {
            kind: "files",
            source_id: None,
            baseline_commit_id: None,
            index_revision: index.version.revision.to_string(),
            index_digest: index.version.digest.to_string(),
            path_prefix: path_prefix.as_ref().map(ToString::to_string),
            format: format.clone(),
            change_type: None,
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_browse_cursor(cursor, &scope))
            .transpose()?;
        let mut entries = logical_entries(&index.records, path_prefix.as_ref(), format.as_deref());
        if let Some(after) = after {
            entries.retain(|entry| entry.path > after);
        }
        let (items, next_cursor) = take_browse_page(entries, page_size, &scope)?;
        Ok(QueryPlaygroundFileListResponse {
            index_version: index_version_body(&index.version),
            items,
            next_cursor,
        })
    }

    /// Returns the public, logical metadata available in the central Index for one file.
    pub async fn query_playground_file_metadata(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundFileMetadataRequest,
    ) -> Result<QueryPlaygroundFileMetadataResponse, Error> {
        let path = parse_logical_path("path", request.path)?;
        let (_, index) = self
            .load_browse_index(
                identity,
                request.tenant_id,
                request.project_id,
                request.artifact_id,
                request.playground_id,
            )
            .await?;
        let record = index
            .records
            .iter()
            .find(|record| record.path == path)
            .ok_or_else(|| resource_not_found("playground file"))?;
        let format = file_format(record.path.as_str());
        Ok(QueryPlaygroundFileMetadataResponse {
            index_version: index_version_body(&index.version),
            metadata: FileMetadataView {
                path: record.path.to_string(),
                size_bytes: record.total_size.to_string(),
                media_type: media_type(&format).map(str::to_owned),
                format,
                row_count: None,
            },
        })
    }

    /// Computes a minimal Dataset Profile from public Index attributes. Rich schema and quality
    /// fields remain absent until an Agent publishes a dedicated, validated profile batch.
    pub async fn query_playground_dataset_profile(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundDatasetProfileRequest,
    ) -> Result<QueryPlaygroundDatasetProfileResponse, Error> {
        let (_, index) = self
            .load_browse_index(
                identity,
                request.tenant_id,
                request.project_id,
                request.artifact_id,
                request.playground_id,
            )
            .await?;
        let logical_size_bytes = index.records.iter().try_fold(0_u64, |total, record| {
            total.checked_add(record.total_size).ok_or_else(|| {
                application_error(
                    ErrorCategory::Internal,
                    "profile_size_overflow",
                    "PROFILE_SIZE_OVERFLOW",
                    "the logical Dataset size cannot be represented",
                    false,
                )
            })
        })?;
        let format_count = index
            .records
            .iter()
            .map(|record| file_format(record.path.as_str()))
            .collect::<BTreeSet<_>>()
            .len();
        Ok(QueryPlaygroundDatasetProfileResponse {
            index_version: index_version_body(&index.version),
            profile: DatasetProfileView {
                state: "ready".to_owned(),
                summary: Some(DatasetProfileSummary {
                    format_count: u32::try_from(format_count).unwrap_or(u32::MAX),
                    logical_file_count: index.records.len().to_string(),
                    logical_size_bytes: logical_size_bytes.to_string(),
                    row_count: None,
                    field_count: None,
                }),
            },
        })
    }

    /// Lists the current workspace delta against its Head Commit, or a frozen Pre-commit delta
    /// against the Head captured by that exact attempt.
    pub async fn query_playground_change_list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundChangeListRequest,
    ) -> Result<QueryPlaygroundChangeListResponse, Error> {
        let QueryPlaygroundChangeListRequest {
            tenant_id,
            project_id,
            artifact_id,
            playground_id,
            precommit_id,
            change_type,
            path_prefix,
            cursor,
            page_size: requested_page_size,
        } = request;
        let change_type = change_type.map(validate_change_type).transpose()?;
        let path_prefix = path_prefix
            .map(|value| parse_logical_path("path_prefix", value))
            .transpose()?;
        let page_size = page_size(requested_page_size)?;

        if let Some(precommit_id) = precommit_id {
            let tenant_id = parse_tenant(tenant_id)?;
            if !self
                .policy
                .is_allowed(identity.principal(), Permission::PlaygroundRead, &tenant_id)
            {
                return Err(resource_not_found("precommit"));
            }
            let project_id = parse_project_id(project_id)?;
            let artifact_id = parse_artifact_id(artifact_id)?;
            let playground_id = parse_playground_id(playground_id)?;
            let precommit_id = PreCommitId::new(precommit_id)
                .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
            let precommits = self.precommit_repository()?;
            let stored = precommits
                .get(&PreCommitKey::new(tenant_id, precommit_id.clone()))
                .await
                .map_err(map_central_error)?
                .filter(|record| {
                    record.project_id == project_id
                        && record.artifact_id == artifact_id
                        && record.playground_id == playground_id
                })
                .ok_or_else(|| resource_not_found("precommit"))?;
            let synchronized = match &self.coordinator {
                Some(coordinator) => coordinator
                    .synchronize_precommit(&stored)
                    .await
                    .map_err(map_central_error)?,
                None => stored,
            };
            let version = synchronized
                .candidate_index_version
                .as_ref()
                .ok_or_else(|| {
                    catalog_conflict(
                        "precommit_not_ready",
                        "PRECOMMIT_NOT_READY",
                        "the Pre-commit has no frozen candidate yet",
                    )
                })?;
            let records = synchronized.candidate_records.as_deref().ok_or_else(|| {
                catalog_conflict(
                    "precommit_candidate_unavailable",
                    "PRECOMMIT_CANDIDATE_UNAVAILABLE",
                    "the Pre-commit candidate snapshot is unavailable",
                )
            })?;
            let baseline_commit_id = synchronized.frozen_head_commit_id;
            let base_records = self
                .load_commit_records(
                    &synchronized.tenant_id,
                    &synchronized.project_id,
                    &synchronized.artifact_id,
                    baseline_commit_id,
                )
                .await?;
            return build_change_list_response(
                "precommit",
                Some(precommit_id.to_string()),
                baseline_commit_id,
                version,
                &base_records,
                records,
                change_type,
                path_prefix,
                cursor.as_deref(),
                page_size,
            );
        }

        let (playground, index) = self
            .load_browse_index(identity, tenant_id, project_id, artifact_id, playground_id)
            .await?;
        let baseline_commit_id = playground.head_commit_id.map(Into::into);
        let base_records = self
            .load_commit_records(
                &playground.tenant_id,
                &playground.project_id,
                &playground.artifact_id,
                baseline_commit_id,
            )
            .await?;
        build_change_list_response(
            "workspace",
            None,
            baseline_commit_id,
            &index.version,
            &base_records,
            &index.records,
            change_type,
            path_prefix,
            cursor.as_deref(),
            page_size,
        )
    }

    async fn load_browse_index(
        &self,
        identity: &AuthenticatedIdentity,
        tenant_id: String,
        project_id: String,
        artifact_id: String,
        playground_id: String,
    ) -> Result<(PlaygroundRecord, crate::PublishedIndex), Error> {
        let tenant_id = parse_tenant(tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::PlaygroundRead, &tenant_id)
        {
            return Err(resource_not_found("playground"));
        }
        let project_id = parse_project_id(project_id)?;
        let artifact_id = parse_artifact_id(artifact_id)?;
        let playground_id = parse_playground_id(playground_id)?;
        let playground = self
            .repository
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("playground"))?;
        require_active_for_read(&playground.lifecycle, "playground")?;
        if playground.state != PlaygroundState::Ready {
            return Err(catalog_conflict(
                "playground_not_ready",
                "PLAYGROUND_NOT_READY",
                "the Playground is not ready for browsing",
            ));
        }
        let index = self
            .indexes
            .published_index(&IndexKey {
                tenant_id,
                project_id,
                artifact_id,
                playground_id,
            })
            .await
            .map_err(map_central_error)?;
        Ok((playground, index))
    }

    fn precommit_repository(&self) -> Result<Arc<dyn PreCommitRepository>, Error> {
        self.precommits.clone().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "precommit_unavailable",
                "PRECOMMIT_UNAVAILABLE",
                "Pre-commit authority is not configured",
                true,
            )
        })
    }

    async fn load_published_commit(
        &self,
        artifact: &ArtifactRecord,
        commit_id: CommitId,
    ) -> Result<CommitRecord, Error> {
        let precommits = self.precommit_repository()?;
        let published = self
            .load_published_commit_graph(artifact, precommits.as_ref())
            .await?;
        published
            .into_iter()
            .find(|commit| commit.commit_id == commit_id)
            .ok_or_else(|| resource_not_found("commit"))
    }

    async fn load_commit_records(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        commit_id: Option<CommitId>,
    ) -> Result<Vec<FileRecord>, Error> {
        let Some(commit_id) = commit_id else {
            return Ok(Vec::new());
        };
        self.precommit_repository()?
            .get_commit(tenant_id, project_id, artifact_id, commit_id)
            .await
            .map_err(map_central_error)?
            .map(|commit| commit.records)
            .ok_or_else(|| {
                catalog_conflict(
                    "workspace_base_diff_unavailable",
                    "WORKSPACE_BASE_DIFF_UNAVAILABLE",
                    "the Head Commit Index is unavailable for this Playground",
                )
            })
    }

    fn precommit_execution(
        &self,
    ) -> Result<(Arc<dyn PreCommitRepository>, Arc<super::JobCoordinator>), Error> {
        let precommits = self.precommit_repository()?;
        let coordinator = self.coordinator.clone().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "precommit_execution_unavailable",
                "PRECOMMIT_EXECUTION_UNAVAILABLE",
                "Pre-commit execution is not configured",
                true,
            )
        })?;
        Ok((precommits, coordinator))
    }

    async fn load_playground(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &PlaygroundId,
    ) -> Result<PlaygroundRecord, Error> {
        let playground = self
            .repository
            .get_playground(tenant_id, project_id, artifact_id, playground_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("playground"))?;
        require_active_for_mutation(&playground.lifecycle, "Playground")?;
        Ok(playground)
    }

    async fn load_precommit_playground(
        &self,
        identity: &AuthenticatedIdentity,
        precommit: &PreCommitRecord,
        permission: Permission,
    ) -> Result<PlaygroundRecord, Error> {
        if !self
            .policy
            .is_allowed(identity.principal(), permission, &precommit.tenant_id)
        {
            return Err(resource_not_found("precommit"));
        }
        self.load_playground(
            &precommit.tenant_id,
            &precommit.project_id,
            &precommit.artifact_id,
            &precommit.playground_id,
        )
        .await
        .map_err(|_| resource_not_found("precommit"))
    }

    async fn validate_precommit_source(
        &self,
        playground: &PlaygroundRecord,
        expected: &WireIndexVersion,
    ) -> Result<(), Error> {
        if playground.state != PlaygroundState::Ready {
            return Err(catalog_conflict(
                "playground_not_ready",
                "PLAYGROUND_NOT_READY",
                "only a Ready Playground can start Pre-commit",
            ));
        }
        let current = self
            .indexes
            .current_version(&IndexKey {
                tenant_id: playground.tenant_id.clone(),
                project_id: playground.project_id.clone(),
                artifact_id: playground.artifact_id.clone(),
                playground_id: playground.playground_id.clone(),
            })
            .await
            .map_err(map_central_error)?;
        if !same_index_version(&current, expected) {
            return Err(catalog_conflict(
                "index_version_conflict",
                "INDEX_VERSION_CONFLICT",
                "expected_index_version no longer matches the Playground",
            ));
        }
        Ok(())
    }

    pub(crate) async fn require_tenant(
        &self,
        identity: &AuthenticatedIdentity,
        permission: Permission,
        tenant_id: &TenantId,
    ) -> Result<(), Error> {
        if !self
            .policy
            .is_allowed(identity.principal(), permission, tenant_id)
        {
            return Err(resource_not_found("tenant"));
        }
        self.repository
            .get_tenant(tenant_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("tenant"))?;
        Ok(())
    }

    async fn require_live_storage_ready(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
        operation: &str,
    ) -> Result<(), Error> {
        let provider = self.require_storage_availability_configured()?;
        let state = provider
            .current_volume_state(tenant_id, storage_volume_id)
            .await
            .map_err(map_central_error)?;
        if state == crate::DerivedVolumeState::Ready {
            tracing::debug!(%operation, %tenant_id, %storage_volume_id, "live StorageVolume gate passed");
            return Ok(());
        }
        let message = match state {
            crate::DerivedVolumeState::Degraded => {
                "the StorageVolume is degraded and cannot execute this operation"
            }
            crate::DerivedVolumeState::Unavailable => "the StorageVolume is currently unreachable",
            crate::DerivedVolumeState::Ready => unreachable!(),
        };
        Err(catalog_conflict(
            "storage_volume_unavailable",
            "STORAGE_VOLUME_UNAVAILABLE",
            message,
        ))
    }

    fn require_storage_availability_configured(
        &self,
    ) -> Result<&Arc<dyn StorageAvailabilityProvider>, Error> {
        self.storage_availability.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "storage_availability_unavailable",
                "STORAGE_AVAILABILITY_UNAVAILABLE",
                "live StorageVolume availability is not configured",
                true,
            )
        })
    }

    /// Projects the same live Agent-derived state used by Playground views onto the public
    /// StorageVolume view. The catalog row is retained as the fallback for compositions that do
    /// not have an Agent registry (for example, metadata-only tests and bootstrap tooling).
    async fn storage_volume_view_with_live_state(
        &self,
        record: &StorageVolumeRecord,
    ) -> Result<StorageVolumeView, Error> {
        let mut view = storage_volume_view(record);
        if let Some(provider) = &self.storage_availability {
            view.state = match provider
                .current_volume_state(&record.tenant_id, &record.storage_volume_id)
                .await
                .map_err(map_central_error)?
            {
                crate::DerivedVolumeState::Ready => "ready",
                crate::DerivedVolumeState::Degraded => "degraded",
                crate::DerivedVolumeState::Unavailable => "unavailable",
            }
            .to_owned();
        }
        Ok(view)
    }

    fn tenant_view(&self, identity: &AuthenticatedIdentity, record: &TenantRecord) -> TenantView {
        TenantView {
            tenant_id: record.tenant_id.to_string(),
            display_name: record.display_name.clone(),
            description: record.description.clone(),
            resource_version: record.resource_version.to_string(),
            created_at_unix_ms: record.created_at_unix_ms.to_string(),
            updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
            permissions: self
                .policy
                .permission_names(identity.principal(), &record.tenant_id),
        }
    }

    async fn playground_view(&self, record: &PlaygroundRecord) -> Result<PlaygroundView, Error> {
        let index_version = self
            .indexes
            .current_version(&IndexKey {
                tenant_id: record.tenant_id.clone(),
                project_id: record.project_id.clone(),
                artifact_id: record.artifact_id.clone(),
                playground_id: record.playground_id.clone(),
            })
            .await
            .map_err(map_central_error)?;
        let active = match &self.precommits {
            Some(precommits) => precommits
                .get_active(
                    &record.tenant_id,
                    &record.project_id,
                    &record.artifact_id,
                    &record.playground_id,
                )
                .await
                .map_err(map_central_error)?,
            None => None,
        };
        let storage_availability = match &self.storage_availability {
            Some(provider) => match provider
                .current_volume_state(&record.tenant_id, &record.storage_volume_id)
                .await
                .map_err(map_central_error)?
            {
                crate::DerivedVolumeState::Ready => "ready",
                crate::DerivedVolumeState::Degraded => "degraded",
                crate::DerivedVolumeState::Unavailable => "unavailable",
            },
            None => "unknown",
        };
        Ok(playground_view(
            record,
            &index_version,
            active.as_ref(),
            storage_availability,
        ))
    }

    async fn snapshot_view(&self, record: &SnapshotRecord) -> Result<SnapshotView, Error> {
        let commit = self
            .precommit_repository()?
            .get_commit(
                &record.tenant_id,
                &record.project_id,
                &record.artifact_id,
                CommitId::from_digest(record.commit_id),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        // A Snapshot is readable only through its single physical Delivery.  Keep the logical
        // Snapshot row queryable when the Delivery is pending or has been removed, but surface
        // that view as unavailable instead of allowing the row's historical `Ready` state to
        // imply that bytes can still be served.
        let delivery = self
            .repository
            .get_snapshot_delivery(&record.tenant_id, &record.delivery_id)
            .await
            .map_err(map_central_error)?;
        let delivery_ready = delivery.as_ref().is_some_and(|delivery| {
            delivery.tenant_id == record.tenant_id
                && delivery.snapshot_id == record.snapshot_id
                && delivery.commit_id == record.commit_id
                && delivery.storage_volume_id == record.storage_volume_id
                && delivery.mode == record.delivery_mode
                && delivery.state == SnapshotDeliveryState::Ready
        });
        let delivery_failed = delivery.as_ref().is_some_and(|delivery| {
            matches!(
                delivery.state,
                SnapshotDeliveryState::Failed | SnapshotDeliveryState::Deleted
            )
        });
        let logical_file_count = commit.records.len().to_string();
        let logical_size_bytes = commit
            .records
            .iter()
            .fold(0_u64, |total, file| total.saturating_add(file.total_size))
            .to_string();
        let (integrity_state, verified_at_unix_ms) = match record.state {
            SnapshotState::Creating => ("pending", None),
            SnapshotState::Ready if delivery_ready => {
                ("verified", Some(record.updated_at_unix_ms.to_string()))
            }
            SnapshotState::Ready if delivery_failed => ("failed", None),
            SnapshotState::Ready => ("pending", None),
            SnapshotState::Abnormal => ("failed", None),
        };
        let verified = record.state == SnapshotState::Ready && delivery_ready;
        // Snapshot lifecycle is logical and immutable; physical data health is resolved from
        // current namespace-scoped v2 object evidence so a lost Volume never turns into a logical
        // deletion, and a newly published object placement is visible without rewriting the
        // Snapshot. Legacy PlacementSets are intentionally not consulted by this read path.
        let data_health = if !delivery_ready {
            DataHealth::Unavailable
        } else if self.placement.is_some() {
            self.v2_commit_data_health(&record.tenant_id, &record.artifact_id, &commit)
                .await?
                .unwrap_or(DataHealth::Unavailable)
        } else if verified {
            DataHealth::Available
        } else {
            DataHealth::Unavailable
        };
        let data_health_name = format!("{data_health:?}").to_ascii_lowercase();
        let issue = if record.state == SnapshotState::Abnormal
            || delivery_failed
            || !delivery_ready
            || matches!(
                data_health,
                neoengram_domain::protocol::DataHealth::Unavailable
            ) {
            Some(ResourceIssueSummary {
                code: if record.state == SnapshotState::Abnormal {
                    "SNAPSHOT_UNAVAILABLE".to_owned()
                } else if delivery_failed {
                    "SNAPSHOT_DELIVERY_UNAVAILABLE".to_owned()
                } else if !delivery_ready {
                    "SNAPSHOT_DELIVERY_NOT_READY".to_owned()
                } else {
                    "DATA_UNAVAILABLE".to_owned()
                },
                message: if delivery_failed {
                    "the Snapshot's only Delivery is unavailable".to_owned()
                } else if !delivery_ready {
                    "the Snapshot's only Delivery is not ready".to_owned()
                } else {
                    "the immutable Snapshot data is unavailable".to_owned()
                },
                retryable: true,
                occurred_at_unix_ms: Some(record.updated_at_unix_ms.to_string()),
            })
        } else {
            None
        };
        Ok(SnapshotView {
            snapshot_id: record.snapshot_id.to_string(),
            tenant_id: record.tenant_id.to_string(),
            project_id: record.project_id.to_string(),
            artifact_id: record.artifact_id.to_string(),
            commit_id: record.commit_id.to_string(),
            delivery_id: record.delivery_id.to_string(),
            edge_cluster_id: record.edge_cluster_id.to_string(),
            storage_volume_id: record.storage_volume_id.to_string(),
            delivery_mode: match record.delivery_mode {
                SnapshotDeliveryMode::Fuse => DeliveryModeBody::Fuse,
                SnapshotDeliveryMode::Copy => DeliveryModeBody::Copy,
                SnapshotDeliveryMode::Hardlink => DeliveryModeBody::Hardlink,
            },
            data_layout: match commit.data_layout {
                CommitDataLayout::FastCdc => crate::dto::DataLayout::FastCdc,
                CommitDataLayout::WholeFile => crate::dto::DataLayout::WholeFile,
            },
            message: commit.message,
            tag_names: commit.tag_names,
            state: snapshot_state_name(record.state).to_owned(),
            data_health: data_health_name,
            issue,
            integrity: SnapshotIntegritySummary {
                state: integrity_state.to_owned(),
                files_verified: if verified {
                    logical_file_count.clone()
                } else {
                    "0".to_owned()
                },
                bytes_verified: if verified {
                    logical_size_bytes.clone()
                } else {
                    "0".to_owned()
                },
                verified_at_unix_ms,
            },
            resource_version: record.resource_version.to_string(),
            lifecycle: resource_lifecycle_view(&record.lifecycle),
            logical_file_count,
            logical_size_bytes,
            created_at_unix_ms: record.created_at_unix_ms.to_string(),
            updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
        })
    }
}

pub(crate) fn storage_volume_view(record: &StorageVolumeRecord) -> StorageVolumeView {
    StorageVolumeView {
        tenant_id: record.tenant_id.to_string(),
        storage_volume_id: record.storage_volume_id.to_string(),
        display_name: record.display_name.clone(),
        edge_cluster_id: record.edge_cluster_id.to_string(),
        region: record.region.clone(),
        backend_type: backend_name(record.backend_type).to_owned(),
        access_mode: access_mode_name(record.access_mode).to_owned(),
        allowed_delivery_modes: record
            .allowed_delivery_modes
            .iter()
            .map(|mode| match mode {
                SnapshotDeliveryMode::Fuse => "fuse",
                SnapshotDeliveryMode::Copy => "copy",
                SnapshotDeliveryMode::Hardlink => "hardlink",
            })
            .map(str::to_owned)
            .collect(),
        hardlink_policy: match record.hardlink_policy {
            HardlinkPolicy::Disabled => "disabled",
            HardlinkPolicy::SealedAcl => "sealed_acl",
            HardlinkPolicy::TrustedLocal => "trusted_local",
        }
        .to_owned(),
        max_whole_file_bytes: record.max_whole_file_bytes.to_string(),
        copy_reserve_bytes: record.copy_reserve_bytes.to_string(),
        pvc_reference: record.pvc_reference.as_ref().map(|reference| PvcReference {
            namespace: reference.namespace.clone(),
            claim_name: reference.claim_name.clone(),
        }),
        state: volume_state_name(record.state).to_owned(),
        resource_version: record.resource_version.to_string(),
        lifecycle: resource_lifecycle_view(&record.lifecycle),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn artifact_view(record: &ArtifactRecord) -> ArtifactView {
    ArtifactView {
        tenant_id: record.tenant_id.to_string(),
        project_id: record.project_id.to_string(),
        artifact_id: record.artifact_id.to_string(),
        display_name: record.display_name.clone(),
        description: record.description.clone(),
        initialization: match &record.initialization {
            CatalogArtifactInitialization::Empty => ArtifactInitializationBody::Empty,
            CatalogArtifactInitialization::Derived {
                source_project_id,
                source_artifact_id,
                source_commit_id,
            } => ArtifactInitializationBody::Derived {
                source_project_id: source_project_id.to_string(),
                source_artifact_id: source_artifact_id.to_string(),
                source_commit_id: source_commit_id.to_string(),
            },
        },
        head_commit_id: record.head_commit_id.map(|id| id.to_string()),
        resource_version: record.resource_version.to_string(),
        lifecycle: resource_lifecycle_view(&record.lifecycle),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn project_view(record: &ProjectRecord) -> ProjectView {
    ProjectView {
        tenant_id: record.tenant_id.to_string(),
        project_id: record.project_id.to_string(),
        display_name: record.display_name.clone(),
        description: record.description.clone(),
        resource_version: record.resource_version.to_string(),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn commit_node_view(record: &CommitRecord) -> CommitNodeView {
    CommitNodeView {
        commit_id: record.commit_id.to_string(),
        parent_commit_id: record.parent_commit_id.map(|id| id.to_string()),
        message: record.message.clone(),
        description: record.description.clone(),
        tag_names: record.tag_names.clone(),
        data_layout: match record.data_layout {
            CommitDataLayout::FastCdc => crate::dto::DataLayout::FastCdc,
            CommitDataLayout::WholeFile => crate::dto::DataLayout::WholeFile,
        },
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
    }
}

fn build_commit_diff_view(
    base: Option<&CommitRecord>,
    target: &CommitRecord,
) -> Result<CommitDiffView, Error> {
    let base_records = base.map_or(&[][..], |commit| commit.records.as_slice());
    let diff = diff_index_snapshots(base_records, &target.records).map_err(map_central_error)?;
    let summary = diff.summary;
    let changes = diff
        .changes
        .into_iter()
        .map(|change| CommitDiffEntry {
            change_type: index_change_kind_name(change.kind).to_owned(),
            path: change.path.to_string(),
            previous_path: change.previous_path.map(|path| path.to_string()),
            old_size_bytes: change.old_size.map(|size| size.to_string()),
            new_size_bytes: change.new_size.map(|size| size.to_string()),
        })
        .collect();
    Ok(CommitDiffView {
        base_commit: base.map(commit_node_view),
        target_commit: commit_node_view(target),
        summary: CommitDiffSummary {
            files_added: summary.files_added.to_string(),
            files_modified: summary.files_modified.to_string(),
            files_deleted: summary.files_deleted.to_string(),
            files_renamed: summary.files_renamed.to_string(),
            bytes_added: summary.bytes_added.to_string(),
            bytes_removed: summary.bytes_removed.to_string(),
        },
        changes,
    })
}

fn playground_view(
    record: &PlaygroundRecord,
    index_version: &WireIndexVersion,
    active_precommit: Option<&PreCommitRecord>,
    storage_availability: &str,
) -> PlaygroundView {
    PlaygroundView {
        tenant_id: record.tenant_id.to_string(),
        project_id: record.project_id.to_string(),
        artifact_id: record.artifact_id.to_string(),
        playground_id: record.playground_id.to_string(),
        storage_volume_id: record.storage_volume_id.to_string(),
        region: record.region.clone(),
        display_name: record.display_name.clone(),
        base_commit_id: record.base_commit_id.map(|id| id.to_string()),
        head_commit_id: record.head_commit_id.map(|id| id.to_string()),
        index_version: IndexVersionBody {
            revision: index_version.revision.to_string(),
            digest: index_version.digest.to_string(),
        },
        state: playground_state_name(record.state).to_owned(),
        storage_availability: storage_availability.to_owned(),
        active_precommit_id: active_precommit.map(|record| record.precommit_id.to_string()),
        issue: None,
        resource_version: record.resource_version.to_string(),
        lifecycle: resource_lifecycle_view(&record.lifecycle),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

pub(crate) fn precommit_view(record: &PreCommitRecord) -> PreCommitView {
    PreCommitView {
        tenant_id: record.tenant_id.to_string(),
        project_id: record.project_id.to_string(),
        artifact_id: record.artifact_id.to_string(),
        playground_id: record.playground_id.to_string(),
        precommit_id: record.precommit_id.to_string(),
        precommit_request_id: record.precommit_request_id.to_string(),
        attempt: record.attempt,
        state: precommit_state_name(record.state).to_owned(),
        phase: precommit_phase_name(record.phase).to_owned(),
        progress: PreCommitProgressView {
            percent: record.progress.percent,
            files_completed: record.progress.files_completed.to_string(),
            files_total: record.progress.files_total.map(|value| value.to_string()),
            bytes_completed: record.progress.bytes_completed.to_string(),
            bytes_total: record.progress.bytes_total.map(|value| value.to_string()),
        },
        checks: record
            .checks
            .iter()
            .map(|check| PreCommitCheckView {
                check_id: check.check_id.to_string(),
                status: match check.status {
                    crate::PreCommitCheckStatus::Pending => "pending",
                    crate::PreCommitCheckStatus::Passed => "passed",
                    crate::PreCommitCheckStatus::Warning => "warning",
                    crate::PreCommitCheckStatus::Failed => "failed",
                }
                .to_owned(),
                summary: check.summary.clone(),
            })
            .collect(),
        warnings: record.warnings.iter().map(precommit_notice_view).collect(),
        blockers: record.blockers.iter().map(precommit_notice_view).collect(),
        source_index_version: index_version_body(&record.source_index_version),
        data_layout: match record.data_layout {
            CommitDataLayout::FastCdc => crate::dto::DataLayout::FastCdc,
            CommitDataLayout::WholeFile => crate::dto::DataLayout::WholeFile,
        },
        candidate_index_version: record
            .candidate_index_version
            .as_ref()
            .map(index_version_body),
        diff_summary: record
            .diff_summary
            .as_ref()
            .map(|summary| PreCommitDiffSummaryView {
                files_added: summary.files_added.to_string(),
                files_modified: summary.files_modified.to_string(),
                files_deleted: summary.files_deleted.to_string(),
                files_renamed: summary.files_renamed.to_string(),
                bytes_added: summary.bytes_added.to_string(),
                bytes_removed: summary.bytes_removed.to_string(),
            }),
        issue: record.issue.as_ref().map(|issue| ResourceIssueSummary {
            code: issue.code.clone(),
            message: issue.message.clone(),
            retryable: record.state == PreCommitState::Abnormal,
            occurred_at_unix_ms: Some(record.updated_at_unix_ms.to_string()),
        }),
        committed_commit_id: record.committed_commit_id.map(|id| id.to_string()),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn precommit_notice_view(notice: &crate::PreCommitNotice) -> PreCommitNoticeView {
    PreCommitNoticeView {
        code: notice.code.clone(),
        message: notice.message.clone(),
        path: notice.path.clone(),
    }
}

const fn precommit_state_name(state: PreCommitState) -> &'static str {
    match state {
        PreCommitState::Running => "running",
        PreCommitState::Ready => "ready",
        PreCommitState::Abnormal => "abnormal",
        PreCommitState::Cancelled => "cancelled",
        PreCommitState::Committed => "committed",
    }
}

const fn precommit_phase_name(phase: PreCommitPhase) -> &'static str {
    match phase {
        PreCommitPhase::Queued => "queued",
        PreCommitPhase::Scanning => "scanning",
        PreCommitPhase::Hashing => "hashing",
        PreCommitPhase::Uploading => "uploading",
        PreCommitPhase::Validating => "validating",
        PreCommitPhase::Idle => "idle",
    }
}

fn parse_index_version(body: IndexVersionBody) -> Result<WireIndexVersion, Error> {
    let revision = parse_canonical_u64(&body.revision).map_err(|_| {
        invalid_request("expected_index_version.revision must be a canonical unsigned integer")
    })?;
    let digest = ContentDigest::from_str(&body.digest).map_err(|_| {
        invalid_request("expected_index_version.digest must be a BLAKE3 hex digest")
    })?;
    Ok(WireIndexVersion {
        revision: IndexRevision::new(revision),
        digest,
        extensions: Default::default(),
    })
}

fn deterministic_precommit_id(
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> Result<PreCommitId, Error> {
    let input = format!("{}\0{}", tenant_id, request_id);
    PreCommitId::new(format!(
        "precommit-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(|_| internal_catalog_error())
}

fn deterministic_snapshot_id(
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> Result<SnapshotId, Error> {
    let input = format!("{}\0{}", tenant_id, request_id);
    SnapshotId::new(format!(
        "snapshot-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(|_| internal_catalog_error())
}

fn deterministic_precommit_job_id(
    tenant_id: &TenantId,
    precommit_id: &PreCommitId,
    attempt: u32,
) -> Result<JobId, Error> {
    let input = format!("{}\0{}\0{}", tenant_id, precommit_id, attempt);
    JobId::new(format!(
        "precommit-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(|_| internal_catalog_error())
}

fn same_index_version(left: &WireIndexVersion, right: &WireIndexVersion) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn split_outcome<T>(outcome: CatalogInsertOutcome<T>) -> (T, bool) {
    match outcome {
        CatalogInsertOutcome::Inserted(record) => (record, false),
        CatalogInsertOutcome::Existing(record) => (record, true),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CursorScope {
    Tenant {
        visible: Option<Vec<String>>,
        query: Option<String>,
    },
    Project {
        tenant_id: String,
        query: Option<String>,
    },
    Volume {
        tenant_id: String,
        region: Option<String>,
        backend_type: Option<String>,
        query: Option<String>,
    },
    Artifact {
        tenant_id: String,
        project_id: Option<String>,
        query: Option<String>,
    },
    CommitGraph {
        tenant_id: String,
        project_id: String,
        artifact_id: String,
        graph_version: String,
        head_commit_id: Option<String>,
    },
    Playground {
        tenant_id: String,
        project_id: Option<String>,
        artifact_id: Option<String>,
        region: Option<String>,
        state: Option<String>,
        query: Option<String>,
    },
    Snapshot {
        tenant_id: String,
        project_id: Option<String>,
        artifact_id: Option<String>,
        commit_id: Option<String>,
        state: Option<String>,
    },
    S3AccessPoint {
        tenant_id: String,
    },
    S3Object {
        access_point_id: String,
        snapshot_id: String,
        index_digest: String,
        prefix: String,
        delimiter: String,
    },
    Deletion {
        tenant_id: String,
        states: Option<Vec<String>>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorDocument {
    version: u8,
    scope_digest: String,
    created_at_unix_ms: String,
    keys: Vec<String>,
}

fn encode_tenant_cursor(scope: &CursorScope, cursor: &TenantListCursor) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.tenant_id.to_string()],
    )
}

fn decode_tenant_cursor(encoded: &str, scope: &CursorScope) -> Result<TenantListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(TenantListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        tenant_id: TenantId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
    })
}

fn encode_project_cursor(scope: &CursorScope, cursor: &ProjectListCursor) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.project_id.to_string()],
    )
}

fn decode_project_cursor(encoded: &str, scope: &CursorScope) -> Result<ProjectListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(ProjectListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        project_id: ProjectId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
    })
}

fn encode_volume_cursor(
    scope: &CursorScope,
    cursor: &StorageVolumeListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.storage_volume_id.to_string()],
    )
}

fn decode_volume_cursor(
    encoded: &str,
    scope: &CursorScope,
) -> Result<StorageVolumeListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(StorageVolumeListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        storage_volume_id: StorageVolumeId::new(cursor.keys[0].clone())
            .map_err(|_| cursor_conflict())?,
    })
}

fn encode_artifact_cursor(
    scope: &CursorScope,
    cursor: &ArtifactListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![
            cursor.project_id.to_string(),
            cursor.artifact_id.to_string(),
        ],
    )
}

fn decode_artifact_cursor(encoded: &str, scope: &CursorScope) -> Result<ArtifactListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 2)?;
    Ok(ArtifactListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        project_id: ProjectId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
        artifact_id: ArtifactId::new(cursor.keys[1].clone()).map_err(|_| cursor_conflict())?,
    })
}

#[derive(Debug, Clone, Copy)]
struct CommitGraphCursor {
    created_at_unix_ms: UnixMillis,
    commit_id: CommitId,
}

fn encode_commit_cursor(scope: &CursorScope, cursor: &CommitGraphCursor) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.commit_id.to_string()],
    )
}

fn decode_commit_cursor(encoded: &str, scope: &CursorScope) -> Result<CommitGraphCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(CommitGraphCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        commit_id: CommitId::from_str(&cursor.keys[0]).map_err(|_| cursor_conflict())?,
    })
}

fn encode_playground_cursor(
    scope: &CursorScope,
    cursor: &PlaygroundListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![
            cursor.project_id.to_string(),
            cursor.artifact_id.to_string(),
            cursor.playground_id.to_string(),
        ],
    )
}

fn decode_playground_cursor(
    encoded: &str,
    scope: &CursorScope,
) -> Result<PlaygroundListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 3)?;
    Ok(PlaygroundListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        project_id: ProjectId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
        artifact_id: ArtifactId::new(cursor.keys[1].clone()).map_err(|_| cursor_conflict())?,
        playground_id: PlaygroundId::new(cursor.keys[2].clone()).map_err(|_| cursor_conflict())?,
    })
}

fn encode_snapshot_cursor(
    scope: &CursorScope,
    cursor: &SnapshotListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.snapshot_id.to_string()],
    )
}

fn decode_snapshot_cursor(encoded: &str, scope: &CursorScope) -> Result<SnapshotListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(SnapshotListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        snapshot_id: SnapshotId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
    })
}

fn encode_deletion_cursor(
    scope: &CursorScope,
    cursor: &DeletionListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.deletion_id.to_string()],
    )
}

fn decode_deletion_cursor(encoded: &str, scope: &CursorScope) -> Result<DeletionListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(DeletionListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        deletion_id: DeletionId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
    })
}

fn encode_s3_access_point_cursor(
    scope: &str,
    cursor: &crate::S3AccessPointListCursor,
) -> Result<String, Error> {
    encode_cursor(
        &CursorScope::S3AccessPoint {
            tenant_id: scope.strip_prefix("s3ap:").unwrap_or(scope).to_owned(),
        },
        cursor.created_at_unix_ms,
        vec![cursor.access_point_id.to_string()],
    )
}

fn decode_s3_access_point_cursor(
    encoded: &str,
    scope: &str,
) -> Result<crate::S3AccessPointListCursor, Error> {
    let cursor = decode_cursor(
        encoded,
        &CursorScope::S3AccessPoint {
            tenant_id: scope.strip_prefix("s3ap:").unwrap_or(scope).to_owned(),
        },
        1,
    )?;
    Ok(crate::S3AccessPointListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        access_point_id: S3AccessPointId::new(cursor.keys[0].clone())
            .map_err(|_| cursor_conflict())?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct S3ObjectCursorScope {
    access_point_id: String,
    policy_generation: u64,
    snapshot_id: String,
    index_digest: String,
    prefix: String,
    delimiter: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct S3ObjectCursorDocument {
    version: u8,
    scope_digest: String,
    position: u64,
    last_key_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedS3ObjectCursor {
    position: usize,
    last_key_digest: String,
}

fn encode_s3_object_cursor(
    signing_key: &[u8; 32],
    scope: &S3ObjectCursorScope,
    position: usize,
    key: &str,
) -> Result<String, Error> {
    if position == 0 {
        return Err(internal_catalog_error());
    }
    let document = S3ObjectCursorDocument {
        version: 2,
        scope_digest: s3_object_cursor_digest(
            b"scope",
            &serde_json::to_vec(scope).map_err(|_| internal_catalog_error())?,
        ),
        position: u64::try_from(position).map_err(|_| internal_catalog_error())?,
        last_key_digest: s3_object_cursor_digest(b"last-key", key.as_bytes()),
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| internal_catalog_error())?;
    let mac = s3_cursor_mac(signing_key, &bytes);
    Ok(format!(
        "{CURSOR_PREFIX}{}.{}",
        URL_SAFE_NO_PAD.encode(bytes),
        URL_SAFE_NO_PAD.encode(mac)
    ))
}

fn decode_s3_object_cursor(
    signing_key: &[u8; 32],
    encoded: &str,
    expected: &S3ObjectCursorScope,
) -> Result<DecodedS3ObjectCursor, Error> {
    if encoded.len() > 2_048 {
        return Err(cursor_conflict());
    }
    let encoded = encoded
        .strip_prefix(CURSOR_PREFIX)
        .ok_or_else(cursor_conflict)?;
    let (payload, encoded_mac) = encoded.split_once('.').ok_or_else(cursor_conflict)?;
    if encoded_mac.contains('.') {
        return Err(cursor_conflict());
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| cursor_conflict())?;
    let mac = URL_SAFE_NO_PAD
        .decode(encoded_mac)
        .map_err(|_| cursor_conflict())?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    let authenticated = s3_cursor_authenticated_bytes(&bytes);
    hmac::verify(&key, &authenticated, &mac).map_err(|_| cursor_conflict())?;
    let document: S3ObjectCursorDocument =
        serde_json::from_slice(&bytes).map_err(|_| cursor_conflict())?;
    let expected_scope = serde_json::to_vec(expected).map_err(|_| internal_catalog_error())?;
    if document.version != 2
        || document.scope_digest != s3_object_cursor_digest(b"scope", &expected_scope)
        || document.last_key_digest.len() != 64
        || !document
            .last_key_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(cursor_conflict());
    }
    let position = usize::try_from(document.position).map_err(|_| cursor_conflict())?;
    if position == 0 {
        return Err(cursor_conflict());
    }
    Ok(DecodedS3ObjectCursor {
        position,
        last_key_digest: document.last_key_digest,
    })
}

fn s3_object_cursor_digest(domain: &[u8], value: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"neoengram-s3-object-cursor-v2\0");
    hasher.update(domain);
    hasher.update(b"\0");
    hasher.update(value);
    hasher.finalize().to_hex().to_string()
}

fn s3_object_cursor_start<T>(
    entries: &[(String, T)],
    cursor: &DecodedS3ObjectCursor,
) -> Result<usize, Error> {
    let key = cursor
        .position
        .checked_sub(1)
        .and_then(|index| entries.get(index))
        .map(|(key, _)| key.as_str())
        .ok_or_else(cursor_conflict)?;
    if s3_object_cursor_digest(b"last-key", key.as_bytes()) != cursor.last_key_digest {
        return Err(cursor_conflict());
    }
    Ok(cursor.position)
}

fn s3_cursor_mac(signing_key: &[u8; 32], payload: &[u8]) -> [u8; 32] {
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    hmac::sign(&key, &s3_cursor_authenticated_bytes(payload))
        .as_ref()
        .try_into()
        .expect("HMAC-SHA256 is 32 bytes")
}

fn s3_cursor_authenticated_bytes(payload: &[u8]) -> Vec<u8> {
    let mut authenticated = b"neoengram-s3-continuation-token-v1\0".to_vec();
    authenticated.extend_from_slice(payload);
    authenticated
}

struct DecodedCursor {
    created_at_unix_ms: UnixMillis,
    keys: Vec<String>,
}

fn encode_cursor(
    scope: &CursorScope,
    created_at_unix_ms: UnixMillis,
    keys: Vec<String>,
) -> Result<String, Error> {
    let document = CursorDocument {
        version: 1,
        scope_digest: scope_digest(scope)?,
        created_at_unix_ms: created_at_unix_ms.to_string(),
        keys,
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| internal_catalog_error())?;
    Ok(format!("{CURSOR_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn decode_cursor(
    encoded: &str,
    scope: &CursorScope,
    key_count: usize,
) -> Result<DecodedCursor, Error> {
    if encoded.len() > 2_048 {
        return Err(cursor_conflict());
    }
    let bytes = encoded
        .strip_prefix(CURSOR_PREFIX)
        .ok_or_else(cursor_conflict)
        .and_then(|value| {
            URL_SAFE_NO_PAD
                .decode(value.as_bytes())
                .map_err(|_| cursor_conflict())
        })?;
    let document: CursorDocument = serde_json::from_slice(&bytes).map_err(|_| cursor_conflict())?;
    if document.version != 1
        || document.scope_digest != scope_digest(scope)?
        || document.keys.len() != key_count
    {
        return Err(cursor_conflict());
    }
    let created_at_unix_ms = parse_canonical_u64(&document.created_at_unix_ms)
        .map(UnixMillis::new)
        .map_err(|_| cursor_conflict())?;
    Ok(DecodedCursor {
        created_at_unix_ms,
        keys: document.keys,
    })
}

fn scope_digest(scope: &CursorScope) -> Result<String, Error> {
    serde_json::to_vec(scope)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|_| internal_catalog_error())
}

fn parse_tenant(value: String) -> Result<TenantId, Error> {
    TenantId::new(value).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}

fn parse_request_id(value: String) -> Result<RequestId, Error> {
    RequestId::new(value).map_err(|error| invalid_request(format!("request_id: {error}")))
}

fn parse_content_digest(field: &'static str, value: &str) -> Result<ContentDigest, Error> {
    ContentDigest::from_str(value).map_err(|_| invalid_request(format!("{field} must be a digest")))
}

fn parse_lifecycle_resource_version(field: &'static str, value: &str) -> Result<u64, Error> {
    parse_canonical_u64(value)
        .map_err(|_| invalid_request(format!("{field} must be a canonical unsigned integer")))
}

fn parse_unix_millis(field: &'static str, value: &str) -> Result<UnixMillis, Error> {
    parse_canonical_u64(value)
        .map(UnixMillis::new)
        .map_err(|_| invalid_request(format!("{field} must be a canonical unsigned integer")))
}

fn parse_deletion_id(value: String) -> Result<DeletionId, Error> {
    DeletionId::new(value).map_err(|error| invalid_request(format!("deletion_id: {error}")))
}

fn parse_retention_hold_id(value: String) -> Result<RetentionHoldId, Error> {
    RetentionHoldId::new(value)
        .map_err(|error| invalid_request(format!("retention_hold_id: {error}")))
}

fn parse_resource_ref(value: ResourceRefBody) -> Result<ResourceRef, Error> {
    match value {
        ResourceRefBody::StorageVolume { storage_volume_id } => Ok(ResourceRef::StorageVolume {
            storage_volume_id: parse_volume_id(storage_volume_id)?,
        }),
        ResourceRefBody::Artifact {
            project_id,
            artifact_id,
        } => Ok(ResourceRef::Artifact {
            project_id: parse_project_id(project_id)?,
            artifact_id: parse_artifact_id(artifact_id)?,
        }),
        ResourceRefBody::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => Ok(ResourceRef::Playground {
            project_id: parse_project_id(project_id)?,
            artifact_id: parse_artifact_id(artifact_id)?,
            playground_id: parse_playground_id(playground_id)?,
        }),
        ResourceRefBody::Snapshot { snapshot_id } => Ok(ResourceRef::Snapshot {
            snapshot_id: parse_snapshot_id(snapshot_id)?,
        }),
    }
}

fn task_scope_for_resource(tenant_id: TenantId, resource: &ResourceRef) -> TaskScope {
    let mut scope = TaskScope::new(tenant_id);
    match resource {
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

fn resource_ref_body(value: &ResourceRef) -> ResourceRefBody {
    match value {
        ResourceRef::StorageVolume { storage_volume_id } => ResourceRefBody::StorageVolume {
            storage_volume_id: storage_volume_id.to_string(),
        },
        ResourceRef::Artifact {
            project_id,
            artifact_id,
        } => ResourceRefBody::Artifact {
            project_id: project_id.to_string(),
            artifact_id: artifact_id.to_string(),
        },
        ResourceRef::Playground {
            project_id,
            artifact_id,
            playground_id,
        } => ResourceRefBody::Playground {
            project_id: project_id.to_string(),
            artifact_id: artifact_id.to_string(),
            playground_id: playground_id.to_string(),
        },
        ResourceRef::Snapshot { snapshot_id } => ResourceRefBody::Snapshot {
            snapshot_id: snapshot_id.to_string(),
        },
    }
}

fn resource_lifecycle_view(lifecycle: &ResourceLifecycle) -> ResourceLifecycleView {
    ResourceLifecycleView {
        state: match lifecycle.state {
            ResourceLifecycleState::Active => "active",
            ResourceLifecycleState::PendingDelete => "pending_delete",
            ResourceLifecycleState::Deleting => "deleting",
            ResourceLifecycleState::Restoring => "restoring",
            ResourceLifecycleState::Deleted => "deleted",
        }
        .to_owned(),
        generation: lifecycle.generation.get().to_string(),
        active_deletion_id: lifecycle
            .active_deletion_id
            .as_ref()
            .map(ToString::to_string),
        delete_requested_at_unix_ms: lifecycle
            .delete_requested_at_unix_ms
            .map(|value| value.get().to_string()),
        purge_after_unix_ms: lifecycle
            .purge_after_unix_ms
            .map(|value| value.get().to_string()),
        deleted_at_unix_ms: lifecycle
            .deleted_at_unix_ms
            .map(|value| value.get().to_string()),
    }
}

fn require_active_for_read(
    lifecycle: &ResourceLifecycle,
    resource_name: &'static str,
) -> Result<(), Error> {
    if lifecycle.is_active() {
        Ok(())
    } else {
        Err(resource_not_found(resource_name))
    }
}

fn require_active_for_mutation(
    lifecycle: &ResourceLifecycle,
    _resource_name: &'static str,
) -> Result<(), Error> {
    if lifecycle.is_active() {
        Ok(())
    } else {
        Err(catalog_conflict(
            "resource_not_active",
            "RESOURCE_NOT_ACTIVE",
            "the resource is not active and cannot accept this operation",
        ))
    }
}

fn deletion_target_view(target: &neoengram_domain::protocol::DeletionTarget) -> DeletionTargetView {
    DeletionTargetView {
        resource: resource_ref_body(&target.resource),
        resource_version: target.resource_version.get().to_string(),
        lifecycle_generation: target.lifecycle_generation.get().to_string(),
        requires_agent_cleanup: target.requires_agent_cleanup,
    }
}

fn deletion_blocker_view(
    blocker: &neoengram_domain::protocol::DeletionBlocker,
) -> DeletionBlockerView {
    DeletionBlockerView {
        code: blocker.code.clone(),
        resource: blocker.resource.as_ref().map(resource_ref_body),
        message: blocker.message.clone(),
    }
}

fn deletion_impact_view(impact: &neoengram_domain::protocol::DeletionImpact) -> DeletionImpactView {
    DeletionImpactView {
        tenant_id: impact.tenant_id.to_string(),
        root: resource_ref_body(&impact.root),
        cascade: impact.cascade,
        confirm_managed_data_erase: impact.confirm_managed_data_erase,
        targets: impact.targets.iter().map(deletion_target_view).collect(),
        active_job_count: impact.active_job_count.get().to_string(),
        active_s3_credential_count: impact.active_s3_credential_count.get().to_string(),
        estimated_file_count: impact.estimated_file_count.get().to_string(),
        estimated_bytes: impact.estimated_bytes.get().to_string(),
        blockers: impact.blockers.iter().map(deletion_blocker_view).collect(),
        issued_at_unix_ms: impact.issued_at_unix_ms.get().to_string(),
        expires_at_unix_ms: impact.expires_at_unix_ms.get().to_string(),
    }
}

fn deletion_state_name(state: DeletionOperationState) -> &'static str {
    match state {
        DeletionOperationState::Requested => "requested",
        DeletionOperationState::Quiescing => "quiescing",
        DeletionOperationState::Quarantining => "quarantining",
        DeletionOperationState::Recoverable => "recoverable",
        DeletionOperationState::Restoring => "restoring",
        DeletionOperationState::Purging => "purging",
        DeletionOperationState::Finalizing => "finalizing",
        DeletionOperationState::Completed => "completed",
        DeletionOperationState::Blocked => "blocked",
        DeletionOperationState::Failed => "failed",
    }
}

fn parse_deletion_state(value: &str) -> Result<DeletionOperationState, Error> {
    match value {
        "requested" => Ok(DeletionOperationState::Requested),
        "quiescing" => Ok(DeletionOperationState::Quiescing),
        "quarantining" => Ok(DeletionOperationState::Quarantining),
        "recoverable" => Ok(DeletionOperationState::Recoverable),
        "restoring" => Ok(DeletionOperationState::Restoring),
        "purging" => Ok(DeletionOperationState::Purging),
        "finalizing" => Ok(DeletionOperationState::Finalizing),
        "completed" => Ok(DeletionOperationState::Completed),
        "blocked" => Ok(DeletionOperationState::Blocked),
        "failed" => Ok(DeletionOperationState::Failed),
        _ => Err(invalid_request("states contains an unknown deletion state")),
    }
}

fn deletion_operation_view(operation: &DeletionOperation) -> DeletionOperationView {
    DeletionOperationView {
        deletion_id: operation.deletion_id.to_string(),
        tenant_id: operation.tenant_id.to_string(),
        root: resource_ref_body(&operation.root),
        state: deletion_state_name(operation.state).to_owned(),
        resource_version: operation.resource_version.get().to_string(),
        targets: operation.targets.iter().map(deletion_target_view).collect(),
        request_id: operation.request_id.to_string(),
        request_digest: operation.request_digest.to_string(),
        impact_digest: operation.impact_digest.to_string(),
        cascade: operation.cascade,
        confirm_managed_data_erase: operation.confirm_managed_data_erase,
        purge_after_unix_ms: operation.purge_after_unix_ms.get().to_string(),
        created_at_unix_ms: operation.created_at_unix_ms.get().to_string(),
        updated_at_unix_ms: operation.updated_at_unix_ms.get().to_string(),
        completion: operation.completion.map(|completion| match completion {
            DeletionCompletion::Restored => "restored".to_owned(),
            DeletionCompletion::Purged => "purged".to_owned(),
        }),
        last_error: operation.last_error.clone(),
        resume_state: operation
            .resume_state
            .map(|state| deletion_state_name(state).to_owned()),
        retry_count: operation.retry_count.get().to_string(),
    }
}

fn retention_hold_view(hold: &RetentionHold) -> RetentionHoldView {
    RetentionHoldView {
        retention_hold_id: hold.retention_hold_id.to_string(),
        tenant_id: hold.tenant_id.to_string(),
        deletion_id: hold.deletion_id.to_string(),
        reason: hold.reason.clone(),
        state: match hold.state {
            RetentionHoldState::Active => "active",
            RetentionHoldState::Released => "released",
        }
        .to_owned(),
        expires_at_unix_ms: hold.expires_at_unix_ms.map(|value| value.get().to_string()),
        created_at_unix_ms: hold.created_at_unix_ms.get().to_string(),
        released_at_unix_ms: hold
            .released_at_unix_ms
            .map(|value| value.get().to_string()),
    }
}

fn lifecycle_request_digest<T: Serialize>(
    operation: &str,
    payload: &T,
) -> Result<ContentDigest, Error> {
    let value = serde_json::json!({ "version": 1, "operation": operation, "payload": payload });
    let bytes = serde_json_canonicalizer::to_vec(&value).map_err(|_| internal_catalog_error())?;
    Ok(ContentDigest::hash(bytes))
}

fn deletion_id_for_request(
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> Result<DeletionId, Error> {
    let digest = blake3::hash(format!("{tenant_id}\0{request_id}").as_bytes())
        .to_hex()
        .to_string();
    DeletionId::new(format!("delete-{}", &digest[..24])).map_err(|_| internal_catalog_error())
}

fn retention_hold_id_for_request(
    deletion_id: &DeletionId,
    request_id: &RequestId,
) -> Result<RetentionHoldId, Error> {
    let digest = blake3::hash(format!("{deletion_id}\0{request_id}").as_bytes())
        .to_hex()
        .to_string();
    RetentionHoldId::new(format!("hold-{}", &digest[..24])).map_err(|_| internal_catalog_error())
}

fn lifecycle_conflict(slug: &'static str, code: &'static str, message: &'static str) -> Error {
    catalog_conflict(slug, code, message)
}

fn lifecycle_request_id_reused() -> Error {
    catalog_conflict(
        "resource_lifecycle_request_id_reused",
        "RESOURCE_LIFECYCLE_REQUEST_ID_REUSED",
        "request_id is already bound to another lifecycle mutation",
    )
}

fn map_lifecycle_mutation_error(error: crate::CentralError) -> Error {
    if error.code() == crate::CentralErrorCode::InvalidState
        && error.message().contains("request identity")
    {
        lifecycle_request_id_reused()
    } else {
        map_central_error(error)
    }
}

fn parse_project_id(value: String) -> Result<ProjectId, Error> {
    ProjectId::new(value).map_err(|error| invalid_request(format!("project_id: {error}")))
}

fn parse_artifact_id(value: String) -> Result<ArtifactId, Error> {
    ArtifactId::new(value).map_err(|error| invalid_request(format!("artifact_id: {error}")))
}

fn parse_commit_id(value: String) -> Result<CommitId, Error> {
    value
        .parse::<CommitId>()
        .map_err(|error| invalid_request(format!("commit_id: {error}")))
}

fn parse_playground_id(value: String) -> Result<PlaygroundId, Error> {
    PlaygroundId::new(value).map_err(|error| invalid_request(format!("playground_id: {error}")))
}

fn parse_snapshot_id(value: String) -> Result<SnapshotId, Error> {
    SnapshotId::new(value).map_err(|error| invalid_request(format!("snapshot_id: {error}")))
}

fn parse_volume_id(value: String) -> Result<StorageVolumeId, Error> {
    StorageVolumeId::new(value)
        .map_err(|error| invalid_request(format!("storage_volume_id: {error}")))
}

fn parse_backend_type(value: &str) -> Result<StorageBackendType, Error> {
    match value {
        "pvc" => Ok(StorageBackendType::Pvc),
        "nfs" => Ok(StorageBackendType::Nfs),
        _ => Err(invalid_request("backend_type must be pvc or nfs")),
    }
}

fn parse_access_mode(value: &str) -> Result<StorageAccessMode, Error> {
    match value {
        "read_write_once" => Ok(StorageAccessMode::ReadWriteOnce),
        "read_write_many" => Ok(StorageAccessMode::ReadWriteMany),
        "read_only_many" => Ok(StorageAccessMode::ReadOnlyMany),
        _ => Err(invalid_request(
            "access_mode must be read_write_once, read_write_many, or read_only_many",
        )),
    }
}

fn parse_delivery_mode(value: &str) -> Result<SnapshotDeliveryMode, Error> {
    match value {
        "fuse" => Ok(SnapshotDeliveryMode::Fuse),
        "copy" => Ok(SnapshotDeliveryMode::Copy),
        "hardlink" => Ok(SnapshotDeliveryMode::Hardlink),
        _ => Err(invalid_request(
            "allowed_delivery_modes contains an unknown mode",
        )),
    }
}

fn parse_hardlink_policy_body(value: Option<String>) -> Result<HardlinkPolicy, Error> {
    match value.as_deref().unwrap_or("disabled") {
        "disabled" => Ok(HardlinkPolicy::Disabled),
        "sealed_acl" => Ok(HardlinkPolicy::SealedAcl),
        "trusted_local" => Ok(HardlinkPolicy::TrustedLocal),
        _ => Err(invalid_request(
            "hardlink_policy must be disabled, sealed_acl, or trusted_local",
        )),
    }
}

fn parse_playground_state(value: &str) -> Result<PlaygroundState, Error> {
    match value {
        "creating" => Ok(PlaygroundState::Creating),
        "ready" => Ok(PlaygroundState::Ready),
        "abnormal" => Ok(PlaygroundState::Abnormal),
        _ => Err(invalid_request(
            "state must be creating, ready, or abnormal",
        )),
    }
}

fn parse_snapshot_state(value: &str) -> Result<SnapshotState, Error> {
    match value {
        "creating" => Ok(SnapshotState::Creating),
        "ready" => Ok(SnapshotState::Ready),
        "abnormal" => Ok(SnapshotState::Abnormal),
        _ => Err(invalid_request(
            "state must be creating, ready, or abnormal",
        )),
    }
}

fn backend_name(value: StorageBackendType) -> &'static str {
    match value {
        StorageBackendType::Pvc => "pvc",
        StorageBackendType::Nfs => "nfs",
    }
}

fn access_mode_name(value: StorageAccessMode) -> &'static str {
    match value {
        StorageAccessMode::ReadWriteOnce => "read_write_once",
        StorageAccessMode::ReadWriteMany => "read_write_many",
        StorageAccessMode::ReadOnlyMany => "read_only_many",
    }
}

fn volume_state_name(value: StorageVolumeState) -> &'static str {
    match value {
        StorageVolumeState::Ready => "ready",
        StorageVolumeState::Degraded => "degraded",
        StorageVolumeState::Unavailable => "unavailable",
    }
}

fn playground_state_name(value: PlaygroundState) -> &'static str {
    match value {
        PlaygroundState::Creating => "creating",
        PlaygroundState::Ready => "ready",
        PlaygroundState::Abnormal => "abnormal",
    }
}

fn snapshot_state_name(value: SnapshotState) -> &'static str {
    match value {
        SnapshotState::Creating => "creating",
        SnapshotState::Ready => "ready",
        SnapshotState::Abnormal => "abnormal",
    }
}

const BROWSE_CURSOR_PREFIX: &str = "ngbrowse_v1_";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BrowseCursorScope {
    kind: &'static str,
    source_id: Option<String>,
    baseline_commit_id: Option<String>,
    index_revision: String,
    index_digest: String,
    path_prefix: Option<String>,
    format: Option<String>,
    change_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowseCursorDocument {
    version: u8,
    scope_digest: String,
    path: String,
}

fn index_version_body(version: &WireIndexVersion) -> IndexVersionBody {
    IndexVersionBody {
        revision: version.revision.to_string(),
        digest: version.digest.to_string(),
    }
}

fn parse_logical_path(field: &'static str, value: String) -> Result<LogicalPath, Error> {
    LogicalPath::parse(value).map_err(|error| invalid_request(format!("{field}: {error}")))
}

fn validate_file_format(value: String) -> Result<String, Error> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(invalid_request("format is invalid"));
    }
    Ok(value.to_ascii_lowercase())
}

fn validate_change_type(value: String) -> Result<String, Error> {
    match value.as_str() {
        "added" | "modified" | "deleted" | "renamed" => Ok(value),
        _ => Err(invalid_request(
            "change_type must be added, modified, deleted, or renamed",
        )),
    }
}

fn file_format(path: &str) -> String {
    path.rsplit_once('.')
        .filter(|(_, extension)| !extension.is_empty() && !extension.contains('/'))
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn media_type(format: &str) -> Option<&'static str> {
    match format {
        "json" => Some("application/json"),
        "csv" => Some("text/csv"),
        "parquet" => Some("application/vnd.apache.parquet"),
        "txt" => Some("text/plain"),
        "yaml" | "yml" => Some("application/yaml"),
        "xml" => Some("application/xml"),
        _ => None,
    }
}

fn path_matches_prefix(path: &LogicalPath, prefix: Option<&LogicalPath>) -> bool {
    prefix.is_none_or(|prefix| path == prefix || prefix.is_ancestor_of(path))
}

#[allow(clippy::too_many_arguments)]
fn build_change_list_response(
    source: &'static str,
    precommit_id: Option<String>,
    baseline_commit_id: Option<CommitId>,
    version: &WireIndexVersion,
    base_records: &[FileRecord],
    candidate_records: &[FileRecord],
    change_type: Option<String>,
    path_prefix: Option<LogicalPath>,
    cursor: Option<&str>,
    page_size: u16,
) -> Result<QueryPlaygroundChangeListResponse, Error> {
    let scope = BrowseCursorScope {
        kind: if source == "precommit" {
            "precommit_changes"
        } else {
            "changes"
        },
        source_id: precommit_id.clone(),
        baseline_commit_id: baseline_commit_id.map(|commit_id| commit_id.to_string()),
        index_revision: version.revision.to_string(),
        index_digest: version.digest.to_string(),
        path_prefix: path_prefix.as_ref().map(ToString::to_string),
        format: None,
        change_type: change_type.clone(),
    };
    let after = cursor
        .map(|cursor| decode_browse_cursor(cursor, &scope))
        .transpose()?;
    let diff = diff_index_snapshots(base_records, candidate_records).map_err(map_central_error)?;
    let matching_changes = diff
        .changes
        .into_iter()
        .filter(|change| path_matches_prefix(&change.path, path_prefix.as_ref()))
        .collect::<Vec<_>>();
    let summary = summarize_index_changes(&matching_changes).map_err(map_central_error)?;
    let mut entries = matching_changes
        .into_iter()
        .filter(|change| {
            change_type
                .as_deref()
                .is_none_or(|requested| requested == index_change_kind_name(change.kind))
        })
        .map(|change| PlaygroundChangeEntry {
            change_type: index_change_kind_name(change.kind).to_owned(),
            format: Some(file_format(change.path.as_str())),
            path: change.path.to_string(),
            previous_path: change.previous_path.map(|path| path.to_string()),
            old_size_bytes: change.old_size.map(|size| size.to_string()),
            new_size_bytes: change.new_size.map(|size| size.to_string()),
        })
        .collect::<Vec<_>>();
    if let Some(after) = after {
        entries.retain(|entry| entry.path > after);
    }
    let (items, next_cursor) = take_change_page(entries, page_size, &scope)?;
    Ok(QueryPlaygroundChangeListResponse {
        source: source.to_owned(),
        precommit_id,
        index_version: index_version_body(version),
        summary: PlaygroundChangeSummary {
            files_added: summary.files_added.to_string(),
            files_modified: summary.files_modified.to_string(),
            files_deleted: summary.files_deleted.to_string(),
            files_renamed: summary.files_renamed.to_string(),
            bytes_added: summary.bytes_added.to_string(),
            bytes_removed: summary.bytes_removed.to_string(),
        },
        items,
        next_cursor,
    })
}

const fn index_change_kind_name(kind: IndexSnapshotChangeKind) -> &'static str {
    match kind {
        IndexSnapshotChangeKind::Added => "added",
        IndexSnapshotChangeKind::Modified => "modified",
        IndexSnapshotChangeKind::Deleted => "deleted",
        IndexSnapshotChangeKind::Renamed => "renamed",
    }
}

fn logical_entries(
    records: &[FileRecord],
    prefix: Option<&LogicalPath>,
    format: Option<&str>,
) -> Vec<LogicalFileEntry> {
    let matching = records
        .iter()
        .filter(|record| path_matches_prefix(&record.path, prefix))
        .filter(|record| format.is_none_or(|wanted| file_format(record.path.as_str()) == wanted));
    let mut files = BTreeMap::<String, LogicalFileEntry>::new();
    for record in matching {
        files.insert(
            record.path.to_string(),
            LogicalFileEntry {
                path: record.path.to_string(),
                entry_type: "file".to_owned(),
                size_bytes: Some(record.total_size.to_string()),
                format: Some(file_format(record.path.as_str())),
                row_count: None,
                updated_at_unix_ms: None,
            },
        );
        let mut current = record.path.parent();
        while let Some(directory) = current {
            if prefix.is_none_or(|wanted| directory == *wanted || wanted.is_ancestor_of(&directory))
            {
                files
                    .entry(directory.to_string())
                    .or_insert_with(|| LogicalFileEntry {
                        path: directory.to_string(),
                        entry_type: "directory".to_owned(),
                        size_bytes: None,
                        format: None,
                        row_count: None,
                        updated_at_unix_ms: None,
                    });
            }
            current = directory.parent();
        }
    }
    files.into_values().collect()
}

fn take_browse_page(
    mut entries: Vec<LogicalFileEntry>,
    page_size: u16,
    scope: &BrowseCursorScope,
) -> Result<(Vec<LogicalFileEntry>, Option<String>), Error> {
    let limit = usize::from(page_size);
    let next = entries.len() > limit;
    if next {
        entries.truncate(limit);
    }
    let next_cursor = next
        .then(|| {
            entries
                .last()
                .map(|entry| encode_browse_cursor(scope, &entry.path))
        })
        .flatten()
        .transpose()?;
    Ok((entries, next_cursor))
}

fn take_change_page(
    mut entries: Vec<PlaygroundChangeEntry>,
    page_size: u16,
    scope: &BrowseCursorScope,
) -> Result<(Vec<PlaygroundChangeEntry>, Option<String>), Error> {
    let limit = usize::from(page_size);
    let next = entries.len() > limit;
    if next {
        entries.truncate(limit);
    }
    let next_cursor = next
        .then(|| {
            entries
                .last()
                .map(|entry| encode_browse_cursor(scope, &entry.path))
        })
        .flatten()
        .transpose()?;
    Ok((entries, next_cursor))
}

fn encode_browse_cursor(scope: &BrowseCursorScope, path: &str) -> Result<String, Error> {
    let document = BrowseCursorDocument {
        version: 1,
        scope_digest: browse_scope_digest(scope)?,
        path: path.to_owned(),
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| internal_catalog_error())?;
    Ok(format!(
        "{BROWSE_CURSOR_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(bytes)
    ))
}

fn decode_browse_cursor(encoded: &str, scope: &BrowseCursorScope) -> Result<String, Error> {
    if encoded.len() > 2_048 {
        return Err(cursor_conflict());
    }
    let value = encoded
        .strip_prefix(BROWSE_CURSOR_PREFIX)
        .ok_or_else(cursor_conflict)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|_| cursor_conflict())?;
    let document: BrowseCursorDocument =
        serde_json::from_slice(&bytes).map_err(|_| cursor_conflict())?;
    if document.version != 1 || document.scope_digest != browse_scope_digest(scope)? {
        return Err(cursor_conflict());
    }
    LogicalPath::parse(document.path.clone()).map_err(|_| cursor_conflict())?;
    Ok(document.path)
}

fn browse_scope_digest(scope: &BrowseCursorScope) -> Result<String, Error> {
    serde_json::to_vec(scope)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|_| internal_catalog_error())
}

fn validate_display_name(value: String) -> Result<String, Error> {
    validate_text("display_name", value, 1, 128)
}

fn validate_description(value: String) -> Result<String, Error> {
    validate_text("description", value, 0, 2_048)
}

fn validate_region(value: String) -> Result<String, Error> {
    let mut bytes = value.bytes();
    if value.len() <= 64
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Ok(value)
    } else {
        Err(invalid_request("region is invalid"))
    }
}

fn validate_query(value: String) -> Result<String, Error> {
    validate_text("query", value, 1, MAX_QUERY_CHARS)
}

fn validate_nfs_server(value: String) -> Result<String, Error> {
    validate_text("nfs_reference.server", value, 1, 253)
}

fn validate_nfs_export_path(value: String) -> Result<String, Error> {
    if value.starts_with('/') && value.len() <= 1_024 && !value.contains('\0') {
        Ok(value)
    } else {
        Err(invalid_request(
            "nfs_reference.export_path must be an absolute NFS export path",
        ))
    }
}

fn validate_text(
    field: &'static str,
    value: String,
    min_chars: usize,
    max_chars: usize,
) -> Result<String, Error> {
    let count = value.chars().count();
    if (min_chars..=max_chars).contains(&count) && !value.chars().any(char::is_control) {
        Ok(value)
    } else {
        Err(invalid_request(format!(
            "{field} must contain {min_chars} to {max_chars} non-control characters"
        )))
    }
}

fn page_size(value: Option<u16>) -> Result<u16, Error> {
    let value = value.unwrap_or(DEFAULT_PAGE_SIZE);
    if (1..=MAX_PAGE_SIZE).contains(&value) {
        Ok(value)
    } else {
        Err(invalid_request("page_size must be between 1 and 100"))
    }
}

fn parse_canonical_u64(value: &str) -> Result<u64, ()> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(());
    }
    value.parse().map_err(|_| ())
}

fn resource_not_found(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::NotFound,
        "resource_not_found",
        "RESOURCE_NOT_FOUND",
        format!("{resource} not found"),
        false,
    )
}

fn cursor_conflict() -> Error {
    catalog_conflict(
        "cursor_scope_conflict",
        "CURSOR_SCOPE_CONFLICT",
        "cursor is invalid or belongs to another catalog query",
    )
}

fn commit_graph_changed() -> Error {
    application_error(
        ErrorCategory::Conflict,
        "commit_graph_changed",
        "COMMIT_GRAPH_CHANGED",
        "the Commit graph changed while it was being read; retry the query",
        true,
    )
}

fn catalog_conflict(code: &'static str, neo_code: &'static str, message: &'static str) -> Error {
    application_error(ErrorCategory::Conflict, code, neo_code, message, false)
}

fn catalog_mutation_error(error: crate::CentralError, reused_code: &'static str) -> Error {
    match error.code() {
        crate::CentralErrorCode::InvalidState => catalog_conflict(
            reused_code,
            match reused_code {
                "tenant_id_reused" => "TENANT_ID_REUSED",
                "artifact_id_reused" => "ARTIFACT_ID_REUSED",
                "storage_volume_id_reused" => "STORAGE_VOLUME_ID_REUSED",
                "playground_id_reused" => "PLAYGROUND_ID_REUSED",
                _ => "MUTATION_CONFLICT",
            },
            "resource ID is already bound to another create request",
        ),
        crate::CentralErrorCode::VolumeOwnerConflict => catalog_conflict(
            "storage_volume_binding_conflict",
            "STORAGE_VOLUME_BINDING_CONFLICT",
            "StorageVolume or PVC identity is already registered",
        ),
        _ => map_central_error(error),
    }
}

fn playground_mutation_error(error: crate::CentralError) -> Error {
    match error.code() {
        crate::CentralErrorCode::ArtifactNotFound => resource_not_found("artifact"),
        crate::CentralErrorCode::StorageVolumeNotFound => resource_not_found("storage volume"),
        crate::CentralErrorCode::ArtifactHeadMismatch if error.retryable() => application_error(
            ErrorCategory::Conflict,
            "artifact_head_changed",
            "ARTIFACT_HEAD_MISMATCH",
            "the Artifact Head changed before Playground creation; retry the request",
            true,
        ),
        crate::CentralErrorCode::ArtifactHeadMismatch => catalog_conflict(
            "artifact_base_commit_mismatch",
            "ARTIFACT_BASE_COMMIT_MISMATCH",
            "the Playground base Commit must match its initial head Commit",
        ),
        crate::CentralErrorCode::StorageVolumeNotReady => catalog_conflict(
            "storage_volume_not_ready",
            "STORAGE_VOLUME_NOT_READY",
            "the selected StorageVolume is not ready for placement",
        ),
        crate::CentralErrorCode::StorageVolumeRegionMismatch => catalog_conflict(
            "storage_volume_region_mismatch",
            "STORAGE_VOLUME_REGION_MISMATCH",
            "the selected StorageVolume region changed before placement",
        ),
        _ => catalog_mutation_error(error, "playground_id_reused"),
    }
}

fn snapshot_mutation_error(error: crate::CentralError) -> Error {
    match error.code() {
        crate::CentralErrorCode::ArtifactNotFound => resource_not_found("artifact"),
        crate::CentralErrorCode::ArtifactHeadMismatch if error.retryable() => application_error(
            ErrorCategory::Conflict,
            "artifact_head_changed",
            "ARTIFACT_HEAD_MISMATCH",
            "the Artifact Head changed before Snapshot creation; retry the request",
            true,
        ),
        _ => catalog_mutation_error(error, "snapshot_request_id_reused"),
    }
}

fn precommit_mutation_error(error: crate::CentralError) -> Error {
    match error.code() {
        crate::CentralErrorCode::ConcurrentUpdate
        | crate::CentralErrorCode::InvalidState
        | crate::CentralErrorCode::JobIdReused => precommit_conflict(
            "the Pre-commit mutation conflicts with its current state or idempotency identity",
        ),
        crate::CentralErrorCode::JobNotFound => resource_not_found("precommit"),
        _ => map_central_error(error),
    }
}

fn precommit_conflict(message: impl Into<String>) -> Error {
    application_error(
        ErrorCategory::Conflict,
        "precommit_conflict",
        "PRECOMMIT_CONFLICT",
        message,
        false,
    )
}

fn parse_s3_access_point_id(value: String) -> Result<S3AccessPointId, Error> {
    S3AccessPointId::new(value)
        .map_err(|error| invalid_request(format!("access_point_id: {error}")))
}

fn parse_s3_credential_id(value: String) -> Result<S3CredentialId, Error> {
    S3CredentialId::new(value).map_err(|error| invalid_request(format!("credential_id: {error}")))
}

fn validate_s3_bucket_name(value: &str) -> Result<String, Error> {
    let value = value.trim();
    if !(3..=63).contains(&value.len())
        || value != value.to_ascii_lowercase()
        || value.starts_with('.')
        || value.ends_with('.')
        || value.starts_with('-')
        || value.ends_with('-')
        || value.contains("..")
        || value.contains(".-")
        || value.contains("-.")
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'.'
        })
    {
        return Err(invalid_request(
            "bucket_name must be 3-63 lowercase DNS-compatible characters",
        ));
    }
    if value.parse::<Ipv4Addr>().is_ok() {
        return Err(invalid_request(
            "bucket_name must not be formatted as an IP address",
        ));
    }
    if matches!(value, "api" | "health" | "console" | "s3") {
        return Err(invalid_request("bucket_name is reserved by the Gateway"));
    }
    Ok(value.to_owned())
}

fn validate_s3_prefix(value: String) -> Result<String, Error> {
    if value.is_empty() {
        return Ok(value);
    }
    let canonical = value.strip_suffix('/').unwrap_or(&value);
    if value.len() > 1024 || canonical.is_empty() || LogicalPath::parse(canonical).is_err() {
        return Err(invalid_request(
            "prefix must use canonical LogicalPath components",
        ));
    }
    Ok(value)
}

fn validate_s3_object_key(value: String) -> Result<String, Error> {
    LogicalPath::parse(value.clone())
        .map(|_| value)
        .map_err(|error| invalid_request(format!("key: {error}")))
}

fn validate_s3_cursor_key(value: &str) -> Result<(), Error> {
    if value.ends_with('/') {
        let prefix = value.to_owned();
        return validate_s3_prefix(prefix).map(|_| ());
    }
    validate_s3_object_key(value.to_owned()).map(|_| ())
}

fn s3_catalog_entries<'a>(
    commit: &'a CommitRecord,
    prefix: &str,
    delimiter: &str,
) -> Vec<(String, Option<&'a FileRecord>)> {
    let mut catalog = BTreeMap::new();
    for record in &commit.records {
        let key = record.path.to_string();
        if !key.starts_with(prefix) {
            continue;
        }
        if !delimiter.is_empty() {
            let rest = &key[prefix.len()..];
            if let Some(index) = rest.find('/') {
                catalog
                    .entry(format!("{}{}", prefix, &rest[..index + 1]))
                    .or_insert(None);
                continue;
            }
        }
        catalog.insert(key, Some(record));
    }
    catalog.into_iter().collect()
}

fn authorized_object(key: &str, record: &FileRecord, commit: &CommitRecord) -> S3AuthorizedObject {
    S3AuthorizedObject {
        key: key.to_owned(),
        size_bytes: record.total_size,
        etag: record.manifest_id.digest(),
        last_modified_unix_ms: commit.created_at_unix_ms,
        content_type: media_type(&file_format(key))
            .unwrap_or("application/octet-stream")
            .to_owned(),
    }
}

fn s3_if_match_matches(value: &str, current: &str) -> bool {
    s3_etag_list_matches(value, current, false)
}

fn s3_if_none_match_matches(value: &str, current: &str) -> bool {
    s3_etag_list_matches(value, current, true)
}

fn s3_etag_list_matches(value: &str, current: &str, allow_weak: bool) -> bool {
    let current = current
        .strip_prefix('"')
        .and_then(|current| current.strip_suffix('"'))
        .unwrap_or(current);
    value.split(',').map(str::trim).any(|candidate| {
        if candidate == "*" {
            return true;
        }
        let (weak, candidate) = candidate
            .strip_prefix("W/")
            .map_or((false, candidate), |candidate| (true, candidate));
        if weak && !allow_weak {
            return false;
        }
        candidate
            .strip_prefix('"')
            .and_then(|candidate| candidate.strip_suffix('"'))
            == Some(current)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum S3AuthorizedRangeError {
    Invalid,
    Unsatisfiable,
}

fn parse_s3_authorized_range(
    value: &str,
    object_size: u64,
) -> Result<(u64, u64), S3AuthorizedRangeError> {
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(S3AuthorizedRangeError::Invalid);
    };
    if spec.is_empty() || spec.contains(',') {
        return Err(S3AuthorizedRangeError::Invalid);
    }
    let Some((start, end)) = spec.split_once('-') else {
        return Err(S3AuthorizedRangeError::Invalid);
    };
    if start.is_empty() {
        let suffix = end
            .parse::<u64>()
            .map_err(|_| S3AuthorizedRangeError::Invalid)?;
        if suffix == 0 || object_size == 0 {
            return Err(S3AuthorizedRangeError::Unsatisfiable);
        }
        let length = suffix.min(object_size);
        return Ok((object_size - length, object_size));
    }
    let start = start
        .parse::<u64>()
        .map_err(|_| S3AuthorizedRangeError::Invalid)?;
    if start >= object_size {
        return Err(S3AuthorizedRangeError::Unsatisfiable);
    }
    let end = if end.is_empty() {
        object_size - 1
    } else {
        end.parse::<u64>()
            .map_err(|_| S3AuthorizedRangeError::Invalid)?
    };
    if end < start {
        return Err(S3AuthorizedRangeError::Unsatisfiable);
    }
    Ok((start, end.min(object_size - 1) + 1))
}

fn s3_access_denied() -> Error {
    application_error(
        ErrorCategory::PermissionDenied,
        "s3_access_denied",
        "S3_ACCESS_DENIED",
        "The S3 request is not authorized",
        false,
    )
}

fn s3_snapshot_unavailable() -> Error {
    application_error(
        ErrorCategory::Unavailable,
        "s3_snapshot_unavailable",
        "SNAPSHOT_UNAVAILABLE",
        "The Snapshot is not currently available",
        true,
    )
}

fn s3_page_size(requested: Option<u16>) -> Result<u16, Error> {
    let value = requested.unwrap_or(100);
    if value == 0 || value > 1000 {
        return Err(invalid_request("page_size must be in 1..=1000"));
    }
    Ok(value)
}

fn s3_mutation_record<T: Serialize>(
    tenant_id: &TenantId,
    request_id: &RequestId,
    operation: S3MutationKind,
    payload: &T,
    created_at_unix_ms: UnixMillis,
) -> Result<S3MutationRecord, Error> {
    let material = serde_json::json!({
        "version": 1,
        "operation": s3_mutation_kind_name(operation),
        "payload": payload,
    });
    let bytes =
        serde_json_canonicalizer::to_vec(&material).map_err(|_| internal_catalog_error())?;
    Ok(S3MutationRecord {
        tenant_id: tenant_id.clone(),
        request_id: request_id.clone(),
        operation,
        request_digest: ContentDigest::hash(bytes),
        created_at_unix_ms,
    })
}

fn s3_mutation_kind_name(value: S3MutationKind) -> &'static str {
    match value {
        S3MutationKind::AccessPointCreate => "access_point_create",
        S3MutationKind::AccessPointEnable => "access_point_enable",
        S3MutationKind::AccessPointDisable => "access_point_disable",
        S3MutationKind::CredentialCreate => "credential_create",
        S3MutationKind::CredentialRevoke => "credential_revoke",
    }
}

fn ensure_s3_mutation_identity(
    existing: &S3MutationRecord,
    requested: &S3MutationRecord,
) -> Result<(), Error> {
    if existing.operation == requested.operation
        && existing.request_digest == requested.request_digest
    {
        Ok(())
    } else {
        Err(s3_request_id_reused())
    }
}

fn map_s3_mutation_error(error: crate::CentralError) -> Error {
    if error.code() == crate::CentralErrorCode::InvalidState
        && error.message().contains("request identity")
    {
        s3_request_id_reused()
    } else {
        map_central_error(error)
    }
}

fn map_s3_credential_mutation_error(error: crate::CentralError) -> Error {
    if error.code() == crate::CentralErrorCode::InvalidState
        && error.message().contains("at most two active credentials")
    {
        catalog_conflict(
            "s3_credential_limit",
            "S3_CREDENTIAL_LIMIT",
            "an Access Point can have at most two active credentials",
        )
    } else {
        map_s3_mutation_error(error)
    }
}

fn s3_request_id_reused() -> Error {
    catalog_conflict(
        "s3_request_id_reused",
        "S3_REQUEST_ID_REUSED",
        "request_id is already bound to another S3 mutation payload",
    )
}

fn s3_access_point_id_for_request(
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> Result<S3AccessPointId, Error> {
    let digest = blake3::hash(format!("{tenant_id}\0{request_id}").as_bytes())
        .to_hex()
        .to_string();
    S3AccessPointId::new(format!("s3ap-{}", &digest[..24]))
        .map_err(|error| invalid_request(format!("access_point_id: {error}")))
}

fn s3_credential_id_for_request(
    access_point_id: &S3AccessPointId,
    request_id: &RequestId,
) -> Result<S3CredentialId, Error> {
    let digest = blake3::hash(format!("{access_point_id}\0{request_id}").as_bytes())
        .to_hex()
        .to_string();
    S3CredentialId::new(format!("s3cred-{}", &digest[..24]))
        .map_err(|error| invalid_request(format!("credential_id: {error}")))
}

fn s3_secret_context(credential: &S3CredentialRecord) -> Vec<u8> {
    format!(
        "{}\0{}\0{}",
        credential.access_point_id, credential.credential_id, credential.access_key_id
    )
    .into_bytes()
}

pub(crate) fn development_s3_envelope_key() -> [u8; 32] {
    *blake3::hash(b"neoengram-development-s3-envelope-key-v1").as_bytes()
}

fn s3_access_point_state_name(value: S3AccessPointState) -> &'static str {
    match value {
        S3AccessPointState::Active => "active",
        S3AccessPointState::Disabled => "disabled",
    }
}

fn s3_credential_view(record: &S3CredentialRecord) -> S3CredentialView {
    S3CredentialView {
        credential_id: record.credential_id.to_string(),
        access_point_id: record.access_point_id.to_string(),
        access_key_id: record.access_key_id.clone(),
        state: match record.state {
            S3CredentialState::Active => "active",
            S3CredentialState::Revoked => "revoked",
            S3CredentialState::Expired => "expired",
        }
        .to_owned(),
        expires_at_unix_ms: record.expires_at_unix_ms.to_string(),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        last_used_at_unix_ms: record.last_used_at_unix_ms.map(|value| value.to_string()),
    }
}

fn internal_catalog_error() -> Error {
    application_error(
        ErrorCategory::Internal,
        "catalog_internal",
        "INTERNAL",
        "the server could not complete the catalog operation",
        false,
    )
}

#[cfg(test)]
mod s3_cursor_tests {
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::{GatewayRegistryRepository as _, PlacementRepository as _};
    use neoengram_domain::core::ObjectId;
    use neoengram_domain::protocol::{
        materialization::ObjectPlacementState, ObjectEncoding, S3SigV4Request, SnapshotDeliveryId,
    };

    const TEST_KEY: [u8; 32] = [0x5a; 32];

    #[derive(Default)]
    struct RecordingS3ReadRevocations {
        calls: StdMutex<Vec<(GatewayPoolId, GatewayS3ReadRevocation)>>,
    }

    #[async_trait]
    impl S3ReadRevocationPublisher for RecordingS3ReadRevocations {
        async fn publish_s3_read_revocation(
            &self,
            gateway_pool_id: &GatewayPoolId,
            revocation: GatewayS3ReadRevocation,
        ) {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((gateway_pool_id.clone(), revocation));
        }
    }

    async fn install_active_s3_gateway_replica(
        registry: &crate::InMemoryGatewayRegistry,
        gateway_pool_id: &neoengram_domain::protocol::GatewayPoolId,
        edge_cluster_id: &EdgeClusterId,
        replica_name: &str,
        heartbeat_at: UnixMillis,
    ) -> crate::GatewayReplicaRecord {
        let mut replica = crate::GatewayReplicaRecord {
            gateway_replica_id: neoengram_domain::protocol::GatewayReplicaId::new(replica_name)
                .unwrap(),
            gateway_pool_id: gateway_pool_id.clone(),
            edge_cluster_id: edge_cluster_id.clone(),
            control_endpoint: format!("https://{replica_name}.control.s3.test"),
            peer_endpoint: format!("https://{replica_name}.peer.s3.test"),
            bootstrap_endpoint: format!("https://{replica_name}.bootstrap.s3.test"),
            software_version: "0.2.0".to_owned(),
            wire_version: neoengram_domain::protocol::CURRENT_WIRE_VERSION,
            capabilities: neoengram_domain::protocol::gateway_capabilities_v1(),
            last_heartbeat_at_unix_ms: None,
            state: GatewayReplicaState::Pending,
            credential: crate::GatewayReplicaCredential {
                activation_token_digest: ContentDigest::hash(
                    format!("activation-token-{replica_name}").as_bytes(),
                ),
                activation_created_at_unix_ms: UnixMillis::new(100),
                activation_expires_at_unix_ms: UnixMillis::new(900_100),
                activation_consumed_at_unix_ms: None,
                public_key_fingerprint: None,
                certificate_generation: None,
                certificate_fingerprint: None,
                certificate_not_after_unix_ms: None,
                certificate: None,
                state: GatewayCredentialState::PendingActivation,
            },
            resource_version: neoengram_domain::protocol::ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
        };
        registry.insert_replica(replica.clone()).await.unwrap();

        let public_key =
            neoengram_domain::protocol::Ed25519PublicKeySpki::from_public_key_bytes([7; 32]);
        let leaf_certificate =
            GatewayOpaqueBytes::new(format!("replica-certificate-{replica_name}").into_bytes())
                .unwrap();
        let certificate_generation = neoengram_domain::protocol::CertificateGeneration::new(1);
        let certificate_not_after = UnixMillis::new(21_600_000);
        replica.credential.public_key_fingerprint = Some(public_key.fingerprint());
        replica.credential.certificate_generation = Some(certificate_generation);
        replica.credential.certificate_fingerprint =
            Some(ContentDigest::hash(leaf_certificate.as_bytes()));
        replica.credential.certificate_not_after_unix_ms = Some(certificate_not_after);
        replica.credential.certificate = Some(crate::GatewayReplicaCertificateRecord {
            request_id: RequestId::new(format!("{replica_name}-activation")).unwrap(),
            public_key_spki: public_key,
            certificate_generation,
            not_before_unix_ms: UnixMillis::new(150),
            not_after_unix_ms: certificate_not_after,
            server_names: BTreeSet::new(),
            leaf_certificate_der: leaf_certificate,
            issuer_chain_der: vec![GatewayOpaqueBytes::new(b"replica-issuer".to_vec()).unwrap()],
        });
        replica.credential.state = GatewayCredentialState::PendingCertificateDelivery;
        replica.resource_version = neoengram_domain::protocol::ResourceVersion::new(2);
        replica.updated_at_unix_ms = UnixMillis::new(150);
        replica = registry.replace_replica(1, replica).await.unwrap();

        replica.state = GatewayReplicaState::Active;
        replica.credential.state = GatewayCredentialState::Active;
        replica.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(200));
        replica.last_heartbeat_at_unix_ms = Some(heartbeat_at);
        replica.resource_version = neoengram_domain::protocol::ResourceVersion::new(3);
        replica.updated_at_unix_ms = heartbeat_at;
        registry.replace_replica(2, replica).await.unwrap()
    }

    #[tokio::test]
    async fn catalog_revocation_publisher_preserves_both_generation_fences() {
        let components = crate::InMemoryComponents::new(100_000);
        let publisher = Arc::new(RecordingS3ReadRevocations::default());
        let policy = Arc::new(
            StaticRbacPolicy::one_principal(
                "s3-test",
                ["*".to_owned()],
                std::iter::empty::<Permission>(),
            )
            .unwrap(),
        );
        let service = CatalogService::new(
            components.control_catalog,
            components.publisher,
            policy,
            components.clock,
        )
        .with_s3_read_revocation_publisher(publisher.clone());
        let access_point = S3AccessPointRecord {
            access_point_id: S3AccessPointId::new("s3ap-test").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            snapshot_id: SnapshotId::new("snapshot-a").unwrap(),
            commit_id: ContentDigest::hash(b"commit-a"),
            delivery_id: SnapshotDeliveryId::new("delivery-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            bucket_name: "dataset-a".to_owned(),
            state: S3AccessPointState::Disabled,
            policy_generation: 9,
            created_at_unix_ms: UnixMillis::new(1),
            updated_at_unix_ms: UnixMillis::new(2),
        };

        service
            .publish_access_point_s3_read_revocation(
                &access_point,
                LifecycleGeneration::new(6),
                "snapshot lifecycle changed",
            )
            .await;

        let calls = publisher
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // The route is resolved from the current Commit placement. This isolated fixture does
        // not install a Commit/Agent route, so revocation fails closed without publishing.
        assert!(calls.is_empty());
    }

    #[test]
    fn continuation_cursor_accepts_a_common_prefix_as_the_last_key() {
        let scope = S3ObjectCursorScope {
            access_point_id: "s3ap-test".to_owned(),
            policy_generation: 3,
            snapshot_id: "snapshot-test".to_owned(),
            index_digest: "index-test".to_owned(),
            prefix: "photos/".to_owned(),
            delimiter: "/".to_owned(),
        };
        let encoded = encode_s3_object_cursor(&TEST_KEY, &scope, 1, "photos/2026/").unwrap();
        let decoded = decode_s3_object_cursor(&TEST_KEY, &encoded, &scope).unwrap();
        let entries = vec![("photos/2026/".to_owned(), ())];
        assert_eq!(s3_object_cursor_start(&entries, &decoded).unwrap(), 1);
    }

    #[test]
    fn continuation_cursor_rejects_tampered_position_or_wrong_key() {
        let scope = S3ObjectCursorScope {
            access_point_id: "s3ap-test".to_owned(),
            policy_generation: 3,
            snapshot_id: "snapshot-test".to_owned(),
            index_digest: "index-test".to_owned(),
            prefix: "photos/".to_owned(),
            delimiter: "/".to_owned(),
        };
        let encoded = encode_s3_object_cursor(&TEST_KEY, &scope, 1, "photos/2026/").unwrap();
        let body = encoded.strip_prefix(CURSOR_PREFIX).unwrap();
        let (payload, mac) = body.split_once('.').unwrap();
        let bytes = URL_SAFE_NO_PAD.decode(payload).unwrap();
        let mut document: S3ObjectCursorDocument = serde_json::from_slice(&bytes).unwrap();
        document.position = 2;
        let tampered_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&document).unwrap());
        let tampered = format!("{CURSOR_PREFIX}{tampered_payload}.{mac}");

        assert!(decode_s3_object_cursor(&TEST_KEY, &tampered, &scope).is_err());
        assert!(decode_s3_object_cursor(&[0x6b; 32], &encoded, &scope).is_err());
    }

    #[test]
    fn continuation_cursor_is_bounded_and_binds_a_long_last_key() {
        let scope = S3ObjectCursorScope {
            access_point_id: "s3ap-test".to_owned(),
            policy_generation: 3,
            snapshot_id: "snapshot-test".to_owned(),
            index_digest: "index-test".to_owned(),
            prefix: "a/".repeat(500),
            delimiter: "/".to_owned(),
        };
        let key = format!("objects/{}.bin", "x".repeat(4_000));
        let encoded = encode_s3_object_cursor(&TEST_KEY, &scope, 1, &key).unwrap();
        assert!(encoded.len() < 512, "cursor length was {}", encoded.len());

        let decoded = decode_s3_object_cursor(&TEST_KEY, &encoded, &scope).unwrap();
        let entries = vec![(key, ())];
        assert_eq!(s3_object_cursor_start(&entries, &decoded).unwrap(), 1);

        let changed_entries = vec![("objects/changed.bin".to_owned(), ())];
        assert!(s3_object_cursor_start(&changed_entries, &decoded).is_err());
    }

    #[test]
    fn etag_preconditions_use_strong_if_match_and_weak_if_none_match() {
        assert!(s3_if_match_matches("\"manifest-a\"", "manifest-a"));
        assert!(!s3_if_match_matches("W/\"manifest-a\"", "manifest-a"));
        assert!(s3_if_none_match_matches("W/\"manifest-a\"", "manifest-a"));
        assert!(!s3_if_match_matches("\"manifest-a", "manifest-a"));
        assert!(s3_if_match_matches("*", "manifest-a"));
    }

    #[test]
    fn presigned_get_supports_aws_expiry_and_unsigned_range_requests() {
        let now = UnixMillis::new(1_786_924_800_000);
        let secret = b"test-s3-presign-secret";
        let signed = presign_s3_get(&S3PresignRequest {
            endpoint: "https://s3.example.test",
            bucket: "dataset-a",
            key: "folder/file.csv",
            region: "us-east-1",
            access_key_id: "NGS3EXAMPLE",
            secret,
            now_unix_seconds: now.get() / 1_000,
            expires_seconds: 3_600,
        })
        .unwrap();
        let url = url::Url::parse(&signed).unwrap();
        let request = S3SigV4Request {
            method: "GET".to_owned(),
            path: url.path().to_owned(),
            query: url.query().unwrap().to_owned(),
            headers: vec![
                ("host".to_owned(), "s3.example.test".to_owned()),
                ("range".to_owned(), "bytes=100-199".to_owned()),
            ],
        };

        let claims = request.verify_at(secret, now.get() / 1_000).unwrap();
        assert_eq!(claims.expires_at_unix_seconds, now.get() / 1_000 + 3_600);
        S3AuthorizeRequest {
            gateway_pool_id: "pool-a".to_owned(),
            bucket: "dataset-a".to_owned(),
            operation: S3AuthorizeOperation::GetObject {
                key: "folder/file.csv".to_owned(),
                range: Some("bytes=100-199".to_owned()),
                if_match: None,
                if_none_match: None,
            },
            sigv4: request,
        }
        .validate_signed_operation_binding()
        .unwrap();
    }

    #[test]
    fn presigned_get_allows_only_loopback_plain_http() {
        let now = UnixMillis::new(1_786_924_800_000);
        let secret = b"test-s3-presign-secret";
        let signed = presign_s3_get(&S3PresignRequest {
            endpoint: "http://127.0.0.1:4174",
            bucket: "dataset-a",
            key: "folder/file.csv",
            region: "us-east-1",
            access_key_id: "NGS3EXAMPLE",
            secret,
            now_unix_seconds: now.get() / 1_000,
            expires_seconds: 300,
        })
        .unwrap();
        let url = url::Url::parse(&signed).unwrap();
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_eq!(url.port(), Some(4174));
        assert_eq!(url.path(), "/dataset-a/folder/file.csv");

        assert!(presign_s3_get(&S3PresignRequest {
            endpoint: "http://s3.example.test",
            bucket: "dataset-a",
            key: "folder/file.csv",
            region: "us-east-1",
            access_key_id: "NGS3EXAMPLE",
            secret,
            now_unix_seconds: now.get() / 1_000,
            expires_seconds: 300,
        })
        .is_err());
    }

    #[tokio::test]
    async fn s3_gateway_pool_readiness_requires_fresh_minimum_replicas() {
        const NOW_UNIX_MS: u64 = 100_000;

        let components = crate::InMemoryComponents::new(NOW_UNIX_MS);
        let registry = Arc::new(crate::InMemoryGatewayRegistry::new());
        let gateway_pool_id = neoengram_domain::protocol::GatewayPoolId::new("pool-s3").unwrap();
        let edge_cluster_id = EdgeClusterId::new("edge-s3").unwrap();
        let principal = neoengram_domain::protocol::PrincipalRef {
            kind: neoengram_domain::protocol::PrincipalKind::System,
            id: neoengram_domain::protocol::PrincipalId::new("s3-test").unwrap(),
            extensions: neoengram_domain::protocol::Extensions::new(),
        };
        let pool = crate::GatewayPoolRecord {
            gateway_pool_id: gateway_pool_id.clone(),
            edge_cluster_id: edge_cluster_id.clone(),
            display_name: "S3 test pool".to_owned(),
            agent_endpoint: "https://agent.s3.test".to_owned(),
            s3_endpoint: Some("https://s3.test".to_owned()),
            desired_replicas: 3,
            minimum_ready_replicas: 2,
            state: GatewayPoolState::Ready,
            config_generation: neoengram_domain::protocol::Generation::new(1),
            resource_version: neoengram_domain::protocol::ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1),
            updated_at_unix_ms: UnixMillis::new(1),
            created_by: principal.clone(),
            updated_by: principal,
        };
        registry.insert_pool(pool.clone()).await.unwrap();
        let policy = Arc::new(
            StaticRbacPolicy::one_principal(
                "s3-test",
                ["*".to_owned()],
                std::iter::empty::<Permission>(),
            )
            .unwrap(),
        );
        let service = CatalogService::new(
            components.control_catalog,
            components.publisher,
            policy,
            components.clock,
        )
        .with_gateway_registry(registry.clone());

        let error = service
            .ensure_s3_gateway_pool_ready(&gateway_pool_id)
            .await
            .unwrap_err();
        assert_eq!(error.category(), ErrorCategory::Unavailable);
        assert_eq!(error.code().as_str(), "s3_snapshot_unavailable");

        install_active_s3_gateway_replica(
            &registry,
            &gateway_pool_id,
            &edge_cluster_id,
            "replica-s3-future",
            UnixMillis::new(NOW_UNIX_MS + 1),
        )
        .await;
        let error = service
            .ensure_s3_gateway_pool_ready(&gateway_pool_id)
            .await
            .unwrap_err();
        assert_eq!(error.category(), ErrorCategory::Unavailable);

        let heartbeat_at_freshness_boundary =
            UnixMillis::new(NOW_UNIX_MS - crate::AGENT_ROUTE_LEASE_MAX_TTL_MS);
        install_active_s3_gateway_replica(
            &registry,
            &gateway_pool_id,
            &edge_cluster_id,
            "replica-s3-a",
            heartbeat_at_freshness_boundary,
        )
        .await;
        let error = service
            .ensure_s3_gateway_pool_ready(&gateway_pool_id)
            .await
            .unwrap_err();
        assert_eq!(error.category(), ErrorCategory::Unavailable);

        let stale_heartbeat =
            UnixMillis::new(NOW_UNIX_MS - crate::AGENT_ROUTE_LEASE_MAX_TTL_MS - 1);
        let mut replica = install_active_s3_gateway_replica(
            &registry,
            &gateway_pool_id,
            &edge_cluster_id,
            "replica-s3-b",
            stale_heartbeat,
        )
        .await;
        let error = service
            .ensure_s3_gateway_pool_ready(&gateway_pool_id)
            .await
            .unwrap_err();
        assert_eq!(error.category(), ErrorCategory::Unavailable);

        replica.last_heartbeat_at_unix_ms = Some(UnixMillis::new(NOW_UNIX_MS));
        replica.resource_version = neoengram_domain::protocol::ResourceVersion::new(4);
        replica.updated_at_unix_ms = UnixMillis::new(NOW_UNIX_MS);
        registry.replace_replica(3, replica).await.unwrap();
        service
            .ensure_s3_gateway_pool_ready(&gateway_pool_id)
            .await
            .unwrap();

        let mut draining = pool;
        draining.state = GatewayPoolState::Draining;
        draining.resource_version = neoengram_domain::protocol::ResourceVersion::new(2);
        draining.updated_at_unix_ms = UnixMillis::new(NOW_UNIX_MS);
        registry.replace_pool(1, draining).await.unwrap();
        let error = service
            .ensure_s3_gateway_pool_ready(&gateway_pool_id)
            .await
            .unwrap_err();
        assert_eq!(error.category(), ErrorCategory::Unavailable);
        assert_eq!(error.code().as_str(), "s3_snapshot_unavailable");
    }

    #[derive(Default)]
    struct NoS3Placement;

    #[async_trait]
    impl S3PlacementProvider for NoS3Placement {
        async fn current_placement(
            &self,
            _tenant_id: &TenantId,
            _storage_volume_id: &StorageVolumeId,
        ) -> crate::CentralResult<Option<S3AgentPlacement>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn s3_read_route_rejects_partial_target_coverage() {
        let components = crate::InMemoryComponents::new(100_000);
        let tenant_id = TenantId::new("tenant-s3-coverage").unwrap();
        let project_id = ProjectId::new("project-s3-coverage").unwrap();
        let artifact_id = ArtifactId::new("artifact-s3-coverage").unwrap();
        let volume_id = StorageVolumeId::new("volume-s3-coverage").unwrap();
        let commit_id = CommitId::from_bytes([41; 32]);
        let first_object = ObjectId::from_bytes([42; 32]);
        let second_object = ObjectId::from_bytes([43; 32]);
        let object_set = ObjectSet::new(vec![
            CommitObject::new(first_object, 4, ObjectEncoding::Raw, 0),
            CommitObject::new(second_object, 6, ObjectEncoding::Raw, 1),
        ])
        .unwrap();
        components
            .placement
            .insert_commit_object_set(neoengram_domain::protocol::CommitObjectSet {
                tenant_id: tenant_id.clone(),
                commit_id,
                object_set: object_set.clone(),
            })
            .await
            .unwrap();
        components
            .placement
            .insert_object_placement_v2(ObjectPlacement {
                placement_id: neoengram_domain::protocol::PlacementId::new("placement-s3-coverage")
                    .unwrap(),
                tenant_id: tenant_id.clone(),
                object_namespace_id: ObjectNamespaceId::new(artifact_id.to_string()).unwrap(),
                object_id: first_object,
                size: neoengram_domain::protocol::DecimalU64::new(4),
                encoding: ObjectEncoding::Raw,
                verified_digest: first_object.digest(),
                storage_volume_id: Some(volume_id.clone()),
                archive_id: None,
                placement_generation: PlacementGeneration::new(1),
                state: ObjectPlacementState::Verified,
                failure_domain: "host-s3-coverage".to_owned(),
            })
            .await
            .unwrap();

        let policy = Arc::new(
            StaticRbacPolicy::one_principal(
                "s3-coverage-test",
                ["*".to_owned()],
                std::iter::empty::<Permission>(),
            )
            .unwrap(),
        );
        let service = CatalogService::new(
            components.control_catalog,
            components.publisher,
            policy,
            components.clock,
        )
        .with_placement_repository(components.placement)
        .with_s3_placement_provider(Arc::new(NoS3Placement))
        .with_gateway_registry(components.gateway_registry);
        let commit = CommitRecord {
            tenant_id: tenant_id.clone(),
            project_id,
            artifact_id,
            source_playground_id: PlaygroundId::new("playground-s3-coverage").unwrap(),
            source_precommit_id: PreCommitId::new("precommit-s3-coverage").unwrap(),
            commit_request_id: RequestId::new("request-s3-coverage").unwrap(),
            commit_id,
            object_set_digest: object_set.object_set_digest,
            root_directory_id: neoengram_domain::core::DirectoryId::from_bytes([44; 32]),
            parent_commit_id: None,
            index_version: WireIndexVersion {
                revision: IndexRevision::new(1),
                digest: ContentDigest::from_bytes([45; 32]),
                extensions: neoengram_domain::protocol::Extensions::new(),
            },
            data_layout: CommitDataLayout::FastCdc,
            records: Vec::new(),
            message: "S3 coverage gate".to_owned(),
            description: None,
            tag_names: Vec::new(),
            created_at_unix_ms: UnixMillis::new(100),
        };

        let error = service
            .resolve_s3_route_for_commit(&tenant_id, &commit, UnixMillis::new(100))
            .await
            .unwrap_err();
        assert_eq!(error.category(), ErrorCategory::Unavailable);
        assert_eq!(error.code().as_str(), "s3_snapshot_unavailable");
    }
}
