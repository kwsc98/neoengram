use std::sync::Arc;

use crate::{CentralError, CentralErrorCode};
use async_trait::async_trait;
use neoengram_domain::core::{Manifest, ManifestId, ObjectId};
use neoengram_domain::protocol::materialization::{
    MaterializationBatch, MaterializationJob, MaterializationJobKey, MaterializationObject,
    MaterializationObjectReceipt, ObjectPlacement as ObjectPlacementV2, ObjectReadLease,
    StagingLease, VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    AgentId, ArtifactId, JobAssignment, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchPage, ObjectReceiptId, PlacementGeneration, StorageVolumeId, TenantId, UnixMillis,
    WireIndexVersion,
};

use crate::{
    AcquireAgentRouteLeaseRequest, AcquireAgentSessionRouteRequest, AgentEnrollmentAuditEvent,
    AgentEnrollmentExpiryReconciliation, AgentEnrollmentLifecycleAuditEvent,
    AgentEnrollmentListPage, AgentEnrollmentListRequest, AgentRegistryRecord,
    AgentRegistryReplacementRecords, AgentRouteLease, AgentRouteLeaseAcquireOutcome,
    AgentRouteLeaseListRequest, AgentRouteLeaseMutationOutcome, AgentSessionRouteAcquireOutcome,
    ArtifactListPage, ArtifactListRequest, ArtifactRecord, AuditEvent, AuthorizationRequest,
    CatalogInsertOutcome, CentralResult, GatewayInsertOutcome, GatewayPoolListRequest,
    GatewayPoolRecord, GatewayReplicaListRequest, GatewayReplicaRecord, IndexKey,
    IndexPublishOutcome, IndexPublishRequest, InitializeIndexSnapshotRequest, JobKey, JobRecord,
    ObjectPlacementEvidence, PlaygroundInsertRequest, PlaygroundListPage, PlaygroundListRequest,
    PlaygroundRecord, PreCommitCancelRequest, PreCommitCommitOutcome, PreCommitCommitRequest,
    PreCommitKey, PreCommitMutationOutcome, PreCommitRecord, PreCommitRestartRequest,
    PreCommitStartRequest, ProjectListPage, ProjectListRequest, ProjectRecord, PublishedIndex,
    ReleaseAgentRouteLeaseRequest, RenewAgentRouteLeaseRequest, StagedMetadataBatch,
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

/// Counts materialization protection leases transitioned to `Expired` by one authority sweep.
///
/// Lease expiry is an authority concern rather than an Agent liveness guess: the sweep records a
/// durable terminal state which a later GC pass can use when deciding whether a source object or
/// staging key is still protected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaterializationLeaseExpiryReconciliation {
    pub expired_object_read_leases: usize,
    pub expired_staging_leases: usize,
}

/// Complete durable state for one materialization plan publication.
///
/// A plan is intentionally represented as one aggregate at the repository boundary.  The
/// Central planner may build it in memory, but the authority must publish the parent Job, child
/// Objects/Batches, protection Leases, and derived Coverage in one visibility/durability
/// boundary.  This prevents a process crash from exposing a Job with only a prefix of its plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationPlan {
    pub job: MaterializationJob,
    pub batches: Vec<MaterializationBatch>,
    pub objects: Vec<MaterializationObject>,
    pub object_read_leases: Vec<ObjectReadLease>,
    pub staging_leases: Vec<StagingLease>,
    pub coverage: VolumeCommitCoverage,
}

/// Result of publishing a materialization plan.  An exact replay returns `Existing`; conflicting
/// idempotency keys are rejected by the repository rather than silently selecting another plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializationPlanInsertOutcome {
    Inserted(MaterializationJob),
    Existing(MaterializationJob),
}

/// CAS request for replacing one materialization plan during retry/replanning. The old plan
/// remains readable for audit, but its active batches and protection leases are retired in the
/// same transaction that publishes this next revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationPlanReplacement {
    pub expected_plan_revision: neoengram_domain::protocol::Generation,
    pub plan: MaterializationPlan,
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
        enrollment_id: &neoengram_domain::protocol::AgentEnrollmentId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_for_tenant(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        enrollment_id: &neoengram_domain::protocol::AgentEnrollmentId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn list_for_tenant(
        &self,
        request: &AgentEnrollmentListRequest,
    ) -> CentralResult<AgentEnrollmentListPage>;
    async fn get_by_agent(
        &self,
        agent_id: &neoengram_domain::protocol::AgentId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_token_request_id(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        token_request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_token_digest(
        &self,
        token_digest: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_bootstrap_request_id(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        bootstrap_request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_decision_request_id(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        decision_request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_installation_id(
        &self,
        installation_id: &neoengram_domain::protocol::AgentInstallationId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_by_public_key_fingerprint(
        &self,
        public_key_fingerprint: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_current_by_volume(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
    ) -> CentralResult<Option<AgentRegistryRecord>>;
    async fn get_pvc_binding(
        &self,
        edge_cluster_id: &neoengram_domain::protocol::EdgeClusterId,
        pvc_identity_digest: &neoengram_domain::protocol::PvcIdentityDigest,
    ) -> CentralResult<Option<crate::PvcVolumeBinding>>;
    async fn expire_stale_token_intents(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
        edge_cluster_id: &neoengram_domain::protocol::EdgeClusterId,
        pvc_identity_digest: &neoengram_domain::protocol::PvcIdentityDigest,
        now_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<usize>;
    async fn expire_stale_review_enrollments(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
        now_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<usize>;
    /// Atomically expires every stale token intent and pending review in the registry.
    async fn reconcile_expired_enrollments(
        &self,
        now_unix_ms: neoengram_domain::protocol::UnixMillis,
    ) -> CentralResult<AgentEnrollmentExpiryReconciliation>;
    /// Returns immutable decision audit events persisted in the same CAS aggregates.
    async fn enrollment_audit_events(&self) -> CentralResult<Vec<AgentEnrollmentAuditEvent>>;
    /// Returns lifecycle events persisted atomically with each enrollment state transition.
    async fn enrollment_lifecycle_audit_events(
        &self,
        tenant_id: &neoengram_domain::protocol::TenantId,
    ) -> CentralResult<Vec<AgentEnrollmentLifecycleAuditEvent>>;
    /// Atomically consumes a strictly increasing bootstrap-status signature timestamp.
    ///
    /// This is an authentication replay watermark, not an aggregate domain mutation: successful
    /// consumption must not advance the public ResourceVersion or enrollment update time.
    async fn consume_bootstrap_status_signed_at(
        &self,
        enrollment_id: &neoengram_domain::protocol::AgentEnrollmentId,
        signed_at_unix_ms: neoengram_domain::protocol::UnixMillis,
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

    /// Atomically revokes the current Agent enrollment and every runtime owner fence for a
    /// permanently deleted StorageVolume. Exact lifecycle replays return the original result.
    async fn revoke_volume_for_lifecycle(
        &self,
        _request: crate::RevokeVolumeForLifecycleRequest,
    ) -> CentralResult<crate::RevokeVolumeForLifecycleOutcome> {
        Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "this Agent registry backend does not support Volume lifecycle revocation",
        )
        .with_retryable(false))
    }
}

/// Central-authoritative Gateway resources and Agent route-generation watermarks.
#[async_trait]
pub trait GatewayRegistryRepository: Send + Sync {
    async fn get_pool(
        &self,
        gateway_pool_id: &neoengram_domain::protocol::GatewayPoolId,
    ) -> CentralResult<Option<GatewayPoolRecord>>;
    async fn get_pool_by_edge_cluster(
        &self,
        edge_cluster_id: &neoengram_domain::protocol::EdgeClusterId,
    ) -> CentralResult<Option<GatewayPoolRecord>>;
    async fn list_pools(
        &self,
        request: &GatewayPoolListRequest,
    ) -> CentralResult<Vec<GatewayPoolRecord>>;
    async fn insert_pool(
        &self,
        record: GatewayPoolRecord,
    ) -> CentralResult<GatewayInsertOutcome<GatewayPoolRecord>>;
    async fn replace_pool(
        &self,
        expected_resource_version: u64,
        record: GatewayPoolRecord,
    ) -> CentralResult<GatewayPoolRecord>;

    async fn get_replica(
        &self,
        gateway_replica_id: &neoengram_domain::protocol::GatewayReplicaId,
    ) -> CentralResult<Option<GatewayReplicaRecord>>;
    async fn get_replica_by_activation_token_digest(
        &self,
        token_digest: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Option<GatewayReplicaRecord>>;
    async fn list_replicas(
        &self,
        request: &GatewayReplicaListRequest,
    ) -> CentralResult<Vec<GatewayReplicaRecord>>;
    async fn insert_replica(
        &self,
        record: GatewayReplicaRecord,
    ) -> CentralResult<GatewayInsertOutcome<GatewayReplicaRecord>>;
    async fn replace_replica(
        &self,
        expected_resource_version: u64,
        record: GatewayReplicaRecord,
    ) -> CentralResult<GatewayReplicaRecord>;

    async fn get_agent_route(
        &self,
        agent_id: &neoengram_domain::protocol::AgentId,
    ) -> CentralResult<Option<AgentRouteLease>>;
    async fn list_agent_routes(
        &self,
        request: &AgentRouteLeaseListRequest,
    ) -> CentralResult<Vec<AgentRouteLease>>;
    async fn acquire_agent_route(
        &self,
        request: AcquireAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseAcquireOutcome>;
    /// Opens the Agent session and acquires its route in one repository transaction.
    async fn acquire_agent_session_route(
        &self,
        request: AcquireAgentSessionRouteRequest,
    ) -> CentralResult<AgentSessionRouteAcquireOutcome>;
    async fn renew_agent_route(
        &self,
        request: RenewAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseMutationOutcome>;
    async fn release_agent_route(
        &self,
        request: ReleaseAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseMutationOutcome>;
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

    async fn get_project(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
    ) -> CentralResult<Option<ProjectRecord>>;
    async fn list_projects(&self, request: &ProjectListRequest) -> CentralResult<ProjectListPage>;
    async fn insert_project(
        &self,
        record: ProjectRecord,
    ) -> CentralResult<CatalogInsertOutcome<ProjectRecord>>;

    async fn get_artifact(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &neoengram_domain::protocol::ArtifactId,
    ) -> CentralResult<Option<ArtifactRecord>>;
    /// Internal lifecycle lookup which remains available after the public resource is fenced.
    async fn get_artifact_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &neoengram_domain::protocol::ArtifactId,
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
        storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
    ) -> CentralResult<Option<StorageVolumeRecord>>;
    /// Internal lifecycle lookup which remains available after the public resource is fenced.
    async fn get_storage_volume_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &neoengram_domain::protocol::StorageVolumeId,
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
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &neoengram_domain::protocol::ArtifactId,
        playground_id: &neoengram_domain::protocol::PlaygroundId,
    ) -> CentralResult<Option<PlaygroundRecord>>;
    /// Internal lifecycle lookup which remains available after the public resource is fenced.
    async fn get_playground_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &neoengram_domain::protocol::ArtifactId,
        playground_id: &neoengram_domain::protocol::PlaygroundId,
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
    #[allow(clippy::too_many_arguments)]
    async fn transition_playground_state(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &neoengram_domain::protocol::ArtifactId,
        playground_id: &neoengram_domain::protocol::PlaygroundId,
        expected: crate::PlaygroundState,
        next: crate::PlaygroundState,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<PlaygroundRecord>;

    /// Atomically advances the fenced Playground Head and the Artifact convenience Head for one
    /// committed Playground. An exact replay already observed by the Playground succeeds without
    /// moving an Artifact Head that another branch may have advanced meanwhile.
    async fn advance_playground_commit(
        &self,
        request: crate::AdvancePlaygroundCommitRequest,
    ) -> CentralResult<crate::AdvancePlaygroundCommitOutcome>;

    async fn get_snapshot(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &neoengram_domain::protocol::SnapshotId,
    ) -> CentralResult<Option<crate::SnapshotRecord>>;
    /// Internal lifecycle lookup which remains available after the public resource is fenced.
    async fn get_snapshot_for_lifecycle(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &neoengram_domain::protocol::SnapshotId,
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

    /// Durable read-only projection records for a Snapshot. Implementations that predate the
    /// delivery catalog fail closed instead of silently treating a request as an in-memory mount.
    async fn get_snapshot_delivery(
        &self,
        tenant_id: &TenantId,
        delivery_id: &neoengram_domain::protocol::SnapshotDeliveryId,
    ) -> CentralResult<Option<crate::SnapshotDeliveryRecord>> {
        let _ = (tenant_id, delivery_id);
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery catalog is not configured",
        ))
    }

    async fn get_snapshot_delivery_by_create_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<crate::SnapshotDeliveryRecord>> {
        let _ = (tenant_id, request_id);
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery catalog is not configured",
        ))
    }

    async fn list_snapshot_deliveries(
        &self,
        request: &crate::SnapshotDeliveryListRequest,
    ) -> CentralResult<Vec<crate::SnapshotDeliveryRecord>> {
        let _ = request;
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery catalog is not configured",
        ))
    }

    async fn insert_snapshot_delivery_idempotent(
        &self,
        request: crate::SnapshotDeliveryInsertRequest,
    ) -> CentralResult<crate::SnapshotDeliveryInsertOutcome> {
        let _ = request;
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery catalog is not configured",
        ))
    }

    async fn replace_snapshot_delivery(
        &self,
        expected_resource_version: u64,
        record: crate::SnapshotDeliveryRecord,
    ) -> CentralResult<crate::SnapshotDeliveryRecord> {
        let _ = (expected_resource_version, record);
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery catalog is not configured",
        ))
    }

    async fn insert_snapshot_delivery_retention_roots(
        &self,
        roots: &[crate::SnapshotDeliveryRetentionRoot],
    ) -> CentralResult<()> {
        let _ = roots;
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery retention roots are not configured",
        ))
    }

    async fn list_snapshot_delivery_retention_roots(
        &self,
        tenant_id: &TenantId,
        delivery_id: &neoengram_domain::protocol::SnapshotDeliveryId,
    ) -> CentralResult<Vec<crate::SnapshotDeliveryRetentionRoot>> {
        let _ = (tenant_id, delivery_id);
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery retention roots are not configured",
        ))
    }

    async fn release_snapshot_delivery_retention_roots(
        &self,
        tenant_id: &TenantId,
        delivery_id: &neoengram_domain::protocol::SnapshotDeliveryId,
    ) -> CentralResult<()> {
        let _ = (tenant_id, delivery_id);
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery retention roots are not configured",
        ))
    }

    async fn get_snapshot_delivery_mutation(
        &self,
        tenant_id: &TenantId,
        request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<crate::SnapshotDeliveryMutationRecord>> {
        let _ = (tenant_id, request_id);
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery mutation receipts are not configured",
        ))
    }

    /// Atomically applies a Delivery CAS transition and persists its idempotency receipt. An
    /// unchanged desired record persists a no-op receipt without advancing ResourceVersion.
    async fn apply_snapshot_delivery_mutation_idempotent(
        &self,
        _request: crate::SnapshotDeliveryMutationRequest,
    ) -> CentralResult<crate::CatalogInsertOutcome<crate::SnapshotDeliveryMutationRecord>> {
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "SnapshotDelivery mutation receipts are not configured",
        ))
    }

    async fn get_s3_access_point(
        &self,
        tenant_id: &TenantId,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
    ) -> CentralResult<Option<crate::S3AccessPointRecord>>;
    async fn get_s3_access_point_by_snapshot(
        &self,
        tenant_id: &TenantId,
        snapshot_id: &neoengram_domain::protocol::SnapshotId,
    ) -> CentralResult<Option<crate::S3AccessPointRecord>>;
    async fn get_s3_access_point_by_bucket(
        &self,
        bucket_name: &str,
    ) -> CentralResult<Option<crate::S3AccessPointRecord>>;
    async fn list_s3_access_points(
        &self,
        request: &crate::S3AccessPointListRequest,
    ) -> CentralResult<crate::S3AccessPointListPage>;
    async fn insert_s3_access_point(
        &self,
        record: crate::S3AccessPointRecord,
    ) -> CentralResult<crate::S3AccessPointInsertOutcome>;
    async fn get_s3_mutation(
        &self,
        tenant_id: &TenantId,
        request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<crate::S3MutationRecord>>;
    /// Atomically commits an Access Point, its bootstrap credential, and the public request
    /// identity. Exact replays return `Existing`; conflicting request identity reuse fails.
    async fn create_s3_access_point_idempotent(
        &self,
        mutation: crate::S3MutationRecord,
        access_point: crate::S3AccessPointRecord,
        credential: crate::S3CredentialRecord,
    ) -> CentralResult<crate::CatalogInsertOutcome<crate::S3AccessPointCreateResult>>;
    /// Atomically applies one enable/disable request, including credential fencing, and records
    /// its public request identity.
    async fn update_s3_access_point_state_idempotent(
        &self,
        mutation: crate::S3MutationRecord,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        state: crate::S3AccessPointState,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<crate::CatalogInsertOutcome<crate::S3AccessPointRecord>>;
    async fn update_s3_access_point_state(
        &self,
        tenant_id: &TenantId,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        state: crate::S3AccessPointState,
        policy_generation: u64,
        updated_at_unix_ms: UnixMillis,
    ) -> CentralResult<crate::S3AccessPointRecord>;
    async fn list_s3_credentials(
        &self,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
    ) -> CentralResult<Vec<crate::S3CredentialRecord>>;
    async fn get_s3_credential_by_access_key(
        &self,
        access_key_id: &str,
    ) -> CentralResult<Option<crate::S3CredentialRecord>>;
    async fn insert_s3_credential(
        &self,
        record: crate::S3CredentialRecord,
    ) -> CentralResult<crate::S3CredentialInsertOutcome>;
    /// Atomically commits a credential and the public create request identity.
    async fn create_s3_credential_idempotent(
        &self,
        mutation: crate::S3MutationRecord,
        credential: crate::S3CredentialRecord,
    ) -> CentralResult<crate::CatalogInsertOutcome<crate::S3CredentialRecord>>;
    /// Atomically revokes an owned credential and records the public revoke request identity.
    async fn revoke_s3_credential_idempotent(
        &self,
        mutation: crate::S3MutationRecord,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        credential_id: &neoengram_domain::protocol::S3CredentialId,
    ) -> CentralResult<crate::CatalogInsertOutcome<crate::S3CredentialRecord>>;
    async fn update_s3_credential_state(
        &self,
        credential_id: &neoengram_domain::protocol::S3CredentialId,
        state: crate::S3CredentialState,
    ) -> CentralResult<crate::S3CredentialRecord>;
    async fn update_s3_credential_last_used(
        &self,
        credential_id: &neoengram_domain::protocol::S3CredentialId,
        last_used_at_unix_ms: UnixMillis,
    ) -> CentralResult<crate::S3CredentialRecord>;
    async fn expire_s3_credentials(
        &self,
        access_point_id: &neoengram_domain::protocol::S3AccessPointId,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<u64>;

    /// Builds and persists a short-lived impact view from the current catalog dependency graph.
    async fn query_deletion_impact(
        &self,
        request: crate::DeletionImpactQuery,
    ) -> CentralResult<crate::DeletionImpactRecord>;
    async fn get_deletion_operation(
        &self,
        tenant_id: &TenantId,
        deletion_id: &neoengram_domain::protocol::DeletionId,
    ) -> CentralResult<Option<neoengram_domain::protocol::DeletionOperation>>;
    async fn list_deletion_operations(
        &self,
        request: &crate::DeletionListRequest,
    ) -> CentralResult<crate::DeletionListPage>;
    /// Linearization point for a delete request. Implementations must revalidate the impact,
    /// fence every target, disable related S3 access, and persist the operation atomically.
    async fn create_deletion_idempotent(
        &self,
        request: crate::CreateDeletionRequest,
    ) -> CentralResult<crate::CatalogInsertOutcome<neoengram_domain::protocol::DeletionOperation>>;
    /// Begins an all-or-nothing restore by fencing every target in `restoring`.
    async fn restore_deletion_idempotent(
        &self,
        request: crate::RestoreDeletionRequest,
    ) -> CentralResult<crate::CatalogInsertOutcome<neoengram_domain::protocol::DeletionOperation>>;
    async fn retry_deletion_idempotent(
        &self,
        request: crate::RetryDeletionRequest,
    ) -> CentralResult<crate::CatalogInsertOutcome<neoengram_domain::protocol::DeletionOperation>>;
    /// Advances the durable saga. Entering purge is retention-gated; completing restore or purge
    /// atomically finalizes every target lifecycle.
    async fn transition_deletion_state(
        &self,
        request: crate::DeletionTransitionRequest,
    ) -> CentralResult<neoengram_domain::protocol::DeletionOperation>;
    async fn list_retention_holds(
        &self,
        tenant_id: &TenantId,
        deletion_id: &neoengram_domain::protocol::DeletionId,
    ) -> CentralResult<Vec<neoengram_domain::protocol::RetentionHold>>;
    async fn create_retention_hold_idempotent(
        &self,
        request: crate::CreateRetentionHoldRequest,
    ) -> CentralResult<crate::CatalogInsertOutcome<neoengram_domain::protocol::RetentionHold>>;
    async fn release_retention_hold_idempotent(
        &self,
        request: crate::ReleaseRetentionHoldRequest,
    ) -> CentralResult<crate::CatalogInsertOutcome<neoengram_domain::protocol::RetentionHold>>;
    async fn append_lifecycle_evidence(
        &self,
        tenant_id: &TenantId,
        deletion_id: &neoengram_domain::protocol::DeletionId,
        batch: crate::LifecycleEvidenceBatch,
    ) -> CentralResult<()>;
    async fn enqueue_lifecycle_assignment(
        &self,
        record: crate::LifecycleAssignmentOutboxRecord,
    ) -> CentralResult<crate::LifecycleAssignmentInsertOutcome>;
    async fn get_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<Option<crate::LifecycleAssignmentOutboxRecord>>;
    /// Lists published, non-retired lifecycle commands for one Agent in stable scope order.
    async fn pending_lifecycle_assignments_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<crate::LifecycleAssignmentOutboxRecord>>;
    /// Makes a reserved lifecycle command visible to Agent delivery. Replays are idempotent.
    async fn publish_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<crate::LifecycleAssignmentOutboxRecord>;
    /// Permanently retires a published lifecycle command after its acknowledgement boundary.
    async fn retire_lifecycle_assignment(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
    ) -> CentralResult<crate::LifecycleAssignmentOutboxRecord>;
    /// Records the first terminal Agent report digest. Different terminal payloads for the same
    /// assignment are rejected, while an exact replay returns the existing row.
    async fn record_lifecycle_report(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_domain::protocol::LifecycleAssignmentId,
        report_digest: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<crate::LifecycleAssignmentOutboxRecord>;
}

/// Durable Placement-first authority. This port deliberately stores only immutable metadata and
/// transfer/workspace state; object bytes remain owned by Agent/Gateway data-plane backends.
#[async_trait]
pub trait PlacementRepository: Send + Sync {
    /// Inserts one namespace-scoped v2 object fact.  Object bytes are never accepted by this
    /// repository; the Agent receipt has already completed its durability barrier.
    async fn insert_object_placement_v2(
        &self,
        placement: ObjectPlacementV2,
    ) -> CentralResult<ObjectPlacementV2>;
    /// Lists readable and historical v2 placements for one exact namespace/object identity.
    async fn object_placements_v2(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        object_id: &ObjectId,
    ) -> CentralResult<Vec<ObjectPlacementV2>>;
    /// Replaces the recomputable Coverage summary at one Volume/generation.  Implementations
    /// reject metadata that does not match the referenced Commit ObjectSet.
    async fn upsert_volume_commit_coverage(
        &self,
        coverage: VolumeCommitCoverage,
    ) -> CentralResult<VolumeCommitCoverage>;
    async fn volume_commit_coverages(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<VolumeCommitCoverage>>;
    /// Inserts a user-visible target materialization.  The idempotency key is the immutable
    /// `(tenant, namespace, commit, target Volume, coverage goal)` carried by the Job itself.
    async fn insert_materialization(
        &self,
        job: MaterializationJob,
    ) -> CentralResult<MaterializationJob>;
    /// Atomically publishes a complete Job/Batch/Object/Lease/Coverage aggregate.  The operation
    /// is idempotent: an exact replay returns `Existing`, while any conflicting child or parent
    /// metadata aborts the whole operation without exposing a partial plan.
    async fn insert_materialization_plan(
        &self,
        plan: MaterializationPlan,
    ) -> CentralResult<MaterializationPlanInsertOutcome>;
    /// Atomically advances a plan revision and publishes its replacement children. The expected
    /// revision is a CAS fence; stale retries are rejected before any old child is retired.
    async fn replace_materialization_plan(
        &self,
        request: MaterializationPlanReplacement,
    ) -> CentralResult<MaterializationPlanInsertOutcome>;
    async fn get_materialization(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Option<MaterializationJob>>;
    async fn get_materialization_by_key(
        &self,
        key: &MaterializationJobKey,
    ) -> CentralResult<Option<MaterializationJob>>;
    /// Lists materializations for one exact namespace/Commit.  Supplying a target Volume narrows
    /// the result to the idempotency scope used by the planner; results are stable by job ID.
    async fn list_materializations(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        commit_id: &neoengram_domain::core::ContentDigest,
        target_storage_volume_id: Option<&StorageVolumeId>,
    ) -> CentralResult<Vec<MaterializationJob>>;
    /// CAS update for plan/state changes. `expected_plan_revision` fences stale planners and old
    /// Batch reports. State-only updates may retain the same revision; a replan increments it.
    async fn replace_materialization(
        &self,
        tenant_id: &TenantId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
        expected_plan_revision: neoengram_domain::protocol::Generation,
        job: MaterializationJob,
    ) -> CentralResult<MaterializationJob>;
    async fn insert_materialization_batch(
        &self,
        batch: MaterializationBatch,
    ) -> CentralResult<MaterializationBatch>;
    /// Fenced state/attempt update for one materialization Batch.
    async fn replace_materialization_batch(
        &self,
        request: crate::MaterializationBatchCasRequest,
    ) -> CentralResult<MaterializationBatch>;
    async fn list_materialization_batches(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Vec<MaterializationBatch>>;
    /// Lists non-terminal v2 batches currently assigned to one target Agent.  The result is
    /// tenant-scoped and deterministic; callers use it to recover work after an Agent reconnects
    /// without exposing or accepting a stale batch from another target.
    async fn list_active_materialization_batches_for_agent(
        &self,
        tenant_id: &TenantId,
        agent_id: &AgentId,
    ) -> CentralResult<Vec<MaterializationBatch>>;
    async fn insert_materialization_object(
        &self,
        tenant_id: &TenantId,
        object: MaterializationObject,
    ) -> CentralResult<MaterializationObject>;
    /// Fenced update for one object checkpoint.  Replays with the same plan/attempt and payload
    /// are idempotent; stale reports cannot overwrite a newer source/route assignment.
    async fn replace_materialization_object(
        &self,
        request: crate::MaterializationObjectCasRequest,
    ) -> CentralResult<MaterializationObject>;
    async fn list_materialization_objects(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Vec<MaterializationObject>>;
    /// Looks up a durable receipt identity without applying a live Batch/route fence.
    ///
    /// The control plane uses this idempotency probe before checking the current Agent session.
    /// An exact replay is safe after reconnect because its receipt and Placement are already
    /// durable; a new receipt still goes through the live fences in
    /// `record_materialization_receipt`.
    async fn get_materialization_receipt(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        receipt_id: &ObjectReceiptId,
    ) -> CentralResult<Option<MaterializationObjectReceipt>>;
    async fn insert_object_read_lease(
        &self,
        lease: ObjectReadLease,
    ) -> CentralResult<ObjectReadLease>;
    async fn insert_staging_lease(&self, lease: StagingLease) -> CentralResult<StagingLease>;
    /// Atomically marks active leases whose TTL has elapsed as `Expired`.  Released and already
    /// expired history is left unchanged, so repeated sweeps are idempotent.
    async fn reconcile_materialization_leases(
        &self,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<MaterializationLeaseExpiryReconciliation>;
    /// Records one durability-barrier receipt and publishes exactly one Verified ObjectPlacement.
    /// Receipt identity is durable and an exact replay returns the original Placement.
    async fn record_materialization_receipt(
        &self,
        request: crate::MaterializationReceiptRequest,
    ) -> CentralResult<ObjectPlacementV2> {
        let receipt = request.receipt;
        receipt
            .validate_against(&request.object)
            .map_err(CentralError::from)?;
        let placement_id =
            crate::placement_authority::materialization_target_placement_id(&receipt)?;
        if let Some(existing) = self
            .object_placements_v2(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.object_id,
            )
            .await?
            .into_iter()
            .find(|placement| placement.placement_id == placement_id)
        {
            if existing.storage_volume_id.as_ref() == Some(&receipt.target_storage_volume_id)
                && existing.placement_generation == receipt.target_placement_generation
                && existing.size == receipt.size
                && existing.encoding == receipt.encoding
                && existing.verified_digest == receipt.verified_digest
                && existing.readable()
            {
                return Ok(existing);
            }
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization receipt placement ID is already bound to different metadata",
            )
            .with_retryable(false));
        }
        let job = self
            .get_materialization(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if job.plan_revision != receipt.plan_revision
            || job.key.object_namespace_id != receipt.object_namespace_id
            || job.key.target_storage_volume_id != receipt.target_storage_volume_id
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt is stale or scoped to another target",
            ));
        }
        let batch = self
            .list_materialization_batches(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .into_iter()
            .find(|batch| batch.batch_id == receipt.batch_id)
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization batch not found",
                )
            })?;
        if batch.plan_revision != receipt.plan_revision
            || batch.batch_attempt != receipt.batch_attempt
            || batch.target.tenant_id != receipt.tenant_id
            || batch.target.object_namespace_id != receipt.object_namespace_id
            || batch.target.storage_volume_id != receipt.target_storage_volume_id
            || batch.target.placement_generation != receipt.target_placement_generation
            || !batch.object_ids.contains(&receipt.object_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt does not match the active batch fence",
            ));
        }
        let object_set = self
            .get_commit_object_set(&receipt.tenant_id, &job.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization Commit ObjectSet not found",
                )
            })?;
        let expected = object_set
            .object_set
            .objects
            .iter()
            .find(|object| object.object_id == receipt.object_id)
            .copied()
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "receipt object is not part of the Commit ObjectSet",
                )
            })?;
        if expected.object_id != request.object.object_id
            || expected.size.get() != request.object.size.get()
            || expected.encoding != request.object.encoding
            || expected.ordinal.get() != request.object.ordinal.get()
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "receipt ObjectRef disagrees with the Commit ObjectSet",
            ));
        }
        let current = self
            .list_materialization_objects(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .into_iter()
            .find(|task| {
                task.object.object_namespace_id == receipt.object_namespace_id
                    && task.object.object_id == receipt.object_id
            })
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization object not found",
                )
            })?;
        if current.plan_revision != receipt.plan_revision {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization object belongs to an obsolete plan",
            ));
        }
        if current.object != request.object {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization object metadata disagrees with the receipt ObjectRef",
            )
            .with_retryable(false));
        }
        if !current.complete()
            && !current.state.can_transition_to(
                neoengram_domain::protocol::materialization::MaterializationObjectState::Verified,
            )
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Object cannot be verified from its current state",
            )
            .with_retryable(false));
        }
        let placement = ObjectPlacementV2 {
            placement_id: placement_id.clone(),
            tenant_id: receipt.tenant_id.clone(),
            object_namespace_id: receipt.object_namespace_id.clone(),
            object_id: receipt.object_id,
            size: receipt.size,
            encoding: receipt.encoding,
            verified_digest: receipt.verified_digest,
            storage_volume_id: Some(receipt.target_storage_volume_id.clone()),
            archive_id: None,
            placement_generation: receipt.target_placement_generation,
            state: neoengram_domain::protocol::materialization::ObjectPlacementState::Verified,
            failure_domain: format!("volume:{}", receipt.target_storage_volume_id),
        };
        let stored_placement = self.insert_object_placement_v2(placement).await?;
        let mut next_object = current.clone();
        next_object.confirmed_offset = receipt.committed_offset;
        next_object.state =
            neoengram_domain::protocol::materialization::MaterializationObjectState::Verified;
        next_object.attempt = receipt.batch_attempt;
        self.replace_materialization_object(crate::MaterializationObjectCasRequest {
            tenant_id: receipt.tenant_id.clone(),
            object_namespace_id: receipt.object_namespace_id.clone(),
            materialization_id: receipt.materialization_id.clone(),
            object_id: receipt.object_id,
            expected_plan_revision: receipt.plan_revision,
            expected_attempt: current.attempt,
            object: next_object,
        })
        .await?;

        let tasks = self
            .list_materialization_objects(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?;
        let verified_objects = tasks.iter().filter(|task| task.complete()).count() as u64;
        let verified_bytes = tasks
            .iter()
            .filter(|task| task.complete())
            .map(|task| task.object.size.get())
            .sum::<u64>();
        let mut next_job = job.clone();
        next_job.verified_object_count =
            neoengram_domain::protocol::DecimalU64::new(verified_objects);
        next_job.verified_bytes = neoengram_domain::protocol::DecimalU64::new(verified_bytes);
        next_job.missing_object_count = neoengram_domain::protocol::DecimalU64::new(
            job.object_count.get().saturating_sub(verified_objects),
        );
        next_job.missing_bytes = neoengram_domain::protocol::DecimalU64::new(
            job.total_bytes.get().saturating_sub(verified_bytes),
        );
        next_job.state = if next_job.key.coverage_goal.satisfied_by(
            next_job.verified_object_count.get(),
            next_job.verified_bytes.get(),
            next_job.object_count.get(),
            next_job.total_bytes.get(),
        ) {
            neoengram_domain::protocol::materialization::MaterializationJobState::Complete
        } else {
            match job.state {
                neoengram_domain::protocol::materialization::MaterializationJobState::Queued
                | neoengram_domain::protocol::materialization::MaterializationJobState::Planning
                | neoengram_domain::protocol::materialization::MaterializationJobState::WaitingForSources
                | neoengram_domain::protocol::materialization::MaterializationJobState::Materializing
                | neoengram_domain::protocol::materialization::MaterializationJobState::Verifying =>
                    neoengram_domain::protocol::materialization::MaterializationJobState::Verifying,
                state => state,
            }
        };
        next_job.updated_at_unix_ms = neoengram_domain::protocol::UnixMillis::new(
            job.updated_at_unix_ms
                .get()
                .max(receipt.verified_at_unix_ms.get()),
        );
        self.replace_materialization(
            &receipt.tenant_id,
            &receipt.materialization_id,
            receipt.plan_revision,
            next_job,
        )
        .await?;

        let mut placements = Vec::new();
        for object in &object_set.object_set.objects {
            placements.extend(
                self.object_placements_v2(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &object.object_id,
                )
                .await?
                .into_iter()
                .filter(|placement| {
                    placement.storage_volume_id.as_ref() == Some(&receipt.target_storage_volume_id)
                        && placement.placement_generation == receipt.target_placement_generation
                }),
            );
        }
        let coverage =
            neoengram_domain::protocol::materialization::VolumeCommitCoverage::from_placements(
                receipt.tenant_id,
                receipt.object_namespace_id,
                job.key.commit_id,
                receipt.target_storage_volume_id,
                receipt.target_placement_generation,
                &object_set.object_set,
                &placements,
            )
            .map_err(CentralError::from)?;
        self.upsert_volume_commit_coverage(coverage).await?;
        Ok(stored_placement)
    }
    async fn release_object_read_lease(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        lease_id: &neoengram_domain::protocol::LeaseId,
    ) -> CentralResult<Option<ObjectReadLease>>;
    async fn release_staging_lease(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        lease_id: &neoengram_domain::protocol::LeaseId,
    ) -> CentralResult<Option<StagingLease>>;
    /// Returns the immutable object manifest required by a Commit, if it has been staged.
    async fn get_commit_object_set(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Option<neoengram_domain::protocol::CommitObjectSet>>;
    /// Inserts a Commit object set exactly once. Replays return the original set; conflicting
    /// metadata for the same tenant/Commit is rejected.
    async fn insert_commit_object_set(
        &self,
        object_set: neoengram_domain::protocol::CommitObjectSet,
    ) -> CentralResult<neoengram_domain::protocol::CommitObjectSet>;
    async fn get_placement_set(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
        backend_id: &neoengram_domain::protocol::BackendId,
    ) -> CentralResult<Option<neoengram_domain::protocol::CommitPlacementSet>>;
    /// Returns every published complete-object-set placement for a Commit in stable backend order.
    /// Readers may probe the returned candidates and fail over without persisting a source Volume
    /// on the logical Commit row.
    async fn published_placement_sets(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<neoengram_domain::protocol::CommitPlacementSet>>;
    /// Returns all PlacementSets for a Commit, including staged, retiring, and deleted records, in
    /// stable backend order. This is the authority source for version-management placement views.
    async fn commit_placement_sets(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<neoengram_domain::protocol::CommitPlacementSet>>;
    /// Returns the first published placement for callers that only need a deterministic default.
    async fn published_placement_set(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Option<neoengram_domain::protocol::CommitPlacementSet>> {
        Ok(self
            .published_placement_sets(tenant_id, commit_id)
            .await?
            .into_iter()
            .next())
    }
    /// Publishes or stages one complete-object-set fence. Publication validation is enforced by
    /// the domain model before it reaches the database.
    async fn insert_placement_set(
        &self,
        placement_set: neoengram_domain::protocol::CommitPlacementSet,
    ) -> CentralResult<neoengram_domain::protocol::CommitPlacementSet>;
    /// Publishes the first complete copy for a newly committed dataset. Implementations should
    /// persist the ObjectSet, verified object placements, and publication fence in one durable
    /// transaction; the default keeps lightweight in-memory adapters behaviorally equivalent.
    async fn publish_initial_placement(
        &self,
        object_set: neoengram_domain::protocol::CommitObjectSet,
        placements: Vec<neoengram_domain::protocol::ObjectPlacement>,
        placement_set: neoengram_domain::protocol::CommitPlacementSet,
    ) -> CentralResult<(
        neoengram_domain::protocol::CommitObjectSet,
        neoengram_domain::protocol::CommitPlacementSet,
    )> {
        let stored = self.insert_commit_object_set(object_set).await?;
        for placement in placements {
            self.insert_object_placement(placement).await?;
        }
        let published = self.insert_placement_set(placement_set).await?;
        Ok((stored, published))
    }
    async fn insert_object_placement(
        &self,
        placement: neoengram_domain::protocol::ObjectPlacement,
    ) -> CentralResult<neoengram_domain::protocol::ObjectPlacement>;
    /// Advances one Placement state without changing its immutable identity or generation.
    /// `Lost` is an explicit administrative declaration; ordinary Agent disconnects should use
    /// `Retiring`/`Deleted` and leave the logical Commit intact.
    async fn set_object_placement_state(
        &self,
        tenant_id: &TenantId,
        object_id: &neoengram_domain::core::ObjectId,
        backend_id: &neoengram_domain::protocol::BackendId,
        placement_generation: neoengram_domain::protocol::PlacementGeneration,
        state: neoengram_domain::protocol::PlacementState,
    ) -> CentralResult<neoengram_domain::protocol::ObjectPlacement>;
    async fn object_placements(
        &self,
        tenant_id: &TenantId,
        object_id: &neoengram_domain::core::ObjectId,
    ) -> CentralResult<Vec<neoengram_domain::protocol::ObjectPlacement>>;
    async fn get_replication(
        &self,
        tenant_id: &TenantId,
        replication_id: &neoengram_domain::protocol::ReplicationId,
    ) -> CentralResult<Option<crate::ReplicationRecord>>;
    async fn get_replication_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<crate::ReplicationRecord>>;
    async fn list_replications_for_commit(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<Vec<crate::ReplicationRecord>>;
    /// Lists replication attempts currently bound to one target Agent.  The result is used by
    /// the reverse Agent channel to derive a durable command delivery set without introducing a
    /// second, non-authoritative replication outbox.
    async fn list_replications_for_agent(
        &self,
        tenant_id: &TenantId,
        agent_id: &AgentId,
    ) -> CentralResult<Vec<crate::ReplicationRecord>>;
    /// Atomically refreshes the source and target session/route bindings for an active attempt.
    /// Agent and mount identities remain fenced by the expected bindings.
    async fn refresh_replication_routes(
        &self,
        request: crate::RefreshReplicationRoutesRequest,
    ) -> CentralResult<crate::ReplicationRecord>;
    async fn insert_replication(
        &self,
        record: crate::ReplicationRecord,
    ) -> CentralResult<crate::ReplicationRecord>;
    /// Advances an active Replication through its non-publication states with attempt fencing.
    async fn transition_replication(
        &self,
        request: crate::ReplicationStateTransitionRequest,
    ) -> CentralResult<crate::ReplicationRecord>;
    /// Starts the next attempt for a failed Replication while retaining durable object offsets.
    async fn retry_replication(
        &self,
        request: crate::RetryReplicationRequest,
    ) -> CentralResult<crate::RetryReplicationResult>;
    /// Cancels an active Replication attempt. Published data cannot be cancelled through this API.
    async fn cancel_replication(
        &self,
        request: crate::CancelReplicationRequest,
    ) -> CentralResult<crate::ReplicationRecord>;
    /// Atomically publishes verified ObjectPlacements and their complete PlacementSet, then marks
    /// the matching fenced Replication attempt Published.
    async fn finalize_replication(
        &self,
        request: crate::FinalizeReplicationRequest,
    ) -> CentralResult<crate::FinalizeReplicationResult>;
    /// Inserts or advances one object checkpoint. Replays with the same payload are idempotent;
    /// an offset may only move forward for the same replication/object identity.
    async fn upsert_replication_object(
        &self,
        record: crate::ReplicationObjectRecord,
    ) -> CentralResult<crate::ReplicationObjectRecord>;
    async fn list_replication_objects(
        &self,
        tenant_id: &TenantId,
        replication_id: &neoengram_domain::protocol::ReplicationId,
    ) -> CentralResult<Vec<crate::ReplicationObjectRecord>>;
    async fn get_workspace(
        &self,
        tenant_id: &TenantId,
        workspace_id: &neoengram_domain::protocol::WorkspaceId,
    ) -> CentralResult<Option<crate::WorkspaceRecord>>;
    async fn get_workspace_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<crate::WorkspaceRecord>>;
    async fn insert_workspace(
        &self,
        record: crate::WorkspaceRecord,
    ) -> CentralResult<crate::WorkspaceRecord>;
    async fn commit_availability(
        &self,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::core::ContentDigest,
    ) -> CentralResult<crate::CommitAvailabilityRecord>;
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
/// `commit` consumes a candidate and inserts its Commit in one authority transaction. Publishing
/// the source Playground Head and Artifact convenience Head remains a separate control-catalog
/// recovery boundary.
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
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &neoengram_domain::protocol::PlaygroundId,
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
    /// Returns the durable result for a prior restart idempotency key, when one exists.
    async fn find_restart_result(
        &self,
        tenant_id: &TenantId,
        restart_request_id: &neoengram_domain::protocol::RequestId,
    ) -> CentralResult<Option<PreCommitRecord>>;
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
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &ArtifactId,
        commit_id: neoengram_domain::core::CommitId,
    ) -> CentralResult<Option<crate::CommitRecord>>;
    /// Lists every published immutable Commit owned by one Artifact. A Commit is visible only
    /// after its source Pre-commit has acknowledged Head publication; callers define presentation
    /// order and pagination and must not limit the result to the current Artifact Head chain.
    async fn list_published_commits(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_domain::protocol::ProjectId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Vec<crate::CommitRecord>>;
    async fn acknowledge_head_publication(
        &self,
        key: &PreCommitKey,
        commit_id: neoengram_domain::core::CommitId,
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
        assignment_id: &neoengram_domain::protocol::AssignmentId,
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
        artifact_placement_id: &neoengram_domain::protocol::ArtifactPlacementId,
        placement_generation: PlacementGeneration,
        object_id: ObjectId,
    ) -> CentralResult<Option<ObjectPlacementEvidence>>;

    /// Returns every Volume carrying authenticated placement evidence for one Artifact.
    async fn artifact_placement_volumes(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
    ) -> CentralResult<Vec<StorageVolumeId>>;

    /// Returns retained Artifacts for which this Volume carries at least one object with no
    /// authenticated copy on another Volume. A non-empty result hard-blocks Volume deletion.
    async fn volume_unique_artifact_replicas(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Vec<ArtifactId>>;
}

/// Durable authority transaction boundary for lifecycle metadata.
#[async_trait]
pub trait AuthorityLifecycleRepository: Send + Sync {
    async fn impact(
        &self,
        tenant_id: &TenantId,
        target: &neoengram_domain::protocol::ResourceRef,
    ) -> CentralResult<crate::AuthorityLifecycleImpact>;
    async fn quiesce(
        &self,
        request: crate::AuthorityLifecycleRequest,
    ) -> CentralResult<crate::AuthorityLifecycleMutationOutcome>;
    async fn finalize(
        &self,
        request: crate::AuthorityLifecycleRequest,
    ) -> CentralResult<crate::AuthorityLifecycleMutationOutcome>;
    async fn get(
        &self,
        tenant_id: &TenantId,
        deletion_id: &neoengram_domain::protocol::DeletionId,
        target: &neoengram_domain::protocol::ResourceRef,
        action: crate::AuthorityLifecycleAction,
    ) -> CentralResult<Option<crate::AuthorityLifecycleRecord>>;
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

    /// Loads immutable Manifest content from the same authority namespace used for Index
    /// publication. Commit creation uses this to validate every file against its declared layout.
    async fn manifest(
        &self,
        _tenant_id: &TenantId,
        _artifact_id: &ArtifactId,
        _manifest_id: ManifestId,
    ) -> CentralResult<Option<Manifest>> {
        Err(crate::CentralError::new(
            crate::CentralErrorCode::InvalidState,
            "this authority backend does not expose immutable Manifests",
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
    gateway_registry: Option<Arc<dyn GatewayRegistryRepository>>,
    control_catalog: Option<Arc<dyn ControlCatalogRepository>>,
    authority_lifecycle: Option<Arc<dyn AuthorityLifecycleRepository>>,
    placement: Option<Arc<dyn PlacementRepository>>,
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
            gateway_registry: None,
            control_catalog: None,
            authority_lifecycle: None,
            placement: None,
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

    #[must_use]
    pub fn with_gateway_registry(mut self, registry: Arc<dyn GatewayRegistryRepository>) -> Self {
        self.gateway_registry = Some(registry);
        self
    }

    #[must_use]
    pub fn gateway_registry(&self) -> Option<Arc<dyn GatewayRegistryRepository>> {
        self.gateway_registry.clone()
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
    pub fn with_authority_lifecycle(
        mut self,
        repository: Arc<dyn AuthorityLifecycleRepository>,
    ) -> Self {
        self.authority_lifecycle = Some(repository);
        self
    }

    #[must_use]
    pub fn authority_lifecycle(&self) -> Option<Arc<dyn AuthorityLifecycleRepository>> {
        self.authority_lifecycle.clone()
    }

    #[must_use]
    pub fn with_placement(mut self, repository: Arc<dyn PlacementRepository>) -> Self {
        self.placement = Some(repository);
        self
    }

    #[must_use]
    pub fn placement(&self) -> Option<Arc<dyn PlacementRepository>> {
        self.placement.clone()
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
