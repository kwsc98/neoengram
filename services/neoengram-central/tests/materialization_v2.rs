use std::sync::Arc;

use neoengram_central::dto::QueryCommitCoverageRequest;
use neoengram_central::{
    AllowAllAuthorizer, AuthenticatedIdentity, AuthorityCapabilities, AuthorityStore,
    CatalogService, ControlCatalogRepository, ControlPlane, InMemoryComponents,
    InMemoryPlacementRepository, MaterializationPlan, MaterializationPlanInsertOutcome,
    MaterializationPlanReplacement, Permission, PlacementRepository, StaticRbacPolicy,
    TenantRecord,
};
use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
use neoengram_domain::protocol::materialization::{
    CoverageGoal, CoverageState, MaterializationBatch, MaterializationBatchState,
    MaterializationJob, MaterializationJobKey, MaterializationJobState, MaterializationObject,
    MaterializationObjectReceipt, MaterializationObjectState, MaterializationReport,
    MaterializationSource, MaterializationTarget, ObjectPlacement, ObjectPlacementState,
    ObjectReadLease, ObjectRef, VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    object_read_lease_id, staging_lease_id, AgentId, ArtifactId, DecimalU64, EdgeClusterId,
    GatewayPoolId, Generation, LeaseId, MountGeneration, ObjectEncoding, ObjectNamespaceId,
    ObjectReceiptId, PlacementGeneration, PlacementId, PrincipalKind, RouteGeneration,
    SessionGeneration, StorageVolumeId, TenantId, UnixMillis,
};
#[cfg(feature = "authority-sqlite")]
use sqlx::{sqlite::SqliteConnectOptions, Connection};

fn tenant_record(tenant_id: &TenantId) -> TenantRecord {
    TenantRecord {
        tenant_id: tenant_id.clone(),
        display_name: "Tenant".to_owned(),
        description: None,
        resource_version: 1,
        created_at_unix_ms: UnixMillis::new(1),
        updated_at_unix_ms: UnixMillis::new(1),
    }
}

fn object_set() -> neoengram_domain::protocol::CommitObjectSet {
    neoengram_domain::protocol::CommitObjectSet {
        tenant_id: TenantId::new("tenant-v2").unwrap(),
        commit_id: CommitId::from_bytes([9; 32]),
        object_set: neoengram_domain::protocol::ObjectSet::new(vec![
            neoengram_domain::protocol::CommitObject::new(
                ObjectId::from_bytes([1; 32]),
                4,
                ObjectEncoding::Raw,
                0,
            ),
            neoengram_domain::protocol::CommitObject::new(
                ObjectId::from_bytes([2; 32]),
                6,
                ObjectEncoding::Raw,
                1,
            ),
        ])
        .unwrap(),
    }
}

fn placement(object_id: ObjectId, id: &str, volume: &str) -> ObjectPlacement {
    ObjectPlacement {
        placement_id: PlacementId::new(id).unwrap(),
        tenant_id: TenantId::new("tenant-v2").unwrap(),
        object_namespace_id: ObjectNamespaceId::new("artifact-v2").unwrap(),
        object_id,
        size: DecimalU64::new(if object_id == ObjectId::from_bytes([1; 32]) {
            4
        } else {
            6
        }),
        encoding: ObjectEncoding::Raw,
        verified_digest: object_id.digest(),
        storage_volume_id: Some(StorageVolumeId::new(volume).unwrap()),
        archive_id: None,
        placement_generation: PlacementGeneration::new(1),
        state: ObjectPlacementState::Verified,
        failure_domain: format!("host-{volume}"),
    }
}

fn job() -> MaterializationJob {
    MaterializationJob {
        materialization_id: neoengram_domain::protocol::MaterializationId::new(
            "materialization-v2",
        )
        .unwrap(),
        key: MaterializationJobKey {
            tenant_id: TenantId::new("tenant-v2").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-v2").unwrap(),
            commit_id: CommitId::from_bytes([9; 32]),
            target_storage_volume_id: StorageVolumeId::new("volume-target").unwrap(),
            coverage_goal: CoverageGoal::Complete,
        },
        artifact_id: ArtifactId::new("artifact-v2").unwrap(),
        state: MaterializationJobState::Queued,
        plan_revision: Generation::new(1),
        object_count: DecimalU64::new(2),
        total_bytes: DecimalU64::new(10),
        verified_object_count: DecimalU64::new(0),
        verified_bytes: DecimalU64::new(0),
        missing_object_count: DecimalU64::new(2),
        missing_bytes: DecimalU64::new(10),
        source_count: DecimalU64::new(2),
        created_at_unix_ms: UnixMillis::new(1),
        updated_at_unix_ms: UnixMillis::new(1),
        deadline_unix_ms: UnixMillis::new(100),
        issue: None,
    }
}

fn materialization_object() -> MaterializationObject {
    let mut object = MaterializationObject::new(
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap(),
        ObjectRef::new(
            ObjectNamespaceId::new("artifact-v2").unwrap(),
            ObjectId::from_bytes([1; 32]),
            4,
            ObjectEncoding::Raw,
            0,
        ),
        Generation::new(1),
    );
    object.state = MaterializationObjectState::Reserved;
    object.primary_source = Some(PlacementId::new("source-placement-v2").unwrap());
    object.current_batch_id =
        Some(neoengram_domain::protocol::MaterializationBatchId::new("batch-v2").unwrap());
    object
}

fn materialization_batch() -> MaterializationBatch {
    MaterializationBatch {
        batch_id: neoengram_domain::protocol::MaterializationBatchId::new("batch-v2").unwrap(),
        materialization_id: neoengram_domain::protocol::MaterializationId::new(
            "materialization-v2",
        )
        .unwrap(),
        plan_revision: Generation::new(1),
        batch_attempt: Generation::new(1),
        source: MaterializationSource {
            placement_id: PlacementId::new("source-placement-v2").unwrap(),
            tenant_id: TenantId::new("tenant-v2").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-v2").unwrap(),
            storage_volume_id: Some(StorageVolumeId::new("volume-source").unwrap()),
            archive_id: None,
            agent_id: AgentId::new("agent-source").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-source").unwrap(),
            gateway_pool_id: GatewayPoolId::new("gateway-source").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            route_generation: RouteGeneration::new(1),
        },
        target: MaterializationTarget {
            tenant_id: TenantId::new("tenant-v2").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-v2").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-target").unwrap(),
            agent_id: AgentId::new("agent-target").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-target").unwrap(),
            gateway_pool_id: GatewayPoolId::new("gateway-target").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            route_generation: RouteGeneration::new(1),
        },
        manifest_digest: ContentDigest::from_bytes([7; 32]),
        object_ids: vec![ObjectId::from_bytes([1; 32])],
        object_count: DecimalU64::new(1),
        total_bytes: DecimalU64::new(4),
        state: MaterializationBatchState::Transferring,
        max_bytes: DecimalU64::new(4),
        deadline_unix_ms: UnixMillis::new(100),
    }
}

fn materialization_receipt(id: &str) -> MaterializationObjectReceipt {
    MaterializationObjectReceipt {
        receipt_id: ObjectReceiptId::new(id).unwrap(),
        materialization_id: neoengram_domain::protocol::MaterializationId::new(
            "materialization-v2",
        )
        .unwrap(),
        batch_id: neoengram_domain::protocol::MaterializationBatchId::new("batch-v2").unwrap(),
        plan_revision: Generation::new(1),
        batch_attempt: Generation::new(1),
        tenant_id: TenantId::new("tenant-v2").unwrap(),
        object_namespace_id: ObjectNamespaceId::new("artifact-v2").unwrap(),
        object_id: ObjectId::from_bytes([1; 32]),
        size: DecimalU64::new(4),
        encoding: ObjectEncoding::Raw,
        verified_digest: ObjectId::from_bytes([1; 32]).digest(),
        target_storage_volume_id: StorageVolumeId::new("volume-target").unwrap(),
        target_placement_generation: PlacementGeneration::new(1),
        committed_offset: DecimalU64::new(4),
        verified_at_unix_ms: UnixMillis::new(10),
    }
}

fn staging_lease_for(
    materialization_id: &str,
    namespace: &str,
    target_volume: &str,
    lease_id: &str,
    state: neoengram_domain::protocol::materialization::MaterializationLeaseState,
) -> neoengram_domain::protocol::materialization::StagingLease {
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new(materialization_id).unwrap();
    let object_namespace_id = ObjectNamespaceId::new(namespace).unwrap();
    let object_id = ObjectId::from_bytes([1; 32]);
    neoengram_domain::protocol::materialization::StagingLease {
        lease_id: LeaseId::new(lease_id).unwrap(),
        materialization_id: materialization_id.clone(),
        plan_revision: Generation::new(1),
        tenant_id: TenantId::new("tenant-v2").unwrap(),
        object_namespace_id: object_namespace_id.clone(),
        object_id,
        target_storage_volume_id: StorageVolumeId::new(target_volume).unwrap(),
        target_placement_generation: PlacementGeneration::new(1),
        staging_key: MaterializationObject::expected_staging_key(
            &materialization_id,
            &object_namespace_id,
            object_id,
        ),
        expires_at_unix_ms: UnixMillis::new(90),
        state,
    }
}

fn object_read_lease_for(
    materialization_id: &str,
    namespace: &str,
    placement_id: &str,
    lease_id: &str,
    state: neoengram_domain::protocol::materialization::MaterializationLeaseState,
) -> ObjectReadLease {
    ObjectReadLease {
        lease_id: LeaseId::new(lease_id).unwrap(),
        materialization_id: neoengram_domain::protocol::MaterializationId::new(materialization_id)
            .unwrap(),
        batch_id: neoengram_domain::protocol::MaterializationBatchId::new(
            if materialization_id == "materialization-v2" {
                "batch-v2"
            } else {
                "batch-v3"
            },
        )
        .unwrap(),
        plan_revision: Generation::new(1),
        tenant_id: TenantId::new("tenant-v2").unwrap(),
        object_namespace_id: ObjectNamespaceId::new(namespace).unwrap(),
        object_id: ObjectId::from_bytes([1; 32]),
        placement_id: PlacementId::new(placement_id).unwrap(),
        placement_generation: PlacementGeneration::new(1),
        expires_at_unix_ms: UnixMillis::new(90),
        state,
    }
}

fn aggregate_plan() -> MaterializationPlan {
    let mut materialization = job();
    materialization.state = MaterializationJobState::Materializing;
    let batch = materialization_batch();
    let first_object = materialization_object();
    let second_object = MaterializationObject::new(
        materialization.materialization_id.clone(),
        ObjectRef::new(
            ObjectNamespaceId::new("artifact-v2").unwrap(),
            ObjectId::from_bytes([2; 32]),
            6,
            ObjectEncoding::Raw,
            1,
        ),
        Generation::new(1),
    );
    let source_lease_id = object_read_lease_id(
        &materialization.materialization_id,
        &batch.batch_id,
        batch.plan_revision,
        batch.batch_attempt,
        &ObjectNamespaceId::new("artifact-v2").unwrap(),
        first_object.object.object_id,
        &batch.source.placement_id,
    )
    .unwrap();
    let source_lease = object_read_lease_for(
        materialization.materialization_id.as_str(),
        "artifact-v2",
        "source-placement-v2",
        source_lease_id.as_str(),
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let staging_id = staging_lease_id(
        &materialization.materialization_id,
        batch.plan_revision,
        &ObjectNamespaceId::new("artifact-v2").unwrap(),
        first_object.object.object_id,
    )
    .unwrap();
    let staging = staging_lease_for(
        materialization.materialization_id.as_str(),
        "artifact-v2",
        "volume-target",
        staging_id.as_str(),
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let coverage = VolumeCommitCoverage::from_placements(
        TenantId::new("tenant-v2").unwrap(),
        ObjectNamespaceId::new("artifact-v2").unwrap(),
        materialization.key.commit_id,
        StorageVolumeId::new("volume-target").unwrap(),
        PlacementGeneration::new(1),
        &object_set().object_set,
        &[],
    )
    .unwrap();
    MaterializationPlan {
        job: materialization,
        batches: vec![batch],
        objects: vec![first_object, second_object],
        object_read_leases: vec![source_lease],
        staging_leases: vec![staging],
        coverage,
    }
}

/// Build a second valid plan that intentionally reuses the materialization ID in a different
/// namespace. The durable key is namespace-scoped, so both plans may coexist; legacy unscoped
/// list calls must reject the ambiguous identity instead of returning a mixed child set.
fn namespace_collision_plan() -> MaterializationPlan {
    let mut plan = aggregate_plan();
    let namespace = ObjectNamespaceId::new("artifact-v3").unwrap();
    let target_volume = StorageVolumeId::new("volume-target-v3").unwrap();
    plan.job.key.object_namespace_id = namespace.clone();
    plan.job.artifact_id = ArtifactId::new("artifact-v3").unwrap();
    plan.job.key.target_storage_volume_id = target_volume.clone();

    let batch = &mut plan.batches[0];
    batch.source.object_namespace_id = namespace.clone();
    batch.target.object_namespace_id = namespace.clone();
    batch.target.storage_volume_id = target_volume.clone();
    batch.source.storage_volume_id = Some(StorageVolumeId::new("volume-source-v3").unwrap());

    for object in &mut plan.objects {
        object.object.object_namespace_id = namespace.clone();
        object.staging_key = MaterializationObject::expected_staging_key(
            &object.materialization_id,
            &namespace,
            object.object.object_id,
        );
    }
    plan.object_read_leases[0].object_namespace_id = namespace.clone();
    plan.staging_leases[0].object_namespace_id = namespace.clone();
    plan.staging_leases[0].target_storage_volume_id = target_volume.clone();
    plan.staging_leases[0].staging_key = MaterializationObject::expected_staging_key(
        &plan.staging_leases[0].materialization_id,
        &namespace,
        plan.staging_leases[0].object_id,
    );
    plan.coverage.object_namespace_id = namespace;
    plan.coverage.storage_volume_id = target_volume;
    plan
}

async fn assert_materialization_lists_are_namespace_scoped(repository: &dyn PlacementRepository) {
    repository
        .insert_commit_object_set(object_set())
        .await
        .unwrap();
    repository
        .insert_object_placement_v2(placement(
            ObjectId::from_bytes([1; 32]),
            "source-placement-v2",
            "volume-source",
        ))
        .await
        .unwrap();
    let mut second_placement = placement(
        ObjectId::from_bytes([1; 32]),
        "source-placement-v2",
        "volume-source-v3",
    );
    second_placement.object_namespace_id = ObjectNamespaceId::new("artifact-v3").unwrap();
    second_placement.failure_domain = "host-volume-source-v3".to_owned();
    repository
        .insert_object_placement_v2(second_placement)
        .await
        .unwrap();
    repository
        .insert_materialization_plan(aggregate_plan())
        .await
        .unwrap();
    repository
        .insert_materialization_plan(namespace_collision_plan())
        .await
        .unwrap();

    let tenant = TenantId::new("tenant-v2").unwrap();
    let id = neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let first_namespace = ObjectNamespaceId::new("artifact-v2").unwrap();
    let second_namespace = ObjectNamespaceId::new("artifact-v3").unwrap();
    let first_batches = repository
        .list_materialization_batches(&tenant, &first_namespace, &id)
        .await
        .unwrap();
    let second_batches = repository
        .list_materialization_batches(&tenant, &second_namespace, &id)
        .await
        .unwrap();
    assert_eq!(first_batches.len(), 1);
    assert_eq!(second_batches.len(), 1);
    assert_eq!(first_batches[0].target.object_namespace_id, first_namespace);
    assert_eq!(
        second_batches[0].target.object_namespace_id,
        second_namespace
    );
    let first_objects = repository
        .list_materialization_objects(&tenant, &first_namespace, &id)
        .await
        .unwrap();
    let second_objects = repository
        .list_materialization_objects(&tenant, &second_namespace, &id)
        .await
        .unwrap();
    assert_eq!(first_objects.len(), 2);
    assert_eq!(second_objects.len(), 2);
    assert!(first_objects
        .iter()
        .all(|object| object.object.object_namespace_id == first_namespace));
    assert!(second_objects
        .iter()
        .all(|object| object.object.object_namespace_id == second_namespace));
}

fn replacement_plan(initial: &MaterializationPlan) -> MaterializationPlan {
    let mut materialization = initial.job.clone();
    materialization.plan_revision = Generation::new(2);
    materialization.updated_at_unix_ms = UnixMillis::new(2);
    materialization.deadline_unix_ms = UnixMillis::new(200);

    let mut batch = initial.batches[0].clone();
    batch.batch_id = neoengram_domain::protocol::MaterializationBatchId::new("batch-v3").unwrap();
    batch.plan_revision = Generation::new(2);
    batch.batch_attempt = Generation::new(2);
    batch.state = MaterializationBatchState::Queued;

    let mut first_object = initial.objects[0].clone();
    first_object.plan_revision = Generation::new(2);
    first_object.attempt = Generation::new(2);
    first_object.current_batch_id = Some(batch.batch_id.clone());
    let mut second_object = initial.objects[1].clone();
    second_object.plan_revision = Generation::new(2);
    second_object.attempt = Generation::new(2);

    let source_lease_id = object_read_lease_id(
        &materialization.materialization_id,
        &batch.batch_id,
        batch.plan_revision,
        batch.batch_attempt,
        &batch.source.object_namespace_id,
        first_object.object.object_id,
        &batch.source.placement_id,
    )
    .unwrap();
    let mut source_lease = initial.object_read_leases[0].clone();
    source_lease.lease_id = source_lease_id;
    source_lease.batch_id = batch.batch_id.clone();
    source_lease.plan_revision = Generation::new(2);
    let staging_id = staging_lease_id(
        &materialization.materialization_id,
        batch.plan_revision,
        &batch.target.object_namespace_id,
        first_object.object.object_id,
    )
    .unwrap();
    let mut staging = initial.staging_leases[0].clone();
    staging.lease_id = staging_id;
    staging.plan_revision = Generation::new(2);
    MaterializationPlan {
        job: materialization,
        batches: vec![batch],
        objects: vec![first_object, second_object],
        object_read_leases: vec![source_lease],
        staging_leases: vec![staging],
        coverage: initial.coverage.clone(),
    }
}

async fn seed_receipt_fixture(repository: &dyn PlacementRepository) {
    repository
        .insert_commit_object_set(object_set())
        .await
        .unwrap();
    let mut materialization = job();
    materialization.state = MaterializationJobState::Materializing;
    repository
        .insert_materialization(materialization)
        .await
        .unwrap();
    repository
        .insert_materialization_batch(materialization_batch())
        .await
        .unwrap();
    repository
        .insert_object_placement_v2(placement(
            ObjectId::from_bytes([1; 32]),
            "source-placement-v2",
            "volume-source",
        ))
        .await
        .unwrap();
    repository
        .insert_materialization_object(
            &TenantId::new("tenant-v2").unwrap(),
            materialization_object(),
        )
        .await
        .unwrap();
    let read_lease_id = object_read_lease_id(
        &neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap(),
        &neoengram_domain::protocol::MaterializationBatchId::new("batch-v2").unwrap(),
        Generation::new(1),
        Generation::new(1),
        &ObjectNamespaceId::new("artifact-v2").unwrap(),
        ObjectId::from_bytes([1; 32]),
        &PlacementId::new("source-placement-v2").unwrap(),
    )
    .unwrap();
    repository
        .insert_object_read_lease(object_read_lease_for(
            "materialization-v2",
            "artifact-v2",
            "source-placement-v2",
            read_lease_id.as_str(),
            neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
        ))
        .await
        .unwrap();
    let staging_lease_id = staging_lease_id(
        &neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap(),
        Generation::new(1),
        &ObjectNamespaceId::new("artifact-v2").unwrap(),
        ObjectId::from_bytes([1; 32]),
    )
    .unwrap();
    let staging_lease = staging_lease_for(
        "materialization-v2",
        "artifact-v2",
        "volume-target",
        staging_lease_id.as_str(),
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    assert_eq!(
        repository
            .insert_staging_lease(staging_lease.clone())
            .await
            .unwrap(),
        staging_lease
    );
    assert_eq!(
        repository
            .insert_staging_lease(staging_lease)
            .await
            .unwrap()
            .lease_id
            .as_str(),
        staging_lease_id.as_str()
    );
}

async fn assert_receipt_semantics(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    let object = materialization_object().object;
    let receipt = materialization_receipt("receipt-v2");
    let first = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: receipt.clone(),
            object: object.clone(),
        })
        .await
        .unwrap();
    let replay = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: receipt.clone(),
            object: object.clone(),
        })
        .await
        .unwrap();
    assert_eq!(first, replay);
    let source_lease_id = object_read_lease_id(
        &receipt.materialization_id,
        &receipt.batch_id,
        receipt.plan_revision,
        receipt.batch_attempt,
        &receipt.object_namespace_id,
        receipt.object_id,
        &PlacementId::new("source-placement-v2").unwrap(),
    )
    .unwrap();
    let error = repository
        .insert_object_read_lease(object_read_lease_for(
            "materialization-v2",
            "artifact-v2",
            "source-placement-v2",
            source_lease_id.as_str(),
            neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
        ))
        .await
        .expect_err("receipt must release its source lease before returning");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );
    let staging_id = staging_lease_id(
        &receipt.materialization_id,
        receipt.plan_revision,
        &receipt.object_namespace_id,
        receipt.object_id,
    )
    .unwrap();
    let error = repository
        .insert_staging_lease(staging_lease_for(
            "materialization-v2",
            "artifact-v2",
            "volume-target",
            staging_id.as_str(),
            neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
        ))
        .await
        .expect_err("receipt must release its staging lease before returning");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );
    let released_staging = repository
        .release_staging_lease(
            &receipt.tenant_id,
            &receipt.object_namespace_id,
            &staging_id,
        )
        .await
        .unwrap()
        .expect("receipt must release its staging lease");
    assert_eq!(
        released_staging.state,
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Released
    );
    let released_source = repository
        .release_object_read_lease(
            &receipt.tenant_id,
            &receipt.object_namespace_id,
            &source_lease_id,
        )
        .await
        .unwrap()
        .expect("receipt must release its source lease");
    assert_eq!(
        released_source.state,
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Released
    );
    let placements = repository
        .object_placements_v2(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &ObjectId::from_bytes([1; 32]),
        )
        .await
        .unwrap();
    assert_eq!(placements.len(), 2);
    assert_eq!(
        placements
            .iter()
            .filter(|placement| placement
                .storage_volume_id
                .as_ref()
                .is_some_and(|volume| { volume.as_str() == "volume-target" }))
            .count(),
        1
    );
    let completed = repository
        .get_materialization(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, MaterializationJobState::Verifying);
    assert_eq!(completed.verified_object_count, DecimalU64::new(1));

    let mut tampered = receipt;
    tampered.verified_at_unix_ms = UnixMillis::new(11);
    let error = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: tampered,
            object,
        })
        .await
        .expect_err("receipt identity reuse must reject changed evidence");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );

    let mut competing = materialization_receipt("receipt-v2-other");
    competing.verified_at_unix_ms = UnixMillis::new(12);
    let error = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: competing,
            object: materialization_object().object,
        })
        .await
        .expect_err("one Batch/object must have only one durable receipt");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );
}

async fn assert_receipt_deadline_is_enforced(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    let object = materialization_object().object;
    let mut receipt = materialization_receipt("receipt-v2-deadline");
    // The batch deadline is exclusive: a receipt observed exactly at the deadline is stale.
    receipt.verified_at_unix_ms = UnixMillis::new(100);
    let error = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt,
            object,
        })
        .await
        .expect_err("a receipt at the batch deadline must be rejected");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::ProtocolInvalid
    );
    assert!(repository
        .object_placements_v2(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &ObjectId::from_bytes([1; 32]),
        )
        .await
        .unwrap()
        .into_iter()
        .all(|placement| placement.storage_volume_id.as_ref()
            != Some(&StorageVolumeId::new("volume-target").unwrap())));
}

async fn assert_receipt_replay_survives_deadline(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    let object = materialization_object().object;
    let receipt = materialization_receipt("receipt-v2-expired-replay");
    let first = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: receipt.clone(),
            object: object.clone(),
        })
        .await
        .unwrap();

    // The durable publication happened before the original deadline. Once the ACK is lost, a
    // reconnect may replay it after that deadline; changing the Batch deadline makes the replay
    // boundary explicit without relying on wall-clock sleeps.
    let mut expired_batch = materialization_batch();
    expired_batch.deadline_unix_ms = UnixMillis::new(5);
    repository
        .replace_materialization_batch(neoengram_central::MaterializationBatchCasRequest {
            tenant_id: receipt.tenant_id.clone(),
            object_namespace_id: receipt.object_namespace_id.clone(),
            materialization_id: receipt.materialization_id.clone(),
            batch_id: receipt.batch_id.clone(),
            expected_plan_revision: receipt.plan_revision,
            expected_batch_attempt: receipt.batch_attempt,
            batch: expired_batch,
        })
        .await
        .unwrap();

    let replay = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt,
            object,
        })
        .await
        .expect("an exact durable receipt replay must ignore the old deadline");
    assert_eq!(first, replay);
}

async fn assert_receipt_progress_is_namespace_scoped(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;

    // A second namespace deliberately reuses the materialization ID and has a completed object.
    // Its task must not be included when the first namespace's Job progress is recomputed.
    let namespace = ObjectNamespaceId::new("artifact-v3").unwrap();
    let target_volume = StorageVolumeId::new("volume-target-v3").unwrap();
    let mut second_job = job();
    second_job.key.object_namespace_id = namespace.clone();
    second_job.key.target_storage_volume_id = target_volume.clone();
    second_job.artifact_id = ArtifactId::new("artifact-v3").unwrap();
    second_job.state = MaterializationJobState::Materializing;
    repository.insert_materialization(second_job).await.unwrap();

    let mut second_batch = materialization_batch();
    second_batch.batch_id =
        neoengram_domain::protocol::MaterializationBatchId::new("batch-v3-progress").unwrap();
    second_batch.source.object_namespace_id = namespace.clone();
    second_batch.target.object_namespace_id = namespace.clone();
    second_batch.target.storage_volume_id = target_volume.clone();
    let second_batch_id = second_batch.batch_id.clone();
    repository
        .insert_materialization_batch(second_batch)
        .await
        .unwrap();

    let mut second_source = placement(
        ObjectId::from_bytes([1; 32]),
        "source-placement-v3-progress",
        "volume-source-v3-progress",
    );
    second_source.object_namespace_id = namespace.clone();
    repository
        .insert_object_placement_v2(second_source)
        .await
        .unwrap();
    let mut second_target = placement(
        ObjectId::from_bytes([1; 32]),
        "target-placement-v3-progress",
        target_volume.as_str(),
    );
    second_target.object_namespace_id = namespace.clone();
    repository
        .insert_object_placement_v2(second_target)
        .await
        .unwrap();
    let mut second_object = materialization_object();
    second_object.object.object_namespace_id = namespace.clone();
    second_object.staging_key = MaterializationObject::expected_staging_key(
        &second_object.materialization_id,
        &namespace,
        second_object.object.object_id,
    );
    second_object.state = MaterializationObjectState::Verified;
    second_object.confirmed_offset = second_object.object.size;
    second_object.current_batch_id = Some(second_batch_id);
    repository
        .insert_materialization_object(&TenantId::new("tenant-v2").unwrap(), second_object)
        .await
        .unwrap();

    let receipt = materialization_receipt("receipt-v2-namespace-progress");
    repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt,
            object: materialization_object().object,
        })
        .await
        .unwrap();
    let first_job = repository
        .get_materialization(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_job.verified_object_count, DecimalU64::new(1));
    assert_eq!(first_job.verified_bytes, DecimalU64::new(4));
    assert_eq!(first_job.state, MaterializationJobState::Verifying);
}

async fn assert_completed_object_retry_repairs_coverage(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    let tenant = TenantId::new("tenant-v2").unwrap();
    let namespace = ObjectNamespaceId::new("artifact-v2").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let object = materialization_object().object;

    let mut target = placement(object.object_id, "target-placement-repair", "volume-target");
    target.failure_domain = "volume:volume-target".to_owned();
    repository.insert_object_placement_v2(target).await.unwrap();

    let current = repository
        .list_materialization_objects(&tenant, &namespace, &materialization_id)
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.object.object_id == object.object_id)
        .unwrap();
    let mut completed = current.clone();
    completed.state = MaterializationObjectState::Verified;
    completed.confirmed_offset = object.size;
    repository
        .replace_materialization_object(neoengram_central::MaterializationObjectCasRequest {
            tenant_id: tenant.clone(),
            object_namespace_id: namespace.clone(),
            materialization_id: materialization_id.clone(),
            object_id: object.object_id,
            expected_plan_revision: current.plan_revision,
            expected_attempt: current.attempt,
            object: completed,
        })
        .await
        .unwrap();

    repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: materialization_receipt("receipt-v2-coverage-repair"),
            object,
        })
        .await
        .unwrap();
    let coverages = repository
        .volume_commit_coverages(&tenant, &namespace, &CommitId::from_bytes([9; 32]).digest())
        .await
        .unwrap();
    assert_eq!(coverages.len(), 1);
    assert_eq!(coverages[0].verified_object_count, DecimalU64::new(1));
}

async fn assert_competing_batch_converges_on_existing_placement(
    repository: &dyn PlacementRepository,
) {
    seed_receipt_fixture(repository).await;
    let tenant = TenantId::new("tenant-v2").unwrap();
    let namespace = ObjectNamespaceId::new("artifact-v2").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let object = materialization_object().object;

    let mut existing_target = placement(
        object.object_id,
        "target-placement-competing",
        "volume-target",
    );
    existing_target.failure_domain = "volume:volume-target".to_owned();
    repository
        .insert_object_placement_v2(existing_target.clone())
        .await
        .unwrap();

    let mut competing_batch = materialization_batch();
    competing_batch.batch_id =
        neoengram_domain::protocol::MaterializationBatchId::new("batch-v3-competing").unwrap();
    repository
        .insert_materialization_batch(competing_batch.clone())
        .await
        .unwrap();
    let current = repository
        .list_materialization_objects(&tenant, &namespace, &materialization_id)
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.object.object_id == object.object_id)
        .unwrap();
    let mut reassigned = current.clone();
    reassigned.current_batch_id = Some(competing_batch.batch_id.clone());
    repository
        .replace_materialization_object(neoengram_central::MaterializationObjectCasRequest {
            tenant_id: tenant.clone(),
            object_namespace_id: namespace.clone(),
            materialization_id: materialization_id.clone(),
            object_id: object.object_id,
            expected_plan_revision: current.plan_revision,
            expected_attempt: current.attempt,
            object: reassigned,
        })
        .await
        .unwrap();

    let converged = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: materialization_receipt("receipt-v2-competing"),
            object,
        })
        .await
        .expect("a losing same-attempt Batch must converge on the durable target Placement");
    assert_eq!(converged, existing_target);
}

async fn assert_control_plane_replay_survives_reconnect() {
    let components = InMemoryComponents::new(50);
    let repository = components.placement.clone();
    seed_receipt_fixture(repository.as_ref()).await;
    let receipt = materialization_receipt("receipt-v2-control-reconnect");
    repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: receipt.clone(),
            object: materialization_object().object,
        })
        .await
        .unwrap();

    // Build a control plane without live route/owner registries so the only changing live fence
    // here is the authenticated session generation. The durable receipt lookup must run before
    // that fence and acknowledge the queued report after an Agent reconnect.
    let authority = AuthorityStore::from_parts(
        components.jobs.clone(),
        components.outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        components.publisher.clone(),
        components.audit.clone(),
        AuthorityCapabilities::IN_MEMORY,
    )
    .with_placement(repository.clone());
    let control = ControlPlane::new(
        Arc::new(AllowAllAuthorizer),
        authority,
        components.clock.clone(),
    );
    let report = MaterializationReport::Receipt {
        receipt,
        target: materialization_batch().target,
        extensions: Default::default(),
    };
    let result = control
        .receive_materialization_report(
            &TenantId::new("tenant-v2").unwrap(),
            &AgentId::new("agent-target").unwrap(),
            SessionGeneration::new(2),
            report,
        )
        .await
        .expect("a durable receipt must replay after an Agent reconnect");
    assert!(result.replayed);
}

async fn assert_receipt_converges_on_existing_placement(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;

    // A placement from another materialization may already occupy the target physical key.  The
    // target tuple is the durable identity of the physical copy, so the receipt must converge on
    // that row even when its report ID is different.
    let mut existing_target = placement(
        ObjectId::from_bytes([1; 32]),
        "placement-existing-target",
        "volume-target",
    );
    existing_target.failure_domain = "volume:volume-target".to_owned();
    repository
        .insert_object_placement_v2(existing_target.clone())
        .await
        .unwrap();

    let object = materialization_object().object;
    let receipt = materialization_receipt("receipt-v2-alias");
    let converged = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: receipt.clone(),
            object: object.clone(),
        })
        .await
        .expect("a competing receipt must converge on the existing target placement");
    assert_eq!(converged, existing_target);

    let replay = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt,
            object: object.clone(),
        })
        .await
        .expect("the converged receipt must replay idempotently");
    assert_eq!(replay, existing_target);

    // Both receipt publication and replay retain the existing physical Placement row and advance
    // the target object exactly once.
    let current = repository
        .list_materialization_objects(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap(),
        )
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.object.object_id == object.object_id)
        .unwrap();
    assert_eq!(current.state, MaterializationObjectState::Verified);
    assert_eq!(current.confirmed_offset, object.size);
    assert_eq!(
        repository
            .object_placements_v2(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &object.object_id,
            )
            .await
            .unwrap()
            .into_iter()
            .filter(|candidate| {
                candidate.storage_volume_id.as_ref()
                    == Some(&StorageVolumeId::new("volume-target").unwrap())
            })
            .count(),
        1
    );
}

async fn assert_receipt_rejects_invalid_object_state(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    let tenant = TenantId::new("tenant-v2").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let object = materialization_object().object;
    let current = repository
        .list_materialization_objects(
            &tenant,
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &materialization_id,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.object.object_id == object.object_id)
        .unwrap();
    let mut invalid_state = current.clone();
    invalid_state.state = MaterializationObjectState::Missing;
    repository
        .replace_materialization_object(neoengram_central::MaterializationObjectCasRequest {
            tenant_id: tenant.clone(),
            object_namespace_id: object.object_namespace_id.clone(),
            materialization_id: materialization_id.clone(),
            object_id: object.object_id,
            expected_plan_revision: current.plan_revision,
            expected_attempt: current.attempt,
            object: invalid_state,
        })
        .await
        .unwrap();

    let error = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: materialization_receipt("receipt-v2-invalid-state"),
            object,
        })
        .await
        .expect_err("an incomplete object in Missing state cannot publish a receipt");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );
    assert_eq!(
        repository
            .object_placements_v2(
                &tenant,
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &ObjectId::from_bytes([1; 32]),
            )
            .await
            .unwrap()
            .into_iter()
            .filter(|candidate| {
                candidate.storage_volume_id.as_ref()
                    == Some(&StorageVolumeId::new("volume-target").unwrap())
            })
            .count(),
        0
    );
}

async fn assert_receipt_replay_survives_replan(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    let object = materialization_object().object;
    let receipt = materialization_receipt("receipt-v2-replan-replay");
    let first = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt: receipt.clone(),
            object: object.clone(),
        })
        .await
        .unwrap();

    let tenant = TenantId::new("tenant-v2").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let current = repository
        .get_materialization(
            &tenant,
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &materialization_id,
        )
        .await
        .unwrap()
        .unwrap();
    let mut replanned = current.clone();
    replanned.plan_revision = Generation::new(2);
    replanned.state = MaterializationJobState::Planning;
    replanned.updated_at_unix_ms = UnixMillis::new(20);
    replanned.deadline_unix_ms = UnixMillis::new(200);
    repository
        .replace_materialization(&tenant, &materialization_id, Generation::new(1), replanned)
        .await
        .unwrap();

    let current_object = repository
        .list_materialization_objects(
            &tenant,
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &materialization_id,
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let mut replanned_object = current_object.clone();
    replanned_object.plan_revision = Generation::new(2);
    replanned_object.attempt = Generation::new(2);
    repository
        .insert_materialization_object(&tenant, replanned_object)
        .await
        .unwrap();

    let replay = repository
        .record_materialization_receipt(neoengram_central::MaterializationReceiptRequest {
            receipt,
            object,
        })
        .await
        .expect("an exact receipt replay must not be fenced by a newer plan");
    assert_eq!(first, replay);
}

async fn assert_replan_preserves_checkpoint(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    let tenant = TenantId::new("tenant-v2").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let current = repository
        .get_materialization(
            &tenant,
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &materialization_id,
        )
        .await
        .unwrap()
        .unwrap();
    let mut replanned = current.clone();
    replanned.plan_revision = Generation::new(2);
    replanned.state = MaterializationJobState::Planning;
    replanned.updated_at_unix_ms = UnixMillis::new(2);
    replanned.deadline_unix_ms = UnixMillis::new(200);
    repository
        .replace_materialization(&tenant, &materialization_id, Generation::new(1), replanned)
        .await
        .unwrap();

    let current_object = repository
        .list_materialization_objects(
            &tenant,
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &materialization_id,
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let mut next_object = current_object.clone();
    next_object.plan_revision = Generation::new(2);
    next_object.attempt = Generation::new(2);
    next_object.confirmed_offset = DecimalU64::new(2);
    next_object.last_error = Some("source disconnected".to_owned());
    let stored = repository
        .insert_materialization_object(&tenant, next_object.clone())
        .await
        .unwrap();
    assert_eq!(stored, next_object);
    let listed = repository
        .list_materialization_objects(
            &tenant,
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &materialization_id,
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].confirmed_offset, DecimalU64::new(2));
    assert_eq!(listed[0].plan_revision, Generation::new(2));
    assert_eq!(listed[0].attempt, Generation::new(2));
}

async fn assert_materialization_ids_are_tenant_scoped(repository: &dyn PlacementRepository) {
    let first_set = object_set();
    repository
        .insert_commit_object_set(first_set.clone())
        .await
        .unwrap();
    let first = job();
    repository
        .insert_materialization(first.clone())
        .await
        .unwrap();
    repository
        .insert_materialization_object(
            &TenantId::new("tenant-v2").unwrap(),
            materialization_object(),
        )
        .await
        .unwrap();

    let second_tenant = TenantId::new("tenant-v3").unwrap();
    let second_namespace = ObjectNamespaceId::new("artifact-v3").unwrap();
    let mut second_set = first_set;
    second_set.tenant_id = second_tenant.clone();
    repository
        .insert_commit_object_set(second_set)
        .await
        .unwrap();
    let mut second = first.clone();
    second.key.tenant_id = second_tenant.clone();
    second.key.object_namespace_id = second_namespace.clone();
    second.key.target_storage_volume_id = StorageVolumeId::new("volume-target-v3").unwrap();
    second.artifact_id = ArtifactId::new("artifact-v3").unwrap();
    // Resource IDs are tenant-scoped; the composite business key still keeps this operation
    // separate from the first tenant's materialization.
    repository
        .insert_materialization(second.clone())
        .await
        .unwrap();
    let mut second_object = materialization_object();
    second_object.object.object_namespace_id = second_namespace.clone();
    second_object.staging_key = MaterializationObject::expected_staging_key(
        &second_object.materialization_id,
        &second_namespace,
        second_object.object.object_id,
    );
    repository
        .insert_materialization_object(&second_tenant, second_object)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_materialization(&second_tenant, &second_namespace, &first.materialization_id,)
            .await
            .unwrap(),
        Some(second)
    );
    assert_eq!(
        repository
            .get_materialization(
                &TenantId::new("tenant-v2").unwrap(),
                &second_namespace,
                &first.materialization_id,
            )
            .await
            .unwrap(),
        None,
        "a materialization ID must not resolve across object namespaces",
    );
}

async fn assert_active_target_ignores_coverage_goal(repository: &dyn PlacementRepository) {
    repository
        .insert_commit_object_set(object_set())
        .await
        .unwrap();
    let first = job();
    repository.insert_materialization(first).await.unwrap();

    let mut second = job();
    second.materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-threshold-v2").unwrap();
    second.key.coverage_goal = CoverageGoal::ObjectCount(DecimalU64::new(1));
    let error = repository.insert_materialization(second).await.unwrap_err();
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::ReplicationAlreadyActive
    );
}

async fn seed_second_namespace_lease_fixture(repository: &dyn PlacementRepository) {
    let namespace = ObjectNamespaceId::new("artifact-v3").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v3").unwrap();

    let mut second_placement = placement(
        ObjectId::from_bytes([1; 32]),
        "source-placement-v3",
        "volume-source-v3",
    );
    second_placement.object_namespace_id = namespace.clone();
    repository
        .insert_object_placement_v2(second_placement)
        .await
        .unwrap();

    let mut second_job = job();
    second_job.materialization_id = materialization_id.clone();
    second_job.key.object_namespace_id = namespace.clone();
    second_job.key.target_storage_volume_id = StorageVolumeId::new("volume-target-v3").unwrap();
    second_job.artifact_id = ArtifactId::new("artifact-v3").unwrap();
    repository.insert_materialization(second_job).await.unwrap();

    let mut second_batch = materialization_batch();
    second_batch.materialization_id = materialization_id.clone();
    second_batch.source.object_namespace_id = namespace.clone();
    second_batch.target.object_namespace_id = namespace.clone();
    second_batch.source.placement_id = PlacementId::new("source-placement-v3").unwrap();
    second_batch.source.storage_volume_id = Some(StorageVolumeId::new("volume-source-v3").unwrap());
    second_batch.target.storage_volume_id = StorageVolumeId::new("volume-target-v3").unwrap();
    second_batch.batch_id =
        neoengram_domain::protocol::MaterializationBatchId::new("batch-v3").unwrap();
    repository
        .insert_materialization_batch(second_batch)
        .await
        .unwrap();

    let mut second_object = materialization_object();
    second_object.materialization_id = materialization_id;
    second_object.object.object_namespace_id = namespace.clone();
    second_object.staging_key = MaterializationObject::expected_staging_key(
        &second_object.materialization_id,
        &namespace,
        second_object.object.object_id,
    );
    second_object.current_batch_id =
        Some(neoengram_domain::protocol::MaterializationBatchId::new("batch-v3").unwrap());
    repository
        .insert_materialization_object(&TenantId::new("tenant-v2").unwrap(), second_object)
        .await
        .unwrap();
}

async fn assert_lease_namespace_and_state_semantics(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;
    repository
        .insert_object_placement_v2(placement(
            ObjectId::from_bytes([1; 32]),
            "source-placement-v2",
            "volume-source",
        ))
        .await
        .unwrap();

    let mut terminal = staging_lease_for(
        "materialization-v2",
        "artifact-v2",
        "volume-target",
        "staging-lease-terminal",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Released,
    );
    let error = repository
        .insert_staging_lease(terminal.clone())
        .await
        .expect_err("new staging leases must be active");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::ProtocolInvalid
    );

    terminal.state =
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Expired;
    let error = repository
        .insert_staging_lease(terminal)
        .await
        .expect_err("expired staging leases must be active on insertion");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::ProtocolInvalid
    );

    let mut read_terminal = object_read_lease_for(
        "materialization-v2",
        "artifact-v2",
        "source-placement-v2",
        "read-lease-terminal",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Released,
    );
    let error = repository
        .insert_object_read_lease(read_terminal.clone())
        .await
        .expect_err("new object read leases must be active");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::ProtocolInvalid
    );
    read_terminal.state =
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Expired;
    let error = repository
        .insert_object_read_lease(read_terminal)
        .await
        .expect_err("expired object read leases must be active on insertion");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::ProtocolInvalid
    );

    seed_second_namespace_lease_fixture(repository).await;
    let first = staging_lease_for(
        "materialization-v2",
        "artifact-v2",
        "volume-target",
        "same-lease-id",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let second = staging_lease_for(
        "materialization-v3",
        "artifact-v3",
        "volume-target-v3",
        "same-lease-id",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    repository
        .insert_staging_lease(first.clone())
        .await
        .unwrap();
    repository
        .insert_staging_lease(second.clone())
        .await
        .unwrap();

    let first_read = object_read_lease_for(
        "materialization-v2",
        "artifact-v2",
        "source-placement-v2",
        "same-read-lease-id",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let second_read = object_read_lease_for(
        "materialization-v3",
        "artifact-v3",
        "source-placement-v3",
        "same-read-lease-id",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    repository
        .insert_object_read_lease(first_read.clone())
        .await
        .unwrap();
    repository
        .insert_object_read_lease(second_read.clone())
        .await
        .unwrap();

    assert!(repository
        .release_staging_lease(
            &first.tenant_id,
            &first.object_namespace_id,
            &first.lease_id,
        )
        .await
        .unwrap()
        .is_some());
    assert!(repository
        .release_object_read_lease(
            &first_read.tenant_id,
            &first_read.object_namespace_id,
            &first_read.lease_id,
        )
        .await
        .unwrap()
        .is_some());
    assert!(repository
        .release_object_read_lease(
            &second_read.tenant_id,
            &second_read.object_namespace_id,
            &second_read.lease_id,
        )
        .await
        .unwrap()
        .is_some());
    assert!(repository
        .release_staging_lease(
            &second.tenant_id,
            &second.object_namespace_id,
            &second.lease_id,
        )
        .await
        .unwrap()
        .is_some());
    assert!(repository
        .release_staging_lease(
            &first.tenant_id,
            &ObjectNamespaceId::new("artifact-missing").unwrap(),
            &first.lease_id,
        )
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .release_object_read_lease(
            &first_read.tenant_id,
            &ObjectNamespaceId::new("artifact-missing").unwrap(),
            &first_read.lease_id,
        )
        .await
        .unwrap()
        .is_none());
}

async fn assert_lease_batch_fence_semantics(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;

    // A source lease is valid only for an object explicitly present in the batch manifest.
    let mut missing_from_batch = object_read_lease_for(
        "materialization-v2",
        "artifact-v2",
        "source-placement-v2",
        "read-lease-object-not-in-batch",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    missing_from_batch.object_id = ObjectId::from_bytes([2; 32]);
    let error = repository
        .insert_object_read_lease(missing_from_batch)
        .await
        .expect_err("read lease object must be present in the batch manifest");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );

    // The source Placement identity and generation are part of the Batch fence.
    let wrong_source = object_read_lease_for(
        "materialization-v2",
        "artifact-v2",
        "different-source-placement",
        "read-lease-wrong-source",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let error = repository
        .insert_object_read_lease(wrong_source)
        .await
        .expect_err("read lease must use the Batch source placement");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );

    let mut wrong_generation = object_read_lease_for(
        "materialization-v2",
        "artifact-v2",
        "source-placement-v2",
        "read-lease-wrong-generation",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    wrong_generation.placement_generation = PlacementGeneration::new(2);
    let error = repository
        .insert_object_read_lease(wrong_generation)
        .await
        .expect_err("read lease must use the Batch source generation");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );

    // Add a second registered object task without adding it to the batch. This reaches the
    // manifest-membership check rather than the materialization-object lookup.
    let mut object_two = materialization_object();
    object_two.object = ObjectRef::new(
        ObjectNamespaceId::new("artifact-v2").unwrap(),
        ObjectId::from_bytes([2; 32]),
        6,
        ObjectEncoding::Raw,
        1,
    );
    object_two.staging_key = MaterializationObject::expected_staging_key(
        &object_two.materialization_id,
        &object_two.object.object_namespace_id,
        object_two.object.object_id,
    );
    repository
        .insert_materialization_object(&TenantId::new("tenant-v2").unwrap(), object_two)
        .await
        .unwrap();
    let mut staging_missing_from_batch = staging_lease_for(
        "materialization-v2",
        "artifact-v2",
        "volume-target",
        "staging-lease-object-not-in-batch",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    staging_missing_from_batch.object_id = ObjectId::from_bytes([2; 32]);
    staging_missing_from_batch.staging_key = MaterializationObject::expected_staging_key(
        &staging_missing_from_batch.materialization_id,
        &staging_missing_from_batch.object_namespace_id,
        staging_missing_from_batch.object_id,
    );
    let error = repository
        .insert_staging_lease(staging_missing_from_batch)
        .await
        .expect_err("staging lease object must be present in the batch manifest");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );

    // Staging is fenced to the target generation carried by the current Batch.
    let mut wrong_target_generation = staging_lease_for(
        "materialization-v2",
        "artifact-v2",
        "volume-target",
        "staging-lease-wrong-generation",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    wrong_target_generation.target_placement_generation = PlacementGeneration::new(2);
    let error = repository
        .insert_staging_lease(wrong_target_generation)
        .await
        .expect_err("staging lease must use the Batch target generation");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );

    // A task whose current batch disappeared must not acquire a lease under an unrelated batch.
    let tenant = TenantId::new("tenant-v2").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let current = repository
        .list_materialization_objects(
            &tenant,
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &materialization_id,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|object| object.object.object_id == ObjectId::from_bytes([1; 32]))
        .unwrap();
    let mut no_current_batch = current.clone();
    no_current_batch.current_batch_id = Some(
        neoengram_domain::protocol::MaterializationBatchId::new("batch-no-longer-current").unwrap(),
    );
    repository
        .replace_materialization_object(neoengram_central::MaterializationObjectCasRequest {
            tenant_id: tenant.clone(),
            object_namespace_id: current.object.object_namespace_id.clone(),
            materialization_id: materialization_id.clone(),
            object_id: current.object.object_id,
            expected_plan_revision: current.plan_revision,
            expected_attempt: current.attempt,
            object: no_current_batch,
        })
        .await
        .unwrap();
    let missing_current_batch = staging_lease_for(
        "materialization-v2",
        "artifact-v2",
        "volume-target",
        "staging-lease-missing-current-batch",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let error = repository
        .insert_staging_lease(missing_current_batch)
        .await
        .expect_err("staging lease must follow the object's current batch");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::ResourceNotFound
    );

    // Even when the source Placement ID is unchanged, the source Volume is a signed Batch
    // fence. Change only the Batch source Volume and ensure the existing physical copy is not
    // accepted under that altered route.
    let mut altered_batch = materialization_batch();
    altered_batch.source.storage_volume_id =
        Some(StorageVolumeId::new("volume-other-source").unwrap());
    repository
        .replace_materialization_batch(neoengram_central::MaterializationBatchCasRequest {
            tenant_id: tenant.clone(),
            object_namespace_id: ObjectNamespaceId::new("artifact-v2").unwrap(),
            materialization_id: materialization_id.clone(),
            batch_id: neoengram_domain::protocol::MaterializationBatchId::new("batch-v2").unwrap(),
            expected_plan_revision: Generation::new(1),
            expected_batch_attempt: Generation::new(1),
            batch: altered_batch,
        })
        .await
        .unwrap();
    let wrong_source_volume = object_read_lease_for(
        "materialization-v2",
        "artifact-v2",
        "source-placement-v2",
        "read-lease-wrong-source-volume",
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let error = repository
        .insert_object_read_lease(wrong_source_volume)
        .await
        .expect_err("read lease must use the Batch source Volume");
    assert_eq!(
        error.code(),
        neoengram_central::CentralErrorCode::InvalidState
    );
}

/// A fallback source is an independent durable Placement.  Its read lease must keep the
/// Placement's own Volume and generation rather than borrowing the primary Batch route fence.
async fn assert_fallback_read_lease_uses_placement_fence(repository: &dyn PlacementRepository) {
    seed_receipt_fixture(repository).await;

    let tenant = TenantId::new("tenant-v2").unwrap();
    let namespace = ObjectNamespaceId::new("artifact-v2").unwrap();
    let materialization_id =
        neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap();
    let object_id = ObjectId::from_bytes([1; 32]);
    let fallback_id = PlacementId::new("source-placement-fallback").unwrap();
    let fallback_volume = StorageVolumeId::new("volume-source-fallback").unwrap();
    let mut fallback = placement(object_id, fallback_id.as_str(), fallback_volume.as_str());
    fallback.placement_generation = PlacementGeneration::new(2);
    fallback.failure_domain = "host-volume-source-fallback".to_owned();
    repository
        .insert_object_placement_v2(fallback)
        .await
        .unwrap();

    let current = repository
        .list_materialization_objects(&tenant, &namespace, &materialization_id)
        .await
        .unwrap()
        .into_iter()
        .find(|object| object.object.object_id == object_id)
        .unwrap();
    let mut with_fallback = current.clone();
    with_fallback.fallback_sources = vec![fallback_id.clone()];
    repository
        .replace_materialization_object(neoengram_central::MaterializationObjectCasRequest {
            tenant_id: tenant.clone(),
            object_namespace_id: namespace.clone(),
            materialization_id: materialization_id.clone(),
            object_id,
            expected_plan_revision: current.plan_revision,
            expected_attempt: current.attempt,
            object: with_fallback,
        })
        .await
        .unwrap();

    let lease_id = object_read_lease_id(
        &materialization_id,
        &neoengram_domain::protocol::MaterializationBatchId::new("batch-v2").unwrap(),
        Generation::new(1),
        Generation::new(1),
        &namespace,
        object_id,
        &fallback_id,
    )
    .unwrap();
    let mut lease = object_read_lease_for(
        materialization_id.as_str(),
        namespace.as_str(),
        fallback_id.as_str(),
        lease_id.as_str(),
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    lease.placement_generation = PlacementGeneration::new(2);
    assert_eq!(
        repository
            .insert_object_read_lease(lease.clone())
            .await
            .unwrap(),
        lease
    );

    // The fallback's generation is intentionally different from the primary Batch generation.
    // Releasing by its deterministic identity proves that the lease was registered independently.
    let released = repository
        .release_object_read_lease(&tenant, &namespace, &lease_id)
        .await
        .unwrap()
        .expect("fallback read lease should be registered");
    assert_eq!(released.placement_id, fallback_id);
    assert_eq!(released.placement_generation, PlacementGeneration::new(2));
}

#[tokio::test]
async fn in_memory_materialization_is_namespace_scoped_and_idempotent() {
    let repository = Arc::new(InMemoryPlacementRepository::default());
    let object_set = object_set();
    repository
        .insert_commit_object_set(object_set.clone())
        .await
        .unwrap();
    let first = placement(ObjectId::from_bytes([1; 32]), "placement-v2-a", "volume-a");
    assert_eq!(
        repository
            .insert_object_placement_v2(first.clone())
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        repository
            .object_placements_v2(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &ObjectId::from_bytes([1; 32]),
            )
            .await
            .unwrap(),
        vec![first]
    );
    let coverage = VolumeCommitCoverage::from_placements(
        TenantId::new("tenant-v2").unwrap(),
        ObjectNamespaceId::new("artifact-v2").unwrap(),
        CommitId::from_bytes([9; 32]),
        StorageVolumeId::new("volume-a").unwrap(),
        PlacementGeneration::new(1),
        &object_set.object_set,
        &[placement(
            ObjectId::from_bytes([1; 32]),
            "placement-v2-a",
            "volume-a",
        )],
    )
    .unwrap();
    assert_eq!(coverage.state, CoverageState::Partial);
    repository
        .upsert_volume_commit_coverage(coverage)
        .await
        .unwrap();
    assert_eq!(
        repository
            .volume_commit_coverages(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &ContentDigest::from_bytes([9; 32]),
            )
            .await
            .unwrap()
            .len(),
        1
    );
    let materialization = job();
    repository
        .insert_materialization(materialization.clone())
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_materialization_by_key(&materialization.key)
            .await
            .unwrap(),
        Some(materialization)
    );
}

#[tokio::test]
async fn in_memory_receipt_is_idempotent_and_advances_authority() {
    let repository = InMemoryPlacementRepository::default();
    assert_receipt_semantics(&repository).await;
}

#[tokio::test]
async fn in_memory_receipt_deadline_is_enforced() {
    assert_receipt_deadline_is_enforced(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_receipt_replay_survives_deadline() {
    assert_receipt_replay_survives_deadline(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_receipt_converges_on_existing_placement() {
    assert_receipt_converges_on_existing_placement(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_receipt_progress_is_namespace_scoped() {
    assert_receipt_progress_is_namespace_scoped(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_completed_object_retry_repairs_coverage() {
    assert_completed_object_retry_repairs_coverage(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_competing_batch_converges_on_existing_placement() {
    assert_competing_batch_converges_on_existing_placement(&InMemoryPlacementRepository::default())
        .await;
}

#[tokio::test]
async fn control_plane_receipt_replay_survives_reconnect() {
    assert_control_plane_replay_survives_reconnect().await;
}

#[tokio::test]
async fn in_memory_receipt_rejects_invalid_object_state_without_publishing() {
    assert_receipt_rejects_invalid_object_state(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_receipt_replay_survives_replan() {
    assert_receipt_replay_survives_replan(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_replan_preserves_staging_checkpoint() {
    assert_replan_preserves_checkpoint(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_materialization_ids_are_tenant_scoped() {
    assert_materialization_ids_are_tenant_scoped(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_active_target_ignores_coverage_goal() {
    assert_active_target_ignores_coverage_goal(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_leases_are_namespace_scoped_and_require_active_state() {
    assert_lease_namespace_and_state_semantics(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_leases_follow_batch_fences() {
    assert_lease_batch_fence_semantics(&InMemoryPlacementRepository::default()).await;
}

#[tokio::test]
async fn in_memory_fallback_read_lease_uses_its_placement_fence() {
    assert_fallback_read_lease_uses_placement_fence(&InMemoryPlacementRepository::default()).await;
}

async fn assert_materialization_plan_publishes_as_one_aggregate(
    repository: &dyn PlacementRepository,
) {
    repository
        .insert_commit_object_set(object_set())
        .await
        .unwrap();
    repository
        .insert_object_placement_v2(placement(
            ObjectId::from_bytes([1; 32]),
            "source-placement-v2",
            "volume-source",
        ))
        .await
        .unwrap();

    let mut materialization = job();
    materialization.state = MaterializationJobState::Materializing;
    let batch = materialization_batch();
    let first_object = materialization_object();
    let second_object = MaterializationObject::new(
        materialization.materialization_id.clone(),
        ObjectRef::new(
            ObjectNamespaceId::new("artifact-v2").unwrap(),
            ObjectId::from_bytes([2; 32]),
            6,
            ObjectEncoding::Raw,
            1,
        ),
        Generation::new(1),
    );
    let source_lease_id = object_read_lease_id(
        &materialization.materialization_id,
        &batch.batch_id,
        batch.plan_revision,
        batch.batch_attempt,
        &ObjectNamespaceId::new("artifact-v2").unwrap(),
        first_object.object.object_id,
        &batch.source.placement_id,
    )
    .unwrap();
    let source_lease = object_read_lease_for(
        materialization.materialization_id.as_str(),
        "artifact-v2",
        "source-placement-v2",
        source_lease_id.as_str(),
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let staging_id = staging_lease_id(
        &materialization.materialization_id,
        batch.plan_revision,
        &ObjectNamespaceId::new("artifact-v2").unwrap(),
        first_object.object.object_id,
    )
    .unwrap();
    let staging = staging_lease_for(
        materialization.materialization_id.as_str(),
        "artifact-v2",
        "volume-target",
        staging_id.as_str(),
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Active,
    );
    let coverage = VolumeCommitCoverage::from_placements(
        TenantId::new("tenant-v2").unwrap(),
        ObjectNamespaceId::new("artifact-v2").unwrap(),
        materialization.key.commit_id,
        StorageVolumeId::new("volume-target").unwrap(),
        PlacementGeneration::new(1),
        &object_set().object_set,
        &[],
    )
    .unwrap();

    let plan = MaterializationPlan {
        job: materialization.clone(),
        batches: vec![batch.clone()],
        objects: vec![first_object.clone(), second_object],
        object_read_leases: vec![source_lease],
        staging_leases: vec![staging],
        coverage,
    };
    let outcome = repository
        .insert_materialization_plan(plan.clone())
        .await
        .unwrap();
    assert_eq!(
        outcome,
        MaterializationPlanInsertOutcome::Inserted(materialization.clone())
    );
    assert_eq!(
        repository
            .list_materialization_batches(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &materialization.materialization_id,
            )
            .await
            .unwrap(),
        vec![batch]
    );
    assert_eq!(
        repository
            .list_materialization_objects(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &materialization.materialization_id,
            )
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        repository.insert_materialization_plan(plan).await.unwrap(),
        MaterializationPlanInsertOutcome::Existing(materialization)
    );
}

async fn assert_materialization_plan_replacement_is_atomic(repository: &dyn PlacementRepository) {
    repository
        .insert_commit_object_set(object_set())
        .await
        .unwrap();
    repository
        .insert_object_placement_v2(placement(
            ObjectId::from_bytes([1; 32]),
            "source-placement-v2",
            "volume-source",
        ))
        .await
        .unwrap();
    let initial = aggregate_plan();
    repository
        .insert_materialization_plan(initial.clone())
        .await
        .unwrap();

    // Invalid replacements must be rejected before the parent CAS or any child retirement.
    let mut invalid_plan = replacement_plan(&initial);
    invalid_plan.objects.pop();
    assert!(repository
        .replace_materialization_plan(MaterializationPlanReplacement {
            expected_plan_revision: Generation::new(1),
            plan: invalid_plan,
        })
        .await
        .is_err());
    assert_eq!(
        repository
            .get_materialization(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &initial.job.materialization_id,
            )
            .await
            .unwrap(),
        Some(initial.job.clone())
    );
    assert_eq!(
        repository
            .list_materialization_batches(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &initial.job.materialization_id,
            )
            .await
            .unwrap(),
        initial.batches.clone()
    );
    assert_eq!(
        repository
            .list_materialization_objects(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &initial.job.materialization_id,
            )
            .await
            .unwrap(),
        initial.objects.clone()
    );

    let replacement = replacement_plan(&initial);
    assert_eq!(
        repository
            .replace_materialization_plan(MaterializationPlanReplacement {
                expected_plan_revision: Generation::new(1),
                plan: replacement.clone(),
            })
            .await
            .unwrap(),
        MaterializationPlanInsertOutcome::Inserted(replacement.job.clone())
    );
    let stored_job = repository
        .get_materialization(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &initial.job.materialization_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_job.plan_revision, Generation::new(2));
    let batches = repository
        .list_materialization_batches(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &initial.job.materialization_id,
        )
        .await
        .unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].state, MaterializationBatchState::Failed);
    assert_eq!(batches[1], replacement.batches[0]);
    let objects = repository
        .list_materialization_objects(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &initial.job.materialization_id,
        )
        .await
        .unwrap();
    assert_eq!(objects, replacement.objects);

    // Releasing an already-retired lease decodes its payload. This catches implementations that
    // update only the indexed state column and leave stale serialized state behind.
    let old_read = repository
        .release_object_read_lease(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &initial.object_read_leases[0].lease_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        old_read.state,
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Released
    );
    let old_staging = repository
        .release_staging_lease(
            &TenantId::new("tenant-v2").unwrap(),
            &ObjectNamespaceId::new("artifact-v2").unwrap(),
            &initial.staging_leases[0].lease_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        old_staging.state,
        neoengram_domain::protocol::materialization::MaterializationLeaseState::Released
    );
}

#[tokio::test]
async fn in_memory_materialization_plan_publishes_as_one_aggregate() {
    assert_materialization_plan_publishes_as_one_aggregate(&InMemoryPlacementRepository::default())
        .await;
}

#[tokio::test]
async fn in_memory_materialization_plan_replacement_is_atomic() {
    assert_materialization_plan_replacement_is_atomic(&InMemoryPlacementRepository::default())
        .await;
}

#[tokio::test]
async fn in_memory_materialization_lists_are_namespace_scoped() {
    assert_materialization_lists_are_namespace_scoped(&InMemoryPlacementRepository::default())
        .await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_materialization_plan_publishes_as_one_transaction() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_materialization_plan_publishes_as_one_aggregate(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_materialization_plan_replacement_is_atomic() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_materialization_plan_replacement_is_atomic(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_materialization_lists_are_namespace_scoped() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_materialization_lists_are_namespace_scoped(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_materialization_round_trip() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    let object_set = object_set();
    repository
        .insert_commit_object_set(object_set.clone())
        .await
        .unwrap();
    let materialization = job();
    repository
        .insert_materialization(materialization.clone())
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_materialization(
                &TenantId::new("tenant-v2").unwrap(),
                &ObjectNamespaceId::new("artifact-v2").unwrap(),
                &materialization.materialization_id,
            )
            .await
            .unwrap(),
        Some(materialization)
    );
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_receipt_is_idempotent_and_advances_authority() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_receipt_semantics(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_receipt_deadline_is_enforced() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_receipt_deadline_is_enforced(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_receipt_replay_survives_deadline() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_receipt_replay_survives_deadline(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_receipt_converges_on_existing_placement() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_receipt_converges_on_existing_placement(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_receipt_progress_is_namespace_scoped() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_receipt_progress_is_namespace_scoped(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_completed_object_retry_repairs_coverage() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_completed_object_retry_repairs_coverage(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_competing_batch_converges_on_existing_placement() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_competing_batch_converges_on_existing_placement(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_receipt_rejects_invalid_object_state_without_publishing() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_receipt_rejects_invalid_object_state(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_receipt_replay_survives_replan() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_receipt_replay_survives_replan(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_replan_preserves_staging_checkpoint() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_replan_preserves_checkpoint(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_materialization_ids_are_tenant_scoped() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_materialization_ids_are_tenant_scoped(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_active_target_ignores_coverage_goal() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_active_target_ignores_coverage_goal(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_leases_are_namespace_scoped_and_require_active_state() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_lease_namespace_and_state_semantics(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_leases_follow_batch_fences() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_lease_batch_fence_semantics(repository.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_fallback_read_lease_indexes_its_placement_volume() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = neoengram_central::open_sqlite_authority(
        neoengram_central::SqliteAuthorityConfig::new(directory.path()),
    )
    .await
    .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    assert_fallback_read_lease_uses_placement_fence(repository.as_ref()).await;

    let namespace = ObjectNamespaceId::new("artifact-v2").unwrap();
    let lease_id = object_read_lease_id(
        &neoengram_domain::protocol::MaterializationId::new("materialization-v2").unwrap(),
        &neoengram_domain::protocol::MaterializationBatchId::new("batch-v2").unwrap(),
        Generation::new(1),
        Generation::new(1),
        &namespace,
        ObjectId::from_bytes([1; 32]),
        &PlacementId::new("source-placement-fallback").unwrap(),
    )
    .unwrap();
    let options = SqliteConnectOptions::new()
        .filename(directory.path().join("authority.sqlite3"))
        .create_if_missing(false);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    let indexed_volume: String = sqlx::query_scalar(
        "SELECT storage_volume_id FROM object_read_leases WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ?",
    )
    .bind("tenant-v2")
    .bind(namespace.as_str())
    .bind(lease_id.as_str())
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(indexed_volume, "volume-source-fallback");
    connection.close().await.unwrap();
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[tokio::test]
async fn coverage_query_reports_an_empty_target_as_partial() {
    let components = InMemoryComponents::new(1_000);
    let object_set = object_set();
    let tenant_id = object_set.tenant_id.clone();
    let namespace = ObjectNamespaceId::new("artifact-v2").unwrap();
    let target_volume = StorageVolumeId::new("volume-empty-target").unwrap();
    components
        .control_catalog
        .insert_tenant(tenant_record(&tenant_id))
        .await
        .unwrap();
    components
        .placement
        .insert_commit_object_set(object_set.clone())
        .await
        .unwrap();
    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-v2",
            [tenant_id.to_string()],
            [Permission::ArtifactCommitReplicate],
        )
        .unwrap(),
    );
    let service = CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy,
        components.clock.clone(),
    )
    .with_placement_repository(components.placement.clone());
    let identity =
        AuthenticatedIdentity::new("user-v2", PrincipalKind::User, "test", "subject-v2").unwrap();

    let request = || QueryCommitCoverageRequest {
        tenant_id: tenant_id.to_string(),
        object_namespace_id: namespace.to_string(),
        commit_id: object_set.commit_id.to_string(),
        storage_volume_id: Some(target_volume.to_string()),
        cursor: None,
        page_size: None,
    };
    let empty = service
        .query_commit_coverage(&identity, request())
        .await
        .unwrap();
    assert_eq!(empty.coverage.len(), 1);
    assert_eq!(
        empty.coverage[0].storage_volume_id,
        target_volume.to_string()
    );
    assert_eq!(empty.coverage[0].state, "partial");
    assert_eq!(empty.coverage[0].verified_objects, "0");
    assert_eq!(empty.coverage[0].total_objects, "2");
    assert_eq!(empty.coverage[0].verified_bytes, "0");
    assert_eq!(empty.coverage[0].total_bytes, "10");

    components
        .placement
        .insert_object_placement_v2(placement(
            ObjectId::from_bytes([1; 32]),
            "target-placement-v2",
            target_volume.as_str(),
        ))
        .await
        .unwrap();
    let partial = service
        .query_commit_coverage(&identity, request())
        .await
        .unwrap();
    assert_eq!(partial.coverage.len(), 1);
    assert_eq!(partial.coverage[0].state, "partial");
    assert_eq!(partial.coverage[0].verified_objects, "1");
    assert_eq!(partial.coverage[0].verified_bytes, "4");
}
