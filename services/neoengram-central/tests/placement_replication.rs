use std::sync::Arc;

use neoengram_central::{
    CancelReplicationRequest, CentralErrorCode, InMemoryComponents, PlacementRepository,
    ReplicationObjectRecord, ReplicationRecord, ReplicationStateTransitionRequest,
    RetryReplicationRequest,
};
use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    ArtifactId, BackendId, CommitObjectSet, CommitPlacementSet, CommitPlacementSetState,
    DataHealth, DecimalU64, ObjectSet, PlacementGeneration, PlacementSetId, ReplicationId,
    ReplicationObjectState, ReplicationState, RequestId, StorageVolumeId, TenantId, UnixMillis,
};

#[cfg(feature = "authority-sqlite")]
use neoengram_central::{open_sqlite_authority, SqliteAuthorityConfig};

#[tokio::test]
async fn in_memory_replication_edges_follow_repository_contract() {
    let components = InMemoryComponents::new(10);
    run_replication_edge_contract(components.placement).await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_replication_edges_follow_repository_contract() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    run_replication_edge_contract(authority.authority_store().placement().unwrap()).await;
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_same_state_progress_cas_never_overwrites_a_successful_newer_report() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    let tenant_id = TenantId::new("tenant-progress-cas").unwrap();
    let mut record = replication_record(
        &tenant_id,
        "progress-cas",
        ContentDigest::from_bytes([41; 32]),
        "backend-progress",
        "volume-progress",
        ReplicationState::Transferring,
        1,
    );
    record.total_objects = 64;
    record.total_bytes = 64;
    repository.insert_replication(record.clone()).await.unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(65));
    let mut reports = Vec::new();
    for progress in (1_u64..=64).rev() {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let tenant_id = tenant_id.clone();
        let replication_id = record.replication_id.clone();
        reports.push(tokio::spawn(async move {
            barrier.wait().await;
            let result = repository
                .transition_replication(ReplicationStateTransitionRequest {
                    tenant_id,
                    replication_id,
                    expected_state: ReplicationState::Transferring,
                    expected_attempt: 1,
                    next_state: ReplicationState::Transferring,
                    completed_objects: progress,
                    completed_bytes: progress,
                    issue_code: None,
                    issue_message: None,
                    updated_at_unix_ms: UnixMillis::new(100 + progress),
                })
                .await;
            (progress, result)
        }));
    }
    barrier.wait().await;

    let mut max_success = None;
    for report in reports {
        let (progress, result) = report.await.unwrap();
        match result {
            Ok(_) => max_success = Some(max_success.unwrap_or(0).max(progress)),
            Err(error) => assert_eq!(error.code(), CentralErrorCode::ConcurrentUpdate),
        }
    }
    let final_record = repository
        .get_replication(&tenant_id, &record.replication_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_record.completed_objects, max_success.unwrap());
    assert_eq!(final_record.completed_bytes, max_success.unwrap());
    authority.close().await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_replication_retry_receipt_survives_reopen() {
    let directory = tempfile::TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = authority.authority_store().placement().unwrap();
    let tenant_id = TenantId::new("tenant-retry-reopen").unwrap();
    let record = replication_record(
        &tenant_id,
        "retry-reopen",
        ContentDigest::from_bytes([39; 32]),
        "backend-retry-reopen",
        "volume-retry-reopen",
        ReplicationState::Failed,
        1,
    );
    repository.insert_replication(record.clone()).await.unwrap();
    let request = RetryReplicationRequest {
        tenant_id: tenant_id.clone(),
        replication_id: record.replication_id.clone(),
        expected_attempt: 1,
        request_id: RequestId::new("retry-reopen-request").unwrap(),
        updated_at_unix_ms: UnixMillis::new(20),
    };
    let first = repository.retry_replication(request.clone()).await.unwrap();
    assert!(!first.replayed);
    authority.close().await;

    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let replay = reopened
        .authority_store()
        .placement()
        .unwrap()
        .retry_replication(request)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.replication, first.replication);
    reopened.close().await;
}

async fn run_replication_edge_contract(repository: Arc<dyn PlacementRepository>) {
    let tenant_id = TenantId::new("tenant-replication-contract").unwrap();
    let commit_id = ContentDigest::from_bytes([31; 32]);
    let first = replication_record(
        &tenant_id,
        "first",
        commit_id,
        "backend-shared",
        "volume-shared",
        ReplicationState::Queued,
        1,
    );
    let second = replication_record(
        &tenant_id,
        "second",
        commit_id,
        "backend-shared",
        "volume-shared",
        ReplicationState::Queued,
        1,
    );
    repository.insert_replication(first.clone()).await.unwrap();
    let duplicate = repository
        .insert_replication(second.clone())
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), CentralErrorCode::ReplicationAlreadyActive);

    repository
        .transition_replication(ReplicationStateTransitionRequest {
            tenant_id: tenant_id.clone(),
            replication_id: first.replication_id.clone(),
            expected_state: ReplicationState::Queued,
            expected_attempt: 1,
            next_state: ReplicationState::Failed,
            completed_objects: 0,
            completed_bytes: 0,
            issue_code: Some("TRANSFER_FAILED".to_owned()),
            issue_message: Some("test failure".to_owned()),
            updated_at_unix_ms: UnixMillis::new(11),
        })
        .await
        .unwrap();

    let retryable = replication_record(
        &tenant_id,
        "retryable",
        ContentDigest::from_bytes([37; 32]),
        "backend-retry",
        "volume-retry",
        ReplicationState::Failed,
        1,
    );
    repository
        .insert_replication(retryable.clone())
        .await
        .unwrap();
    let retry_request = RetryReplicationRequest {
        tenant_id: tenant_id.clone(),
        replication_id: retryable.replication_id.clone(),
        expected_attempt: 1,
        request_id: RequestId::new("retry-request-1").unwrap(),
        updated_at_unix_ms: UnixMillis::new(20),
    };
    let first_retry = repository
        .retry_replication(retry_request.clone())
        .await
        .unwrap();
    assert!(!first_retry.replayed);
    assert_eq!(first_retry.replication.attempt, 2);
    assert_eq!(first_retry.replication.state, ReplicationState::Queued);
    repository
        .transition_replication(ReplicationStateTransitionRequest {
            tenant_id: tenant_id.clone(),
            replication_id: retryable.replication_id.clone(),
            expected_state: ReplicationState::Queued,
            expected_attempt: 2,
            next_state: ReplicationState::Planning,
            completed_objects: 0,
            completed_bytes: 0,
            issue_code: None,
            issue_message: None,
            updated_at_unix_ms: UnixMillis::new(21),
        })
        .await
        .unwrap();
    let replay = repository
        .retry_replication(RetryReplicationRequest {
            updated_at_unix_ms: UnixMillis::new(22),
            ..retry_request
        })
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.replication.attempt, 2);
    assert_eq!(replay.replication.state, ReplicationState::Queued);

    let conflicting_retry = repository
        .retry_replication(RetryReplicationRequest {
            tenant_id: tenant_id.clone(),
            replication_id: first.replication_id.clone(),
            expected_attempt: 1,
            request_id: RequestId::new("retry-request-1").unwrap(),
            updated_at_unix_ms: UnixMillis::new(20),
        })
        .await
        .unwrap_err();
    assert_eq!(
        conflicting_retry.code(),
        CentralErrorCode::ReplicationRetryRequestReused
    );

    repository.insert_replication(second).await.unwrap();

    // A target PlacementSet is a durable claim even when it is not currently Published.  The
    // repository must reject a new active replication at this boundary; checking only in the
    // service leaves a race between create and finalize on SQLite and with concurrent in-memory users.
    let claimed_commit = ContentDigest::from_bytes([36; 32]);
    let claimed_object_set = ObjectSet::new(Vec::new()).unwrap();
    repository
        .insert_placement_set(CommitPlacementSet {
            placement_set_id: PlacementSetId::new("placement-set-claimed").unwrap(),
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(claimed_commit),
            backend_id: BackendId::new("backend-claimed").unwrap(),
            storage_volume_id: Some(StorageVolumeId::new("volume-claimed").unwrap()),
            archive_id: None,
            object_set_digest: claimed_object_set.object_set_digest,
            object_count: DecimalU64::new(0),
            verified_object_count: DecimalU64::new(0),
            placement_generation: PlacementGeneration::new(1),
            state: CommitPlacementSetState::Staged,
        })
        .await
        .unwrap();
    let claimed_replication = replication_record(
        &tenant_id,
        "claimed-target",
        claimed_commit,
        "backend-claimed",
        "volume-claimed",
        ReplicationState::Queued,
        1,
    );
    let claimed_error = repository
        .insert_replication(claimed_replication)
        .await
        .unwrap_err();
    assert_eq!(
        claimed_error.code(),
        CentralErrorCode::ReplicationAlreadyActive
    );

    // A failed replication can be retried only while its target remains unclaimed. The durable
    // PlacementSet check must run inside the retry mutation, not only during initial scheduling.
    let retry_claimed = replication_record(
        &tenant_id,
        "retry-claimed-target",
        claimed_commit,
        "backend-claimed",
        "volume-claimed",
        ReplicationState::Failed,
        1,
    );
    repository
        .insert_replication(retry_claimed.clone())
        .await
        .unwrap();
    let retry_claimed_error = repository
        .retry_replication(RetryReplicationRequest {
            tenant_id: tenant_id.clone(),
            replication_id: retry_claimed.replication_id,
            expected_attempt: 1,
            request_id: RequestId::new("retry-claimed-request").unwrap(),
            updated_at_unix_ms: UnixMillis::new(30),
        })
        .await
        .unwrap_err();
    assert_eq!(
        retry_claimed_error.code(),
        CentralErrorCode::ReplicationAlreadyActive
    );

    let attempt_fenced = replication_record(
        &tenant_id,
        "attempt-fenced",
        ContentDigest::from_bytes([32; 32]),
        "backend-attempt",
        "volume-attempt",
        ReplicationState::Queued,
        2,
    );
    repository
        .insert_replication(attempt_fenced.clone())
        .await
        .unwrap();
    let stale_checkpoint = repository
        .upsert_replication_object(ReplicationObjectRecord {
            tenant_id: tenant_id.clone(),
            replication_id: attempt_fenced.replication_id,
            object_id: ObjectId::from_bytes([33; 32]),
            offset: 0,
            state: ReplicationObjectState::Queued,
            retry_count: 1,
            updated_at_unix_ms: UnixMillis::new(11),
        })
        .await
        .unwrap_err();
    assert_eq!(stale_checkpoint.code(), CentralErrorCode::ConcurrentUpdate);

    let cancelled = replication_record(
        &tenant_id,
        "cancelled",
        ContentDigest::from_bytes([34; 32]),
        "backend-cancelled",
        "volume-cancelled",
        ReplicationState::Cancelled,
        2,
    );
    repository
        .insert_replication(cancelled.clone())
        .await
        .unwrap();
    let stale_cancel = repository
        .cancel_replication(CancelReplicationRequest {
            tenant_id: tenant_id.clone(),
            replication_id: cancelled.replication_id,
            expected_attempt: 1,
            updated_at_unix_ms: UnixMillis::new(11),
        })
        .await
        .unwrap_err();
    assert_eq!(stale_cancel.code(), CentralErrorCode::ConcurrentUpdate);

    let empty_commit = ContentDigest::from_bytes([35; 32]);
    let empty_set = ObjectSet::new(Vec::new()).unwrap();
    repository
        .insert_commit_object_set(CommitObjectSet {
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(empty_commit),
            object_set: empty_set.clone(),
        })
        .await
        .unwrap();
    let empty_volume = StorageVolumeId::new("volume-empty").unwrap();
    repository
        .insert_placement_set(CommitPlacementSet {
            placement_set_id: PlacementSetId::new("placement-set-empty").unwrap(),
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(empty_commit),
            backend_id: BackendId::new("backend-empty").unwrap(),
            storage_volume_id: Some(empty_volume.clone()),
            archive_id: None,
            object_set_digest: empty_set.object_set_digest,
            object_count: DecimalU64::new(0),
            verified_object_count: DecimalU64::new(0),
            placement_generation: PlacementGeneration::new(1),
            state: CommitPlacementSetState::Published,
        })
        .await
        .unwrap();
    let availability = repository
        .commit_availability(&tenant_id, &empty_commit)
        .await
        .unwrap();
    assert_eq!(availability.data_health, DataHealth::Available);
    assert_eq!(availability.verified_placements, 1);
    assert_eq!(availability.verified_storage_volume_ids, vec![empty_volume]);
}

fn replication_record(
    tenant_id: &TenantId,
    suffix: &str,
    commit_id: ContentDigest,
    backend_id: &str,
    volume_id: &str,
    state: ReplicationState,
    attempt: u64,
) -> ReplicationRecord {
    ReplicationRecord {
        tenant_id: tenant_id.clone(),
        replication_id: ReplicationId::new(format!("replication-{suffix}")).unwrap(),
        artifact_id: Some(ArtifactId::new("artifact-a").unwrap()),
        commit_id,
        target_backend_id: backend_id.to_owned(),
        target_storage_volume_id: StorageVolumeId::new(volume_id).unwrap(),
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
        target_edge_cluster_id: None,
        target_gateway_pool_id: None,
        target_placement_generation: None,
        target_agent_id: None,
        target_session_generation: None,
        target_mount_generation: None,
        target_route_generation: None,
        transfer_route_id: None,
        transfer_id: None,
        target_placement_set_id: None,
        staging_id: None,
        object_set_digest: ContentDigest::from_bytes([36; 32]),
        state,
        request_id: RequestId::new(format!("request-{suffix}")).unwrap(),
        attempt,
        completed_objects: 0,
        total_objects: 1,
        completed_bytes: 0,
        total_bytes: 1,
        issue_code: None,
        issue_message: None,
        created_at_unix_ms: UnixMillis::new(10),
        updated_at_unix_ms: UnixMillis::new(10),
    }
}
