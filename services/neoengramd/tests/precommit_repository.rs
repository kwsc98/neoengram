use neoengram_core::{
    Commit, CommitId, ContentDigest, DirectoryId, FileRecord, IndexVersion, LogicalPath, ManifestId,
};
use neoengram_protocol::{
    ArtifactId, AssignmentGeneration, AssignmentId, ControlError, DecisionGeneration, ErrorCode,
    Extensions, IndexRevision, JobDecision, JobId, JobState, PrincipalId, PrincipalKind,
    PrincipalRef, ProjectId, PublishDecision, RequestId, ResourceVersion, TenantId, UnixMillis,
    WireIndexVersion,
};
use neoengramd::{
    AddJobSpec, AuthorityStore, CentralErrorCode, CommitRecord, InMemoryComponents, JobOperation,
    JobRecord, PreCommitCancelRequest, PreCommitCommitRequest, PreCommitId, PreCommitKey,
    PreCommitRestartRequest, PreCommitStartRequest, PreCommitState, PublishedIndex,
};

#[cfg(feature = "authority-sqlite")]
use neoengramd::{open_sqlite_authority, SqliteAuthorityConfig};

struct ContractResult {
    key: PreCommitKey,
    commit_id: CommitId,
}

#[tokio::test]
async fn precommit_contract_runs_against_memory() {
    let components = InMemoryComponents::new(100);
    run_contract(components.authority_store()).await;
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn precommit_contract_runs_against_sqlite_and_recovers() {
    let directory = tempfile::TempDir::new().unwrap();
    let result = {
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let result = run_contract(authority.authority_store()).await;
        authority.integrity_check().await.unwrap();
        result
    };

    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = reopened.authority_store().precommits().unwrap();
    let precommit = repository.get(&result.key).await.unwrap().unwrap();
    assert_eq!(precommit.state, PreCommitState::Committed);
    assert_eq!(precommit.committed_commit_id, Some(result.commit_id));
    assert_eq!(
        precommit.head_published_at_unix_ms,
        Some(UnixMillis::new(350))
    );
    assert_eq!(precommit.candidate_records, Some(frozen_records()));
    let diff_summary = precommit.diff_summary.expect("persisted root diff summary");
    assert_eq!(diff_summary.files_added, 2);
    assert_eq!(diff_summary.bytes_added, 10);
    let commit = repository
        .get_commit(
            &result.key.tenant_id,
            &ProjectId::new("project-a").unwrap(),
            &ArtifactId::new("artifact-a").unwrap(),
            result.commit_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(commit.message, "publish candidate");
    assert_eq!(commit.root_directory_id, DirectoryId::from_bytes([6; 32]));
    assert_eq!(commit.records, frozen_records());
    assert_eq!(
        commit
            .source_storage_volume_id
            .as_ref()
            .map(|id| id.as_str()),
        Some("volume-a")
    );
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_migrates_v4_to_v5_without_authority_state_loss() {
    use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};

    let directory = tempfile::TempDir::new().unwrap();
    {
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        authority.close().await;
    }
    let options = SqliteConnectOptions::new()
        .filename(directory.path().join("authority.sqlite3"))
        .foreign_keys(false);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE precommit_mutations;
         DROP TABLE commit_records;
         DROP TABLE precommit_records;
         DROP TABLE object_placements;
         PRAGMA user_version = 4;",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let migrated = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    migrated.integrity_check().await.unwrap();
    assert!(migrated.authority_store().precommits().is_some());
}

async fn run_contract(store: AuthorityStore) -> ContractResult {
    let repository = store.precommits().expect("Pre-commit repository");
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = neoengram_protocol::PlaygroundId::new("playground-a").unwrap();
    let source = version(4, 4);
    let records = frozen_records();
    let candidate = WireIndexVersion::from(IndexVersion::from_snapshot(5, &records).unwrap());
    let key = PreCommitKey::new(tenant_id.clone(), PreCommitId::new("precommit-a").unwrap());
    let start = PreCommitStartRequest {
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
        precommit_id: key.precommit_id.clone(),
        precommit_request_id: RequestId::new("start-a").unwrap(),
        source_index_version: source.clone(),
        frozen_head_commit_id: None,
        job_id: JobId::new("job-a").unwrap(),
        created_at_unix_ms: UnixMillis::new(100),
    };
    let started = repository.start(start.clone()).await.unwrap();
    assert!(!started.replayed);
    assert_eq!(started.precommit.state, PreCommitState::Running);
    assert_eq!(started.precommit.attempt, 1);
    assert_eq!(
        repository
            .get_active(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .unwrap()
            .unwrap()
            .precommit_id,
        key.precommit_id
    );
    assert_eq!(repository.list_running(None, 10).await.unwrap().len(), 1);

    let mut overlapping = start.clone();
    overlapping.precommit_id = PreCommitId::new("precommit-overlap").unwrap();
    overlapping.precommit_request_id = RequestId::new("start-overlap").unwrap();
    overlapping.job_id = JobId::new("job-overlap").unwrap();
    assert_eq!(
        repository.start(overlapping).await.unwrap_err().code(),
        CentralErrorCode::ConcurrentUpdate
    );

    let mut replay = start.clone();
    replay.precommit_id = PreCommitId::new("discarded-generated-id").unwrap();
    replay.job_id = JobId::new("discarded-generated-job").unwrap();
    replay.frozen_head_commit_id = Some(CommitId::from_bytes([9; 32]));
    replay.created_at_unix_ms = UnixMillis::new(999);
    let replayed = repository.start(replay).await.unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.precommit, started.precommit);

    let mut conflicting_start = start;
    conflicting_start.source_index_version = version(6, 6);
    assert_eq!(
        repository
            .start(conflicting_start)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );
    assert!(repository
        .get(&PreCommitKey::new(
            TenantId::new("tenant-b").unwrap(),
            key.precommit_id.clone(),
        ))
        .await
        .unwrap()
        .is_none());

    let mut queued = terminal_job(
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
        &key,
        JobId::new("job-a").unwrap(),
        source.clone(),
        JobState::Succeeded,
        PublishDecision::Publish {
            published_index_version: candidate.clone(),
            extensions: Extensions::new(),
        },
    );
    queued.state = JobState::Queued;
    queued.decision = None;
    assert_eq!(
        repository
            .sync_job(
                queued.clone(),
                Some(PublishedIndex {
                    version: candidate.clone(),
                    records: records.clone(),
                }),
                UnixMillis::new(140),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ProtocolInvalid
    );
    let unchanged = repository
        .sync_job(queued.clone(), None, UnixMillis::new(150))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        unchanged.resource_version,
        started.precommit.resource_version
    );
    assert_eq!(
        unchanged.updated_at_unix_ms,
        started.precommit.updated_at_unix_ms
    );
    assert_eq!(
        repository
            .sync_job(queued, None, UnixMillis::new(160))
            .await
            .unwrap()
            .unwrap(),
        unchanged
    );

    let succeeded = terminal_job(
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
        &key,
        JobId::new("job-a").unwrap(),
        source.clone(),
        JobState::Succeeded,
        PublishDecision::Publish {
            published_index_version: candidate.clone(),
            extensions: Extensions::new(),
        },
    );
    let mut invalid_published = PublishedIndex {
        version: candidate.clone(),
        records: records.clone(),
    };
    invalid_published.records.reverse();
    assert_eq!(
        repository
            .sync_job(succeeded.clone(), None, UnixMillis::new(180))
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ProtocolInvalid
    );
    assert_eq!(
        repository
            .sync_job(
                succeeded.clone(),
                Some(invalid_published),
                UnixMillis::new(190),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ProtocolInvalid
    );
    let published = PublishedIndex {
        version: candidate.clone(),
        records: records.clone(),
    };
    let ready = repository
        .sync_job(
            succeeded.clone(),
            Some(published.clone()),
            UnixMillis::new(200),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.state, PreCommitState::Ready);
    assert_eq!(ready.candidate_index_version, Some(candidate.clone()));
    assert_eq!(ready.candidate_records, Some(records.clone()));
    let diff_summary = ready.diff_summary.as_ref().expect("root diff summary");
    assert_eq!(diff_summary.files_added, 2);
    assert_eq!(diff_summary.bytes_added, 10);
    assert!(repository.list_running(None, 10).await.unwrap().is_empty());
    assert_eq!(
        repository
            .sync_job(succeeded, Some(published), UnixMillis::new(210))
            .await
            .unwrap()
            .unwrap(),
        ready
    );

    let root_directory_id = DirectoryId::from_bytes([6; 32]);
    let created_at_unix_ms = UnixMillis::new(300);
    let commit_id = Commit::new(
        root_directory_id,
        None,
        "publish candidate",
        created_at_unix_ms.get(),
    )
    .unwrap()
    .canonical_id()
    .unwrap();
    let commit_request = PreCommitCommitRequest {
        key: key.clone(),
        expected_candidate_index_version: candidate.clone(),
        commit: CommitRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            source_playground_id: playground_id.clone(),
            source_storage_volume_id: Some(
                neoengram_protocol::StorageVolumeId::new("volume-a").unwrap(),
            ),
            source_precommit_id: key.precommit_id.clone(),
            commit_request_id: RequestId::new("commit-a").unwrap(),
            commit_id,
            root_directory_id,
            parent_commit_id: None,
            index_version: candidate,
            records: records.clone(),
            message: "publish candidate".to_owned(),
            description: Some("validated candidate".to_owned()),
            tag_names: vec!["reviewed".to_owned()],
            created_at_unix_ms,
        },
    };

    let mut invalid_snapshot = commit_request.clone();
    invalid_snapshot.commit.commit_request_id = RequestId::new("commit-bad-snapshot").unwrap();
    invalid_snapshot.commit.records.reverse();
    assert_eq!(
        repository
            .commit(invalid_snapshot)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );

    let mut invalid_commit_id = commit_request.clone();
    invalid_commit_id.commit.commit_request_id = RequestId::new("commit-bad-id").unwrap();
    invalid_commit_id.commit.commit_id = CommitId::from_bytes([8; 32]);
    assert_eq!(
        repository
            .commit(invalid_commit_id)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ProtocolInvalid
    );

    let mut invalid_snapshot_digest = commit_request.clone();
    invalid_snapshot_digest.commit.commit_request_id =
        RequestId::new("commit-bad-snapshot-digest").unwrap();
    invalid_snapshot_digest.commit.records[0].total_size += 1;
    assert_eq!(
        repository
            .commit(invalid_snapshot_digest)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );

    let committed = repository.commit(commit_request.clone()).await.unwrap();
    assert!(!committed.replayed);
    assert_eq!(
        committed.consumed_precommit.state,
        PreCommitState::Committed
    );
    assert_eq!(committed.commit.commit_id, commit_id);
    assert!(repository
        .get_active(&tenant_id, &project_id, &artifact_id, &playground_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository
            .list_unpublished_commits(None, 10)
            .await
            .unwrap()
            .as_slice(),
        &[committed.consumed_precommit.clone()]
    );
    assert!(repository
        .list_unpublished_commits(Some(&key), 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        repository
            .acknowledge_head_publication(
                &key,
                CommitId::from_bytes([99; 32]),
                UnixMillis::new(340),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );
    let acknowledged = repository
        .acknowledge_head_publication(&key, commit_id, UnixMillis::new(350))
        .await
        .unwrap();
    assert_eq!(
        acknowledged.head_published_at_unix_ms,
        Some(UnixMillis::new(350))
    );
    let acknowledgement_replay = repository
        .acknowledge_head_publication(&key, commit_id, UnixMillis::new(999))
        .await
        .unwrap();
    assert_eq!(acknowledgement_replay, acknowledged);
    assert!(repository
        .list_unpublished_commits(None, 10)
        .await
        .unwrap()
        .is_empty());

    let mut commit_replay = commit_request.clone();
    commit_replay.commit.commit_id = CommitId::from_bytes([8; 32]);
    commit_replay.commit.created_at_unix_ms = UnixMillis::new(999);
    let committed_replay = repository.commit(commit_replay).await.unwrap();
    assert!(committed_replay.replayed);
    assert_eq!(committed_replay.commit.commit_id, commit_id);

    let mut authority_conflict = commit_request.clone();
    authority_conflict.commit.root_directory_id = DirectoryId::from_bytes([19; 32]);
    assert_eq!(
        repository
            .commit(authority_conflict)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );

    let mut snapshot_conflict = commit_request.clone();
    snapshot_conflict.commit.records.pop();
    assert_eq!(
        repository
            .commit(snapshot_conflict)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );

    let mut commit_conflict = commit_request;
    commit_conflict.commit.message = "another payload".to_owned();
    assert_eq!(
        repository.commit(commit_conflict).await.unwrap_err().code(),
        CentralErrorCode::ConcurrentUpdate
    );

    exercise_missing_frozen_head(
        repository.as_ref(),
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
    )
    .await;
    exercise_restart_and_cancel(
        repository.as_ref(),
        tenant_id,
        project_id,
        artifact_id,
        playground_id,
    )
    .await;
    ContractResult { key, commit_id }
}

async fn exercise_missing_frozen_head(
    repository: &dyn neoengramd::PreCommitRepository,
    tenant_id: &TenantId,
    project_id: &ProjectId,
    artifact_id: &ArtifactId,
    playground_id: &neoengram_protocol::PlaygroundId,
) {
    let source = version(8, 8);
    let records = frozen_records();
    let candidate = WireIndexVersion::from(IndexVersion::from_snapshot(9, &records).unwrap());
    let key = PreCommitKey::new(
        tenant_id.clone(),
        PreCommitId::new("precommit-missing-head").unwrap(),
    );
    let job_id = JobId::new("job-missing-head").unwrap();
    repository
        .start(PreCommitStartRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            precommit_id: key.precommit_id.clone(),
            precommit_request_id: RequestId::new("start-missing-head").unwrap(),
            source_index_version: source.clone(),
            frozen_head_commit_id: Some(CommitId::from_bytes([98; 32])),
            job_id: job_id.clone(),
            created_at_unix_ms: UnixMillis::new(360),
        })
        .await
        .unwrap();
    let succeeded = terminal_job(
        tenant_id,
        project_id,
        artifact_id,
        playground_id,
        &key,
        job_id,
        source,
        JobState::Succeeded,
        PublishDecision::Publish {
            published_index_version: candidate.clone(),
            extensions: Extensions::new(),
        },
    );
    assert_eq!(
        repository
            .sync_job(
                succeeded,
                Some(PublishedIndex {
                    version: candidate,
                    records,
                }),
                UnixMillis::new(370),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ProtocolInvalid
    );
    let stored = repository.get(&key).await.unwrap().unwrap();
    assert_eq!(stored.state, PreCommitState::Running);
    assert!(stored.diff_summary.is_none());
    repository
        .cancel(PreCommitCancelRequest {
            key,
            cancel_request_id: RequestId::new("cancel-missing-head").unwrap(),
            cancelled_at_unix_ms: UnixMillis::new(380),
        })
        .await
        .unwrap();
}

fn frozen_records() -> Vec<FileRecord> {
    vec![
        FileRecord::new(
            LogicalPath::parse("dataset/a.bin").unwrap(),
            ManifestId::from_bytes([11; 32]),
            10,
            1,
        )
        .unwrap(),
        FileRecord::new(
            LogicalPath::parse("dataset/b.bin").unwrap(),
            ManifestId::from_bytes([12; 32]),
            0,
            0,
        )
        .unwrap(),
    ]
}

async fn exercise_restart_and_cancel(
    repository: &dyn neoengramd::PreCommitRepository,
    tenant_id: TenantId,
    project_id: ProjectId,
    artifact_id: ArtifactId,
    playground_id: neoengram_protocol::PlaygroundId,
) {
    let source = version(10, 10);
    let key = PreCommitKey::new(
        tenant_id.clone(),
        PreCommitId::new("precommit-retry").unwrap(),
    );
    repository
        .start(PreCommitStartRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            precommit_id: key.precommit_id.clone(),
            precommit_request_id: RequestId::new("start-retry").unwrap(),
            source_index_version: source.clone(),
            frozen_head_commit_id: None,
            job_id: JobId::new("job-failed").unwrap(),
            created_at_unix_ms: UnixMillis::new(400),
        })
        .await
        .unwrap();
    let failed = terminal_job(
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
        &key,
        JobId::new("job-failed").unwrap(),
        source,
        JobState::Failed,
        PublishDecision::Reject {
            error: ControlError {
                code: ErrorCode::new("SCAN_FAILED").unwrap(),
                message: "scan failed".to_owned(),
                retryable: true,
                retry_after_ms: None,
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        },
    );
    let abnormal = repository
        .sync_job(failed, None, UnixMillis::new(500))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(abnormal.state, PreCommitState::Abnormal);
    assert_eq!(abnormal.blockers[0].code, "SCAN_FAILED");
    assert_eq!(
        repository
            .get_active(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .unwrap()
            .unwrap()
            .precommit_id,
        key.precommit_id
    );

    let restart = PreCommitRestartRequest {
        key: key.clone(),
        restart_request_id: RequestId::new("restart-a").unwrap(),
        source_index_version: version(11, 11),
        frozen_head_commit_id: None,
        job_id: JobId::new("job-restarted").unwrap(),
        restarted_at_unix_ms: UnixMillis::new(600),
    };
    let restarted = repository.restart(restart.clone()).await.unwrap();
    assert_eq!(restarted.precommit.attempt, 2);
    assert_eq!(restarted.precommit.state, PreCommitState::Running);
    assert!(repository.restart(restart).await.unwrap().replayed);
    assert_eq!(repository.list_running(None, 10).await.unwrap().len(), 1);

    let cancel = PreCommitCancelRequest {
        key: key.clone(),
        cancel_request_id: RequestId::new("cancel-a").unwrap(),
        cancelled_at_unix_ms: UnixMillis::new(700),
    };
    let cancelled = repository.cancel(cancel.clone()).await.unwrap();
    assert_eq!(cancelled.precommit.state, PreCommitState::Cancelled);
    assert!(repository.cancel(cancel).await.unwrap().replayed);
    assert!(repository
        .get_active(&tenant_id, &project_id, &artifact_id, &playground_id)
        .await
        .unwrap()
        .is_none());
    assert!(repository.list_running(None, 10).await.unwrap().is_empty());
}

#[allow(clippy::too_many_arguments)]
fn terminal_job(
    tenant_id: &TenantId,
    project_id: &ProjectId,
    artifact_id: &ArtifactId,
    playground_id: &neoengram_protocol::PlaygroundId,
    _key: &PreCommitKey,
    job_id: JobId,
    expected_index_version: WireIndexVersion,
    state: JobState,
    decision: PublishDecision,
) -> JobRecord {
    JobRecord {
        spec: AddJobSpec {
            job_id: job_id.clone(),
            principal: PrincipalRef {
                kind: PrincipalKind::User,
                id: PrincipalId::new("user-a").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            expected_index_version,
            request_digest: ContentDigest::from_bytes([3; 32]),
            deadline_unix_ms: UnixMillis::new(10_000),
            paths: Vec::new(),
            all: true,
            extensions: Extensions::new(),
        },
        operation: JobOperation::Add,
        workspace_spec: None,
        snapshot_spec: None,
        state,
        resource_version: ResourceVersion::new(1),
        assignment: None,
        workspace_assignment: None,
        snapshot_assignment: None,
        accepted: None,
        progress: None,
        prepared: None,
        publication_candidate: None,
        decision: Some(JobDecision {
            job_id,
            assignment_id: AssignmentId::new("assignment-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            decision_generation: DecisionGeneration::new(1),
            decision,
            final_state: state,
            extensions: Extensions::new(),
        }),
        finalized: None,
        finalized_ack: None,
        failure: None,
    }
}

fn version(revision: u64, digest: u8) -> WireIndexVersion {
    WireIndexVersion {
        revision: IndexRevision::new(revision),
        digest: ContentDigest::from_bytes([digest; 32]),
        extensions: Extensions::new(),
    }
}
