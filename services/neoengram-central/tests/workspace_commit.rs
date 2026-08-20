use std::sync::Arc;

use fusen_rs::ErrorCategory;
use neoengram_central::{
    canonical_commit_id_with_layout, AddJobSpec, AdvancePlaygroundCommitRequest,
    ArtifactInitialization, ArtifactRecord, CatalogPvcReference, Clock, CommitRecord,
    ControlCatalogRepository, ControlPlane, InMemoryComponents, IndexKey, IndexPublishOutcome,
    IndexPublishRequest, IndexPublisher, JobInsertOutcome, JobKey, JobOperation, JobRecord,
    JobRepository, PreCommitCommitRequest, PreCommitId, PreCommitRepository, PreCommitStartRequest,
    PublishedIndex, StorageAccessMode, StorageBackendType, StorageVolumeRecord, StorageVolumeState,
    TenantRecord,
};
use neoengram_central::{
    dto::{
        CommitPlaygroundRequest, CreatePlaygroundRequest, CreateSnapshotRequest, DataLayout,
        IndexVersionBody, QueryArtifactCommitGraphRequest, QueryPlaygroundChangeListRequest,
    },
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy},
    service::{CatalogService, JobCoordinator, WorkspaceCommitService},
};
use neoengram_domain::core::{
    ChunkRef, ChunkingStrategy, Commit, ContentDigest, DirectoryId, FileRecord, IndexVersion,
    LogicalPath, Manifest, ObjectId,
};
use neoengram_domain::protocol::{
    ArtifactId, AssignmentGeneration, AssignmentId, CommitDataLayout, DecimalU64,
    DecisionGeneration, EdgeClusterId, Extensions, IndexDeltaRecord, JobDecision, JobId, JobState,
    PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef, ProjectId, PublishDecision, RequestId,
    ResourceVersion, StorageVolumeId, TenantId, UnixMillis,
};

mod support;
use support::ReadyStorageAvailability;

#[tokio::test]
async fn commit_consumes_frozen_candidate_and_publishes_both_heads() {
    let components = InMemoryComponents::new(5_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
    seed_catalog(
        &components,
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
    )
    .await;
    let index_key = IndexKey {
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
    };
    let initial_files = [("dataset/delete.bin", 11, 20), ("dataset/keep.bin", 12, 10)];
    let initial_records = file_records(&initial_files);
    let source = publish_file_index(
        &components,
        index_key.clone(),
        "seed-index-a",
        &initial_files,
        &[],
    )
    .await;
    let precommit_id = PreCommitId::new("precommit-a").unwrap();
    let job_id = JobId::new("precommit-job-a").unwrap();
    components
        .precommits
        .start(PreCommitStartRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            precommit_id: precommit_id.clone(),
            precommit_request_id: RequestId::new("precommit-request-a").unwrap(),
            source_index_version: source.clone(),
            frozen_head_commit_id: None,
            data_layout: CommitDataLayout::FastCdc,
            job_id: job_id.clone(),
            created_at_unix_ms: UnixMillis::new(4_000),
        })
        .await
        .unwrap();
    assert!(matches!(
        components
            .jobs
            .insert_or_load(succeeded_job(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                playground_id.clone(),
                job_id,
                source.clone(),
            ))
            .await
            .unwrap(),
        JobInsertOutcome::Inserted(_)
    ));

    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-a",
            [tenant_id.to_string()],
            [
                Permission::ArtifactRead,
                Permission::PlaygroundCreate,
                Permission::PlaygroundRead,
                Permission::SnapshotCreate,
            ],
        )
        .unwrap(),
    );
    let service = WorkspaceCommitService::from_authority(
        &components.authority_store(),
        policy.clone(),
        components.clock.clone(),
    )
    .unwrap();
    let identity =
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap();
    let request = CommitPlaygroundRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        playground_id: playground_id.to_string(),
        commit_request_id: "commit-request-a".to_owned(),
        precommit_id: precommit_id.to_string(),
        expected_candidate_index_version: IndexVersionBody {
            revision: source.revision.to_string(),
            digest: source.digest.to_string(),
        },
        data_layout: DataLayout::FastCdc,
        message: "Publish reviewed workspace".to_owned(),
        description: Some("Frozen candidate".to_owned()),
        tag_names: vec!["reviewed/v1".to_owned()],
    };

    let committed = service
        .commit_playground(&identity, request.clone())
        .await
        .unwrap();
    assert!(!committed.replayed);
    assert_eq!(committed.commit.records, initial_records);
    let diff_summary = committed
        .consumed_precommit
        .diff_summary
        .as_ref()
        .expect("root Pre-commit diff summary");
    assert_eq!(diff_summary.files_added, 2);
    assert_eq!(diff_summary.bytes_added, 30);
    assert_eq!(
        committed.consumed_precommit.head_published_at_unix_ms,
        Some(UnixMillis::new(5_000))
    );
    let artifact = components
        .control_catalog
        .get_artifact(&tenant_id, &project_id, &artifact_id)
        .await
        .unwrap()
        .unwrap();
    let playground = components
        .control_catalog
        .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
        .await
        .unwrap()
        .unwrap();
    let commit_digest: ContentDigest = committed.commit.commit_id.into();
    assert_eq!(
        committed.commit.source_storage_volume_id.as_str(),
        "volume-a"
    );
    assert_eq!(artifact.head_commit_id, Some(commit_digest));
    assert_eq!(playground.head_commit_id, Some(commit_digest));
    assert!(components
        .precommits
        .list_unpublished_commits(None, 10)
        .await
        .unwrap()
        .is_empty());

    let authority = components.authority_store();
    let coordinator = Arc::new(
        JobCoordinator::from_authority(
            Arc::new(ControlPlane::new(
                policy.clone(),
                authority.clone(),
                components.clock.clone(),
            )),
            &authority,
            components.clock.clone(),
            30_000,
        )
        .unwrap(),
    );
    let catalog = CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy,
        components.clock.clone(),
    )
    .with_precommits(components.precommits.clone())
    .with_coordinator(coordinator)
    .with_storage_availability_provider(Arc::new(ReadyStorageAvailability));
    let derived_request = CreatePlaygroundRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        playground_id: "playground-derived".to_owned(),
        storage_volume_id: "volume-a".to_owned(),
        display_name: "Derived Playground".to_owned(),
        base_commit_id: None,
    };
    let derived = catalog
        .create_playground(&identity, derived_request.clone())
        .await
        .unwrap();
    assert!(!derived.replayed);
    assert_eq!(derived.playground.state, "creating");
    assert_eq!(
        derived.playground.base_commit_id,
        Some(commit_digest.to_string())
    );
    assert_eq!(
        derived.playground.index_version.revision,
        committed.commit.index_version.revision.to_string()
    );
    assert_eq!(
        components
            .publisher
            .published_index(&IndexKey {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: PlaygroundId::new("playground-derived").unwrap(),
            })
            .await
            .unwrap()
            .records,
        initial_records
    );
    let materialization = components
        .jobs
        .all()
        .unwrap()
        .into_iter()
        .find(|job| job.operation == JobOperation::WorkspaceMaterialize)
        .expect("derived Playground materialization Job");
    let materialization_spec = materialization.workspace_spec.unwrap();
    assert_eq!(materialization_spec.base_commit_id, Some(commit_digest));
    assert_eq!(
        materialization_spec.base_index_version,
        Some(committed.commit.index_version.clone())
    );

    let change_request = QueryPlaygroundChangeListRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        playground_id: playground_id.to_string(),
        precommit_id: None,
        change_type: None,
        path_prefix: None,
        cursor: None,
        page_size: Some(50),
    };
    let unchanged = catalog
        .query_playground_change_list(&identity, change_request.clone())
        .await
        .unwrap();
    assert!(unchanged.items.is_empty());
    assert_eq!(unchanged.summary.files_added, "0");
    assert_eq!(unchanged.summary.files_modified, "0");
    assert_eq!(unchanged.summary.files_deleted, "0");

    publish_file_index(
        &components,
        index_key,
        "seed-index-b",
        &[("dataset/keep.bin", 13, 15), ("dataset/new.bin", 14, 7)],
        &["dataset/delete.bin"],
    )
    .await;
    let changed = catalog
        .query_playground_change_list(&identity, change_request)
        .await
        .unwrap();
    assert_eq!(
        changed
            .items
            .iter()
            .map(|entry| (entry.path.as_str(), entry.change_type.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("dataset/delete.bin", "deleted"),
            ("dataset/keep.bin", "modified"),
            ("dataset/new.bin", "added"),
        ]
    );
    assert_eq!(changed.summary.files_added, "1");
    assert_eq!(changed.summary.files_modified, "1");
    assert_eq!(changed.summary.files_deleted, "1");
    assert_eq!(changed.summary.bytes_added, "12");
    assert_eq!(changed.summary.bytes_removed, "20");

    let replay = service
        .commit_playground(&identity, request.clone())
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.commit, committed.commit);

    let mut reused_identity = request;
    reused_identity.message = "Publish a different workspace".to_owned();
    let conflict = service
        .commit_playground(&identity, reused_identity)
        .await
        .unwrap_err();
    assert_eq!(conflict.category(), ErrorCategory::Conflict);
    assert_eq!(conflict.code().as_str(), "commit_request_id_reused");

    components.clock.advance(1).unwrap();
    let second_source = components
        .publisher
        .current_version(&IndexKey {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
        })
        .await
        .unwrap();
    let second_precommit_id = PreCommitId::new("precommit-b").unwrap();
    let second_job_id = JobId::new("precommit-job-b").unwrap();
    components
        .precommits
        .start(PreCommitStartRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            precommit_id: second_precommit_id.clone(),
            precommit_request_id: RequestId::new("precommit-request-b").unwrap(),
            source_index_version: second_source.clone(),
            frozen_head_commit_id: Some(committed.commit.commit_id),
            data_layout: CommitDataLayout::FastCdc,
            job_id: second_job_id.clone(),
            created_at_unix_ms: components.clock.now(),
        })
        .await
        .unwrap();
    components
        .jobs
        .insert_or_load(succeeded_job(
            tenant_id.clone(),
            project_id.clone(),
            artifact_id.clone(),
            playground_id.clone(),
            second_job_id,
            second_source.clone(),
        ))
        .await
        .unwrap();
    let second_committed = service
        .commit_playground(
            &identity,
            CommitPlaygroundRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                playground_id: playground_id.to_string(),
                commit_request_id: "commit-request-b".to_owned(),
                precommit_id: second_precommit_id.to_string(),
                expected_candidate_index_version: IndexVersionBody {
                    revision: second_source.revision.to_string(),
                    digest: second_source.digest.to_string(),
                },
                data_layout: DataLayout::FastCdc,
                message: "Publish second workspace version".to_owned(),
                description: None,
                tag_names: vec!["reviewed/v2".to_owned()],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        second_committed.commit.parent_commit_id,
        Some(committed.commit.commit_id)
    );

    let graph_first = catalog
        .query_artifact_commit_graph(
            &identity,
            QueryArtifactCommitGraphRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                cursor: None,
                page_size: Some(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        graph_first.graph.head_commit_id,
        Some(second_committed.commit.commit_id.to_string())
    );
    assert_eq!(
        graph_first.graph.nodes[0].commit_id,
        second_committed.commit.commit_id.to_string()
    );
    let graph_cursor = graph_first.graph.next_cursor.unwrap();
    let graph_second = catalog
        .query_artifact_commit_graph(
            &identity,
            QueryArtifactCommitGraphRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                cursor: Some(graph_cursor.clone()),
                page_size: Some(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(graph_second.graph.nodes.len(), 1);
    assert_eq!(
        graph_second.graph.nodes[0].commit_id,
        committed.commit.commit_id.to_string()
    );
    assert!(graph_second.graph.next_cursor.is_none());

    let historical_request = CreatePlaygroundRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        playground_id: "playground-historical".to_owned(),
        storage_volume_id: "volume-a".to_owned(),
        display_name: "Historical Playground".to_owned(),
        base_commit_id: Some(committed.commit.commit_id.to_string()),
    };
    let historical = catalog
        .create_playground(&identity, historical_request.clone())
        .await
        .unwrap();
    assert!(!historical.replayed);
    assert_eq!(
        historical.playground.base_commit_id,
        Some(committed.commit.commit_id.to_string())
    );
    assert_eq!(
        components
            .publisher
            .published_index(&IndexKey {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: PlaygroundId::new("playground-historical").unwrap(),
            })
            .await
            .unwrap()
            .records,
        initial_records
    );
    assert!(
        catalog
            .create_playground(&identity, historical_request)
            .await
            .unwrap()
            .replayed
    );

    components
        .control_catalog
        .insert_storage_volume(StorageVolumeRecord {
            tenant_id: tenant_id.clone(),
            storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
            display_name: "Other Volume".to_owned(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            region: "local".to_owned(),
            backend_type: StorageBackendType::Pvc,
            access_mode: StorageAccessMode::ReadWriteMany,
            allowed_delivery_modes: vec![
                neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
                neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
            ],
            hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
            max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
            copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
            pvc_reference: Some(CatalogPvcReference {
                namespace: "default".to_owned(),
                claim_name: "other-workspace".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            created_at_unix_ms: components.clock.now(),
            updated_at_unix_ms: components.clock.now(),
        })
        .await
        .unwrap();
    let cross_volume_playground = catalog
        .create_playground(
            &identity,
            CreatePlaygroundRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                playground_id: "playground-cross-volume".to_owned(),
                storage_volume_id: "volume-b".to_owned(),
                display_name: "Cross-volume Playground".to_owned(),
                base_commit_id: Some(committed.commit.commit_id.to_string()),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        cross_volume_playground.code().as_str(),
        "playground_volume_has_no_commit_data"
    );
    let cross_volume_snapshot = catalog
        .create_snapshot(
            &identity,
            CreateSnapshotRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                commit_id: committed.commit.commit_id.to_string(),
                storage_volume_id: "volume-b".to_owned(),
                snapshot_request_id: "snapshot-request-cross-volume".to_owned(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        cross_volume_snapshot.code().as_str(),
        "snapshot_volume_has_no_commit_data"
    );

    let historical_playground_id = PlaygroundId::new("playground-historical").unwrap();
    components
        .control_catalog
        .transition_playground_state(
            &tenant_id,
            &project_id,
            &artifact_id,
            &historical_playground_id,
            neoengram_central::PlaygroundState::Creating,
            neoengram_central::PlaygroundState::Ready,
            components.clock.now(),
        )
        .await
        .unwrap();
    components.clock.advance(1).unwrap();
    let branch_source = components
        .publisher
        .current_version(&IndexKey {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: historical_playground_id.clone(),
        })
        .await
        .unwrap();
    let branch_precommit_id = PreCommitId::new("precommit-branch").unwrap();
    let branch_job_id = JobId::new("precommit-job-branch").unwrap();
    components
        .precommits
        .start(PreCommitStartRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: historical_playground_id.clone(),
            precommit_id: branch_precommit_id.clone(),
            precommit_request_id: RequestId::new("precommit-request-branch").unwrap(),
            source_index_version: branch_source.clone(),
            frozen_head_commit_id: Some(committed.commit.commit_id),
            data_layout: CommitDataLayout::FastCdc,
            job_id: branch_job_id.clone(),
            created_at_unix_ms: components.clock.now(),
        })
        .await
        .unwrap();
    components
        .jobs
        .insert_or_load(succeeded_job(
            tenant_id.clone(),
            project_id.clone(),
            artifact_id.clone(),
            historical_playground_id.clone(),
            branch_job_id,
            branch_source.clone(),
        ))
        .await
        .unwrap();
    let branch_commit = service
        .commit_playground(
            &identity,
            CommitPlaygroundRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                playground_id: historical_playground_id.to_string(),
                commit_request_id: "commit-request-branch".to_owned(),
                precommit_id: branch_precommit_id.to_string(),
                expected_candidate_index_version: IndexVersionBody {
                    revision: branch_source.revision.to_string(),
                    digest: branch_source.digest.to_string(),
                },
                data_layout: DataLayout::FastCdc,
                message: "Publish sibling from historical Commit".to_owned(),
                description: None,
                tag_names: vec!["reviewed/branch".to_owned()],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        branch_commit.commit.parent_commit_id,
        Some(committed.commit.commit_id)
    );
    assert_eq!(
        components
            .control_catalog
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .unwrap()
            .unwrap()
            .head_commit_id,
        Some(second_committed.commit.commit_id.into()),
        "publishing a sibling must not move another Playground"
    );
    let branched_graph = catalog
        .query_artifact_commit_graph(
            &identity,
            QueryArtifactCommitGraphRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                cursor: None,
                page_size: Some(10),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        branched_graph.graph.head_commit_id,
        Some(branch_commit.commit.commit_id.to_string())
    );
    assert_eq!(branched_graph.graph.nodes.len(), 3);
    assert!(branched_graph.graph.nodes.iter().any(|node| {
        node.commit_id == branch_commit.commit.commit_id.to_string()
            && node.parent_commit_id == Some(committed.commit.commit_id.to_string())
    }));
    assert!(branched_graph.graph.nodes.iter().any(|node| {
        node.commit_id == second_committed.commit.commit_id.to_string()
            && node.parent_commit_id == Some(committed.commit.commit_id.to_string())
    }));

    let mut unknown_commit = derived_request.clone();
    unknown_commit.playground_id = "playground-unknown-commit".to_owned();
    unknown_commit.base_commit_id = Some("bb".repeat(32));
    let unknown_error = catalog
        .create_playground(&identity, unknown_commit)
        .await
        .unwrap_err();
    assert_eq!(unknown_error.category(), ErrorCategory::NotFound);
    assert_eq!(unknown_error.code().as_str(), "resource_not_found");

    components
        .control_catalog
        .insert_artifact(ArtifactRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: ArtifactId::new("artifact-b").unwrap(),
            display_name: "Artifact B".to_owned(),
            description: None,
            initialization: ArtifactInitialization::Empty,
            head_commit_id: None,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            created_at_unix_ms: components.clock.now(),
            updated_at_unix_ms: components.clock.now(),
        })
        .await
        .unwrap();
    let mut cross_artifact = derived_request.clone();
    cross_artifact.artifact_id = "artifact-b".to_owned();
    cross_artifact.playground_id = "playground-cross-artifact".to_owned();
    cross_artifact.base_commit_id = Some(committed.commit.commit_id.to_string());
    let cross_error = catalog
        .create_playground(&identity, cross_artifact)
        .await
        .unwrap_err();
    assert_eq!(cross_error.category(), ErrorCategory::NotFound);
    assert_eq!(cross_error.code().as_str(), unknown_error.code().as_str());

    let next_head = ContentDigest::from_bytes([0xee; 32]);
    components
        .control_catalog
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id,
            expected_head_commit_id: Some(second_committed.commit.commit_id.into()),
            commit_id: next_head,
            updated_at_unix_ms: UnixMillis::new(5_002),
        })
        .await
        .unwrap();
    let stale_cursor = catalog
        .query_artifact_commit_graph(
            &identity,
            QueryArtifactCommitGraphRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                cursor: Some(graph_cursor),
                page_size: Some(1),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(stale_cursor.category(), ErrorCategory::Conflict);
    assert_eq!(stale_cursor.code().as_str(), "cursor_scope_conflict");
    let replayed_derived = catalog
        .create_playground(&identity, derived_request)
        .await
        .unwrap();
    assert!(replayed_derived.replayed);
    assert_eq!(
        replayed_derived.playground.base_commit_id,
        Some(commit_digest.to_string())
    );
}

#[tokio::test]
async fn commit_graph_exposes_only_commits_that_reached_a_published_head() {
    let components = InMemoryComponents::new(5_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
    seed_catalog(
        &components,
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
    )
    .await;

    let source = components
        .publisher
        .current_version(&IndexKey {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
        })
        .await
        .unwrap();
    let precommit_id = PreCommitId::new("precommit-publication-window").unwrap();
    let precommit_key =
        neoengram_central::PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
    let job_id = JobId::new("precommit-job-publication-window").unwrap();
    components
        .precommits
        .start(PreCommitStartRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            precommit_id: precommit_id.clone(),
            precommit_request_id: RequestId::new("precommit-request-publication-window").unwrap(),
            source_index_version: source.clone(),
            frozen_head_commit_id: None,
            data_layout: CommitDataLayout::FastCdc,
            job_id: job_id.clone(),
            created_at_unix_ms: UnixMillis::new(4_000),
        })
        .await
        .unwrap();
    components
        .precommits
        .sync_job(
            succeeded_job(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                playground_id.clone(),
                job_id,
                source.clone(),
            ),
            Some(PublishedIndex {
                version: source.clone(),
                records: Vec::new(),
            }),
            UnixMillis::new(4_100),
        )
        .await
        .unwrap();

    let root_directory_id = DirectoryId::from_bytes([7; 32]);
    let created_at_unix_ms = UnixMillis::new(4_200);
    let message = "Commit awaiting Head publication";
    let core_commit_id = Commit::new(root_directory_id, None, message, created_at_unix_ms.get())
        .unwrap()
        .canonical_id()
        .unwrap();
    let commit_id = canonical_commit_id_with_layout(core_commit_id, CommitDataLayout::FastCdc);
    let committed = components
        .precommits
        .commit(PreCommitCommitRequest {
            key: precommit_key.clone(),
            expected_candidate_index_version: source.clone(),
            data_layout: CommitDataLayout::FastCdc,
            commit: CommitRecord {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                source_playground_id: playground_id.clone(),
                source_storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
                source_precommit_id: precommit_id,
                commit_request_id: RequestId::new("commit-request-publication-window").unwrap(),
                commit_id,
                root_directory_id,
                parent_commit_id: None,
                index_version: source,
                data_layout: CommitDataLayout::FastCdc,
                records: Vec::new(),
                message: message.to_owned(),
                description: None,
                tag_names: Vec::new(),
                created_at_unix_ms,
            },
        })
        .await
        .unwrap();
    assert!(committed
        .consumed_precommit
        .head_published_at_unix_ms
        .is_none());

    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-a",
            [tenant_id.to_string()],
            [
                Permission::ArtifactRead,
                Permission::PlaygroundCreate,
                Permission::SnapshotCreate,
            ],
        )
        .unwrap(),
    );
    let catalog = CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy,
        components.clock.clone(),
    )
    .with_precommits(components.precommits.clone())
    .with_storage_availability_provider(Arc::new(ReadyStorageAvailability));
    let identity =
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap();
    let request = QueryArtifactCommitGraphRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        cursor: None,
        page_size: Some(10),
    };

    let authority_only = catalog
        .query_artifact_commit_graph(&identity, request.clone())
        .await
        .unwrap();
    assert!(authority_only.graph.nodes.is_empty());

    let unpublished_playground = catalog
        .create_playground(
            &identity,
            CreatePlaygroundRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                playground_id: "playground-unpublished-base".to_owned(),
                storage_volume_id: "volume-a".to_owned(),
                display_name: "Unpublished base".to_owned(),
                base_commit_id: Some(commit_id.to_string()),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(unpublished_playground.code().as_str(), "resource_not_found");
    let unpublished_snapshot = catalog
        .create_snapshot(
            &identity,
            CreateSnapshotRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                commit_id: commit_id.to_string(),
                storage_volume_id: "volume-a".to_owned(),
                snapshot_request_id: "snapshot-request-unpublished-base".to_owned(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(unpublished_snapshot.code().as_str(), "resource_not_found");

    components
        .control_catalog
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id,
            project_id,
            artifact_id,
            playground_id,
            expected_head_commit_id: None,
            commit_id: commit_id.into(),
            updated_at_unix_ms: UnixMillis::new(4_300),
        })
        .await
        .unwrap();
    assert!(components
        .precommits
        .get(&precommit_key)
        .await
        .unwrap()
        .unwrap()
        .head_published_at_unix_ms
        .is_none());

    let head_published_before_ack = catalog
        .query_artifact_commit_graph(&identity, request)
        .await
        .unwrap();
    assert_eq!(head_published_before_ack.graph.nodes.len(), 1);
    assert_eq!(
        head_published_before_ack.graph.nodes[0].commit_id,
        commit_id.to_string()
    );
}

#[tokio::test]
async fn commit_hides_cross_tenant_scope_behind_the_same_not_found_contract() {
    let components = InMemoryComponents::new(5_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
    seed_catalog(
        &components,
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
    )
    .await;
    let source = components
        .publisher
        .current_version(&IndexKey {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
        })
        .await
        .unwrap();
    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-a",
            ["tenant-b".to_owned()],
            [Permission::PlaygroundCreate],
        )
        .unwrap(),
    );
    let service = WorkspaceCommitService::from_authority(
        &components.authority_store(),
        policy,
        components.clock.clone(),
    )
    .unwrap();
    let identity =
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap();
    let request = CommitPlaygroundRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        playground_id: playground_id.to_string(),
        commit_request_id: "commit-request-hidden".to_owned(),
        precommit_id: "precommit-hidden".to_owned(),
        expected_candidate_index_version: IndexVersionBody {
            revision: source.revision.to_string(),
            digest: source.digest.to_string(),
        },
        data_layout: DataLayout::FastCdc,
        message: "Hidden workspace".to_owned(),
        description: None,
        tag_names: Vec::new(),
    };

    let hidden = service
        .commit_playground(&identity, request.clone())
        .await
        .unwrap_err();
    let mut absent_but_visible = request;
    absent_but_visible.tenant_id = "tenant-b".to_owned();
    let absent = service
        .commit_playground(&identity, absent_but_visible)
        .await
        .unwrap_err();

    assert_eq!(hidden.category(), ErrorCategory::NotFound);
    assert_eq!(absent.category(), ErrorCategory::NotFound);
    assert_eq!(hidden.code().as_str(), "resource_not_found");
    assert_eq!(absent.code().as_str(), hidden.code().as_str());
}

async fn seed_catalog(
    components: &InMemoryComponents,
    tenant_id: &TenantId,
    project_id: &ProjectId,
    artifact_id: &ArtifactId,
    playground_id: &PlaygroundId,
) {
    components
        .control_catalog
        .insert_tenant(TenantRecord {
            tenant_id: tenant_id.clone(),
            display_name: "Tenant".to_owned(),
            description: None,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_artifact(ArtifactRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            display_name: "Artifact".to_owned(),
            description: None,
            initialization: ArtifactInitialization::Empty,
            head_commit_id: None,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            created_at_unix_ms: UnixMillis::new(1_100),
            updated_at_unix_ms: UnixMillis::new(1_100),
        })
        .await
        .unwrap();
    let volume_id = StorageVolumeId::new("volume-a").unwrap();
    components
        .control_catalog
        .insert_storage_volume(StorageVolumeRecord {
            tenant_id: tenant_id.clone(),
            storage_volume_id: volume_id.clone(),
            display_name: "Volume".to_owned(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            region: "local".to_owned(),
            backend_type: StorageBackendType::Pvc,
            access_mode: StorageAccessMode::ReadWriteMany,
            allowed_delivery_modes: vec![
                neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
                neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
            ],
            hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
            max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
            copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
            pvc_reference: Some(CatalogPvcReference {
                namespace: "default".to_owned(),
                claim_name: "workspace".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            created_at_unix_ms: UnixMillis::new(1_200),
            updated_at_unix_ms: UnixMillis::new(1_200),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_playground(neoengram_central::PlaygroundRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            storage_volume_id: volume_id,
            region: "local".to_owned(),
            display_name: "Playground".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: neoengram_central::PlaygroundState::Ready,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            relative_root: "playgrounds/project-a/artifact-a/playground-a".to_owned(),
            created_at_unix_ms: UnixMillis::new(1_300),
            updated_at_unix_ms: UnixMillis::new(1_300),
        })
        .await
        .unwrap();
}

fn succeeded_job(
    tenant_id: TenantId,
    project_id: ProjectId,
    artifact_id: ArtifactId,
    playground_id: PlaygroundId,
    job_id: JobId,
    expected_index_version: neoengram_domain::protocol::WireIndexVersion,
) -> JobRecord {
    JobRecord {
        spec: AddJobSpec {
            job_id: job_id.clone(),
            principal: PrincipalRef {
                kind: PrincipalKind::User,
                id: PrincipalId::new("user-a").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id,
            project_id,
            artifact_id,
            playground_id,
            expected_index_version: expected_index_version.clone(),
            data_layout: neoengram_domain::protocol::CommitDataLayout::FastCdc,
            request_digest: ContentDigest::from_bytes([3; 32]),
            deadline_unix_ms: UnixMillis::new(10_000),
            paths: Vec::new(),
            all: true,
            extensions: Extensions::new(),
        },
        operation: JobOperation::Add,
        workspace_spec: None,
        delivery_spec: None,
        state: JobState::Succeeded,
        resource_version: ResourceVersion::new(1),
        assignment: None,
        workspace_assignment: None,
        delivery_assignment: None,
        accepted: None,
        progress: None,
        prepared: None,
        publication_candidate: None,
        decision: Some(JobDecision {
            job_id,
            assignment_id: AssignmentId::new("assignment-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            decision_generation: DecisionGeneration::new(1),
            decision: PublishDecision::Publish {
                published_index_version: expected_index_version,
                extensions: Extensions::new(),
            },
            final_state: JobState::Succeeded,
            extensions: Extensions::new(),
        }),
        finalized: None,
        finalized_ack: None,
        failure: None,
    }
}

fn file_manifest(seed: u8, total_size: u64) -> Manifest {
    Manifest::new(
        total_size,
        ChunkingStrategy::FastCdc,
        (total_size > 0)
            .then(|| ChunkRef::new(ObjectId::from_bytes([seed; 32]), 0, total_size).unwrap())
            .into_iter()
            .collect(),
    )
    .unwrap()
}

fn file_record(path: &str, seed: u8, total_size: u64) -> FileRecord {
    let manifest = file_manifest(seed, total_size);
    FileRecord::new(
        LogicalPath::parse(path).unwrap(),
        manifest.canonical_id().unwrap(),
        total_size,
        u64::from(total_size > 0),
    )
    .unwrap()
}

fn file_records(files: &[(&str, u8, u64)]) -> Vec<FileRecord> {
    files
        .iter()
        .map(|(path, seed, size)| file_record(path, *seed, *size))
        .collect()
}

async fn publish_file_index(
    components: &InMemoryComponents,
    index_key: IndexKey,
    job_id: &str,
    files: &[(&str, u8, u64)],
    deleted_paths: &[&str],
) -> neoengram_domain::protocol::WireIndexVersion {
    let records = file_records(files);
    let expected_index_version = components
        .publisher
        .current_version(&index_key)
        .await
        .unwrap();
    let mut mutations = deleted_paths
        .iter()
        .map(|path| IndexDeltaRecord::Delete {
            path: LogicalPath::parse(*path).unwrap(),
            extensions: Extensions::new(),
        })
        .chain(records.iter().map(|record| IndexDeltaRecord::Upsert {
            path: record.path.clone(),
            manifest_id: record.manifest_id,
            total_size: DecimalU64::new(record.total_size),
            chunk_count: DecimalU64::new(record.chunk_count),
            extensions: Extensions::new(),
        }))
        .collect::<Vec<_>>();
    mutations.sort_by_key(|mutation| match mutation {
        IndexDeltaRecord::Upsert { path, .. } | IndexDeltaRecord::Delete { path, .. } => {
            path.clone()
        }
    });
    let next_revision = expected_index_version
        .revision
        .get()
        .checked_add(1)
        .unwrap();
    let expected_result_digest = IndexVersion::from_snapshot(next_revision, &records)
        .unwrap()
        .digest;
    let outcome = components
        .publisher
        .compare_and_swap(IndexPublishRequest {
            job_key: JobKey::new(index_key.tenant_id.clone(), JobId::new(job_id).unwrap()),
            index_key,
            expected_index_version,
            expected_result_digest,
            manifests: files
                .iter()
                .map(|(_, seed, size)| file_manifest(*seed, *size))
                .collect(),
            mutations,
        })
        .await
        .unwrap();
    let IndexPublishOutcome::Published(version) = outcome else {
        panic!("test Index publication was rejected: {outcome:?}");
    };
    version
}
