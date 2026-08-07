use std::sync::Arc;

use fusen_rs::ErrorCategory;
use neoengram_core::{ContentDigest, FileRecord, LogicalPath, ManifestId};
use neoengram_protocol::{
    ArtifactId, AssignmentGeneration, AssignmentId, DecisionGeneration, EdgeClusterId, Extensions,
    JobDecision, JobId, JobState, PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef,
    ProjectId, PublishDecision, RequestId, ResourceVersion, StorageVolumeId, TenantId, UnixMillis,
};
use neoengram_server::{
    dto::{
        CommitPlaygroundRequest, CreatePlaygroundRequest, IndexVersionBody, JsonExtensions,
        QueryArtifactCommitGraphRequest, QueryPlaygroundChangeListRequest,
    },
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy},
    service::{CatalogService, JobCoordinator, WorkspaceCommitService},
};
use neoengramd::{
    AddJobSpec, AdvancePlaygroundCommitRequest, ArtifactInitialization, ArtifactRecord,
    CatalogPvcReference, Clock, ControlCatalogRepository, ControlPlane, InMemoryComponents,
    IndexKey, IndexPublisher, JobInsertOutcome, JobOperation, JobRecord, JobRepository,
    PreCommitId, PreCommitRepository, PreCommitStartRequest, StorageAccessMode, StorageBackendType,
    StorageVolumeRecord, StorageVolumeState, TenantRecord,
};

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
    let initial_records = vec![
        file_record("dataset/delete.bin", 11, 20),
        file_record("dataset/keep.bin", 12, 10),
    ];
    let source = components
        .publisher
        .seed(index_key.clone(), 1, initial_records.clone())
        .unwrap();
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
        message: "Publish reviewed workspace".to_owned(),
        description: Some("Frozen candidate".to_owned()),
        tag_names: vec!["reviewed/v1".to_owned()],
        extensions: JsonExtensions::default(),
    };

    for reserved_head in ["expected_head_commit_id", "source_head_commit_id"] {
        let mut request_with_client_head = request.clone();
        request_with_client_head.extensions.0.insert(
            reserved_head.to_owned(),
            serde_json::json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        );
        let error = service
            .commit_playground(&identity, request_with_client_head)
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "protocol_invalid");
    }

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
        committed
            .commit
            .source_storage_volume_id
            .as_ref()
            .map(|id| id.as_str()),
        Some("volume-a")
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
    .with_coordinator(coordinator);
    let derived_request = CreatePlaygroundRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        playground_id: "playground-derived".to_owned(),
        storage_volume_id: "volume-a".to_owned(),
        display_name: "Derived Playground".to_owned(),
        base_commit_id: None,
        extensions: JsonExtensions::default(),
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

    components
        .publisher
        .seed(
            index_key,
            2,
            vec![
                file_record("dataset/keep.bin", 13, 15),
                file_record("dataset/new.bin", 14, 7),
            ],
        )
        .unwrap();
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
                message: "Publish second workspace version".to_owned(),
                description: None,
                tag_names: vec!["reviewed/v2".to_owned()],
                extensions: JsonExtensions::default(),
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
        extensions: JsonExtensions::default(),
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
        message: "Hidden workspace".to_owned(),
        description: None,
        tag_names: Vec::new(),
        extensions: JsonExtensions::default(),
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
            pvc_reference: Some(CatalogPvcReference {
                namespace: "default".to_owned(),
                claim_name: "workspace".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(1_200),
            updated_at_unix_ms: UnixMillis::new(1_200),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_playground(neoengramd::PlaygroundRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            storage_volume_id: volume_id,
            region: "local".to_owned(),
            display_name: "Playground".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: neoengramd::PlaygroundState::Ready,
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
    expected_index_version: neoengram_protocol::WireIndexVersion,
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
            request_digest: ContentDigest::from_bytes([3; 32]),
            deadline_unix_ms: UnixMillis::new(10_000),
            paths: Vec::new(),
            all: true,
            extensions: Extensions::new(),
        },
        operation: JobOperation::Add,
        workspace_spec: None,
        snapshot_spec: None,
        state: JobState::Succeeded,
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

fn file_record(path: &str, manifest: u8, total_size: u64) -> FileRecord {
    FileRecord::new(
        LogicalPath::parse(path).unwrap(),
        ManifestId::from_bytes([manifest; 32]),
        total_size,
        u64::from(total_size > 0),
    )
    .unwrap()
}
