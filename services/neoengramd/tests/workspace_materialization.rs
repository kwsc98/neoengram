use neoengram_core::LogicalPath;
use neoengram_protocol::{
    AgentId, AgentMountId, ArtifactId, AssignmentGeneration, AssignmentId, ControlError,
    DecimalU64, EdgeClusterId, ErrorCode, Extensions, JobAccepted, JobFailed, JobFailureStage,
    JobId, JobProgress, JobState, MountGeneration, OwnerGeneration, PlaygroundId, PrincipalId,
    PrincipalKind, PrincipalRef, ProjectId, SessionGeneration, StorageVolumeId, TenantId,
    UnixMillis, WorkspaceMaterializeOperation,
};
use neoengramd::{
    AgentReport, ArtifactInitialization, ArtifactRecord, AssignWorkspaceMaterializationRequest,
    CatalogPvcReference, ControlCatalogRepository, CreateWorkspaceMaterializationRequest,
    InMemoryComponents, PlaygroundRecord, PlaygroundState, ReceiveReportRequest, StorageAccessMode,
    StorageBackendType, StorageVolumeRecord, StorageVolumeState, TenantRecord,
    WorkspaceMaterializeSpec, WorkspaceMaterializeTarget,
};

#[tokio::test]
async fn materialization_reports_publish_the_playground_lifecycle_idempotently() {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
    let storage_volume_id = StorageVolumeId::new("volume-a").unwrap();
    let now = UnixMillis::new(1_000);
    components
        .control_catalog
        .insert_tenant(TenantRecord {
            tenant_id: tenant_id.clone(),
            display_name: "Tenant A".to_owned(),
            description: None,
            resource_version: 1,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_artifact(ArtifactRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            display_name: "Artifact A".to_owned(),
            description: None,
            initialization: ArtifactInitialization::Empty,
            head_commit_id: None,
            resource_version: 1,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_storage_volume(StorageVolumeRecord {
            tenant_id: tenant_id.clone(),
            storage_volume_id: storage_volume_id.clone(),
            display_name: "Volume A".to_owned(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            region: "local".to_owned(),
            backend_type: StorageBackendType::Pvc,
            access_mode: StorageAccessMode::ReadWriteOnce,
            pvc_reference: Some(CatalogPvcReference {
                namespace: "default".to_owned(),
                claim_name: "volume-a".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
        .await
        .unwrap();
    let relative_root =
        LogicalPath::parse("playgrounds/project-a/artifact-a/playground-a").unwrap();
    components
        .control_catalog
        .insert_playground(PlaygroundRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            storage_volume_id: storage_volume_id.clone(),
            region: "local".to_owned(),
            display_name: "Playground A".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: PlaygroundState::Creating,
            relative_root: relative_root.to_string(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
        .await
        .unwrap();

    let principal = PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new("workspace-materializer").unwrap(),
        extensions: Extensions::new(),
    };
    let job_id = JobId::new("materialize-a").unwrap();
    let operation = WorkspaceMaterializeOperation {
        job_id: job_id.clone(),
        principal: principal.clone(),
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
        storage_volume_id: storage_volume_id.clone(),
        relative_root: relative_root.clone(),
        base_commit_id: None,
        base_index_version: None,
        deadline_unix_ms: UnixMillis::new(10_000),
        extensions: Extensions::new(),
    };
    let request_digest = operation.request_digest().unwrap();
    let control = components.control_plane();
    let created = control
        .create_workspace_materialization(CreateWorkspaceMaterializationRequest {
            spec: WorkspaceMaterializeSpec {
                job_id: job_id.clone(),
                principal,
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: playground_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                relative_root,
                base_commit_id: None,
                base_index_version: None,
                request_digest,
                deadline_unix_ms: UnixMillis::new(10_000),
            },
        })
        .await
        .unwrap();
    assert_eq!(created.job.state, JobState::Queued);

    let agent_id = AgentId::new("agent-a").unwrap();
    let assignment_id = AssignmentId::new("assignment-a").unwrap();
    let assigned = control
        .assign_workspace_materialization(AssignWorkspaceMaterializationRequest {
            tenant_id: tenant_id.clone(),
            job_id: job_id.clone(),
            target: WorkspaceMaterializeTarget {
                assignment_id: assignment_id.clone(),
                assignment_generation: AssignmentGeneration::new(1),
                agent_id: agent_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                agent_mount_id: AgentMountId::new("mount-a").unwrap(),
                mount_generation: MountGeneration::new(1),
                owner_generation: OwnerGeneration::new(1),
            },
        })
        .await
        .unwrap();
    assert_eq!(assigned.job.state, JobState::Assigned);
    assert_eq!(
        control
            .poll_agent_messages(&agent_id, SessionGeneration::new(1), 32)
            .await
            .unwrap()
            .len(),
        1
    );

    control
        .receive_report(ReceiveReportRequest {
            tenant_id: tenant_id.clone(),
            agent_id: agent_id.clone(),
            report: AgentReport::Accepted(JobAccepted {
                job_id: job_id.clone(),
                assignment_id: assignment_id.clone(),
                assignment_generation: AssignmentGeneration::new(1),
                accepted_at_unix_ms: UnixMillis::new(1_100),
                request_digest,
                extensions: Extensions::new(),
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        control
            .poll_agent_messages(&agent_id, SessionGeneration::new(2), 32)
            .await
            .unwrap()
            .len(),
        1,
        "Accepted materialization remains recoverable until a terminal report"
    );
    control
        .receive_report(ReceiveReportRequest {
            tenant_id: tenant_id.clone(),
            agent_id: agent_id.clone(),
            report: AgentReport::Progress(progress(
                &job_id,
                &assignment_id,
                JobState::Running,
                "materializing",
            )),
        })
        .await
        .unwrap();
    let accepted_replay = control
        .receive_report(ReceiveReportRequest {
            tenant_id: tenant_id.clone(),
            agent_id: agent_id.clone(),
            report: AgentReport::Accepted(JobAccepted {
                job_id: job_id.clone(),
                assignment_id: assignment_id.clone(),
                assignment_generation: AssignmentGeneration::new(1),
                accepted_at_unix_ms: UnixMillis::new(1_150),
                request_digest,
                extensions: Extensions::new(),
            }),
        })
        .await
        .unwrap();
    assert!(accepted_replay.replayed);
    assert_eq!(accepted_replay.job.state, JobState::Running);
    assert_eq!(
        control
            .poll_agent_messages(&agent_id, SessionGeneration::new(3), 32)
            .await
            .unwrap()
            .len(),
        1,
        "Running materialization remains recoverable after an Agent restart"
    );
    let succeeded = progress(&job_id, &assignment_id, JobState::Succeeded, "materialized");
    control
        .receive_report(ReceiveReportRequest {
            tenant_id: tenant_id.clone(),
            agent_id: agent_id.clone(),
            report: AgentReport::Progress(succeeded.clone()),
        })
        .await
        .unwrap();
    assert!(control
        .poll_agent_messages(&agent_id, SessionGeneration::new(4), 32)
        .await
        .unwrap()
        .is_empty());
    let replay = control
        .receive_report(ReceiveReportRequest {
            tenant_id: tenant_id.clone(),
            agent_id,
            report: AgentReport::Progress(succeeded),
        })
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.job.state, JobState::Succeeded);
    let playground = components
        .control_catalog
        .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(playground.state, PlaygroundState::Ready);

    let failed_playground_id = PlaygroundId::new("playground-failed").unwrap();
    let failed_root =
        LogicalPath::parse("playgrounds/project-a/artifact-a/playground-failed").unwrap();
    components
        .control_catalog
        .insert_playground(PlaygroundRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: failed_playground_id.clone(),
            storage_volume_id: storage_volume_id.clone(),
            region: "local".to_owned(),
            display_name: "Failed Playground".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: PlaygroundState::Creating,
            relative_root: failed_root.to_string(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
        .await
        .unwrap();
    let failed_job_id = JobId::new("materialize-failed").unwrap();
    let failed_principal = PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new("workspace-materializer").unwrap(),
        extensions: Extensions::new(),
    };
    let failed_operation = WorkspaceMaterializeOperation {
        job_id: failed_job_id.clone(),
        principal: failed_principal.clone(),
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: failed_playground_id.clone(),
        storage_volume_id: storage_volume_id.clone(),
        relative_root: failed_root.clone(),
        base_commit_id: None,
        base_index_version: None,
        deadline_unix_ms: UnixMillis::new(10_000),
        extensions: Extensions::new(),
    };
    let failed_digest = failed_operation.request_digest().unwrap();
    control
        .create_workspace_materialization(CreateWorkspaceMaterializationRequest {
            spec: WorkspaceMaterializeSpec {
                job_id: failed_job_id.clone(),
                principal: failed_principal,
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: failed_playground_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                relative_root: failed_root,
                base_commit_id: None,
                base_index_version: None,
                request_digest: failed_digest,
                deadline_unix_ms: UnixMillis::new(10_000),
            },
        })
        .await
        .unwrap();
    let failed_assignment_id = AssignmentId::new("assignment-failed").unwrap();
    control
        .assign_workspace_materialization(AssignWorkspaceMaterializationRequest {
            tenant_id: tenant_id.clone(),
            job_id: failed_job_id.clone(),
            target: WorkspaceMaterializeTarget {
                assignment_id: failed_assignment_id.clone(),
                assignment_generation: AssignmentGeneration::new(1),
                agent_id: AgentId::new("agent-failed").unwrap(),
                storage_volume_id: storage_volume_id.clone(),
                agent_mount_id: AgentMountId::new("mount-failed").unwrap(),
                mount_generation: MountGeneration::new(1),
                owner_generation: OwnerGeneration::new(1),
            },
        })
        .await
        .unwrap();
    control
        .receive_report(ReceiveReportRequest {
            tenant_id: tenant_id.clone(),
            agent_id: AgentId::new("agent-failed").unwrap(),
            report: AgentReport::Failed(JobFailed {
                tenant_id: tenant_id.clone(),
                job_id: failed_job_id,
                assignment_id: failed_assignment_id,
                assignment_generation: AssignmentGeneration::new(1),
                final_state: JobState::Failed,
                failed_at_unix_ms: UnixMillis::new(1_200),
                stage: JobFailureStage::Execution,
                error: ControlError {
                    code: ErrorCode::new("WORKSPACE_MATERIALIZE_IO_FAILED").unwrap(),
                    message: "could not create the directory".to_owned(),
                    retryable: false,
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        components
            .control_catalog
            .get_playground(&tenant_id, &project_id, &artifact_id, &failed_playground_id,)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlaygroundState::Abnormal
    );
}

fn progress(
    job_id: &JobId,
    assignment_id: &AssignmentId,
    state: JobState,
    phase: &str,
) -> JobProgress {
    JobProgress {
        job_id: job_id.clone(),
        assignment_id: assignment_id.clone(),
        assignment_generation: AssignmentGeneration::new(1),
        state,
        phase: phase.to_owned(),
        files_completed: DecimalU64::new(0),
        bytes_completed: DecimalU64::new(0),
        retry_after_ms: None,
        extensions: Extensions::new(),
    }
}
