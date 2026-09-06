use neoengram_central::{
    AgentReport, ArtifactInitialization, ArtifactRecord, AssignWorkspaceMaterializationRequest,
    CatalogPvcReference, ControlCatalogRepository, CreateWorkspaceMaterializationRequest,
    InMemoryComponents, ReceiveReportRequest, StorageAccessMode, StorageBackendType,
    StorageVolumeRecord, StorageVolumeState, TenantRecord, WorkspaceMaterializeSpec,
    WorkspaceMaterializeTarget, WorkspaceRecord, WorkspaceState,
};
use neoengram_domain::core::LogicalPath;
use neoengram_domain::protocol::{
    AgentId, AgentMountId, ArtifactId, AssignmentGeneration, AssignmentId, ControlError,
    DecimalU64, EdgeClusterId, ErrorCode, Extensions, Generation, JobAccepted, JobFailed,
    JobFailureStage, JobId, JobProgress, JobState, MountGeneration, OwnerGeneration, PrincipalId,
    PrincipalKind, PrincipalRef, ProjectId, ResourceLifecycle, SessionGeneration, StorageVolumeId,
    TaskExecutionFence, TaskId, TenantId, UnixMillis, WorkspaceId, WorkspaceMaterializeOperation,
};

fn task_fence(job_id: &JobId) -> TaskExecutionFence {
    TaskExecutionFence::new(
        TaskId::new(format!("task-{job_id}")).unwrap(),
        Generation::new(1),
        "materialize",
        Generation::new(1),
        Generation::new(1),
    )
}

#[tokio::test]
async fn materialization_reports_publish_the_workspace_lifecycle_idempotently() {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let workspace_id = WorkspaceId::new("workspace-a").unwrap();
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
            lifecycle: ResourceLifecycle::active(),
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
            allowed_delivery_modes: vec![
                neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
                neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
            ],
            hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
            max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
            copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
            pvc_reference: Some(CatalogPvcReference {
                namespace: "default".to_owned(),
                claim_name: "volume-a".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            lifecycle: ResourceLifecycle::active(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
        .await
        .unwrap();
    let relative_root = LogicalPath::parse("workspaces/project-a/artifact-a/workspace-a").unwrap();
    components
        .control_catalog
        .insert_workspace(WorkspaceRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            workspace_id: workspace_id.clone(),
            storage_volume_id: storage_volume_id.clone(),
            region: "local".to_owned(),
            display_name: "Workspace A".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: WorkspaceState::Creating,
            resource_version: 1,
            lifecycle: ResourceLifecycle::active(),
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
        workspace_id: workspace_id.clone(),
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
                workspace_id: workspace_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                relative_root,
                base_commit_id: None,
                base_index_version: None,
                request_digest,
                deadline_unix_ms: UnixMillis::new(10_000),
                operation_task_id: None,
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
            .deliverable_agent_messages(&agent_id, SessionGeneration::new(1), 32)
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
                task_fence: task_fence(&job_id),
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
            .deliverable_agent_messages(&agent_id, SessionGeneration::new(2), 32)
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
                task_fence: task_fence(&job_id),
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
            .deliverable_agent_messages(&agent_id, SessionGeneration::new(3), 32)
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
        .deliverable_agent_messages(&agent_id, SessionGeneration::new(4), 32)
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
    let workspace = components
        .control_catalog
        .get_workspace(&tenant_id, &project_id, &artifact_id, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(workspace.state, WorkspaceState::Ready);

    let failed_workspace_id = WorkspaceId::new("workspace-failed").unwrap();
    let failed_root =
        LogicalPath::parse("workspaces/project-a/artifact-a/workspace-failed").unwrap();
    components
        .control_catalog
        .insert_workspace(WorkspaceRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            workspace_id: failed_workspace_id.clone(),
            storage_volume_id: storage_volume_id.clone(),
            region: "local".to_owned(),
            display_name: "Failed Workspace".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: WorkspaceState::Creating,
            resource_version: 1,
            lifecycle: ResourceLifecycle::active(),
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
        workspace_id: failed_workspace_id.clone(),
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
                workspace_id: failed_workspace_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                relative_root: failed_root,
                base_commit_id: None,
                base_index_version: None,
                request_digest: failed_digest,
                deadline_unix_ms: UnixMillis::new(10_000),
                operation_task_id: None,
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
                job_id: failed_job_id.clone(),
                task_fence: task_fence(&failed_job_id),
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
            .get_workspace(&tenant_id, &project_id, &artifact_id, &failed_workspace_id,)
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkspaceState::Abnormal
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
        task_fence: task_fence(job_id),
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
