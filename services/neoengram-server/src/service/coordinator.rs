use std::sync::{Arc, Mutex};

use neoengram_protocol::{
    ArtifactPlacementId, AssignmentGeneration, AssignmentId, JobState, PlacementGeneration,
};
use neoengramd::{
    AgentRegistryRepository, AssignJobRequest, AssignmentTarget, CentralError, CentralErrorCode,
    CentralResult, Clock, ControlCatalogRepository, ControlPlane, ExpireAddJobRequest, IndexKey,
    IndexPublisher, JobKey, JobRecord, JobRepository, ResumePublicationRequest,
};

const DEFAULT_RECOVERY_BATCH_SIZE: usize = 1_024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoordinatorRun {
    pub examined: usize,
    pub assigned: usize,
    pub expired: usize,
    pub finalized: usize,
}

/// Single-process development scheduler and publication recovery loop.
pub struct JobCoordinator {
    control: Arc<ControlPlane>,
    jobs: Arc<dyn JobRepository>,
    catalog: Arc<dyn ControlCatalogRepository>,
    registry: Arc<dyn AgentRegistryRepository>,
    indexes: Arc<dyn IndexPublisher>,
    clock: Arc<dyn Clock>,
    heartbeat_timeout_ms: u64,
    recovery_cursor: Mutex<Option<JobKey>>,
}

impl JobCoordinator {
    pub fn from_authority(
        control: Arc<ControlPlane>,
        authority: &neoengramd::AuthorityStore,
        clock: Arc<dyn Clock>,
        heartbeat_timeout_ms: u64,
    ) -> CentralResult<Self> {
        let catalog = authority.control_catalog().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no control catalog composition",
            )
        })?;
        let registry = authority.agent_registry().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no Agent registry composition",
            )
        })?;
        Ok(Self {
            control,
            jobs: authority.jobs(),
            catalog,
            registry,
            indexes: authority.publisher(),
            clock,
            heartbeat_timeout_ms,
            recovery_cursor: Mutex::new(None),
        })
    }

    /// Rejects fabricated Playground scope and stale client Index defaults before Job creation.
    pub async fn validate_spec(&self, spec: &neoengramd::AddJobSpec) -> CentralResult<()> {
        if self
            .jobs
            .get(&neoengramd::JobKey::new(
                spec.tenant_id.clone(),
                spec.job_id.clone(),
            ))
            .await?
            .is_some()
        {
            // Exact replay and JobId reuse are decided against the immutable persisted spec.
            return Ok(());
        }
        let _playground = self
            .catalog
            .get_playground(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.playground_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "the requested Playground is not visible or does not exist",
                )
                .with_retryable(false)
            })?;
        let current = self
            .indexes
            .current_version(&IndexKey {
                tenant_id: spec.tenant_id.clone(),
                project_id: spec.project_id.clone(),
                artifact_id: spec.artifact_id.clone(),
                playground_id: spec.playground_id.clone(),
            })
            .await?;
        if !same_index_version(&current, &spec.expected_index_version) {
            return Err(CentralError::new(
                CentralErrorCode::MetadataInvalid,
                "expected_index_version differs from the authoritative Playground IndexVersion",
            )
            .with_retryable(false));
        }
        Ok(())
    }

    /// Performs one immediate scheduling attempt. Lack of a Ready owner leaves the Job queued.
    pub async fn schedule(&self, job: &JobRecord) -> CentralResult<Option<JobRecord>> {
        if job.state != JobState::Queued {
            return Ok(None);
        }
        let playground = self
            .catalog
            .get_playground(
                &job.spec.tenant_id,
                &job.spec.project_id,
                &job.spec.artifact_id,
                &job.spec.playground_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::JobNotFound,
                    "the Job's Playground no longer exists",
                )
            })?;
        let current = self.indexes.current_version(&job.index_key()).await?;
        if !same_index_version(&current, &job.spec.expected_index_version) {
            return Err(CentralError::new(
                CentralErrorCode::MetadataInvalid,
                "the Job's expected IndexVersion no longer matches its Playground",
            ));
        }
        let Some(owner) = self
            .registry
            .get_current_by_volume(&job.spec.tenant_id, &playground.storage_volume_id)
            .await?
        else {
            return Ok(None);
        };
        if owner.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms)
            != neoengramd::DerivedVolumeState::Ready
        {
            return Ok(None);
        }
        let Some(instance) = owner.instance.as_ref() else {
            return Ok(None);
        };
        if owner.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
            || owner.owner.active_agent_mount_id.as_ref() != Some(&owner.mount.agent_mount_id)
        {
            return Ok(None);
        }
        let target = AssignmentTarget {
            assignment_id: deterministic_assignment_id(job)?,
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: instance.agent_id.clone(),
            edge_cluster_id: owner.enrollment.edge_cluster_id.clone(),
            storage_volume_id: playground.storage_volume_id,
            artifact_placement_id: deterministic_placement_id(job, &owner.mount.storage_volume_id)?,
            placement_generation: PlacementGeneration::new(1),
            agent_mount_id: owner.mount.agent_mount_id.clone(),
            mount_generation: owner.mount.mount_generation,
            owner_generation: owner.owner.owner_generation,
            lease: None,
        };
        let result = self
            .control
            .assign_job(AssignJobRequest {
                actor: job.spec.principal.clone(),
                tenant_id: job.spec.tenant_id.clone(),
                job_id: job.spec.job_id.clone(),
                target,
            })
            .await?;
        Ok(Some(result.job))
    }

    pub async fn reconcile_once(&self) -> CentralResult<CoordinatorRun> {
        self.reconcile(DEFAULT_RECOVERY_BATCH_SIZE).await
    }

    pub async fn reconcile(&self, limit: usize) -> CentralResult<CoordinatorRun> {
        if limit == 0 {
            return Ok(CoordinatorRun::default());
        }
        let now = self.clock.now();
        let after = self
            .recovery_cursor
            .lock()
            .map_err(|_| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "Job coordinator recovery cursor lock is poisoned",
                )
            })?
            .clone();
        let mut jobs = self
            .jobs
            .list_recoverable(after.as_ref(), now, limit)
            .await?;
        if jobs.is_empty() && after.is_some() {
            jobs = self.jobs.list_recoverable(None, now, limit).await?;
        }
        // Advance before side effects so one persistently poisoned Job cannot pin the page.
        *self.recovery_cursor.lock().map_err(|_| {
            CentralError::new(
                CentralErrorCode::Internal,
                "Job coordinator recovery cursor lock is poisoned",
            )
        })? = jobs.last().map(JobRecord::key);
        let mut run = CoordinatorRun {
            examined: jobs.len(),
            ..CoordinatorRun::default()
        };
        for job in jobs {
            if let Err(error) = self.reconcile_job(&job, now, &mut run).await {
                tracing::warn!(
                    tenant_id = %job.spec.tenant_id,
                    job_id = %job.spec.job_id,
                    state = ?job.state,
                    %error,
                    "Job coordinator could not recover one Job; continuing the page"
                );
            }
        }
        Ok(run)
    }

    async fn reconcile_job(
        &self,
        job: &JobRecord,
        now: neoengram_protocol::UnixMillis,
        run: &mut CoordinatorRun,
    ) -> CentralResult<()> {
        if job.spec.deadline_unix_ms.get() <= now.get()
            && matches!(
                job.state,
                JobState::Queued
                    | JobState::Assigned
                    | JobState::Accepted
                    | JobState::Running
                    | JobState::Prepared
                    | JobState::CancelRequested
            )
        {
            match self
                .control
                .expire_add_job(ExpireAddJobRequest {
                    actor: job.spec.principal.clone(),
                    tenant_id: job.spec.tenant_id.clone(),
                    job_id: job.spec.job_id.clone(),
                })
                .await
            {
                Ok(_) => run.expired += 1,
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                Err(error) => return Err(error),
            }
            return Ok(());
        }
        match job.state {
            JobState::Queued => match self.schedule(job).await {
                Ok(Some(_)) => run.assigned += 1,
                Ok(None) => {}
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                Err(error) => return Err(error),
            },
            JobState::Prepared => match self
                .control
                .finalize_prepared(ResumePublicationRequest {
                    tenant_id: job.spec.tenant_id.clone(),
                    job_id: job.spec.job_id.clone(),
                })
                .await
            {
                Ok(_) => run.finalized += 1,
                Err(error)
                    if matches!(
                        error.code(),
                        CentralErrorCode::BatchIncomplete
                            | CentralErrorCode::ObjectNotDurable
                            | CentralErrorCode::ConcurrentUpdate
                    ) => {}
                Err(error) => return Err(error),
            },
            JobState::Publishing => match self
                .control
                .resume_publication(ResumePublicationRequest {
                    tenant_id: job.spec.tenant_id.clone(),
                    job_id: job.spec.job_id.clone(),
                })
                .await
            {
                Ok(_) => run.finalized += 1,
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {}
                Err(error) => return Err(error),
            },
            _ => {}
        }
        Ok(())
    }
}

fn same_index_version(
    left: &neoengram_protocol::WireIndexVersion,
    right: &neoengram_protocol::WireIndexVersion,
) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn deterministic_assignment_id(job: &JobRecord) -> CentralResult<AssignmentId> {
    let input = format!("{}\0{}", job.spec.tenant_id, job.spec.job_id);
    AssignmentId::new(format!(
        "assignment-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

fn deterministic_placement_id(
    job: &JobRecord,
    volume_id: &neoengram_protocol::StorageVolumeId,
) -> CentralResult<ArtifactPlacementId> {
    let input = format!(
        "{}\0{}\0{}",
        job.spec.tenant_id, job.spec.artifact_id, volume_id
    );
    ArtifactPlacementId::new(format!(
        "placement-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(CentralError::from)
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use neoengram_core::{
        ChunkingStrategy, ContentDigest, FileRecord, IndexVersion, LogicalPath, Manifest,
    };
    use neoengram_protocol::{
        AgentId, AgentMountId, ArtifactId, AssignmentGeneration, AssignmentId, DecimalU64,
        EdgeClusterId, Extensions, IndexDeltaRecord, JobAccepted, JobId, JobPrepared, JobProgress,
        ManifestRecord, MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage,
        MetadataBatchRecords, MetadataBatchScope, MetadataPublication, MountGeneration,
        OwnerGeneration, PlacementGeneration, PlaygroundId, PrincipalId, PrincipalKind,
        PrincipalRef, ProjectId, ResourceVersion, StorageVolumeId, TenantId, UnixMillis,
        WireChunkingStrategy,
    };
    use neoengramd::{
        Action, AddJobSpec, AgentReport, AuthorityCapabilities, AuthorityStore,
        AuthorizationRequest, Authorizer, CreateAddJobRequest, FinalizeAddRequest,
        InMemoryComponents, InMemoryJobRepository, JobInsertOutcome, MetadataBatchSubmission,
        ReceiveReportRequest, StageMetadataBatchRequest,
    };

    use super::*;

    #[tokio::test]
    async fn recovery_cursor_eventually_processes_more_jobs_than_one_page() {
        let components = InMemoryComponents::new(100);
        for index in 0..5 {
            components
                .jobs
                .insert_or_load(queued_job(&components, &format!("job-{index:02}"), 99).await)
                .await
                .unwrap();
        }
        let (_, coordinator) = test_coordinator(
            &components,
            components.jobs.clone(),
            components.authorizer.clone(),
        );

        let first = coordinator.reconcile(2).await.unwrap();
        let second = coordinator.reconcile(2).await.unwrap();
        let third = coordinator.reconcile(2).await.unwrap();

        assert_eq!([first.examined, second.examined, third.examined], [2, 2, 1]);
        assert_eq!(first.expired + second.expired + third.expired, 5);
        assert!(components
            .jobs
            .all()
            .unwrap()
            .iter()
            .all(|job| job.state == JobState::TimedOut));
    }

    #[tokio::test]
    async fn poisoned_job_does_not_starve_later_work_with_single_item_pages() {
        let components = InMemoryComponents::new(100);
        let poisoned = queued_job(&components, "job-a-poisoned", 99).await;
        let later = queued_job(&components, "job-b-later", 99).await;
        components
            .jobs
            .insert_or_load(poisoned.clone())
            .await
            .unwrap();
        components.jobs.insert_or_load(later.clone()).await.unwrap();
        let jobs = Arc::new(PoisonReplaceRepository {
            inner: components.jobs.clone(),
            poisoned: poisoned.key(),
        });
        let (_, coordinator) =
            test_coordinator(&components, jobs.clone(), components.authorizer.clone());

        let first = coordinator.reconcile(1).await.unwrap();

        assert_eq!(first.examined, 1);
        assert_eq!(first.expired, 0);
        assert_eq!(
            components
                .jobs
                .get(&poisoned.key())
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::Queued
        );
        assert_eq!(
            components
                .jobs
                .get(&later.key())
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::Queued
        );

        let second = coordinator.reconcile(1).await.unwrap();

        assert_eq!(second.examined, 1);
        assert_eq!(second.expired, 1);
        assert_eq!(
            components
                .jobs
                .get(&later.key())
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::TimedOut
        );
        // The poison remains retryable after wraparound without turning the round into an error.
        assert_eq!(coordinator.reconcile(1).await.unwrap().examined, 1);
    }

    #[tokio::test]
    async fn prepared_recovery_bypasses_mutable_principal_finalize_permission() {
        let components = InMemoryComponents::new(100);
        let authorizer = Arc::new(RevocableFinalizeAuthorizer::default());
        let (control, coordinator) =
            test_coordinator(&components, components.jobs.clone(), authorizer.clone());
        let queued = queued_job(&components, "job-prepared", 10_000).await;
        let spec = queued.spec.clone();
        let actor = spec.principal.clone();
        let target = assignment_target();
        control
            .create_add_job(CreateAddJobRequest {
                actor: actor.clone(),
                spec: spec.clone(),
            })
            .await
            .unwrap();
        control
            .assign_job(AssignJobRequest {
                actor: actor.clone(),
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: target.clone(),
            })
            .await
            .unwrap();
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: spec.tenant_id.clone(),
                agent_id: target.agent_id.clone(),
                report: AgentReport::Accepted(JobAccepted {
                    job_id: spec.job_id.clone(),
                    assignment_id: target.assignment_id.clone(),
                    assignment_generation: target.assignment_generation,
                    accepted_at_unix_ms: UnixMillis::new(101),
                    request_digest: spec.request_digest,
                    extensions: Extensions::new(),
                }),
            })
            .await
            .unwrap();
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: spec.tenant_id.clone(),
                agent_id: target.agent_id.clone(),
                report: AgentReport::Progress(JobProgress {
                    job_id: spec.job_id.clone(),
                    assignment_id: target.assignment_id.clone(),
                    assignment_generation: target.assignment_generation,
                    state: JobState::Running,
                    phase: "prepare".to_owned(),
                    files_completed: DecimalU64::new(0),
                    bytes_completed: DecimalU64::new(0),
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                }),
            })
            .await
            .unwrap();
        let (prepared, pages) = prepared_metadata(&spec, &target);
        control
            .receive_report(ReceiveReportRequest {
                tenant_id: spec.tenant_id.clone(),
                agent_id: target.agent_id.clone(),
                report: AgentReport::Prepared(prepared.clone()),
            })
            .await
            .unwrap();
        for descriptor in &prepared.metadata_batches {
            control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: spec.tenant_id.clone(),
                    job_id: spec.job_id.clone(),
                    agent_id: target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Descriptor(descriptor.clone()),
                })
                .await
                .unwrap();
        }
        for page in pages {
            control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: spec.tenant_id.clone(),
                    job_id: spec.job_id.clone(),
                    agent_id: target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Page(page),
                })
                .await
                .unwrap();
        }

        authorizer.revoke();
        let denied = control
            .finalize_add(FinalizeAddRequest {
                actor,
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(denied.code(), CentralErrorCode::Unauthorized);

        let run = coordinator.reconcile(10).await.unwrap();
        assert_eq!(run.finalized, 1);
        assert_eq!(
            components
                .jobs
                .get(&spec_key(&spec))
                .await
                .unwrap()
                .unwrap()
                .state,
            JobState::Succeeded
        );
    }

    fn test_coordinator(
        components: &InMemoryComponents,
        jobs: Arc<dyn JobRepository>,
        authorizer: Arc<dyn Authorizer>,
    ) -> (Arc<ControlPlane>, JobCoordinator) {
        let authority = AuthorityStore::from_parts(
            jobs.clone(),
            components.outbox.clone(),
            components.metadata.clone(),
            components.objects.clone(),
            components.publisher.clone(),
            components.audit.clone(),
            AuthorityCapabilities::IN_MEMORY,
        )
        .with_agent_registry(components.agent_registry.clone())
        .with_control_catalog(components.control_catalog.clone());
        let control = Arc::new(ControlPlane::new(
            authorizer,
            authority,
            components.clock.clone(),
        ));
        let coordinator = JobCoordinator {
            control: control.clone(),
            jobs,
            catalog: components.control_catalog.clone(),
            registry: components.agent_registry.clone(),
            indexes: components.publisher.clone(),
            clock: components.clock.clone(),
            heartbeat_timeout_ms: 30_000,
            recovery_cursor: Mutex::new(None),
        };
        (control, coordinator)
    }

    async fn queued_job(
        components: &InMemoryComponents,
        job_id: &str,
        deadline_unix_ms: u64,
    ) -> JobRecord {
        let tenant_id = TenantId::new("tenant-a").unwrap();
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let playground_id = PlaygroundId::new("playground-a").unwrap();
        let expected_index_version = components
            .publisher
            .current_version(&IndexKey {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: playground_id.clone(),
            })
            .await
            .unwrap();
        let principal = PrincipalRef {
            kind: PrincipalKind::User,
            id: PrincipalId::new("user-a").unwrap(),
            extensions: Extensions::new(),
        };
        let mut spec = AddJobSpec {
            job_id: JobId::new(job_id).unwrap(),
            principal,
            tenant_id,
            project_id,
            artifact_id,
            playground_id,
            expected_index_version,
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(deadline_unix_ms),
            paths: vec![LogicalPath::parse("dataset/file.bin").unwrap()],
            all: false,
            extensions: Extensions::new(),
        };
        spec.request_digest = spec.computed_request_digest().unwrap();
        JobRecord {
            spec,
            state: JobState::Queued,
            resource_version: ResourceVersion::new(1),
            assignment: None,
            accepted: None,
            progress: None,
            prepared: None,
            publication_candidate: None,
            decision: None,
            finalized: None,
            finalized_ack: None,
            failure: None,
        }
    }

    fn assignment_target() -> AssignmentTarget {
        AssignmentTarget {
            assignment_id: AssignmentId::new("assignment-prepared").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            lease: None,
        }
    }

    fn prepared_metadata(
        spec: &AddJobSpec,
        target: &AssignmentTarget,
    ) -> (JobPrepared, Vec<MetadataBatchPage>) {
        let manifest = Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new()).unwrap();
        let manifest_id = manifest.canonical_id().unwrap();
        let path = LogicalPath::parse("dataset/file.bin").unwrap();
        let result_record = FileRecord::from_manifest(path.clone(), &manifest).unwrap();
        let result_index_digest = IndexVersion::from_snapshot(1, &[result_record])
            .unwrap()
            .digest;
        let scope = MetadataBatchScope {
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id: spec.playground_id.clone(),
            job_id: spec.job_id.clone(),
            base_index_version: spec.expected_index_version.clone(),
            extensions: Extensions::new(),
        };
        let manifest_page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-manifest").unwrap(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::Manifest(vec![ManifestRecord {
                manifest_id,
                total_size: DecimalU64::new(0),
                chunking: WireChunkingStrategy::FastCdc,
                chunk_start: DecimalU64::new(0),
                chunks: Vec::new(),
                extensions: Extensions::new(),
            }]),
            Extensions::new(),
        )
        .unwrap();
        let index_page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-index").unwrap(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::IndexDelta(vec![IndexDeltaRecord::Upsert {
                path,
                manifest_id,
                total_size: DecimalU64::new(0),
                chunk_count: DecimalU64::new(0),
                extensions: Extensions::new(),
            }]),
            Extensions::new(),
        )
        .unwrap();
        let receipt_page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-receipt").unwrap(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::ObjectReceipt(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let pages = vec![manifest_page, index_page, receipt_page];
        let descriptors = pages
            .iter()
            .map(|page| {
                MetadataBatchDescriptor::from_pages(
                    page.batch_id.clone(),
                    scope.clone(),
                    std::slice::from_ref(page),
                    Extensions::new(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let publication_digest = MetadataPublication::from_pages(&pages)
            .unwrap()
            .publication_digest(&scope, result_index_digest)
            .unwrap();
        let prepared = JobPrepared::new(
            spec.job_id.clone(),
            target.assignment_id.clone(),
            target.assignment_generation,
            spec.expected_index_version.clone(),
            result_index_digest,
            publication_digest,
            descriptors,
            Extensions::new(),
        )
        .unwrap();
        (prepared, pages)
    }

    fn spec_key(spec: &AddJobSpec) -> JobKey {
        JobKey::new(spec.tenant_id.clone(), spec.job_id.clone())
    }

    struct PoisonReplaceRepository {
        inner: Arc<InMemoryJobRepository>,
        poisoned: JobKey,
    }

    #[async_trait]
    impl JobRepository for PoisonReplaceRepository {
        async fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
            self.inner.get(key).await
        }

        async fn list_recoverable(
            &self,
            after: Option<&JobKey>,
            now: UnixMillis,
            limit: usize,
        ) -> CentralResult<Vec<JobRecord>> {
            self.inner.list_recoverable(after, now, limit).await
        }

        async fn list_pending_decisions_for_agent(
            &self,
            agent_id: &AgentId,
            limit: usize,
        ) -> CentralResult<Vec<JobRecord>> {
            self.inner
                .list_pending_decisions_for_agent(agent_id, limit)
                .await
        }

        async fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
            self.inner.insert_or_load(job).await
        }

        async fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
            if job.key() == self.poisoned {
                return Err(CentralError::new(
                    CentralErrorCode::StorageFailure,
                    "injected poisoned Job write",
                ));
            }
            self.inner.replace(expected, job).await
        }
    }

    #[derive(Default)]
    struct RevocableFinalizeAuthorizer {
        revoked: std::sync::atomic::AtomicBool,
    }

    impl RevocableFinalizeAuthorizer {
        fn revoke(&self) {
            self.revoked
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Authorizer for RevocableFinalizeAuthorizer {
        async fn authorize(&self, request: &AuthorizationRequest) -> CentralResult<()> {
            if request.action == Action::FinalizeAdd
                && self.revoked.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CentralError::new(
                    CentralErrorCode::Unauthorized,
                    "principal finalize permission was revoked",
                ));
            }
            Ok(())
        }
    }
}
