use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use async_trait::async_trait;
use neoengram_core::{
    CommitId, FileRecord, IndexVersion, LogicalPath, Manifest, ManifestId, ObjectId,
};
use neoengram_protocol::{
    AgentId, ArtifactId, JobAssignment, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchPage, ObjectReceiptId, PlacementGeneration, StorageVolumeId, TenantId, UnixMillis,
    WireIndexVersion,
};

use crate::{
    apply_cancel, apply_commit, apply_head_publication_ack, apply_job_sync, apply_restart,
    assignment_identity, build_started, same_cancel_request, same_commit_request,
    same_restart_request, same_start_request,
    validation::{invalid, same_index_version, validate_initial_index_snapshot},
    AgentEnrollmentAuditEvent, AssignmentOutbox, AssignmentPublishOutcome,
    AssignmentReserveOutcome, AssignmentRetireOutcome, AuditEvent, AuditSink,
    AuthorityCapabilities, AuthorityStore, AuthorizationRequest, Authorizer, CentralErrorCode,
    CentralResult, Clock, ControlPlane, InMemoryAgentRegistry, IndexKey, IndexPublishOutcome,
    IndexPublishRejection, IndexPublishRequest, IndexPublisher, InitializeIndexSnapshotRequest,
    JobInsertOutcome, JobKey, JobRecord, JobRepository, MetadataBatchStager, ObjectCatalog,
    ObjectPlacementEvidence, PreCommitCancelRequest, PreCommitCommitOutcome,
    PreCommitCommitRequest, PreCommitCommitSnapshot, PreCommitKey, PreCommitMutationOutcome,
    PreCommitRecord, PreCommitRepository, PreCommitRestartRequest, PreCommitStartRequest,
    PublishedIndex, StagedMetadataBatch,
};

#[derive(Debug, Default)]
pub struct AllowAllAuthorizer;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    async fn authorize(&self, _request: &AuthorizationRequest) -> CentralResult<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DenyAllAuthorizer;

#[async_trait]
impl Authorizer for DenyAllAuthorizer {
    async fn authorize(&self, _request: &AuthorizationRequest) -> CentralResult<()> {
        Err(invalid(
            CentralErrorCode::Unauthorized,
            "the actor is not authorized for this managed Add operation",
        ))
    }
}

#[derive(Debug, Default)]
pub struct InMemoryJobRepository {
    jobs: Mutex<BTreeMap<JobKey, JobRecord>>,
}

impl InMemoryJobRepository {
    pub fn all(&self) -> CentralResult<Vec<JobRecord>> {
        Ok(lock(&self.jobs)?.values().cloned().collect())
    }
}

#[async_trait]
impl JobRepository for InMemoryJobRepository {
    async fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        Ok(lock(&self.jobs)?.get(key).cloned())
    }

    async fn list_recoverable(
        &self,
        after: Option<&JobKey>,
        now: UnixMillis,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>> {
        Ok(lock(&self.jobs)?
            .values()
            .filter(|job| after.is_none_or(|after| job.key() > *after))
            .filter(|job| crate::mapper_recovery_predicate(job, now))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_pending_decisions_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobRecord>> {
        Ok(lock(&self.jobs)?
            .values()
            .filter(|job| pending_decision_for_agent(job, agent_id))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        let key = job.key();
        let mut jobs = lock(&self.jobs)?;
        if let Some(existing) = jobs.get(&key) {
            return Ok(JobInsertOutcome::Existing(existing.clone()));
        }
        jobs.insert(key, job.clone());
        Ok(JobInsertOutcome::Inserted(job))
    }

    async fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        let key = job.key();
        let mut jobs = lock(&self.jobs)?;
        let persisted = jobs.get(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::JobNotFound,
                format!("job {} disappeared during update", key.job_id),
            )
        })?;
        if persisted.resource_version.get() != expected
            || job.resource_version.get() != expected.saturating_add(1)
        {
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                format!("job {} ResourceVersion changed", key.job_id),
            ));
        }
        jobs.insert(key, job.clone());
        Ok(job)
    }
}

#[derive(Debug, Default)]
pub struct InMemoryPreCommitRepository {
    state: Mutex<InMemoryPreCommitState>,
}

#[derive(Debug, Default)]
struct InMemoryPreCommitState {
    precommits: BTreeMap<PreCommitKey, PreCommitRecord>,
    commits: BTreeMap<
        (
            TenantId,
            neoengram_protocol::ProjectId,
            ArtifactId,
            CommitId,
        ),
        crate::CommitRecord,
    >,
    mutations: BTreeMap<(TenantId, neoengram_protocol::RequestId), InMemoryPreCommitMutation>,
}

#[derive(Debug, Clone)]
enum InMemoryPreCommitMutation {
    Start {
        request: PreCommitStartRequest,
        result: PreCommitRecord,
    },
    Restart {
        request: PreCommitRestartRequest,
        result: PreCommitRecord,
    },
    Cancel {
        request: PreCommitCancelRequest,
        result: PreCommitRecord,
    },
    Commit {
        request: PreCommitCommitRequest,
        result: PreCommitCommitSnapshot,
    },
}

#[async_trait]
impl PreCommitRepository for InMemoryPreCommitRepository {
    async fn start(
        &self,
        request: PreCommitStartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.tenant_id.clone(),
            request.precommit_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Start {
                    request: stored,
                    result,
                } if same_start_request(stored, &request) => Ok(PreCommitMutationOutcome {
                    precommit: result.clone(),
                    replayed: true,
                }),
                _ => Err(precommit_request_conflict()),
            };
        }
        let record = build_started(&request)?;
        if state.precommits.contains_key(&record.key()) {
            return Err(precommit_request_conflict());
        }
        ensure_active_available(&state, &record, None)?;
        ensure_job_identity_available(&state, &record)?;
        state.precommits.insert(record.key(), record.clone());
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Start {
                request,
                result: record.clone(),
            },
        );
        Ok(PreCommitMutationOutcome {
            precommit: record,
            replayed: false,
        })
    }

    async fn get(&self, key: &PreCommitKey) -> CentralResult<Option<PreCommitRecord>> {
        Ok(lock(&self.state)?.precommits.get(key).cloned())
    }

    async fn get_active(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_protocol::ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &neoengram_protocol::PlaygroundId,
    ) -> CentralResult<Option<PreCommitRecord>> {
        let state = lock(&self.state)?;
        let mut matches = state.precommits.values().filter(|record| {
            &record.tenant_id == tenant_id
                && &record.project_id == project_id
                && &record.artifact_id == artifact_id
                && &record.playground_id == playground_id
                && precommit_is_active(record)
        });
        let result = matches.next().cloned();
        if matches.next().is_some() {
            return Err(crate::CentralError::new(
                CentralErrorCode::Internal,
                "more than one active Pre-commit exists for a Playground",
            ));
        }
        Ok(result)
    }

    async fn list_running(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        Ok(lock(&self.state)?
            .precommits
            .iter()
            .filter(|(key, record)| {
                after.is_none_or(|after| *key > after)
                    && record.state == crate::PreCommitState::Running
            })
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn list_unpublished_commits(
        &self,
        after: Option<&PreCommitKey>,
        limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        Ok(lock(&self.state)?
            .precommits
            .iter()
            .filter(|(key, record)| {
                after.is_none_or(|after| *key > after)
                    && record.state == crate::PreCommitState::Committed
                    && record.head_published_at_unix_ms.is_none()
            })
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn restart(
        &self,
        request: PreCommitRestartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.key.tenant_id.clone(),
            request.restart_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Restart {
                    request: stored,
                    result,
                } if same_restart_request(stored, &request) => Ok(PreCommitMutationOutcome {
                    precommit: result.clone(),
                    replayed: true,
                }),
                _ => Err(precommit_request_conflict()),
            };
        }
        let stored = state
            .precommits
            .get(&request.key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let restarted = apply_restart(stored, &request)?;
        ensure_active_available(&state, &restarted, Some(&restarted.key()))?;
        ensure_job_identity_available(&state, &restarted)?;
        state.precommits.insert(restarted.key(), restarted.clone());
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Restart {
                request,
                result: restarted.clone(),
            },
        );
        Ok(PreCommitMutationOutcome {
            precommit: restarted,
            replayed: false,
        })
    }

    async fn cancel(
        &self,
        request: PreCommitCancelRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.key.tenant_id.clone(),
            request.cancel_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Cancel {
                    request: stored,
                    result,
                } if same_cancel_request(stored, &request) => Ok(PreCommitMutationOutcome {
                    precommit: result.clone(),
                    replayed: true,
                }),
                _ => Err(precommit_request_conflict()),
            };
        }
        let stored = state
            .precommits
            .get(&request.key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let cancelled = apply_cancel(stored, &request)?;
        state.precommits.insert(cancelled.key(), cancelled.clone());
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Cancel {
                request,
                result: cancelled.clone(),
            },
        );
        Ok(PreCommitMutationOutcome {
            precommit: cancelled,
            replayed: false,
        })
    }

    async fn sync_job(
        &self,
        job: JobRecord,
        published_index: Option<PublishedIndex>,
        observed_at_unix_ms: UnixMillis,
    ) -> CentralResult<Option<PreCommitRecord>> {
        let mut state = lock(&self.state)?;
        let found = state
            .precommits
            .iter()
            .find(|(_, precommit)| {
                precommit.tenant_id == job.spec.tenant_id && precommit.job_id == job.spec.job_id
            })
            .map(|(key, precommit)| (key.clone(), precommit.clone()));
        let Some((key, stored)) = found else {
            return Ok(None);
        };
        let base_records = match stored.frozen_head_commit_id {
            None => Some(Vec::new()),
            Some(commit_id) => state
                .commits
                .get(&(
                    stored.tenant_id.clone(),
                    stored.project_id.clone(),
                    stored.artifact_id.clone(),
                    commit_id,
                ))
                .map(|commit| commit.records.clone()),
        };
        let Some(synchronized) = apply_job_sync(
            stored.clone(),
            &job,
            published_index.as_ref(),
            base_records.as_deref(),
            observed_at_unix_ms,
        )?
        else {
            return Ok(None);
        };
        if synchronized != stored {
            state.precommits.insert(key, synchronized.clone());
        }
        Ok(Some(synchronized))
    }

    async fn commit(
        &self,
        request: PreCommitCommitRequest,
    ) -> CentralResult<PreCommitCommitOutcome> {
        let mut state = lock(&self.state)?;
        let mutation_key = (
            request.key.tenant_id.clone(),
            request.commit.commit_request_id.clone(),
        );
        if let Some(existing) = state.mutations.get(&mutation_key) {
            return match existing {
                InMemoryPreCommitMutation::Commit {
                    request: stored,
                    result,
                } if same_commit_request(stored, &request) => {
                    Ok(PreCommitCommitOutcome::from_snapshot(result.clone(), true))
                }
                _ => Err(precommit_request_conflict()),
            };
        }
        let stored = state
            .precommits
            .get(&request.key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let consumed = apply_commit(stored, &request)?;
        let commit_key = (
            consumed.commit.tenant_id.clone(),
            consumed.commit.project_id.clone(),
            consumed.commit.artifact_id.clone(),
            consumed.commit.commit_id,
        );
        if state.commits.contains_key(&commit_key) {
            return Err(precommit_request_conflict());
        }
        state.commits.insert(commit_key, consumed.commit.clone());
        state.precommits.insert(
            consumed.consumed_precommit.key(),
            consumed.consumed_precommit.clone(),
        );
        state.mutations.insert(
            mutation_key,
            InMemoryPreCommitMutation::Commit {
                request,
                result: consumed.clone(),
            },
        );
        Ok(PreCommitCommitOutcome::from_snapshot(consumed, false))
    }

    async fn get_commit(
        &self,
        tenant_id: &TenantId,
        project_id: &neoengram_protocol::ProjectId,
        artifact_id: &ArtifactId,
        commit_id: CommitId,
    ) -> CentralResult<Option<crate::CommitRecord>> {
        Ok(lock(&self.state)?
            .commits
            .get(&(
                tenant_id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                commit_id,
            ))
            .cloned())
    }

    async fn acknowledge_head_publication(
        &self,
        key: &PreCommitKey,
        commit_id: CommitId,
        published_at_unix_ms: UnixMillis,
    ) -> CentralResult<PreCommitRecord> {
        let mut state = lock(&self.state)?;
        let stored = state
            .precommits
            .get(key)
            .cloned()
            .ok_or_else(precommit_not_found)?;
        let acknowledged =
            apply_head_publication_ack(stored.clone(), commit_id, published_at_unix_ms)?;
        if acknowledged != stored {
            state.precommits.insert(key.clone(), acknowledged.clone());
        }
        Ok(acknowledged)
    }
}

fn ensure_job_identity_available(
    state: &InMemoryPreCommitState,
    candidate: &PreCommitRecord,
) -> CentralResult<()> {
    if state.precommits.values().any(|stored| {
        stored.key() != candidate.key()
            && stored.tenant_id == candidate.tenant_id
            && stored.job_id == candidate.job_id
    }) {
        return Err(precommit_request_conflict());
    }
    Ok(())
}

fn ensure_active_available(
    state: &InMemoryPreCommitState,
    candidate: &PreCommitRecord,
    except: Option<&PreCommitKey>,
) -> CentralResult<()> {
    if state.precommits.values().any(|stored| {
        except.is_none_or(|except| stored.key() != *except)
            && stored.tenant_id == candidate.tenant_id
            && stored.project_id == candidate.project_id
            && stored.artifact_id == candidate.artifact_id
            && stored.playground_id == candidate.playground_id
            && precommit_is_active(stored)
    }) {
        return Err(precommit_request_conflict());
    }
    Ok(())
}

fn precommit_is_active(record: &PreCommitRecord) -> bool {
    matches!(
        record.state,
        crate::PreCommitState::Running
            | crate::PreCommitState::Ready
            | crate::PreCommitState::Abnormal
    )
}

fn precommit_not_found() -> crate::CentralError {
    crate::CentralError::new(
        CentralErrorCode::JobNotFound,
        "tenant-scoped Pre-commit was not found",
    )
    .with_retryable(false)
}

fn precommit_request_conflict() -> crate::CentralError {
    crate::CentralError::new(
        CentralErrorCode::ConcurrentUpdate,
        "Pre-commit mutation identity was reused with another payload",
    )
    .with_retryable(false)
}

#[derive(Debug, Default)]
pub struct InMemoryAssignmentOutbox {
    assignments: Mutex<
        BTreeMap<(TenantId, neoengram_protocol::AssignmentId), InMemoryAssignmentReservation>,
    >,
}

#[derive(Debug, Clone)]
struct InMemoryAssignmentReservation {
    assignment: JobAssignment,
    published: bool,
    retired: bool,
}

impl InMemoryAssignmentOutbox {
    pub fn messages(&self) -> CentralResult<Vec<JobAssignment>> {
        Ok(lock(&self.assignments)?
            .values()
            .filter(|reservation| reservation.published)
            .map(|reservation| reservation.assignment.clone())
            .collect())
    }
}

#[async_trait]
impl AssignmentOutbox for InMemoryAssignmentOutbox {
    async fn reserve(&self, assignment: JobAssignment) -> CentralResult<AssignmentReserveOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut assignments = lock(&self.assignments)?;
        let key = (tenant_id.clone(), assignment_id.clone());
        if let Some(existing) = assignments.get(&key) {
            if existing.assignment == assignment {
                return Ok(AssignmentReserveOutcome::Existing);
            }
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "assignment ID {} was reused with another payload",
                    assignment_id
                ),
            ));
        }
        assignments.insert(
            key,
            InMemoryAssignmentReservation {
                assignment,
                published: false,
                retired: false,
            },
        );
        Ok(AssignmentReserveOutcome::Reserved)
    }

    async fn publish(&self, assignment: JobAssignment) -> CentralResult<AssignmentPublishOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut assignments = lock(&self.assignments)?;
        let key = (tenant_id.clone(), assignment_id.clone());
        let existing = assignments.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                format!(
                    "assignment {} must be reserved before publication",
                    assignment_id
                ),
            )
        })?;
        if existing.assignment != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "assignment ID {} was reused with another payload",
                    assignment_id
                ),
            ));
        }
        if existing.published {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        existing.published = true;
        Ok(AssignmentPublishOutcome::Published)
    }

    async fn reactivate(
        &self,
        assignment: JobAssignment,
    ) -> CentralResult<AssignmentPublishOutcome> {
        let (tenant_id, _job_id, assignment_id, _agent_id) = assignment_identity(&assignment);
        let mut assignments = lock(&self.assignments)?;
        let existing = assignments
            .get_mut(&(tenant_id.clone(), assignment_id.clone()))
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    format!("assignment {assignment_id} must be reserved before reactivation"),
                )
            })?;
        if existing.assignment != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!("assignment ID {assignment_id} was reused with another payload"),
            ));
        }
        if existing.published && !existing.retired {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        existing.published = true;
        existing.retired = false;
        Ok(AssignmentPublishOutcome::Published)
    }

    async fn retire(
        &self,
        tenant_id: &TenantId,
        assignment_id: &neoengram_protocol::AssignmentId,
    ) -> CentralResult<AssignmentRetireOutcome> {
        let mut assignments = lock(&self.assignments)?;
        let reservation = assignments
            .get_mut(&(tenant_id.clone(), assignment_id.clone()))
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    format!("assignment {assignment_id} is not reserved"),
                )
            })?;
        if !reservation.published {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("assignment {assignment_id} is not published"),
            ));
        }
        if reservation.retired {
            return Ok(AssignmentRetireOutcome::AlreadyRetired);
        }
        reservation.retired = true;
        Ok(AssignmentRetireOutcome::Retired)
    }

    async fn pending_for_agent(
        &self,
        agent_id: &AgentId,
        limit: usize,
    ) -> CentralResult<Vec<JobAssignment>> {
        Ok(lock(&self.assignments)?
            .values()
            .filter(|reservation| reservation.published && !reservation.retired)
            .filter(|reservation| assignment_identity(&reservation.assignment).3 == agent_id)
            .take(limit)
            .map(|reservation| reservation.assignment.clone())
            .collect())
    }
}

fn pending_decision_for_agent(job: &JobRecord, agent_id: &AgentId) -> bool {
    job.decision.is_some()
        && job.finalized_ack.is_none()
        && job
            .assignment
            .as_ref()
            .is_some_and(|assignment| &assignment.agent_id == agent_id)
}

#[derive(Debug, Default)]
pub struct InMemoryMetadataBatchStager {
    batches: Mutex<BTreeMap<(TenantId, MetadataBatchId), StagedMetadataBatch>>,
}

impl InMemoryMetadataBatchStager {
    pub fn batches(&self) -> CentralResult<Vec<StagedMetadataBatch>> {
        Ok(lock(&self.batches)?.values().cloned().collect())
    }

    /// Removes transient staging material to exercise post-publication catalog independence.
    pub fn clear(&self) -> CentralResult<()> {
        lock(&self.batches)?.clear();
        Ok(())
    }
}

#[async_trait]
impl MetadataBatchStager for InMemoryMetadataBatchStager {
    async fn stage_descriptor(&self, descriptor: MetadataBatchDescriptor) -> CentralResult<bool> {
        descriptor.validate()?;
        let mut batches = lock(&self.batches)?;
        let key = (
            descriptor.scope.tenant_id.clone(),
            descriptor.batch_id.clone(),
        );
        if let Some(existing) = batches.get(&key) {
            if existing.descriptor == descriptor {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!("metadata batch {} descriptor changed", descriptor.batch_id),
            ));
        }
        batches.insert(
            key,
            StagedMetadataBatch {
                descriptor,
                pages: BTreeMap::new(),
            },
        );
        Ok(false)
    }

    async fn stage_page(
        &self,
        descriptor: &MetadataBatchDescriptor,
        page: MetadataBatchPage,
    ) -> CentralResult<bool> {
        descriptor.validate_page(&page)?;
        let mut batches = lock(&self.batches)?;
        let key = (
            descriptor.scope.tenant_id.clone(),
            descriptor.batch_id.clone(),
        );
        let staged = batches.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::BatchIncomplete,
                format!(
                    "metadata batch {} descriptor must be staged before its pages",
                    descriptor.batch_id
                ),
            )
        })?;
        if staged.descriptor != *descriptor {
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!(
                    "metadata batch {} descriptor differs from staging",
                    descriptor.batch_id
                ),
            ));
        }
        if let Some(existing) = staged.pages.get(&page.page_number) {
            if existing == &page {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!(
                    "metadata batch {} page {} changed",
                    descriptor.batch_id, page.page_number
                ),
            ));
        }
        staged.pages.insert(page.page_number, page);
        Ok(false)
    }

    async fn get(
        &self,
        tenant_id: &TenantId,
        batch_id: &MetadataBatchId,
    ) -> CentralResult<Option<StagedMetadataBatch>> {
        Ok(lock(&self.batches)?
            .get(&(tenant_id.clone(), batch_id.clone()))
            .cloned())
    }
}

type PlacementReceiptKey = (TenantId, ObjectReceiptId);

#[derive(Debug, Default)]
pub struct InMemoryObjectCatalog {
    placements: Mutex<BTreeMap<PlacementReceiptKey, ObjectPlacementEvidence>>,
}

#[async_trait]
impl ObjectCatalog for InMemoryObjectCatalog {
    async fn record_placement(&self, evidence: &ObjectPlacementEvidence) -> CentralResult<()> {
        let receipt = &evidence.receipt;
        let key = (receipt.tenant_id.clone(), receipt.receipt_id.clone());
        let mut placements = lock(&self.placements)?;
        if let Some(existing) = placements.get(&key) {
            return if existing == evidence {
                Ok(())
            } else {
                Err(invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!(
                        "object placement receipt {} has conflicting evidence",
                        receipt.receipt_id
                    ),
                ))
            };
        }
        if placements.values().any(|existing| {
            existing.receipt.tenant_id == receipt.tenant_id
                && existing.receipt.artifact_id == receipt.artifact_id
                && existing.receipt.storage_volume_id == receipt.storage_volume_id
                && existing.receipt.artifact_placement_id == receipt.artifact_placement_id
                && existing.placement_generation == evidence.placement_generation
                && existing.receipt.object_id == receipt.object_id
                && existing.receipt.size != receipt.size
        }) {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                format!(
                    "object {} has conflicting placement sizes",
                    receipt.object_id
                ),
            ));
        }
        placements.insert(key, evidence.clone());
        Ok(())
    }

    async fn object_placement(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        storage_volume_id: &StorageVolumeId,
        artifact_placement_id: &neoengram_protocol::ArtifactPlacementId,
        placement_generation: PlacementGeneration,
        object_id: ObjectId,
    ) -> CentralResult<Option<ObjectPlacementEvidence>> {
        Ok(lock(&self.placements)?
            .values()
            .filter(|evidence| {
                let receipt = &evidence.receipt;
                &receipt.tenant_id == tenant_id
                    && &receipt.artifact_id == artifact_id
                    && &receipt.storage_volume_id == storage_volume_id
                    && &receipt.artifact_placement_id == artifact_placement_id
                    && evidence.placement_generation == placement_generation
                    && receipt.object_id == object_id
            })
            .max_by_key(|evidence| evidence.receipt.verified_at_unix_ms)
            .cloned())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct IndexSnapshot {
    version: WireIndexVersion,
    records: BTreeMap<LogicalPath, FileRecord>,
}

#[derive(Debug, Default)]
struct PublisherState {
    indexes: BTreeMap<IndexKey, IndexSnapshot>,
    manifests: BTreeMap<(TenantId, ArtifactId, ManifestId), Manifest>,
    publications: BTreeMap<JobKey, (IndexPublishRequest, IndexPublishOutcome)>,
}

#[derive(Debug, Default)]
pub struct InMemoryIndexPublisher {
    state: Mutex<PublisherState>,
}

impl InMemoryIndexPublisher {
    fn reject(
        state: &mut PublisherState,
        request: IndexPublishRequest,
        rejection: IndexPublishRejection,
    ) -> IndexPublishOutcome {
        let outcome = IndexPublishOutcome::Rejected(rejection);
        state
            .publications
            .insert(request.job_key.clone(), (request, outcome.clone()));
        outcome
    }

    pub fn seed(
        &self,
        key: IndexKey,
        revision: u64,
        records: Vec<FileRecord>,
    ) -> CentralResult<WireIndexVersion> {
        let version = WireIndexVersion::from(
            IndexVersion::from_snapshot(revision, &records).map_err(|error| {
                invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("invalid seeded Index: {error}"),
                )
            })?,
        );
        let mapped = records
            .into_iter()
            .map(|record| (record.path.clone(), record))
            .collect();
        lock(&self.state)?.indexes.insert(
            key,
            IndexSnapshot {
                version: version.clone(),
                records: mapped,
            },
        );
        Ok(version)
    }

    pub fn snapshot(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        let state = lock(&self.state)?;
        let snapshot = state.indexes.get(key).cloned().unwrap_or(empty_snapshot()?);
        Ok(PublishedIndex {
            version: snapshot.version,
            records: snapshot.records.into_values().collect(),
        })
    }

    pub fn manifest(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        manifest_id: ManifestId,
    ) -> CentralResult<Option<Manifest>> {
        Ok(lock(&self.state)?
            .manifests
            .get(&(tenant_id.clone(), artifact_id.clone(), manifest_id))
            .cloned())
    }
}

#[async_trait]
impl IndexPublisher for InMemoryIndexPublisher {
    async fn initialize_snapshot(
        &self,
        request: InitializeIndexSnapshotRequest,
    ) -> CentralResult<WireIndexVersion> {
        validate_initial_index_snapshot(&request.version, &request.records)?;
        let records = request
            .records
            .into_iter()
            .map(|record| (record.path.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let snapshot = IndexSnapshot {
            version: request.version,
            records,
        };
        let mut state = lock(&self.state)?;
        if let Some(existing) = state.indexes.get(&request.index_key) {
            if same_index_version(&existing.version, &snapshot.version)
                && existing.records == snapshot.records
            {
                return Ok(existing.version.clone());
            }
            return Err(invalid(
                CentralErrorCode::ConcurrentUpdate,
                format!(
                    "Index for Playground {} is already initialized with a different snapshot",
                    request.index_key.playground_id
                ),
            ));
        }
        let version = snapshot.version.clone();
        state.indexes.insert(request.index_key, snapshot);
        Ok(version)
    }

    async fn compare_and_swap(
        &self,
        request: IndexPublishRequest,
    ) -> CentralResult<IndexPublishOutcome> {
        if request.job_key.tenant_id != request.index_key.tenant_id {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "Index publication Job and Index tenants differ",
            ));
        }
        let mut state = lock(&self.state)?;
        if let Some((existing_request, outcome)) = state.publications.get(&request.job_key) {
            if existing_request == &request {
                return Ok(outcome.clone());
            }
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "job {} attempted a different Index publication",
                    request.job_key.job_id
                ),
            ));
        }

        let current = state
            .indexes
            .get(&request.index_key)
            .cloned()
            .unwrap_or(empty_snapshot()?);
        if !same_index_version(&current.version, &request.expected_index_version) {
            let outcome = IndexPublishOutcome::Conflict(current.version);
            state
                .publications
                .insert(request.job_key.clone(), (request, outcome.clone()));
            return Ok(outcome);
        }

        let mut manifests = BTreeMap::new();
        for manifest in &request.manifests {
            let manifest_id = match manifest.canonical_id() {
                Ok(manifest_id) => manifest_id,
                Err(error) => {
                    return Ok(Self::reject(
                        &mut state,
                        request,
                        IndexPublishRejection::InvalidMetadata {
                            message: format!("invalid publication Manifest: {error}"),
                        },
                    ));
                }
            };
            if manifests.insert(manifest_id, manifest.clone()).is_some() {
                return Ok(Self::reject(
                    &mut state,
                    request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!("publication repeats Manifest {manifest_id}"),
                    },
                ));
            }
        }
        let mut referenced_manifests = BTreeSet::new();
        let mut manifest_metadata_error = None;
        for mutation in &request.mutations {
            let neoengram_protocol::IndexDeltaRecord::Upsert {
                manifest_id,
                total_size,
                chunk_count,
                ..
            } = mutation
            else {
                continue;
            };
            referenced_manifests.insert(*manifest_id);
            if let Some(manifest) = manifests.get(manifest_id) {
                match manifest.chunk_count() {
                    Ok(observed_chunk_count)
                        if manifest.total_size == total_size.get()
                            && observed_chunk_count == chunk_count.get() => {}
                    Ok(_) => {
                        manifest_metadata_error = Some(format!(
                            "Index upsert metadata differs from Manifest {manifest_id}"
                        ));
                        break;
                    }
                    Err(error) => {
                        manifest_metadata_error =
                            Some(format!("invalid publication Manifest: {error}"));
                        break;
                    }
                }
            }
        }
        if let Some(message) = manifest_metadata_error {
            return Ok(Self::reject(
                &mut state,
                request,
                IndexPublishRejection::InvalidMetadata { message },
            ));
        }
        if manifests.keys().copied().collect::<BTreeSet<_>>() != referenced_manifests {
            return Ok(Self::reject(
                &mut state,
                request,
                IndexPublishRejection::InvalidMetadata {
                    message: "publication Manifests are not the exact Index upsert set".to_owned(),
                },
            ));
        }
        for (manifest_id, manifest) in &manifests {
            let key = (
                request.index_key.tenant_id.clone(),
                request.index_key.artifact_id.clone(),
                *manifest_id,
            );
            if state
                .manifests
                .get(&key)
                .is_some_and(|existing| existing != manifest)
            {
                return Ok(Self::reject(
                    &mut state,
                    request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!(
                            "immutable catalog already contains different content for Manifest {manifest_id}"
                        ),
                    },
                ));
            }
        }

        let mut records = current.records;
        for mutation in &request.mutations {
            match mutation {
                neoengram_protocol::IndexDeltaRecord::Upsert {
                    path,
                    manifest_id,
                    total_size,
                    chunk_count,
                    ..
                } => {
                    let record = match FileRecord::new(
                        path.clone(),
                        *manifest_id,
                        total_size.get(),
                        chunk_count.get(),
                    ) {
                        Ok(record) => record,
                        Err(error) => {
                            return Ok(Self::reject(
                                &mut state,
                                request,
                                IndexPublishRejection::InvalidMetadata {
                                    message: format!("invalid Index upsert: {error}"),
                                },
                            ));
                        }
                    };
                    records.insert(path.clone(), record);
                }
                neoengram_protocol::IndexDeltaRecord::Delete { path, .. } => {
                    records.remove(path);
                }
            }
        }
        let Some(revision) = current.version.revision.get().checked_add(1) else {
            return Ok(Self::reject(
                &mut state,
                request,
                IndexPublishRejection::RevisionExhausted,
            ));
        };
        let ordered = records.values().cloned().collect::<Vec<_>>();
        let version = match IndexVersion::from_snapshot(revision, &ordered) {
            Ok(version) => WireIndexVersion::from(version),
            Err(error) => {
                return Ok(Self::reject(
                    &mut state,
                    request,
                    IndexPublishRejection::InvalidMetadata {
                        message: format!("published Index is invalid: {error}"),
                    },
                ));
            }
        };
        if version.digest != request.expected_result_digest {
            let rejection = IndexPublishRejection::ResultDigestMismatch {
                expected_digest: request.expected_result_digest,
                observed_digest: version.digest,
            };
            return Ok(Self::reject(&mut state, request, rejection));
        }
        let outcome = IndexPublishOutcome::Published(version.clone());
        state.indexes.insert(
            request.index_key.clone(),
            IndexSnapshot { version, records },
        );
        for (manifest_id, manifest) in manifests {
            state
                .manifests
                .entry((
                    request.index_key.tenant_id.clone(),
                    request.index_key.artifact_id.clone(),
                    manifest_id,
                ))
                .or_insert(manifest);
        }
        state
            .publications
            .insert(request.job_key.clone(), (request, outcome.clone()));
        Ok(outcome)
    }

    async fn current_version(&self, key: &IndexKey) -> CentralResult<WireIndexVersion> {
        Ok(lock(&self.state)?
            .indexes
            .get(key)
            .map(|snapshot| snapshot.version.clone())
            .unwrap_or(empty_snapshot()?.version))
    }

    async fn published_index(&self, key: &IndexKey) -> CentralResult<PublishedIndex> {
        self.snapshot(key)
    }
}

#[derive(Debug, Default)]
pub struct InMemoryAuditSink {
    events: Mutex<BTreeMap<(TenantId, String), AuditEvent>>,
    enrollment_events: Mutex<BTreeMap<(TenantId, String), AgentEnrollmentAuditEvent>>,
}

impl InMemoryAuditSink {
    pub fn events(&self) -> CentralResult<Vec<AuditEvent>> {
        Ok(lock(&self.events)?.values().cloned().collect())
    }

    pub fn enrollment_events(&self) -> CentralResult<Vec<AgentEnrollmentAuditEvent>> {
        Ok(lock(&self.enrollment_events)?.values().cloned().collect())
    }
}

#[async_trait]
impl AuditSink for InMemoryAuditSink {
    async fn record(&self, event: AuditEvent) -> CentralResult<bool> {
        let mut events = lock(&self.events)?;
        let key = (event.job_key.tenant_id.clone(), event.event_id.clone());
        if let Some(existing) = events.get(&key) {
            if existing.kind == event.kind
                && existing.job_key == event.job_key
                && existing.state == event.state
            {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::Internal,
                format!("audit event ID {} was reused", event.event_id),
            ));
        }
        events.insert(key, event);
        Ok(false)
    }

    async fn record_enrollment_decision(
        &self,
        event: AgentEnrollmentAuditEvent,
    ) -> CentralResult<bool> {
        let mut events = lock(&self.enrollment_events)?;
        let key = (event.tenant_id.clone(), event.event_id.clone());
        if let Some(existing) = events.get(&key) {
            if existing.kind == event.kind
                && existing.enrollment_id == event.enrollment_id
                && existing.storage_volume_id == event.storage_volume_id
                && existing.decision_request_id == event.decision_request_id
                && existing.resource_version == event.resource_version
                && existing.actor == event.actor
            {
                return Ok(true);
            }
            return Err(invalid(
                CentralErrorCode::Internal,
                format!("enrollment audit event ID {} was reused", event.event_id),
            ));
        }
        events.insert(key, event);
        Ok(false)
    }
}

#[derive(Debug)]
pub struct InMemoryClock {
    now_ms: AtomicU64,
}

impl InMemoryClock {
    #[must_use]
    pub const fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    pub fn set(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }

    pub fn advance(&self, delta_ms: u64) -> CentralResult<u64> {
        self.now_ms
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(delta_ms)
            })
            .map(|previous| previous + delta_ms)
            .map_err(|_| invalid(CentralErrorCode::Internal, "clock overflow"))
    }
}

impl Default for InMemoryClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Clock for InMemoryClock {
    fn now(&self) -> UnixMillis {
        UnixMillis::new(self.now_ms.load(Ordering::SeqCst))
    }
}

/// Convenient composition root for deterministic component and state-machine tests.
pub struct InMemoryComponents {
    pub authorizer: Arc<AllowAllAuthorizer>,
    pub jobs: Arc<InMemoryJobRepository>,
    pub outbox: Arc<InMemoryAssignmentOutbox>,
    pub metadata: Arc<InMemoryMetadataBatchStager>,
    pub objects: Arc<InMemoryObjectCatalog>,
    pub publisher: Arc<InMemoryIndexPublisher>,
    pub audit: Arc<InMemoryAuditSink>,
    pub precommits: Arc<InMemoryPreCommitRepository>,
    pub agent_registry: Arc<InMemoryAgentRegistry>,
    pub control_catalog: Arc<crate::InMemoryControlCatalog>,
    pub clock: Arc<InMemoryClock>,
}

impl InMemoryComponents {
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self {
            authorizer: Arc::new(AllowAllAuthorizer),
            jobs: Arc::new(InMemoryJobRepository::default()),
            outbox: Arc::new(InMemoryAssignmentOutbox::default()),
            metadata: Arc::new(InMemoryMetadataBatchStager::default()),
            objects: Arc::new(InMemoryObjectCatalog::default()),
            publisher: Arc::new(InMemoryIndexPublisher::default()),
            audit: Arc::new(InMemoryAuditSink::default()),
            precommits: Arc::new(InMemoryPreCommitRepository::default()),
            agent_registry: Arc::new(InMemoryAgentRegistry::new()),
            control_catalog: Arc::new(crate::InMemoryControlCatalog::default()),
            clock: Arc::new(InMemoryClock::new(now_ms)),
        }
    }

    #[must_use]
    pub fn control_plane(&self) -> ControlPlane {
        ControlPlane::new(
            self.authorizer.clone(),
            self.authority_store(),
            self.clock.clone(),
        )
    }

    #[must_use]
    pub fn authority_store(&self) -> AuthorityStore {
        AuthorityStore::from_parts(
            self.jobs.clone(),
            self.outbox.clone(),
            self.metadata.clone(),
            self.objects.clone(),
            self.publisher.clone(),
            self.audit.clone(),
            AuthorityCapabilities::IN_MEMORY,
        )
        .with_precommits(self.precommits.clone())
        .with_agent_registry(self.agent_registry.clone())
        .with_control_catalog(self.control_catalog.clone())
    }
}

fn empty_snapshot() -> CentralResult<IndexSnapshot> {
    let version = WireIndexVersion::from(IndexVersion::from_snapshot(0, &[]).map_err(|error| {
        invalid(
            CentralErrorCode::Internal,
            format!("failed to construct empty Index: {error}"),
        )
    })?);
    Ok(IndexSnapshot {
        version,
        records: BTreeMap::new(),
    })
}

fn lock<T>(mutex: &Mutex<T>) -> CentralResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        invalid(
            CentralErrorCode::Internal,
            "in-memory adapter lock poisoned",
        )
    })
}
