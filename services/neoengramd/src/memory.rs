use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use async_trait::async_trait;
use neoengram_core::{FileRecord, IndexVersion, LogicalPath, Manifest, ManifestId, ObjectId};
use neoengram_protocol::{
    ArtifactId, AssignmentOperation, JobAssignment, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchPage, ObjectDurabilityReceipt, TenantId, UnixMillis, WireIndexVersion,
};

use crate::{
    validation::{durable_from_receipt, invalid, same_index_version},
    AssignmentOutbox, AssignmentPublishOutcome, AssignmentReserveOutcome, AuditEvent, AuditSink,
    AuthorityCapabilities, AuthorityStore, AuthorizationRequest, Authorizer, CentralErrorCode,
    CentralResult, Clock, ControlPlane, DurableObject, IndexKey, IndexPublishOutcome,
    IndexPublishRejection, IndexPublishRequest, IndexPublisher, JobInsertOutcome, JobKey,
    JobRecord, JobRepository, MetadataBatchStager, ObjectCatalog, PublishedIndex,
    StagedMetadataBatch,
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
pub struct InMemoryAssignmentOutbox {
    assignments: Mutex<
        BTreeMap<(TenantId, neoengram_protocol::AssignmentId), InMemoryAssignmentReservation>,
    >,
}

#[derive(Debug, Clone)]
struct InMemoryAssignmentReservation {
    assignment: JobAssignment,
    published: bool,
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
        let AssignmentOperation::Add { input: add, .. } = &assignment.assignment;
        let mut assignments = lock(&self.assignments)?;
        let key = (add.tenant_id.clone(), add.assignment_id.clone());
        if let Some(existing) = assignments.get(&key) {
            if existing.assignment == assignment {
                return Ok(AssignmentReserveOutcome::Existing);
            }
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "assignment ID {} was reused with another payload",
                    add.assignment_id
                ),
            ));
        }
        assignments.insert(
            key,
            InMemoryAssignmentReservation {
                assignment,
                published: false,
            },
        );
        Ok(AssignmentReserveOutcome::Reserved)
    }

    async fn publish(&self, assignment: JobAssignment) -> CentralResult<AssignmentPublishOutcome> {
        let AssignmentOperation::Add { input: add, .. } = &assignment.assignment;
        let mut assignments = lock(&self.assignments)?;
        let key = (add.tenant_id.clone(), add.assignment_id.clone());
        let existing = assignments.get_mut(&key).ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                format!(
                    "assignment {} must be reserved before publication",
                    add.assignment_id
                ),
            )
        })?;
        if existing.assignment != assignment {
            return Err(invalid(
                CentralErrorCode::JobIdReused,
                format!(
                    "assignment ID {} was reused with another payload",
                    add.assignment_id
                ),
            ));
        }
        if existing.published {
            return Ok(AssignmentPublishOutcome::AlreadyPublished);
        }
        existing.published = true;
        Ok(AssignmentPublishOutcome::Published)
    }
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

type ObjectKey = (TenantId, ArtifactId, ObjectId);

#[derive(Debug, Default)]
pub struct InMemoryObjectCatalog {
    objects: Mutex<BTreeMap<ObjectKey, DurableObject>>,
}

impl InMemoryObjectCatalog {
    fn record_durability_inner(&self, receipt: &ObjectDurabilityReceipt) -> CentralResult<()> {
        let key = (
            receipt.tenant_id.clone(),
            receipt.artifact_id.clone(),
            receipt.object.object_id,
        );
        let mut objects = lock(&self.objects)?;
        // A later upload attempt cannot revoke an earlier proof for immutable content.
        if let Some(durable) = durable_from_receipt(receipt)? {
            if let Some(existing) = objects.get(&key) {
                if existing != &durable {
                    return Err(invalid(
                        CentralErrorCode::MetadataInvalid,
                        format!(
                            "object {} has conflicting durability metadata",
                            receipt.object.object_id
                        ),
                    ));
                }
            } else {
                objects.insert(key, durable);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectCatalog for InMemoryObjectCatalog {
    async fn record_durability(&self, receipt: &ObjectDurabilityReceipt) -> CentralResult<()> {
        self.record_durability_inner(receipt)
    }

    async fn durable_object(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object_id: ObjectId,
    ) -> CentralResult<Option<DurableObject>> {
        Ok(lock(&self.objects)?
            .get(&(tenant_id.clone(), artifact_id.clone(), object_id))
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
}

#[derive(Debug, Default)]
pub struct InMemoryAuditSink {
    events: Mutex<BTreeMap<(TenantId, String), AuditEvent>>,
}

impl InMemoryAuditSink {
    pub fn events(&self) -> CentralResult<Vec<AuditEvent>> {
        Ok(lock(&self.events)?.values().cloned().collect())
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
