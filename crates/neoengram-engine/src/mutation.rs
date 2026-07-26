use std::io::{Cursor, Read};

use serde::{de, Deserialize, Deserializer, Serialize};

use neoengram_core::{ContentDigest, LogicalPath, Manifest, ManifestId, ObjectSpec};

use crate::{
    canonical::CanonicalHasher, Clock, EngineError, EngineResult, ErrorCode, FailureInjector,
    FailurePoint, ImmutableCatalogReader, IndexVersion, JournalStore, LockManager, ObjectStore,
    PageRequest, ProgressEvent, ProgressPhase, ProgressSink, ProgressUnit, Worktree, MAX_PAGE_SIZE,
};

/// Domain separator for the canonical identity of a complete [`MutationPlan`].
pub const MUTATION_PLAN_HASH_DOMAIN: &[u8] = b"neoengram-engine-mutation-plan-v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct JournalId(String);

impl JournalId {
    pub fn new(value: impl Into<String>) -> EngineResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "journal id cannot be empty",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(&self) -> EngineResult<()> {
        if self.0.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "journal id cannot be empty",
            ));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for JournalId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Stable identity and fencing inputs attached to every managed mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationIdentity {
    pub tenant_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub job_id: String,
    pub storage_volume_id: String,
    pub owner_generation: u64,
    pub fencing_token: u64,
}

impl MutationIdentity {
    pub fn validate(&self) -> EngineResult<()> {
        for (name, value) in [
            ("tenant_id", &self.tenant_id),
            ("artifact_id", &self.artifact_id),
            ("playground_id", &self.playground_id),
            ("job_id", &self.job_id),
            ("storage_volume_id", &self.storage_volume_id),
        ] {
            if value.is_empty() {
                return Err(EngineError::new(
                    ErrorCode::InvalidArgument,
                    format!("{name} cannot be empty"),
                ));
            }
        }
        if self.owner_generation == 0 || self.fencing_token == 0 {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "mutation owner generation and fencing token must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationCheckpoint {
    Start,
    BeforeJournalPublish,
    BeforeIrreversibleWrite,
    BeforeReceipt,
    Recovery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MutationAction {
    CreateDirectory {
        path: LogicalPath,
    },
    WriteFile {
        path: LogicalPath,
        manifest_id: ManifestId,
        total_size: u64,
    },
    RemoveFile {
        path: LogicalPath,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationPlan {
    pub plan_id: String,
    pub identity: MutationIdentity,
    pub expected_index_version: IndexVersion,
    pub actions: Vec<MutationAction>,
    digest: ContentDigest,
}

impl MutationPlan {
    /// Constructs a validated plan and computes its canonical digest.
    pub fn new(
        plan_id: impl Into<String>,
        identity: MutationIdentity,
        expected_index_version: IndexVersion,
        actions: Vec<MutationAction>,
    ) -> EngineResult<Self> {
        let mut plan = Self {
            plan_id: plan_id.into(),
            identity,
            expected_index_version,
            actions,
            digest: ContentDigest::from_bytes([0; 32]),
        };
        plan.validate_content()?;
        plan.digest = plan.compute_canonical_digest()?;
        Ok(plan)
    }

    /// Returns the digest stored with this plan.
    #[must_use]
    pub const fn digest(&self) -> ContentDigest {
        self.digest
    }

    /// Recomputes the canonical digest from every plan identity, fencing, CAS, and action field.
    pub fn canonical_digest(&self) -> EngineResult<ContentDigest> {
        self.validate_content()?;
        self.compute_canonical_digest()
    }

    pub fn validate(&self) -> EngineResult<()> {
        self.validate_content()?;
        let canonical_digest = self.compute_canonical_digest()?;
        if self.digest != canonical_digest {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "mutation plan digest does not match its canonical content",
            )
            .with_context("stored_digest", self.digest.to_string())
            .with_context("canonical_digest", canonical_digest.to_string()));
        }
        Ok(())
    }

    fn validate_content(&self) -> EngineResult<()> {
        if self.plan_id.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "plan_id cannot be empty",
            ));
        }
        self.identity.validate()
    }

    fn compute_canonical_digest(&self) -> EngineResult<ContentDigest> {
        let mut hasher = CanonicalHasher::new(MUTATION_PLAN_HASH_DOMAIN)?;
        hasher.write_str(&self.plan_id)?;
        hasher.write_str(&self.identity.tenant_id)?;
        hasher.write_str(&self.identity.artifact_id)?;
        hasher.write_str(&self.identity.playground_id)?;
        hasher.write_str(&self.identity.job_id)?;
        hasher.write_str(&self.identity.storage_volume_id)?;
        hasher.write_u64(self.identity.owner_generation);
        hasher.write_u64(self.identity.fencing_token);
        hasher.write_u64(self.expected_index_version.revision);
        hasher.write_digest(self.expected_index_version.digest)?;
        hasher.write_len(self.actions.len())?;
        for action in &self.actions {
            match action {
                MutationAction::CreateDirectory { path } => {
                    hasher.write_u8(1);
                    hasher.write_str(path.as_str())?;
                }
                MutationAction::WriteFile {
                    path,
                    manifest_id,
                    total_size,
                } => {
                    hasher.write_u8(2);
                    hasher.write_str(path.as_str())?;
                    hasher.write_bytes(manifest_id.as_bytes())?;
                    hasher.write_u64(*total_size);
                }
                MutationAction::RemoveFile { path } => {
                    hasher.write_u8(3);
                    hasher.write_str(path.as_str())?;
                }
            }
        }
        Ok(hasher.finish())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalPhase {
    Prepared,
    Applying,
    Applied,
    AwaitingFinalize,
    Finalized,
    RollingBack,
    RecoveryRequired,
}

impl JournalPhase {
    #[must_use]
    pub const fn is_active(self) -> bool {
        !matches!(self, Self::Finalized)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecord {
    pub id: JournalId,
    pub revision: u64,
    pub phase: JournalPhase,
    pub identity: MutationIdentity,
    pub plan_id: String,
    pub plan_digest: ContentDigest,
    /// Adapter-independent recovery payload owned by the operation implementation.
    pub recovery_payload: Vec<u8>,
    pub updated_at_unix_ms: u64,
}

impl JournalRecord {
    pub fn validate(&self) -> EngineResult<()> {
        self.id.validate()?;
        self.identity.validate()?;
        if self.plan_id.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "journal plan_id cannot be empty",
            ));
        }
        Ok(())
    }

    pub fn validate_successor(&self, previous: &Self) -> EngineResult<()> {
        previous.validate()?;
        self.validate()?;
        let expected_revision = previous.revision.checked_add(1).ok_or_else(|| {
            EngineError::new(ErrorCode::Conflict, "journal revision cannot advance")
        })?;
        if self.id != previous.id
            || self.identity != previous.identity
            || self.plan_id != previous.plan_id
            || self.plan_digest != previous.plan_digest
            || self.revision != expected_revision
        {
            return Err(EngineError::new(
                ErrorCode::Conflict,
                "journal successor changes immutable identity or skips a revision",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeReceiptStatus {
    Applied,
    RolledBack,
    RecoveryRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeReceipt {
    pub plan_id: String,
    pub job_id: String,
    pub journal_id: JournalId,
    pub status: WorktreeReceiptStatus,
    pub actions_applied: u64,
    pub bytes_written: u64,
    pub owner_generation: u64,
    pub fencing_token: u64,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
}

/// Input to the journaled Worktree mutation use case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecuteMutationRequest {
    pub plan: MutationPlan,
    /// Adapter-owned recovery state persisted before the first Worktree change.
    pub recovery_payload: Vec<u8>,
}

/// A receipt is produced before any authoritative Index/ref publication occurs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecuteMutationResult {
    pub receipt: WorktreeReceipt,
}

/// Adapter-reported statistics for one completely applied Worktree plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationApplication {
    pub actions_applied: u64,
    pub bytes_written: u64,
}

/// Explicit acknowledgement used only after an outer publisher has committed authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeMutationRequest {
    pub journal_id: JournalId,
    pub plan_digest: ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeMutationResult {
    pub journal_id: JournalId,
    pub finalized_at_unix_ms: u64,
}

/// Applies a Worktree plan only after publishing a durable journal.
#[allow(clippy::too_many_arguments)]
pub fn execute_mutation(
    request: &ExecuteMutationRequest,
    worktree: &dyn Worktree,
    catalog: &dyn ImmutableCatalogReader,
    objects: &dyn ObjectStore,
    journals: &dyn JournalStore,
    locks: &dyn LockManager,
    clock: &dyn Clock,
    progress: &dyn ProgressSink,
    failures: &dyn FailureInjector,
) -> EngineResult<ExecuteMutationResult> {
    request.plan.validate()?;
    let guard = locks.acquire(&request.plan.identity)?;
    if guard.identity() != &request.plan.identity {
        return Err(EngineError::new(
            ErrorCode::FenceRejected,
            "mutation lock returned a guard for a different identity",
        ));
    }
    guard.checkpoint(MutationCheckpoint::Start)?;
    failures.check(FailurePoint::BeforeJournalPublish)?;
    guard.checkpoint(MutationCheckpoint::BeforeJournalPublish)?;

    let started_at_unix_ms = clock.now_unix_ms()?;
    let journal_id = JournalId::new(request.plan.plan_id.clone())?;
    let mut journal = JournalRecord {
        id: journal_id.clone(),
        revision: 0,
        phase: JournalPhase::Prepared,
        identity: request.plan.identity.clone(),
        plan_id: request.plan.plan_id.clone(),
        plan_digest: request.plan.digest(),
        recovery_payload: request.recovery_payload.clone(),
        updated_at_unix_ms: started_at_unix_ms,
    };
    journals.create(&journal)?;
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Journaling,
        ProgressUnit::Actions,
        0,
    ))?;

    advance_journal(
        journals,
        &mut journal,
        JournalPhase::Applying,
        clock.now_unix_ms()?,
    )?;
    let result = guard
        .checkpoint(MutationCheckpoint::BeforeIrreversibleWrite)
        .and_then(|()| failures.check(FailurePoint::BeforeMutation))
        .and_then(|()| {
            worktree.apply_mutation(&request.plan, catalog, objects, progress, failures)
        });
    let application = match result {
        Ok(result) => result,
        Err(error) => {
            mark_recovery_required(journals, &mut journal, clock);
            return Err(error);
        }
    };

    advance_journal(
        journals,
        &mut journal,
        JournalPhase::Applied,
        clock.now_unix_ms()?,
    )?;
    if let Err(error) = guard
        .checkpoint(MutationCheckpoint::BeforeReceipt)
        .and_then(|()| failures.check(FailurePoint::BeforeReceipt))
    {
        mark_recovery_required(journals, &mut journal, clock);
        return Err(error);
    }
    let finished_at_unix_ms = clock.now_unix_ms()?;
    advance_journal(
        journals,
        &mut journal,
        JournalPhase::AwaitingFinalize,
        finished_at_unix_ms,
    )?;
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Completed,
        ProgressUnit::Actions,
        application.actions_applied,
    ))?;

    Ok(ExecuteMutationResult {
        receipt: WorktreeReceipt {
            plan_id: request.plan.plan_id.clone(),
            job_id: request.plan.identity.job_id.clone(),
            journal_id,
            status: WorktreeReceiptStatus::Applied,
            actions_applied: application.actions_applied,
            bytes_written: application.bytes_written,
            owner_generation: request.plan.identity.owner_generation,
            fencing_token: request.plan.identity.fencing_token,
            started_at_unix_ms,
            finished_at_unix_ms,
        },
    })
}

/// Marks the journal final only after the caller's authoritative publisher has succeeded.
pub fn finalize_mutation(
    request: &FinalizeMutationRequest,
    journals: &dyn JournalStore,
    clock: &dyn Clock,
) -> EngineResult<FinalizeMutationResult> {
    let mut journal = journals.load(&request.journal_id)?.ok_or_else(|| {
        EngineError::new(ErrorCode::ResourceNotFound, "mutation journal is missing")
    })?;
    if journal.plan_digest != request.plan_digest {
        return Err(EngineError::new(
            ErrorCode::Conflict,
            "mutation finalization digest does not match the durable journal",
        ));
    }
    if journal.phase == JournalPhase::Finalized {
        return Ok(FinalizeMutationResult {
            journal_id: journal.id,
            finalized_at_unix_ms: journal.updated_at_unix_ms,
        });
    }
    if journal.phase != JournalPhase::AwaitingFinalize {
        return Err(EngineError::new(
            ErrorCode::RecoveryRequired,
            "mutation journal is not awaiting authoritative finalization",
        ));
    }
    let finalized_at_unix_ms = clock.now_unix_ms()?;
    advance_journal(
        journals,
        &mut journal,
        JournalPhase::Finalized,
        finalized_at_unix_ms,
    )?;
    Ok(FinalizeMutationResult {
        journal_id: journal.id,
        finalized_at_unix_ms,
    })
}

pub(crate) fn apply_worktree_actions<W>(
    plan: &MutationPlan,
    worktree: &W,
    catalog: &dyn ImmutableCatalogReader,
    objects: &dyn ObjectStore,
    progress: &dyn ProgressSink,
    failures: &dyn FailureInjector,
) -> EngineResult<MutationApplication>
where
    W: Worktree + ?Sized,
{
    let total = u64::try_from(plan.actions.len())
        .map_err(|_| EngineError::new(ErrorCode::Internal, "mutation action count exceeds u64"))?;
    let mut applied = 0_u64;
    let mut bytes_written = 0_u64;
    for action in &plan.actions {
        failures.check(FailurePoint::BeforeMutation)?;
        match action {
            MutationAction::CreateDirectory { path } => worktree.create_dir_all(path)?,
            MutationAction::WriteFile {
                path,
                manifest_id,
                total_size,
            } => {
                let manifest = load_manifest(catalog, manifest_id)?;
                if manifest.total_size != *total_size {
                    return Err(EngineError::new(
                        ErrorCode::IntegrityViolation,
                        "mutation action size differs from its canonical Manifest",
                    )
                    .with_context("manifest_id", manifest_id.to_string()));
                }
                let mut source = ManifestObjectReader::new(objects, manifest.chunks);
                worktree.write_file_atomic(path, *total_size, &mut source)?;
                bytes_written = bytes_written.checked_add(*total_size).ok_or_else(|| {
                    EngineError::new(ErrorCode::Internal, "mutation byte count exceeds u64")
                })?;
            }
            MutationAction::RemoveFile { path } => {
                if !worktree.remove_file(path)? {
                    return Err(EngineError::new(
                        ErrorCode::ResourceNotFound,
                        "mutation target file is missing",
                    )
                    .with_context("logical_path", path.to_string()));
                }
            }
        }
        applied = applied.checked_add(1).ok_or_else(|| {
            EngineError::new(ErrorCode::Internal, "mutation action count exceeds u64")
        })?;
        progress.emit(
            &ProgressEvent::new(
                ProgressPhase::ApplyingMutation,
                ProgressUnit::Actions,
                applied,
            )
            .with_total(total),
        )?;
    }
    Ok(MutationApplication {
        actions_applied: applied,
        bytes_written,
    })
}

fn load_manifest(
    catalog: &dyn ImmutableCatalogReader,
    manifest_id: &ManifestId,
) -> EngineResult<Manifest> {
    let metadata = catalog.get_manifest(manifest_id)?.ok_or_else(|| {
        EngineError::new(ErrorCode::ObjectMissing, "mutation Manifest is missing")
            .with_context("manifest_id", manifest_id.to_string())
    })?;
    let mut chunks = Vec::new();
    let mut request = PageRequest::first(MAX_PAGE_SIZE)?;
    loop {
        let page = catalog.scan_manifest_chunks(manifest_id, &request)?;
        if page.items.len() > request.limit as usize {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Manifest reader returned more chunks than requested",
            ));
        }
        chunks.extend(page.items);
        let Some(next) = page.next else {
            break;
        };
        if request.after.as_ref() == Some(&next) {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Manifest reader returned a non-advancing cursor",
            ));
        }
        request = PageRequest::new(Some(next), MAX_PAGE_SIZE)?;
    }
    if u64::try_from(chunks.len()).ok() != Some(metadata.chunk_count) {
        return Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            "Manifest chunk count differs from immutable catalog metadata",
        ));
    }
    let manifest = Manifest::new(metadata.total_size, metadata.chunking, chunks)?;
    if manifest.canonical_id()? != *manifest_id {
        return Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            "Manifest content does not match its canonical ID",
        ));
    }
    Ok(manifest)
}

struct ManifestObjectReader<'a> {
    objects: &'a dyn ObjectStore,
    chunks: std::vec::IntoIter<neoengram_core::ChunkRef>,
    current: Cursor<Vec<u8>>,
}

impl<'a> ManifestObjectReader<'a> {
    fn new(objects: &'a dyn ObjectStore, chunks: Vec<neoengram_core::ChunkRef>) -> Self {
        Self {
            objects,
            chunks: chunks.into_iter(),
            current: Cursor::new(Vec::new()),
        }
    }
}

impl Read for ManifestObjectReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let read = self.current.read(buffer)?;
            if read != 0 {
                return Ok(read);
            }
            let Some(chunk) = self.chunks.next() else {
                return Ok(0);
            };
            let capacity = usize::try_from(chunk.size).map_err(std::io::Error::other)?;
            let mut bytes = Vec::with_capacity(capacity);
            self.objects
                .copy_to(&ObjectSpec::new(chunk.object_id, chunk.size), &mut bytes)
                .map_err(std::io::Error::other)?;
            self.current = Cursor::new(bytes);
        }
    }
}

fn advance_journal(
    journals: &dyn JournalStore,
    journal: &mut JournalRecord,
    phase: JournalPhase,
    updated_at_unix_ms: u64,
) -> EngineResult<()> {
    let expected_revision = journal.revision;
    let mut next = journal.clone();
    next.revision = expected_revision.checked_add(1).ok_or_else(|| {
        EngineError::new(ErrorCode::Internal, "mutation journal revision exceeds u64")
    })?;
    next.phase = phase;
    next.updated_at_unix_ms = updated_at_unix_ms;
    journals.compare_exchange(expected_revision, &next)?;
    *journal = next;
    Ok(())
}

fn mark_recovery_required(
    journals: &dyn JournalStore,
    journal: &mut JournalRecord,
    clock: &dyn Clock,
) {
    let timestamp = clock.now_unix_ms().unwrap_or(journal.updated_at_unix_ms);
    let _ = advance_journal(journals, journal, JournalPhase::RecoveryRequired, timestamp);
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
    };

    use neoengram_core::{
        ChunkRef, Commit, CommitId, DirectoryEntry, DirectoryId, ManifestId, ObjectId,
    };

    use super::*;
    use crate::{
        DirectoryMetadata, ImmutableCatalogReader, LockManager, ManifestMetadata, MutationGuard,
        NoopFailureInjector, ObjectMetadata, ObjectPage, ObjectPutOutcome, Page, WorktreeEntry,
        WorktreeScanRequest,
    };

    #[derive(Debug)]
    struct RecordingJournalStore {
        record: Mutex<Option<JournalRecord>>,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RecordingJournalStore {
        fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                record: Mutex::new(None),
                events,
            }
        }
    }

    impl JournalStore for RecordingJournalStore {
        fn create(&self, journal: &JournalRecord) -> EngineResult<()> {
            journal.validate()?;
            let mut record = self.record.lock().unwrap();
            if record.is_some() {
                return Err(EngineError::new(
                    ErrorCode::AlreadyExists,
                    "journal already exists",
                ));
            }
            self.events.lock().unwrap().push("journal_prepared");
            *record = Some(journal.clone());
            Ok(())
        }

        fn load(&self, id: &JournalId) -> EngineResult<Option<JournalRecord>> {
            Ok(self
                .record
                .lock()
                .unwrap()
                .as_ref()
                .filter(|record| &record.id == id)
                .cloned())
        }

        fn compare_exchange(
            &self,
            expected_revision: u64,
            journal: &JournalRecord,
        ) -> EngineResult<()> {
            let mut record = self.record.lock().unwrap();
            let current = record.as_ref().ok_or_else(|| {
                EngineError::new(ErrorCode::ResourceNotFound, "journal is missing")
            })?;
            if current.revision != expected_revision {
                return Err(EngineError::new(ErrorCode::Conflict, "stale journal"));
            }
            journal.validate_successor(current)?;
            self.events.lock().unwrap().push(match journal.phase {
                JournalPhase::Applying => "journal_applying",
                JournalPhase::Applied => "journal_applied",
                JournalPhase::AwaitingFinalize => "journal_awaiting_finalize",
                JournalPhase::Finalized => "journal_finalized",
                JournalPhase::RecoveryRequired => "journal_recovery_required",
                _ => "journal_other",
            });
            *record = Some(journal.clone());
            Ok(())
        }

        fn list_active(&self) -> EngineResult<Vec<JournalRecord>> {
            Ok(self
                .record
                .lock()
                .unwrap()
                .iter()
                .filter(|record| record.phase.is_active())
                .cloned()
                .collect())
        }

        fn remove(&self, id: &JournalId) -> EngineResult<bool> {
            let mut record = self.record.lock().unwrap();
            if record.as_ref().is_some_and(|record| &record.id == id) {
                *record = None;
                return Ok(true);
            }
            Ok(false)
        }
    }

    #[derive(Debug)]
    struct RecordingWorktree {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Worktree for RecordingWorktree {
        fn inspect(&self, _path: &LogicalPath) -> EngineResult<Option<WorktreeEntry>> {
            Ok(None)
        }

        fn scan_files(
            &self,
            _request: &WorktreeScanRequest,
            _visitor: &mut dyn FnMut(WorktreeEntry) -> EngineResult<()>,
        ) -> EngineResult<()> {
            Ok(())
        }

        fn open_file(&self, _path: &LogicalPath) -> EngineResult<Box<dyn Read + Send>> {
            Err(EngineError::new(
                ErrorCode::ResourceNotFound,
                "test file is missing",
            ))
        }

        fn create_dir_all(&self, _path: &LogicalPath) -> EngineResult<()> {
            self.events.lock().unwrap().push("worktree");
            Ok(())
        }

        fn write_file_atomic(
            &self,
            _path: &LogicalPath,
            _expected_size: u64,
            _source: &mut dyn Read,
        ) -> EngineResult<()> {
            self.events.lock().unwrap().push("worktree");
            Ok(())
        }

        fn remove_file(&self, _path: &LogicalPath) -> EngineResult<bool> {
            self.events.lock().unwrap().push("worktree");
            Ok(true)
        }

        fn rename_no_replace(
            &self,
            _source: &LogicalPath,
            _destination: &LogicalPath,
        ) -> EngineResult<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct EmptyCatalog;

    impl ImmutableCatalogReader for EmptyCatalog {
        fn get_commit(&self, _id: &CommitId) -> EngineResult<Option<Commit>> {
            Ok(None)
        }

        fn get_manifest(&self, _id: &ManifestId) -> EngineResult<Option<ManifestMetadata>> {
            Ok(None)
        }

        fn scan_manifest_chunks(
            &self,
            _id: &ManifestId,
            _request: &PageRequest,
        ) -> EngineResult<Page<ChunkRef>> {
            Ok(Page {
                items: Vec::new(),
                next: None,
            })
        }

        fn get_directory(&self, _id: &DirectoryId) -> EngineResult<Option<DirectoryMetadata>> {
            Ok(None)
        }

        fn scan_directory_entries(
            &self,
            _id: &DirectoryId,
            _request: &PageRequest,
        ) -> EngineResult<Page<DirectoryEntry>> {
            Ok(Page {
                items: Vec::new(),
                next: None,
            })
        }
    }

    #[derive(Debug, Default)]
    struct EmptyObjectStore;

    impl ObjectStore for EmptyObjectStore {
        fn initialize(&self) -> EngineResult<()> {
            Ok(())
        }

        fn validate_layout(&self) -> EngineResult<()> {
            Ok(())
        }

        fn put_from(
            &self,
            _expected: &ObjectSpec,
            _source: &mut dyn Read,
        ) -> EngineResult<ObjectPutOutcome> {
            Ok(ObjectPutOutcome::AlreadyPresent)
        }

        fn copy_to(&self, _expected: &ObjectSpec, _target: &mut dyn Write) -> EngineResult<()> {
            Err(EngineError::new(
                ErrorCode::ObjectMissing,
                "test object is missing",
            ))
        }

        fn stat(&self, _id: &ObjectId) -> EngineResult<Option<ObjectMetadata>> {
            Ok(None)
        }

        fn list_page(&self, _request: &PageRequest) -> EngineResult<ObjectPage> {
            Ok(ObjectPage {
                objects: Vec::new(),
                next: None,
            })
        }

        fn remove(&self, _id: &ObjectId) -> EngineResult<bool> {
            Ok(false)
        }

        fn durability_barrier(&self) -> EngineResult<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct TestLockManager;

    #[derive(Debug)]
    struct TestGuard(MutationIdentity);

    impl MutationGuard for TestGuard {
        fn identity(&self) -> &MutationIdentity {
            &self.0
        }

        fn checkpoint(&self, _checkpoint: MutationCheckpoint) -> EngineResult<()> {
            Ok(())
        }
    }

    impl LockManager for TestLockManager {
        fn acquire(&self, identity: &MutationIdentity) -> EngineResult<Box<dyn MutationGuard>> {
            Ok(Box::new(TestGuard(identity.clone())))
        }
    }

    #[derive(Debug, Default)]
    struct IncrementingClock(AtomicU64);

    impl Clock for IncrementingClock {
        fn now_unix_ms(&self) -> EngineResult<u64> {
            Ok(self.0.fetch_add(1, Ordering::SeqCst))
        }
    }

    #[derive(Debug)]
    struct FailBeforeMutation(Mutex<bool>);

    impl FailureInjector for FailBeforeMutation {
        fn check(&self, point: FailurePoint) -> EngineResult<()> {
            let mut armed = self.0.lock().unwrap();
            if *armed && point == FailurePoint::BeforeMutation {
                *armed = false;
                return Err(EngineError::new(
                    ErrorCode::InjectedFailure,
                    "injected before Worktree mutation",
                ));
            }
            Ok(())
        }
    }

    fn request() -> ExecuteMutationRequest {
        ExecuteMutationRequest {
            plan: MutationPlan::new(
                "plan-a",
                MutationIdentity {
                    tenant_id: "tenant-a".to_owned(),
                    artifact_id: "artifact-a".to_owned(),
                    playground_id: "playground-a".to_owned(),
                    job_id: "job-a".to_owned(),
                    storage_volume_id: "volume-a".to_owned(),
                    owner_generation: 3,
                    fencing_token: 4,
                },
                IndexVersion::new(7, ContentDigest::from_bytes([7; 32])),
                vec![MutationAction::CreateDirectory {
                    path: LogicalPath::parse("dataset").unwrap(),
                }],
            )
            .unwrap(),
            recovery_payload: b"recover".to_vec(),
        }
    }

    #[test]
    fn durable_journal_precedes_worktree_and_authority_precedes_finalization() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let journals = RecordingJournalStore::new(Arc::clone(&events));
        let request = request();
        let plan_digest = request.plan.digest();
        let result = execute_mutation(
            &request,
            &RecordingWorktree {
                events: Arc::clone(&events),
            },
            &EmptyCatalog,
            &EmptyObjectStore,
            &journals,
            &TestLockManager,
            &IncrementingClock::default(),
            &crate::NoopProgressSink,
            &NoopFailureInjector,
        )
        .unwrap();
        assert_eq!(result.receipt.status, WorktreeReceiptStatus::Applied);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                "journal_prepared",
                "journal_applying",
                "worktree",
                "journal_applied",
                "journal_awaiting_finalize"
            ]
        );

        // Standalone/managed publishers run only after this receipt is returned.
        events.lock().unwrap().push("authority_published");
        let finalized = finalize_mutation(
            &FinalizeMutationRequest {
                journal_id: result.receipt.journal_id.clone(),
                plan_digest,
            },
            &journals,
            &IncrementingClock::default(),
        )
        .unwrap();
        assert_eq!(finalized.journal_id, result.receipt.journal_id);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                "journal_prepared",
                "journal_applying",
                "worktree",
                "journal_applied",
                "journal_awaiting_finalize",
                "authority_published",
                "journal_finalized"
            ]
        );
    }

    #[test]
    fn mutation_plan_digest_rejects_content_tampering() {
        let plan = request().plan;
        assert_eq!(plan.digest(), plan.canonical_digest().unwrap());
        assert_eq!(
            plan.digest().to_string(),
            "587a7989e0bd1f3a147b079016d17b029ce9bf91a050621ed423741300c34b6f"
        );

        let mut forged_digest = plan.clone();
        forged_digest.digest = ContentDigest::from_bytes([9; 32]);
        assert_eq!(
            forged_digest.validate().unwrap_err().code(),
            ErrorCode::IntegrityViolation
        );

        let mut tampered = plan;
        tampered.actions.push(MutationAction::RemoveFile {
            path: LogicalPath::parse("obsolete.bin").unwrap(),
        });
        assert_eq!(
            tampered.validate().unwrap_err().code(),
            ErrorCode::IntegrityViolation
        );
        assert_ne!(tampered.digest(), tampered.canonical_digest().unwrap());
    }

    #[test]
    fn failure_after_journal_requires_recovery_without_touching_worktree() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let journals = RecordingJournalStore::new(Arc::clone(&events));
        let error = execute_mutation(
            &request(),
            &RecordingWorktree {
                events: Arc::clone(&events),
            },
            &EmptyCatalog,
            &EmptyObjectStore,
            &journals,
            &TestLockManager,
            &IncrementingClock::default(),
            &crate::NoopProgressSink,
            &FailBeforeMutation(Mutex::new(true)),
        )
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::InjectedFailure);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                "journal_prepared",
                "journal_applying",
                "journal_recovery_required"
            ]
        );
    }
}
