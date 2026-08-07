use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use neoengram_core::{
    validate_index_snapshot, Commit, CommitId, DirectoryId, FileRecord, IndexVersion, LogicalPath,
};
use neoengram_protocol::{
    ArtifactId, JobId, ProjectId, ProtocolError, RequestId, ResourceVersion, StorageVolumeId,
    TenantId, UnixMillis, WireIndexVersion,
};
use serde::{Deserialize, Serialize};

use crate::{CentralError, CentralErrorCode, CentralResult, JobRecord};

/// Tenant-scoped identity of one durable Pre-commit session.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PreCommitId(RequestId);

impl PreCommitId {
    pub fn new(value: impl Into<String>) -> Result<Self, ProtocolError> {
        RequestId::new(value).map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for PreCommitId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PreCommitId {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PreCommitKey {
    pub tenant_id: TenantId,
    pub precommit_id: PreCommitId,
}

impl PreCommitKey {
    #[must_use]
    pub fn new(tenant_id: TenantId, precommit_id: PreCommitId) -> Self {
        Self {
            tenant_id,
            precommit_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreCommitState {
    Running,
    Ready,
    Abnormal,
    Cancelled,
    Committed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreCommitPhase {
    Queued,
    Scanning,
    Hashing,
    Uploading,
    Validating,
    Idle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PreCommitProgress {
    pub percent: u8,
    pub files_completed: u64,
    pub files_total: Option<u64>,
    pub bytes_completed: u64,
    pub bytes_total: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreCommitCheckStatus {
    Pending,
    Passed,
    Warning,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitCheck {
    pub check_id: RequestId,
    pub status: PreCommitCheckStatus,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitNotice {
    pub code: String,
    pub message: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CommitDiffSummary {
    pub files_added: u64,
    pub files_modified: u64,
    pub files_deleted: u64,
    pub files_renamed: u64,
    pub bytes_added: u64,
    pub bytes_removed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexSnapshotChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSnapshotChange {
    pub kind: IndexSnapshotChangeKind,
    pub path: LogicalPath,
    pub previous_path: Option<LogicalPath>,
    pub old_size: Option<u64>,
    pub new_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSnapshotDiff {
    pub summary: CommitDiffSummary,
    pub changes: Vec<IndexSnapshotChange>,
}

/// Compares two canonical Index snapshots. Rename detection is deliberately conservative: only a
/// unique removed/added pair with the same immutable Manifest identity is treated as a rename.
pub fn diff_index_snapshots(
    base: &[FileRecord],
    candidate: &[FileRecord],
) -> CentralResult<IndexSnapshotDiff> {
    validate_index_snapshot(base).map_err(|error| {
        protocol_invalid(format!("base Index snapshot is not canonical: {error}"))
    })?;
    validate_index_snapshot(candidate).map_err(|error| {
        protocol_invalid(format!(
            "candidate Index snapshot is not canonical: {error}"
        ))
    })?;

    let mut added = Vec::new();
    let mut deleted = Vec::new();
    let mut changes = Vec::new();
    let (mut base_index, mut candidate_index) = (0, 0);
    while base_index < base.len() || candidate_index < candidate.len() {
        match (base.get(base_index), candidate.get(candidate_index)) {
            (Some(old), Some(new)) => match old.path.cmp(&new.path) {
                std::cmp::Ordering::Less => {
                    deleted.push(old.clone());
                    base_index += 1;
                }
                std::cmp::Ordering::Greater => {
                    added.push(new.clone());
                    candidate_index += 1;
                }
                std::cmp::Ordering::Equal => {
                    if old != new {
                        changes.push(IndexSnapshotChange {
                            kind: IndexSnapshotChangeKind::Modified,
                            path: new.path.clone(),
                            previous_path: None,
                            old_size: Some(old.total_size),
                            new_size: Some(new.total_size),
                        });
                    }
                    base_index += 1;
                    candidate_index += 1;
                }
            },
            (Some(old), None) => {
                deleted.push(old.clone());
                base_index += 1;
            }
            (None, Some(new)) => {
                added.push(new.clone());
                candidate_index += 1;
            }
            (None, None) => break,
        }
    }

    type ContentIdentity = (neoengram_core::ManifestId, u64, u64);
    let mut added_by_content = BTreeMap::<ContentIdentity, Vec<usize>>::new();
    for (index, record) in added.iter().enumerate() {
        added_by_content
            .entry((record.manifest_id, record.total_size, record.chunk_count))
            .or_default()
            .push(index);
    }
    let mut deleted_by_content = BTreeMap::<ContentIdentity, Vec<usize>>::new();
    for (index, record) in deleted.iter().enumerate() {
        deleted_by_content
            .entry((record.manifest_id, record.total_size, record.chunk_count))
            .or_default()
            .push(index);
    }
    let mut renamed_additions = BTreeMap::new();
    let mut renamed_deletions = BTreeSet::new();
    for (identity, added_indexes) in &added_by_content {
        let Some(deleted_indexes) = deleted_by_content.get(identity) else {
            continue;
        };
        if let ([added_index], [deleted_index]) =
            (added_indexes.as_slice(), deleted_indexes.as_slice())
        {
            renamed_additions.insert(*added_index, *deleted_index);
            renamed_deletions.insert(*deleted_index);
        }
    }

    for (index, record) in added.into_iter().enumerate() {
        if let Some(deleted_index) = renamed_additions.get(&index) {
            let previous = &deleted[*deleted_index];
            changes.push(IndexSnapshotChange {
                kind: IndexSnapshotChangeKind::Renamed,
                path: record.path,
                previous_path: Some(previous.path.clone()),
                old_size: Some(previous.total_size),
                new_size: Some(record.total_size),
            });
        } else {
            changes.push(IndexSnapshotChange {
                kind: IndexSnapshotChangeKind::Added,
                path: record.path,
                previous_path: None,
                old_size: None,
                new_size: Some(record.total_size),
            });
        }
    }
    for (index, record) in deleted.into_iter().enumerate() {
        if !renamed_deletions.contains(&index) {
            changes.push(IndexSnapshotChange {
                kind: IndexSnapshotChangeKind::Deleted,
                path: record.path,
                previous_path: None,
                old_size: Some(record.total_size),
                new_size: None,
            });
        }
    }
    changes.sort_by(|left, right| left.path.cmp(&right.path));
    let summary = summarize_index_changes(&changes)?;
    Ok(IndexSnapshotDiff { summary, changes })
}

pub fn summarize_index_changes(
    changes: &[IndexSnapshotChange],
) -> CentralResult<CommitDiffSummary> {
    let mut summary = CommitDiffSummary::default();
    for change in changes {
        match change.kind {
            IndexSnapshotChangeKind::Added => {
                summary.files_added = checked_increment(summary.files_added)?;
            }
            IndexSnapshotChangeKind::Modified => {
                summary.files_modified = checked_increment(summary.files_modified)?;
            }
            IndexSnapshotChangeKind::Deleted => {
                summary.files_deleted = checked_increment(summary.files_deleted)?;
            }
            IndexSnapshotChangeKind::Renamed => {
                summary.files_renamed = checked_increment(summary.files_renamed)?;
            }
        }
        let old_size = change.old_size.unwrap_or(0);
        let new_size = change.new_size.unwrap_or(0);
        if new_size >= old_size {
            summary.bytes_added = checked_add(summary.bytes_added, new_size - old_size)?;
        } else {
            summary.bytes_removed = checked_add(summary.bytes_removed, old_size - new_size)?;
        }
    }
    Ok(summary)
}

/// Authoritative Pre-commit aggregate. `frozen_head_commit_id` and `job_id` are internal fields and
/// must not be projected by a public Web adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitRecord {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: neoengram_protocol::PlaygroundId,
    pub precommit_id: PreCommitId,
    pub precommit_request_id: RequestId,
    pub attempt: u32,
    pub state: PreCommitState,
    pub phase: PreCommitPhase,
    pub progress: PreCommitProgress,
    pub checks: Vec<PreCommitCheck>,
    pub warnings: Vec<PreCommitNotice>,
    pub blockers: Vec<PreCommitNotice>,
    pub source_index_version: WireIndexVersion,
    pub candidate_index_version: Option<WireIndexVersion>,
    /// Complete authority snapshot frozen together with `candidate_index_version`.
    pub candidate_records: Option<Vec<FileRecord>>,
    pub diff_summary: Option<CommitDiffSummary>,
    pub issue: Option<PreCommitNotice>,
    pub committed_commit_id: Option<CommitId>,
    /// Recovery acknowledgement written only after both control-catalog Head pointers observe the
    /// committed Commit.
    pub head_published_at_unix_ms: Option<UnixMillis>,
    pub frozen_head_commit_id: Option<CommitId>,
    pub job_id: JobId,
    pub resource_version: ResourceVersion,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

impl PreCommitRecord {
    #[must_use]
    pub fn key(&self) -> PreCommitKey {
        PreCommitKey::new(self.tenant_id.clone(), self.precommit_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitStartRequest {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: neoengram_protocol::PlaygroundId,
    /// Server-proposed identity. A replay is resolved by `precommit_request_id` and may return a
    /// different, previously persisted ID.
    pub precommit_id: PreCommitId,
    pub precommit_request_id: RequestId,
    pub source_index_version: WireIndexVersion,
    pub frozen_head_commit_id: Option<CommitId>,
    pub job_id: JobId,
    pub created_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreCommitMutationOutcome {
    pub precommit: PreCommitRecord,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitRestartRequest {
    pub key: PreCommitKey,
    pub restart_request_id: RequestId,
    pub source_index_version: WireIndexVersion,
    pub frozen_head_commit_id: Option<CommitId>,
    pub job_id: JobId,
    pub restarted_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitCancelRequest {
    pub key: PreCommitKey,
    pub cancel_request_id: RequestId,
    pub cancelled_at_unix_ms: UnixMillis,
}

/// Immutable Commit catalog row created by consuming a Pre-commit candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub source_playground_id: neoengram_protocol::PlaygroundId,
    /// Immutable placement provenance for the Chunk objects referenced by this Commit.
    ///
    /// `None` is accepted only when decoding Commit payloads written before placement provenance
    /// was introduced. New Commit publication always stores this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_storage_volume_id: Option<StorageVolumeId>,
    pub source_precommit_id: PreCommitId,
    pub commit_request_id: RequestId,
    pub commit_id: CommitId,
    /// Canonical root produced while sealing this exact frozen Index snapshot.
    pub root_directory_id: DirectoryId,
    pub parent_commit_id: Option<CommitId>,
    pub index_version: WireIndexVersion,
    /// Immutable, canonical-path-ordered snapshot. Commit reads must never follow a mutable
    /// Playground Index pointer to reconstruct historical content.
    pub records: Vec<FileRecord>,
    pub message: String,
    pub description: Option<String>,
    pub tag_names: Vec<String>,
    pub created_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitCommitRequest {
    pub key: PreCommitKey,
    pub expected_candidate_index_version: WireIndexVersion,
    pub commit: CommitRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreCommitCommitSnapshot {
    pub commit: CommitRecord,
    pub consumed_precommit: PreCommitRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreCommitCommitOutcome {
    pub commit: CommitRecord,
    pub consumed_precommit: PreCommitRecord,
    pub replayed: bool,
}

impl PreCommitCommitOutcome {
    pub(crate) fn from_snapshot(snapshot: PreCommitCommitSnapshot, replayed: bool) -> Self {
        Self {
            commit: snapshot.commit,
            consumed_precommit: snapshot.consumed_precommit,
            replayed,
        }
    }
}

pub(crate) fn build_started(request: &PreCommitStartRequest) -> CentralResult<PreCommitRecord> {
    request.source_index_version.validate()?;
    let record = PreCommitRecord {
        tenant_id: request.tenant_id.clone(),
        project_id: request.project_id.clone(),
        artifact_id: request.artifact_id.clone(),
        playground_id: request.playground_id.clone(),
        precommit_id: request.precommit_id.clone(),
        precommit_request_id: request.precommit_request_id.clone(),
        attempt: 1,
        state: PreCommitState::Running,
        phase: PreCommitPhase::Queued,
        progress: PreCommitProgress::default(),
        checks: Vec::new(),
        warnings: Vec::new(),
        blockers: Vec::new(),
        source_index_version: request.source_index_version.clone(),
        candidate_index_version: None,
        candidate_records: None,
        diff_summary: None,
        issue: None,
        committed_commit_id: None,
        head_published_at_unix_ms: None,
        frozen_head_commit_id: request.frozen_head_commit_id,
        job_id: request.job_id.clone(),
        resource_version: ResourceVersion::new(1),
        created_at_unix_ms: request.created_at_unix_ms,
        updated_at_unix_ms: request.created_at_unix_ms,
    };
    validate_record(&record)?;
    Ok(record)
}

pub(crate) fn apply_restart(
    mut record: PreCommitRecord,
    request: &PreCommitRestartRequest,
) -> CentralResult<PreCommitRecord> {
    if record.key() != request.key {
        return Err(conflict("restart scope differs from the stored Pre-commit"));
    }
    if !matches!(
        record.state,
        PreCommitState::Abnormal | PreCommitState::Cancelled
    ) {
        return Err(conflict(
            "only abnormal or cancelled Pre-commit sessions can be restarted",
        ));
    }
    request.source_index_version.validate()?;
    record.attempt = record
        .attempt
        .checked_add(1)
        .filter(|attempt| *attempt <= i32::MAX as u32)
        .ok_or_else(|| conflict("Pre-commit attempt is exhausted"))?;
    record.state = PreCommitState::Running;
    record.phase = PreCommitPhase::Queued;
    record.progress = PreCommitProgress::default();
    record.checks.clear();
    record.warnings.clear();
    record.blockers.clear();
    record.source_index_version = request.source_index_version.clone();
    record.candidate_index_version = None;
    record.candidate_records = None;
    record.diff_summary = None;
    record.issue = None;
    record.committed_commit_id = None;
    record.head_published_at_unix_ms = None;
    record.frozen_head_commit_id = request.frozen_head_commit_id;
    record.job_id = request.job_id.clone();
    record.resource_version = ResourceVersion::new(
        record
            .resource_version
            .get()
            .checked_add(1)
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "ResourceVersion is exhausted",
                )
            })?,
    );
    record.updated_at_unix_ms = request.restarted_at_unix_ms;
    validate_record(&record)?;
    Ok(record)
}

pub(crate) fn apply_cancel(
    mut record: PreCommitRecord,
    request: &PreCommitCancelRequest,
) -> CentralResult<PreCommitRecord> {
    if record.key() != request.key {
        return Err(conflict("cancel scope differs from the stored Pre-commit"));
    }
    if matches!(
        record.state,
        PreCommitState::Cancelled | PreCommitState::Committed
    ) {
        return Err(conflict(
            "the Pre-commit cannot be cancelled in its current state",
        ));
    }
    record.state = PreCommitState::Cancelled;
    record.phase = PreCommitPhase::Idle;
    record.candidate_index_version = None;
    record.candidate_records = None;
    record.diff_summary = None;
    record.committed_commit_id = None;
    record.head_published_at_unix_ms = None;
    record.resource_version = next_version(record.resource_version)?;
    record.updated_at_unix_ms = request.cancelled_at_unix_ms;
    validate_record(&record)?;
    Ok(record)
}

pub(crate) fn apply_job_sync(
    mut record: PreCommitRecord,
    job: &JobRecord,
    published_index: Option<&crate::PublishedIndex>,
    base_records: Option<&[FileRecord]>,
    observed_at_unix_ms: UnixMillis,
) -> CentralResult<Option<PreCommitRecord>> {
    if record.tenant_id != job.spec.tenant_id || record.job_id != job.spec.job_id {
        return Ok(None);
    }
    if record.project_id != job.spec.project_id
        || record.artifact_id != job.spec.artifact_id
        || record.playground_id != job.spec.playground_id
        || !same_index_version(
            &record.source_index_version,
            &job.spec.expected_index_version,
        )
    {
        return Err(conflict(
            "the associated Add Job scope differs from its Pre-commit attempt",
        ));
    }
    use neoengram_protocol::{JobState, PublishDecision};
    let published_index = match job.state {
        JobState::Succeeded => {
            let published_index = published_index.ok_or_else(|| {
                protocol_invalid("a succeeded Pre-commit Add Job requires its published Index")
            })?;
            validate_published_index(published_index)?;
            Some(published_index)
        }
        _ if published_index.is_some() => {
            return Err(protocol_invalid(
                "only a succeeded Pre-commit Add Job may carry a published Index",
            ));
        }
        _ => None,
    };
    if published_index.is_some() && base_records.is_none() {
        return Err(protocol_invalid(
            "the frozen Head Commit snapshot is missing for this Pre-commit",
        ));
    }
    if !matches!(record.state, PreCommitState::Running) {
        if let Some(published_index) = published_index {
            if record
                .candidate_index_version
                .as_ref()
                .is_none_or(|version| !same_index_version(version, &published_index.version))
                || record.candidate_records.as_ref() != Some(&published_index.records)
            {
                return Err(conflict(
                    "published Index differs from the frozen Pre-commit candidate",
                ));
            }
            if record.diff_summary.is_none() {
                if let Some(base_records) = base_records {
                    record.diff_summary =
                        Some(diff_index_snapshots(base_records, &published_index.records)?.summary);
                    record.resource_version = next_version(record.resource_version)?;
                    record.updated_at_unix_ms = observed_at_unix_ms;
                    validate_record(&record)?;
                }
            }
        }
        return Ok(Some(record));
    }

    let previous = record.clone();
    match job.state {
        JobState::Succeeded => {
            let published = job
                .decision
                .as_ref()
                .and_then(|decision| match &decision.decision {
                    PublishDecision::Publish {
                        published_index_version,
                        ..
                    } => Some(published_index_version.clone()),
                    _ => None,
                })
                .ok_or_else(|| {
                    conflict("a succeeded Pre-commit Add Job has no publish decision")
                })?;
            published.validate()?;
            let published_index = published_index.expect("succeeded state checked above");
            if !same_index_version(&published, &published_index.version) {
                return Err(conflict(
                    "Add Job publish decision differs from its authoritative Index snapshot",
                ));
            }
            record.state = PreCommitState::Ready;
            record.phase = PreCommitPhase::Idle;
            record.progress.percent = 100;
            record.blockers.clear();
            record.issue = None;
            record.candidate_index_version = Some(published);
            record.candidate_records = Some(published_index.records.clone());
            record.diff_summary = base_records
                .map(|base_records| diff_index_snapshots(base_records, &published_index.records))
                .transpose()?
                .map(|diff| diff.summary);
        }
        JobState::Conflicted
        | JobState::Rejected
        | JobState::Failed
        | JobState::Cancelled
        | JobState::TimedOut
        | JobState::RecoveryRequired => {
            let notice = job_failure_notice(job);
            record.state = PreCommitState::Abnormal;
            record.phase = PreCommitPhase::Idle;
            record.candidate_index_version = None;
            record.candidate_records = None;
            record.diff_summary = None;
            record.blockers = vec![notice.clone()];
            record.issue = Some(notice);
        }
        JobState::Prepared | JobState::Publishing => {
            record.phase = PreCommitPhase::Validating;
        }
        JobState::Accepted | JobState::Running | JobState::CancelRequested => {
            record.phase = job
                .progress
                .as_ref()
                .map_or(PreCommitPhase::Scanning, |progress| {
                    phase_from_job_progress(&progress.phase)
                });
            if let Some(progress) = &job.progress {
                record.progress.files_completed = progress.files_completed.get();
                record.progress.bytes_completed = progress.bytes_completed.get();
            }
        }
        JobState::Queued | JobState::Assigned => {
            record.phase = PreCommitPhase::Queued;
        }
        JobState::Unknown => {
            return Err(conflict(
                "an unknown Add Job state cannot advance Pre-commit",
            ));
        }
    }
    if record == previous {
        return Ok(Some(previous));
    }
    record.resource_version = next_version(record.resource_version)?;
    record.updated_at_unix_ms = observed_at_unix_ms;
    validate_record(&record)?;
    Ok(Some(record))
}

pub(crate) fn apply_commit(
    mut record: PreCommitRecord,
    request: &PreCommitCommitRequest,
) -> CentralResult<PreCommitCommitSnapshot> {
    if record.key() != request.key
        || record.tenant_id != request.commit.tenant_id
        || record.project_id != request.commit.project_id
        || record.artifact_id != request.commit.artifact_id
        || record.playground_id != request.commit.source_playground_id
        || record.precommit_id != request.commit.source_precommit_id
    {
        return Err(conflict("Commit scope differs from the stored Pre-commit"));
    }
    if record.state != PreCommitState::Ready
        || record.phase != PreCommitPhase::Idle
        || !record.blockers.is_empty()
    {
        return Err(conflict(
            "Commit requires a ready, idle Pre-commit without blockers",
        ));
    }
    let candidate = record
        .candidate_index_version
        .as_ref()
        .ok_or_else(|| conflict("a ready Pre-commit lost its candidate IndexVersion"))?;
    let candidate_records = record
        .candidate_records
        .as_ref()
        .ok_or_else(|| conflict("a ready Pre-commit lost its frozen Index snapshot"))?;
    if !same_index_version(candidate, &request.expected_candidate_index_version)
        || !same_index_version(candidate, &request.commit.index_version)
    {
        return Err(conflict("Commit candidate IndexVersion changed"));
    }
    if request.commit.parent_commit_id != record.frozen_head_commit_id {
        return Err(CentralError::new(
            CentralErrorCode::ArtifactHeadMismatch,
            "Commit parent differs from the Head frozen by this Pre-commit attempt",
        ));
    }
    if &request.commit.records != candidate_records {
        return Err(conflict(
            "Commit Index snapshot differs from the frozen Pre-commit candidate",
        ));
    }
    validate_commit(&request.commit)?;
    record.state = PreCommitState::Committed;
    record.phase = PreCommitPhase::Idle;
    record.committed_commit_id = Some(request.commit.commit_id);
    record.head_published_at_unix_ms = None;
    record.resource_version = next_version(record.resource_version)?;
    record.updated_at_unix_ms = request.commit.created_at_unix_ms;
    validate_record(&record)?;
    Ok(PreCommitCommitSnapshot {
        commit: request.commit.clone(),
        consumed_precommit: record,
    })
}

pub(crate) fn same_start_request(
    left: &PreCommitStartRequest,
    right: &PreCommitStartRequest,
) -> bool {
    left.tenant_id == right.tenant_id
        && left.project_id == right.project_id
        && left.artifact_id == right.artifact_id
        && left.playground_id == right.playground_id
        && left.precommit_request_id == right.precommit_request_id
        && same_index_version(&left.source_index_version, &right.source_index_version)
}

pub(crate) fn same_restart_request(
    left: &PreCommitRestartRequest,
    right: &PreCommitRestartRequest,
) -> bool {
    left.key == right.key
        && left.restart_request_id == right.restart_request_id
        && same_index_version(&left.source_index_version, &right.source_index_version)
}

pub(crate) fn same_cancel_request(
    left: &PreCommitCancelRequest,
    right: &PreCommitCancelRequest,
) -> bool {
    left.key == right.key && left.cancel_request_id == right.cancel_request_id
}

pub(crate) fn same_commit_request(
    left: &PreCommitCommitRequest,
    right: &PreCommitCommitRequest,
) -> bool {
    left.key == right.key
        && left.commit.commit_request_id == right.commit.commit_request_id
        && left.commit.tenant_id == right.commit.tenant_id
        && left.commit.project_id == right.commit.project_id
        && left.commit.artifact_id == right.commit.artifact_id
        && left.commit.source_playground_id == right.commit.source_playground_id
        && left.commit.source_storage_volume_id == right.commit.source_storage_volume_id
        && left.commit.source_precommit_id == right.commit.source_precommit_id
        && same_index_version(
            &left.expected_candidate_index_version,
            &right.expected_candidate_index_version,
        )
        && same_index_version(&left.commit.index_version, &right.commit.index_version)
        && left.commit.root_directory_id == right.commit.root_directory_id
        && left.commit.records == right.commit.records
        && left.commit.message == right.commit.message
        && left.commit.description == right.commit.description
        && left.commit.tag_names == right.commit.tag_names
}

pub(crate) fn validate_record(record: &PreCommitRecord) -> CentralResult<()> {
    record.source_index_version.validate()?;
    if let Some(candidate) = &record.candidate_index_version {
        candidate.validate()?;
    }
    match (&record.candidate_index_version, &record.candidate_records) {
        (Some(version), Some(records)) => {
            validate_published_index(&crate::PublishedIndex {
                version: version.clone(),
                records: records.clone(),
            })?;
        }
        (None, None) => {}
        _ => {
            return Err(protocol_invalid(
                "Pre-commit candidate version and frozen snapshot must be stored together",
            ));
        }
    }
    if record.diff_summary.is_some() && record.candidate_records.is_none() {
        return Err(protocol_invalid(
            "Pre-commit diff summary requires its frozen candidate snapshot",
        ));
    }
    if record.attempt == 0 || record.attempt > i32::MAX as u32 {
        return Err(protocol_invalid(
            "Pre-commit attempt is outside the supported range",
        ));
    }
    if record.progress.percent > 100 {
        return Err(protocol_invalid("Pre-commit progress percent exceeds 100"));
    }
    if record.checks.len() > 100 || record.warnings.len() > 100 || record.blockers.len() > 100 {
        return Err(protocol_invalid(
            "Pre-commit result collection exceeds 100 entries",
        ));
    }
    if record.state == PreCommitState::Running {
        if record.phase == PreCommitPhase::Idle || record.candidate_index_version.is_some() {
            return Err(protocol_invalid(
                "a running Pre-commit must have an execution phase and no candidate",
            ));
        }
    } else if record.phase != PreCommitPhase::Idle {
        return Err(protocol_invalid(
            "every non-running Pre-commit must use the idle phase",
        ));
    }
    if matches!(
        record.state,
        PreCommitState::Ready | PreCommitState::Committed
    ) && (record.candidate_index_version.is_none()
        || record.candidate_records.is_none()
        || (record.state == PreCommitState::Ready && !record.blockers.is_empty()))
    {
        return Err(protocol_invalid(
            "a ready or committed Pre-commit requires its frozen candidate snapshot",
        ));
    }
    if record.state == PreCommitState::Abnormal && record.blockers.is_empty() {
        return Err(protocol_invalid(
            "an abnormal Pre-commit requires at least one blocker",
        ));
    }
    if (record.state == PreCommitState::Committed) != record.committed_commit_id.is_some() {
        return Err(protocol_invalid(
            "committed Pre-commit state and Commit identity disagree",
        ));
    }
    if !matches!(
        record.state,
        PreCommitState::Ready | PreCommitState::Committed
    ) && record.candidate_records.is_some()
    {
        return Err(protocol_invalid(
            "only ready or committed Pre-commit sessions may retain a candidate snapshot",
        ));
    }
    if record.state != PreCommitState::Committed && record.head_published_at_unix_ms.is_some() {
        return Err(protocol_invalid(
            "only a committed Pre-commit may acknowledge Head publication",
        ));
    }
    Ok(())
}

pub(crate) fn apply_head_publication_ack(
    mut record: PreCommitRecord,
    commit_id: CommitId,
    published_at_unix_ms: UnixMillis,
) -> CentralResult<PreCommitRecord> {
    if record.state != PreCommitState::Committed || record.committed_commit_id != Some(commit_id) {
        return Err(conflict(
            "Head publication acknowledgement differs from the committed Pre-commit",
        ));
    }
    if record.head_published_at_unix_ms.is_some() {
        return Ok(record);
    }
    record.head_published_at_unix_ms = Some(published_at_unix_ms);
    record.updated_at_unix_ms = published_at_unix_ms;
    record.resource_version = next_version(record.resource_version)?;
    validate_record(&record)?;
    Ok(record)
}

pub(crate) fn validate_commit(commit: &CommitRecord) -> CentralResult<()> {
    commit.index_version.validate()?;
    validate_index_snapshot(&commit.records).map_err(|error| {
        protocol_invalid(format!("Commit Index snapshot is not canonical: {error}"))
    })?;
    let computed_index =
        IndexVersion::from_snapshot(commit.index_version.revision.get(), &commit.records).map_err(
            |error| protocol_invalid(format!("failed to digest Commit Index snapshot: {error}")),
        )?;
    if computed_index.digest != commit.index_version.digest {
        return Err(protocol_invalid(
            "Commit IndexVersion digest differs from its frozen snapshot",
        ));
    }
    if commit.message.trim().is_empty() || commit.message.chars().count() > 4096 {
        return Err(protocol_invalid(
            "Commit message must contain 1 to 4096 characters",
        ));
    }
    if commit
        .description
        .as_ref()
        .is_some_and(|description| description.chars().count() > 2048)
    {
        return Err(protocol_invalid("Commit description is too long"));
    }
    if commit.tag_names.len() > 20
        || commit.tag_names.iter().any(|tag| !valid_tag_name(tag))
        || commit.tag_names.iter().collect::<BTreeSet<_>>().len() != commit.tag_names.len()
    {
        return Err(protocol_invalid(
            "Commit tag names must be non-empty, unique, and contain at most 20 entries",
        ));
    }
    let canonical_commit = Commit::new(
        commit.root_directory_id,
        commit.parent_commit_id,
        commit.message.clone(),
        commit.created_at_unix_ms.get(),
    )
    .and_then(|commit| commit.canonical_id())
    .map_err(|error| protocol_invalid(format!("Commit is not canonical: {error}")))?;
    if canonical_commit != commit.commit_id {
        return Err(protocol_invalid(
            "CommitId differs from root, parent, message, and creation time",
        ));
    }
    Ok(())
}

fn valid_tag_name(tag: &str) -> bool {
    if tag.is_empty() || tag.len() > 128 || tag.starts_with("refs/") {
        return false;
    }
    let mut bytes = tag.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'/' | b'-'))
}

fn next_version(version: ResourceVersion) -> CentralResult<ResourceVersion> {
    version
        .get()
        .checked_add(1)
        .map(ResourceVersion::new)
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "ResourceVersion is exhausted",
            )
        })
}

fn phase_from_job_progress(phase: &str) -> PreCommitPhase {
    let phase = phase.to_ascii_lowercase();
    if phase.contains("hash") {
        PreCommitPhase::Hashing
    } else if phase.contains("upload") || phase.contains("transfer") {
        PreCommitPhase::Uploading
    } else if phase.contains("valid") || phase.contains("publish") || phase.contains("prepare") {
        PreCommitPhase::Validating
    } else {
        PreCommitPhase::Scanning
    }
}

fn job_failure_notice(job: &JobRecord) -> PreCommitNotice {
    if let Some(failure) = &job.failure {
        return PreCommitNotice {
            code: failure.error.code.as_str().to_owned(),
            message: failure.error.message.clone(),
            path: None,
        };
    }
    if let Some(neoengram_protocol::PublishDecision::Reject { error, .. }) =
        job.decision.as_ref().map(|decision| &decision.decision)
    {
        return PreCommitNotice {
            code: error.code.as_str().to_owned(),
            message: error.message.clone(),
            path: None,
        };
    }
    let code = match job.state {
        neoengram_protocol::JobState::Conflicted => "INDEX_CONFLICT",
        neoengram_protocol::JobState::Cancelled => "PRECOMMIT_JOB_CANCELLED",
        neoengram_protocol::JobState::TimedOut => "PRECOMMIT_JOB_TIMED_OUT",
        neoengram_protocol::JobState::RecoveryRequired => "PRECOMMIT_JOB_RECOVERY_REQUIRED",
        _ => "PRECOMMIT_JOB_FAILED",
    };
    PreCommitNotice {
        code: code.to_owned(),
        message: format!("associated Add Job reached {:?}", job.state),
        path: None,
    }
}

fn same_index_version(left: &WireIndexVersion, right: &WireIndexVersion) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn checked_increment(value: u64) -> CentralResult<u64> {
    checked_add(value, 1)
}

fn checked_add(left: u64, right: u64) -> CentralResult<u64> {
    left.checked_add(right).ok_or_else(|| {
        CentralError::new(
            CentralErrorCode::InvalidState,
            "Index diff summary exceeds the supported unsigned range",
        )
        .with_retryable(false)
    })
}

fn validate_published_index(index: &crate::PublishedIndex) -> CentralResult<()> {
    index.version.validate()?;
    validate_index_snapshot(&index.records).map_err(|error| {
        protocol_invalid(format!(
            "published Index snapshot is not canonical: {error}"
        ))
    })?;
    let computed = IndexVersion::from_snapshot(index.version.revision.get(), &index.records)
        .map_err(|error| protocol_invalid(format!("failed to digest published Index: {error}")))?;
    if computed.digest != index.version.digest {
        return Err(protocol_invalid(
            "published IndexVersion digest differs from its snapshot",
        ));
    }
    Ok(())
}

fn conflict(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::ConcurrentUpdate, message).with_retryable(false)
}

fn protocol_invalid(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::ProtocolInvalid, message).with_retryable(false)
}
