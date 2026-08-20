//! Bounded, fail-closed filesystem executor for resource lifecycle assignments.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use crate::{
    verify_volume_marker, AgentError, AgentErrorCode, AgentResult, SingleVolumeAgentConfig,
};
use neoengram_domain::core::{ContentDigest, LogicalPath};
use neoengram_domain::protocol::{
    AgentResourceLifecycleAssignment, AgentResourceLifecycleScope, AssignmentOperation, DecimalU64,
    JobAssignment, JobId, ResourceLifecycleAction, ResourceLifecycleEvidence,
    ResourceLifecycleReport, ResourceLifecycleReportState, UnixMillis,
};
use neoengram_runtime::fs::{sync_directory, VerifiedRoot};
use serde::{Deserialize, Serialize};

use crate::SnapshotDeliveryMountManager;

const FILESYSTEM_JOURNAL_FORMAT: u32 = 1;
const FILESYSTEM_JOURNAL_DIRECTORY: &str = "filesystem";
const QUARANTINE_DIRECTORY: &str = ".neoengram-quarantine";
const MAX_FILESYSTEM_JOURNAL_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) trait ResourceLifecycleExecutor: Send + Sync {
    fn prepare_fence(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<()>;

    fn validate_job_admission(&self, assignment: &JobAssignment) -> AgentResult<()>;

    fn execute(
        &self,
        command: &AgentResourceLifecycleAssignment,
        now_unix_ms: UnixMillis,
    ) -> AgentResult<ResourceLifecycleReport>;
}

#[derive(Debug)]
pub(crate) struct FilesystemResourceLifecycleExecutor {
    volume: SingleVolumeAgentConfig,
    snapshot_deliveries: Arc<SnapshotDeliveryMountManager>,
    journal: FilesystemLifecycleJournalStore,
}

impl FilesystemResourceLifecycleExecutor {
    pub(crate) fn new(
        volume: SingleVolumeAgentConfig,
        snapshot_deliveries: Arc<SnapshotDeliveryMountManager>,
    ) -> AgentResult<Self> {
        volume.validate()?;
        let journal = FilesystemLifecycleJournalStore::open(
            volume
                .state_root
                .join("lifecycle")
                .join(FILESYSTEM_JOURNAL_DIRECTORY),
        )?;
        Ok(Self {
            volume,
            snapshot_deliveries,
            journal,
        })
    }

    fn validate_command(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<()> {
        self.volume.validate_lifecycle_assignment(
            command,
            command.session_generation,
            UnixMillis::new(0),
        )?;
        verify_volume_marker(&self.volume.data_root, &self.volume.expected_volume_marker)
    }

    fn quarantine(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<ResourceLifecycleEvidence> {
        let root = VerifiedRoot::open(&self.volume.data_root).map_err(AgentError::from)?;
        let mut journal = self.journal.load_or_create(command)?;
        if journal.state == FilesystemLifecycleState::Quarantined {
            return aggregate_evidence(&journal, &self.volume.expected_volume_marker, false);
        }
        if matches!(
            journal.state,
            FilesystemLifecycleState::Restoring
                | FilesystemLifecycleState::Restored
                | FilesystemLifecycleState::Purging
                | FilesystemLifecycleState::Purged
        ) {
            return Err(lifecycle_state_error(
                "quarantine cannot run after restore or purge has started",
            ));
        }
        journal.state = FilesystemLifecycleState::Quarantining;
        self.journal.put(command, &journal)?;

        for index in 0..journal.roots.len() {
            let relative = LogicalPath::parse(journal.roots[index].relative_root.clone())
                .map_err(|error| lifecycle_error(error.to_string()))?;
            let quarantine = quarantine_path(command, &relative)?;
            let source = resolve_managed_optional(&root, &relative)?;
            let destination = resolve_optional(&root, &quarantine)?;
            match journal.roots[index].was_present {
                Some(true) => match (source, destination) {
                    (None, Some(destination)) => {
                        journal.roots[index].quarantine_evidence =
                            Some(scan_tree(&destination, &relative)?);
                    }
                    (Some(_), Some(_)) => {
                        return Err(lifecycle_state_error(
                            "managed resource exists in both active and quarantine locations",
                        ));
                    }
                    (None, None) => {
                        return Err(lifecycle_state_error(
                            "previously quarantined resource data is missing",
                        ));
                    }
                    (Some(source), None) => {
                        let destination = create_destination_parent(&root, &quarantine)?;
                        validate_tree_root(&source)?;
                        ensure_same_volume(
                            &source,
                            destination.parent().ok_or_else(|| {
                                lifecycle_error("quarantine destination has no parent")
                            })?,
                        )?;
                        self.verify_live_volume_marker()?;
                        fs::rename(&source, &destination).map_err(rename_error)?;
                        sync_rename_parents(&source, &destination)?;
                        journal.roots[index].quarantine_evidence =
                            Some(scan_tree(&destination, &relative)?);
                    }
                },
                Some(false) => {
                    if source.is_some() || destination.is_some() {
                        return Err(lifecycle_state_error(
                            "resource appeared after an empty quarantine inventory was fenced",
                        ));
                    }
                }
                None => match (source, destination) {
                    (None, None) => {
                        journal.roots[index].was_present = Some(false);
                    }
                    (None, Some(destination)) => {
                        journal.roots[index].was_present = Some(true);
                        journal.roots[index].quarantine_evidence =
                            Some(scan_tree(&destination, &relative)?);
                    }
                    (Some(_), Some(_)) => {
                        return Err(lifecycle_state_error(
                            "managed resource exists in both active and quarantine locations",
                        ));
                    }
                    (Some(source), None) => {
                        let destination = create_destination_parent(&root, &quarantine)?;
                        validate_tree_root(&source)?;
                        ensure_same_volume(
                            &source,
                            destination.parent().ok_or_else(|| {
                                lifecycle_error("quarantine destination has no parent")
                            })?,
                        )?;
                        self.verify_live_volume_marker()?;
                        fs::rename(&source, &destination).map_err(rename_error)?;
                        sync_rename_parents(&source, &destination)?;
                        journal.roots[index].was_present = Some(true);
                        journal.roots[index].quarantine_evidence =
                            Some(scan_tree(&destination, &relative)?);
                    }
                },
            }
            self.journal.put(command, &journal)?;
        }
        journal.state = FilesystemLifecycleState::Quarantined;
        self.journal.put(command, &journal)?;
        aggregate_evidence(&journal, &self.volume.expected_volume_marker, false)
    }

    fn restore(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<ResourceLifecycleEvidence> {
        let root = VerifiedRoot::open(&self.volume.data_root).map_err(AgentError::from)?;
        let mut journal = self.journal.require(command)?;
        if journal.state == FilesystemLifecycleState::Restored {
            return aggregate_evidence(&journal, &self.volume.expected_volume_marker, false);
        }
        if matches!(
            journal.state,
            FilesystemLifecycleState::Purging | FilesystemLifecycleState::Purged
        ) {
            return Err(lifecycle_state_error(
                "resource cannot be restored after purge has started",
            ));
        }
        if journal
            .roots
            .iter()
            .any(|entry| entry.was_present.is_none())
        {
            return Err(lifecycle_state_error(
                "resource quarantine inventory is incomplete",
            ));
        }
        journal.state = FilesystemLifecycleState::Restoring;
        self.journal.put(command, &journal)?;
        for index in 0..journal.roots.len() {
            let relative = LogicalPath::parse(journal.roots[index].relative_root.clone())
                .map_err(|error| lifecycle_error(error.to_string()))?;
            let quarantine = quarantine_path(command, &relative)?;
            let source = resolve_managed_optional(&root, &relative)?;
            let destination = resolve_optional(&root, &quarantine)?;
            if journal.roots[index].was_present == Some(false) {
                if source.is_some() || destination.is_some() {
                    return Err(lifecycle_state_error(
                        "empty quarantined resource was recreated before restore",
                    ));
                }
                continue;
            }
            match (source, destination) {
                (Some(source), None) => {
                    journal.roots[index].quarantine_evidence = Some(scan_tree(&source, &relative)?);
                }
                (None, Some(destination)) => {
                    journal.roots[index].quarantine_evidence =
                        Some(scan_tree(&destination, &relative)?);
                    let source = create_managed_destination_parent(&root, &relative)?;
                    ensure_same_volume(
                        &destination,
                        source
                            .parent()
                            .ok_or_else(|| lifecycle_error("restore destination has no parent"))?,
                    )?;
                    self.verify_live_volume_marker()?;
                    fs::rename(&destination, &source).map_err(rename_error)?;
                    sync_rename_parents(&destination, &source)?;
                }
                (Some(_), Some(_)) => {
                    return Err(lifecycle_state_error(
                        "resource exists in both active and quarantine locations during restore",
                    ));
                }
                (None, None) => {
                    return Err(lifecycle_state_error(
                        "quarantined resource disappeared before restore",
                    ));
                }
            }
            self.journal.put(command, &journal)?;
        }
        self.snapshot_deliveries.lifecycle_restore(command)?;
        journal.state = FilesystemLifecycleState::Restored;
        self.journal.put(command, &journal)?;
        aggregate_evidence(&journal, &self.volume.expected_volume_marker, false)
    }

    fn purge(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<ResourceLifecycleEvidence> {
        let root = VerifiedRoot::open(&self.volume.data_root).map_err(AgentError::from)?;
        let mut journal = self.journal.require(command)?;
        if journal.state == FilesystemLifecycleState::Purged {
            return aggregate_evidence(&journal, &self.volume.expected_volume_marker, true);
        }
        if journal.state == FilesystemLifecycleState::Restored {
            return Err(lifecycle_state_error(
                "restored resource cannot be purged by its previous deletion operation",
            ));
        }
        if journal
            .roots
            .iter()
            .any(|entry| entry.was_present.is_none())
        {
            return Err(lifecycle_state_error(
                "resource quarantine inventory is incomplete",
            ));
        }
        self.snapshot_deliveries.lifecycle_purge(command)?;
        journal.state = FilesystemLifecycleState::Purging;
        self.journal.put(command, &journal)?;
        for index in 0..journal.roots.len() {
            if journal.roots[index].was_present == Some(false) {
                continue;
            }
            let relative = LogicalPath::parse(journal.roots[index].relative_root.clone())
                .map_err(|error| lifecycle_error(error.to_string()))?;
            let quarantine = quarantine_path(command, &relative)?;
            match resolve_optional(&root, &quarantine)? {
                Some(path) => {
                    if journal.roots[index].purge_evidence.is_none() {
                        journal.roots[index].purge_evidence = Some(scan_tree(&path, &relative)?);
                        self.journal.put(command, &journal)?;
                    }
                    purge_tree(&path, &mut || self.verify_live_volume_marker())?;
                    sync_directory(path.parent().ok_or_else(|| {
                        lifecycle_error("purged resource has no parent directory")
                    })?)
                    .map_err(AgentError::from)?;
                }
                None if journal.roots[index].purge_evidence.is_some() => {}
                None => {
                    return Err(lifecycle_state_error(
                        "quarantined resource is missing without durable purge evidence",
                    ));
                }
            }
        }
        journal.state = FilesystemLifecycleState::Purged;
        self.journal.put(command, &journal)?;
        aggregate_evidence(&journal, &self.volume.expected_volume_marker, true)
    }

    fn verify_live_volume_marker(&self) -> AgentResult<()> {
        verify_volume_marker(&self.volume.data_root, &self.volume.expected_volume_marker)
    }
}

impl ResourceLifecycleExecutor for FilesystemResourceLifecycleExecutor {
    fn prepare_fence(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<()> {
        self.validate_command(command)?;
        if !matches!(command.assignment.action, ResourceLifecycleAction::Restore) {
            self.journal.load_or_create(command)?;
            self.snapshot_deliveries.lifecycle_fence(command)?;
        }
        Ok(())
    }

    fn validate_job_admission(&self, assignment: &JobAssignment) -> AgentResult<()> {
        self.snapshot_deliveries
            .validate_lifecycle_job_admission(assignment)?;
        Ok(())
    }

    fn execute(
        &self,
        command: &AgentResourceLifecycleAssignment,
        now_unix_ms: UnixMillis,
    ) -> AgentResult<ResourceLifecycleReport> {
        self.validate_command(command)?;
        let (state, evidence) = match command.assignment.action {
            ResourceLifecycleAction::CancelJobs => {
                (ResourceLifecycleReportState::JobsCancelled, None)
            }
            ResourceLifecycleAction::Quarantine => (
                ResourceLifecycleReportState::Quarantined,
                Some(self.quarantine(command)?),
            ),
            ResourceLifecycleAction::Restore => (
                ResourceLifecycleReportState::Restored,
                Some(self.restore(command)?),
            ),
            ResourceLifecycleAction::Purge => (
                ResourceLifecycleReportState::Purged,
                Some(self.purge(command)?),
            ),
        };
        let mut report = ResourceLifecycleReport::accepted(command, now_unix_ms);
        report.state = state;
        report.evidence = evidence;
        report
            .validate_for_assignment(command)
            .map_err(AgentError::from)?;
        Ok(report)
    }
}

#[derive(Debug, Default)]
pub(crate) struct LifecycleJobGate {
    state: Mutex<LifecycleJobGateState>,
}

#[derive(Debug, Default)]
struct LifecycleJobGateState {
    fences: Vec<AgentResourceLifecycleScope>,
    active: BTreeMap<JobId, ActiveJob>,
}

#[derive(Debug, Clone)]
struct ActiveJob {
    scope: JobScope,
    count: usize,
}

#[derive(Debug, Clone)]
struct JobScope {
    project_id: neoengram_domain::protocol::ProjectId,
    artifact_id: neoengram_domain::protocol::ArtifactId,
    playground_id: Option<neoengram_domain::protocol::PlaygroundId>,
    snapshot_id: Option<neoengram_domain::protocol::SnapshotId>,
    storage_volume_id: neoengram_domain::protocol::StorageVolumeId,
}

pub(crate) struct LifecycleJobGuard {
    gate: Arc<LifecycleJobGate>,
    job_id: JobId,
}

impl Drop for LifecycleJobGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            let remove = state.active.get_mut(&self.job_id).is_some_and(|active| {
                active.count = active.count.saturating_sub(1);
                active.count == 0
            });
            if remove {
                state.active.remove(&self.job_id);
            }
        }
    }
}

impl LifecycleJobGate {
    pub(crate) fn begin(
        self: &Arc<Self>,
        assignment: &JobAssignment,
    ) -> AgentResult<LifecycleJobGuard> {
        let (job_id, scope) = JobScope::from_assignment(assignment);
        let mut state = self
            .state
            .lock()
            .map_err(|_| lifecycle_error("lifecycle Job gate is poisoned"))?;
        if state.fences.iter().any(|fence| scope.matches(fence)) {
            return Err(lifecycle_state_error(
                "Agent Job is fenced by an active resource lifecycle operation",
            ));
        }
        match state.active.get_mut(&job_id) {
            Some(active) if active.scope.matches_same(&scope) => {
                active.count = active.count.saturating_add(1);
            }
            Some(_) => {
                return Err(AgentError::new(
                    AgentErrorCode::AssignmentMismatch,
                    "Agent Job ID is active for another resource scope",
                ));
            }
            None => {
                state
                    .active
                    .insert(job_id.clone(), ActiveJob { scope, count: 1 });
            }
        }
        Ok(LifecycleJobGuard {
            gate: Arc::clone(self),
            job_id,
        })
    }

    pub(crate) fn fence(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<usize> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| lifecycle_error("lifecycle Job gate is poisoned"))?;
        if !state.fences.contains(&command.resource_scope) {
            state.fences.push(command.resource_scope.clone());
        }
        Ok(state
            .active
            .values()
            .filter(|active| active.scope.matches(&command.resource_scope))
            .map(|active| active.count)
            .sum())
    }

    pub(crate) fn restore(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| lifecycle_error("lifecycle Job gate is poisoned"))?;
        state
            .fences
            .retain(|scope| scope != &command.resource_scope);
        Ok(())
    }
}

impl JobScope {
    fn from_assignment(assignment: &JobAssignment) -> (JobId, Self) {
        match &assignment.assignment {
            AssignmentOperation::Add { input, .. } => (
                input.job_id.clone(),
                Self {
                    project_id: input.project_id.clone(),
                    artifact_id: input.artifact_id.clone(),
                    playground_id: Some(input.playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: input.storage_volume_id.clone(),
                },
            ),
            AssignmentOperation::WorkspaceMaterialize { input, .. } => (
                input.job_id.clone(),
                Self {
                    project_id: input.project_id.clone(),
                    artifact_id: input.artifact_id.clone(),
                    playground_id: Some(input.playground_id.clone()),
                    snapshot_id: None,
                    storage_volume_id: input.storage_volume_id.clone(),
                },
            ),
            AssignmentOperation::SnapshotDelivery { input, .. } => (
                input.job_id.clone(),
                Self {
                    project_id: input.project_id.clone(),
                    artifact_id: input.artifact_id.clone(),
                    playground_id: None,
                    snapshot_id: Some(input.snapshot_id.clone()),
                    storage_volume_id: input.storage_volume_id.clone(),
                },
            ),
        }
    }

    fn matches_same(&self, other: &Self) -> bool {
        self.project_id == other.project_id
            && self.artifact_id == other.artifact_id
            && self.playground_id == other.playground_id
            && self.snapshot_id == other.snapshot_id
            && self.storage_volume_id == other.storage_volume_id
    }

    fn matches(&self, scope: &AgentResourceLifecycleScope) -> bool {
        match scope {
            AgentResourceLifecycleScope::StorageVolume { storage_volume_id } => {
                storage_volume_id == &self.storage_volume_id
            }
            AgentResourceLifecycleScope::Artifact {
                project_id,
                artifact_id,
                storage_volume_id,
                ..
            } => {
                project_id == &self.project_id
                    && artifact_id == &self.artifact_id
                    && storage_volume_id == &self.storage_volume_id
            }
            AgentResourceLifecycleScope::Playground {
                project_id,
                artifact_id,
                playground_id,
                storage_volume_id,
                ..
            } => {
                project_id == &self.project_id
                    && artifact_id == &self.artifact_id
                    && self.playground_id.as_ref() == Some(playground_id)
                    && storage_volume_id == &self.storage_volume_id
            }
            AgentResourceLifecycleScope::Snapshot {
                project_id,
                artifact_id,
                snapshot_id,
                storage_volume_id,
                ..
            } => {
                project_id == &self.project_id
                    && artifact_id == &self.artifact_id
                    && self.snapshot_id.as_ref() == Some(snapshot_id)
                    && storage_volume_id == &self.storage_volume_id
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FilesystemLifecycleState {
    Fenced,
    Quarantining,
    Quarantined,
    Restoring,
    Restored,
    Purging,
    Purged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TreeEvidence {
    file_count: u64,
    object_count: u64,
    byte_count: u64,
    object_set_digest: ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilesystemRootJournal {
    relative_root: String,
    was_present: Option<bool>,
    quarantine_evidence: Option<TreeEvidence>,
    purge_evidence: Option<TreeEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilesystemLifecycleJournal {
    format: u32,
    deletion_id: neoengram_domain::protocol::DeletionId,
    tenant_id: neoengram_domain::protocol::TenantId,
    resource_scope: AgentResourceLifecycleScope,
    request_digest: ContentDigest,
    state: FilesystemLifecycleState,
    roots: Vec<FilesystemRootJournal>,
}

#[derive(Debug)]
struct FilesystemLifecycleJournalStore {
    root: PathBuf,
}

impl FilesystemLifecycleJournalStore {
    fn open(root: PathBuf) -> AgentResult<Self> {
        fs::create_dir_all(&root).map_err(lifecycle_io)?;
        let metadata = fs::symlink_metadata(&root).map_err(lifecycle_io)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(lifecycle_error(
                "filesystem lifecycle journal root is not an ordinary directory",
            ));
        }
        Ok(Self { root })
    }

    fn path(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<PathBuf> {
        let digest = neoengram_domain::protocol::jcs_blake3(&command.resource_scope)
            .map_err(AgentError::from)?;
        Ok(self
            .root
            .join(format!("{}-{digest}.json", command.assignment.deletion_id)))
    }

    fn load_or_create(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<FilesystemLifecycleJournal> {
        match self.load(command)? {
            Some(journal) => Ok(journal),
            None => {
                let roots = command
                    .resource_scope
                    .canonical_relative_roots(&command.assignment.tenant_id)
                    .map_err(AgentError::from)?
                    .into_iter()
                    .map(|root| FilesystemRootJournal {
                        relative_root: root.as_str().to_owned(),
                        was_present: None,
                        quarantine_evidence: None,
                        purge_evidence: None,
                    })
                    .collect();
                let journal = FilesystemLifecycleJournal {
                    format: FILESYSTEM_JOURNAL_FORMAT,
                    deletion_id: command.assignment.deletion_id.clone(),
                    tenant_id: command.assignment.tenant_id.clone(),
                    resource_scope: command.resource_scope.clone(),
                    request_digest: command.assignment.request_digest,
                    state: FilesystemLifecycleState::Fenced,
                    roots,
                };
                self.put(command, &journal)?;
                Ok(journal)
            }
        }
    }

    fn require(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<FilesystemLifecycleJournal> {
        self.load(command)?.ok_or_else(|| {
            lifecycle_state_error("lifecycle action has no durable quarantine journal")
        })
    }

    fn load(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<Option<FilesystemLifecycleJournal>> {
        let path = self.path(command)?;
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(lifecycle_io(error)),
        };
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_FILESYSTEM_JOURNAL_BYTES
        {
            return Err(lifecycle_error(
                "filesystem lifecycle journal file is invalid",
            ));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(&path)
            .map_err(lifecycle_io)?
            .take(MAX_FILESYSTEM_JOURNAL_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(lifecycle_io)?;
        let journal: FilesystemLifecycleJournal = serde_json::from_slice(&bytes)
            .map_err(|error| lifecycle_error(format!("invalid lifecycle journal: {error}")))?;
        self.validate(command, &journal)?;
        Ok(Some(journal))
    }

    fn put(
        &self,
        command: &AgentResourceLifecycleAssignment,
        journal: &FilesystemLifecycleJournal,
    ) -> AgentResult<()> {
        self.validate(command, journal)?;
        let bytes = serde_json::to_vec(journal).map_err(|error| {
            lifecycle_error(format!("failed to encode lifecycle journal: {error}"))
        })?;
        if bytes.len() as u64 > MAX_FILESYSTEM_JOURNAL_BYTES {
            return Err(lifecycle_error(
                "filesystem lifecycle journal exceeds its size limit",
            ));
        }
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root).map_err(lifecycle_io)?;
        temporary.write_all(&bytes).map_err(lifecycle_io)?;
        temporary.as_file().sync_all().map_err(lifecycle_io)?;
        temporary
            .persist(self.path(command)?)
            .map_err(|error| lifecycle_io(error.error))?;
        sync_directory(&self.root).map_err(AgentError::from)
    }

    fn validate(
        &self,
        command: &AgentResourceLifecycleAssignment,
        journal: &FilesystemLifecycleJournal,
    ) -> AgentResult<()> {
        let expected_roots = command
            .resource_scope
            .canonical_relative_roots(&command.assignment.tenant_id)
            .map_err(AgentError::from)?
            .into_iter()
            .map(|root| root.as_str().to_owned())
            .collect::<Vec<_>>();
        let observed_roots = journal
            .roots
            .iter()
            .map(|root| root.relative_root.clone())
            .collect::<Vec<_>>();
        if journal.format != FILESYSTEM_JOURNAL_FORMAT
            || journal.deletion_id != command.assignment.deletion_id
            || journal.tenant_id != command.assignment.tenant_id
            || journal.resource_scope != command.resource_scope
            || journal.request_digest != command.assignment.request_digest
            || observed_roots != expected_roots
        {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "filesystem lifecycle journal does not match the signed deletion command",
            ));
        }
        Ok(())
    }
}

fn quarantine_path(
    command: &AgentResourceLifecycleAssignment,
    relative: &LogicalPath,
) -> AgentResult<LogicalPath> {
    LogicalPath::parse(format!(
        "{QUARANTINE_DIRECTORY}/{}/{}",
        command.assignment.deletion_id, relative
    ))
    .map_err(|error| lifecycle_error(error.to_string()))
}

fn resolve_optional(root: &VerifiedRoot, logical: &LogicalPath) -> AgentResult<Option<PathBuf>> {
    resolve_optional_components(root, logical.components())
}

fn resolve_managed_optional(
    root: &VerifiedRoot,
    logical: &LogicalPath,
) -> AgentResult<Option<PathBuf>> {
    let components = managed_components(logical);
    resolve_optional_components(root, components.iter().map(String::as_str))
}

fn resolve_optional_components<'a>(
    root: &VerifiedRoot,
    components: impl IntoIterator<Item = &'a str>,
) -> AgentResult<Option<PathBuf>> {
    root.verify_identity().map_err(AgentError::from)?;
    let mut current = root.as_path().to_path_buf();
    let components = components.into_iter().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(lifecycle_io(error)),
        };
        if metadata.file_type().is_symlink() {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "lifecycle path traverses a symlink",
            ));
        }
        if index + 1 < components.len() && !metadata.is_dir() {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "lifecycle path ancestor is not an ordinary directory",
            ));
        }
    }
    Ok(Some(current))
}

fn create_destination_parent(root: &VerifiedRoot, logical: &LogicalPath) -> AgentResult<PathBuf> {
    let parent = logical
        .parent()
        .ok_or_else(|| lifecycle_error("lifecycle destination has no parent"))?;
    root.create_dir_all(&parent).map_err(AgentError::from)?;
    root.resolve_for_create(logical).map_err(AgentError::from)
}

fn create_managed_destination_parent(
    root: &VerifiedRoot,
    logical: &LogicalPath,
) -> AgentResult<PathBuf> {
    create_physical_destination(root, &managed_components(logical))
}

fn create_physical_destination(root: &VerifiedRoot, components: &[String]) -> AgentResult<PathBuf> {
    root.verify_identity().map_err(AgentError::from)?;
    let mut current = root.as_path().to_path_buf();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    "managed lifecycle destination ancestor is not an ordinary directory",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current).map_err(lifecycle_io)?;
                sync_directory(current.parent().ok_or_else(|| {
                    lifecycle_error("managed lifecycle destination has no parent")
                })?)
                .map_err(AgentError::from)?;
            }
            Err(error) => return Err(lifecycle_io(error)),
        }
    }
    let final_component = components
        .last()
        .ok_or_else(|| lifecycle_error("managed lifecycle destination is empty"))?;
    current.push(final_component);
    match fs::symlink_metadata(&current) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "managed lifecycle destination is a symlink",
        )),
        Ok(_) => Ok(current),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(current),
        Err(error) => Err(lifecycle_io(error)),
    }
}

fn managed_components(logical: &LogicalPath) -> Vec<String> {
    let mut components = logical.components().map(str::to_owned).collect::<Vec<_>>();
    if components
        .first()
        .is_some_and(|component| component == "objects")
    {
        components.insert(0, ".neoengram".to_owned());
    }
    components
}

fn validate_tree_root(path: &Path) -> AgentResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(lifecycle_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "managed lifecycle root is not an ordinary directory",
        ));
    }
    // Scan once before rename so a symlink or special entry blocks isolation itself.
    scan_tree(
        path,
        &LogicalPath::parse("validation-root").expect("static logical path"),
    )?;
    Ok(())
}

fn ensure_same_volume(source: &Path, destination_parent: &Path) -> AgentResult<()> {
    #[cfg(unix)]
    {
        let source = fs::symlink_metadata(source).map_err(lifecycle_io)?;
        let destination = fs::symlink_metadata(destination_parent).map_err(lifecycle_io)?;
        if source.dev() != destination.dev() {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "lifecycle quarantine requires an atomic same-Volume rename",
            ));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (source, destination_parent);
    }
    Ok(())
}

fn sync_rename_parents(source: &Path, destination: &Path) -> AgentResult<()> {
    let source_parent = source
        .parent()
        .ok_or_else(|| lifecycle_error("lifecycle source has no parent"))?;
    let destination_parent = destination
        .parent()
        .ok_or_else(|| lifecycle_error("lifecycle destination has no parent"))?;
    sync_directory(source_parent).map_err(AgentError::from)?;
    if destination_parent != source_parent {
        sync_directory(destination_parent).map_err(AgentError::from)?;
    }
    Ok(())
}

fn scan_tree(path: &Path, logical_root: &LogicalPath) -> AgentResult<TreeEvidence> {
    let metadata = fs::symlink_metadata(path).map_err(lifecycle_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "lifecycle inventory root is not an ordinary directory",
        ));
    }
    let count_as_objects =
        logical_root.as_str() == "objects" || logical_root.as_str().starts_with("objects/");
    let mut files = 0_u64;
    let mut objects = 0_u64;
    let mut bytes = 0_u64;
    let mut inventory = Vec::<(String, bool, u64)>::new();
    let mut stack = vec![(path.to_path_buf(), String::new())];
    while let Some((directory, prefix)) = stack.pop() {
        let mut entries = fs::read_dir(&directory)
            .map_err(lifecycle_io)?
            .map(|entry| entry.map_err(lifecycle_io))
            .collect::<AgentResult<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let name = entry.file_name().into_string().map_err(|_| {
                AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    "managed resource contains a non-UTF-8 path",
                )
            })?;
            let relative = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            LogicalPath::parse(relative.clone()).map_err(|error| {
                AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    format!("managed resource contains a non-canonical path: {error}"),
                )
            })?;
            let child = entry.path();
            let metadata = fs::symlink_metadata(&child).map_err(lifecycle_io)?;
            if metadata.file_type().is_symlink() {
                return Err(AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    "managed resource contains a symlink",
                ));
            }
            if metadata.is_dir() {
                inventory.push((relative.clone(), true, 0));
                stack.push((child, relative));
            } else if metadata.is_file() {
                files = files
                    .checked_add(1)
                    .ok_or_else(|| lifecycle_error("lifecycle file count exceeds u64"))?;
                if count_as_objects {
                    objects = objects
                        .checked_add(1)
                        .ok_or_else(|| lifecycle_error("lifecycle object count exceeds u64"))?;
                }
                bytes = bytes
                    .checked_add(metadata.len())
                    .ok_or_else(|| lifecycle_error("lifecycle byte count exceeds u64"))?;
                inventory.push((relative, false, metadata.len()));
            } else {
                return Err(AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    "managed resource contains a special filesystem entry",
                ));
            }
        }
    }
    inventory.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"neoengram.lifecycle.inventory.v1\0");
    hasher.update(logical_root.as_str().as_bytes());
    hasher.update(b"\0");
    for (relative, directory, size) in inventory {
        hasher.update(if directory { b"d" } else { b"f" });
        hasher.update(&(relative.len() as u64).to_be_bytes());
        hasher.update(relative.as_bytes());
        hasher.update(&size.to_be_bytes());
    }
    Ok(TreeEvidence {
        file_count: files,
        object_count: objects,
        byte_count: bytes,
        object_set_digest: ContentDigest::from_bytes(*hasher.finalize().as_bytes()),
    })
}

fn purge_tree(
    path: &Path,
    before_destructive: &mut impl FnMut() -> AgentResult<()>,
) -> AgentResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(lifecycle_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "purge root is not an ordinary directory",
        ));
    }
    make_purge_directory_writable(path, &metadata)?;
    let mut entries = fs::read_dir(path)
        .map_err(lifecycle_io)?
        .map(|entry| entry.map_err(lifecycle_io))
        .collect::<AgentResult<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let child = entry.path();
        let metadata = fs::symlink_metadata(&child).map_err(lifecycle_io)?;
        if metadata.file_type().is_symlink() {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "purge refused a symlink in managed data",
            ));
        }
        if metadata.is_dir() {
            purge_tree(&child, before_destructive)?;
        } else if metadata.is_file() {
            before_destructive()?;
            fs::remove_file(&child).map_err(lifecycle_io)?;
        } else {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "purge refused a special filesystem entry",
            ));
        }
    }
    before_destructive()?;
    fs::remove_dir(path).map_err(lifecycle_io)
}

#[cfg(unix)]
fn make_purge_directory_writable(path: &Path, expected: &fs::Metadata) -> AgentResult<()> {
    use std::os::unix::fs::PermissionsExt;

    // Open without following links and compare the handle identity before changing permissions.
    // Permission changes are made through that verified handle, never by resolving the path again.
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let directory = options.open(path).map_err(lifecycle_io)?;
    let opened = directory.metadata().map_err(lifecycle_io)?;
    if !opened.is_dir() || opened.dev() != expected.dev() || opened.ino() != expected.ino() {
        return Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "purge directory changed before its permissions were opened",
        ));
    }
    directory
        .set_permissions(fs::Permissions::from_mode(0o700))
        .map_err(lifecycle_io)?;
    let current = fs::symlink_metadata(path).map_err(lifecycle_io)?;
    if !current.is_dir()
        || current.file_type().is_symlink()
        || current.dev() != opened.dev()
        || current.ino() != opened.ino()
    {
        return Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "purge directory changed while its permissions were being prepared",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn make_purge_directory_writable(path: &Path, _expected: &fs::Metadata) -> AgentResult<()> {
    let mut permissions = fs::metadata(path).map_err(lifecycle_io)?.permissions();
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).map_err(lifecycle_io)
}

fn aggregate_evidence(
    journal: &FilesystemLifecycleJournal,
    volume_marker_id: &neoengram_domain::protocol::VolumeMarkerId,
    purge: bool,
) -> AgentResult<ResourceLifecycleEvidence> {
    let mut files = 0_u64;
    let mut objects = 0_u64;
    let mut bytes = 0_u64;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"neoengram.lifecycle.deletion-proof.v1\0");
    for root in &journal.roots {
        let evidence = if purge {
            root.purge_evidence.as_ref()
        } else {
            root.quarantine_evidence.as_ref()
        };
        let Some(evidence) = evidence else {
            if root.was_present == Some(false) {
                hasher.update(root.relative_root.as_bytes());
                hasher.update(b"\0absent\0");
                continue;
            }
            return Err(lifecycle_state_error(
                "lifecycle proof inventory is incomplete",
            ));
        };
        files = files
            .checked_add(evidence.file_count)
            .ok_or_else(|| lifecycle_error("deletion proof file count exceeds u64"))?;
        objects = objects
            .checked_add(evidence.object_count)
            .ok_or_else(|| lifecycle_error("deletion proof object count exceeds u64"))?;
        bytes = bytes
            .checked_add(evidence.byte_count)
            .ok_or_else(|| lifecycle_error("deletion proof byte count exceeds u64"))?;
        hasher.update(root.relative_root.as_bytes());
        hasher.update(b"\0");
        hasher.update(evidence.object_set_digest.as_bytes());
        hasher.update(&evidence.file_count.to_be_bytes());
        hasher.update(&evidence.object_count.to_be_bytes());
        hasher.update(&evidence.byte_count.to_be_bytes());
    }
    Ok(ResourceLifecycleEvidence {
        volume_marker_id: volume_marker_id.clone(),
        file_count: DecimalU64::new(files),
        object_count: DecimalU64::new(objects),
        byte_count: DecimalU64::new(bytes),
        object_set_digest: ContentDigest::from_bytes(*hasher.finalize().as_bytes()),
        extensions: Default::default(),
    })
}

fn rename_error(error: io::Error) -> AgentError {
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::EXDEV) {
        return AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "lifecycle quarantine refused a cross-Volume rename",
        );
    }
    lifecycle_io(error)
}

fn lifecycle_io(error: io::Error) -> AgentError {
    AgentError::new(
        AgentErrorCode::MountUnavailable,
        format!("resource lifecycle filesystem operation failed: {error}"),
    )
}

fn lifecycle_error(message: impl Into<String>) -> AgentError {
    AgentError::new(AgentErrorCode::ExecutionFailed, message)
}

fn lifecycle_state_error(message: impl Into<String>) -> AgentError {
    AgentError::new(AgentErrorCode::InvalidState, message)
}

#[cfg(test)]
mod tests {
    use neoengram_domain::protocol::{
        AgentId, AgentInstallationId, AgentMountId, ArtifactId, ArtifactPlacementId, DeletionId,
        EdgeClusterId, Extensions, LifecycleAssignmentId, LifecycleGeneration, MountAccessMode,
        MountGeneration, OwnerGeneration, PlacementGeneration, PlaygroundId, ProjectId,
        ResourceLifecycleAssignment, ResourceRef, SessionGeneration, SnapshotId, StorageVolumeId,
        TenantId, VolumeMarkerId,
    };

    use super::*;

    #[derive(Debug)]
    struct NeverBridge;

    impl crate::ExecutionBridge for NeverBridge {
        fn authoritative_index(
            &self,
            _assignment: &neoengram_domain::protocol::AddAssignment,
        ) -> AgentResult<crate::AuthoritativeIndexSnapshot> {
            panic!("lifecycle test must not query an index")
        }

        fn workspace_materialization_snapshot(
            &self,
            _assignment: &neoengram_domain::protocol::WorkspaceMaterializeAssignment,
        ) -> AgentResult<crate::WorkspaceMaterializationSnapshot> {
            panic!("lifecycle test must not materialize a Workspace")
        }

        fn stage_metadata_descriptor(
            &self,
            _assignment: &neoengram_domain::protocol::AddAssignment,
            _descriptor: &neoengram_domain::protocol::MetadataBatchDescriptor,
        ) -> AgentResult<()> {
            panic!("lifecycle test must not stage metadata")
        }

        fn stage_metadata_page(
            &self,
            _assignment: &neoengram_domain::protocol::AddAssignment,
            _page: &neoengram_domain::protocol::MetadataBatchPage,
        ) -> AgentResult<()> {
            panic!("lifecycle test must not stage metadata")
        }

        fn now_unix_ms(&self) -> AgentResult<u64> {
            Ok(1_000)
        }
    }

    #[test]
    fn inventory_rejects_symlinks_and_is_stable() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("root");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("a"), b"abc").unwrap();
        fs::create_dir(root.join("nested")).unwrap();
        fs::write(root.join("nested/b"), b"defg").unwrap();
        let logical = LogicalPath::parse("objects/tenant/artifact").unwrap();
        let first = scan_tree(&root, &logical).unwrap();
        let second = scan_tree(&root, &logical).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.file_count, 2);
        assert_eq!(first.object_count, 2);
        assert_eq!(first.byte_count, 7);

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(temporary.path(), root.join("escape")).unwrap();
            assert_eq!(
                scan_tree(&root, &logical).unwrap_err().code(),
                AgentErrorCode::ScopeMismatch
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn purge_removes_read_only_delivery_directories() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().unwrap();
        let delivery = temporary.path().join("delivery");
        let nested = delivery.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("object.bin"), b"immutable").unwrap();
        fs::set_permissions(nested.join("object.bin"), fs::Permissions::from_mode(0o444)).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o555)).unwrap();
        fs::set_permissions(&delivery, fs::Permissions::from_mode(0o555)).unwrap();

        let mut destructive_checks = 0_u64;
        purge_tree(&delivery, &mut || {
            destructive_checks += 1;
            Ok(())
        })
        .unwrap();

        assert!(!delivery.exists());
        assert_eq!(destructive_checks, 3);
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_quarantine_and_purge_remove_copy_and_hardlink_deliveries() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let temporary = tempfile::tempdir().unwrap();
        let data_root = temporary.path().join("data");
        let state_root = temporary.path().join("state");
        fs::create_dir(&data_root).unwrap();
        fs::create_dir(&state_root).unwrap();
        fs::write(
            data_root.join(crate::VOLUME_MARKER_FILE_NAME),
            b"volume-a\n",
        )
        .unwrap();

        let snapshot = data_root.join("snapshots/project-a/artifact-a/snapshot-a");
        let copy = snapshot.join("deliveries/delivery-copy/nested");
        let hardlink = snapshot.join("deliveries/delivery-hardlink");
        fs::create_dir_all(&copy).unwrap();
        fs::create_dir_all(&hardlink).unwrap();
        fs::write(copy.join("copy.bin"), b"copy payload").unwrap();

        let retained_object = data_root
            .join(".neoengram/objects/tenants/tenant-a/artifacts/artifact-a/objects/object-a");
        fs::create_dir_all(retained_object.parent().unwrap()).unwrap();
        fs::write(&retained_object, b"hardlink payload").unwrap();
        fs::hard_link(&retained_object, hardlink.join("hardlink.bin")).unwrap();
        let retained_identity = fs::metadata(&retained_object).unwrap();

        for file in [copy.join("copy.bin"), hardlink.join("hardlink.bin")] {
            fs::set_permissions(file, fs::Permissions::from_mode(0o444)).unwrap();
        }
        for directory in [
            copy.clone(),
            snapshot.join("deliveries/delivery-copy"),
            hardlink,
        ] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o555)).unwrap();
        }

        let volume = test_volume(data_root.clone(), state_root.clone());
        let deliveries = Arc::new(
            SnapshotDeliveryMountManager::for_binding(
                data_root.clone(),
                &state_root,
                volume.agent_id.clone(),
                volume.tenant_id.clone(),
                volume.storage_volume_id.clone(),
                volume.agent_mount_id.clone(),
                volume.mount_generation,
                volume.owner_generation,
                SessionGeneration::new(2),
                Arc::new(NeverBridge),
            )
            .unwrap(),
        );
        let executor = FilesystemResourceLifecycleExecutor::new(volume, deliveries).unwrap();

        let quarantine = snapshot_assignment(
            "deletion-snapshot",
            "quarantine-snapshot",
            ResourceLifecycleAction::Quarantine,
        );
        executor.prepare_fence(&quarantine).unwrap();
        assert_eq!(
            executor
                .execute(&quarantine, UnixMillis::new(1_000))
                .unwrap()
                .state,
            ResourceLifecycleReportState::Quarantined
        );
        assert!(!snapshot.exists());

        let purge = snapshot_assignment(
            "deletion-snapshot",
            "purge-snapshot",
            ResourceLifecycleAction::Purge,
        );
        let report = executor.execute(&purge, UnixMillis::new(1_001)).unwrap();
        assert_eq!(report.state, ResourceLifecycleReportState::Purged);
        assert_eq!(report.evidence.unwrap().file_count.get(), 2);
        assert!(!snapshot.exists());

        let retained_after_purge = fs::metadata(&retained_object).unwrap();
        assert_eq!(
            (retained_after_purge.dev(), retained_after_purge.ino()),
            (retained_identity.dev(), retained_identity.ino())
        );
        assert_eq!(fs::read(retained_object).unwrap(), b"hardlink payload");
    }

    #[test]
    fn filesystem_journal_rejects_scope_reuse() {
        let temporary = tempfile::tempdir().unwrap();
        let store = FilesystemLifecycleJournalStore::open(temporary.path().to_path_buf()).unwrap();
        let first = lifecycle_assignment("deletion-a", "artifact-a");
        store.load_or_create(&first).unwrap();
        let mut changed = first.clone();
        changed.assignment.request_digest = ContentDigest::from_bytes([0x55; 32]);
        assert_eq!(
            store.load_or_create(&changed).unwrap_err().code(),
            AgentErrorCode::AssignmentMismatch
        );
    }

    #[test]
    fn playground_quarantine_restore_and_purge_are_durable_and_idempotent() {
        let temporary = tempfile::tempdir().unwrap();
        let data_root = temporary.path().join("data");
        let state_root = temporary.path().join("state");
        fs::create_dir(&data_root).unwrap();
        fs::create_dir(&state_root).unwrap();
        fs::write(
            data_root.join(crate::VOLUME_MARKER_FILE_NAME),
            b"volume-a\n",
        )
        .unwrap();
        let playground = data_root.join("playgrounds/project-a/artifact-a/playground-a");
        fs::create_dir_all(playground.join("nested")).unwrap();
        fs::write(playground.join("one.txt"), b"one").unwrap();
        fs::write(playground.join("nested/two.txt"), b"two-two").unwrap();

        let volume = test_volume(data_root.clone(), state_root.clone());
        let snapshots = Arc::new(
            SnapshotDeliveryMountManager::for_binding(
                data_root.clone(),
                &state_root,
                volume.agent_id.clone(),
                volume.tenant_id.clone(),
                volume.storage_volume_id.clone(),
                volume.agent_mount_id.clone(),
                volume.mount_generation,
                volume.owner_generation,
                SessionGeneration::new(2),
                Arc::new(NeverBridge),
            )
            .unwrap(),
        );
        let executor = FilesystemResourceLifecycleExecutor::new(volume, snapshots).unwrap();

        let quarantine = playground_assignment(
            "deletion-a",
            "quarantine-a",
            ResourceLifecycleAction::Quarantine,
        );
        executor.prepare_fence(&quarantine).unwrap();
        let report = executor
            .execute(&quarantine, UnixMillis::new(1_000))
            .unwrap();
        assert_eq!(report.state, ResourceLifecycleReportState::Quarantined);
        let evidence = report.evidence.unwrap();
        assert_eq!(evidence.file_count.get(), 2);
        assert_eq!(evidence.byte_count.get(), 10);
        assert!(!playground.exists());
        assert_eq!(
            executor
                .execute(&quarantine, UnixMillis::new(1_001))
                .unwrap()
                .state,
            ResourceLifecycleReportState::Quarantined
        );

        let restore =
            playground_assignment("deletion-a", "restore-a", ResourceLifecycleAction::Restore);
        assert_eq!(
            executor
                .execute(&restore, UnixMillis::new(1_002))
                .unwrap()
                .state,
            ResourceLifecycleReportState::Restored
        );
        assert_eq!(
            fs::read(playground.join("nested/two.txt")).unwrap(),
            b"two-two"
        );

        let quarantine = playground_assignment(
            "deletion-b",
            "quarantine-b",
            ResourceLifecycleAction::Quarantine,
        );
        executor.prepare_fence(&quarantine).unwrap();
        executor
            .execute(&quarantine, UnixMillis::new(1_003))
            .unwrap();
        let purge = playground_assignment("deletion-b", "purge-b", ResourceLifecycleAction::Purge);
        let report = executor.execute(&purge, UnixMillis::new(1_004)).unwrap();
        assert_eq!(report.state, ResourceLifecycleReportState::Purged);
        assert_eq!(report.evidence.unwrap().file_count.get(), 2);
        assert!(!playground.exists());
        assert_eq!(
            executor
                .execute(&purge, UnixMillis::new(1_005))
                .unwrap()
                .state,
            ResourceLifecycleReportState::Purged
        );
    }

    #[test]
    fn artifact_quarantine_maps_the_canonical_object_scope_to_the_hidden_cas() {
        let temporary = tempfile::tempdir().unwrap();
        let data_root = temporary.path().join("data");
        let state_root = temporary.path().join("state");
        fs::create_dir(&data_root).unwrap();
        fs::create_dir(&state_root).unwrap();
        fs::write(
            data_root.join(crate::VOLUME_MARKER_FILE_NAME),
            b"volume-a\n",
        )
        .unwrap();
        let cas =
            data_root.join(".neoengram/objects/tenants/tenant-a/artifacts/artifact-a/objects");
        fs::create_dir_all(&cas).unwrap();
        fs::write(cas.join("object-a"), b"payload").unwrap();

        let volume = test_volume(data_root.clone(), state_root.clone());
        let snapshots = Arc::new(
            SnapshotDeliveryMountManager::for_binding(
                data_root.clone(),
                &state_root,
                volume.agent_id.clone(),
                volume.tenant_id.clone(),
                volume.storage_volume_id.clone(),
                volume.agent_mount_id.clone(),
                volume.mount_generation,
                volume.owner_generation,
                SessionGeneration::new(2),
                Arc::new(NeverBridge),
            )
            .unwrap(),
        );
        let executor = FilesystemResourceLifecycleExecutor::new(volume, snapshots).unwrap();
        let command = lifecycle_assignment("deletion-cas", "artifact-a");
        executor.prepare_fence(&command).unwrap();
        let report = executor.execute(&command, UnixMillis::new(1_000)).unwrap();
        assert_eq!(report.state, ResourceLifecycleReportState::Quarantined);
        assert_eq!(report.evidence.unwrap().object_count.get(), 1);
        assert!(!cas.exists());
        assert!(data_root
            .join(
                ".neoengram-quarantine/deletion-cas/objects/tenants/tenant-a/artifacts/artifact-a/objects/object-a"
            )
            .is_file());
    }

    fn test_volume(data_root: PathBuf, state_root: PathBuf) -> SingleVolumeAgentConfig {
        SingleVolumeAgentConfig {
            agent_id: AgentId::new("agent-a").unwrap(),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(2),
            owner_generation: OwnerGeneration::new(2),
            expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadWrite,
            data_root,
            state_root,
        }
    }

    fn playground_assignment(
        deletion_id: &str,
        assignment_id: &str,
        action: ResourceLifecycleAction,
    ) -> AgentResourceLifecycleAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let playground_id = PlaygroundId::new("playground-a").unwrap();
        AgentResourceLifecycleAssignment {
            assignment: ResourceLifecycleAssignment {
                assignment_id: LifecycleAssignmentId::new(assignment_id).unwrap(),
                tenant_id: TenantId::new("tenant-a").unwrap(),
                deletion_id: DeletionId::new(deletion_id).unwrap(),
                resource: ResourceRef::Playground {
                    project_id: project_id.clone(),
                    artifact_id: artifact_id.clone(),
                    playground_id: playground_id.clone(),
                },
                action,
                lifecycle_generation: LifecycleGeneration::new(2),
                request_digest: ContentDigest::from_bytes([0x66; 32]),
                deadline_unix_ms: UnixMillis::new(10_000),
            },
            resource_scope: AgentResourceLifecycleScope::Playground {
                project_id,
                artifact_id,
                playground_id,
                storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
                artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
                placement_generation: PlacementGeneration::new(2),
            },
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            volume_marker_id: VolumeMarkerId::new("volume-a").unwrap(),
            session_generation: SessionGeneration::new(2),
            mount_generation: MountGeneration::new(2),
            owner_generation: OwnerGeneration::new(2),
            extensions: Extensions::new(),
        }
    }

    fn snapshot_assignment(
        deletion_id: &str,
        assignment_id: &str,
        action: ResourceLifecycleAction,
    ) -> AgentResourceLifecycleAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-a").unwrap();
        AgentResourceLifecycleAssignment {
            assignment: ResourceLifecycleAssignment {
                assignment_id: LifecycleAssignmentId::new(assignment_id).unwrap(),
                tenant_id: TenantId::new("tenant-a").unwrap(),
                deletion_id: DeletionId::new(deletion_id).unwrap(),
                resource: ResourceRef::Snapshot {
                    snapshot_id: snapshot_id.clone(),
                },
                action,
                lifecycle_generation: LifecycleGeneration::new(2),
                request_digest: ContentDigest::from_bytes([0x77; 32]),
                deadline_unix_ms: UnixMillis::new(10_000),
            },
            resource_scope: AgentResourceLifecycleScope::Snapshot {
                project_id,
                artifact_id,
                snapshot_id,
                storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
                artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
                placement_generation: PlacementGeneration::new(2),
            },
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            volume_marker_id: VolumeMarkerId::new("volume-a").unwrap(),
            session_generation: SessionGeneration::new(2),
            mount_generation: MountGeneration::new(2),
            owner_generation: OwnerGeneration::new(2),
            extensions: Extensions::new(),
        }
    }

    fn lifecycle_assignment(
        deletion_id: &str,
        artifact_id: &str,
    ) -> AgentResourceLifecycleAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new(artifact_id).unwrap();
        AgentResourceLifecycleAssignment {
            assignment: ResourceLifecycleAssignment {
                assignment_id: LifecycleAssignmentId::new("lifecycle-a").unwrap(),
                tenant_id: TenantId::new("tenant-a").unwrap(),
                deletion_id: DeletionId::new(deletion_id).unwrap(),
                resource: ResourceRef::Artifact {
                    project_id: project_id.clone(),
                    artifact_id: artifact_id.clone(),
                },
                action: ResourceLifecycleAction::Quarantine,
                lifecycle_generation: LifecycleGeneration::new(2),
                request_digest: ContentDigest::from_bytes([0x44; 32]),
                deadline_unix_ms: UnixMillis::new(10_000),
            },
            resource_scope: AgentResourceLifecycleScope::Artifact {
                project_id,
                artifact_id,
                storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
                artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
                placement_generation: PlacementGeneration::new(2),
            },
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            volume_marker_id: VolumeMarkerId::new("volume-a").unwrap(),
            session_generation: SessionGeneration::new(2),
            mount_generation: MountGeneration::new(2),
            owner_generation: OwnerGeneration::new(2),
            extensions: Extensions::new(),
        }
    }
}
