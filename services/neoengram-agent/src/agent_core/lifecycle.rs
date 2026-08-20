//! Durable Agent-side fencing journal for storage-resource lifecycle commands.
//!
//! The journal is deliberately separate from the managed-Add ledger. Lifecycle commands are
//! destructive and have a different idempotency key (`LifecycleAssignmentId`) and generation
//! fence. A physical executor can claim a command, perform its bounded filesystem operation, and
//! then persist the signed-compatible report through this API. Replaying a command with another
//! digest or generation is rejected rather than interpreted as a retry.

use std::path::PathBuf;

use neoengram_domain::protocol::{
    AgentResourceLifecycleAssignment, ContentDigest, LifecycleAssignmentId, LifecycleGeneration,
    ResourceLifecycleAction, ResourceLifecycleReport, ResourceLifecycleReportState, UnixMillis,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

use super::{
    sqlite_storage::{
        storage_corruption, storage_error, LockedSqlite, SqliteDefinition,
        AGENT_STATE_APPLICATION_ID, AGENT_STATE_DATABASE_FILE, AGENT_STATE_LOCK_FILE,
        AGENT_STATE_SCHEMA_VERSION,
    },
    AgentError, AgentErrorCode, AgentResult,
};

const DATABASE_FILE: &str = AGENT_STATE_DATABASE_FILE;
const LOCK_FILE: &str = AGENT_STATE_LOCK_FILE;
const APPLICATION_ID: i64 = AGENT_STATE_APPLICATION_ID;
const SCHEMA_VERSION: i64 = AGENT_STATE_SCHEMA_VERSION;
const MAGIC: &str = "neoengram-agent-lifecycle-v1";
const RECORD_FORMAT: u32 = 1;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS lifecycle_metadata (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    magic TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS lifecycle_records (
    assignment_id TEXT NOT NULL PRIMARY KEY,
    deletion_id TEXT NOT NULL,
    lifecycle_generation TEXT NOT NULL CHECK (
        lifecycle_generation <> '' AND lifecycle_generation NOT GLOB '*[^0-9]*'
    ),
    request_digest TEXT NOT NULL CHECK (
        length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'
    ),
    delivery_digest TEXT NOT NULL CHECK (
        length(delivery_digest) = 64 AND delivery_digest NOT GLOB '*[^0-9a-f]*'
    ),
    state TEXT NOT NULL,
    claimed_at_unix_ms TEXT NOT NULL CHECK (
        claimed_at_unix_ms <> '' AND claimed_at_unix_ms NOT GLOB '*[^0-9]*'
    ),
    completed_at_unix_ms TEXT,
    payload BLOB NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS lifecycle_records_deletion ON lifecycle_records (deletion_id);
"#;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleJournalState {
    Claimed,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LifecycleJournalRecord {
    pub command: AgentResourceLifecycleAssignment,
    pub assignment_id: LifecycleAssignmentId,
    pub deletion_id: neoengram_domain::protocol::DeletionId,
    pub lifecycle_generation: LifecycleGeneration,
    pub request_digest: ContentDigest,
    pub delivery_digest: ContentDigest,
    pub action: ResourceLifecycleAction,
    pub state: LifecycleJournalState,
    pub claimed_at_unix_ms: UnixMillis,
    pub completed_at_unix_ms: Option<UnixMillis>,
    pub report: Option<ResourceLifecycleReport>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleClaimOutcome {
    Claimed(LifecycleJournalRecord),
    Existing(LifecycleJournalRecord),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleCompleteOutcome {
    Completed(LifecycleJournalRecord),
    Existing(LifecycleJournalRecord),
}

pub trait LifecycleJournal: Send + Sync {
    fn claim(
        &self,
        assignment: &AgentResourceLifecycleAssignment,
        now_unix_ms: UnixMillis,
    ) -> AgentResult<LifecycleClaimOutcome>;

    fn complete(&self, report: ResourceLifecycleReport) -> AgentResult<LifecycleCompleteOutcome>;

    fn get(
        &self,
        assignment_id: &LifecycleAssignmentId,
    ) -> AgentResult<Option<LifecycleJournalRecord>>;

    /// Returns commands durably claimed without a terminal report. Agents use this set during
    /// startup to reconstruct no-remount fences before replaying ordinary Jobs.
    fn list_unfinished(&self) -> AgentResult<Vec<LifecycleJournalRecord>>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct SqliteLifecycleJournalConfig {
    pub root: PathBuf,
    pub agent_id: neoengram_domain::protocol::AgentId,
    pub tenant_id: neoengram_domain::protocol::TenantId,
}

impl SqliteLifecycleJournalConfig {
    #[must_use]
    pub fn new(
        root: impl Into<PathBuf>,
        agent_id: neoengram_domain::protocol::AgentId,
        tenant_id: neoengram_domain::protocol::TenantId,
    ) -> Self {
        Self {
            root: root.into(),
            agent_id,
            tenant_id,
        }
    }
}

impl std::fmt::Debug for SqliteLifecycleJournalConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteLifecycleJournalConfig")
            .field("root_configured", &!self.root.as_os_str().is_empty())
            .field("agent_id", &self.agent_id)
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

#[derive(Debug)]
pub struct SqliteLifecycleJournal {
    storage: LockedSqlite,
    agent_id: neoengram_domain::protocol::AgentId,
    tenant_id: neoengram_domain::protocol::TenantId,
}

impl SqliteLifecycleJournal {
    pub fn open(config: SqliteLifecycleJournalConfig) -> AgentResult<Self> {
        let storage = LockedSqlite::open(
            &config.root,
            SqliteDefinition {
                database_file: DATABASE_FILE,
                lock_file: LOCK_FILE,
                application_id: APPLICATION_ID,
                schema_version: SCHEMA_VERSION,
                schema: SCHEMA,
                tables: &["lifecycle_metadata", "lifecycle_records"],
            },
        )?;
        let journal = Self {
            storage,
            agent_id: config.agent_id,
            tenant_id: config.tenant_id,
        };
        journal.bind_or_validate_identity()?;
        Ok(journal)
    }

    pub fn integrity_check(&self) -> AgentResult<()> {
        self.storage.integrity_check()?;
        self.validate_metadata()
    }

    fn bind_or_validate_identity(&self) -> AgentResult<()> {
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let identity = transaction
            .query_row(
                "SELECT magic, agent_id, tenant_id FROM lifecycle_metadata WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(storage_error)?;
        match identity {
            None => {
                transaction
                    .execute(
                        "INSERT INTO lifecycle_metadata (singleton, magic, agent_id, tenant_id) VALUES (1, ?1, ?2, ?3)",
                        params![MAGIC, self.agent_id.as_str(), self.tenant_id.as_str()],
                    )
                    .map_err(storage_error)?;
            }
            Some((magic, agent_id, tenant_id))
                if magic == MAGIC
                    && agent_id == self.agent_id.as_str()
                    && tenant_id == self.tenant_id.as_str() => {}
            Some(_) => return Err(identity_mismatch()),
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()
    }

    fn validate_metadata(&self) -> AgentResult<()> {
        let connection = self.storage.connection()?;
        let identity: (String, String, String) = connection
            .query_row(
                "SELECT magic, agent_id, tenant_id FROM lifecycle_metadata WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(storage_error)?;
        if identity.0 != MAGIC
            || identity.1 != self.agent_id.as_str()
            || identity.2 != self.tenant_id.as_str()
        {
            return Err(identity_mismatch());
        }
        Ok(())
    }

    fn validate_assignment(
        &self,
        assignment: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<()> {
        assignment.validate().map_err(AgentError::from)?;
        if assignment.assignment.tenant_id != self.tenant_id {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "lifecycle assignment belongs to another Tenant",
            ));
        }
        if assignment.agent_id != self.agent_id {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "lifecycle assignment targets another Agent",
            ));
        }
        Ok(())
    }
}

impl LifecycleJournal for SqliteLifecycleJournal {
    fn claim(
        &self,
        assignment: &AgentResourceLifecycleAssignment,
        now_unix_ms: UnixMillis,
    ) -> AgentResult<LifecycleClaimOutcome> {
        self.validate_assignment(assignment)?;
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if let Some(existing) = load_record(&transaction, &assignment.assignment.assignment_id)? {
            validate_same_assignment(&existing, assignment)?;
            transaction.commit().map_err(storage_error)?;
            return Ok(LifecycleClaimOutcome::Existing(existing));
        }
        let record = LifecycleJournalRecord {
            command: assignment.clone(),
            assignment_id: assignment.assignment.assignment_id.clone(),
            deletion_id: assignment.assignment.deletion_id.clone(),
            lifecycle_generation: assignment.assignment.lifecycle_generation,
            request_digest: assignment.assignment.request_digest,
            delivery_digest: neoengram_domain::protocol::jcs_blake3(assignment)
                .map_err(AgentError::from)?,
            action: assignment.assignment.action,
            state: LifecycleJournalState::Claimed,
            claimed_at_unix_ms: now_unix_ms,
            completed_at_unix_ms: None,
            report: None,
        };
        insert_record(&transaction, &record)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(LifecycleClaimOutcome::Claimed(record))
    }

    fn complete(&self, report: ResourceLifecycleReport) -> AgentResult<LifecycleCompleteOutcome> {
        report.validate().map_err(AgentError::from)?;
        if report.tenant_id != self.tenant_id || report.agent_id != self.agent_id {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "lifecycle report belongs to another Agent or Tenant",
            ));
        }
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let existing = load_record(&transaction, &report.assignment_id)?.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::AssignmentNotFound,
                "lifecycle report has no durable claim",
            )
        })?;
        report
            .validate_for_assignment(&existing.command)
            .map_err(AgentError::from)?;
        if existing.request_digest != report.request_digest
            || existing.deletion_id != report.deletion_id
            || existing.lifecycle_generation != report.lifecycle_generation
            || existing.action != report.action
        {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "lifecycle report does not match its durable claim",
            ));
        }
        if let Some(previous) = &existing.report {
            if previous == &report {
                transaction.commit().map_err(storage_error)?;
                return Ok(LifecycleCompleteOutcome::Existing(existing));
            }
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "lifecycle assignment already has a different terminal report",
            ));
        }
        let terminal = matches!(
            report.state,
            ResourceLifecycleReportState::Quarantined
                | ResourceLifecycleReportState::Restored
                | ResourceLifecycleReportState::Purged
                | ResourceLifecycleReportState::JobsCancelled
                | ResourceLifecycleReportState::Blocked
                | ResourceLifecycleReportState::Failed
        );
        if !terminal {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "only a terminal lifecycle report can complete a journal claim",
            ));
        }
        let mut completed = existing.clone();
        completed.state = if matches!(
            report.state,
            ResourceLifecycleReportState::Blocked | ResourceLifecycleReportState::Failed
        ) {
            LifecycleJournalState::Failed
        } else {
            LifecycleJournalState::Completed
        };
        completed.completed_at_unix_ms = Some(report.reported_at_unix_ms);
        completed.report = Some(report);
        update_record(&transaction, &completed)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(LifecycleCompleteOutcome::Completed(completed))
    }

    fn get(
        &self,
        assignment_id: &LifecycleAssignmentId,
    ) -> AgentResult<Option<LifecycleJournalRecord>> {
        let connection = self.storage.connection()?;
        load_record(&connection, assignment_id)
    }

    fn list_unfinished(&self) -> AgentResult<Vec<LifecycleJournalRecord>> {
        let connection = self.storage.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT assignment_id, payload FROM lifecycle_records WHERE completed_at_unix_ms IS NULL ORDER BY claimed_at_unix_ms, assignment_id",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(storage_error)?;
        rows.map(|row| {
            let (assignment_id, payload) = row.map_err(storage_error)?;
            let stored: StoredRecord = serde_json::from_slice(&payload)
                .map_err(|_| storage_corruption("lifecycle journal payload is invalid"))?;
            if stored.format != RECORD_FORMAT
                || stored.value.assignment_id.as_str() != assignment_id
                || stored.value.state != LifecycleJournalState::Claimed
                || stored.value.completed_at_unix_ms.is_some()
                || stored.value.report.is_some()
            {
                return Err(storage_corruption(
                    "unfinished lifecycle journal record is inconsistent",
                ));
            }
            Ok(stored.value)
        })
        .collect()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecord {
    format: u32,
    value: LifecycleJournalRecord,
}

fn insert_record(
    transaction: &rusqlite::Transaction<'_>,
    record: &LifecycleJournalRecord,
) -> AgentResult<()> {
    let payload = serde_json::to_vec(&StoredRecord {
        format: RECORD_FORMAT,
        value: record.clone(),
    })
    .map_err(storage_error)?;
    transaction
        .execute(
            "INSERT INTO lifecycle_records (assignment_id, deletion_id, lifecycle_generation, request_digest, delivery_digest, state, claimed_at_unix_ms, completed_at_unix_ms, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                record.assignment_id.as_str(),
                record.deletion_id.as_str(),
                record.lifecycle_generation.get().to_string(),
                record.request_digest.to_string(),
                record.delivery_digest.to_string(),
                serde_json::to_string(&record.state).map_err(storage_error)?,
                record.claimed_at_unix_ms.get().to_string(),
                record.completed_at_unix_ms.map(|value| value.get().to_string()),
                payload,
            ],
        )
        .map_err(storage_error)?;
    Ok(())
}

fn update_record(
    transaction: &rusqlite::Transaction<'_>,
    record: &LifecycleJournalRecord,
) -> AgentResult<()> {
    let payload = serde_json::to_vec(&StoredRecord {
        format: RECORD_FORMAT,
        value: record.clone(),
    })
    .map_err(storage_error)?;
    let changed = transaction
        .execute(
            "UPDATE lifecycle_records SET state = ?2, completed_at_unix_ms = ?3, payload = ?4 WHERE assignment_id = ?1",
            params![
                record.assignment_id.as_str(),
                serde_json::to_string(&record.state).map_err(storage_error)?,
                record.completed_at_unix_ms.map(|value| value.get().to_string()),
                payload,
            ],
        )
        .map_err(storage_error)?;
    if changed != 1 {
        return Err(storage_corruption(
            "lifecycle journal update affected an unexpected number of rows",
        ));
    }
    Ok(())
}

fn load_record(
    connection: &rusqlite::Connection,
    assignment_id: &LifecycleAssignmentId,
) -> AgentResult<Option<LifecycleJournalRecord>> {
    let payload = connection
        .query_row(
            "SELECT payload FROM lifecycle_records WHERE assignment_id = ?1",
            params![assignment_id.as_str()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(storage_error)?;
    payload
        .map(|payload| {
            let stored: StoredRecord = serde_json::from_slice(&payload)
                .map_err(|_| storage_corruption("lifecycle journal payload is invalid"))?;
            if stored.format != RECORD_FORMAT {
                return Err(storage_corruption(
                    "unsupported lifecycle journal record format",
                ));
            }
            if stored.value.assignment_id != *assignment_id {
                return Err(storage_corruption(
                    "lifecycle journal payload identity does not match its key",
                ));
            }
            Ok(stored.value)
        })
        .transpose()
}

fn validate_same_assignment(
    existing: &LifecycleJournalRecord,
    assignment: &AgentResourceLifecycleAssignment,
) -> AgentResult<()> {
    if existing.request_digest != assignment.assignment.request_digest
        || existing.deletion_id != assignment.assignment.deletion_id
        || existing.lifecycle_generation != assignment.assignment.lifecycle_generation
        || existing.action != assignment.assignment.action
        || existing.delivery_digest
            != neoengram_domain::protocol::jcs_blake3(assignment).map_err(AgentError::from)?
        || existing.command != *assignment
    {
        return Err(AgentError::new(
            AgentErrorCode::AssignmentMismatch,
            "lifecycle assignment identity or generation was reused with different content",
        ));
    }
    Ok(())
}

fn identity_mismatch() -> AgentError {
    AgentError::new(
        AgentErrorCode::AssignmentMismatch,
        "Agent lifecycle journal identity does not match the configured Agent/Tenant",
    )
}

#[cfg(test)]
mod tests {
    use neoengram_domain::protocol::{
        AgentId, AgentMountId, AgentResourceLifecycleScope, ArtifactId, ArtifactPlacementId,
        ContentDigest, DeletionId, EdgeClusterId, LifecycleAssignmentId, LifecycleGeneration,
        PlacementGeneration, ProjectId, ResourceLifecycleAction, ResourceLifecycleAssignment,
        ResourceRef, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId,
    };

    use super::*;

    fn assignment() -> AgentResourceLifecycleAssignment {
        AgentResourceLifecycleAssignment {
            assignment: ResourceLifecycleAssignment {
                assignment_id: LifecycleAssignmentId::new("la-1").unwrap(),
                tenant_id: TenantId::new("tenant-a").unwrap(),
                deletion_id: DeletionId::new("del-1").unwrap(),
                resource: ResourceRef::Artifact {
                    project_id: ProjectId::new("project-a").unwrap(),
                    artifact_id: ArtifactId::new("artifact-a").unwrap(),
                },
                action: ResourceLifecycleAction::Quarantine,
                lifecycle_generation: LifecycleGeneration::new(3),
                request_digest: ContentDigest::from_bytes([0x11; 32]),
                deadline_unix_ms: UnixMillis::new(10_000),
            },
            resource_scope: AgentResourceLifecycleScope::Artifact {
                project_id: ProjectId::new("project-a").unwrap(),
                artifact_id: ArtifactId::new("artifact-a").unwrap(),
                storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
                artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
                placement_generation: PlacementGeneration::new(2),
            },
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            volume_marker_id: VolumeMarkerId::new("volume-a").unwrap(),
            session_generation: neoengram_domain::protocol::SessionGeneration::new(4),
            mount_generation: neoengram_domain::protocol::MountGeneration::new(5),
            owner_generation: neoengram_domain::protocol::OwnerGeneration::new(6),
            extensions: Default::default(),
        }
    }

    #[test]
    fn claim_is_idempotent_and_rejects_digest_reuse() {
        let root = tempfile::tempdir().unwrap();
        let config = SqliteLifecycleJournalConfig::new(
            root.path(),
            AgentId::new("agent-a").unwrap(),
            TenantId::new("tenant-a").unwrap(),
        );
        let journal = SqliteLifecycleJournal::open(config.clone()).unwrap();
        let command = assignment();
        let first = journal.claim(&command, UnixMillis::new(100)).unwrap();
        assert!(matches!(first, LifecycleClaimOutcome::Claimed(_)));
        assert_eq!(journal.list_unfinished().unwrap().len(), 1);
        let replay = journal.claim(&command, UnixMillis::new(101)).unwrap();
        assert!(matches!(replay, LifecycleClaimOutcome::Existing(_)));
        let mut report = ResourceLifecycleReport::accepted(&command, UnixMillis::new(200));
        report.state = ResourceLifecycleReportState::Quarantined;
        let completed = journal.complete(report.clone()).unwrap();
        assert!(matches!(completed, LifecycleCompleteOutcome::Completed(_)));
        assert!(journal.list_unfinished().unwrap().is_empty());
        assert!(matches!(
            journal.complete(report).unwrap(),
            LifecycleCompleteOutcome::Existing(_)
        ));
        drop(journal);
        let reopened = SqliteLifecycleJournal::open(config).unwrap();
        assert_eq!(
            reopened
                .get(&command.assignment.assignment_id)
                .unwrap()
                .unwrap()
                .state,
            LifecycleJournalState::Completed
        );

        let mut changed = command;
        changed.assignment.request_digest = ContentDigest::from_bytes([0x22; 32]);
        assert!(reopened.claim(&changed, UnixMillis::new(102)).is_err());
    }
}
