use std::path::PathBuf;

use neoengram_core::ContentDigest;
use neoengram_protocol::{AgentId, TenantId};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::{
    sqlite_storage::{storage_corruption, storage_error, LockedSqlite, SqliteDefinition},
    AgentError, AgentErrorCode, AgentResult, AssignmentKey, ClaimOutcome, Ledger, LedgerClaim,
    LedgerRecord,
};

const DATABASE_FILE: &str = "ledger.sqlite3";
const LOCK_FILE: &str = "ledger.lock";
const APPLICATION_ID: i64 = 0x4e45_4f4c;
const SCHEMA_VERSION: i64 = 1;
const RECORD_FORMAT: u32 = 1;
const LEDGER_MAGIC: &str = "neoengram-agent-ledger-v1";

const SCHEMA: &str = r#"
CREATE TABLE ledger_metadata (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    magic TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL
) STRICT;
CREATE TABLE ledger_records (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    request_digest TEXT NOT NULL CHECK (
        length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*'
    ),
    revision TEXT NOT NULL CHECK (
        revision <> '' AND revision NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
) STRICT;
"#;

#[derive(Debug)]
pub struct SqliteLedger {
    storage: LockedSqlite,
    agent_id: AgentId,
    tenant_id: TenantId,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SqliteLedgerConfig {
    pub root: PathBuf,
    pub agent_id: AgentId,
    pub tenant_id: TenantId,
}

impl std::fmt::Debug for SqliteLedgerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteLedgerConfig")
            .field("root_configured", &!self.root.as_os_str().is_empty())
            .field("agent_id", &self.agent_id)
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl SqliteLedgerConfig {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, agent_id: AgentId, tenant_id: TenantId) -> Self {
        Self {
            root: root.into(),
            agent_id,
            tenant_id,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecordV1 {
    format: u32,
    value: LedgerRecord,
}

struct StoredColumns {
    tenant_id: String,
    job_id: String,
    request_digest: String,
    revision: String,
    payload: Vec<u8>,
}

impl SqliteLedger {
    /// Opens or creates the durable ledger inside an explicit, process-owned directory.
    pub fn open(config: SqliteLedgerConfig) -> AgentResult<Self> {
        let storage = LockedSqlite::open(
            &config.root,
            SqliteDefinition {
                database_file: DATABASE_FILE,
                lock_file: LOCK_FILE,
                application_id: APPLICATION_ID,
                schema_version: SCHEMA_VERSION,
                schema: SCHEMA,
                tables: &["ledger_metadata", "ledger_records"],
            },
        )?;
        let ledger = Self {
            storage,
            agent_id: config.agent_id,
            tenant_id: config.tenant_id,
        };
        ledger.bind_or_validate_identity()?;
        Ok(ledger)
    }

    pub fn integrity_check(&self) -> AgentResult<()> {
        self.storage.integrity_check()?;
        self.validate_metadata()
    }

    fn validate_metadata(&self) -> AgentResult<()> {
        let connection = self.storage.connection()?;
        let (magic, agent_id, tenant_id): (String, String, String) = connection
            .query_row(
                "SELECT magic, agent_id, tenant_id FROM ledger_metadata WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(storage_error)?;
        if magic != LEDGER_MAGIC {
            return Err(storage_corruption("Agent ledger identity is invalid"));
        }
        if agent_id != self.agent_id.as_str() || tenant_id != self.tenant_id.as_str() {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "Agent ledger database identity does not match the configured Agent/Tenant",
            ));
        }
        Ok(())
    }

    fn bind_or_validate_identity(&self) -> AgentResult<()> {
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let identity = transaction
            .query_row(
                "SELECT magic, agent_id, tenant_id FROM ledger_metadata WHERE singleton = 1",
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
                let records: i64 = transaction
                    .query_row("SELECT count(*) FROM ledger_records", [], |row| row.get(0))
                    .map_err(storage_error)?;
                if records != 0 {
                    return Err(storage_corruption(
                        "Agent ledger identity is missing from a non-empty database",
                    ));
                }
                transaction
                    .execute(
                        "INSERT INTO ledger_metadata \
                         (singleton, magic, agent_id, tenant_id) VALUES (1, ?1, ?2, ?3)",
                        params![
                            LEDGER_MAGIC,
                            self.agent_id.as_str(),
                            self.tenant_id.as_str()
                        ],
                    )
                    .map_err(storage_error)?;
            }
            Some((magic, agent_id, tenant_id))
                if magic == LEDGER_MAGIC
                    && agent_id == self.agent_id.as_str()
                    && tenant_id == self.tenant_id.as_str() => {}
            Some(_) => {
                return Err(AgentError::new(
                    AgentErrorCode::AssignmentMismatch,
                    "Agent ledger database identity does not match the configured Agent/Tenant",
                ));
            }
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()
    }

    fn validate_assignment_scope(
        &self,
        agent_id: &AgentId,
        tenant_id: &TenantId,
    ) -> AgentResult<()> {
        if agent_id != &self.agent_id || tenant_id != &self.tenant_id {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "assignment does not belong to this Agent/Tenant ledger",
            ));
        }
        Ok(())
    }
}

impl Ledger for SqliteLedger {
    fn claim(&self, claim: LedgerClaim) -> AgentResult<ClaimOutcome> {
        self.validate_assignment_scope(&claim.assignment.agent_id, &claim.assignment.tenant_id)?;
        let key = AssignmentKey::from_assignment(&claim.assignment);
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if let Some(existing) = load_in_transaction(&transaction, &key)? {
            transaction.commit().map_err(storage_error)?;
            if existing.request_digest == claim.assignment.request_digest {
                return Ok(ClaimOutcome::Existing(existing));
            }
            return Err(AgentError::new(
                AgentErrorCode::JobIdReused,
                format!("job {key} was already claimed with a different request digest"),
            ));
        }

        let record = LedgerRecord::claimed(claim.assignment, claim.claimed_at_unix_ms);
        insert_record(&transaction, &record)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(ClaimOutcome::Claimed(record))
    }

    fn load(&self, key: &AssignmentKey) -> AgentResult<Option<LedgerRecord>> {
        if key.tenant_id != self.tenant_id {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "assignment key does not belong to this Tenant ledger",
            ));
        }
        let connection = self.storage.connection()?;
        load_from_connection(&connection, key)
    }

    fn list_active(&self) -> AgentResult<Vec<LedgerRecord>> {
        let connection = self.storage.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT tenant_id, job_id, request_digest, revision, payload \
                 FROM ledger_records WHERE tenant_id = ?1 ORDER BY job_id",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(params![self.tenant_id.as_str()], columns_from_row)
            .map_err(storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage_error)?;
        let records = rows
            .into_iter()
            .map(decode)
            .collect::<AgentResult<Vec<_>>>()?;
        Ok(records
            .into_iter()
            .filter(|record| record.state != crate::AgentAssignmentState::Completed)
            .collect())
    }

    fn compare_exchange(
        &self,
        expected_revision: u64,
        mut next: LedgerRecord,
    ) -> AgentResult<LedgerRecord> {
        self.validate_assignment_scope(&next.assignment.agent_id, &next.assignment.tenant_id)?;
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let current = load_in_transaction(&transaction, &next.key)?.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::AssignmentNotFound,
                format!("assignment {} is not present in the ledger", next.key),
            )
        })?;
        if current.revision != expected_revision {
            return Err(AgentError::new(
                AgentErrorCode::LedgerConflict,
                format!(
                    "assignment {} revision changed from {} to {}",
                    next.key, expected_revision, current.revision
                ),
            ));
        }
        if current.key != next.key
            || current.request_digest != next.request_digest
            || current.assignment != next.assignment
            || next.key != AssignmentKey::from_assignment(&next.assignment)
            || next.request_digest != next.assignment.request_digest
        {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "ledger compare-exchange cannot change the durable assignment payload",
            ));
        }
        next.revision = expected_revision.checked_add(1).ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::Internal,
                "ledger revision overflow while updating assignment",
            )
        })?;
        let payload = encode(&next)?;
        let changed = transaction
            .execute(
                "UPDATE ledger_records SET revision = ?1, payload = ?2 \
                 WHERE tenant_id = ?3 AND job_id = ?4 AND revision = ?5",
                params![
                    next.revision.to_string(),
                    payload,
                    next.key.tenant_id.as_str(),
                    next.key.job_id.as_str(),
                    expected_revision.to_string(),
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentError::new(
                AgentErrorCode::LedgerConflict,
                "ledger record changed during compare-exchange",
            ));
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(next)
    }
}

fn insert_record(transaction: &Transaction<'_>, record: &LedgerRecord) -> AgentResult<()> {
    let payload = encode(record)?;
    transaction
        .execute(
            "INSERT INTO ledger_records \
             (tenant_id, job_id, request_digest, revision, payload) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                record.key.tenant_id.as_str(),
                record.key.job_id.as_str(),
                record.request_digest.to_string(),
                record.revision.to_string(),
                payload,
            ],
        )
        .map_err(storage_error)?;
    Ok(())
}

fn load_in_transaction(
    transaction: &Transaction<'_>,
    key: &AssignmentKey,
) -> AgentResult<Option<LedgerRecord>> {
    load_row(
        transaction
            .query_row(
                "SELECT tenant_id, job_id, request_digest, revision, payload \
                 FROM ledger_records WHERE tenant_id = ?1 AND job_id = ?2",
                params![key.tenant_id.as_str(), key.job_id.as_str()],
                columns_from_row,
            )
            .optional()
            .map_err(storage_error)?,
    )
}

fn load_from_connection(
    connection: &rusqlite::Connection,
    key: &AssignmentKey,
) -> AgentResult<Option<LedgerRecord>> {
    load_row(
        connection
            .query_row(
                "SELECT tenant_id, job_id, request_digest, revision, payload \
                 FROM ledger_records WHERE tenant_id = ?1 AND job_id = ?2",
                params![key.tenant_id.as_str(), key.job_id.as_str()],
                columns_from_row,
            )
            .optional()
            .map_err(storage_error)?,
    )
}

fn columns_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredColumns> {
    Ok(StoredColumns {
        tenant_id: row.get(0)?,
        job_id: row.get(1)?,
        request_digest: row.get(2)?,
        revision: row.get(3)?,
        payload: row.get(4)?,
    })
}

fn load_row(columns: Option<StoredColumns>) -> AgentResult<Option<LedgerRecord>> {
    columns.map(decode).transpose()
}

fn encode(record: &LedgerRecord) -> AgentResult<Vec<u8>> {
    serde_json::to_vec(&StoredRecordV1 {
        format: RECORD_FORMAT,
        value: record.clone(),
    })
    .map_err(storage_error)
}

fn decode(columns: StoredColumns) -> AgentResult<LedgerRecord> {
    let stored: StoredRecordV1 = serde_json::from_slice(&columns.payload)
        .map_err(|_| storage_corruption("Agent ledger record payload is invalid"))?;
    if stored.format != RECORD_FORMAT {
        return Err(storage_corruption(
            "Agent ledger record format is unsupported",
        ));
    }
    let record = stored.value;
    let revision = columns
        .revision
        .parse::<u64>()
        .map_err(|_| storage_corruption("Agent ledger revision is invalid"))?;
    let digest = columns
        .request_digest
        .parse::<ContentDigest>()
        .map_err(|_| storage_corruption("Agent ledger request digest is invalid"))?;
    if columns.tenant_id != record.key.tenant_id.as_str()
        || columns.job_id != record.key.job_id.as_str()
        || digest != record.request_digest
        || revision != record.revision
        || record.key != AssignmentKey::from_assignment(&record.assignment)
        || record.request_digest != record.assignment.request_digest
    {
        return Err(storage_corruption(
            "Agent ledger indexed columns differ from the stored record",
        ));
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_config_debug_redacts_state_root() {
        let config = SqliteLedgerConfig::new(
            "/sensitive/agent-ledger-root",
            AgentId::new("agent-a").unwrap(),
            TenantId::new("tenant-a").unwrap(),
        );

        let debug = format!("{config:?}");

        assert!(debug.contains("root_configured: true"));
        assert!(!debug.contains("/sensitive/agent-ledger-root"));
    }
}
