use std::{fmt, path::Path};

use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::{
    sqlite_storage::{storage_corruption, storage_error, LockedSqlite, SqliteDefinition},
    AgentError, AgentErrorCode, AgentResult,
};

const DATABASE_FILE: &str = "system.sqlite3";
const LOCK_FILE: &str = "system.lock";
const APPLICATION_ID: i64 = 0x4e45_4f53;
const SCHEMA_VERSION: i64 = 1;
const RECORD_FORMAT: u32 = 1;
const SYSTEM_MAGIC: &str = "neoengram-agent-system-v1";
const MAX_ID_BYTES: usize = 256;
const MAX_PRIVATE_KEY_BYTES: usize = 1024 * 1024;
const MAX_CERTIFICATE_BYTES: usize = 1024 * 1024;
const MAX_CERTIFICATE_CHAIN_LENGTH: usize = 16;

const SCHEMA: &str = r#"
CREATE TABLE system_metadata (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    magic TEXT NOT NULL
) STRICT;
INSERT INTO system_metadata (singleton, magic)
VALUES (1, 'neoengram-agent-system-v1');
CREATE TABLE system_identity (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    bootstrap_request_id TEXT NOT NULL,
    installation_id TEXT NOT NULL,
    revision TEXT NOT NULL CHECK (
        revision <> '' AND revision NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL
) STRICT;
"#;

/// Locally generated private key bytes. Formatting never exposes the key material.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrivateKeyMaterial(Vec<u8>);

impl PrivateKeyMaterial {
    pub fn new(bytes: impl Into<Vec<u8>>) -> AgentResult<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > MAX_PRIVATE_KEY_BYTES {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "Agent private key material has an invalid length",
            ));
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn expose_secret(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for PrivateKeyMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrivateKeyMaterial([REDACTED])")
    }
}

/// Immutable values generated before the first bootstrap attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemIdentitySeed {
    pub bootstrap_request_id: String,
    pub installation_id: String,
    pub private_key: PrivateKeyMaterial,
}

impl SystemIdentitySeed {
    pub fn new(
        bootstrap_request_id: impl Into<String>,
        installation_id: impl Into<String>,
        private_key: PrivateKeyMaterial,
    ) -> AgentResult<Self> {
        let value = Self {
            bootstrap_request_id: bootstrap_request_id.into(),
            installation_id: installation_id.into(),
            private_key,
        };
        validate_identifier("bootstrap request ID", &value.bootstrap_request_id)?;
        validate_identifier("installation ID", &value.installation_id)?;
        Ok(value)
    }
}

/// Center-approved identity. Once stored, neither value can be rebound in place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovedAgentIdentity {
    pub agent_id: String,
    pub enrollment_id: String,
}

impl ApprovedAgentIdentity {
    pub fn new(agent_id: impl Into<String>, enrollment_id: impl Into<String>) -> AgentResult<Self> {
        let value = Self {
            agent_id: agent_id.into(),
            enrollment_id: enrollment_id.into(),
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> AgentResult<()> {
        validate_identifier("approved Agent ID", &self.agent_id)?;
        validate_identifier("approved enrollment ID", &self.enrollment_id)
    }
}

/// Durable certificate material and the generations under which it was issued.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCertificateState {
    pub certificate_chain_pem: Vec<String>,
    pub certificate_generation: u64,
    pub session_generation: u64,
    pub mount_generation: u64,
    pub owner_generation: u64,
}

impl fmt::Debug for AgentCertificateState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentCertificateState")
            .field("certificate_chain_pem", &"[REDACTED]")
            .field(
                "certificate_chain_length",
                &self.certificate_chain_pem.len(),
            )
            .field("certificate_generation", &self.certificate_generation)
            .field("session_generation", &self.session_generation)
            .field("mount_generation", &self.mount_generation)
            .field("owner_generation", &self.owner_generation)
            .finish()
    }
}

impl AgentCertificateState {
    pub fn validate(&self) -> AgentResult<()> {
        if self.certificate_chain_pem.is_empty()
            || self.certificate_chain_pem.len() > MAX_CERTIFICATE_CHAIN_LENGTH
            || self.certificate_chain_pem.iter().any(|certificate| {
                certificate.is_empty() || certificate.len() > MAX_CERTIFICATE_BYTES
            })
        {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "Agent certificate chain has an invalid length",
            ));
        }
        if self.certificate_generation == 0
            || self.session_generation == 0
            || self.mount_generation == 0
            || self.owner_generation == 0
        {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "Agent certificate and runtime generations must be positive",
            ));
        }
        Ok(())
    }
}

/// Complete durable system identity. `revision` is advanced only by successful CAS updates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemIdentityRecord {
    pub revision: u64,
    pub bootstrap_request_id: String,
    pub installation_id: String,
    pub private_key: PrivateKeyMaterial,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved: Option<ApprovedAgentIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate: Option<AgentCertificateState>,
}

impl SystemIdentityRecord {
    fn from_seed(seed: SystemIdentitySeed) -> Self {
        Self {
            revision: 1,
            bootstrap_request_id: seed.bootstrap_request_id,
            installation_id: seed.installation_id,
            private_key: seed.private_key,
            approved: None,
            certificate: None,
        }
    }

    fn validate(&self) -> AgentResult<()> {
        if self.revision == 0 {
            return Err(storage_corruption(
                "Agent system identity revision must be positive",
            ));
        }
        validate_identifier("bootstrap request ID", &self.bootstrap_request_id)?;
        validate_identifier("installation ID", &self.installation_id)?;
        PrivateKeyMaterial::new(self.private_key.0.clone())?;
        if let Some(approved) = &self.approved {
            approved.validate()?;
        }
        if let Some(certificate) = &self.certificate {
            if self.approved.is_none() {
                return Err(storage_corruption(
                    "Agent certificate exists before approval identity is bound",
                ));
            }
            certificate.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct SqliteSystemIdentityStore {
    storage: LockedSqlite,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredIdentityV1 {
    format: u32,
    value: SystemIdentityRecord,
}

struct StoredColumns {
    bootstrap_request_id: String,
    installation_id: String,
    revision: String,
    payload: Vec<u8>,
}

impl SqliteSystemIdentityStore {
    /// Opens the system database. The identity is created separately with [`Self::initialize`].
    pub fn open(root: impl AsRef<Path>) -> AgentResult<Self> {
        let storage = LockedSqlite::open(
            root.as_ref(),
            SqliteDefinition {
                database_file: DATABASE_FILE,
                lock_file: LOCK_FILE,
                application_id: APPLICATION_ID,
                schema_version: SCHEMA_VERSION,
                schema: SCHEMA,
                tables: &["system_identity", "system_metadata"],
            },
        )?;
        let store = Self { storage };
        store.validate_metadata()?;
        Ok(store)
    }

    /// Atomically creates the immutable bootstrap identity, or replays the exact existing seed.
    pub fn initialize(&self, seed: SystemIdentitySeed) -> AgentResult<SystemIdentityRecord> {
        let candidate = SystemIdentityRecord::from_seed(seed);
        candidate.validate()?;
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if let Some(existing) = load_in_transaction(&transaction)? {
            transaction.commit().map_err(storage_error)?;
            if same_immutable_seed(&existing, &candidate) {
                return Ok(existing);
            }
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "Agent system identity is already initialized with different immutable values",
            ));
        }
        insert_record(&transaction, &candidate)?;
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(candidate)
    }

    pub fn load(&self) -> AgentResult<Option<SystemIdentityRecord>> {
        let connection = self.storage.connection()?;
        let columns = connection
            .query_row(
                "SELECT bootstrap_request_id, installation_id, revision, payload \
                 FROM system_identity WHERE singleton = 1",
                [],
                columns_from_row,
            )
            .optional()
            .map_err(storage_error)?;
        columns.map(decode).transpose()
    }

    /// Binds the approved center identity once. An exact replay returns the durable result.
    pub fn bind_approved(
        &self,
        expected_revision: u64,
        approved: ApprovedAgentIdentity,
    ) -> AgentResult<SystemIdentityRecord> {
        approved.validate()?;
        let mut next = self.required_identity()?;
        if next.approved.as_ref() == Some(&approved) {
            return Ok(next);
        }
        if next.approved.is_some() {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "approved Agent identity cannot be rebound",
            ));
        }
        next.approved = Some(approved);
        self.compare_exchange(expected_revision, next)
    }

    /// Installs the next certificate generation. Exact replay is idempotent.
    pub fn install_certificate(
        &self,
        expected_revision: u64,
        expected_certificate_generation: u64,
        certificate: AgentCertificateState,
    ) -> AgentResult<SystemIdentityRecord> {
        certificate.validate()?;
        let mut next = self.required_identity()?;
        if next.certificate.as_ref() == Some(&certificate) {
            return Ok(next);
        }
        if next.approved.is_none() {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "Agent certificate cannot be installed before approval",
            ));
        }
        let current_generation = next
            .certificate
            .as_ref()
            .map_or(0, |current| current.certificate_generation);
        if current_generation != expected_certificate_generation
            || certificate.certificate_generation
                != expected_certificate_generation
                    .checked_add(1)
                    .ok_or_else(|| {
                        AgentError::new(
                            AgentErrorCode::GenerationMismatch,
                            "Agent certificate generation overflow",
                        )
                    })?
        {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "Agent certificate generation changed",
            ));
        }
        if let Some(current) = &next.certificate {
            if certificate.session_generation < current.session_generation
                || certificate.mount_generation < current.mount_generation
                || certificate.owner_generation < current.owner_generation
            {
                return Err(AgentError::new(
                    AgentErrorCode::GenerationMismatch,
                    "Agent runtime generations cannot move backwards",
                ));
            }
        }
        next.certificate = Some(certificate);
        self.compare_exchange(expected_revision, next)
    }

    /// Atomically persists a validated replacement while preserving immutable identity fields.
    pub fn compare_exchange(
        &self,
        expected_revision: u64,
        mut next: SystemIdentityRecord,
    ) -> AgentResult<SystemIdentityRecord> {
        next.validate()?;
        let mut connection = self.storage.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let current = load_in_transaction(&transaction)?.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                "Agent system identity is not initialized",
            )
        })?;
        if same_record_ignoring_revision(&current, &next) {
            transaction.commit().map_err(storage_error)?;
            return Ok(current);
        }
        if current.revision != expected_revision {
            return Err(AgentError::new(
                AgentErrorCode::LedgerConflict,
                "Agent system identity revision changed",
            ));
        }
        if !same_immutable_seed(&current, &next)
            || current
                .approved
                .as_ref()
                .is_some_and(|approved| next.approved.as_ref() != Some(approved))
            || current.certificate.is_some() && next.certificate.is_none()
        {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "Agent system identity immutable values cannot change",
            ));
        }
        validate_certificate_transition(&current, &next)?;
        next.revision = expected_revision.checked_add(1).ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::Internal,
                "Agent system identity revision overflow",
            )
        })?;
        let payload = encode(&next)?;
        let changed = transaction
            .execute(
                "UPDATE system_identity SET revision = ?1, payload = ?2 \
                 WHERE singleton = 1 AND revision = ?3",
                params![
                    next.revision.to_string(),
                    payload,
                    expected_revision.to_string()
                ],
            )
            .map_err(storage_error)?;
        if changed != 1 {
            return Err(AgentError::new(
                AgentErrorCode::LedgerConflict,
                "Agent system identity changed during compare-exchange",
            ));
        }
        transaction.commit().map_err(storage_error)?;
        drop(connection);
        self.storage.secure_files()?;
        Ok(next)
    }

    pub fn integrity_check(&self) -> AgentResult<()> {
        self.storage.integrity_check()?;
        self.validate_metadata()?;
        if let Some(identity) = self.load()? {
            identity.validate()?;
        }
        Ok(())
    }

    fn required_identity(&self) -> AgentResult<SystemIdentityRecord> {
        self.load()?.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                "Agent system identity is not initialized",
            )
        })
    }

    fn validate_metadata(&self) -> AgentResult<()> {
        let connection = self.storage.connection()?;
        let magic: String = connection
            .query_row(
                "SELECT magic FROM system_metadata WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(storage_error)?;
        if magic != SYSTEM_MAGIC {
            return Err(storage_corruption(
                "Agent system database identity is invalid",
            ));
        }
        Ok(())
    }
}

fn validate_certificate_transition(
    current: &SystemIdentityRecord,
    next: &SystemIdentityRecord,
) -> AgentResult<()> {
    match (&current.certificate, &next.certificate) {
        (None, Some(next_certificate)) if next_certificate.certificate_generation == 1 => Ok(()),
        (Some(current_certificate), Some(next_certificate))
            if next_certificate.certificate_generation
                == current_certificate.certificate_generation.saturating_add(1)
                && next_certificate.session_generation
                    >= current_certificate.session_generation
                && next_certificate.mount_generation >= current_certificate.mount_generation
                && next_certificate.owner_generation >= current_certificate.owner_generation =>
        {
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err(AgentError::new(
            AgentErrorCode::GenerationMismatch,
            "Agent certificate update must advance exactly one generation",
        )),
    }
}

fn same_immutable_seed(left: &SystemIdentityRecord, right: &SystemIdentityRecord) -> bool {
    left.bootstrap_request_id == right.bootstrap_request_id
        && left.installation_id == right.installation_id
        && left.private_key == right.private_key
}

fn same_record_ignoring_revision(
    left: &SystemIdentityRecord,
    right: &SystemIdentityRecord,
) -> bool {
    same_immutable_seed(left, right)
        && left.approved == right.approved
        && left.certificate == right.certificate
}

fn validate_identifier(name: &'static str, value: &str) -> AgentResult<()> {
    if value.is_empty()
        || value.len() > MAX_ID_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(AgentError::new(
            AgentErrorCode::InvalidState,
            format!("Agent {name} is invalid"),
        ));
    }
    Ok(())
}

fn insert_record(transaction: &Transaction<'_>, record: &SystemIdentityRecord) -> AgentResult<()> {
    transaction
        .execute(
            "INSERT INTO system_identity \
             (singleton, bootstrap_request_id, installation_id, revision, payload) \
             VALUES (1, ?1, ?2, ?3, ?4)",
            params![
                record.bootstrap_request_id,
                record.installation_id,
                record.revision.to_string(),
                encode(record)?,
            ],
        )
        .map_err(storage_error)?;
    Ok(())
}

fn load_in_transaction(transaction: &Transaction<'_>) -> AgentResult<Option<SystemIdentityRecord>> {
    let columns = transaction
        .query_row(
            "SELECT bootstrap_request_id, installation_id, revision, payload \
             FROM system_identity WHERE singleton = 1",
            [],
            columns_from_row,
        )
        .optional()
        .map_err(storage_error)?;
    columns.map(decode).transpose()
}

fn columns_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredColumns> {
    Ok(StoredColumns {
        bootstrap_request_id: row.get(0)?,
        installation_id: row.get(1)?,
        revision: row.get(2)?,
        payload: row.get(3)?,
    })
}

fn encode(record: &SystemIdentityRecord) -> AgentResult<Vec<u8>> {
    serde_json::to_vec(&StoredIdentityV1 {
        format: RECORD_FORMAT,
        value: record.clone(),
    })
    .map_err(storage_error)
}

fn decode(columns: StoredColumns) -> AgentResult<SystemIdentityRecord> {
    let stored: StoredIdentityV1 = serde_json::from_slice(&columns.payload)
        .map_err(|_| storage_corruption("Agent system identity payload is invalid"))?;
    if stored.format != RECORD_FORMAT {
        return Err(storage_corruption(
            "Agent system identity format is unsupported",
        ));
    }
    let record = stored.value;
    record.validate()?;
    let revision = columns
        .revision
        .parse::<u64>()
        .map_err(|_| storage_corruption("Agent system identity revision is invalid"))?;
    if columns.bootstrap_request_id != record.bootstrap_request_id
        || columns.installation_id != record.installation_id
        || revision != record.revision
    {
        return Err(storage_corruption(
            "Agent system identity indexed columns differ from its payload",
        ));
    }
    Ok(record)
}
