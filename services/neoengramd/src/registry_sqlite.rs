use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use fs2::FileExt;
use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentEnrollmentId, AgentEnrollmentState, AgentId, AgentInstallationId, EdgeClusterId,
    PvcIdentityDigest, RequestId, ResourceVersion, StorageVolumeId, TenantId, UnixMillis,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
    Row, SqlitePool,
};

use crate::{
    checked_next_registry_resource_version, ensure_immutable_registry_scope,
    validate_registry_insert, validate_registry_record, validate_registry_replace_transition,
    validate_registry_replacement_transition, AgentRegistryInsertOutcome, AgentRegistryRecord,
    AgentRegistryReplacementRecords, AgentRegistryRepository, CentralError, CentralErrorCode,
    CentralResult, PvcVolumeBinding,
};

const DATABASE_FILE_NAME: &str = "agent-registry.sqlite3";
const LOCK_FILE_NAME: &str = "agent-registry.lock";
const SQLITE_APPLICATION_ID: i64 = 0x4e45_4f52;
const SQLITE_SCHEMA_VERSION: i64 = 2;
const STORED_FORMAT_VERSION: u32 = 1;

const SCHEMA_SQL: &str = r#"
CREATE TABLE agent_registry_records (
    enrollment_id TEXT NOT NULL PRIMARY KEY,
    agent_id TEXT NOT NULL UNIQUE,
    token_id TEXT NOT NULL UNIQUE,
    token_request_id TEXT NOT NULL,
    token_digest TEXT NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    edge_cluster_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL,
    pvc_identity_digest TEXT NOT NULL,
    pvc_binding_role TEXT CHECK (pvc_binding_role IN ('owner', 'replacement')),
    bootstrap_request_id TEXT,
    decision_request_id TEXT,
    installation_id TEXT,
    public_key_fingerprint TEXT,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL
) STRICT;
CREATE INDEX agent_registry_volume_scope
    ON agent_registry_records (tenant_id, storage_volume_id);
CREATE INDEX agent_registry_pvc_identity_scope
    ON agent_registry_records (edge_cluster_id, pvc_identity_digest);
CREATE UNIQUE INDEX agent_registry_active_volume_binding
    ON agent_registry_records (tenant_id, storage_volume_id, pvc_binding_role)
    WHERE pvc_binding_role IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_active_pvc_binding
    ON agent_registry_records (edge_cluster_id, pvc_identity_digest, pvc_binding_role)
    WHERE pvc_binding_role IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_token_request_identity
    ON agent_registry_records (tenant_id, token_request_id);
CREATE UNIQUE INDEX agent_registry_bootstrap_request_identity
    ON agent_registry_records (tenant_id, bootstrap_request_id)
    WHERE bootstrap_request_id IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_decision_request_identity
    ON agent_registry_records (tenant_id, decision_request_id)
    WHERE decision_request_id IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_installation_identity
    ON agent_registry_records (installation_id)
    WHERE installation_id IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_public_key_identity
    ON agent_registry_records (public_key_fingerprint)
    WHERE public_key_fingerprint IS NOT NULL;
"#;

#[derive(Clone)]
pub struct SqliteAgentRegistryConfig {
    pub path: PathBuf,
    pub busy_timeout: Duration,
}

impl std::fmt::Debug for SqliteAgentRegistryConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteAgentRegistryConfig")
            .field("path", &"[REDACTED]")
            .field("busy_timeout", &self.busy_timeout)
            .finish()
    }
}

impl SqliteAgentRegistryConfig {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

pub struct SqliteAgentRegistry {
    inner: Arc<SqliteAgentRegistryStore>,
}

impl SqliteAgentRegistry {
    #[must_use]
    pub fn repository(&self) -> Arc<dyn AgentRegistryRepository> {
        self.inner.clone()
    }

    pub async fn integrity_check(&self) -> CentralResult<()> {
        validate_current_schema(&self.inner.pool).await?;
        validate_integrity(&self.inner.pool).await
    }
}

pub async fn open_sqlite_agent_registry(
    config: SqliteAgentRegistryConfig,
) -> CentralResult<SqliteAgentRegistry> {
    let root = prepare_root(&config.path)?;
    let lock = open_lock(&root.join(LOCK_FILE_NAME))?;
    let database = root.join(DATABASE_FILE_NAME);
    validate_database_path(&database)?;
    let initialize =
        !database.exists() || fs::metadata(&database).map_err(storage_error)?.len() == 0;
    let options = SqliteConnectOptions::new()
        .filename(&database)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(config.busy_timeout);
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(storage_error)?;
    initialize_or_validate(&pool, initialize).await?;
    secure_file(&database)?;
    validate_integrity(&pool).await?;
    Ok(SqliteAgentRegistry {
        inner: Arc::new(SqliteAgentRegistryStore { pool, _lock: lock }),
    })
}

struct SqliteAgentRegistryStore {
    pool: SqlitePool,
    _lock: File,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredV1<T> {
    format: u32,
    value: T,
}

#[async_trait]
impl AgentRegistryRepository for SqliteAgentRegistryStore {
    async fn get(
        &self,
        enrollment_id: &AgentEnrollmentId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, \
             resource_version, payload FROM agent_registry_records WHERE enrollment_id = ?",
        )
        .bind(enrollment_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_row).transpose()
    }

    async fn get_by_agent(&self, agent_id: &AgentId) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, \
             resource_version, payload FROM agent_registry_records WHERE agent_id = ?",
        )
        .bind(agent_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_row).transpose()
    }

    async fn get_by_token_request_id(
        &self,
        tenant_id: &TenantId,
        token_request_id: &RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE tenant_id = ? AND token_request_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(token_request_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_row).transpose()
    }

    async fn get_by_token_digest(
        &self,
        token_digest: &ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE token_digest = ?",
        )
        .bind(token_digest.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_row).transpose()
    }

    async fn get_by_bootstrap_request_id(
        &self,
        tenant_id: &TenantId,
        bootstrap_request_id: &RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records \
             WHERE tenant_id = ? AND bootstrap_request_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(bootstrap_request_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let record = row.map(decode_row).transpose()?;
        if record.as_ref().is_some_and(|record| {
            record.enrollment.bootstrap_request_id.as_ref() != Some(bootstrap_request_id)
        }) {
            return Err(storage_corruption(
                "bootstrap request index differs from the stored aggregate",
            ));
        }
        Ok(record)
    }

    async fn get_by_decision_request_id(
        &self,
        tenant_id: &TenantId,
        decision_request_id: &RequestId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records \
             WHERE tenant_id = ? AND decision_request_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(decision_request_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let record = row.map(decode_row).transpose()?;
        if record.as_ref().is_some_and(|record| {
            record
                .enrollment
                .decision_request
                .as_ref()
                .is_none_or(|request| &request.decision_request_id != decision_request_id)
        }) {
            return Err(storage_corruption(
                "decision request index differs from the stored aggregate",
            ));
        }
        Ok(record)
    }

    async fn get_by_installation_id(
        &self,
        installation_id: &AgentInstallationId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE installation_id = ?",
        )
        .bind(installation_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let record = row.map(decode_row).transpose()?;
        if record.as_ref().is_some_and(|record| {
            record
                .candidate
                .as_ref()
                .is_none_or(|candidate| &candidate.installation_id != installation_id)
        }) {
            return Err(storage_corruption(
                "installation index differs from the stored aggregate",
            ));
        }
        Ok(record)
    }

    async fn get_by_public_key_fingerprint(
        &self,
        public_key_fingerprint: &ContentDigest,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE public_key_fingerprint = ?",
        )
        .bind(public_key_fingerprint.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let record = row.map(decode_row).transpose()?;
        if record.as_ref().is_some_and(|record| {
            record
                .candidate
                .as_ref()
                .is_none_or(|candidate| &candidate.public_key_fingerprint != public_key_fingerprint)
        }) {
            return Err(storage_corruption(
                "public-key index differs from the stored aggregate",
            ));
        }
        Ok(record)
    }

    async fn get_current_by_volume(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Option<AgentRegistryRecord>> {
        let rows = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE tenant_id = ? AND storage_volume_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(storage_volume_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut current = None;
        for row in rows {
            let record = decode_row(row)?;
            if record.enrollment.state == AgentEnrollmentState::Approved {
                if current.is_some() {
                    return Err(storage_corruption(
                        "multiple approved Agents own one Tenant Volume",
                    ));
                }
                current = Some(record);
            }
        }
        Ok(current)
    }

    async fn get_pvc_binding(
        &self,
        edge_cluster_id: &EdgeClusterId,
        pvc_identity_digest: &PvcIdentityDigest,
    ) -> CentralResult<Option<PvcVolumeBinding>> {
        let rows = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records \
             WHERE edge_cluster_id = ? AND pvc_identity_digest = ?",
        )
        .bind(edge_cluster_id.as_str())
        .bind(pvc_identity_digest.as_digest().to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut binding = None;
        for row in rows {
            let record = decode_row(row)?;
            if is_terminal(record.enrollment.state) {
                continue;
            }
            let candidate = pvc_binding(&record);
            if binding
                .as_ref()
                .is_some_and(|existing| existing != &candidate)
            {
                return Err(storage_corruption(
                    "one EdgeCluster PVC identity is bound to multiple Volumes",
                ));
            }
            binding = Some(candidate);
        }
        Ok(binding)
    }

    async fn expire_stale_token_intents(
        &self,
        tenant_id: &TenantId,
        storage_volume_id: &StorageVolumeId,
        edge_cluster_id: &EdgeClusterId,
        pvc_identity_digest: &PvcIdentityDigest,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<usize> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let rows = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, \
             pvc_binding_role, resource_version, payload FROM agent_registry_records \
             WHERE (tenant_id = ? AND storage_volume_id = ?) \
                OR (edge_cluster_id = ? AND pvc_identity_digest = ?)",
        )
        .bind(tenant_id.as_str())
        .bind(storage_volume_id.as_str())
        .bind(edge_cluster_id.as_str())
        .bind(pvc_identity_digest.as_digest().to_string())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let mut expired = 0;
        for row in rows {
            let mut record = decode_row(row)?;
            if record.enrollment.state != AgentEnrollmentState::TokenIssued
                || record.enrollment.expires_at_unix_ms.get() > now_unix_ms.get()
            {
                continue;
            }
            let previous = record.resource_version.get();
            let next = checked_next_registry_resource_version(previous)?;
            record.enrollment.state = AgentEnrollmentState::Expired;
            record.resource_version = ResourceVersion::new(next);
            validate_registry_record(&record)?;
            let payload = encode(&record)?;
            let result = sqlx::query(
                "UPDATE agent_registry_records SET pvc_binding_role = NULL, \
                 resource_version = ?, payload = ? \
                 WHERE enrollment_id = ? AND resource_version = ?",
            )
            .bind(next.to_string())
            .bind(payload)
            .bind(record.enrollment.enrollment_id.as_str())
            .bind(previous.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(registry_update_error)?;
            if result.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "Agent token intent changed during expiry reconciliation",
                ));
            }
            expired += 1;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(expired)
    }

    async fn enrollment_audit_events(
        &self,
    ) -> CentralResult<Vec<crate::AgentEnrollmentAuditEvent>> {
        let rows = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records ORDER BY tenant_id, enrollment_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut events = rows
            .into_iter()
            .map(decode_row)
            .collect::<CentralResult<Vec<_>>>()?
            .into_iter()
            .filter_map(|record| record.decision_audit_event)
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            (&left.tenant_id, &left.event_id).cmp(&(&right.tenant_id, &right.event_id))
        });
        Ok(events)
    }

    async fn insert_or_load(
        &self,
        record: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryInsertOutcome> {
        validate_registry_insert(&record)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let existing = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE enrollment_id = ?",
        )
        .bind(record.enrollment.enrollment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(decode_row)
        .transpose()?;
        if let Some(existing) = existing {
            return Ok(AgentRegistryInsertOutcome::Existing(existing));
        }
        let conflicts = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, \
             resource_version, payload FROM agent_registry_records \
             WHERE agent_id = ? OR token_id = ? \
                OR (tenant_id = ? AND token_request_id = ?) OR token_digest = ? \
                OR (tenant_id = ? AND storage_volume_id = ?) \
                OR (edge_cluster_id = ? AND pvc_identity_digest = ?)",
        )
        .bind(record.enrollment.reserved_agent_id.as_str())
        .bind(record.enrollment.token_id.as_str())
        .bind(record.enrollment.tenant_id.as_str())
        .bind(record.enrollment.token_request_id.as_str())
        .bind(record.enrollment.bootstrap_token_digest.to_string())
        .bind(record.enrollment.tenant_id.as_str())
        .bind(record.enrollment.storage_volume_id.as_str())
        .bind(record.enrollment.edge_cluster_id.as_str())
        .bind(
            record
                .enrollment
                .pvc_identity_digest
                .as_digest()
                .to_string(),
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        for row in conflicts {
            let existing = decode_row(row)?;
            if existing.enrollment.tenant_id == record.enrollment.tenant_id
                && existing.enrollment.token_request_id == record.enrollment.token_request_id
            {
                return Ok(AgentRegistryInsertOutcome::Existing(existing));
            }
            if identity_conflicts(&existing, &record)
                || volume_intent_conflicts(&existing, &record)
                || pvc_identity_conflicts(&existing, &record)
            {
                return Err(CentralError::new(
                    CentralErrorCode::VolumeOwnerConflict,
                    "Agent identity or Tenant Volume already has an active enrollment",
                )
                .with_retryable(false));
            }
        }
        let payload = encode(&record)?;
        let inserted = sqlx::query(
            "INSERT INTO agent_registry_records \
             (enrollment_id, agent_id, token_id, token_request_id, token_digest, tenant_id, \
              edge_cluster_id, storage_volume_id, pvc_identity_digest, bootstrap_request_id, \
              pvc_binding_role, decision_request_id, installation_id, public_key_fingerprint, \
              resource_version, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.enrollment.enrollment_id.as_str())
        .bind(record.enrollment.reserved_agent_id.as_str())
        .bind(record.enrollment.token_id.as_str())
        .bind(record.enrollment.token_request_id.as_str())
        .bind(record.enrollment.bootstrap_token_digest.to_string())
        .bind(record.enrollment.tenant_id.as_str())
        .bind(record.enrollment.edge_cluster_id.as_str())
        .bind(record.enrollment.storage_volume_id.as_str())
        .bind(
            record
                .enrollment
                .pvc_identity_digest
                .as_digest()
                .to_string(),
        )
        .bind(optional_request_id(
            record.enrollment.bootstrap_request_id.as_ref(),
        ))
        .bind(pvc_binding_role(&record))
        .bind(decision_request_id(&record))
        .bind(candidate_installation_id(&record))
        .bind(candidate_public_key_fingerprint(&record))
        .bind(record.resource_version.get().to_string())
        .bind(payload)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = inserted {
            if error
                .as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
            {
                transaction.rollback().await.map_err(storage_error)?;
                return resolve_unique_insert_conflict(&self.pool, &record).await;
            }
            return Err(storage_error(error));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(AgentRegistryInsertOutcome::Inserted(record))
    }

    async fn replace(
        &self,
        expected_resource_version: u64,
        record: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryRecord> {
        validate_registry_record(&record)?;
        let stored = self
            .get(&record.enrollment.enrollment_id)
            .await?
            .ok_or_else(|| missing("Agent enrollment disappeared during update"))?;
        let next_resource_version =
            checked_next_registry_resource_version(expected_resource_version)?;
        if stored.resource_version.get() != expected_resource_version
            || record.resource_version.get() != next_resource_version
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Agent registry ResourceVersion changed",
            ));
        }
        ensure_immutable_registry_scope(&stored, &record)?;
        validate_registry_replace_transition(&stored, &record)?;
        let payload = encode(&record)?;
        let result = sqlx::query(
            "UPDATE agent_registry_records SET bootstrap_request_id = ?, decision_request_id = ?, \
             installation_id = ?, public_key_fingerprint = ?, pvc_binding_role = ?, \
             resource_version = ?, payload = ? \
             WHERE enrollment_id = ? AND agent_id = ? AND resource_version = ?",
        )
        .bind(optional_request_id(
            record.enrollment.bootstrap_request_id.as_ref(),
        ))
        .bind(decision_request_id(&record))
        .bind(candidate_installation_id(&record))
        .bind(candidate_public_key_fingerprint(&record))
        .bind(pvc_binding_role(&record))
        .bind(record.resource_version.get().to_string())
        .bind(payload)
        .bind(record.enrollment.enrollment_id.as_str())
        .bind(record.enrollment.reserved_agent_id.as_str())
        .bind(expected_resource_version.to_string())
        .execute(&self.pool)
        .await
        .map_err(registry_update_error)?;
        if result.rows_affected() != 1 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Agent registry ResourceVersion changed",
            ));
        }
        Ok(record)
    }

    async fn activate_replacement(
        &self,
        expected_previous_resource_version: u64,
        revoked: AgentRegistryRecord,
        expected_replacement_resource_version: u64,
        replacement: AgentRegistryRecord,
    ) -> CentralResult<AgentRegistryReplacementRecords> {
        validate_registry_record(&revoked)?;
        validate_registry_record(&replacement)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let previous_row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE enrollment_id = ?",
        )
        .bind(revoked.enrollment.enrollment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| missing("replacement target enrollment is missing"))?;
        let replacement_row = sqlx::query(
            "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
             tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
             FROM agent_registry_records WHERE enrollment_id = ?",
        )
        .bind(replacement.enrollment.enrollment_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| missing("replacement enrollment is missing"))?;
        let stored_previous = decode_row(previous_row)?;
        let stored_replacement = decode_row(replacement_row)?;
        validate_registry_replacement_transition(
            &stored_previous,
            expected_previous_resource_version,
            &revoked,
            &stored_replacement,
            expected_replacement_resource_version,
            &replacement,
        )?;
        ensure_immutable_registry_scope(&stored_previous, &revoked)?;
        ensure_immutable_registry_scope(&stored_replacement, &replacement)?;

        let revoked_payload = encode(&revoked)?;
        let revoked_result = sqlx::query(
            "UPDATE agent_registry_records SET bootstrap_request_id = ?, decision_request_id = ?, \
             installation_id = ?, public_key_fingerprint = ?, pvc_binding_role = ?, \
             resource_version = ?, payload = ? \
             WHERE enrollment_id = ? AND agent_id = ? AND resource_version = ?",
        )
        .bind(optional_request_id(
            revoked.enrollment.bootstrap_request_id.as_ref(),
        ))
        .bind(decision_request_id(&revoked))
        .bind(candidate_installation_id(&revoked))
        .bind(candidate_public_key_fingerprint(&revoked))
        .bind(pvc_binding_role(&revoked))
        .bind(revoked.resource_version.get().to_string())
        .bind(revoked_payload)
        .bind(revoked.enrollment.enrollment_id.as_str())
        .bind(revoked.enrollment.reserved_agent_id.as_str())
        .bind(expected_previous_resource_version.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(registry_update_error)?;
        let replacement_payload = encode(&replacement)?;
        let replacement_result = sqlx::query(
            "UPDATE agent_registry_records SET bootstrap_request_id = ?, decision_request_id = ?, \
             installation_id = ?, public_key_fingerprint = ?, pvc_binding_role = ?, \
             resource_version = ?, payload = ? \
             WHERE enrollment_id = ? AND agent_id = ? AND resource_version = ?",
        )
        .bind(optional_request_id(
            replacement.enrollment.bootstrap_request_id.as_ref(),
        ))
        .bind(decision_request_id(&replacement))
        .bind(candidate_installation_id(&replacement))
        .bind(candidate_public_key_fingerprint(&replacement))
        .bind(pvc_binding_role(&replacement))
        .bind(replacement.resource_version.get().to_string())
        .bind(replacement_payload)
        .bind(replacement.enrollment.enrollment_id.as_str())
        .bind(replacement.enrollment.reserved_agent_id.as_str())
        .bind(expected_replacement_resource_version.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(registry_update_error)?;
        if revoked_result.rows_affected() != 1 || replacement_result.rows_affected() != 1 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "Agent replacement records changed before atomic approval",
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(AgentRegistryReplacementRecords {
            revoked,
            replacement,
        })
    }
}

fn optional_request_id(request_id: Option<&RequestId>) -> Option<&str> {
    request_id.map(RequestId::as_str)
}

fn decision_request_id(record: &AgentRegistryRecord) -> Option<&str> {
    record
        .enrollment
        .decision_request
        .as_ref()
        .map(|request| request.decision_request_id.as_str())
}

fn candidate_installation_id(record: &AgentRegistryRecord) -> Option<&str> {
    record
        .candidate
        .as_ref()
        .map(|candidate| candidate.installation_id.as_str())
}

fn candidate_public_key_fingerprint(record: &AgentRegistryRecord) -> Option<String> {
    record
        .candidate
        .as_ref()
        .map(|candidate| candidate.public_key_fingerprint.to_string())
}

fn pvc_binding_role(record: &AgentRegistryRecord) -> Option<&'static str> {
    if is_terminal(record.enrollment.state) {
        None
    } else if record.enrollment.state == AgentEnrollmentState::Approved
        || record.enrollment.replaces_enrollment_id.is_none()
    {
        Some("owner")
    } else {
        Some("replacement")
    }
}

fn registry_update_error(error: sqlx::Error) -> CentralError {
    if let Some(database_error) = error.as_database_error() {
        if database_error.is_unique_violation() {
            let message = database_error.message();
            let (code, explanation) = if message.contains("pvc_binding_role") {
                (
                    CentralErrorCode::VolumeOwnerConflict,
                    "Agent registry Volume or PVC binding is already active",
                )
            } else if message.contains("installation_id")
                || message.contains("public_key_fingerprint")
            {
                (
                    CentralErrorCode::AgentIdentityMismatch,
                    "Agent candidate identity is already bound to another enrollment",
                )
            } else if message.contains("decision_request_id") {
                (
                    CentralErrorCode::EnrollmentDecisionConflict,
                    "Agent decision request identity is already bound to another enrollment",
                )
            } else {
                (
                    CentralErrorCode::EnrollmentIdReused,
                    "Agent registry request identity is already bound to another enrollment",
                )
            };
            return CentralError::new(code, explanation).with_retryable(false);
        }
    }
    storage_error(error)
}

async fn resolve_unique_insert_conflict(
    pool: &SqlitePool,
    record: &AgentRegistryRecord,
) -> CentralResult<AgentRegistryInsertOutcome> {
    let bootstrap_request_id = optional_request_id(record.enrollment.bootstrap_request_id.as_ref());
    let decision_request_id = decision_request_id(record);
    let installation_id = candidate_installation_id(record);
    let public_key_fingerprint = candidate_public_key_fingerprint(record);
    let rows = sqlx::query(
        "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
         tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, pvc_binding_role, resource_version, payload \
         FROM agent_registry_records WHERE enrollment_id = ? OR agent_id = ? OR token_id = ? \
         OR (tenant_id = ? AND token_request_id = ?) OR token_digest = ? \
         OR (tenant_id = ? AND bootstrap_request_id = ?) \
         OR (tenant_id = ? AND decision_request_id = ?) \
         OR installation_id = ? OR public_key_fingerprint = ? \
         OR (tenant_id = ? AND storage_volume_id = ?) \
         OR (edge_cluster_id = ? AND pvc_identity_digest = ?)",
    )
    .bind(record.enrollment.enrollment_id.as_str())
    .bind(record.enrollment.reserved_agent_id.as_str())
    .bind(record.enrollment.token_id.as_str())
    .bind(record.enrollment.tenant_id.as_str())
    .bind(record.enrollment.token_request_id.as_str())
    .bind(record.enrollment.bootstrap_token_digest.to_string())
    .bind(record.enrollment.tenant_id.as_str())
    .bind(bootstrap_request_id)
    .bind(record.enrollment.tenant_id.as_str())
    .bind(decision_request_id)
    .bind(installation_id)
    .bind(public_key_fingerprint)
    .bind(record.enrollment.tenant_id.as_str())
    .bind(record.enrollment.storage_volume_id.as_str())
    .bind(record.enrollment.edge_cluster_id.as_str())
    .bind(
        record
            .enrollment
            .pvc_identity_digest
            .as_digest()
            .to_string(),
    )
    .fetch_all(pool)
    .await
    .map_err(storage_error)?;
    let conflicts = rows
        .into_iter()
        .map(decode_row)
        .collect::<CentralResult<Vec<_>>>()?;

    if let Some(existing) = conflicts.iter().find(|existing| *existing == record) {
        return Ok(AgentRegistryInsertOutcome::Existing(existing.clone()));
    }
    if let Some(existing) = conflicts.iter().find(|existing| {
        existing.enrollment.enrollment_id == record.enrollment.enrollment_id
            || (existing.enrollment.tenant_id == record.enrollment.tenant_id
                && existing.enrollment.token_request_id == record.enrollment.token_request_id)
    }) {
        return Ok(AgentRegistryInsertOutcome::Existing(existing.clone()));
    }
    if conflicts.iter().any(|existing| {
        identity_conflicts(existing, record)
            || volume_intent_conflicts(existing, record)
            || pvc_identity_conflicts(existing, record)
    }) {
        return Err(CentralError::new(
            CentralErrorCode::VolumeOwnerConflict,
            "Agent identity or Tenant Volume already has an active enrollment",
        )
        .with_retryable(false));
    }
    if conflicts
        .iter()
        .any(|existing| bootstrap_request_identity_conflicts(existing, record))
    {
        return Err(CentralError::new(
            CentralErrorCode::EnrollmentIdReused,
            "Agent bootstrap request identity is already bound to another enrollment",
        )
        .with_retryable(false));
    }
    if conflicts
        .iter()
        .any(|existing| decision_request_identity_conflicts(existing, record))
    {
        return Err(CentralError::new(
            CentralErrorCode::EnrollmentDecisionConflict,
            "Agent decision request identity is already bound to another enrollment",
        )
        .with_retryable(false));
    }
    if conflicts
        .iter()
        .any(|existing| candidate_identity_conflicts(existing, record))
    {
        return Err(CentralError::new(
            CentralErrorCode::AgentIdentityMismatch,
            "Agent candidate identity is already bound to another enrollment",
        )
        .with_retryable(false));
    }
    Err(storage_corruption(
        "unique Agent enrollment conflict disappeared before reload",
    ))
}

fn identity_conflicts(existing: &AgentRegistryRecord, candidate: &AgentRegistryRecord) -> bool {
    existing.enrollment.reserved_agent_id == candidate.enrollment.reserved_agent_id
        || existing.enrollment.token_id == candidate.enrollment.token_id
        || (existing.enrollment.tenant_id == candidate.enrollment.tenant_id
            && existing.enrollment.token_request_id == candidate.enrollment.token_request_id)
        || existing.enrollment.bootstrap_token_digest == candidate.enrollment.bootstrap_token_digest
}

fn bootstrap_request_identity_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    if existing.enrollment.tenant_id != candidate.enrollment.tenant_id {
        return false;
    }
    existing.enrollment.bootstrap_request_id.is_some()
        && existing.enrollment.bootstrap_request_id == candidate.enrollment.bootstrap_request_id
}

fn decision_request_identity_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    if existing.enrollment.tenant_id != candidate.enrollment.tenant_id {
        return false;
    }
    existing
        .enrollment
        .decision_request
        .as_ref()
        .zip(candidate.enrollment.decision_request.as_ref())
        .is_some_and(|(left, right)| left.decision_request_id == right.decision_request_id)
}

fn candidate_identity_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    existing
        .candidate
        .as_ref()
        .zip(candidate.candidate.as_ref())
        .is_some_and(|(left, right)| {
            left.installation_id == right.installation_id
                || left.public_key_fingerprint == right.public_key_fingerprint
        })
}

fn volume_intent_conflicts(
    existing: &AgentRegistryRecord,
    candidate: &AgentRegistryRecord,
) -> bool {
    if existing.enrollment.tenant_id != candidate.enrollment.tenant_id
        || existing.enrollment.storage_volume_id != candidate.enrollment.storage_volume_id
        || matches!(
            existing.enrollment.state,
            AgentEnrollmentState::Rejected
                | AgentEnrollmentState::Expired
                | AgentEnrollmentState::Revoked
        )
    {
        return false;
    }
    candidate.enrollment.replaces_enrollment_id.as_ref() != Some(&existing.enrollment.enrollment_id)
        || existing.enrollment.state != AgentEnrollmentState::Approved
}

fn pvc_identity_conflicts(existing: &AgentRegistryRecord, candidate: &AgentRegistryRecord) -> bool {
    if existing.enrollment.edge_cluster_id != candidate.enrollment.edge_cluster_id
        || existing.enrollment.pvc_identity_digest != candidate.enrollment.pvc_identity_digest
        || matches!(
            existing.enrollment.state,
            AgentEnrollmentState::Rejected
                | AgentEnrollmentState::Expired
                | AgentEnrollmentState::Revoked
        )
    {
        return false;
    }
    existing.enrollment.tenant_id != candidate.enrollment.tenant_id
        || existing.enrollment.storage_volume_id != candidate.enrollment.storage_volume_id
        || candidate.enrollment.replaces_enrollment_id.as_ref()
            != Some(&existing.enrollment.enrollment_id)
        || existing.enrollment.state != AgentEnrollmentState::Approved
}

fn pvc_binding(record: &AgentRegistryRecord) -> PvcVolumeBinding {
    PvcVolumeBinding {
        pvc_identity_digest: record.enrollment.pvc_identity_digest,
        tenant_id: record.enrollment.tenant_id.clone(),
        edge_cluster_id: record.enrollment.edge_cluster_id.clone(),
        storage_volume_id: record.enrollment.storage_volume_id.clone(),
    }
}

fn is_terminal(state: AgentEnrollmentState) -> bool {
    matches!(
        state,
        AgentEnrollmentState::Rejected
            | AgentEnrollmentState::Expired
            | AgentEnrollmentState::Revoked
    )
}

fn encode(record: &AgentRegistryRecord) -> CentralResult<Vec<u8>> {
    serde_json::to_vec(&StoredV1 {
        format: STORED_FORMAT_VERSION,
        value: record,
    })
    .map_err(|error| storage_corruption(format!("failed to encode Agent registry record: {error}")))
}

fn decode_row(row: SqliteRow) -> CentralResult<AgentRegistryRecord> {
    let payload: Vec<u8> = row.try_get("payload").map_err(storage_error)?;
    let stored: StoredV1<AgentRegistryRecord> =
        serde_json::from_slice(&payload).map_err(|error| {
            storage_corruption(format!("Agent registry record is invalid JSON: {error}"))
        })?;
    if stored.format != STORED_FORMAT_VERSION {
        return Err(storage_corruption(format!(
            "Agent registry record format {} is unsupported",
            stored.format
        )));
    }
    validate_registry_record(&stored.value)?;
    let enrollment_id: String = row.try_get("enrollment_id").map_err(storage_error)?;
    let agent_id: String = row.try_get("agent_id").map_err(storage_error)?;
    let token_id: String = row.try_get("token_id").map_err(storage_error)?;
    let token_request_id: String = row.try_get("token_request_id").map_err(storage_error)?;
    let token_digest: String = row.try_get("token_digest").map_err(storage_error)?;
    let indexed_bootstrap_request_id: Option<String> =
        row.try_get("bootstrap_request_id").map_err(storage_error)?;
    let indexed_decision_request_id: Option<String> =
        row.try_get("decision_request_id").map_err(storage_error)?;
    let indexed_installation_id: Option<String> =
        row.try_get("installation_id").map_err(storage_error)?;
    let indexed_public_key_fingerprint: Option<String> = row
        .try_get("public_key_fingerprint")
        .map_err(storage_error)?;
    let tenant_id: String = row.try_get("tenant_id").map_err(storage_error)?;
    let edge_cluster_id: String = row.try_get("edge_cluster_id").map_err(storage_error)?;
    let storage_volume_id: String = row.try_get("storage_volume_id").map_err(storage_error)?;
    let pvc_identity_digest: String = row.try_get("pvc_identity_digest").map_err(storage_error)?;
    let indexed_pvc_binding_role: Option<String> =
        row.try_get("pvc_binding_role").map_err(storage_error)?;
    let resource_version: String = row.try_get("resource_version").map_err(storage_error)?;
    if enrollment_id != stored.value.enrollment.enrollment_id.as_str()
        || agent_id != stored.value.enrollment.reserved_agent_id.as_str()
        || token_id != stored.value.enrollment.token_id.as_str()
        || token_request_id != stored.value.enrollment.token_request_id.as_str()
        || token_digest != stored.value.enrollment.bootstrap_token_digest.to_string()
        || indexed_bootstrap_request_id.as_deref()
            != optional_request_id(stored.value.enrollment.bootstrap_request_id.as_ref())
        || indexed_decision_request_id.as_deref() != decision_request_id(&stored.value)
        || indexed_installation_id.as_deref() != candidate_installation_id(&stored.value)
        || indexed_public_key_fingerprint != candidate_public_key_fingerprint(&stored.value)
        || tenant_id != stored.value.enrollment.tenant_id.as_str()
        || edge_cluster_id != stored.value.enrollment.edge_cluster_id.as_str()
        || storage_volume_id != stored.value.enrollment.storage_volume_id.as_str()
        || pvc_identity_digest
            != stored
                .value
                .enrollment
                .pvc_identity_digest
                .as_digest()
                .to_string()
        || indexed_pvc_binding_role.as_deref() != pvc_binding_role(&stored.value)
        || resource_version != stored.value.resource_version.get().to_string()
    {
        return Err(storage_corruption(
            "Agent registry indexed columns differ from the stored aggregate",
        ));
    }
    Ok(stored.value)
}

async fn initialize_or_validate(pool: &SqlitePool, initialize: bool) -> CentralResult<()> {
    let application_id: i64 = sqlx::query_scalar("PRAGMA application_id")
        .fetch_one(pool)
        .await
        .map_err(storage_error)?;
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(pool)
        .await
        .map_err(storage_error)?;
    if initialize {
        if application_id != 0 || user_version != 0 {
            return Err(storage_corruption(
                "empty Agent registry carries unexpected schema identity",
            ));
        }
        let mut transaction = pool.begin().await.map_err(storage_error)?;
        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("PRAGMA application_id = 1313165138")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("PRAGMA user_version = 2")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        return Ok(());
    }
    if application_id != SQLITE_APPLICATION_ID || user_version != SQLITE_SCHEMA_VERSION {
        return Err(storage_corruption(format!(
            "unsupported Agent registry schema identity {application_id}/{user_version}"
        )));
    }
    validate_current_schema(pool).await
}

async fn validate_current_schema(pool: &SqlitePool) -> CentralResult<()> {
    let mut expected_objects = BTreeSet::new();
    for expected_statement in SCHEMA_SQL
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        let (object_type, name) = schema_object(expected_statement)?;
        expected_objects.insert((object_type, name));
        let stored: Option<String> =
            sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?")
                .bind(object_type)
                .bind(name)
                .fetch_optional(pool)
                .await
                .map_err(storage_error)?;
        let stored = stored.ok_or_else(|| {
            storage_corruption(format!("Agent registry {object_type} {name} is missing"))
        })?;
        if normalize_schema_sql(&stored) != normalize_schema_sql(expected_statement) {
            return Err(storage_corruption(format!(
                "Agent registry {object_type} {name} differs from the current schema"
            )));
        }
    }

    let actual_objects = sqlx::query_as::<_, (String, String)>(
        "SELECT type, name FROM sqlite_schema \
         WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' \
         ORDER BY type, name",
    )
    .fetch_all(pool)
    .await
    .map_err(storage_error)?
    .into_iter()
    .collect::<BTreeSet<_>>();
    let expected_objects = expected_objects
        .into_iter()
        .map(|(object_type, name)| (object_type.to_owned(), name.to_owned()))
        .collect::<BTreeSet<_>>();
    if actual_objects != expected_objects {
        return Err(storage_corruption(
            "Agent registry table or index set differs from the current schema",
        ));
    }
    Ok(())
}

fn schema_object(statement: &str) -> CentralResult<(&'static str, &str)> {
    let words = statement.split_ascii_whitespace().collect::<Vec<_>>();
    match words.as_slice() {
        ["CREATE", "TABLE", name, ..] => Ok(("table", name)),
        ["CREATE", "INDEX", name, ..] | ["CREATE", "UNIQUE", "INDEX", name, ..] => {
            Ok(("index", name))
        }
        _ => Err(storage_corruption(
            "invalid embedded Agent registry schema statement",
        )),
    }
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect()
}

async fn validate_integrity(pool: &SqlitePool) -> CentralResult<()> {
    let results: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(pool)
        .await
        .map_err(storage_error)?;
    if results != ["ok"] {
        return Err(storage_corruption(format!(
            "Agent registry integrity check failed: {}",
            results.join("; ")
        )));
    }
    let rows = sqlx::query(
        "SELECT enrollment_id, agent_id, token_id, token_request_id, token_digest, bootstrap_request_id, decision_request_id, installation_id, public_key_fingerprint, \
         tenant_id, edge_cluster_id, storage_volume_id, pvc_identity_digest, \
         pvc_binding_role, resource_version, payload FROM agent_registry_records",
    )
    .fetch_all(pool)
    .await
    .map_err(storage_error)?;
    for row in rows {
        let _ = decode_row(row)?;
    }
    Ok(())
}

fn prepare_root(path: &Path) -> CentralResult<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(storage_corruption("Agent registry path must be explicit"));
    }
    if path.exists() {
        let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(storage_corruption(
                "Agent registry path must be a real directory",
            ));
        }
    } else {
        fs::create_dir_all(path).map_err(storage_error)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(storage_error)?;
    }
    fs::canonicalize(path).map_err(storage_error)
}

fn open_lock(path: &Path) -> CentralResult<File> {
    if path.exists()
        && fs::symlink_metadata(path)
            .map_err(storage_error)?
            .file_type()
            .is_symlink()
    {
        return Err(storage_corruption(
            "Agent registry lock cannot be a symbolic link",
        ));
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(storage_error)?;
    file.try_lock_exclusive().map_err(|_error| {
        CentralError::new(
            CentralErrorCode::StorageFailure,
            "Agent registry is already open",
        )
    })?;
    secure_file(path)?;
    Ok(file)
}

fn validate_database_path(path: &Path) -> CentralResult<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(storage_corruption(
            "Agent registry database must be a regular file",
        ));
    }
    Ok(())
}

fn secure_file(path: &Path) -> CentralResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(storage_error)?;
    }
    Ok(())
}

fn storage_error(_error: impl std::fmt::Display) -> CentralError {
    CentralError::new(
        CentralErrorCode::StorageFailure,
        "Agent registry storage operation failed",
    )
}

fn missing(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::EnrollmentNotFound, message).with_retryable(false)
}

fn storage_corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}
