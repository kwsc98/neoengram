use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use fs2::FileExt;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    SqlitePool,
};

use crate::{CentralError, CentralErrorCode, CentralResult};

const DATABASE_FILE_NAME: &str = "agent-registry.sqlite3";
const LOCK_FILE_NAME: &str = "agent-registry.lock";
const SQLITE_APPLICATION_ID: i64 = 0x4e45_4f52;
const SQLITE_SCHEMA_VERSION: i64 = 3;

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
    payload BLOB NOT NULL,
    enrollment_state TEXT NOT NULL DEFAULT 'legacy' CHECK (
        enrollment_state IN (
            'legacy', 'token_issued', 'pending_approval', 'approved', 'enrolled',
            'rejected', 'expired', 'revoked'
        )
    ),
    registration_kind TEXT NOT NULL DEFAULT 'legacy' CHECK (
        registration_kind IN ('legacy', 'initial', 'replacement')
    ),
    enrollment_created_at_unix_ms INTEGER NOT NULL DEFAULT 0 CHECK (
        enrollment_created_at_unix_ms >= 0
    ),
    display_name TEXT
) STRICT;
CREATE TABLE agent_bootstrap_status_watermarks (
    enrollment_id TEXT NOT NULL PRIMARY KEY
        REFERENCES agent_registry_records(enrollment_id) ON DELETE CASCADE,
    signed_at_unix_ms INTEGER NOT NULL CHECK (signed_at_unix_ms >= 0)
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
CREATE INDEX agent_registry_tenant_enrollment_keyset
    ON agent_registry_records (
        tenant_id, enrollment_created_at_unix_ms DESC, enrollment_id ASC
    );
CREATE INDEX agent_registry_tenant_status_keyset
    ON agent_registry_records (
        tenant_id, enrollment_state, registration_kind,
        enrollment_created_at_unix_ms DESC, enrollment_id ASC
    );
"#;

pub(crate) struct SqliteAgentRegistryDataSource {
    pool: SqlitePool,
    lock: Mutex<Option<File>>,
}

impl SqliteAgentRegistryDataSource {
    pub(crate) async fn open(path: &Path, busy_timeout: Duration) -> CentralResult<Self> {
        let root = prepare_root(path)?;
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
            .busy_timeout(busy_timeout);
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(storage_error)?;
        initialize_or_validate(&pool, initialize).await?;
        secure_file(&database)?;
        validate_integrity(&pool).await?;

        Ok(Self {
            pool,
            lock: Mutex::new(Some(lock)),
        })
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub(crate) async fn readiness_check(&self) -> CentralResult<()> {
        let _: i64 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    pub(crate) async fn integrity_check(&self) -> CentralResult<()> {
        validate_current_schema(&self.pool).await?;
        validate_integrity(&self.pool).await
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
        drop(
            self.lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
    }
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
        sqlx::query("PRAGMA user_version = 3")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        return Ok(());
    }
    if application_id != SQLITE_APPLICATION_ID {
        return Err(storage_corruption(format!(
            "unsupported Agent registry schema identity {application_id}/{user_version}"
        )));
    }
    match user_version {
        2 => migrate_v2_to_v3(pool).await?,
        SQLITE_SCHEMA_VERSION => {}
        _ => {
            return Err(storage_corruption(format!(
                "unsupported Agent registry schema identity {application_id}/{user_version}"
            )));
        }
    }
    validate_current_schema(pool).await
}

async fn migrate_v2_to_v3(pool: &SqlitePool) -> CentralResult<()> {
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    sqlx::raw_sql(
        "ALTER TABLE agent_registry_records ADD COLUMN enrollment_state TEXT NOT NULL \
             DEFAULT 'legacy' CHECK (enrollment_state IN (\
                 'legacy', 'token_issued', 'pending_approval', 'approved', 'enrolled', \
                 'rejected', 'expired', 'revoked'\
             ));
         ALTER TABLE agent_registry_records ADD COLUMN registration_kind TEXT NOT NULL \
             DEFAULT 'legacy' CHECK (registration_kind IN ('legacy', 'initial', 'replacement'));
         ALTER TABLE agent_registry_records ADD COLUMN enrollment_created_at_unix_ms INTEGER \
             NOT NULL DEFAULT 0 CHECK (enrollment_created_at_unix_ms >= 0);
         ALTER TABLE agent_registry_records ADD COLUMN display_name TEXT;
         CREATE TABLE agent_bootstrap_status_watermarks (
             enrollment_id TEXT NOT NULL PRIMARY KEY
                 REFERENCES agent_registry_records(enrollment_id) ON DELETE CASCADE,
             signed_at_unix_ms INTEGER NOT NULL CHECK (signed_at_unix_ms >= 0)
         ) STRICT;
         CREATE INDEX agent_registry_tenant_enrollment_keyset
             ON agent_registry_records (
                 tenant_id, enrollment_created_at_unix_ms DESC, enrollment_id ASC
             );
         CREATE INDEX agent_registry_tenant_status_keyset
             ON agent_registry_records (
                 tenant_id, enrollment_state, registration_kind,
                 enrollment_created_at_unix_ms DESC, enrollment_id ASC
             );
         PRAGMA user_version = 3;",
    )
    .execute(&mut *transaction)
    .await
    .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)
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
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(pool)
        .await
        .map_err(storage_error)?;
    if !violations.is_empty() {
        return Err(storage_corruption(
            "Agent registry foreign-key check failed",
        ));
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

fn storage_corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}
