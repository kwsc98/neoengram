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

const DATABASE_FILE_NAME: &str = "authority.sqlite3";
const LOCK_FILE_NAME: &str = "authority.lock";
const SQLITE_APPLICATION_ID: i64 = 0x4e45_4f41;
const SQLITE_SCHEMA_VERSION: i64 = 2;

const V1_TABLES: &[&str] = &[
    "control_jobs",
    "assignment_outbox",
    "metadata_batch_descriptors",
    "metadata_batch_pages",
    "durable_objects",
    "playground_indexes",
    "playground_index_records",
    "immutable_manifests",
    "index_publications",
    "audit_events",
];

const V2_TABLES: &[&str] = &[
    "control_jobs",
    "assignment_outbox",
    "metadata_batch_descriptors",
    "metadata_batch_pages",
    "durable_objects",
    "playground_indexes",
    "playground_index_records",
    "immutable_manifests",
    "index_publications",
    "audit_events",
    "agent_enrollment_audit_events",
];

const ENROLLMENT_AUDIT_SCHEMA_SQL: &str = r#"
CREATE TABLE agent_enrollment_audit_events (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    occurred_at TEXT NOT NULL CHECK (occurred_at <> '' AND occurred_at NOT GLOB '*[^0-9]*'),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, event_id)
) STRICT;
"#;

const SCHEMA_SQL: &str = r#"
CREATE TABLE control_jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
) STRICT;

CREATE TABLE assignment_outbox (
    tenant_id TEXT NOT NULL,
    assignment_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    published INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0, 1)),
    PRIMARY KEY (tenant_id, assignment_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES control_jobs (tenant_id, job_id)
) STRICT;

CREATE TABLE metadata_batch_descriptors (
    tenant_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, batch_id)
) STRICT;

CREATE TABLE metadata_batch_pages (
    tenant_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    page_number INTEGER NOT NULL CHECK (page_number >= 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, batch_id, page_number),
    FOREIGN KEY (tenant_id, batch_id)
        REFERENCES metadata_batch_descriptors (tenant_id, batch_id)
        ON DELETE CASCADE
) STRICT;

CREATE TABLE durable_objects (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    size TEXT NOT NULL CHECK (size <> '' AND size NOT GLOB '*[^0-9]*'),
    verified_digest BLOB NOT NULL CHECK (length(verified_digest) = 32),
    storage_version TEXT NOT NULL CHECK (storage_version <> ''),
    PRIMARY KEY (tenant_id, artifact_id, object_id)
) STRICT;

CREATE TABLE playground_indexes (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    revision TEXT NOT NULL CHECK (revision <> '' AND revision NOT GLOB '*[^0-9]*'),
    digest BLOB NOT NULL CHECK (length(digest) = 32),
    PRIMARY KEY (tenant_id, artifact_id, playground_id)
) STRICT;

CREATE TABLE playground_index_records (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    path TEXT NOT NULL,
    manifest_id BLOB NOT NULL CHECK (length(manifest_id) = 32),
    total_size TEXT NOT NULL CHECK (total_size <> '' AND total_size NOT GLOB '*[^0-9]*'),
    chunk_count TEXT NOT NULL CHECK (chunk_count <> '' AND chunk_count NOT GLOB '*[^0-9]*'),
    PRIMARY KEY (tenant_id, artifact_id, playground_id, path),
    FOREIGN KEY (tenant_id, artifact_id, playground_id)
        REFERENCES playground_indexes (tenant_id, artifact_id, playground_id)
        ON DELETE CASCADE
) STRICT;

CREATE TABLE immutable_manifests (
    tenant_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    manifest_id BLOB NOT NULL CHECK (length(manifest_id) = 32),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, artifact_id, manifest_id)
) STRICT;

CREATE TABLE index_publications (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    request_payload BLOB NOT NULL,
    outcome_payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
) STRICT;

CREATE TABLE audit_events (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    occurred_at TEXT NOT NULL CHECK (occurred_at <> '' AND occurred_at NOT GLOB '*[^0-9]*'),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, event_id)
) STRICT;

CREATE TABLE agent_enrollment_audit_events (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    occurred_at TEXT NOT NULL CHECK (occurred_at <> '' AND occurred_at NOT GLOB '*[^0-9]*'),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, event_id)
) STRICT;
"#;

pub(crate) struct SqliteAuthorityDataSource {
    pool: SqlitePool,
    lock: Mutex<Option<File>>,
}

impl SqliteAuthorityDataSource {
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
        secure_database_file(&database)?;
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

fn prepare_root(path: &Path) -> CentralResult<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(storage_corruption("SQLite authority path must be explicit"));
    }
    if path.exists() {
        let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(storage_corruption(
                "SQLite authority path must be a real directory",
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
            "SQLite authority lock cannot be a symbolic link",
        ));
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(storage_error)?;
    file.try_lock_exclusive().map_err(|error| {
        CentralError::new(
            CentralErrorCode::StorageFailure,
            format!("SQLite authority is already open: {error}"),
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(storage_error)?;
    }
    Ok(file)
}

fn validate_database_path(path: &Path) -> CentralResult<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(storage_corruption(
            "SQLite authority database must be a regular file, not a symbolic link",
        ));
    }
    Ok(())
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
                "empty SQLite authority database carries unexpected schema identity",
            ));
        }
        let mut transaction = pool.begin().await.map_err(storage_error)?;
        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("PRAGMA application_id = 1313165121")
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
    if application_id != SQLITE_APPLICATION_ID {
        return Err(storage_corruption(format!(
            "SQLite authority application_id {application_id} is unsupported"
        )));
    }
    match user_version {
        1 => migrate_v1_to_v2(pool).await?,
        SQLITE_SCHEMA_VERSION => {}
        _ => {
            return Err(storage_corruption(format!(
                "SQLite authority user_version {user_version} is unsupported"
            )));
        }
    }
    validate_table_set(pool, V2_TABLES).await?;
    validate_current_schema(pool).await
}

async fn migrate_v1_to_v2(pool: &SqlitePool) -> CentralResult<()> {
    validate_table_set(pool, V1_TABLES).await?;
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    sqlx::raw_sql(ENROLLMENT_AUDIT_SCHEMA_SQL)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    sqlx::query("PRAGMA user_version = 2")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)
}

async fn validate_table_set(pool: &SqlitePool, expected: &[&str]) -> CentralResult<()> {
    let actual: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .map_err(storage_error)?;
    let expected_names = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual_names = actual.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if actual_names != expected_names {
        return Err(storage_corruption(
            "SQLite authority table set differs from the current schema",
        ));
    }
    Ok(())
}

async fn validate_current_schema(pool: &SqlitePool) -> CentralResult<()> {
    for expected_statement in SCHEMA_SQL
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        let table = expected_statement
            .split_ascii_whitespace()
            .nth(2)
            .ok_or_else(|| storage_corruption("invalid embedded SQLite authority schema"))?;
        let stored: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?")
                .bind(table)
                .fetch_one(pool)
                .await
                .map_err(storage_error)?;
        if normalize_schema_sql(&stored) != normalize_schema_sql(expected_statement) {
            return Err(storage_corruption(format!(
                "SQLite authority table {table} differs from the current schema"
            )));
        }
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect()
}

fn secure_database_file(path: &Path) -> CentralResult<()> {
    validate_database_path(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(storage_error)?;
    }
    Ok(())
}

async fn validate_integrity(pool: &SqlitePool) -> CentralResult<()> {
    let results: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(pool)
        .await
        .map_err(storage_error)?;
    if results.as_slice() != ["ok"] {
        return Err(storage_corruption(format!(
            "SQLite authority integrity check failed: {}",
            results.join("; ")
        )));
    }
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(pool)
        .await
        .map_err(storage_error)?;
    if !violations.is_empty() {
        return Err(storage_corruption(
            "SQLite authority foreign-key check failed",
        ));
    }
    Ok(())
}

fn storage_error(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(
        CentralErrorCode::StorageFailure,
        format!("SQLite authority storage operation failed: {error}"),
    )
}

fn storage_corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}
