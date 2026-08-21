use std::{
    collections::{BTreeMap, BTreeSet},
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
// `NEAU` identifies the consolidated clean-slate authority database.  No prior authority or
// Agent Registry identity is accepted here; a directory containing an older database must be
// initialized afresh.
const SQLITE_APPLICATION_ID: i64 = 0x4e45_4155;
const SQLITE_SCHEMA_VERSION: i64 = 13;
const LEGACY_DATABASE_FILES: &[&str] = &[
    "agent-registry.sqlite3",
    "gateway-registry.sqlite3",
    "catalog.sqlite3",
    "metadata.sqlite3",
    "object.sqlite3",
    "outbox.sqlite3",
    "authority.db",
];

const CORE_SCHEMA_SQL: &str = r#"
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
    retired INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    PRIMARY KEY (tenant_id, assignment_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES control_jobs (tenant_id, job_id),
    CHECK (retired = 0 OR published = 1)
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

CREATE TABLE object_placements (
    tenant_id TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL,
    artifact_placement_id TEXT NOT NULL,
    placement_generation TEXT NOT NULL CHECK (
        placement_generation <> '' AND placement_generation NOT GLOB '*[^0-9]*'
    ),
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    size TEXT NOT NULL CHECK (size <> '' AND size NOT GLOB '*[^0-9]*'),
    verified_at_unix_ms TEXT NOT NULL CHECK (
        verified_at_unix_ms <> '' AND verified_at_unix_ms NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, receipt_id)
) STRICT;

CREATE TABLE playground_indexes (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    revision TEXT NOT NULL CHECK (revision <> '' AND revision NOT GLOB '*[^0-9]*'),
    digest BLOB NOT NULL CHECK (length(digest) = 32),
    PRIMARY KEY (tenant_id, project_id, artifact_id, playground_id)
) STRICT;

CREATE TABLE playground_index_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    path TEXT NOT NULL,
    manifest_id BLOB NOT NULL CHECK (length(manifest_id) = 32),
    total_size TEXT NOT NULL CHECK (total_size <> '' AND total_size NOT GLOB '*[^0-9]*'),
    chunk_count TEXT NOT NULL CHECK (chunk_count <> '' AND chunk_count NOT GLOB '*[^0-9]*'),
    PRIMARY KEY (tenant_id, project_id, artifact_id, playground_id, path),
    FOREIGN KEY (tenant_id, project_id, artifact_id, playground_id)
        REFERENCES playground_indexes (tenant_id, project_id, artifact_id, playground_id)
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

CREATE TABLE precommit_records (
    tenant_id TEXT NOT NULL,
    precommit_id TEXT NOT NULL,
    precommit_request_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    current_job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('running', 'ready', 'abnormal', 'cancelled', 'committed')
    ),
    attempt INTEGER NOT NULL CHECK (attempt >= 1 AND attempt <= 2147483647),
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, precommit_id),
    UNIQUE (tenant_id, precommit_request_id),
    UNIQUE (tenant_id, current_job_id)
) STRICT;

CREATE TABLE precommit_mutations (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('start', 'restart', 'cancel', 'commit')),
    precommit_id TEXT NOT NULL,
    request_payload BLOB NOT NULL,
    result_payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, request_id),
    FOREIGN KEY (tenant_id, precommit_id)
        REFERENCES precommit_records (tenant_id, precommit_id)
) STRICT;

CREATE TABLE commit_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    commit_id BLOB NOT NULL CHECK (length(commit_id) = 32),
    commit_request_id TEXT NOT NULL,
    precommit_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, project_id, artifact_id, commit_id),
    UNIQUE (tenant_id, commit_request_id),
    FOREIGN KEY (tenant_id, precommit_id)
        REFERENCES precommit_records (tenant_id, precommit_id)
) STRICT;

CREATE TABLE lifecycle_cleanup_records (
    tenant_id TEXT NOT NULL,
    deletion_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    target_kind TEXT NOT NULL CHECK (
        target_kind IN ('storage_volume', 'artifact', 'playground', 'snapshot')
    ),
    action TEXT NOT NULL CHECK (
        action IN ('quiesce', 'quarantine', 'restore', 'purge', 'finalize')
    ),
    state TEXT NOT NULL CHECK (
        state IN ('requested', 'running', 'completed', 'blocked', 'failed')
    ),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    resource_generation TEXT NOT NULL CHECK (
        resource_generation <> '' AND resource_generation NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, deletion_id, target_id, action)
) STRICT;

CREATE TABLE objects (
    tenant_id TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    size INTEGER NOT NULL CHECK (size >= 0),
    encoding TEXT NOT NULL CHECK (encoding IN ('raw', 'zstd')),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    PRIMARY KEY (tenant_id, object_id)
) STRICT;

CREATE TABLE commit_objects (
    tenant_id TEXT NOT NULL,
    commit_id BLOB NOT NULL CHECK (length(commit_id) = 32),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    size INTEGER NOT NULL CHECK (size >= 0),
    encoding TEXT NOT NULL CHECK (encoding IN ('raw', 'zstd')),
    PRIMARY KEY (tenant_id, commit_id, ordinal),
    UNIQUE (tenant_id, commit_id, object_id),
    FOREIGN KEY (tenant_id, object_id) REFERENCES objects (tenant_id, object_id)
) STRICT;

CREATE TABLE commit_object_sets (
    tenant_id TEXT NOT NULL,
    commit_id BLOB NOT NULL CHECK (length(commit_id) = 32),
    object_set_digest BLOB NOT NULL CHECK (length(object_set_digest) = 32),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    PRIMARY KEY (tenant_id, commit_id)
) STRICT;

CREATE TABLE commit_placement_sets (
    tenant_id TEXT NOT NULL,
    placement_set_id TEXT NOT NULL,
    commit_id BLOB NOT NULL CHECK (length(commit_id) = 32),
    backend_id TEXT NOT NULL,
    storage_volume_id TEXT,
    archive_id TEXT,
    object_set_digest BLOB NOT NULL CHECK (length(object_set_digest) = 32),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    verified_object_count INTEGER NOT NULL CHECK (verified_object_count >= 0),
    placement_generation INTEGER NOT NULL CHECK (placement_generation > 0),
    state TEXT NOT NULL CHECK (state IN ('staged', 'published', 'retiring', 'deleted')),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, placement_set_id),
    CHECK ((storage_volume_id IS NULL) <> (archive_id IS NULL)),
    UNIQUE (tenant_id, commit_id, backend_id),
    CHECK (verified_object_count <= object_count),
    CHECK (state <> 'published' OR verified_object_count = object_count)
) STRICT;

CREATE INDEX commit_placement_sets_lookup
    ON commit_placement_sets (tenant_id, commit_id, state, backend_id);

CREATE TABLE placement_objects (
    tenant_id TEXT NOT NULL,
    placement_id TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    backend_id TEXT NOT NULL,
    storage_volume_id TEXT,
    archive_id TEXT,
    edge_cluster_id TEXT,
    gateway_pool_id TEXT,
    region TEXT,
    placement_generation INTEGER NOT NULL CHECK (placement_generation > 0),
    state TEXT NOT NULL CHECK (state IN ('verified', 'retiring', 'deleted', 'lost')),
    verified_size INTEGER NOT NULL CHECK (verified_size >= 0),
    verified_digest BLOB NOT NULL CHECK (length(verified_digest) = 32),
    failure_domain TEXT NOT NULL CHECK (length(failure_domain) BETWEEN 1 AND 256),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, placement_id),
    CHECK ((storage_volume_id IS NULL) <> (archive_id IS NULL)),
    UNIQUE (tenant_id, object_id, backend_id, placement_generation)
) STRICT;

CREATE INDEX placement_objects_lookup
    ON placement_objects (tenant_id, object_id, state, backend_id);

CREATE TABLE replications (
    tenant_id TEXT NOT NULL,
    replication_id TEXT NOT NULL,
    commit_id BLOB NOT NULL CHECK (length(commit_id) = 32),
    target_backend_id TEXT NOT NULL,
    target_storage_volume_id TEXT,
    target_archive_id TEXT,
    object_set_digest BLOB NOT NULL CHECK (length(object_set_digest) = 32),
    completed_objects INTEGER NOT NULL CHECK (completed_objects >= 0),
    total_objects INTEGER NOT NULL CHECK (total_objects >= 0),
    state TEXT NOT NULL CHECK (state IN ('queued', 'planning', 'transferring', 'verifying', 'published', 'failed', 'cancelled')),
    request_id TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, replication_id),
    UNIQUE (tenant_id, request_id),
    CHECK ((target_storage_volume_id IS NULL) <> (target_archive_id IS NULL))
) STRICT;

CREATE TABLE replication_objects (
    tenant_id TEXT NOT NULL,
    replication_id TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    offset INTEGER NOT NULL CHECK (offset >= 0),
    state TEXT NOT NULL CHECK (state IN ('queued', 'transferring', 'verified', 'failed')),
    retry_count INTEGER NOT NULL CHECK (retry_count >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= 0),
    PRIMARY KEY (tenant_id, replication_id, object_id),
    FOREIGN KEY (tenant_id, replication_id)
        REFERENCES replications (tenant_id, replication_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE workspaces (
    tenant_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    base_commit_id BLOB CHECK (base_commit_id IS NULL OR length(base_commit_id) = 32),
    target_storage_volume_id TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK (lifecycle IN ('provisioning', 'active', 'unavailable', 'deleting', 'deleted')),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, workspace_id),
    UNIQUE (tenant_id, request_id)
) STRICT;
"#;

/// Returns the complete current authority schema. Context-specific table definitions remain
/// owned by their bounded contexts, but are installed in the same SQLite transaction so authority,
/// Agent Registry, Gateway Registry, S3 and lifecycle mutations share one database identity.
pub(crate) fn schema_sql() -> String {
    format!(
        "{}\n{}",
        CORE_SCHEMA_SQL,
        crate::datasource::sqlite::agent_registry::SCHEMA_SQL
    )
}

pub(crate) struct SqliteAuthorityDataSource {
    pool: SqlitePool,
    lock: Mutex<Option<File>>,
}

impl SqliteAuthorityDataSource {
    pub(crate) async fn open(path: &Path, busy_timeout: Duration) -> CentralResult<Self> {
        let root = prepare_root(path)?;
        reject_legacy_layout(&root)?;
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

fn reject_legacy_layout(root: &Path) -> CentralResult<()> {
    for file in LEGACY_DATABASE_FILES {
        let path = root.join(file);
        if path.exists() {
            return Err(storage_corruption(format!(
                "legacy Central database {} is unsupported; initialize a clean authority directory",
                path.display()
            )));
        }
    }
    for directory in [
        "agent-registry",
        "gateway-registry",
        "catalog",
        "metadata",
        "object",
        "outbox",
    ] {
        let path = root.join(directory);
        if path.exists() {
            return Err(storage_corruption(format!(
                "legacy Central database directory {} is unsupported; initialize a clean authority directory",
                path.display()
            )));
        }
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
        let schema = schema_sql();
        sqlx::raw_sql(&schema)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("PRAGMA application_id = 1313161557")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("PRAGMA user_version = 13")
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
    if user_version != SQLITE_SCHEMA_VERSION {
        return Err(storage_corruption(format!(
            "SQLite authority schema {user_version} is unsupported; initialize a clean current-format database"
        )));
    }
    validate_current_schema(pool).await
}

async fn validate_current_schema(pool: &SqlitePool) -> CentralResult<()> {
    let actual_objects = sqlx::query_as::<_, (String, String, String)>(
        "SELECT type, name, sql FROM sqlite_schema \
         WHERE name NOT LIKE 'sqlite_%' \
         ORDER BY type, name",
    )
    .fetch_all(pool)
    .await
    .map_err(storage_error)?
    .into_iter()
    .map(|(object_type, name, sql)| ((object_type, name), sql))
    .collect::<BTreeMap<_, _>>();

    let mut expected_objects = BTreeSet::new();
    let schema = schema_sql();
    for expected_statement in schema
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        let (object_type, name) = schema_object(expected_statement)?;
        let key = (object_type.to_owned(), name.to_owned());
        expected_objects.insert(key.clone());
        let stored = actual_objects.get(&key).ok_or_else(|| {
            storage_corruption(format!("SQLite authority {object_type} {name} is missing"))
        })?;
        if normalize_schema_sql(stored) != normalize_schema_sql(expected_statement) {
            return Err(storage_corruption(format!(
                "SQLite authority {object_type} {name} differs from the current schema"
            )));
        }
    }
    if actual_objects.keys().cloned().collect::<BTreeSet<_>>() != expected_objects {
        return Err(storage_corruption(
            "SQLite authority table or index set differs from the current schema",
        ));
    }
    Ok(())
}

fn schema_object(statement: &str) -> CentralResult<(&'static str, &str)> {
    let tokens = statement.split_ascii_whitespace().collect::<Vec<_>>();
    let (object_type, name_index) = match tokens.as_slice() {
        ["CREATE", "TABLE", _, ..] => ("table", 2),
        ["CREATE", "INDEX", _, ..] | ["CREATE", "UNIQUE", "INDEX", _, ..] => {
            ("index", if tokens[1] == "INDEX" { 2 } else { 3 })
        }
        _ => {
            return Err(storage_corruption(format!(
                "unsupported embedded SQLite schema statement: {statement}"
            )))
        }
    };
    let name = tokens
        .get(name_index)
        .ok_or_else(|| storage_corruption("embedded SQLite schema object has no name"))?
        .trim_matches('(');
    Ok((object_type, name))
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
