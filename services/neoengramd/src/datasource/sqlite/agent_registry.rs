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
    Sqlite, SqlitePool, Transaction,
};

use crate::{CentralError, CentralErrorCode, CentralResult};

const DATABASE_FILE_NAME: &str = "agent-registry.sqlite3";
const LOCK_FILE_NAME: &str = "agent-registry.lock";
const SQLITE_APPLICATION_ID: i64 = 0x4e45_4f52;
const SQLITE_SCHEMA_VERSION: i64 = 6;

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
CREATE TABLE tenant_catalog_records (
    tenant_id TEXT NOT NULL PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms)
) STRICT;
CREATE INDEX tenant_catalog_keyset
    ON tenant_catalog_records (created_at_unix_ms DESC, tenant_id ASC);
CREATE TABLE artifact_catalog_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    initialization_mode TEXT NOT NULL CHECK (initialization_mode IN ('empty', 'derived')),
    source_project_id TEXT,
    source_artifact_id TEXT,
    source_commit_digest BLOB,
    head_commit_digest BLOB CHECK (
        head_commit_digest IS NULL OR length(head_commit_digest) = 32
    ),
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, artifact_id),
    UNIQUE (tenant_id, project_id, artifact_id),
    CHECK (
        (initialization_mode = 'empty' AND source_project_id IS NULL
            AND source_artifact_id IS NULL AND source_commit_digest IS NULL)
        OR
        (initialization_mode = 'derived' AND source_project_id IS NOT NULL
            AND source_artifact_id IS NOT NULL AND length(source_commit_digest) = 32)
    ),
    FOREIGN KEY (tenant_id) REFERENCES tenant_catalog_records(tenant_id),
    FOREIGN KEY (tenant_id, source_project_id, source_artifact_id)
        REFERENCES artifact_catalog_records(tenant_id, project_id, artifact_id)
) STRICT;
CREATE INDEX artifact_catalog_keyset
    ON artifact_catalog_records (
        tenant_id, created_at_unix_ms DESC, project_id ASC, artifact_id ASC
    );
CREATE INDEX artifact_catalog_filter_keyset
    ON artifact_catalog_records (
        tenant_id, project_id, created_at_unix_ms DESC, artifact_id ASC
    );
CREATE TABLE storage_volume_catalog_records (
    tenant_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    edge_cluster_id TEXT NOT NULL,
    region TEXT NOT NULL,
    backend_type TEXT NOT NULL CHECK (backend_type IN ('pvc', 'nfs')),
    access_mode TEXT NOT NULL CHECK (
        access_mode IN ('read_write_once', 'read_write_many', 'read_only_many')
    ),
    pvc_namespace TEXT,
    pvc_claim_name TEXT,
    nfs_server TEXT,
    nfs_export_path TEXT,
    state TEXT NOT NULL CHECK (state IN ('ready', 'degraded', 'unavailable')),
    enrollment_id TEXT,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, storage_volume_id),
    CHECK (
        (backend_type = 'pvc' AND pvc_namespace IS NOT NULL AND pvc_claim_name IS NOT NULL
            AND nfs_server IS NULL AND nfs_export_path IS NULL)
        OR
        (backend_type = 'nfs' AND pvc_namespace IS NULL AND pvc_claim_name IS NULL
            AND nfs_server IS NOT NULL AND nfs_export_path IS NOT NULL)
    )
) STRICT;
CREATE UNIQUE INDEX storage_volume_catalog_pvc_identity
    ON storage_volume_catalog_records (edge_cluster_id, pvc_namespace, pvc_claim_name)
    WHERE backend_type = 'pvc';
CREATE INDEX storage_volume_catalog_keyset
    ON storage_volume_catalog_records (
        tenant_id, created_at_unix_ms DESC, storage_volume_id ASC
    );
CREATE INDEX storage_volume_catalog_filter_keyset
    ON storage_volume_catalog_records (
        tenant_id, region, backend_type, created_at_unix_ms DESC, storage_volume_id ASC
    );
CREATE TABLE playground_catalog_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL,
    region TEXT NOT NULL,
    display_name TEXT NOT NULL,
    base_commit_digest BLOB CHECK (
        base_commit_digest IS NULL OR length(base_commit_digest) = 32
    ),
    head_commit_digest BLOB CHECK (
        head_commit_digest IS NULL OR length(head_commit_digest) = 32
    ),
    state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'abnormal')),
    relative_root TEXT NOT NULL CHECK (
        relative_root <> '' AND substr(relative_root, 1, 1) <> '/'
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, project_id, artifact_id, playground_id),
    FOREIGN KEY (tenant_id, project_id, artifact_id)
        REFERENCES artifact_catalog_records(tenant_id, project_id, artifact_id),
    FOREIGN KEY (tenant_id, storage_volume_id)
        REFERENCES storage_volume_catalog_records(tenant_id, storage_volume_id)
) STRICT;
CREATE INDEX playground_catalog_keyset
    ON playground_catalog_records (
        tenant_id, created_at_unix_ms DESC, project_id ASC, artifact_id ASC, playground_id ASC
    );
CREATE INDEX playground_catalog_filter_keyset
    ON playground_catalog_records (
        tenant_id, project_id, artifact_id, region, state,
        created_at_unix_ms DESC, playground_id ASC
    );
CREATE TABLE snapshot_catalog_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    snapshot_request_id TEXT NOT NULL,
    commit_digest BLOB NOT NULL CHECK (length(commit_digest) = 32),
    storage_volume_id TEXT NOT NULL,
    region TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'abnormal')),
    phase TEXT NOT NULL CHECK (phase IN ('planning', 'materializing', 'verifying', 'idle')),
    relative_root TEXT NOT NULL CHECK (
        relative_root <> '' AND substr(relative_root, 1, 1) <> '/'
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, snapshot_id),
    UNIQUE (tenant_id, snapshot_request_id),
    UNIQUE (tenant_id, project_id, artifact_id, commit_digest, storage_volume_id),
    FOREIGN KEY (tenant_id, project_id, artifact_id)
        REFERENCES artifact_catalog_records(tenant_id, project_id, artifact_id),
    FOREIGN KEY (tenant_id, storage_volume_id)
        REFERENCES storage_volume_catalog_records(tenant_id, storage_volume_id)
) STRICT;
CREATE INDEX snapshot_catalog_keyset
    ON snapshot_catalog_records (tenant_id, created_at_unix_ms DESC, snapshot_id ASC);
CREATE INDEX snapshot_catalog_filter_keyset
    ON snapshot_catalog_records (
        tenant_id, project_id, artifact_id, region, state,
        created_at_unix_ms DESC, snapshot_id ASC
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
        sqlx::query("PRAGMA user_version = 6")
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
        2 => {
            migrate_v2_to_v3(pool).await?;
            migrate_v3_to_v5(pool).await?;
            migrate_v5_to_v6(pool).await?;
        }
        3 => {
            migrate_v3_to_v5(pool).await?;
            migrate_v5_to_v6(pool).await?;
        }
        4 => {
            migrate_v4_to_v5(pool).await?;
            migrate_v5_to_v6(pool).await?;
        }
        5 => migrate_v5_to_v6(pool).await?,
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

async fn migrate_v3_to_v5(pool: &SqlitePool) -> CentralResult<()> {
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    if catalog_schema_presence(&mut transaction).await? == CatalogSchemaPresence::Absent {
        create_catalog_schema_v5(&mut transaction).await?;
    }
    sqlx::query("PRAGMA user_version = 5")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)
}

async fn migrate_v4_to_v5(pool: &SqlitePool) -> CentralResult<()> {
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    let derived_authority: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT tenant_id, project_id, artifact_id, playground_id \
         FROM playground_catalog_records LIMIT 1",
    )
    .fetch_optional(&mut *transaction)
    .await
    .map_err(storage_error)?;
    if let Some((tenant_id, project_id, artifact_id, playground_id)) = derived_authority {
        return Err(storage_corruption(format!(
            "cannot migrate Playground {tenant_id}/{project_id}/{artifact_id}/{playground_id}: Artifact authority cannot be inferred from a derived v4 Playground; export and remove the v4 Playground, create the Artifact explicitly, then recreate the Playground"
        )));
    }
    create_catalog_schema_objects(&mut transaction, "artifact_catalog_").await?;
    sqlx::query("DROP INDEX playground_catalog_keyset")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    sqlx::query("DROP INDEX playground_catalog_filter_keyset")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    sqlx::query("ALTER TABLE playground_catalog_records RENAME TO playground_catalog_records_v4")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    create_catalog_schema_objects(&mut transaction, "playground_catalog_").await?;
    sqlx::query(
        "INSERT INTO playground_catalog_records \
         (tenant_id, project_id, artifact_id, playground_id, storage_volume_id, region, \
          display_name, base_commit_digest, head_commit_digest, \
          state, relative_root, created_at_unix_ms, updated_at_unix_ms) \
         SELECT tenant_id, project_id, artifact_id, playground_id, storage_volume_id, region, \
                display_name, base_commit_digest, head_commit_digest, \
                state, relative_root, created_at_unix_ms, updated_at_unix_ms \
         FROM playground_catalog_records_v4",
    )
    .execute(&mut *transaction)
    .await
    .map_err(storage_error)?;
    sqlx::query("DROP TABLE playground_catalog_records_v4")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    sqlx::query("PRAGMA user_version = 5")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)
}

async fn migrate_v5_to_v6(pool: &SqlitePool) -> CentralResult<()> {
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    if snapshot_catalog_schema_presence(&mut transaction).await? == CatalogSchemaPresence::Absent {
        create_catalog_schema_objects(&mut transaction, "snapshot_catalog_").await?;
    }
    sqlx::query("PRAGMA user_version = 6")
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogSchemaPresence {
    Absent,
    Current,
}

async fn create_catalog_schema_v5(transaction: &mut Transaction<'_, Sqlite>) -> CentralResult<()> {
    for statement in SCHEMA_SQL
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        let (_, name) = schema_object(statement)?;
        if is_catalog_schema_object(name) && !name.starts_with("snapshot_catalog_") {
            sqlx::query(statement)
                .execute(&mut **transaction)
                .await
                .map_err(storage_error)?;
        }
    }
    Ok(())
}

async fn create_catalog_schema_objects(
    transaction: &mut Transaction<'_, Sqlite>,
    name_prefix: &str,
) -> CentralResult<()> {
    for statement in SCHEMA_SQL
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        let (_, name) = schema_object(statement)?;
        if name.starts_with(name_prefix) {
            sqlx::query(statement)
                .execute(&mut **transaction)
                .await
                .map_err(storage_error)?;
        }
    }
    Ok(())
}

async fn catalog_schema_presence(
    transaction: &mut Transaction<'_, Sqlite>,
) -> CentralResult<CatalogSchemaPresence> {
    schema_presence(
        transaction,
        is_catalog_schema_object,
        "Agent registry control catalog schema is only partially present",
    )
    .await
}

async fn snapshot_catalog_schema_presence(
    transaction: &mut Transaction<'_, Sqlite>,
) -> CentralResult<CatalogSchemaPresence> {
    schema_presence(
        transaction,
        is_snapshot_catalog_schema_object,
        "Agent registry Snapshot catalog schema is only partially present",
    )
    .await
}

async fn schema_presence(
    transaction: &mut Transaction<'_, Sqlite>,
    includes: fn(&str) -> bool,
    partial_schema_message: &'static str,
) -> CentralResult<CatalogSchemaPresence> {
    let mut expected_count = 0_usize;
    let mut present_count = 0_usize;
    for expected_statement in SCHEMA_SQL
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        let (object_type, name) = schema_object(expected_statement)?;
        if !includes(name) {
            continue;
        }
        expected_count += 1;
        let stored: Option<String> =
            sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type = ? AND name = ?")
                .bind(object_type)
                .bind(name)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(storage_error)?;
        let Some(stored) = stored else {
            continue;
        };
        if normalize_schema_sql(&stored) != normalize_schema_sql(expected_statement) {
            return Err(storage_corruption(format!(
                "Agent registry {object_type} {name} differs from the current schema"
            )));
        }
        present_count += 1;
    }
    if present_count == 0 {
        Ok(CatalogSchemaPresence::Absent)
    } else if present_count == expected_count {
        Ok(CatalogSchemaPresence::Current)
    } else {
        Err(storage_corruption(partial_schema_message))
    }
}

fn is_catalog_schema_object(name: &str) -> bool {
    name.starts_with("tenant_catalog_")
        || name.starts_with("artifact_catalog_")
        || name.starts_with("storage_volume_catalog_")
        || name.starts_with("playground_catalog_")
        || name.starts_with("snapshot_catalog_")
}

fn is_snapshot_catalog_schema_object(name: &str) -> bool {
    name.starts_with("snapshot_catalog_")
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
