use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::Duration,
};

use fs2::FileExt;
use rusqlite::Connection;

use super::{AgentError, AgentErrorCode, AgentResult};

/// Every durable Agent concern lives in this one database. Adapters use a shared handle for a
/// given state root so there is one in-process connection and one lock domain, while the lock
/// file still fences a second Agent process from opening the same state directory.
pub(crate) const AGENT_STATE_DATABASE_FILE: &str = "agent-state.sqlite3";
pub(crate) const AGENT_STATE_LOCK_FILE: &str = "agent-state.lock";
pub(crate) const AGENT_STATE_APPLICATION_ID: i64 = 0x4e45_4153; // NEAS
pub(crate) const AGENT_STATE_SCHEMA_VERSION: i64 = 1;
const CURRENT_TABLES: &[&str] = &[
    "ledger_metadata",
    "ledger_records",
    "outbound_metadata",
    "outbound_reports",
    "system_metadata",
    "system_identity",
    "lifecycle_metadata",
    "lifecycle_records",
    "placement_inventory_metadata",
    "placement_inventory",
];
const LEGACY_FILES: &[&str] = &[
    "ledger.sqlite3",
    "outbound.sqlite3",
    "system.sqlite3",
    "lifecycle.sqlite3",
    "ledger.lock",
    "outbound.lock",
    "system.lock",
    "lifecycle.lock",
];

struct LockedSqliteInner {
    connection: Mutex<Connection>,
    database: PathBuf,
    _lock: File,
}

#[derive(Clone)]
pub(crate) struct LockedSqlite {
    inner: Arc<LockedSqliteInner>,
}

static OPEN_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<LockedSqliteInner>>>> = OnceLock::new();

impl std::fmt::Debug for LockedSqlite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LockedSqlite")
            .field("database", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

pub(crate) struct SqliteDefinition<'a> {
    pub database_file: &'static str,
    pub lock_file: &'static str,
    pub application_id: i64,
    pub schema_version: i64,
    pub schema: &'a str,
    pub tables: &'a [&'a str],
}

impl LockedSqlite {
    pub(crate) fn open(root: &Path, definition: SqliteDefinition<'_>) -> AgentResult<Self> {
        let root = prepare_root(root)?;
        reject_legacy_layout(&root)?;
        if definition.lock_file != AGENT_STATE_LOCK_FILE {
            return Err(storage_corruption(
                "Agent SQLite adapters must use the shared state lock",
            ));
        }
        let lock_path = root.join(AGENT_STATE_LOCK_FILE);
        let database = root.join(definition.database_file);
        reject_special_path(&database, "database")?;
        reject_special_path(&lock_path, "lock")?;
        for suffix in ["-wal", "-shm"] {
            reject_special_path(
                &root.join(format!("{}{suffix}", definition.database_file)),
                "SQLite auxiliary file",
            )?;
        }

        let registry = OPEN_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut open_databases = registry.lock().map_err(|_| {
            AgentError::new(
                AgentErrorCode::Internal,
                "Agent SQLite open registry lock is poisoned",
            )
        })?;

        // Multiple adapters are opened independently by the runtime. Reuse the exact same
        // connection and file lock for all of them; opening the same lock path a second time
        // would otherwise fail even within one process on some platforms.
        if let Some(existing) = open_databases.get(&database).and_then(Weak::upgrade) {
            let storage = Self { inner: existing };
            storage.ensure_definition(&definition)?;
            return Ok(storage);
        }
        open_databases.retain(|_, storage| storage.strong_count() != 0);

        let lock = open_lock(&lock_path)?;
        let initialize =
            !database.exists() || fs::metadata(&database).map_err(storage_error)?.len() == 0;
        let connection = Connection::open(&database).map_err(storage_error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(storage_error)?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(storage_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage_error)?;
        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(storage_error)?;
        if initialize {
            let enabled: String = connection
                .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
                .map_err(storage_error)?;
            if !enabled.eq_ignore_ascii_case("wal") {
                return Err(storage_corruption("SQLite refused WAL journal mode"));
            }
            initialize_schema(&connection, &definition)?;
        } else if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(storage_corruption(
                "SQLite database is not using WAL journal mode",
            ));
        } else {
            // A shared Agent database is assembled by the first adapter that opens it.  Each
            // subsequent adapter contributes its own idempotent table fragment.
            validate_identity(&connection, &definition)?;
            ensure_schema(&connection, &definition)?;
        }

        validate_schema(&connection, &definition)?;
        validate_integrity(&connection)?;
        let storage = Self {
            inner: Arc::new(LockedSqliteInner {
                connection: Mutex::new(connection),
                database: database.clone(),
                _lock: lock,
            }),
        };
        storage.secure_files()?;
        open_databases.insert(database, Arc::downgrade(&storage.inner));
        Ok(storage)
    }

    fn ensure_definition(&self, definition: &SqliteDefinition<'_>) -> AgentResult<()> {
        let connection = self.connection()?;
        validate_identity(&connection, definition)?;
        ensure_schema(&connection, definition)?;
        validate_schema(&connection, definition)?;
        validate_integrity(&connection)
    }

    pub(crate) fn connection(&self) -> AgentResult<MutexGuard<'_, Connection>> {
        self.inner.connection.lock().map_err(|_| {
            AgentError::new(
                AgentErrorCode::Internal,
                "Agent SQLite connection lock is poisoned",
            )
        })
    }

    pub(crate) fn integrity_check(&self) -> AgentResult<()> {
        let connection = self.connection()?;
        validate_integrity(&connection)
    }

    pub(crate) fn secure_files(&self) -> AgentResult<()> {
        secure_file(&self.inner.database)?;
        let file_name = self
            .inner
            .database
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| storage_corruption("SQLite database name is not valid UTF-8"))?;
        let root = self
            .inner
            .database
            .parent()
            .ok_or_else(|| storage_corruption("SQLite database has no parent directory"))?;
        for suffix in ["-wal", "-shm"] {
            let path = root.join(format!("{file_name}{suffix}"));
            if path.exists() {
                reject_special_path(&path, "SQLite auxiliary file")?;
                secure_file(&path)?;
            }
        }
        Ok(())
    }
}

fn initialize_schema(
    connection: &Connection,
    definition: &SqliteDefinition<'_>,
) -> AgentResult<()> {
    let application_id = definition.application_id;
    let schema_version = definition.schema_version;
    let statements = format!(
        "BEGIN IMMEDIATE;\n{}\nPRAGMA application_id = {application_id};\nPRAGMA user_version = {schema_version};\nCOMMIT;",
        definition.schema
    );
    connection.execute_batch(&statements).map_err(storage_error)
}

fn ensure_schema(connection: &Connection, definition: &SqliteDefinition<'_>) -> AgentResult<()> {
    let statements = format!("BEGIN IMMEDIATE;\n{}\nCOMMIT;", definition.schema);
    connection.execute_batch(&statements).map_err(storage_error)
}

fn validate_identity(
    connection: &Connection,
    definition: &SqliteDefinition<'_>,
) -> AgentResult<()> {
    let application_id: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(storage_error)?;
    let schema_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(storage_error)?;
    if application_id != definition.application_id || schema_version != definition.schema_version {
        return Err(storage_corruption(format!(
            "unsupported Agent SQLite schema identity {application_id}/{schema_version}"
        )));
    }
    Ok(())
}

fn validate_schema(connection: &Connection, definition: &SqliteDefinition<'_>) -> AgentResult<()> {
    validate_identity(connection, definition)?;

    let mut statement = connection
        .prepare(
            "SELECT name FROM sqlite_schema \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .map_err(storage_error)?;
    let actual = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(storage_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage_error)?;
    if actual
        .iter()
        .any(|table| !CURRENT_TABLES.iter().any(|expected| expected == table))
        || definition
            .tables
            .iter()
            .any(|expected| !actual.iter().any(|table| table == expected))
    {
        return Err(storage_corruption(
            "Agent SQLite table set differs from the current shared schema",
        ));
    }
    Ok(())
}

fn reject_legacy_layout(root: &Path) -> AgentResult<()> {
    for file in LEGACY_FILES {
        let path = root.join(file);
        if path.exists() {
            return Err(storage_corruption(format!(
                "legacy Agent state file {} is unsupported; reinitialize the state directory",
                path.display()
            )));
        }
    }
    for directory in ["ledger", "outbound", "system", "lifecycle"] {
        let path = root.join(directory);
        if path.is_dir() {
            for file in LEGACY_FILES {
                let nested = path.join(file);
                if nested.exists() {
                    return Err(storage_corruption(format!(
                        "legacy Agent state file {} is unsupported; reinitialize the state directory",
                        nested.display()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_integrity(connection: &Connection) -> AgentResult<()> {
    let mut statement = connection
        .prepare("PRAGMA quick_check")
        .map_err(storage_error)?;
    let results = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(storage_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage_error)?;
    if results.as_slice() != ["ok"] {
        return Err(storage_corruption("Agent SQLite quick check failed"));
    }
    let violations: i64 = connection
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .map_err(storage_error)?;
    if violations != 0 {
        return Err(storage_corruption("Agent SQLite foreign-key check failed"));
    }
    Ok(())
}

fn prepare_root(path: &Path) -> AgentResult<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(storage_corruption("Agent SQLite root must be explicit"));
    }
    if path.exists() {
        let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(storage_corruption(
                "Agent SQLite root must be a real directory",
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

fn open_lock(path: &Path) -> AgentResult<File> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(storage_error)?;
    file.try_lock_exclusive().map_err(|_| {
        AgentError::new(
            AgentErrorCode::LedgerConflict,
            "Agent SQLite database is already open in another process",
        )
    })?;
    secure_file(path)?;
    Ok(file)
}

fn reject_special_path(path: &Path, kind: &'static str) -> AgentResult<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path).map_err(storage_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(storage_corruption(format!(
            "Agent SQLite {kind} must be a regular file"
        )));
    }
    Ok(())
}

fn secure_file(path: &Path) -> AgentResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(storage_error)?;
    }
    Ok(())
}

pub(crate) fn storage_error(_error: impl std::fmt::Display) -> AgentError {
    AgentError::new(
        AgentErrorCode::Internal,
        "Agent local SQLite operation failed",
    )
}

pub(crate) fn storage_corruption(message: impl Into<String>) -> AgentError {
    AgentError::new(AgentErrorCode::Internal, message)
}
