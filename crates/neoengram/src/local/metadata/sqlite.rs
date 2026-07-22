//! SQLite metadata backend.

use std::{
    collections::BTreeMap,
    fmt, fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    rc::Rc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{
    Chunk, ChunkingStrategy, Commit, DirectoryEntry, DirectoryEntryKind, INDEX_FORMAT_VERSION,
};
use rusqlite::{params, params_from_iter, types::Value, Connection, OpenFlags, OptionalExtension};

use crate::local::fs::durable::{ensure_or_create_directory, sync_parent};

use super::{
    normalize_head_state, validate_directory_entry, validate_metadata_object_id,
    validate_reference_name, validate_reference_prefix, DirectoryHasher, DirectoryReader,
    DirectoryRef, DirectoryWriter, FileRecord, FileSetReader, HeadCas, HeadState, IndexReader,
    IndexTxn, IndexVersion, ManifestHasher, ManifestReader, ManifestRef, MetadataReader,
    MetadataSnapshot, MetadataStore, Page, PageRequest, ReferenceCas, StoredReference, WorkspaceId,
    WorkspaceRecord, DEFAULT_WORKSPACE_ID,
};

const DATABASE_FILE_NAME: &str = "metadata.sqlite3";
const SQLITE_APPLICATION_ID: i64 = 0x4e45_4f45;
const SQLITE_SCHEMA_VERSION: i64 = 5;
const WRITE_BATCH_SIZE: usize = 1_024;
const WRITE_BATCH_BYTES: usize = 4 * 1024 * 1024;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const INDEX_RECORD_HASH_DOMAIN: &[u8] = b"neoengram-sqlite-index-record-v1";
const INDEX_VERSION_HASH_DOMAIN: &[u8] = b"neoengram-sqlite-index-version-v1";

const SCHEMA_SQL: &str = r#"
CREATE TABLE repository_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    default_head_reference TEXT NOT NULL CHECK (
        default_head_reference <> ''
            AND instr(default_head_reference, char(10)) = 0
            AND instr(default_head_reference, char(13)) = 0
    )
) STRICT;

CREATE TABLE workspaces (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    name TEXT NOT NULL UNIQUE COLLATE BINARY CHECK (
        name <> '' AND instr(name, '/') = 0 AND instr(name, char(92)) = 0
    ),
    root_path TEXT NOT NULL UNIQUE COLLATE BINARY CHECK (root_path <> ''),
    head_kind TEXT NOT NULL CHECK (head_kind IN ('attached', 'detached')),
    head_target TEXT NOT NULL CHECK (
        head_target <> '' AND instr(head_target, char(10)) = 0
            AND instr(head_target, char(13)) = 0
    ),
    base_commit_id TEXT CHECK (
        base_commit_id IS NULL OR (
            length(base_commit_id) = 64
                AND base_commit_id = lower(base_commit_id)
        )
    ),
    index_format INTEGER NOT NULL CHECK (index_format >= 0),
    index_revision INTEGER NOT NULL CHECK (index_revision >= 0),
    index_count INTEGER NOT NULL CHECK (index_count >= 0),
    index_accumulator BLOB NOT NULL CHECK (length(index_accumulator) = 32)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX one_workspace_per_attached_ref
    ON workspaces(head_target) WHERE head_kind = 'attached';

CREATE TABLE manifest_sets (
    set_id BLOB PRIMARY KEY CHECK (length(set_id) = 16),
    total_size INTEGER NOT NULL CHECK (total_size >= 0),
    chunking INTEGER NOT NULL CHECK (chunking IN (1, 2)),
    chunk_count INTEGER CHECK (chunk_count IS NULL OR chunk_count >= 0),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    UNIQUE (set_id, total_size, chunking, chunk_count)
) STRICT;

CREATE TABLE manifest_chunks (
    set_id BLOB NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    offset INTEGER NOT NULL CHECK (offset >= 0),
    size INTEGER NOT NULL CHECK (size > 0),
    PRIMARY KEY (set_id, ordinal),
    FOREIGN KEY (set_id) REFERENCES manifest_sets(set_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX manifest_chunks_by_offset ON manifest_chunks(set_id, offset);

CREATE TABLE manifests (
    id BLOB PRIMARY KEY CHECK (length(id) = 32),
    set_id BLOB NOT NULL UNIQUE,
    total_size INTEGER NOT NULL CHECK (total_size >= 0),
    chunking INTEGER NOT NULL CHECK (chunking IN (1, 2)),
    chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
    UNIQUE (id, total_size, chunking, chunk_count),
    UNIQUE (id, total_size, chunk_count),
    FOREIGN KEY (set_id, total_size, chunking, chunk_count)
        REFERENCES manifest_sets(set_id, total_size, chunking, chunk_count) ON DELETE RESTRICT
) STRICT;

CREATE TABLE directory_sets (
    set_id BLOB PRIMARY KEY CHECK (length(set_id) = 16),
    entry_count INTEGER CHECK (entry_count IS NULL OR entry_count >= 0),
    file_count INTEGER CHECK (file_count IS NULL OR file_count >= 0),
    total_size INTEGER CHECK (total_size IS NULL OR total_size >= 0),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    UNIQUE (set_id, entry_count, file_count, total_size)
) STRICT;

CREATE TABLE directory_entries (
    set_id BLOB NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    name TEXT NOT NULL COLLATE BINARY CHECK (
        name <> '' AND name <> '.' AND name <> '..'
            AND instr(name, '/') = 0 AND instr(name, char(92)) = 0
    ),
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    target_id BLOB NOT NULL CHECK (length(target_id) = 32),
    total_size INTEGER NOT NULL CHECK (total_size >= 0),
    PRIMARY KEY (set_id, ordinal),
    UNIQUE (set_id, name),
    FOREIGN KEY (set_id) REFERENCES directory_sets(set_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE directories (
    id BLOB PRIMARY KEY CHECK (length(id) = 32),
    set_id BLOB NOT NULL UNIQUE,
    entry_count INTEGER NOT NULL CHECK (entry_count >= 0),
    file_count INTEGER NOT NULL CHECK (file_count >= 0),
    total_size INTEGER NOT NULL CHECK (total_size >= 0),
    FOREIGN KEY (set_id, entry_count, file_count, total_size)
        REFERENCES directory_sets(set_id, entry_count, file_count, total_size) ON DELETE RESTRICT
) STRICT;

CREATE TABLE workspace_index_files (
    workspace_id BLOB NOT NULL CHECK (length(workspace_id) = 16),
    path TEXT NOT NULL COLLATE BINARY,
    total_size INTEGER NOT NULL CHECK (total_size >= 0),
    chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
    manifest_id BLOB NOT NULL CHECK (length(manifest_id) = 32),
    record_digest BLOB NOT NULL CHECK (length(record_digest) = 32),
    PRIMARY KEY (workspace_id, path),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (manifest_id, total_size, chunk_count)
        REFERENCES manifests(id, total_size, chunk_count) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE commits (
    id BLOB PRIMARY KEY CHECK (length(id) = 32),
    payload BLOB NOT NULL
) STRICT;

CREATE TABLE refs (
    name TEXT PRIMARY KEY COLLATE BINARY,
    target TEXT NOT NULL CHECK (
        target <> '' AND instr(target, char(10)) = 0 AND instr(target, char(13)) = 0
    )
) STRICT, WITHOUT ROWID;
"#;

#[derive(Debug)]
pub(super) struct SqliteMetadataStore {
    root: PathBuf,
    workspace_id: WorkspaceId,
}

impl SqliteMetadataStore {
    #[cfg(test)]
    pub(super) fn new(root: PathBuf) -> Self {
        Self::for_workspace(root, DEFAULT_WORKSPACE_ID)
    }

    pub(super) fn for_workspace(root: PathBuf, workspace_id: WorkspaceId) -> Self {
        Self { root, workspace_id }
    }

    fn open_existing(&self, query_only: bool) -> Result<Connection> {
        let database = canonical_database_path(&self.root)?;
        ensure_database_file(&database)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(&database, flags)
            .with_context(|| format!("无法打开 SQLite 元数据库: {}", database.display()))?;
        configure_connection(&connection, query_only)?;
        validate_database_identity(&connection)?;
        Ok(connection)
    }

    fn read_session(&self) -> Result<Rc<ReadSession>> {
        let connection = self.open_existing(true)?;
        connection
            .execute_batch("BEGIN DEFERRED")
            .context("无法开始 SQLite 元数据读事务")?;
        // BEGIN DEFERRED does not establish a snapshot until the first read.
        connection
            .query_row(
                "SELECT 1 FROM workspaces WHERE id = ?1",
                params![self.workspace_id.as_slice()],
                |_| Ok(()),
            )
            .context("无法固定 SQLite 元数据读视图")?;
        Ok(Rc::new(ReadSession { connection }))
    }

    fn create_manifest_set(&self, total_size: u64, chunking: ChunkingStrategy) -> Result<Vec<u8>> {
        let connection = self.open_existing(false)?;
        let set_id = random_set_id(&connection)?;
        let now = current_unix_ms()?;
        run_immediate(&connection, || {
            connection.execute(
                "INSERT INTO manifest_sets(set_id, total_size, chunking, chunk_count, created_at_ms) \
                 VALUES (?1, ?2, ?3, NULL, ?4)",
                params![
                    set_id.as_slice(),
                    encode_u64(total_size, "Manifest 文件大小")?,
                    encode_chunking(chunking),
                    now
                ],
            )?;
            Ok(())
        })?;
        Ok(set_id)
    }

    fn create_directory_set(&self) -> Result<Vec<u8>> {
        let connection = self.open_existing(false)?;
        let set_id = random_set_id(&connection)?;
        let now = current_unix_ms()?;
        run_immediate(&connection, || {
            connection.execute(
                "INSERT INTO directory_sets(\
                    set_id, entry_count, file_count, total_size, created_at_ms\
                 ) VALUES (?1, NULL, NULL, NULL, ?2)",
                params![set_id.as_slice(), now],
            )?;
            Ok(())
        })?;
        Ok(set_id)
    }

    fn insert_manifest_batch(
        &self,
        set_id: &[u8],
        first_ordinal: u64,
        chunks: &[Chunk],
    ) -> Result<()> {
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            let mut statement = connection.prepare_cached(
                "INSERT INTO manifest_chunks(set_id, ordinal, object_id, offset, size) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (index, chunk) in chunks.iter().enumerate() {
                let ordinal = first_ordinal
                    .checked_add(u64::try_from(index).context("Manifest 批次序号超出 u64")?)
                    .context("Manifest Chunk 序号溢出")?;
                let object_id = decode_id(&chunk.hash)?;
                statement.execute(params![
                    set_id,
                    encode_u64(ordinal, "Manifest Chunk 序号")?,
                    object_id.as_slice(),
                    encode_u64(chunk.offset, "Manifest Chunk 偏移")?,
                    encode_u64(chunk.size, "Manifest Chunk 大小")?,
                ])?;
            }
            Ok(())
        })
    }

    fn insert_directory_batch(&self, set_id: &[u8], entries: &[DirectoryEntry]) -> Result<()> {
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            let mut statement = connection.prepare_cached(
                "INSERT INTO directory_entries(\
                    set_id, ordinal, name, kind, target_id, total_size\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for entry in entries {
                let target_id = decode_id(&entry.target_id)?;
                statement.execute(params![
                    set_id,
                    encode_u64(entry.ordinal, "Directory 子项序号")?,
                    entry.name,
                    match entry.kind {
                        DirectoryEntryKind::File => 1_i64,
                        DirectoryEntryKind::Directory => 2_i64,
                    },
                    target_id.as_slice(),
                    encode_u64(entry.total_size, "Directory 文件大小")?,
                ])?;
            }
            Ok(())
        })
    }
}

struct ReadSession {
    connection: Connection,
}

impl fmt::Debug for ReadSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReadSession(..)")
    }
}

impl Drop for ReadSession {
    fn drop(&mut self) {
        let _ = self.connection.execute_batch("ROLLBACK");
    }
}

#[derive(Debug)]
struct StoredIndexState {
    format: u32,
    revision: u64,
    count: u64,
    accumulator: [u8; 32],
    version: IndexVersion,
}

fn configure_connection(connection: &Connection, query_only: bool) -> Result<()> {
    connection
        .busy_timeout(SQLITE_BUSY_TIMEOUT)
        .context("无法设置 SQLite busy timeout")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "fullfsync", "ON")?;
    if query_only {
        connection.pragma_update(None, "query_only", "ON")?;
    }
    Ok(())
}

fn validate_database_identity(connection: &Connection) -> Result<()> {
    let application_id: i64 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    ensure!(
        application_id == SQLITE_APPLICATION_ID,
        "SQLite 元数据库 application_id 不匹配: {application_id}"
    );
    let schema_version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    ensure!(
        schema_version == SQLITE_SCHEMA_VERSION,
        "不支持的 SQLite 元数据 schema 版本 {schema_version}（当前支持 {SQLITE_SCHEMA_VERSION}）"
    );
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    ensure!(
        journal_mode.eq_ignore_ascii_case("wal"),
        "SQLite 元数据库未使用 WAL: {journal_mode}"
    );
    Ok(())
}

fn ensure_database_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("无法检查 SQLite 元数据库: {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "SQLite 元数据库不是普通文件: {}",
        path.display()
    );
    Ok(())
}

fn ensure_database_root(path: &Path) -> Result<()> {
    ensure_or_create_directory(path)
}

fn canonical_database_path(root: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(root)
        .with_context(|| format!("无法检查 SQLite 元数据目录: {}", root.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "SQLite 元数据根路径不是普通目录: {}",
        root.display()
    );
    let canonical_root = fs::canonicalize(root)
        .with_context(|| format!("无法规范化 SQLite 元数据目录: {}", root.display()))?;
    Ok(canonical_root.join(DATABASE_FILE_NAME))
}

fn validate_stored_file_record(connection: &Connection, file: &FileRecord) -> Result<()> {
    validate_file_record(file)?;
    let manifest_id = decode_id(&file.manifest_id)?;
    let stored = connection
        .query_row(
            "SELECT total_size, chunk_count FROM manifests WHERE id = ?1",
            params![manifest_id.as_slice()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .context("无法检查 FileRecord 引用的 Manifest")?;
    let Some((total_size, chunk_count)) = stored else {
        bail!("Manifest 不存在: {}", file.manifest_id);
    };
    ensure!(
        decode_u64(total_size, "Manifest 文件大小")? == file.total_size,
        "文件 {} 的大小与 Manifest 不一致",
        file.path
    );
    ensure!(
        decode_u64(chunk_count, "Manifest Chunk 数量")? == file.chunk_count,
        "文件 {} 的 Chunk 数量与 Manifest 不一致",
        file.path
    );
    Ok(())
}

fn validate_stored_directory_entry(
    connection: &Connection,
    entry: &DirectoryEntry,
) -> Result<(u64, u64)> {
    validate_directory_entry(entry)?;
    let target_id = decode_id(&entry.target_id)?;
    match entry.kind {
        DirectoryEntryKind::File => {
            let total_size = connection
                .query_row(
                    "SELECT total_size FROM manifests WHERE id = ?1",
                    params![target_id.as_slice()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .with_context(|| format!("Manifest 不存在: {}", entry.target_id))?;
            let total_size = decode_u64(total_size, "Manifest 文件大小")?;
            ensure!(
                total_size == entry.total_size,
                "Directory 文件 {} 的大小与 Manifest 不一致",
                entry.name
            );
            Ok((1, total_size))
        }
        DirectoryEntryKind::Directory => {
            let stats = connection
                .query_row(
                    "SELECT file_count, total_size FROM directories WHERE id = ?1",
                    params![target_id.as_slice()],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?
                .with_context(|| format!("Directory 不存在: {}", entry.target_id))?;
            Ok((
                decode_u64(stats.0, "Directory 文件数量")?,
                decode_u64(stats.1, "Directory 逻辑大小")?,
            ))
        }
    }
}

fn run_immediate<T>(connection: &Connection, operation: impl FnOnce() -> Result<T>) -> Result<T> {
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .context("无法开始 SQLite 元数据写事务")?;
    match operation() {
        Ok(value) => match connection.execute_batch("COMMIT") {
            Ok(()) => Ok(value),
            Err(error) => {
                let _ = connection.execute_batch("ROLLBACK");
                Err(error).context("无法提交 SQLite 元数据事务")
            }
        },
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn current_unix_ms() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 Unix Epoch")?
        .as_millis();
    i64::try_from(millis).context("当前时间超出 SQLite INTEGER 范围")
}

fn encode_u64(value: u64, kind: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{kind}超出 SQLite INTEGER 范围: {value}"))
}

fn decode_u64(value: i64, kind: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{kind}包含负数: {value}"))
}

const fn encode_chunking(chunking: ChunkingStrategy) -> i64 {
    match chunking {
        ChunkingStrategy::FastCdc => 1,
        ChunkingStrategy::WholeFile => 2,
    }
}

fn decode_chunking(value: i64) -> Result<ChunkingStrategy> {
    match value {
        1 => Ok(ChunkingStrategy::FastCdc),
        2 => Ok(ChunkingStrategy::WholeFile),
        _ => bail!("Manifest 分块策略无效: {value}"),
    }
}

fn decode_id(id: &str) -> Result<[u8; 32]> {
    validate_metadata_object_id(id)?;
    Ok(*blake3::Hash::from_hex(id)
        .with_context(|| format!("无法解析元数据对象 ID: {id}"))?
        .as_bytes())
}

fn encode_id(value: Vec<u8>, kind: &str) -> Result<String> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|value: Vec<u8>| anyhow::anyhow!("{kind} ID 长度无效: {}", value.len()))?;
    Ok(blake3::Hash::from_bytes(bytes).to_hex().to_string())
}

fn decode_digest(value: Vec<u8>, kind: &str) -> Result<[u8; 32]> {
    value
        .try_into()
        .map_err(|value: Vec<u8>| anyhow::anyhow!("{kind}摘要长度无效: {}", value.len()))
}

fn random_set_id(connection: &Connection) -> Result<Vec<u8>> {
    connection
        .query_row("SELECT randomblob(16)", [], |row| row.get(0))
        .context("无法生成 SQLite staging set ID")
}

fn index_record_digest(file: &FileRecord) -> Result<[u8; 32]> {
    let encoded = serde_json::to_vec(file).context("无法序列化 Index 文件记录")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(INDEX_RECORD_HASH_DOMAIN);
    hasher.update(&[0]);
    hasher.update(&encoded);
    Ok(*hasher.finalize().as_bytes())
}

fn index_version(format: u32, revision: u64, count: u64, accumulator: [u8; 32]) -> IndexVersion {
    let mut hasher = blake3::Hasher::new();
    hasher.update(INDEX_VERSION_HASH_DOMAIN);
    hasher.update(&[0]);
    hasher.update(&format.to_le_bytes());
    hasher.update(&count.to_le_bytes());
    hasher.update(&accumulator);
    IndexVersion::new(revision, *hasher.finalize().as_bytes())
}

fn read_index_state(
    connection: &Connection,
    workspace_id: &WorkspaceId,
) -> Result<StoredIndexState> {
    let (format, revision, count, accumulator): (i64, i64, i64, Vec<u8>) = connection
        .query_row(
            "SELECT index_format, index_revision, index_count, index_accumulator \
             FROM workspaces WHERE id = ?1",
            params![workspace_id.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .context("无法读取 SQLite Index 状态")?;
    let format = u32::try_from(format).context("Index format 超出 u32 范围")?;
    ensure!(
        format == INDEX_FORMAT_VERSION,
        "不支持的 Index 格式版本 {format}（当前支持 {INDEX_FORMAT_VERSION}）"
    );
    let revision = decode_u64(revision, "Index revision")?;
    let count = decode_u64(count, "Index 文件数量")?;
    let accumulator = decode_digest(accumulator, "Index accumulator")?;
    Ok(StoredIndexState {
        format,
        revision,
        count,
        accumulator,
        version: index_version(format, revision, count, accumulator),
    })
}

fn xor_digest(target: &mut [u8; 32], digest: &[u8; 32]) {
    for (target, digest) in target.iter_mut().zip(digest) {
        *target ^= digest;
    }
}

fn validate_file_record(file: &FileRecord) -> Result<()> {
    validate_metadata_object_id(&file.manifest_id)?;
    ensure!(
        (file.total_size == 0) == (file.chunk_count == 0),
        "文件 {} 的大小与 Chunk 数量不一致",
        file.path
    );
    encode_u64(file.total_size, "文件大小")?;
    encode_u64(file.chunk_count, "文件 Chunk 数量")?;
    Ok(())
}

fn normalize_optional_target(target: Option<&str>, kind: &str) -> Result<Option<String>> {
    target
        .map(|target| {
            let target = target.trim();
            ensure!(!target.is_empty(), "{kind}不能为空");
            ensure!(!target.contains(['\n', '\r']), "{kind}不能包含换行符");
            Ok(target.to_owned())
        })
        .transpose()
}

fn path_is_in_scope(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn reference_is_in_scope(name: &str, prefix: &str) -> bool {
    name == prefix
        || name
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn descendant_bounds(prefix: &str) -> (String, String) {
    (format!("{prefix}/"), format!("{prefix}0"))
}

fn file_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let manifest_id: Vec<u8> = row.get(3)?;
    let manifest_id = encode_id(manifest_id, "Manifest").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Blob, error.into())
    })?;
    let total_size = decode_u64(row.get(1)?, "文件大小").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Integer, error.into())
    })?;
    let chunk_count = decode_u64(row.get(2)?, "文件 Chunk 数量").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Integer, error.into())
    })?;
    Ok(FileRecord {
        path: row.get(0)?,
        total_size,
        chunk_count,
        manifest_id,
    })
}

fn directory_entry_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DirectoryEntry> {
    let ordinal = decode_u64(row.get(0)?, "Directory 子项序号").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Integer, error.into())
    })?;
    let kind = match row.get::<_, i64>(2)? {
        1 => DirectoryEntryKind::File,
        2 => DirectoryEntryKind::Directory,
        value => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Integer,
                anyhow::anyhow!("Directory 子项类型无效: {value}").into(),
            ));
        }
    };
    let target_id = encode_id(row.get(3)?, "Directory target").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Blob, error.into())
    })?;
    let total_size = decode_u64(row.get(4)?, "Directory 文件大小").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Integer, error.into())
    })?;
    Ok(DirectoryEntry {
        ordinal,
        name: row.get(1)?,
        kind,
        target_id,
        total_size,
    })
}

fn get_file_from_table(
    connection: &Connection,
    table: &str,
    workspace_id: Option<&[u8]>,
    path: &str,
) -> Result<Option<FileRecord>> {
    let (sql, values) = if let Some(workspace_id) = workspace_id {
        (
            format!(
                "SELECT path, total_size, chunk_count, manifest_id FROM {table} \
                 WHERE workspace_id = ?1 AND path = ?2"
            ),
            vec![
                Value::Blob(workspace_id.to_vec()),
                Value::Text(path.to_owned()),
            ],
        )
    } else {
        (
            format!(
                "SELECT path, total_size, chunk_count, manifest_id FROM {table} WHERE path = ?1"
            ),
            vec![Value::Text(path.to_owned())],
        )
    };
    connection
        .query_row(&sql, params_from_iter(values), file_from_row)
        .optional()
        .with_context(|| format!("无法从 {table} 点查文件: {path}"))
}

fn scan_files_from_table(
    connection: &Connection,
    table: &str,
    workspace_id: Option<&[u8]>,
    prefix: Option<&str>,
    request: &PageRequest,
) -> Result<Page<FileRecord>> {
    let limit = request.limit_usize()?;
    let retained_limit = limit.checked_add(1).context("文件分页大小溢出")?;
    let mut sql =
        format!("SELECT path, total_size, chunk_count, manifest_id FROM {table} WHERE 1 = 1");
    let mut values = Vec::new();
    if let Some(workspace_id) = workspace_id {
        sql.push_str(" AND workspace_id = ?");
        values.push(Value::Blob(workspace_id.to_vec()));
    }
    if let Some(after) = &request.after {
        sql.push_str(" AND path > ?");
        values.push(Value::Text(after.clone()));
    }
    if let Some(prefix) = prefix.filter(|prefix| !prefix.is_empty()) {
        let (descendant_start, descendant_end) = descendant_bounds(prefix);
        sql.push_str(" AND (path = ? OR (path >= ? AND path < ?))");
        values.push(Value::Text(prefix.to_owned()));
        values.push(Value::Text(descendant_start));
        values.push(Value::Text(descendant_end));
    }
    sql.push_str(" ORDER BY path COLLATE BINARY LIMIT ?");
    values.push(Value::Integer(i64::try_from(retained_limit)?));

    let mut statement = connection
        .prepare(&sql)
        .with_context(|| format!("无法准备 {table} 文件分页查询"))?;
    let rows = statement
        .query_map(params_from_iter(values), file_from_row)
        .with_context(|| format!("无法查询 {table} 文件页"))?;
    let mut items = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("无法读取 {table} 文件页"))?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next = has_more.then(|| {
        items
            .last()
            .expect("有后续文件页时当前页不可能为空")
            .path
            .clone()
    });
    Ok(Page { items, next })
}

fn scan_id_table(
    connection: &Connection,
    table: &str,
    request: &PageRequest,
) -> Result<Page<String>> {
    let limit = request.limit_usize()?;
    let retained_limit = limit.checked_add(1).context("对象 ID 分页大小溢出")?;
    let mut sql = format!("SELECT id FROM {table}");
    let mut values = Vec::new();
    if let Some(after) = &request.after {
        sql.push_str(" WHERE id > ?");
        values.push(Value::Blob(decode_id(after)?.to_vec()));
    }
    sql.push_str(" ORDER BY id LIMIT ?");
    values.push(Value::Integer(i64::try_from(retained_limit)?));
    let mut statement = connection
        .prepare(&sql)
        .with_context(|| format!("无法准备 {table} ID 分页查询"))?;
    let rows = statement.query_map(params_from_iter(values), |row| row.get::<_, Vec<u8>>(0))?;
    let encoded = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut items = encoded
        .into_iter()
        .map(|id| encode_id(id, table))
        .collect::<Result<Vec<_>>>()?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next = has_more.then(|| {
        items
            .last()
            .expect("有后续 ID 页时当前页不可能为空")
            .clone()
    });
    Ok(Page { items, next })
}

fn scan_references_from_connection(
    connection: &Connection,
    prefix: &str,
    request: &PageRequest,
) -> Result<Page<StoredReference>> {
    validate_reference_prefix(prefix)?;
    let limit = request.limit_usize()?;
    let retained_limit = limit.checked_add(1).context("引用分页大小溢出")?;
    let mut sql = String::from("SELECT name, target FROM refs WHERE 1 = 1");
    let mut values = Vec::new();
    if let Some(after) = &request.after {
        sql.push_str(" AND name > ?");
        values.push(Value::Text(after.clone()));
    }
    let (descendant_start, descendant_end) = descendant_bounds(prefix);
    sql.push_str(" AND (name = ? OR (name >= ? AND name < ?))");
    values.push(Value::Text(prefix.to_owned()));
    values.push(Value::Text(descendant_start));
    values.push(Value::Text(descendant_end));
    sql.push_str(" ORDER BY name COLLATE BINARY LIMIT ?");
    values.push(Value::Integer(i64::try_from(retained_limit)?));

    let mut statement = connection.prepare(&sql).context("无法准备引用分页查询")?;
    let rows = statement.query_map(params_from_iter(values), |row| {
        Ok(StoredReference {
            name: row.get(0)?,
            target: row.get(1)?,
        })
    })?;
    let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    for reference in &items {
        validate_reference_name(&reference.name)?;
        ensure!(
            reference_is_in_scope(&reference.name, prefix),
            "SQLite 返回了范围外引用: {}",
            reference.name
        );
        ensure!(
            normalize_optional_target(Some(&reference.target), "引用目标")?.as_deref()
                == Some(reference.target.as_str()),
            "引用目标未规范化: {}",
            reference.name
        );
    }
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next = has_more.then(|| {
        items
            .last()
            .expect("有后续引用页时当前页不可能为空")
            .name
            .clone()
    });
    Ok(Page { items, next })
}

#[derive(Debug)]
struct SqliteManifestReader {
    session: Rc<ReadSession>,
    set_id: Vec<u8>,
    total_size: u64,
    chunk_count: u64,
    chunking: ChunkingStrategy,
}

impl ManifestReader for SqliteManifestReader {
    fn total_size(&self) -> u64 {
        self.total_size
    }

    fn chunk_count(&self) -> u64 {
        self.chunk_count
    }

    fn chunking(&self) -> ChunkingStrategy {
        self.chunking
    }

    fn scan_chunks(&self, request: &PageRequest) -> Result<Page<Chunk>> {
        let limit = request.limit_usize()?;
        let retained_limit = limit.checked_add(1).context("Chunk 分页大小溢出")?;
        let after = request
            .after
            .as_deref()
            .map(|cursor| {
                cursor
                    .parse::<u64>()
                    .with_context(|| format!("Chunk cursor 无效: {cursor}"))
            })
            .transpose()?;
        if let Some(after) = after {
            ensure!(after < self.chunk_count, "Chunk cursor 超出 Manifest 范围");
        }
        let after_sql = after
            .map(|value| encode_u64(value, "Chunk cursor"))
            .transpose()?
            .unwrap_or(-1);
        let mut statement = self.session.connection.prepare(
            "SELECT ordinal, object_id, offset, size FROM manifest_chunks \
             WHERE set_id = ?1 AND ordinal > ?2 ORDER BY ordinal LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![
                self.set_id.as_slice(),
                after_sql,
                i64::try_from(retained_limit)?
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )?;
        let stored = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut expected_ordinal = after.map_or(0, |value| value + 1);
        let mut chunks = Vec::with_capacity(stored.len());
        let mut previous_end = if expected_ordinal == 0 {
            0
        } else {
            let (offset, size): (i64, i64) = self.session.connection.query_row(
                "SELECT offset, size FROM manifest_chunks WHERE set_id = ?1 AND ordinal = ?2",
                params![
                    self.set_id.as_slice(),
                    encode_u64(expected_ordinal - 1, "Chunk 序号")?
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            decode_u64(offset, "Chunk 偏移")?
                .checked_add(decode_u64(size, "Chunk 大小")?)
                .context("Manifest Chunk 偏移溢出")?
        };
        for (ordinal, object_id, offset, size) in stored {
            let ordinal = decode_u64(ordinal, "Chunk 序号")?;
            ensure!(
                ordinal == expected_ordinal,
                "Manifest Chunk 序号不连续: {ordinal}"
            );
            let offset = decode_u64(offset, "Chunk 偏移")?;
            let size = decode_u64(size, "Chunk 大小")?;
            ensure!(offset == previous_end, "Manifest Chunk 偏移不连续");
            ensure!(size > 0, "Manifest Chunk 大小必须大于零");
            previous_end = offset
                .checked_add(size)
                .context("Manifest Chunk 偏移溢出")?;
            chunks.push(Chunk {
                hash: encode_id(object_id, "Chunk")?,
                offset,
                size,
            });
            expected_ordinal = expected_ordinal
                .checked_add(1)
                .context("Manifest Chunk 序号溢出")?;
        }
        let has_more = chunks.len() > limit;
        if has_more {
            chunks.pop();
            expected_ordinal = expected_ordinal
                .checked_sub(1)
                .context("Manifest Chunk 序号下溢")?;
        } else {
            ensure!(
                expected_ordinal == self.chunk_count,
                "Manifest Chunk 数量与 header 不一致"
            );
            ensure!(
                previous_end == self.total_size,
                "Manifest Chunk 总大小与 header 不一致"
            );
        }
        let next = has_more.then(|| {
            expected_ordinal
                .checked_sub(1)
                .expect("有后续 Chunk 页时当前页不可能为空")
                .to_string()
        });
        Ok(Page {
            items: chunks,
            next,
        })
    }

    fn scan_chunks_from_offset(&self, offset: u64, limit: NonZeroU32) -> Result<Page<Chunk>> {
        ensure!(offset <= self.total_size, "Manifest 读取偏移超出文件范围");
        let limit = PageRequest::new(None, limit.get())?.limit_usize()?;
        let retained_limit = limit.checked_add(1).context("Chunk range 页大小溢出")?;
        let mut statement = self.session.connection.prepare(
            "SELECT ordinal, object_id, offset, size FROM manifest_chunks \
             WHERE set_id = ?1 AND offset + size > ?2 \
             ORDER BY ordinal LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![
                self.set_id.as_slice(),
                encode_u64(offset, "Manifest 读取偏移")?,
                i64::try_from(retained_limit)?
            ],
            |row| {
                Ok(Chunk {
                    hash: encode_id(row.get(1)?, "Chunk").map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Blob,
                            error.into(),
                        )
                    })?,
                    offset: decode_u64(row.get(2)?, "Chunk 偏移").map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Integer,
                            error.into(),
                        )
                    })?,
                    size: decode_u64(row.get(3)?, "Chunk 大小").map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Integer,
                            error.into(),
                        )
                    })?,
                })
            },
        )?;
        let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let has_more = items.len() > limit;
        if has_more {
            items.pop();
        }
        let next = has_more.then(|| {
            let last = items
                .last()
                .expect("有后续 Chunk range 页时当前页不可能为空");
            last.offset
                .checked_add(last.size)
                .expect("已验证的 Manifest Chunk 结束偏移不会溢出")
                .to_string()
        });
        Ok(Page { items, next })
    }
}

#[derive(Debug)]
struct SqliteDirectoryReader {
    session: Rc<ReadSession>,
    set_id: Vec<u8>,
    entry_count: u64,
    file_count: u64,
    total_size: u64,
}

impl DirectoryReader for SqliteDirectoryReader {
    fn entry_count(&self) -> u64 {
        self.entry_count
    }

    fn file_count(&self) -> u64 {
        self.file_count
    }

    fn total_size(&self) -> u64 {
        self.total_size
    }

    fn get_entry(&self, name: &str) -> Result<Option<DirectoryEntry>> {
        self.session
            .connection
            .query_row(
                "SELECT ordinal, name, kind, target_id, total_size \
                 FROM directory_entries WHERE set_id = ?1 AND name = ?2",
                params![self.set_id.as_slice(), name],
                directory_entry_from_row,
            )
            .optional()
            .with_context(|| format!("无法点查 Directory 子项: {name}"))
    }

    fn scan_entries(&self, request: &PageRequest) -> Result<Page<DirectoryEntry>> {
        let limit = request.limit_usize()?;
        let retained_limit = limit.checked_add(1).context("Directory 页大小溢出")?;
        let after = request
            .after
            .as_deref()
            .map(|value| value.parse::<u64>().context("Directory cursor 无效"))
            .transpose()?;
        let after = after
            .map(|value| encode_u64(value, "Directory cursor"))
            .transpose()?
            .unwrap_or(-1);
        let mut statement = self.session.connection.prepare(
            "SELECT ordinal, name, kind, target_id, total_size \
             FROM directory_entries WHERE set_id = ?1 AND ordinal > ?2 \
             ORDER BY ordinal LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![
                self.set_id.as_slice(),
                after,
                i64::try_from(retained_limit)?
            ],
            directory_entry_from_row,
        )?;
        let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let has_more = items.len() > limit;
        if has_more {
            items.pop();
        }
        let next = has_more.then(|| {
            items
                .last()
                .expect("有后续 Directory 页时当前页不可能为空")
                .ordinal
                .to_string()
        });
        Ok(Page { items, next })
    }
}

#[derive(Debug)]
struct SqliteIndexReader {
    session: Rc<ReadSession>,
    workspace_id: WorkspaceId,
    state: StoredIndexState,
}

impl FileSetReader for SqliteIndexReader {
    fn get_file(&self, path: &str) -> Result<Option<FileRecord>> {
        get_file_from_table(
            &self.session.connection,
            "workspace_index_files",
            Some(self.workspace_id.as_slice()),
            path,
        )
    }

    fn scan_files(&self, prefix: Option<&str>, request: &PageRequest) -> Result<Page<FileRecord>> {
        scan_files_from_table(
            &self.session.connection,
            "workspace_index_files",
            Some(self.workspace_id.as_slice()),
            prefix,
            request,
        )
    }
}

impl IndexReader for SqliteIndexReader {
    fn format_version(&self) -> u32 {
        self.state.format
    }

    fn version(&self) -> &IndexVersion {
        &self.state.version
    }
}

struct SqliteIndexTxn<'a> {
    store: &'a SqliteMetadataStore,
    base_connection: Option<Connection>,
    validation_connection: Connection,
    state: StoredIndexState,
    deleted_prefixes: Vec<String>,
    upserts: BTreeMap<String, FileRecord>,
}

impl fmt::Debug for SqliteIndexTxn<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteIndexTxn")
            .field("state", &self.state)
            .field("deleted_prefixes", &self.deleted_prefixes)
            .field("upsert_count", &self.upserts.len())
            .finish_non_exhaustive()
    }
}

impl SqliteIndexTxn<'_> {
    fn base_connection(&self) -> Result<&Connection> {
        self.base_connection
            .as_ref()
            .context("SQLite Index 事务的 base snapshot 已结束")
    }

    fn base_file_deleted(&self, path: &str) -> bool {
        self.deleted_prefixes
            .iter()
            .any(|prefix| path_is_in_scope(path, prefix))
    }

    fn scan_current_files(
        &self,
        prefix: Option<&str>,
        request: &PageRequest,
    ) -> Result<Page<FileRecord>> {
        let limit = request.limit_usize()?;
        let retained_limit = limit.checked_add(1).context("Index 事务分页大小溢出")?;
        let mut retained = BTreeMap::<String, FileRecord>::new();

        for file in self.upserts.values().filter(|file| {
            request
                .after
                .as_ref()
                .is_none_or(|after| file.path.as_str() > after.as_str())
                && prefix.is_none_or(|prefix| path_is_in_scope(&file.path, prefix))
        }) {
            retained.insert(file.path.clone(), file.clone());
            if retained.len() == retained_limit {
                break;
            }
        }

        let mut base_request = PageRequest::new(request.after.clone(), 4_096)?;
        loop {
            let page = scan_files_from_table(
                self.base_connection()?,
                "workspace_index_files",
                Some(self.store.workspace_id.as_slice()),
                prefix,
                &base_request,
            )?;
            let last_scanned = page.items.last().map(|file| file.path.clone());
            for file in page.items {
                if self.base_file_deleted(&file.path) || self.upserts.contains_key(&file.path) {
                    continue;
                }
                retained.insert(file.path.clone(), file);
                if retained.len() > retained_limit {
                    retained.pop_last();
                }
            }

            let no_later_base = page.next.is_none();
            let passed_retained_end = retained.len() >= retained_limit
                && last_scanned.as_ref().is_some_and(|last_scanned| {
                    retained
                        .last_key_value()
                        .is_some_and(|(last_retained, _)| last_scanned >= last_retained)
                });
            if no_later_base || passed_retained_end {
                break;
            }
            base_request.after = page.next;
        }

        let has_more = retained.len() > limit;
        if has_more {
            retained.pop_last();
        }
        let items = retained.into_values().collect::<Vec<_>>();
        let next = has_more.then(|| {
            items
                .last()
                .expect("有后续 Index 事务页时当前页不可能为空")
                .path
                .clone()
        });
        Ok(Page { items, next })
    }
}

impl FileSetReader for SqliteIndexTxn<'_> {
    fn get_file(&self, path: &str) -> Result<Option<FileRecord>> {
        if let Some(file) = self.upserts.get(path) {
            return Ok(Some(file.clone()));
        }
        if self.base_file_deleted(path) {
            return Ok(None);
        }
        get_file_from_table(
            self.base_connection()?,
            "workspace_index_files",
            Some(self.store.workspace_id.as_slice()),
            path,
        )
    }

    fn scan_files(&self, prefix: Option<&str>, request: &PageRequest) -> Result<Page<FileRecord>> {
        self.scan_current_files(prefix, request)
    }
}

impl IndexReader for SqliteIndexTxn<'_> {
    fn format_version(&self) -> u32 {
        self.state.format
    }

    fn version(&self) -> &IndexVersion {
        &self.state.version
    }
}

impl IndexTxn for SqliteIndexTxn<'_> {
    fn upsert_file(&mut self, file: &FileRecord) -> Result<()> {
        // This autocommit connection sees Manifests published after the fixed base snapshot opened.
        validate_stored_file_record(&self.validation_connection, file)?;
        self.upserts.insert(file.path.clone(), file.clone());
        Ok(())
    }

    fn delete_prefix(&mut self, prefix: &str) -> Result<u64> {
        let mut request = PageRequest::first(4_096)?;
        let mut deleted = 0_u64;
        loop {
            let page = self.scan_current_files(Some(prefix), &request)?;
            deleted = deleted
                .checked_add(u64::try_from(page.items.len()).context("Index 删除计数超出 u64")?)
                .context("Index 删除计数溢出")?;
            let Some(next) = page.next else {
                break;
            };
            request.after = Some(next);
        }

        self.upserts
            .retain(|path, _| !path_is_in_scope(path, prefix));
        if !self
            .deleted_prefixes
            .iter()
            .any(|existing| path_is_in_scope(prefix, existing))
        {
            self.deleted_prefixes
                .retain(|existing| !path_is_in_scope(existing, prefix));
            self.deleted_prefixes.push(prefix.to_owned());
        }
        Ok(deleted)
    }

    fn commit(mut self: Box<Self>) -> Result<IndexVersion> {
        // End the fixed read snapshot before taking the writer lock. Upgrading an old WAL snapshot
        // would otherwise fail with SQLITE_BUSY_SNAPSHOT and could block nested metadata writes.
        drop(self.base_connection.take());
        let connection = self.store.open_existing(false)?;
        run_immediate(&connection, || {
            let current = read_index_state(&connection, &self.store.workspace_id)?;
            ensure!(
                current.version == self.state.version,
                "Index 在事务执行期间被其他操作更新"
            );
            let mut count = current.count;
            let mut accumulator = current.accumulator;

            for prefix in &self.deleted_prefixes {
                apply_index_delete(
                    &connection,
                    &self.store.workspace_id,
                    prefix,
                    &mut count,
                    &mut accumulator,
                )?;
            }
            for file in self.upserts.values() {
                apply_index_upsert(
                    &connection,
                    &self.store.workspace_id,
                    file,
                    &mut count,
                    &mut accumulator,
                )?;
            }

            let revision = current
                .revision
                .checked_add(1)
                .context("Index revision 溢出")?;
            connection.execute(
                "UPDATE workspaces SET index_revision = ?1, index_count = ?2, \
                 index_accumulator = ?3 WHERE id = ?4",
                params![
                    encode_u64(revision, "Index revision")?,
                    encode_u64(count, "Index 文件数量")?,
                    accumulator.as_slice(),
                    self.store.workspace_id.as_slice(),
                ],
            )?;
            Ok(index_version(current.format, revision, count, accumulator))
        })
    }
}

fn apply_index_delete(
    connection: &Connection,
    workspace_id: &WorkspaceId,
    prefix: &str,
    count: &mut u64,
    accumulator: &mut [u8; 32],
) -> Result<()> {
    let (select_sql, delete_sql, values) = if prefix.is_empty() {
        (
            "SELECT record_digest FROM workspace_index_files WHERE workspace_id = ?1".to_owned(),
            "DELETE FROM workspace_index_files WHERE workspace_id = ?1".to_owned(),
            vec![Value::Blob(workspace_id.to_vec())],
        )
    } else {
        let (descendant_start, descendant_end) = descendant_bounds(prefix);
        (
            "SELECT record_digest FROM workspace_index_files \
             WHERE workspace_id = ?1 AND (path = ?2 OR (path >= ?3 AND path < ?4))"
                .to_owned(),
            "DELETE FROM workspace_index_files WHERE workspace_id = ?1 \
             AND (path = ?2 OR (path >= ?3 AND path < ?4))"
                .to_owned(),
            vec![
                Value::Blob(workspace_id.to_vec()),
                Value::Text(prefix.to_owned()),
                Value::Text(descendant_start),
                Value::Text(descendant_end),
            ],
        )
    };
    let mut removed = 0_u64;
    {
        let mut statement = connection.prepare(&select_sql)?;
        let mut rows = statement.query(params_from_iter(values.iter()))?;
        while let Some(row) = rows.next()? {
            let digest = decode_digest(row.get(0)?, "Index record")?;
            xor_digest(accumulator, &digest);
            removed = removed.checked_add(1).context("Index 删除计数溢出")?;
        }
    }
    connection.execute(&delete_sql, params_from_iter(values))?;
    *count = count.checked_sub(removed).context("Index 文件数量下溢")?;
    Ok(())
}

fn apply_index_upsert(
    connection: &Connection,
    workspace_id: &WorkspaceId,
    file: &FileRecord,
    count: &mut u64,
    accumulator: &mut [u8; 32],
) -> Result<()> {
    validate_file_record(file)?;
    let old_digest = connection
        .query_row(
            "SELECT record_digest FROM workspace_index_files \
             WHERE workspace_id = ?1 AND path = ?2",
            params![workspace_id.as_slice(), file.path],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    if let Some(old_digest) = old_digest {
        xor_digest(accumulator, &decode_digest(old_digest, "Index 原记录")?);
    } else {
        *count = count.checked_add(1).context("Index 文件数量溢出")?;
    }
    let digest = index_record_digest(file)?;
    xor_digest(accumulator, &digest);
    let manifest_id = decode_id(&file.manifest_id)?;
    connection.execute(
        "INSERT INTO workspace_index_files(\
             workspace_id, path, total_size, chunk_count, manifest_id, record_digest\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT(workspace_id, path) DO UPDATE SET total_size = excluded.total_size, \
         chunk_count = excluded.chunk_count, manifest_id = excluded.manifest_id, \
         record_digest = excluded.record_digest",
        params![
            workspace_id.as_slice(),
            file.path,
            encode_u64(file.total_size, "Index 文件大小")?,
            encode_u64(file.chunk_count, "Index Chunk 数量")?,
            manifest_id.as_slice(),
            digest.as_slice(),
        ],
    )?;
    Ok(())
}

struct SqliteDirectoryWriter<'a> {
    store: &'a SqliteMetadataStore,
    validation_connection: Connection,
    set_id: Vec<u8>,
    hasher: Option<DirectoryHasher>,
    last_name: Option<String>,
    batch: Vec<DirectoryEntry>,
    batch_bytes: usize,
    file_count: u64,
    total_size: u64,
    poisoned: bool,
    cleanup_required: bool,
}

impl fmt::Debug for SqliteDirectoryWriter<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteDirectoryWriter")
            .field("set_id", &self.set_id)
            .field("last_name", &self.last_name)
            .field("batch_len", &self.batch.len())
            .field("batch_bytes", &self.batch_bytes)
            .field("poisoned", &self.poisoned)
            .field("cleanup_required", &self.cleanup_required)
            .finish_non_exhaustive()
    }
}

impl SqliteDirectoryWriter<'_> {
    fn flush(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.batch);
        self.batch_bytes = 0;
        if let Err(error) = self.store.insert_directory_batch(&self.set_id, &batch) {
            self.poisoned = true;
            return Err(error).context("无法写入 SQLite Directory staging 批次");
        }
        Ok(())
    }
}

impl Drop for SqliteDirectoryWriter<'_> {
    fn drop(&mut self) {
        if self.cleanup_required {
            let _ = cleanup_directory_set(self.store, &self.set_id);
        }
    }
}

impl DirectoryWriter for SqliteDirectoryWriter<'_> {
    fn append_entry(&mut self, entry: &DirectoryEntry) -> Result<()> {
        ensure!(
            !self.poisoned,
            "SQLite DirectoryWriter 已因先前写入错误失效"
        );
        let (file_count, total_size) =
            validate_stored_directory_entry(&self.validation_connection, entry)?;
        if let Some(previous) = &self.last_name {
            ensure!(
                previous.as_str() < entry.name.as_str(),
                "DirectoryWriter 要求子项名称严格递增: {}",
                entry.name
            );
        }
        let next_batch_bytes = self
            .batch_bytes
            .checked_add(entry.name.len().saturating_add(96))
            .context("Directory staging 批次大小溢出")?;
        self.hasher
            .as_mut()
            .context("SQLite DirectoryWriter hasher 已结束")?
            .push(entry)?;
        self.file_count = self
            .file_count
            .checked_add(file_count)
            .context("Directory 文件数量溢出")?;
        self.total_size = self
            .total_size
            .checked_add(total_size)
            .context("Directory 逻辑大小溢出")?;
        self.batch_bytes = next_batch_bytes;
        self.last_name = Some(entry.name.clone());
        self.batch.push(entry.clone());
        if self.batch.len() >= WRITE_BATCH_SIZE || self.batch_bytes >= WRITE_BATCH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<DirectoryRef> {
        ensure!(
            !self.poisoned,
            "SQLite DirectoryWriter 已因先前写入错误失效"
        );
        self.flush()?;
        let (id, entry_count) = self
            .hasher
            .take()
            .context("SQLite DirectoryWriter hasher 已结束")?
            .finish();
        let directory = DirectoryRef {
            id,
            entry_count,
            file_count: self.file_count,
            total_size: self.total_size,
        };
        let created = publish_directory_set(self.store, &self.set_id, &directory)?;
        if !created {
            let _ = cleanup_directory_set(self.store, &self.set_id);
        }
        self.cleanup_required = false;
        Ok(directory)
    }
}

fn publish_manifest_set(
    store: &SqliteMetadataStore,
    set_id: &[u8],
    manifest: &ManifestRef,
    total_size: u64,
) -> Result<bool> {
    let connection = store.open_existing(false)?;
    let id = decode_id(&manifest.id)?;
    run_immediate(&connection, || {
        let updated = connection.execute(
            "UPDATE manifest_sets SET chunk_count = ?1 \
             WHERE set_id = ?2 AND chunk_count IS NULL",
            params![
                encode_u64(manifest.chunk_count, "Manifest Chunk 数量")?,
                set_id
            ],
        )?;
        ensure!(updated == 1, "Manifest staging set 不存在或已经结束");

        let existing = connection
            .query_row(
                "SELECT set_id, total_size, chunking, chunk_count FROM manifests WHERE id = ?1",
                params![id.as_slice()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some((existing_set, existing_size, existing_chunking, existing_count)) = existing {
            ensure!(
                decode_u64(existing_size, "已有 Manifest 文件大小")? == total_size
                    && decode_chunking(existing_chunking)? == manifest.chunking
                    && decode_u64(existing_count, "已有 Manifest Chunk 数量")?
                        == manifest.chunk_count
                    && manifest_sets_equal(&connection, &existing_set, set_id)?,
                "同一 Manifest ID 已存在不同内容: {}",
                manifest.id
            );
            return Ok(false);
        }

        connection.execute(
            "INSERT INTO manifests(id, set_id, total_size, chunking, chunk_count) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id.as_slice(),
                set_id,
                encode_u64(total_size, "Manifest 文件大小")?,
                encode_chunking(manifest.chunking),
                encode_u64(manifest.chunk_count, "Manifest Chunk 数量")?,
            ],
        )?;
        Ok(true)
    })
}

fn publish_directory_set(
    store: &SqliteMetadataStore,
    set_id: &[u8],
    directory: &DirectoryRef,
) -> Result<bool> {
    let connection = store.open_existing(false)?;
    let id_bytes = decode_id(&directory.id)?;
    run_immediate(&connection, || {
        let updated = connection.execute(
            "UPDATE directory_sets SET entry_count = ?1, file_count = ?2, total_size = ?3 \
             WHERE set_id = ?4 AND entry_count IS NULL",
            params![
                encode_u64(directory.entry_count, "Directory 子项数量")?,
                encode_u64(directory.file_count, "Directory 文件数量")?,
                encode_u64(directory.total_size, "Directory 逻辑大小")?,
                set_id
            ],
        )?;
        ensure!(updated == 1, "Directory staging set 不存在或已经结束");

        let existing = connection
            .query_row(
                "SELECT set_id, entry_count, file_count, total_size \
                 FROM directories WHERE id = ?1",
                params![id_bytes.as_slice()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some((existing_set, entries, files, size)) = existing {
            ensure!(
                decode_u64(entries, "已有 Directory 子项数量")? == directory.entry_count
                    && decode_u64(files, "已有 Directory 文件数量")? == directory.file_count
                    && decode_u64(size, "已有 Directory 逻辑大小")? == directory.total_size
                    && directory_sets_equal(&connection, &existing_set, set_id)?,
                "同一 Directory ID 已存在不同内容: {}",
                directory.id
            );
            return Ok(false);
        }

        connection.execute(
            "INSERT INTO directories(\
                id, set_id, entry_count, file_count, total_size\
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id_bytes.as_slice(),
                set_id,
                encode_u64(directory.entry_count, "Directory 子项数量")?,
                encode_u64(directory.file_count, "Directory 文件数量")?,
                encode_u64(directory.total_size, "Directory 逻辑大小")?,
            ],
        )?;
        Ok(true)
    })
}

fn manifest_sets_equal(connection: &Connection, left: &[u8], right: &[u8]) -> Result<bool> {
    let different: i64 = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM manifest_chunks AS left_chunk \
             LEFT JOIN manifest_chunks AS right_chunk \
               ON right_chunk.set_id = ?2 AND right_chunk.ordinal = left_chunk.ordinal \
             WHERE left_chunk.set_id = ?1 AND (right_chunk.ordinal IS NULL \
                OR right_chunk.object_id <> left_chunk.object_id \
                OR right_chunk.offset <> left_chunk.offset \
                OR right_chunk.size <> left_chunk.size) \
             UNION ALL \
             SELECT 1 FROM manifest_chunks AS right_chunk \
             LEFT JOIN manifest_chunks AS left_chunk \
               ON left_chunk.set_id = ?1 AND left_chunk.ordinal = right_chunk.ordinal \
             WHERE right_chunk.set_id = ?2 AND left_chunk.ordinal IS NULL \
             LIMIT 1 \
         )",
        params![left, right],
        |row| row.get(0),
    )?;
    Ok(different == 0)
}

fn directory_sets_equal(connection: &Connection, left: &[u8], right: &[u8]) -> Result<bool> {
    let different: i64 = connection.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM directory_entries AS left_entry \
             LEFT JOIN directory_entries AS right_entry \
               ON right_entry.set_id = ?2 AND right_entry.ordinal = left_entry.ordinal \
             WHERE left_entry.set_id = ?1 AND (right_entry.ordinal IS NULL \
                OR right_entry.name <> left_entry.name \
                OR right_entry.kind <> left_entry.kind \
                OR right_entry.target_id <> left_entry.target_id \
                OR right_entry.total_size <> left_entry.total_size) \
             UNION ALL \
             SELECT 1 FROM directory_entries AS right_entry \
             LEFT JOIN directory_entries AS left_entry \
               ON left_entry.set_id = ?1 AND left_entry.ordinal = right_entry.ordinal \
             WHERE right_entry.set_id = ?2 AND left_entry.ordinal IS NULL \
             LIMIT 1 \
         )",
        params![left, right],
        |row| row.get(0),
    )?;
    Ok(different == 0)
}

fn cleanup_manifest_set(store: &SqliteMetadataStore, set_id: &[u8]) -> Result<()> {
    let connection = store.open_existing(false)?;
    run_immediate(&connection, || {
        connection.execute(
            "DELETE FROM manifest_sets WHERE set_id = ?1 \
             AND NOT EXISTS (SELECT 1 FROM manifests WHERE manifests.set_id = manifest_sets.set_id)",
            params![set_id],
        )?;
        Ok(())
    })
}

fn cleanup_directory_set(store: &SqliteMetadataStore, set_id: &[u8]) -> Result<()> {
    let connection = store.open_existing(false)?;
    run_immediate(&connection, || {
        connection.execute(
            "DELETE FROM directory_sets WHERE set_id = ?1 \
             AND NOT EXISTS (\
                 SELECT 1 FROM directories \
                 WHERE directories.set_id = directory_sets.set_id\
             )",
            params![set_id],
        )?;
        Ok(())
    })
}

fn read_head_state(connection: &Connection, workspace_id: &WorkspaceId) -> Result<HeadState> {
    let (kind, target): (String, String) = connection
        .query_row(
            "SELECT head_kind, head_target FROM workspaces WHERE id = ?1",
            params![workspace_id.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("无法读取 SQLite HEAD 状态")?;
    let state = match kind.as_str() {
        "attached" => HeadState::Attached {
            reference: target.trim().to_owned(),
        },
        "detached" => HeadState::Detached {
            commit_id: target.trim().to_owned(),
        },
        _ => bail!("SQLite HEAD 类型无效: {kind}"),
    };
    state.validate()?;
    Ok(state)
}

fn encode_head_state(state: &HeadState) -> Result<(&'static str, &str)> {
    state.validate()?;
    Ok(match state {
        HeadState::Attached { reference } => ("attached", reference),
        HeadState::Detached { commit_id } => ("detached", commit_id),
    })
}

fn validate_workspace_name(name: &str) -> Result<&str> {
    ensure!(name == name.trim(), "Workspace 名称不能包含首尾空白");
    ensure!(!name.is_empty(), "Workspace 名称不能为空");
    ensure!(name != "." && name != "..", "Workspace 名称无效: {name}");
    ensure!(
        !name
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\')),
        "Workspace 名称包含非法字符: {name}"
    );
    Ok(name)
}

fn validate_workspace_root_path(path: &str) -> Result<&str> {
    ensure!(path == path.trim(), "Workspace 路径不能包含首尾空白");
    ensure!(!path.is_empty(), "Workspace 路径不能为空");
    ensure!(
        !path.chars().any(char::is_control),
        "Workspace 路径包含控制字符"
    );
    Ok(path)
}

fn workspace_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspaceRecord> {
    let id: Vec<u8> = row.get(0)?;
    let id: WorkspaceId = id.try_into().map_err(|id: Vec<u8>| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            anyhow::anyhow!("Workspace ID 长度无效: {}", id.len()).into(),
        )
    })?;
    let kind: String = row.get(3)?;
    let target: String = row.get(4)?;
    let head = match kind.as_str() {
        "attached" => HeadState::Attached { reference: target },
        "detached" => HeadState::Detached { commit_id: target },
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                anyhow::anyhow!("Workspace HEAD 类型无效: {kind}").into(),
            ));
        }
    };
    Ok(WorkspaceRecord {
        id,
        name: row.get(1)?,
        root_path: row.get(2)?,
        head,
        base_commit_id: row.get(5)?,
    })
}

fn validate_workspace_record(record: &WorkspaceRecord) -> Result<()> {
    validate_workspace_name(&record.name)?;
    validate_workspace_root_path(&record.root_path)?;
    record.head.validate()?;
    if let Some(base_commit_id) = &record.base_commit_id {
        validate_metadata_object_id(base_commit_id)?;
    }
    if let HeadState::Detached { commit_id } = &record.head {
        ensure!(
            record.base_commit_id.as_deref() == Some(commit_id),
            "Detached Workspace 的 HEAD 与 base Commit 不一致"
        );
    }
    Ok(())
}

fn get_reference_from_connection(connection: &Connection, name: &str) -> Result<Option<String>> {
    validate_reference_name(name)?;
    let target = connection
        .query_row(
            "SELECT target FROM refs WHERE name = ?1",
            params![name],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .with_context(|| format!("无法读取 SQLite 引用: {name}"))?;
    if let Some(target) = &target {
        ensure!(
            normalize_optional_target(Some(target), "引用目标")?.as_deref()
                == Some(target.as_str()),
            "引用目标未规范化: {name}"
        );
    }
    Ok(target)
}

fn open_manifest_in_session(
    session: Rc<ReadSession>,
    id: &str,
) -> Result<Option<Box<dyn ManifestReader>>> {
    let id_bytes = decode_id(id)?;
    let stored = session
        .connection
        .query_row(
            "SELECT set_id, total_size, chunking, chunk_count FROM manifests WHERE id = ?1",
            params![id_bytes.as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .with_context(|| format!("无法打开 SQLite Manifest: {id}"))?;
    stored
        .map(|(set_id, total_size, chunking, chunk_count)| {
            Ok(Box::new(SqliteManifestReader {
                session,
                set_id,
                total_size: decode_u64(total_size, "Manifest 文件大小")?,
                chunk_count: decode_u64(chunk_count, "Manifest Chunk 数量")?,
                chunking: decode_chunking(chunking)?,
            }) as Box<dyn ManifestReader>)
        })
        .transpose()
}

fn open_directory_in_session(
    session: Rc<ReadSession>,
    id: &str,
) -> Result<Option<Box<dyn DirectoryReader>>> {
    let id_bytes = decode_id(id)?;
    let stored = session
        .connection
        .query_row(
            "SELECT set_id, entry_count, file_count, total_size \
             FROM directories WHERE id = ?1",
            params![id_bytes.as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .with_context(|| format!("无法打开 SQLite Directory: {id}"))?;
    stored
        .map(|(set_id, entry_count, file_count, total_size)| {
            Ok(Box::new(SqliteDirectoryReader {
                session,
                set_id,
                entry_count: decode_u64(entry_count, "Directory 子项数量")?,
                file_count: decode_u64(file_count, "Directory 文件数量")?,
                total_size: decode_u64(total_size, "Directory 逻辑大小")?,
            }) as Box<dyn DirectoryReader>)
        })
        .transpose()
}

fn get_commit_from_connection(connection: &Connection, id: &str) -> Result<Option<Commit>> {
    let id_bytes = decode_id(id)?;
    let payload = connection
        .query_row(
            "SELECT payload FROM commits WHERE id = ?1",
            params![id_bytes.as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .with_context(|| format!("无法读取 SQLite Commit: {id}"))?;
    payload
        .map(|payload| {
            serde_json::from_slice(&payload).with_context(|| format!("Commit 数据损坏: {id}"))
        })
        .transpose()
}

impl MetadataReader for SqliteMetadataStore {
    fn read_head_state(&self) -> Result<HeadState> {
        let connection = self.open_existing(true)?;
        read_head_state(&connection, &self.workspace_id)
    }

    fn get_reference(&self, name: &str) -> Result<Option<String>> {
        let connection = self.open_existing(true)?;
        get_reference_from_connection(&connection, name)
    }

    fn open_manifest(&self, id: &str) -> Result<Option<Box<dyn ManifestReader + '_>>> {
        open_manifest_in_session(self.read_session()?, id)
    }

    fn open_directory(&self, id: &str) -> Result<Option<Box<dyn DirectoryReader + '_>>> {
        open_directory_in_session(self.read_session()?, id)
    }

    fn get_commit(&self, id: &str) -> Result<Option<Commit>> {
        let connection = self.open_existing(true)?;
        get_commit_from_connection(&connection, id)
    }
}

#[derive(Debug)]
struct SqliteMetadataSnapshot {
    session: Rc<ReadSession>,
    workspace_id: WorkspaceId,
}

impl MetadataReader for SqliteMetadataSnapshot {
    fn read_head_state(&self) -> Result<HeadState> {
        read_head_state(&self.session.connection, &self.workspace_id)
    }

    fn get_reference(&self, name: &str) -> Result<Option<String>> {
        get_reference_from_connection(&self.session.connection, name)
    }

    fn open_manifest(&self, id: &str) -> Result<Option<Box<dyn ManifestReader + '_>>> {
        open_manifest_in_session(Rc::clone(&self.session), id)
    }

    fn open_directory(&self, id: &str) -> Result<Option<Box<dyn DirectoryReader + '_>>> {
        open_directory_in_session(Rc::clone(&self.session), id)
    }

    fn get_commit(&self, id: &str) -> Result<Option<Commit>> {
        get_commit_from_connection(&self.session.connection, id)
    }
}

impl MetadataSnapshot for SqliteMetadataSnapshot {
    fn scan_directory_ids(&self, request: &PageRequest) -> Result<Page<String>> {
        scan_id_table(&self.session.connection, "directories", request)
    }

    fn scan_manifest_ids(&self, request: &PageRequest) -> Result<Page<String>> {
        scan_id_table(&self.session.connection, "manifests", request)
    }

    fn scan_commit_ids(&self, request: &PageRequest) -> Result<Page<String>> {
        scan_id_table(&self.session.connection, "commits", request)
    }

    fn scan_references(
        &self,
        prefix: &str,
        request: &PageRequest,
    ) -> Result<Page<StoredReference>> {
        scan_references_from_connection(&self.session.connection, prefix, request)
    }
}

impl MetadataStore for SqliteMetadataStore {
    fn initialize(&self, head_reference: &str) -> Result<()> {
        validate_reference_name(head_reference)?;
        ensure_database_root(&self.root)?;
        let database = canonical_database_path(&self.root)?;
        let database_existed = database.try_exists()?;
        if database_existed {
            ensure_database_file(&database)?;
        }
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(&database, flags)
            .with_context(|| format!("无法创建 SQLite 元数据库: {}", database.display()))?;
        configure_connection(&connection, false)?;

        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let schema_version: i64 =
            connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        let user_objects: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if application_id == 0 && schema_version == 0 {
            ensure!(
                user_objects == 0,
                "拒绝把已有 SQLite 数据库初始化为 NeoEngram 元数据库"
            );
            let journal_mode: String = connection
                .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
                .context("无法启用 SQLite WAL")?;
            ensure!(
                journal_mode.eq_ignore_ascii_case("wal"),
                "SQLite 拒绝启用 WAL: {journal_mode}"
            );
            run_immediate(&connection, || {
                connection.execute_batch(SCHEMA_SQL)?;
                connection.pragma_update(None, "application_id", SQLITE_APPLICATION_ID)?;
                connection.pragma_update(None, "user_version", SQLITE_SCHEMA_VERSION)?;
                connection.execute(
                    "INSERT INTO repository_state(singleton, default_head_reference) \
                     VALUES (1, ?1)",
                    params![head_reference],
                )?;
                connection.execute(
                    "INSERT INTO workspaces(\
                         id, name, root_path, head_kind, head_target, base_commit_id,\
                         index_format, index_revision, index_count, index_accumulator\
                     ) VALUES (?1, 'main', '.', 'attached', ?2, NULL, ?3, 0, 0, ?4)",
                    params![
                        DEFAULT_WORKSPACE_ID.as_slice(),
                        head_reference,
                        i64::from(INDEX_FORMAT_VERSION),
                        [0_u8; 32].as_slice(),
                    ],
                )?;
                Ok(())
            })?;
            if !database_existed {
                sync_parent(&database)?;
            }
        } else {
            validate_database_identity(&connection)?;
        }
        self.validate_layout()
    }

    fn validate_layout(&self) -> Result<()> {
        let root = fs::symlink_metadata(&self.root)
            .with_context(|| format!("无法检查 SQLite 元数据目录: {}", self.root.display()))?;
        ensure!(
            root.is_dir() && !root.file_type().is_symlink(),
            "SQLite 元数据根路径不是普通目录: {}",
            self.root.display()
        );
        let connection = self.open_existing(true)?;
        let required_tables: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name IN ( \
             'repository_state', 'workspaces', 'workspace_index_files', 'manifest_sets', \
             'manifest_chunks', 'manifests', 'directory_sets', 'directory_entries', \
             'directories', 'commits', 'refs')",
            [],
            |row| row.get(0),
        )?;
        ensure!(required_tables == 11, "SQLite 元数据库缺少必要数据表");
        let default_reference: String = connection.query_row(
            "SELECT default_head_reference FROM repository_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        validate_reference_name(&default_reference)?;
        read_index_state(&connection, &self.workspace_id)?;
        read_head_state(&connection, &self.workspace_id)?;
        Ok(())
    }

    fn verify_integrity(&self) -> Result<()> {
        self.validate_layout()?;
        let session = self.read_session()?;
        let integrity_check: String =
            session
                .connection
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        ensure!(
            integrity_check == "ok",
            "SQLite integrity_check 失败: {integrity_check}"
        );
        {
            let mut statement = session.connection.prepare("PRAGMA foreign_key_check")?;
            let mut rows = statement.query([])?;
            ensure!(rows.next()?.is_none(), "SQLite 元数据库外键损坏");
        }
        let workspace_ids = {
            let mut statement = session.connection.prepare(
                "SELECT id, name, root_path, head_kind, head_target, base_commit_id \
                 FROM workspaces ORDER BY name COLLATE BINARY",
            )?;
            let records = statement
                .query_map([], workspace_record_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for record in &records {
                validate_workspace_record(record)?;
            }
            records
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>()
        };
        for workspace_id in workspace_ids {
            verify_index(&session.connection, &workspace_id)?;
        }
        verify_published_manifests(Rc::clone(&session))?;
        verify_published_directories(Rc::clone(&session))?;
        verify_commits_and_references(Rc::clone(&session))?;
        Ok(())
    }

    fn read_index(&self) -> Result<Box<dyn IndexReader + '_>> {
        let session = self.read_session()?;
        let state = read_index_state(&session.connection, &self.workspace_id)?;
        Ok(Box::new(SqliteIndexReader {
            session,
            workspace_id: self.workspace_id,
            state,
        }))
    }

    fn begin_index_transaction(
        &self,
        expected: Option<&IndexVersion>,
    ) -> Result<Box<dyn IndexTxn + '_>> {
        let validation_connection = self.open_existing(true)?;
        let connection = self.open_existing(true)?;
        connection.execute_batch("BEGIN DEFERRED")?;
        let state = read_index_state(&connection, &self.workspace_id)?;
        if let Some(expected) = expected {
            ensure!(
                &state.version == expected,
                "Index 已被其他操作更新，无法基于旧版本提交"
            );
        }
        Ok(Box::new(SqliteIndexTxn {
            store: self,
            base_connection: Some(connection),
            validation_connection,
            state,
            deleted_prefixes: Vec::new(),
            upserts: BTreeMap::new(),
        }))
    }

    fn snapshot(&self) -> Result<Box<dyn MetadataSnapshot + '_>> {
        Ok(Box::new(SqliteMetadataSnapshot {
            session: self.read_session()?,
            workspace_id: self.workspace_id,
        }))
    }

    fn put_manifest(
        &self,
        total_size: u64,
        chunking: ChunkingStrategy,
        chunks: &mut dyn Iterator<Item = Result<Chunk>>,
    ) -> Result<ManifestRef> {
        encode_u64(total_size, "Manifest 文件大小")?;
        let set_id = self.create_manifest_set(total_size, chunking)?;
        let result = (|| {
            let mut hasher = ManifestHasher::new(total_size, chunking);
            let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);
            let mut written = 0_u64;
            for chunk in chunks {
                let chunk = chunk?;
                hasher.push(&chunk)?;
                encode_u64(chunk.offset, "Manifest Chunk 偏移")?;
                encode_u64(chunk.size, "Manifest Chunk 大小")?;
                batch.push(chunk);
                if batch.len() >= WRITE_BATCH_SIZE {
                    self.insert_manifest_batch(&set_id, written, &batch)?;
                    written = written
                        .checked_add(u64::try_from(batch.len())?)
                        .context("Manifest Chunk 数量溢出")?;
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                self.insert_manifest_batch(&set_id, written, &batch)?;
                written = written
                    .checked_add(u64::try_from(batch.len())?)
                    .context("Manifest Chunk 数量溢出")?;
            }
            let manifest = hasher.finish()?;
            ensure!(
                written == manifest.chunk_count,
                "Manifest staging Chunk 数量不一致"
            );
            let created = publish_manifest_set(self, &set_id, &manifest, total_size)?;
            if !created {
                let _ = cleanup_manifest_set(self, &set_id);
            }
            Ok(manifest)
        })();
        if result.is_err() {
            let _ = cleanup_manifest_set(self, &set_id);
        }
        result
    }

    fn begin_directory_write(&self) -> Result<Box<dyn DirectoryWriter + '_>> {
        let validation_connection = self.open_existing(true)?;
        Ok(Box::new(SqliteDirectoryWriter {
            store: self,
            validation_connection,
            set_id: self.create_directory_set()?,
            hasher: Some(DirectoryHasher::new()),
            last_name: None,
            batch: Vec::with_capacity(WRITE_BATCH_SIZE),
            batch_bytes: 0,
            file_count: 0,
            total_size: 0,
            poisoned: false,
            cleanup_required: true,
        }))
    }

    fn put_commit(&self, id: &str, commit: &Commit) -> Result<()> {
        let id = decode_id(id)?;
        let payload = serde_json::to_vec(commit).context("无法序列化 Commit")?;
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            let existing = connection
                .query_row(
                    "SELECT payload FROM commits WHERE id = ?1",
                    params![id.as_slice()],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            match existing {
                Some(existing) => {
                    let existing: Commit =
                        serde_json::from_slice(&existing).context("已有 Commit 数据损坏")?;
                    ensure!(&existing == commit, "同一 Commit ID 已存在不同内容");
                    Ok(())
                }
                None => {
                    connection.execute(
                        "INSERT INTO commits(id, payload) VALUES (?1, ?2)",
                        params![id.as_slice(), payload],
                    )?;
                    Ok(())
                }
            }
        })
    }

    fn compare_exchange_head(
        &self,
        expected: &HeadState,
        new_state: &HeadState,
    ) -> Result<HeadCas> {
        let expected = normalize_head_state(expected);
        let new_state = normalize_head_state(new_state);
        expected.validate()?;
        new_state.validate()?;
        let (new_kind, new_target) = encode_head_state(&new_state)?;
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            let actual = normalize_head_state(&read_head_state(&connection, &self.workspace_id)?);
            if actual != expected {
                return Ok(HeadCas::Mismatch { actual });
            }
            let base_commit_id = match &new_state {
                HeadState::Attached { reference } => {
                    get_reference_from_connection(&connection, reference)?
                }
                HeadState::Detached { commit_id } => Some(commit_id.clone()),
            };
            connection.execute(
                "UPDATE workspaces SET head_kind = ?1, head_target = ?2, base_commit_id = ?3 \
                 WHERE id = ?4",
                params![
                    new_kind,
                    new_target,
                    base_commit_id,
                    self.workspace_id.as_slice(),
                ],
            )?;
            Ok(HeadCas::Updated)
        })
    }

    fn compare_exchange_reference(
        &self,
        name: &str,
        expected: Option<&str>,
        new_target: Option<&str>,
    ) -> Result<ReferenceCas> {
        validate_reference_name(name)?;
        let expected = normalize_optional_target(expected, "期望引用目标")?;
        let new_target = normalize_optional_target(new_target, "新引用目标")?;
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            let actual = get_reference_from_connection(&connection, name)?;
            if actual != expected {
                return Ok(ReferenceCas::Mismatch { actual });
            }
            match new_target {
                Some(target) => {
                    connection.execute(
                        "INSERT INTO refs(name, target) VALUES (?1, ?2) \
                         ON CONFLICT(name) DO UPDATE SET target = excluded.target",
                        params![name, target],
                    )?;
                    connection.execute(
                        "UPDATE workspaces SET base_commit_id = ?1 \
                         WHERE id = ?2 AND head_kind = 'attached' AND head_target = ?3",
                        params![target, self.workspace_id.as_slice(), name],
                    )?;
                }
                None if actual.is_some() => {
                    connection.execute("DELETE FROM refs WHERE name = ?1", params![name])?;
                }
                None => {}
            }
            Ok(ReferenceCas::Updated)
        })
    }

    fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
        commit_id: &str,
    ) -> Result<WorkspaceRecord> {
        validate_workspace_name(name)?;
        validate_workspace_root_path(root_path)?;
        validate_metadata_object_id(commit_id)?;
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            ensure!(
                get_commit_from_connection(&connection, commit_id)?.is_some(),
                "Workspace base Commit 不存在: {commit_id}"
            );
            let id: WorkspaceId = connection
                .query_row("SELECT randomblob(16)", [], |row| row.get::<_, Vec<u8>>(0))?
                .try_into()
                .map_err(|id: Vec<u8>| {
                    anyhow::anyhow!("SQLite 生成的 Workspace ID 长度无效: {}", id.len())
                })?;
            connection
                .execute(
                    "INSERT INTO workspaces(\
                     id, name, root_path, head_kind, head_target, base_commit_id,\
                     index_format, index_revision, index_count, index_accumulator\
                 ) VALUES (?1, ?2, ?3, 'detached', ?4, ?4, ?5, 0, 0, ?6)",
                    params![
                        id.as_slice(),
                        name,
                        root_path,
                        commit_id,
                        i64::from(INDEX_FORMAT_VERSION),
                        [0_u8; 32].as_slice(),
                    ],
                )
                .with_context(|| format!("无法注册 Workspace: {name}"))?;
            Ok(WorkspaceRecord {
                id,
                name: name.to_owned(),
                root_path: root_path.to_owned(),
                head: HeadState::Detached {
                    commit_id: commit_id.to_owned(),
                },
                base_commit_id: Some(commit_id.to_owned()),
            })
        })
    }

    fn attach_workspace_to_branch(
        &self,
        id: WorkspaceId,
        expected_commit_id: &str,
        reference: &str,
    ) -> Result<()> {
        validate_metadata_object_id(expected_commit_id)?;
        validate_reference_name(reference)?;
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            ensure!(
                get_commit_from_connection(&connection, expected_commit_id)?.is_some(),
                "Workspace base Commit 不存在: {expected_commit_id}"
            );
            let record = connection
                .query_row(
                    "SELECT id, name, root_path, head_kind, head_target, base_commit_id \
                     FROM workspaces WHERE id = ?1",
                    params![id.as_slice()],
                    workspace_record_from_row,
                )
                .optional()?
                .context("待激活的 Workspace 注册信息不存在")?;
            validate_workspace_record(&record)?;
            ensure!(
                record.head
                    == (HeadState::Detached {
                        commit_id: expected_commit_id.to_owned(),
                    })
                    && record.base_commit_id.as_deref() == Some(expected_commit_id),
                "Workspace 在分支激活前发生变化: {}",
                record.name
            );
            ensure!(
                get_reference_from_connection(&connection, reference)?.is_none(),
                "分支已存在，不能作为新 Workspace 分支创建: {reference}"
            );
            let owner = connection
                .query_row(
                    "SELECT name FROM workspaces \
                     WHERE head_kind = 'attached' AND head_target = ?1 AND id <> ?2",
                    params![reference, id.as_slice()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(owner) = owner {
                bail!("分支 {reference} 已被 Workspace {owner} 占用");
            }
            connection.execute(
                "INSERT INTO refs(name, target) VALUES (?1, ?2)",
                params![reference, expected_commit_id],
            )?;
            let updated = connection.execute(
                "UPDATE workspaces SET head_kind = 'attached', head_target = ?1 \
                 WHERE id = ?2 AND head_kind = 'detached' AND head_target = ?3 \
                     AND base_commit_id = ?3",
                params![reference, id.as_slice(), expected_commit_id],
            )?;
            ensure!(updated == 1, "Workspace 在分支激活期间发生变化");
            Ok(())
        })
    }

    fn get_workspace(&self, name: &str) -> Result<Option<WorkspaceRecord>> {
        validate_workspace_name(name)?;
        let connection = self.open_existing(true)?;
        connection
            .query_row(
                "SELECT id, name, root_path, head_kind, head_target, base_commit_id \
                 FROM workspaces WHERE name = ?1",
                params![name],
                workspace_record_from_row,
            )
            .optional()
            .with_context(|| format!("无法读取 Workspace: {name}"))
    }

    fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>> {
        let connection = self.open_existing(true)?;
        let mut statement = connection.prepare(
            "SELECT id, name, root_path, head_kind, head_target, base_commit_id \
             FROM workspaces ORDER BY name COLLATE BINARY",
        )?;
        let rows = statement.query_map([], workspace_record_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("无法读取 Workspace 列表")
    }

    fn remove_workspace(&self, id: WorkspaceId) -> Result<bool> {
        ensure!(id != DEFAULT_WORKSPACE_ID, "不能删除默认 Workspace");
        let connection = self.open_existing(false)?;
        run_immediate(&connection, || {
            let deleted = connection.execute(
                "DELETE FROM workspaces WHERE id = ?1",
                params![id.as_slice()],
            )?;
            Ok(deleted != 0)
        })
    }
}

fn verify_index(connection: &Connection, workspace_id: &WorkspaceId) -> Result<()> {
    let state = read_index_state(connection, workspace_id)?;
    let mut statement = connection.prepare(
        "SELECT path, total_size, chunk_count, manifest_id, record_digest \
         FROM workspace_index_files WHERE workspace_id = ?1 ORDER BY path COLLATE BINARY",
    )?;
    let mut rows = statement.query(params![workspace_id.as_slice()])?;
    let mut count = 0_u64;
    let mut accumulator = [0_u8; 32];
    let mut previous: Option<String> = None;
    while let Some(row) = rows.next()? {
        let file = file_from_row(row)?;
        validate_file_record(&file)?;
        if let Some(previous) = &previous {
            ensure!(previous < &file.path, "SQLite Index 路径未严格递增");
        }
        let stored = decode_digest(row.get(4)?, "Index record")?;
        ensure!(
            stored == index_record_digest(&file)?,
            "Index record 摘要损坏: {}",
            file.path
        );
        xor_digest(&mut accumulator, &stored);
        count = count.checked_add(1).context("Index 文件数量溢出")?;
        previous = Some(file.path);
    }
    ensure!(count == state.count, "Index header 文件数量不一致");
    ensure!(
        accumulator == state.accumulator,
        "Index accumulator 与文件内容不一致"
    );
    Ok(())
}

fn verify_published_manifests(session: Rc<ReadSession>) -> Result<()> {
    let mut id_request = PageRequest::first(4_096)?;
    loop {
        let id_page = scan_id_table(&session.connection, "manifests", &id_request)?;
        for id in &id_page.items {
            let reader = open_manifest_in_session(Rc::clone(&session), id)?
                .with_context(|| format!("Manifest 在完整性检查期间消失: {id}"))?;
            let mut hasher = ManifestHasher::new(reader.total_size(), reader.chunking());
            let mut chunk_request = PageRequest::first(4_096)?;
            let mut count = 0_u64;
            loop {
                let page = reader.scan_chunks(&chunk_request)?;
                for chunk in page.items {
                    hasher.push(&chunk)?;
                    count = count.checked_add(1).context("Manifest Chunk 数量溢出")?;
                }
                let Some(next) = page.next else {
                    break;
                };
                chunk_request.after = Some(next);
            }
            let calculated = hasher.finish()?;
            ensure!(
                calculated.id == *id
                    && calculated.chunk_count == reader.chunk_count()
                    && calculated.chunking == reader.chunking(),
                "Manifest 内容与其 ID 或 header 不匹配: {id}"
            );
            ensure!(
                count == reader.chunk_count(),
                "Manifest Chunk 数量不一致: {id}"
            );
        }
        let Some(next) = id_page.next else {
            break;
        };
        id_request.after = Some(next);
    }
    Ok(())
}

fn verify_published_directories(session: Rc<ReadSession>) -> Result<()> {
    let mut id_request = PageRequest::first(4_096)?;
    loop {
        let id_page = scan_id_table(&session.connection, "directories", &id_request)?;
        for id in &id_page.items {
            let reader = open_directory_in_session(Rc::clone(&session), id)?
                .with_context(|| format!("Directory 在完整性检查期间消失: {id}"))?;
            let mut hasher = DirectoryHasher::new();
            let mut entry_request = PageRequest::first(4_096)?;
            let mut entry_count = 0_u64;
            let mut file_count = 0_u64;
            let mut total_size = 0_u64;
            let mut previous: Option<String> = None;
            loop {
                let page = reader.scan_entries(&entry_request)?;
                for entry in page.items {
                    validate_directory_entry(&entry)?;
                    if let Some(previous) = &previous {
                        ensure!(previous < &entry.name, "Directory 名称未严格递增: {id}");
                    }
                    let stats = validate_stored_directory_entry(&session.connection, &entry)?;
                    hasher.push(&entry)?;
                    entry_count = entry_count
                        .checked_add(1)
                        .context("Directory 子项数量溢出")?;
                    file_count = file_count
                        .checked_add(stats.0)
                        .context("Directory 文件数量溢出")?;
                    total_size = total_size
                        .checked_add(stats.1)
                        .context("Directory 大小溢出")?;
                    previous = Some(entry.name);
                }
                let Some(next) = page.next else {
                    break;
                };
                entry_request.after = Some(next);
            }
            let (calculated, calculated_count) = hasher.finish();
            ensure!(
                calculated == *id
                    && calculated_count == reader.entry_count()
                    && entry_count == reader.entry_count()
                    && file_count == reader.file_count()
                    && total_size == reader.total_size(),
                "Directory 内容与其 ID 或 header 不匹配: {id}"
            );
        }
        let Some(next) = id_page.next else {
            break;
        };
        id_request.after = Some(next);
    }
    Ok(())
}

fn verify_commits_and_references(session: Rc<ReadSession>) -> Result<()> {
    let mut id_request = PageRequest::first(4_096)?;
    loop {
        let page = scan_id_table(&session.connection, "commits", &id_request)?;
        for id in &page.items {
            ensure!(
                get_commit_from_connection(&session.connection, id)?.is_some(),
                "Commit 在完整性检查期间消失: {id}"
            );
        }
        let Some(next) = page.next else {
            break;
        };
        id_request.after = Some(next);
    }

    let mut reference_request = PageRequest::first(4_096)?;
    loop {
        let page =
            scan_references_from_connection(&session.connection, "refs", &reference_request)?;
        for reference in &page.items {
            validate_reference_name(&reference.name)?;
            validate_metadata_object_id(&reference.target)?;
        }
        let Some(next) = page.next else {
            break;
        };
        reference_request.after = Some(next);
    }

    let mut statement = session.connection.prepare(
        "SELECT id, name, root_path, head_kind, head_target, base_commit_id \
         FROM workspaces ORDER BY name COLLATE BINARY",
    )?;
    let records = statement
        .query_map([], workspace_record_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for record in records {
        validate_workspace_record(&record)?;
        if let Some(base_commit_id) = &record.base_commit_id {
            ensure!(
                get_commit_from_connection(&session.connection, base_commit_id)?.is_some(),
                "Workspace {} 的 HEAD/base Commit 不存在: {base_commit_id}",
                record.name
            );
        }
        if let HeadState::Attached { reference } = &record.head {
            let target = get_reference_from_connection(&session.connection, reference)?;
            ensure!(
                target == record.base_commit_id,
                "Workspace {} 的 base Commit 与分支 {} 不一致",
                record.name,
                reference
            );
        }
    }
    Ok(())
}
