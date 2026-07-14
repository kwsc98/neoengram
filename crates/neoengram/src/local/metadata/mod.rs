//! 可替换的元数据持久化边界。
//!
//! `JsonMetadataStore` 和 `SqliteMetadataStore` 实现同一契约。Chunk payload 与工作区
//! 恢复 journal 具有不同生命周期和原子性边界，因此不在本模块中。
//! 完整的逐操作语义见同目录 `README.md`。

use std::{fmt::Debug, num::NonZeroU32, path::Path, sync::Arc};

use anyhow::{ensure, Context, Result};
use neoengram_core::{Chunk, Commit};
use serde::{Deserialize, Serialize};

use crate::local::STORAGE_TEMP_FILE_PREFIX;

mod json;
mod sqlite;

use json::JsonMetadataStore;
use sqlite::SqliteMetadataStore;

/// 单次存储扫描允许返回的最大记录数。后端必须拒绝更大的请求，防止调用方绕过分页接口
/// 再次把完整仓库物化到内存。
const MAX_PAGE_SIZE: u32 = 4_096;
const MANIFEST_HASH_DOMAIN: &[u8] = b"neoengram-file-manifest-v2";
const TREE_HASH_DOMAIN: &[u8] = b"neoengram-tree-v2";

/// 基于排他 continuation cursor 的有界扫描请求。
///
/// Cursor 只允许传回产生它的同类 reader；它不是可跨后端、跨 snapshot 持久化的标识。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageRequest {
    pub(crate) after: Option<String>,
    pub(crate) limit: NonZeroU32,
}

impl PageRequest {
    pub(crate) fn new(after: Option<String>, limit: u32) -> Result<Self> {
        let limit = NonZeroU32::new(limit).context("分页大小必须大于零")?;
        let request = Self { after, limit };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn first(limit: u32) -> Result<Self> {
        Self::new(None, limit)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.limit.get() <= MAX_PAGE_SIZE,
            "分页大小不能超过 {MAX_PAGE_SIZE}: {}",
            self.limit
        );
        Ok(())
    }

    pub(crate) fn limit_usize(&self) -> Result<usize> {
        self.validate()?;
        usize::try_from(self.limit.get()).context("当前平台无法表示分页大小")
    }
}

/// 一个有界结果页。`next` 为 `None` 表示扫描完成；否则把它放进下一次请求的 `after`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Page<T> {
    pub(crate) items: Vec<T>,
    pub(crate) next: Option<String>,
}

/// Index 的不透明版本。它只用于乐观并发检查，不得被解释成递增序号。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IndexVersion {
    revision: u64,
    digest: [u8; 32],
}

impl IndexVersion {
    const fn new(revision: u64, digest: [u8; 32]) -> Self {
        Self { revision, digest }
    }
}

/// 已发布 Manifest 的逻辑引用。内容 ID 和 Chunk 数由存储层在单次流式消费后返回。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestRef {
    pub(crate) id: String,
    pub(crate) chunk_count: u64,
}

/// 文件集合中的轻量记录。Chunk 清单通过对应的 `ManifestReader` 分页读取。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileRecord {
    pub(crate) path: String,
    pub(crate) total_size: u64,
    pub(crate) chunk_count: u64,
    pub(crate) manifest_id: String,
}

impl FileRecord {
    pub(crate) fn from_manifest(path: String, total_size: u64, manifest: &ManifestRef) -> Self {
        Self {
            path,
            total_size,
            chunk_count: manifest.chunk_count,
            manifest_id: manifest.id.clone(),
        }
    }
}

/// 一个不可变文件 recipe 的分页读取边界。
pub(crate) trait ManifestReader: Debug {
    fn total_size(&self) -> u64;
    fn chunk_count(&self) -> u64;
    fn scan_chunks(&self, request: &PageRequest) -> Result<Page<Chunk>>;
}

/// Index 和 Tree 共同提供的文件/Chunk 有界读取协议。
pub(crate) trait FileSetReader: Debug {
    fn get_file(&self, path: &str) -> Result<Option<FileRecord>>;

    /// `prefix` 为空时扫描所有文件；非空时包含该路径本身及其 `/` 子孙。
    fn scan_files(&self, prefix: Option<&str>, request: &PageRequest) -> Result<Page<FileRecord>>;
}

pub(crate) trait IndexReader: FileSetReader {
    fn format_version(&self) -> u32;
    fn version(&self) -> &IndexVersion;
}

/// Index 的串行化 read-modify-write 边界。
///
/// JSON 后端可以在内部物化完整 Index；SQLite 后端应把这些方法映射到同一数据库事务。
pub(crate) trait IndexTxn: IndexReader {
    /// 新增或替换文件；引用的 Manifest 必须已存在且大小、Chunk 数与记录一致。
    fn upsert_file(&mut self, file: &FileRecord) -> Result<()>;
    fn delete_prefix(&mut self, prefix: &str) -> Result<u64>;
    fn commit(self: Box<Self>) -> Result<IndexVersion>;
}

pub(crate) trait TreeReader: FileSetReader {
    fn file_count(&self) -> u64;
}

/// 追加式 Tree 构造器。批次之间也必须保持路径严格递增；`finish` 是唯一发布点。
pub(crate) trait TreeWriter: Debug {
    /// 追加文件；引用的 Manifest 必须已存在且大小、Chunk 数与记录一致。
    fn append_file(&mut self, file: &FileRecord) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<String>;
}

/// 仓库元数据的物理存储类型。该值会写入 repository.json，打开仓库时据此选择后端。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MetadataStoreKind {
    Json,
    Sqlite,
}

/// HEAD 的持久状态。Attached 通过命名引用解析当前 Commit，Detached 直接指向 Commit。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum HeadState {
    Attached { reference: String },
    Detached { commit_id: String },
}

impl HeadState {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Attached { reference } => validate_reference_name(reference),
            Self::Detached { commit_id } => validate_metadata_object_id(commit_id),
        }
    }
}

/// 规范化 HEAD 状态以便 compare-exchange 比较；引用名与 Commit ID 去除首尾空白。
fn normalize_head_state(state: &HeadState) -> HeadState {
    match state {
        HeadState::Attached { reference } => HeadState::Attached {
            reference: reference.trim().to_owned(),
        },
        HeadState::Detached { commit_id } => HeadState::Detached {
            commit_id: commit_id.trim().to_owned(),
        },
    }
}

/// 一个命名引用及其目标。引用名使用 `refs/...` 形式，与具体后端的文件名或表结构无关。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredReference {
    pub(crate) name: String,
    pub(crate) target: String,
}

/// 引用 compare-exchange 的结果。`actual` 是发生冲突时锁内重新读取的当前值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReferenceCas {
    Updated,
    Mismatch { actual: Option<String> },
}

/// HEAD compare-exchange 的结果。HEAD 始终存在，因此冲突时返回完整实际状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeadCas {
    Updated,
    Mismatch { actual: HeadState },
}

/// HEAD/ref 与不可变历史对象的点查边界。实现不得为了一次点查枚举整个仓库。
pub(crate) trait MetadataReader: Debug {
    fn read_head_state(&self) -> Result<HeadState>;
    fn get_reference(&self, name: &str) -> Result<Option<String>>;
    fn open_manifest(&self, id: &str) -> Result<Option<Box<dyn ManifestReader + '_>>>;
    fn open_tree(&self, id: &str) -> Result<Option<Box<dyn TreeReader + '_>>>;
    fn get_commit(&self, id: &str) -> Result<Option<Commit>>;
}

/// 一次固定的历史元数据读视图。Tree/Manifest/Commit ID 和 refs 的翻页 cursor 都只在
/// 这个 snapshot 内有效；SQLite 后端应使用 read transaction 实现相同语义。
pub(crate) trait MetadataSnapshot: MetadataReader {
    fn scan_tree_ids(&self, request: &PageRequest) -> Result<Page<String>>;
    fn scan_manifest_ids(&self, request: &PageRequest) -> Result<Page<String>>;
    fn scan_commit_ids(&self, request: &PageRequest) -> Result<Page<String>>;
    fn scan_references(&self, prefix: &str, request: &PageRequest)
        -> Result<Page<StoredReference>>;
}

/// Index、不可变历史元数据和 refs 的持久化边界。
///
/// Repository 负责工作区路径、Tree/Commit 内容 ID 和对象内容等领域校验；后端仍必须在
/// 访问物理存储前拒绝非法对象 ID、引用名和引用前缀。后端负责事务化 Index、引用 CAS、
/// 不可变且幂等地发布 Manifest/Tree/Commit，以及有界、严格递增的 snapshot 枚举。
///
/// `initialize` 必须幂等且不能覆盖现有状态。`begin_index_transaction` 成功后必须基于一个
/// 固定版本执行 read-modify-write；`expected` 不匹配时不得返回事务。Chunk payload 和
/// checkout/rm journal 不属于这个接口。
pub(crate) trait MetadataStore: MetadataReader + Send + Sync {
    fn kind(&self) -> MetadataStoreKind;

    fn initialize(&self, head_reference: &str) -> Result<()>;
    fn validate_layout(&self) -> Result<()>;
    fn verify_integrity(&self) -> Result<()>;

    fn read_index(&self) -> Result<Box<dyn IndexReader + '_>>;
    fn begin_index_transaction(
        &self,
        expected: Option<&IndexVersion>,
    ) -> Result<Box<dyn IndexTxn + '_>>;

    fn snapshot(&self) -> Result<Box<dyn MetadataSnapshot + '_>>;

    /// 单次消费完整 Chunk 流并发布不可变 Manifest。输入或校验失败时不得发布；成功时
    /// 返回由实际内容计算出的 ID 和 Chunk 数，调用方无需预先物化或 Hash 清单。
    fn put_manifest(
        &self,
        total_size: u64,
        chunks: &mut dyn Iterator<Item = Result<Chunk>>,
    ) -> Result<ManifestRef>;
    fn begin_tree_write(&self) -> Result<Box<dyn TreeWriter + '_>>;
    fn put_commit(&self, id: &str, commit: &Commit) -> Result<()>;

    /// 当且仅当 HEAD 当前状态等于 `expected` 时原子替换为 `new_state`。
    fn compare_exchange_head(&self, expected: &HeadState, new_state: &HeadState)
        -> Result<HeadCas>;

    /// 当且仅当当前值等于 `expected` 时写入 `new_target`。两个参数的 `None` 分别表示
    /// “期望引用不存在”和“删除引用”，因此该方法不提供无条件覆盖模式。
    fn compare_exchange_reference(
        &self,
        name: &str,
        expected: Option<&str>,
        new_target: Option<&str>,
    ) -> Result<ReferenceCas>;
}

pub(crate) fn open_metadata_store(
    kind: MetadataStoreKind,
    metadata_root: &Path,
) -> Arc<dyn MetadataStore> {
    match kind {
        MetadataStoreKind::Json => Arc::new(JsonMetadataStore::new(metadata_root.to_path_buf())),
        MetadataStoreKind::Sqlite => {
            Arc::new(SqliteMetadataStore::new(metadata_root.to_path_buf()))
        }
    }
}

fn validate_metadata_object_id(id: &str) -> Result<()> {
    ensure!(
        id.len() == 64
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "元数据对象 ID 不是 64 位小写十六进制: {id}"
    );
    Ok(())
}

fn file_manifest_id(total_size: u64, chunks: &[Chunk]) -> Result<String> {
    Ok(describe_manifest(total_size, chunks)?.id)
}

pub(crate) fn describe_manifest(total_size: u64, chunks: &[Chunk]) -> Result<ManifestRef> {
    let mut hasher = ManifestHasher::new(total_size);
    for chunk in chunks {
        hasher.push(chunk)?;
    }
    hasher.finish()
}

struct ManifestHasher {
    hasher: blake3::Hasher,
    total_size: u64,
    next_offset: u64,
    chunk_count: u64,
}

impl ManifestHasher {
    fn new(total_size: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(MANIFEST_HASH_DOMAIN);
        hasher.update(&[0]);
        hasher.update(&total_size.to_le_bytes());
        Self {
            hasher,
            total_size,
            next_offset: 0,
            chunk_count: 0,
        }
    }

    fn push(&mut self, chunk: &Chunk) -> Result<()> {
        validate_metadata_object_id(&chunk.hash)?;
        ensure!(chunk.size > 0, "Manifest Chunk 大小必须大于零");
        ensure!(
            chunk.offset == self.next_offset,
            "Manifest Chunk 偏移不连续: {}",
            chunk.hash
        );
        let next_offset = self
            .next_offset
            .checked_add(chunk.size)
            .context("Manifest Chunk 总大小溢出")?;
        let chunk_count = self
            .chunk_count
            .checked_add(1)
            .context("Manifest Chunk 数量溢出")?;
        self.hasher.update(chunk.hash.as_bytes());
        self.hasher.update(&chunk.offset.to_le_bytes());
        self.hasher.update(&chunk.size.to_le_bytes());
        self.next_offset = next_offset;
        self.chunk_count = chunk_count;
        Ok(())
    }

    fn finish(mut self) -> Result<ManifestRef> {
        ensure!(
            self.next_offset == self.total_size,
            "Manifest Chunk 总大小与文件大小不一致"
        );
        self.hasher.update(&self.chunk_count.to_le_bytes());
        Ok(ManifestRef {
            id: self.hasher.finalize().to_hex().to_string(),
            chunk_count: self.chunk_count,
        })
    }
}

pub(crate) fn tree_records_id(files: &[FileRecord]) -> Result<String> {
    let mut hasher = TreeHasher::new();
    for file in files {
        hasher.push(file)?;
    }
    Ok(hasher.finish().0)
}

#[derive(Debug)]
struct TreeHasher {
    hasher: blake3::Hasher,
    first: bool,
    file_count: u64,
}

impl TreeHasher {
    fn new() -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(TREE_HASH_DOMAIN);
        hasher.update(&[0]);
        hasher.update(b"[");
        Self {
            hasher,
            first: true,
            file_count: 0,
        }
    }

    fn push(&mut self, file: &FileRecord) -> Result<()> {
        if !self.first {
            self.hasher.update(b",");
        }
        let encoded = serde_json::to_vec(file).context("无法序列化 Tree 文件记录")?;
        self.hasher.update(&encoded);
        self.file_count = self
            .file_count
            .checked_add(1)
            .context("Tree 文件数量溢出")?;
        self.first = false;
        Ok(())
    }

    fn finish(mut self) -> (String, u64) {
        self.hasher.update(b"]");
        (self.hasher.finalize().to_hex().to_string(), self.file_count)
    }
}

#[cfg(test)]
fn legacy_tree_records_id(files: &[FileRecord]) -> Result<String> {
    let canonical = serde_json::to_vec(files).context("无法序列化 Tree 文件记录")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(TREE_HASH_DOMAIN);
    hasher.update(&[0]);
    hasher.update(&canonical);
    Ok(hasher.finalize().to_hex().to_string())
}

fn validate_reference_name(name: &str) -> Result<()> {
    ensure!(name.starts_with("refs/"), "引用名必须位于 refs/ 下: {name}");
    ensure!(!name.contains('\\'), "引用名必须使用 `/` 分隔: {name}");
    for component in name.split('/') {
        ensure!(
            !component.is_empty() && component != "." && component != "..",
            "引用名未规范化: {name}"
        );
        ensure!(
            !component.starts_with(STORAGE_TEMP_FILE_PREFIX),
            "引用名使用了保留的临时文件前缀: {name}"
        );
        ensure!(
            component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
            "引用名包含不支持的字符: {name}"
        );
    }
    Ok(())
}

fn validate_reference_prefix(prefix: &str) -> Result<()> {
    if prefix == "refs" {
        return Ok(());
    }
    validate_reference_name(prefix)
}

#[cfg(test)]
mod tests;
