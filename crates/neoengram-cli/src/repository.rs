use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, ensure, Context, Result};
use fs2::FileExt;
use neoengram_core::{Commit, FileNode, Index, ObjectStore, Tree, INDEX_FORMAT_VERSION};
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use crate::storage::{
    file::{
        ensure_directory, ensure_or_create_directory, json_bytes, publish_json_if_absent,
        read_json, sync_parent,
    },
    metadata::{
        describe_manifest, open_metadata_store, tree_records_id, FileRecord, FileSetReader,
        MetadataReader, MetadataStore, MetadataStoreKind, Page, PageRequest, ReferenceCas,
        StoredReference,
    },
    object::{open_object_store, ObjectStoreKind},
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

pub(crate) const NEOENGRAM_DIR_NAME: &str = ".neoengram";
const OBJECTS_DIR_NAME: &str = "objects";
const FILES_DIR_NAME: &str = "files";
const METADATA_DIR_NAME: &str = "metadata";
const REPOSITORY_FILE_NAME: &str = "repository.json";
const TRANSACTIONS_DIR_NAME: &str = "transactions";
const STAGING_DIR_NAME: &str = "staging";
const WRITE_LOCK_FILE_NAME: &str = "write.lock";
const DEFAULT_HEAD_REFERENCE: &str = "refs/heads/main";
const CURRENT_FORMAT_VERSION: u32 = 3;
const COMMIT_HASH_DOMAIN: &[u8] = b"neoengram-commit-v2";
const LOCK_FORMAT_VERSION: u32 = 1;
const STORAGE_SCAN_PAGE_SIZE: u32 = 1_024;

/// 保存在 NeoEngram 控制目录 `metadata/repository.json` 中的后端选择与仓库格式配置。
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepositoryConfig {
    format_version: u32,
    metadata_store: MetadataStoreKind,
    object_store: ObjectStoreKind,
}

/// 写锁文件只用于诊断，互斥语义由操作系统 advisory lock 提供。
///
/// 因此进程被 kill 或机器重启后，锁会由内核自动释放；持久存在的文件不再等同于活锁。
#[derive(Debug, Serialize)]
struct RepositoryLockInfo<'a> {
    format_version: u32,
    pid: u32,
    operation: &'a str,
    checkout_transaction: Option<&'a str>,
}

impl RepositoryConfig {
    const fn current(metadata_store: MetadataStoreKind, object_store: ObjectStoreKind) -> Self {
        Self {
            format_version: CURRENT_FORMAT_VERSION,
            metadata_store,
            object_store,
        }
    }
}

pub(crate) fn is_neoengram_dir_name(name: &OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.eq_ignore_ascii_case(NEOENGRAM_DIR_NAME))
}

fn neoengram_dir_exists(root: &Path) -> Result<bool> {
    let path = root.join(NEOENGRAM_DIR_NAME);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "NeoEngram 内部路径不是普通目录: {}",
                path.display()
            );
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("无法检查 NeoEngram 仓库目录: {}", path.display()))
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Repository {
    root: PathBuf,
    metadata_store: Arc<dyn MetadataStore>,
    object_store: Arc<dyn ObjectStore>,
    object_store_kind: ObjectStoreKind,
}

impl Repository {
    pub(crate) fn at(root: PathBuf) -> Result<Self> {
        Self::with_store_kinds(root, MetadataStoreKind::Json, ObjectStoreKind::Loose)
    }

    pub(crate) fn open_or_default(root: PathBuf) -> Result<Self> {
        if neoengram_dir_exists(&root)? {
            let repository_dir = root.join(NEOENGRAM_DIR_NAME);
            Self::open(root).with_context(|| {
                format!(
                    "已有目录不是受支持的 NeoEngram 仓库: {}",
                    repository_dir.display()
                )
            })
        } else {
            Self::at(root)
        }
    }

    fn open(root: PathBuf) -> Result<Self> {
        let metadata_dir = root.join(NEOENGRAM_DIR_NAME).join(METADATA_DIR_NAME);
        let repository_file = metadata_dir.join(REPOSITORY_FILE_NAME);
        let config = read_repository_config(&repository_file)?;
        ensure!(
            config.format_version == CURRENT_FORMAT_VERSION,
            "不支持的 NeoEngram 仓库格式版本 {}（当前支持 {}）",
            config.format_version,
            CURRENT_FORMAT_VERSION
        );
        Self::with_store_kinds(root, config.metadata_store, config.object_store)
    }

    fn with_store_kinds(
        root: PathBuf,
        metadata_kind: MetadataStoreKind,
        object_kind: ObjectStoreKind,
    ) -> Result<Self> {
        let metadata_dir = root.join(NEOENGRAM_DIR_NAME).join(METADATA_DIR_NAME);
        let objects_dir = root.join(NEOENGRAM_DIR_NAME).join(OBJECTS_DIR_NAME);
        Ok(Self {
            root,
            metadata_store: open_metadata_store(metadata_kind, &metadata_dir),
            object_store: open_object_store(object_kind, &objects_dir),
            object_store_kind: object_kind,
        })
    }

    /// 从当前目录向上寻找最近的真实 `.neoengram` 目录。
    pub(crate) fn discover(start: &Path) -> Result<Self> {
        let canonical_start = fs::canonicalize(start)
            .with_context(|| format!("无法解析当前目录: {}", start.display()))?;

        for root in canonical_start.ancestors() {
            if neoengram_dir_exists(root)? {
                let repository = Self::open(root.to_path_buf())?;
                repository.validate()?;
                return Ok(repository);
            }
        }

        bail!("当前目录不在 NeoEngram 仓库中，请先运行 `neoengram init`")
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn repository_dir(&self) -> PathBuf {
        self.root.join(NEOENGRAM_DIR_NAME)
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn staging_dir(&self) -> PathBuf {
        self.repository_dir().join(STAGING_DIR_NAME)
    }

    pub(crate) fn files_dir(&self) -> PathBuf {
        self.repository_dir().join(FILES_DIR_NAME)
    }

    fn metadata_dir(&self) -> PathBuf {
        self.repository_dir().join(METADATA_DIR_NAME)
    }

    pub(crate) fn transactions_dir(&self) -> PathBuf {
        self.repository_dir().join(TRANSACTIONS_DIR_NAME)
    }

    pub(crate) fn unfinished_transactions(&self) -> Result<Vec<PathBuf>> {
        let mut transactions = Vec::new();
        for entry in fs::read_dir(self.transactions_dir()).with_context(|| {
            format!(
                "无法读取 Checkout 事务目录: {}",
                self.transactions_dir().display()
            )
        })? {
            let entry = entry.context("无法读取 Checkout 事务项")?;
            let metadata = fs::symlink_metadata(entry.path())
                .with_context(|| format!("无法检查 Checkout 事务项: {}", entry.path().display()))?;
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "Checkout 事务项不是普通目录: {}",
                entry.path().display()
            );
            transactions.push(entry.path());
        }
        transactions.sort_unstable();
        Ok(transactions)
    }

    pub(crate) fn ensure_no_unfinished_transactions(&self) -> Result<()> {
        let transactions = self.unfinished_transactions()?;
        ensure!(
            transactions.is_empty(),
            "检测到未完成的工作区事务，请先运行 `neoengram recover` 或 `neoengram recover --abort`: {}",
            transactions
                .first()
                .map_or_else(|| self.transactions_dir(), PathBuf::from)
                .display()
        );
        Ok(())
    }

    fn repository_file(&self) -> PathBuf {
        self.metadata_dir().join(REPOSITORY_FILE_NAME)
    }

    fn write_lock_file(&self) -> PathBuf {
        self.metadata_dir().join(WRITE_LOCK_FILE_NAME)
    }

    /// 创建或补全当前仓库布局。已有 index、HEAD 和历史永远不会被覆盖。
    pub(crate) fn initialize_layout(&self) -> Result<()> {
        for directory in [
            self.repository_dir(),
            self.files_dir(),
            self.transactions_dir(),
            self.staging_dir(),
            self.metadata_dir(),
        ] {
            ensure_or_create_directory(&directory)?;
        }
        self.object_store.initialize()?;

        let current_config =
            RepositoryConfig::current(self.metadata_store.kind(), self.object_store_kind);
        publish_json_if_absent(&self.repository_file(), &current_config)?;
        let config = read_repository_config(&self.repository_file())?;
        ensure!(
            config == current_config,
            "已有仓库使用不受支持的格式版本 {}",
            config.format_version
        );

        self.metadata_store.initialize(DEFAULT_HEAD_REFERENCE)?;
        self.metadata_store.read_index()?;

        self.validate()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for (directory, kind) in [
            (self.files_dir(), "完整文件缓存"),
            (self.transactions_dir(), "Checkout 事务"),
            (self.staging_dir(), "输入暂存"),
            (self.metadata_dir(), "元数据"),
        ] {
            ensure_directory(&directory, kind)?;
        }

        let config = read_repository_config(&self.repository_file())?;
        ensure!(
            config.format_version == CURRENT_FORMAT_VERSION,
            "不支持的 NeoEngram 仓库格式版本 {}（当前支持 {}）",
            config.format_version,
            CURRENT_FORMAT_VERSION
        );
        ensure!(
            config.metadata_store == self.metadata_store.kind(),
            "仓库元数据后端配置与已打开后端不一致"
        );
        ensure!(
            config.object_store == self.object_store_kind,
            "仓库对象后端配置与已打开后端不一致"
        );
        self.object_store.validate_layout()?;
        self.metadata_store.validate_layout()?;
        Ok(())
    }

    /// 获取普通仓库写锁。
    ///
    /// 所有会改变 index、ref 或工作区的普通命令都必须走这个入口。存在未完成的 Checkout
    /// 时会统一拒绝操作，避免 add/commit 绕过恢复流程继续写元数据。
    pub(crate) fn acquire_write_lock(&self) -> Result<RepositoryWriteLock> {
        let lock = self.acquire_lock("repository-write")?;
        self.ensure_no_unfinished_transactions()?;
        Ok(lock)
    }

    /// 恢复命令必须能在事务目录存在时取得锁，因此使用独立入口。
    pub(crate) fn acquire_recovery_lock(&self) -> Result<RepositoryWriteLock> {
        self.acquire_lock("checkout-recover")
    }

    fn acquire_lock(&self, operation: &str) -> Result<RepositoryWriteLock> {
        let path = self.write_lock_file();
        let existed = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "仓库写锁路径不是普通文件: {}",
                    path.display()
                );
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| format!("无法检查仓库写锁: {}", path.display()));
            }
        };

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options
            .open(&path)
            .with_context(|| format!("无法打开仓库写锁: {}", path.display()))?;
        let opened_metadata = file
            .metadata()
            .with_context(|| format!("无法检查已打开的仓库写锁: {}", path.display()))?;
        let path_metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("无法再次检查仓库写锁: {}", path.display()))?;
        ensure!(
            opened_metadata.is_file()
                && path_metadata.is_file()
                && !path_metadata.file_type().is_symlink(),
            "仓库写锁路径不是普通文件: {}",
            path.display()
        );
        #[cfg(unix)]
        ensure!(
            opened_metadata.dev() == path_metadata.dev()
                && opened_metadata.ino() == path_metadata.ino(),
            "仓库写锁在打开期间被替换: {}",
            path.display()
        );
        if let Err(error) = FileExt::try_lock_exclusive(&file) {
            let owner = fs::read_to_string(&path)
                .ok()
                .filter(|contents| !contents.trim().is_empty())
                .unwrap_or_else(|| "锁持有者信息不可用".to_owned());
            bail!(
                "另一个 NeoEngram 写操作仍在执行（{}；锁文件 {}）：{error}",
                owner.trim(),
                path.display()
            );
        }
        let mut guard = RepositoryWriteLock { file, path };
        guard.set_operation(operation, None)?;
        if !existed {
            sync_parent(&guard.path)?;
        }
        Ok(guard)
    }

    pub(crate) fn read_index(&self) -> Result<Index> {
        let reader = self.metadata_store.read_index()?;
        let index = Index {
            format_version: reader.format_version(),
            files: collect_file_set(self.metadata_store.as_ref(), reader.as_ref())?,
        };
        validate_index(&index)?;
        Ok(index)
    }

    pub(crate) fn write_index(&self, index: &Index) -> Result<()> {
        validate_index(index)?;
        let reader = self.metadata_store.read_index()?;
        let expected = reader.version().clone();
        drop(reader);
        let mut records = Vec::with_capacity(index.files.len());
        for file in &index.files {
            let mut chunks = file.chunks.iter().cloned().map(Ok);
            let manifest = self
                .metadata_store
                .put_manifest(file.total_size, &mut chunks)?;
            let record = FileRecord::from_manifest(file.path.clone(), file.total_size, &manifest);
            records.push(record);
        }
        let mut transaction = self
            .metadata_store
            .begin_index_transaction(Some(&expected))?;
        transaction.delete_prefix("")?;
        for record in &records {
            transaction.upsert_file(record)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn validate_index_snapshot(&self, index: &Index) -> Result<()> {
        validate_index(index)
    }

    pub(crate) fn validate_file_snapshot(&self, file: &FileNode) -> Result<()> {
        validate_files(std::slice::from_ref(file))
    }

    pub(crate) fn validate_logical_path(&self, path: &str) -> Result<()> {
        validate_repository_path(path)
    }

    pub(crate) fn tree_id(&self, tree: &Tree) -> Result<String> {
        validate_files(&tree.files)?;
        let records = tree
            .files
            .iter()
            .map(file_record_from_node)
            .collect::<Result<Vec<_>>>()?;
        tree_records_id(&records)
    }

    pub(crate) fn store_tree(&self, tree: &Tree) -> Result<String> {
        validate_files(&tree.files)?;
        let mut writer = self.metadata_store.begin_tree_write()?;
        for file in &tree.files {
            let mut chunks = file.chunks.iter().cloned().map(Ok);
            let manifest = self
                .metadata_store
                .put_manifest(file.total_size, &mut chunks)?;
            let record = FileRecord::from_manifest(file.path.clone(), file.total_size, &manifest);
            writer.append_file(&record)?;
        }
        writer.finish()
    }

    pub(crate) fn read_tree(&self, id: &str) -> Result<Tree> {
        validate_hash(id, "Tree")?;
        let reader = self
            .metadata_store
            .open_tree(id)?
            .with_context(|| format!("Tree 不存在: {id}"))?;
        let expected_file_count = reader.file_count();
        let tree = Tree {
            files: collect_file_set(self.metadata_store.as_ref(), reader.as_ref())?,
        };
        ensure!(
            u64::try_from(tree.files.len()).context("Tree 文件数量超出 u64")?
                == expected_file_count,
            "Tree 文件数量与存储记录不一致: {id}"
        );
        validate_files(&tree.files)?;
        ensure!(self.tree_id(&tree)? == id, "Tree 内容与其 ID 不匹配: {id}");
        Ok(tree)
    }

    pub(crate) fn list_tree_ids(&self) -> Result<Vec<String>> {
        let snapshot = self.metadata_store.snapshot()?;
        let ids = collect_pages(|request| snapshot.scan_tree_ids(request))?;
        validate_sorted_ids(&ids, "Tree")?;
        Ok(ids)
    }

    pub(crate) fn commit_id(&self, commit: &Commit) -> Result<String> {
        validate_commit(commit)?;
        typed_json_hash(COMMIT_HASH_DOMAIN, commit)
    }

    pub(crate) fn store_commit(&self, commit: &Commit) -> Result<String> {
        let id = self.commit_id(commit)?;
        self.metadata_store.put_commit(&id, commit)?;
        Ok(id)
    }

    pub(crate) fn read_commit(&self, id: &str) -> Result<Commit> {
        read_commit(self.metadata_store.as_ref(), id)
    }

    pub(crate) fn list_commit_ids(&self) -> Result<Vec<String>> {
        let snapshot = self.metadata_store.snapshot()?;
        let ids = collect_pages(|request| snapshot.scan_commit_ids(request))?;
        validate_sorted_ids(&ids, "Commit")?;
        Ok(ids)
    }

    pub(crate) fn list_references(&self, prefix: &str) -> Result<Vec<StoredReference>> {
        let snapshot = self.metadata_store.snapshot()?;
        let references = collect_pages(|request| snapshot.scan_references(prefix, request))?;
        let mut previous: Option<&str> = None;
        for reference in &references {
            if let Some(previous) = previous {
                ensure!(
                    previous < reference.name.as_str(),
                    "元数据后端返回了未排序或重复的引用: {}",
                    reference.name
                );
            }
            previous = Some(&reference.name);
        }
        Ok(references)
    }

    pub(crate) fn verify_metadata_store_integrity(&self) -> Result<()> {
        self.metadata_store.verify_integrity()
    }

    pub(crate) fn current_commit_id(&self) -> Result<Option<String>> {
        current_commit_id(self.metadata_store.as_ref())
    }

    pub(crate) fn update_current_commit(&self, expected: Option<&str>, id: &str) -> Result<()> {
        validate_hash(id, "Commit")?;
        if let Some(expected) = expected {
            validate_hash(expected, "期望父 Commit")?;
        }
        match self.metadata_store.compare_exchange_reference(
            DEFAULT_HEAD_REFERENCE,
            expected,
            Some(id),
        )? {
            ReferenceCas::Updated => Ok(()),
            ReferenceCas::Mismatch { actual } => bail!(
                "分支在提交期间发生变化（期望 {:?}，实际 {:?}），拒绝覆盖并发提交",
                expected,
                actual
            ),
        }
    }
}

fn file_record_from_node(file: &FileNode) -> Result<FileRecord> {
    let manifest = describe_manifest(file.total_size, &file.chunks)?;
    Ok(FileRecord::from_manifest(
        file.path.clone(),
        file.total_size,
        &manifest,
    ))
}

fn current_commit_id<R: MetadataReader + ?Sized>(reader: &R) -> Result<Option<String>> {
    validate_head(reader)?;
    let contents = reader.get_reference(DEFAULT_HEAD_REFERENCE)?;
    let Some(contents) = contents else {
        return Ok(None);
    };
    let id = contents.trim();
    if id.is_empty() {
        return Ok(None);
    }
    validate_hash(id, "Commit")?;
    Ok(Some(id.to_owned()))
}

fn validate_head<R: MetadataReader + ?Sized>(reader: &R) -> Result<()> {
    let reference = reader.read_head_reference()?;
    ensure!(
        reference == DEFAULT_HEAD_REFERENCE,
        "HEAD 必须指向 {DEFAULT_HEAD_REFERENCE}，实际为 {reference}"
    );
    Ok(())
}

fn read_commit<R: MetadataReader + ?Sized>(reader: &R, id: &str) -> Result<Commit> {
    validate_hash(id, "Commit")?;
    let commit = reader
        .get_commit(id)?
        .with_context(|| format!("Commit 不存在: {id}"))?;
    validate_commit(&commit)?;
    ensure!(
        typed_json_hash(COMMIT_HASH_DOMAIN, &commit)? == id,
        "Commit 内容与其 ID 不匹配: {id}"
    );
    Ok(commit)
}

fn collect_file_set<R: MetadataReader + ?Sized>(
    store: &R,
    reader: &dyn FileSetReader,
) -> Result<Vec<FileNode>> {
    let records = collect_pages(|request| reader.scan_files(None, request))?;
    if let Some(first) = records.first() {
        ensure!(
            reader.get_file(&first.path)?.as_ref() == Some(first),
            "元数据后端的点查结果与分页结果不一致: {}",
            first.path
        );
    }
    let mut files = Vec::with_capacity(records.len());
    for record in records {
        let manifest = store
            .open_manifest(&record.manifest_id)?
            .with_context(|| format!("Manifest 不存在: {}", record.manifest_id))?;
        ensure!(
            manifest.total_size() == record.total_size,
            "文件 {} 的大小与 Manifest 不一致",
            record.path
        );
        ensure!(
            manifest.chunk_count() == record.chunk_count,
            "文件 {} 的 Chunk 数量与 Manifest 不一致",
            record.path
        );
        let chunks = collect_pages(|request| manifest.scan_chunks(request))?;
        ensure!(
            u64::try_from(chunks.len()).context("Chunk 数量超出 u64")? == record.chunk_count,
            "文件 {} 的 Chunk 数量与存储记录不一致",
            record.path
        );
        files.push(FileNode {
            path: record.path,
            total_size: record.total_size,
            chunks,
        });
    }
    Ok(files)
}

fn collect_pages<T>(mut scan: impl FnMut(&PageRequest) -> Result<Page<T>>) -> Result<Vec<T>> {
    let mut request = PageRequest::first(STORAGE_SCAN_PAGE_SIZE)?;
    let mut items = Vec::new();
    loop {
        let page = scan(&request)?;
        ensure!(
            page.items.len() <= request.limit_usize()?,
            "元数据后端返回的分页超过请求上限"
        );
        items.extend(page.items);
        let Some(next) = page.next else {
            break;
        };
        ensure!(
            request.after.as_deref() != Some(next.as_str()),
            "元数据后端返回了不前进的分页游标"
        );
        request.after = Some(next);
    }
    Ok(items)
}

pub(crate) struct RepositoryWriteLock {
    file: fs::File,
    path: PathBuf,
}

impl RepositoryWriteLock {
    pub(crate) fn set_operation(
        &mut self,
        operation: &str,
        checkout_transaction: Option<&str>,
    ) -> Result<()> {
        let bytes = json_bytes(&RepositoryLockInfo {
            format_version: LOCK_FORMAT_VERSION,
            pid: std::process::id(),
            operation,
            checkout_transaction,
        })?;
        self.file.set_len(0).context("无法清空仓库写锁信息")?;
        self.file
            .seek(SeekFrom::Start(0))
            .context("无法定位仓库写锁信息")?;
        self.file
            .write_all(&bytes)
            .context("无法写入仓库写锁信息")?;
        self.file.sync_all().context("无法同步仓库写锁信息")
    }
}

impl Drop for RepositoryWriteLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn validate_index(index: &Index) -> Result<()> {
    ensure!(
        index.format_version == INDEX_FORMAT_VERSION,
        "不支持的 index 格式版本 {}（当前支持 {}）",
        index.format_version,
        INDEX_FORMAT_VERSION
    );
    validate_files(&index.files)
}

fn validate_files(files: &[FileNode]) -> Result<()> {
    let mut previous_path: Option<&str> = None;
    let mut portable_paths: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for file in files {
        validate_repository_path(&file.path)?;
        if let Some(previous) = previous_path {
            ensure!(
                previous < file.path.as_str(),
                "文件路径必须严格升序且唯一: {}",
                file.path
            );
        }
        previous_path = Some(&file.path);

        // Windows 默认大小写不敏感，并且路径前缀冲突不一定在字节序排序后相邻，例如
        // `a`、`a-b`、`a/b`。用可移植 key 检查所有祖先和潜在子孙，避免生成只能在
        // 某些平台读取、或同时把一个路径解释为文件和目录的 Tree。
        let portable_path = portable_path_key(&file.path);
        ensure!(
            !portable_paths.contains(&portable_path),
            "Tree 包含跨平台大小写冲突的路径: {}",
            file.path
        );
        for (offset, _) in portable_path.match_indices('/') {
            ensure!(
                !portable_paths.contains(&portable_path[..offset]),
                "Tree 同时包含文件及其子路径: {}",
                file.path
            );
        }
        let descendant_prefix = format!("{portable_path}/");
        if let Some(descendant) = portable_paths.range(descendant_prefix.clone()..).next() {
            ensure!(
                !descendant.starts_with(&descendant_prefix),
                "Tree 同时包含文件及其子路径: {}",
                file.path
            );
        }
        portable_paths.insert(portable_path);

        let mut expected_offset = 0_u64;
        for chunk in &file.chunks {
            validate_hash(&chunk.hash, "Chunk")?;
            ensure!(chunk.size > 0, "Chunk 大小必须大于零: {}", chunk.hash);
            ensure!(
                chunk.offset == expected_offset,
                "文件 {} 的 Chunk 偏移不连续",
                file.path
            );
            expected_offset = expected_offset
                .checked_add(chunk.size)
                .context("累计 Chunk 大小溢出")?;
        }
        ensure!(
            expected_offset == file.total_size,
            "文件 {} 的 Chunk 总大小与文件大小不一致",
            file.path
        );
    }
    Ok(())
}

fn validate_repository_path(path: &str) -> Result<()> {
    ensure!(!path.is_empty(), "仓库文件路径不能为空");
    ensure!(!path.starts_with('/'), "仓库文件路径不能是绝对路径: {path}");
    ensure!(
        !path.contains('\\'),
        "仓库文件路径必须使用 `/` 分隔: {path}"
    );
    ensure!(!path.contains('\0'), "仓库文件路径包含 NUL: {path}");

    for component in path.split('/') {
        ensure!(
            !component.is_empty() && component != "." && component != "..",
            "仓库文件路径未规范化: {path}"
        );
        ensure!(
            !is_neoengram_dir_name(OsStr::new(component)),
            "不能记录 NeoEngram 内部路径: {path}"
        );
        validate_portable_component(component, path)?;
    }
    Ok(())
}

fn portable_path_key(path: &str) -> String {
    path.split('/')
        .map(|component| {
            component
                .nfc()
                .flat_map(char::to_lowercase)
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn validate_portable_component(component: &str, path: &str) -> Result<()> {
    ensure!(
        component.nfc().eq(component.chars()),
        "仓库路径必须使用 Unicode NFC 规范形式: {path}"
    );
    ensure!(
        !component.ends_with(' ') && !component.ends_with('.'),
        "仓库路径组件不能以空格或句点结尾: {path}"
    );
    ensure!(
        !component.chars().any(|character| {
            character <= '\u{1f}' || matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*')
        }),
        "仓库路径包含跨平台不支持的字符: {path}"
    );

    let device_name = component
        .split('.')
        .next()
        .unwrap_or(component)
        .to_ascii_uppercase();
    let reserved = matches!(
        device_name.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$" | "CONIN$" | "CONOUT$"
    ) || device_name.strip_prefix("COM").is_some_and(|suffix| {
        matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
    }) || device_name.strip_prefix("LPT").is_some_and(|suffix| {
        matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
    });
    ensure!(!reserved, "仓库路径使用 Windows 保留设备名: {path}");
    Ok(())
}

fn validate_commit(commit: &Commit) -> Result<()> {
    validate_hash(&commit.tree_hash, "Tree")?;
    if let Some(parent) = &commit.parent {
        validate_hash(parent, "父 Commit")?;
    }
    ensure!(!commit.message.trim().is_empty(), "Commit message 不能为空");
    Ok(())
}

fn validate_hash(hash: &str, kind: &str) -> Result<()> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "{kind} ID 不是有效的小写 BLAKE3 Hash: {hash}"
    );
    Ok(())
}

fn validate_sorted_ids(ids: &[String], kind: &str) -> Result<()> {
    let mut previous: Option<&str> = None;
    for id in ids {
        validate_hash(id, kind)?;
        if let Some(previous) = previous {
            ensure!(
                previous < id.as_str(),
                "元数据后端返回了未排序或重复的 {kind} ID: {id}"
            );
        }
        previous = Some(id);
    }
    Ok(())
}

fn typed_json_hash<T: Serialize>(domain: &[u8], value: &T) -> Result<String> {
    let mut json = serde_json::to_value(value).context("无法构造规范 JSON")?;
    json.sort_all_objects();
    let canonical = serde_json::to_vec(&json).context("无法序列化规范 JSON")?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&[0]);
    hasher.update(&canonical);
    Ok(hasher.finalize().to_hex().to_string())
}

fn read_repository_config(path: &Path) -> Result<RepositoryConfig> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("仓库配置文件不存在: {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "仓库配置路径不是普通文件: {}",
        path.display()
    );
    read_json(path)
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use neoengram_core::{Commit, FileNode, Tree};

    use super::{validate_files, validate_repository_path, Repository};

    #[test]
    fn rejects_non_portable_repository_paths() {
        for path in [
            "/absolute.bin",
            "C:drive-relative.bin",
            "dir\\windows.bin",
            "dir/../escape.bin",
            "CON.txt",
            "aux",
            "LPT9.log",
            "name.",
            "name ",
            "bad?.bin",
            ".NeoEngram/private.bin",
            "cafe\u{301}.bin",
        ] {
            assert!(
                validate_repository_path(path).is_err(),
                "unexpected portable path: {path}"
            );
        }
        assert!(validate_repository_path("caf\u{e9}/model.bin").is_ok());
    }

    #[test]
    fn rejects_case_and_file_directory_collisions() {
        assert!(validate_files(&[empty_node("Model.bin"), empty_node("model.bin")]).is_err());
        assert!(
            validate_files(&[empty_node("a"), empty_node("a-b"), empty_node("a/child"),]).is_err()
        );
    }

    #[test]
    fn current_tree_and_commit_ids_are_stable() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let repository = Repository::at(temporary.path().to_path_buf())?;
        let tree = Tree::default();
        let tree_id = repository.tree_id(&tree)?;
        assert_eq!(
            tree_id,
            "c729abf9a9ed307e4043f7e3e57ef8feb0f416e7c34da3b9873f49c14eeb8653"
        );

        let commit = Commit {
            tree_hash: tree_id,
            parent: None,
            message: "snapshot".to_owned(),
            created_at_unix_ms: 1,
        };
        assert_eq!(
            repository.commit_id(&commit)?,
            "3d4a818bfacc9c0f1119baf3af2b68792f8e2495f73ebb9e0ff7b59123e1516a"
        );
        Ok(())
    }

    fn empty_node(path: &str) -> FileNode {
        FileNode {
            path: path.to_owned(),
            total_size: 0,
            chunks: Vec::new(),
        }
    }
}
