//! `MetadataStore` 的开发期 JSON 文件实现。

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Component, Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use fs2::FileExt;
use neoengram_core::{Chunk, Commit, INDEX_FORMAT_VERSION};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::storage::file::{
    ensure_directory, ensure_or_create_directory, publish_bytes_if_absent, publish_immutable_json,
    publish_json_if_absent, read_json, read_json_optional, sync_parent, write_bytes_atomic,
    write_json_atomic,
};
use crate::storage::STORAGE_TEMP_FILE_PREFIX;

use super::{
    file_manifest_id, tree_records_id, validate_metadata_object_id, validate_reference_name,
    validate_reference_prefix, FileRecord, FileSetReader, IndexReader, IndexTxn, IndexVersion,
    ManifestHasher, ManifestReader, ManifestRef, MetadataReader, MetadataSnapshot, MetadataStore,
    MetadataStoreKind, Page, PageRequest, ReferenceCas, StoredReference, TreeReader, TreeWriter,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

const INDEX_FILE_NAME: &str = "index.json";
const HEAD_FILE_NAME: &str = "HEAD";
const TREES_DIR_NAME: &str = "trees";
const MANIFESTS_DIR_NAME: &str = "manifests";
const COMMITS_DIR_NAME: &str = "commits";
const REFS_DIR_NAME: &str = "refs";
const INDEX_LOCK_FILE_NAME: &str = "index.lock";
const REFS_LOCK_FILE_NAME: &str = "refs.lock";
const INDEX_VERSION_DOMAIN: &[u8] = b"neoengram-index-version-v2";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonIndexState {
    format_version: u32,
    revision: u64,
    files: Vec<FileRecord>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonManifest {
    total_size: u64,
    chunks: Vec<Chunk>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonTree {
    files: Vec<FileRecord>,
}

#[derive(Debug)]
pub(super) struct JsonMetadataStore {
    root: PathBuf,
}

impl JsonMetadataStore {
    pub(super) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn index_file(&self) -> PathBuf {
        self.root.join(INDEX_FILE_NAME)
    }

    fn head_file(&self) -> PathBuf {
        self.root.join(HEAD_FILE_NAME)
    }

    fn trees_dir(&self) -> PathBuf {
        self.root.join(TREES_DIR_NAME)
    }

    fn commits_dir(&self) -> PathBuf {
        self.root.join(COMMITS_DIR_NAME)
    }

    fn manifests_dir(&self) -> PathBuf {
        self.root.join(MANIFESTS_DIR_NAME)
    }

    fn refs_dir(&self) -> PathBuf {
        self.root.join(REFS_DIR_NAME)
    }

    fn tree_file(&self, id: &str) -> Result<PathBuf> {
        validate_metadata_object_id(id)?;
        Ok(self.trees_dir().join(format!("{id}.json")))
    }

    fn commit_file(&self, id: &str) -> Result<PathBuf> {
        validate_metadata_object_id(id)?;
        Ok(self.commits_dir().join(format!("{id}.json")))
    }

    fn manifest_file(&self, id: &str) -> Result<PathBuf> {
        validate_metadata_object_id(id)?;
        Ok(self.manifests_dir().join(format!("{id}.json")))
    }

    fn reference_file(&self, name: &str) -> Result<PathBuf> {
        validate_reference_name(name)?;
        Ok(logical_join(&self.root, name))
    }

    fn read_index_state(&self) -> Result<JsonIndexState> {
        ensure_regular_file(&self.index_file(), "index")?;
        read_json(&self.index_file())
    }

    fn read_tree_data(&self, id: &str) -> Result<Option<JsonTree>> {
        let path = self.tree_file(id)?;
        ensure_regular_file_if_present(&path, "Tree")?;
        let tree: Option<JsonTree> = read_json_optional(&path)?;
        if let Some(tree) = &tree {
            ensure!(
                tree_records_id(&tree.files)? == id,
                "Tree 内容与其 ID 不匹配: {id}"
            );
        }
        Ok(tree)
    }

    fn read_commit_data(&self, id: &str) -> Result<Option<Commit>> {
        let path = self.commit_file(id)?;
        ensure_regular_file_if_present(&path, "Commit")?;
        read_json_optional(&path)
    }

    fn read_manifest_data(&self, id: &str) -> Result<Option<JsonManifest>> {
        let path = self.manifest_file(id)?;
        ensure_regular_file_if_present(&path, "Manifest")?;
        let manifest: Option<JsonManifest> = read_json_optional(&path)?;
        if let Some(manifest) = &manifest {
            ensure!(
                file_manifest_id(manifest.total_size, &manifest.chunks)? == id,
                "Manifest 内容与其 ID 不匹配: {id}"
            );
        }
        Ok(manifest)
    }

    fn validate_stored_file_record(&self, file: &FileRecord) -> Result<()> {
        validate_file_record(file)?;
        let manifest = self
            .read_manifest_data(&file.manifest_id)?
            .with_context(|| format!("Manifest 不存在: {}", file.manifest_id))?;
        ensure!(
            manifest.total_size == file.total_size,
            "文件 {} 的大小与 Manifest 不一致",
            file.path
        );
        ensure!(
            u64::try_from(manifest.chunks.len()).context("Chunk 数量超出 u64")? == file.chunk_count,
            "文件 {} 的 Chunk 数量与 Manifest 不一致",
            file.path
        );
        Ok(())
    }

    fn read_head_reference_data(&self) -> Result<String> {
        let path = self.head_file();
        ensure_regular_file(&path, "HEAD")?;
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("无法读取 HEAD: {}", path.display()))?;
        let reference = contents
            .strip_prefix("ref: ")
            .and_then(|value| value.strip_suffix('\n'))
            .context("HEAD 格式无效")?;
        ensure!(!reference.contains('\n'), "HEAD 包含多余内容");
        validate_reference_name(reference)?;
        Ok(reference.to_owned())
    }

    fn read_reference_data(&self, name: &str) -> Result<Option<String>> {
        let path = self.reference_file(name)?;
        ensure_regular_file_if_present(&path, "引用")?;
        match fs::read_to_string(&path) {
            Ok(contents) => Ok(Some(contents.trim().to_owned())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("无法读取引用: {}", path.display())),
        }
    }

    fn list_references_data(&self) -> Result<Vec<StoredReference>> {
        let start = self.refs_dir();
        let mut references = Vec::new();
        for entry in WalkDir::new(&start).follow_links(false).min_depth(1) {
            let entry = entry.with_context(|| format!("无法遍历引用目录: {}", start.display()))?;
            if entry.file_type().is_dir() {
                continue;
            }
            if entry.file_type().is_file() && is_unpublished_temporary_file(entry.file_name()) {
                continue;
            }
            ensure!(
                entry.file_type().is_file(),
                "引用不是普通文件: {}",
                entry.path().display()
            );
            let relative = entry
                .path()
                .strip_prefix(&self.root)
                .with_context(|| format!("引用不在元数据根目录内: {}", entry.path().display()))?;
            let name = logical_path(relative)?;
            validate_reference_name(&name)?;
            let target = fs::read_to_string(entry.path())
                .with_context(|| format!("无法读取引用: {}", entry.path().display()))?
                .trim()
                .to_owned();
            references.push(StoredReference { name, target });
        }
        references.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        Ok(references)
    }

    fn acquire_storage_lock(&self, name: &str, kind: &str, shared: bool) -> Result<File> {
        let path = self.root.join(name);
        ensure_regular_file_if_present(&path, kind)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options
            .open(&path)
            .with_context(|| format!("无法打开{kind}: {}", path.display()))?;
        let opened_metadata = file
            .metadata()
            .with_context(|| format!("无法检查{kind}: {}", path.display()))?;
        let path_metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("无法再次检查{kind}: {}", path.display()))?;
        ensure!(
            opened_metadata.is_file()
                && path_metadata.is_file()
                && !path_metadata.file_type().is_symlink(),
            "{kind}不是普通文件: {}",
            path.display()
        );
        #[cfg(unix)]
        ensure!(
            opened_metadata.dev() == path_metadata.dev()
                && opened_metadata.ino() == path_metadata.ino(),
            "{kind}在打开期间被替换: {}",
            path.display()
        );
        if shared {
            FileExt::lock_shared(&file)
                .with_context(|| format!("无法取得{kind}共享锁: {}", path.display()))?;
        } else {
            FileExt::try_lock_exclusive(&file)
                .with_context(|| format!("另一个元数据事务正在持有{kind}: {}", path.display()))?;
        }
        Ok(file)
    }
}

impl MetadataReader for JsonMetadataStore {
    fn read_head_reference(&self) -> Result<String> {
        self.read_head_reference_data()
    }

    fn get_reference(&self, name: &str) -> Result<Option<String>> {
        self.read_reference_data(name)
    }

    fn open_manifest(&self, id: &str) -> Result<Option<Box<dyn ManifestReader + '_>>> {
        self.read_manifest_data(id)?
            .map(JsonManifestReader::new)
            .transpose()
            .map(|reader| reader.map(|reader| Box::new(reader) as Box<dyn ManifestReader>))
    }

    fn open_tree(&self, id: &str) -> Result<Option<Box<dyn TreeReader + '_>>> {
        self.read_tree_data(id)?
            .map(JsonTreeReader::new)
            .transpose()
            .map(|reader| reader.map(|reader| Box::new(reader) as Box<dyn TreeReader>))
    }

    fn get_commit(&self, id: &str) -> Result<Option<Commit>> {
        self.read_commit_data(id)
    }
}

impl MetadataStore for JsonMetadataStore {
    fn kind(&self) -> MetadataStoreKind {
        MetadataStoreKind::Json
    }

    fn initialize(&self, head_reference: &str) -> Result<()> {
        validate_reference_name(head_reference)?;
        for directory in [
            self.root.clone(),
            self.trees_dir(),
            self.manifests_dir(),
            self.commits_dir(),
            self.refs_dir(),
        ] {
            ensure_or_create_directory(&directory)?;
        }
        ensure_reference_parent(&self.root, head_reference)?;

        publish_json_if_absent(
            &self.index_file(),
            &JsonIndexState {
                format_version: INDEX_FORMAT_VERSION,
                revision: 0,
                files: Vec::new(),
            },
        )?;
        publish_bytes_if_absent(
            &self.head_file(),
            format!("ref: {head_reference}\n").as_bytes(),
        )?;
        self.validate_layout()
    }

    fn validate_layout(&self) -> Result<()> {
        for (directory, kind) in [
            (&self.root, "元数据"),
            (&self.trees_dir(), "Tree"),
            (&self.manifests_dir(), "Manifest"),
            (&self.commits_dir(), "Commit"),
            (&self.refs_dir(), "引用"),
        ] {
            ensure_directory(directory, kind)?;
        }
        ensure_regular_file(&self.index_file(), "index")?;
        self.read_head_reference_data()?;
        Ok(())
    }

    fn verify_integrity(&self) -> Result<()> {
        self.validate_layout()?;
        let snapshot = self.snapshot()?;

        let mut request = PageRequest::first(super::MAX_PAGE_SIZE)?;
        loop {
            let page = snapshot.scan_tree_ids(&request)?;
            for id in &page.items {
                ensure!(snapshot.open_tree(id)?.is_some(), "Tree 在枚举后消失: {id}");
            }
            let Some(next) = page.next else {
                break;
            };
            request.after = Some(next);
        }

        let mut request = PageRequest::first(super::MAX_PAGE_SIZE)?;
        loop {
            let page = snapshot.scan_manifest_ids(&request)?;
            for id in &page.items {
                ensure!(
                    self.open_manifest(id)?.is_some(),
                    "Manifest 在枚举后消失: {id}"
                );
            }
            let Some(next) = page.next else {
                break;
            };
            request.after = Some(next);
        }

        let mut request = PageRequest::first(super::MAX_PAGE_SIZE)?;
        loop {
            let page = snapshot.scan_commit_ids(&request)?;
            for id in &page.items {
                ensure!(
                    snapshot.get_commit(id)?.is_some(),
                    "Commit 在枚举后消失: {id}"
                );
            }
            let Some(next) = page.next else {
                break;
            };
            request.after = Some(next);
        }

        let mut request = PageRequest::first(super::MAX_PAGE_SIZE)?;
        loop {
            let page = snapshot.scan_references("refs", &request)?;
            let Some(next) = page.next else {
                break;
            };
            request.after = Some(next);
        }
        Ok(())
    }

    fn read_index(&self) -> Result<Box<dyn IndexReader + '_>> {
        Ok(Box::new(JsonIndexReader::new(self.read_index_state()?)?))
    }

    fn begin_index_transaction(
        &self,
        expected: Option<&IndexVersion>,
    ) -> Result<Box<dyn IndexTxn + '_>> {
        let lock = self.acquire_storage_lock(INDEX_LOCK_FILE_NAME, "Index 事务锁", false)?;
        let reader = JsonIndexReader::new(self.read_index_state()?)?;
        if let Some(expected) = expected {
            ensure!(
                reader.version() == expected,
                "Index 已被其他操作更新，无法基于旧版本提交"
            );
        }
        Ok(Box::new(JsonIndexTxn {
            store: self,
            base_version: reader.version.clone(),
            base_revision: reader.revision,
            format_version: reader.format_version,
            files: reader.files,
            _lock: lock,
        }))
    }

    fn snapshot(&self) -> Result<Box<dyn MetadataSnapshot + '_>> {
        // 先固定 refs，再按发布顺序的逆序捕获 Commit、Tree 和 Manifest ID。不可变对象
        // 不删除，因此任何可见 ref 的依赖闭包都包含在同一个 snapshot 中。
        let refs_lock = self.acquire_storage_lock(REFS_LOCK_FILE_NAME, "引用事务锁", true)?;
        let head_reference = self.read_head_reference_data()?;
        let references = self.list_references_data()?;
        drop(refs_lock);
        let commit_ids = list_json_ids(&self.commits_dir(), "Commit")?;
        let tree_ids = list_json_ids(&self.trees_dir(), "Tree")?;
        let manifest_ids = list_json_ids(&self.manifests_dir(), "Manifest")?;

        Ok(Box::new(JsonMetadataSnapshot {
            store: self,
            head_reference,
            references,
            tree_ids,
            manifest_ids,
            commit_ids,
        }))
    }

    fn put_manifest(
        &self,
        total_size: u64,
        chunks: &mut dyn Iterator<Item = Result<Chunk>>,
    ) -> Result<ManifestRef> {
        let mut hasher = ManifestHasher::new(total_size);
        let mut stored_chunks = Vec::new();
        for chunk in chunks {
            let chunk = chunk?;
            hasher.push(&chunk)?;
            stored_chunks.push(chunk);
        }
        let manifest = hasher.finish()?;
        let path = self.manifest_file(&manifest.id)?;
        ensure_regular_file_if_present(&path, "Manifest")?;
        publish_immutable_json(
            &path,
            &JsonManifest {
                total_size,
                chunks: stored_chunks,
            },
        )?;
        Ok(manifest)
    }

    fn begin_tree_write(&self) -> Result<Box<dyn TreeWriter + '_>> {
        Ok(Box::new(JsonTreeWriter {
            store: self,
            files: Vec::new(),
        }))
    }

    fn put_commit(&self, id: &str, commit: &Commit) -> Result<()> {
        let path = self.commit_file(id)?;
        ensure_regular_file_if_present(&path, "Commit")?;
        publish_immutable_json(&path, commit)
    }

    fn compare_exchange_reference(
        &self,
        name: &str,
        expected: Option<&str>,
        new_target: Option<&str>,
    ) -> Result<ReferenceCas> {
        let path = self.reference_file(name)?;
        let expected = normalize_optional_target(expected, "期望引用目标")?;
        let new_target = normalize_optional_target(new_target, "新引用目标")?;
        let _lock = self.acquire_storage_lock(REFS_LOCK_FILE_NAME, "引用事务锁", false)?;
        let actual = self.read_reference_data(name)?;
        if actual.as_deref() != expected {
            return Ok(ReferenceCas::Mismatch { actual });
        }

        match new_target {
            Some(target) => {
                ensure_reference_parent(&self.root, name)?;
                ensure_regular_file_if_present(&path, "引用")?;
                write_bytes_atomic(&path, format!("{target}\n").as_bytes())?;
            }
            None if actual.is_some() => {
                ensure_regular_file(&path, "引用")?;
                fs::remove_file(&path)
                    .with_context(|| format!("无法删除引用: {}", path.display()))?;
                sync_parent(&path)?;
            }
            None => {}
        }
        Ok(ReferenceCas::Updated)
    }
}

#[derive(Debug)]
struct JsonManifestReader {
    manifest: JsonManifest,
    chunk_count: u64,
}

impl JsonManifestReader {
    fn new(manifest: JsonManifest) -> Result<Self> {
        let chunk_count =
            u64::try_from(manifest.chunks.len()).context("Manifest Chunk 数量超出 u64")?;
        Ok(Self {
            manifest,
            chunk_count,
        })
    }
}

impl ManifestReader for JsonManifestReader {
    fn total_size(&self) -> u64 {
        self.manifest.total_size
    }

    fn chunk_count(&self) -> u64 {
        self.chunk_count
    }

    fn scan_chunks(&self, request: &PageRequest) -> Result<Page<Chunk>> {
        paginate_chunks(&self.manifest.chunks, request)
    }
}

#[derive(Debug)]
struct JsonIndexReader {
    format_version: u32,
    files: Vec<FileRecord>,
    revision: u64,
    version: IndexVersion,
}

impl JsonIndexReader {
    fn new(state: JsonIndexState) -> Result<Self> {
        ensure!(
            state.format_version == INDEX_FORMAT_VERSION,
            "不支持的 Index 格式版本 {}（当前支持 {}）",
            state.format_version,
            INDEX_FORMAT_VERSION
        );
        ensure_file_records_strictly_sorted(&state.files, "Index")?;
        let version = index_version(&state)?;
        Ok(Self {
            format_version: state.format_version,
            files: state.files,
            revision: state.revision,
            version,
        })
    }
}

impl FileSetReader for JsonIndexReader {
    fn get_file(&self, path: &str) -> Result<Option<FileRecord>> {
        get_file(&self.files, path)
    }

    fn scan_files(&self, prefix: Option<&str>, request: &PageRequest) -> Result<Page<FileRecord>> {
        scan_files(&self.files, prefix, request)
    }
}

impl IndexReader for JsonIndexReader {
    fn format_version(&self) -> u32 {
        self.format_version
    }

    fn version(&self) -> &IndexVersion {
        &self.version
    }
}

#[derive(Debug)]
struct JsonIndexTxn<'a> {
    store: &'a JsonMetadataStore,
    format_version: u32,
    files: Vec<FileRecord>,
    base_version: IndexVersion,
    base_revision: u64,
    _lock: File,
}

impl FileSetReader for JsonIndexTxn<'_> {
    fn get_file(&self, path: &str) -> Result<Option<FileRecord>> {
        get_file(&self.files, path)
    }

    fn scan_files(&self, prefix: Option<&str>, request: &PageRequest) -> Result<Page<FileRecord>> {
        scan_files(&self.files, prefix, request)
    }
}

impl IndexReader for JsonIndexTxn<'_> {
    fn format_version(&self) -> u32 {
        self.format_version
    }

    fn version(&self) -> &IndexVersion {
        &self.base_version
    }
}

impl IndexTxn for JsonIndexTxn<'_> {
    fn upsert_file(&mut self, file: &FileRecord) -> Result<()> {
        self.store.validate_stored_file_record(file)?;
        match self
            .files
            .binary_search_by(|existing| existing.path.cmp(&file.path))
        {
            Ok(index) => self.files[index] = file.clone(),
            Err(index) => self.files.insert(index, file.clone()),
        }
        Ok(())
    }

    fn delete_prefix(&mut self, prefix: &str) -> Result<u64> {
        let before = self.files.len();
        self.files
            .retain(|file| !path_is_in_scope(&file.path, prefix));
        let deleted = before
            .checked_sub(self.files.len())
            .context("Index 删除计数下溢")?;
        u64::try_from(deleted).context("Index 删除计数超出 u64")
    }

    fn commit(self: Box<Self>) -> Result<IndexVersion> {
        let current = self.store.read_index_state()?;
        ensure!(
            index_version(&current)? == self.base_version,
            "Index 在事务执行期间被其他操作更新"
        );
        let revision = self
            .base_revision
            .checked_add(1)
            .context("Index revision 溢出")?;
        let state = JsonIndexState {
            format_version: self.format_version,
            revision,
            files: self.files,
        };
        ensure_regular_file_if_present(&self.store.index_file(), "index")?;
        write_json_atomic(&self.store.index_file(), &state)?;
        index_version(&state)
    }
}

#[derive(Debug)]
struct JsonTreeReader {
    tree: JsonTree,
    file_count: u64,
}

impl JsonTreeReader {
    fn new(tree: JsonTree) -> Result<Self> {
        ensure_file_records_strictly_sorted(&tree.files, "Tree")?;
        let file_count = u64::try_from(tree.files.len()).context("Tree 文件数量超出 u64")?;
        Ok(Self { tree, file_count })
    }
}

impl FileSetReader for JsonTreeReader {
    fn get_file(&self, path: &str) -> Result<Option<FileRecord>> {
        get_file(&self.tree.files, path)
    }

    fn scan_files(&self, prefix: Option<&str>, request: &PageRequest) -> Result<Page<FileRecord>> {
        scan_files(&self.tree.files, prefix, request)
    }
}

impl TreeReader for JsonTreeReader {
    fn file_count(&self) -> u64 {
        self.file_count
    }
}

#[derive(Debug)]
struct JsonTreeWriter<'a> {
    store: &'a JsonMetadataStore,
    files: Vec<FileRecord>,
}

impl TreeWriter for JsonTreeWriter<'_> {
    fn append_file(&mut self, file: &FileRecord) -> Result<()> {
        self.store.validate_stored_file_record(file)?;
        if let Some(previous) = self.files.last() {
            ensure!(
                previous.path.as_str() < file.path.as_str(),
                "TreeWriter 要求文件路径严格递增: {}",
                file.path
            );
        }
        self.files.push(file.clone());
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<String> {
        let id = tree_records_id(&self.files)?;
        let path = self.store.tree_file(&id)?;
        ensure_regular_file_if_present(&path, "Tree")?;
        publish_immutable_json(&path, &JsonTree { files: self.files })?;
        Ok(id)
    }
}

#[derive(Debug)]
struct JsonMetadataSnapshot<'a> {
    store: &'a JsonMetadataStore,
    head_reference: String,
    references: Vec<StoredReference>,
    tree_ids: Vec<String>,
    manifest_ids: Vec<String>,
    commit_ids: Vec<String>,
}

impl MetadataReader for JsonMetadataSnapshot<'_> {
    fn read_head_reference(&self) -> Result<String> {
        Ok(self.head_reference.clone())
    }

    fn get_reference(&self, name: &str) -> Result<Option<String>> {
        validate_reference_name(name)?;
        match self
            .references
            .binary_search_by(|reference| reference.name.as_str().cmp(name))
        {
            Ok(index) => Ok(Some(self.references[index].target.clone())),
            Err(_) => Ok(None),
        }
    }

    fn open_manifest(&self, id: &str) -> Result<Option<Box<dyn ManifestReader + '_>>> {
        validate_metadata_object_id(id)?;
        if self
            .manifest_ids
            .binary_search_by(|item| item.as_str().cmp(id))
            .is_err()
        {
            return Ok(None);
        }
        self.store.open_manifest(id)
    }

    fn open_tree(&self, id: &str) -> Result<Option<Box<dyn TreeReader + '_>>> {
        validate_metadata_object_id(id)?;
        if self
            .tree_ids
            .binary_search_by(|item| item.as_str().cmp(id))
            .is_err()
        {
            return Ok(None);
        }
        self.store
            .read_tree_data(id)?
            .map(JsonTreeReader::new)
            .transpose()
            .map(|reader| reader.map(|reader| Box::new(reader) as Box<dyn TreeReader>))
    }

    fn get_commit(&self, id: &str) -> Result<Option<Commit>> {
        validate_metadata_object_id(id)?;
        if self
            .commit_ids
            .binary_search_by(|item| item.as_str().cmp(id))
            .is_err()
        {
            return Ok(None);
        }
        self.store.read_commit_data(id)
    }
}

impl MetadataSnapshot for JsonMetadataSnapshot<'_> {
    fn scan_tree_ids(&self, request: &PageRequest) -> Result<Page<String>> {
        paginate_strings(&self.tree_ids, request)
    }

    fn scan_manifest_ids(&self, request: &PageRequest) -> Result<Page<String>> {
        paginate_strings(&self.manifest_ids, request)
    }

    fn scan_commit_ids(&self, request: &PageRequest) -> Result<Page<String>> {
        paginate_strings(&self.commit_ids, request)
    }

    fn scan_references(
        &self,
        prefix: &str,
        request: &PageRequest,
    ) -> Result<Page<StoredReference>> {
        validate_reference_prefix(prefix)?;
        paginate_references(&self.references, prefix, request)
    }
}

fn index_version(state: &JsonIndexState) -> Result<IndexVersion> {
    let bytes = serde_json::to_vec(&(state.format_version, &state.files))
        .context("无法序列化 Index 版本")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(INDEX_VERSION_DOMAIN);
    hasher.update(&[0]);
    hasher.update(&bytes);
    Ok(IndexVersion::new(
        state.revision,
        *hasher.finalize().as_bytes(),
    ))
}

fn get_file(files: &[FileRecord], path: &str) -> Result<Option<FileRecord>> {
    Ok(files
        .binary_search_by(|file| file.path.as_str().cmp(path))
        .ok()
        .map(|index| files[index].clone()))
}

fn scan_files(
    files: &[FileRecord],
    prefix: Option<&str>,
    request: &PageRequest,
) -> Result<Page<FileRecord>> {
    let limit = request.limit_usize()?;
    let start = request.after.as_ref().map_or(0, |after| {
        files.partition_point(|file| file.path.as_str() <= after.as_str())
    });
    let mut records = Vec::with_capacity(limit.saturating_add(1));
    for file in &files[start..] {
        if prefix.is_none_or(|prefix| path_is_in_scope(&file.path, prefix)) {
            records.push(file.clone());
            if records.len() > limit {
                break;
            }
        }
    }
    let has_more = records.len() > limit;
    if has_more {
        records.pop();
    }
    let next = has_more.then(|| {
        records
            .last()
            .expect("有后续页时当前页不可能为空")
            .path
            .clone()
    });
    Ok(Page {
        items: records,
        next,
    })
}

fn paginate_chunks(chunks: &[Chunk], request: &PageRequest) -> Result<Page<Chunk>> {
    let limit = request.limit_usize()?;
    let start = match &request.after {
        Some(after) => after
            .parse::<usize>()
            .with_context(|| format!("Chunk cursor 无效: {after}"))?
            .checked_add(1)
            .context("Chunk cursor 溢出")?,
        None => 0,
    };
    ensure!(start <= chunks.len(), "Chunk cursor 超出 Manifest 范围");
    let end = start.saturating_add(limit).min(chunks.len());
    let items = chunks[start..end].to_vec();
    let next = (end < chunks.len()).then(|| {
        end.checked_sub(1)
            .expect("非空 Chunk 页的结束位置必须大于零")
            .to_string()
    });
    Ok(Page { items, next })
}

fn ensure_file_records_strictly_sorted(files: &[FileRecord], kind: &str) -> Result<()> {
    for file in files {
        validate_file_record(file)?;
    }
    for pair in files.windows(2) {
        ensure!(
            pair[0].path < pair[1].path,
            "{kind} 文件路径未严格递增: {} -> {}",
            pair[0].path,
            pair[1].path
        );
    }
    Ok(())
}

fn validate_file_record(file: &FileRecord) -> Result<()> {
    validate_metadata_object_id(&file.manifest_id)?;
    ensure!(
        (file.total_size == 0) == (file.chunk_count == 0),
        "文件 {} 的大小与 Chunk 数量不一致",
        file.path
    );
    Ok(())
}

fn paginate_strings(values: &[String], request: &PageRequest) -> Result<Page<String>> {
    let limit = request.limit_usize()?;
    let start = request.after.as_ref().map_or(0, |after| {
        values.partition_point(|value| value.as_str() <= after.as_str())
    });
    let end = start.saturating_add(limit).min(values.len());
    let items = values[start..end].to_vec();
    let next =
        (end < values.len()).then(|| items.last().expect("有后续页时当前页不可能为空").clone());
    Ok(Page { items, next })
}

fn paginate_references(
    references: &[StoredReference],
    prefix: &str,
    request: &PageRequest,
) -> Result<Page<StoredReference>> {
    let limit = request.limit_usize()?;
    let start = request.after.as_ref().map_or(0, |after| {
        references.partition_point(|reference| reference.name.as_str() <= after.as_str())
    });
    let mut items = Vec::with_capacity(limit.saturating_add(1));
    for reference in &references[start..] {
        if reference_is_in_scope(&reference.name, prefix) {
            items.push(reference.clone());
            if items.len() > limit {
                break;
            }
        }
    }
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next = has_more.then(|| {
        items
            .last()
            .expect("有后续页时当前页不可能为空")
            .name
            .clone()
    });
    Ok(Page { items, next })
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

fn normalize_optional_target<'a>(target: Option<&'a str>, kind: &str) -> Result<Option<&'a str>> {
    target
        .map(|target| {
            let target = target.trim();
            ensure!(!target.is_empty(), "{kind}不能为空");
            ensure!(!target.contains(['\n', '\r']), "{kind}不能包含换行符");
            Ok(target)
        })
        .transpose()
}

fn ensure_reference_parent(root: &Path, name: &str) -> Result<()> {
    let path = logical_join(root, name);
    let parent = path
        .parent()
        .with_context(|| format!("引用没有父目录: {name}"))?;
    let relative = parent
        .strip_prefix(root)
        .with_context(|| format!("引用父目录逃逸元数据根目录: {name}"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            anyhow::bail!("引用父目录未规范化: {name}");
        };
        current.push(component);
        ensure_or_create_directory(&current)?;
    }
    Ok(())
}

fn list_json_ids(directory: &Path, kind: &str) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("无法读取 {kind} 目录: {}", directory.display()))?
    {
        let entry = entry.with_context(|| format!("无法读取 {kind} 目录项"))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("无法读取 {kind} 类型: {}", entry.path().display()))?;
        if file_type.is_file() && is_unpublished_temporary_file(&entry.file_name()) {
            continue;
        }
        ensure!(
            file_type.is_file(),
            "{kind} 目录包含非普通文件: {}",
            entry.path().display()
        );
        let path = entry.path();
        ensure!(
            path.extension().and_then(|extension| extension.to_str()) == Some("json"),
            "{kind} 元数据扩展名无效: {}",
            path.display()
        );
        let id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .with_context(|| format!("{kind} 文件名不是有效 UTF-8: {}", path.display()))?;
        validate_metadata_object_id(id)?;
        ids.push(id.to_owned());
    }
    ids.sort_unstable();
    Ok(ids)
}

fn is_unpublished_temporary_file(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with(STORAGE_TEMP_FILE_PREFIX))
}

fn ensure_regular_file(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{kind} 文件不存在: {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{kind} 路径不是普通文件: {}",
        path.display()
    );
    Ok(())
}

fn ensure_regular_file_if_present(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "{kind} 路径不是普通文件: {}",
                path.display()
            );
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("无法检查 {kind}: {}", path.display())),
    }
}

fn logical_join(base: &Path, logical: &str) -> PathBuf {
    let mut path = base.to_path_buf();
    for component in logical.split('/') {
        path.push(component);
    }
    path
}

fn logical_path(path: &Path) -> Result<String> {
    let mut components = Vec::new();
    for component in path.components() {
        let Component::Normal(component) = component else {
            anyhow::bail!("元数据路径未规范化: {}", path.display());
        };
        components.push(
            component
                .to_str()
                .with_context(|| format!("元数据路径不是有效 UTF-8: {}", path.display()))?,
        );
    }
    Ok(components.join("/"))
}
