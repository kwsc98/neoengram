use std::collections::HashSet;

use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{Commit, DirectoryEntry, DirectoryEntryKind, FileNode, Index, Tree};

use crate::local::metadata::{
    DirectoryWriter, FileRecord, FileSetReader, HeadCas, HeadState, IndexVersion, MetadataReader,
    MetadataStore, Page, PageRequest, ReferenceCas, StoredReference,
};

use super::{
    validation::{
        commit_content_id, validate_commit, validate_files, validate_hash, validate_sorted_ids,
    },
    Repository, DEFAULT_HEAD_REFERENCE,
};

const STORAGE_SCAN_PAGE_SIZE: u32 = 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryHead {
    state: HeadState,
    commit_id: Option<String>,
}

/// A materialized Index and the exact opaque storage version it was read from.
///
/// Keeping the two values together prevents a long-running command from accidentally rereading
/// a newer Index and combining it with work computed from the original worktree scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionedIndex {
    index: Index,
    version: IndexVersion,
}

impl VersionedIndex {
    pub(crate) fn index(&self) -> &Index {
        &self.index
    }

    pub(crate) fn version(&self) -> &IndexVersion {
        &self.version
    }

    pub(crate) fn into_parts(self) -> (Index, IndexVersion) {
        (self.index, self.version)
    }
}

impl RepositoryHead {
    pub(crate) fn state(&self) -> &HeadState {
        &self.state
    }

    pub(crate) fn commit_id(&self) -> Option<&str> {
        self.commit_id.as_deref()
    }

    pub(crate) fn is_detached(&self) -> bool {
        matches!(self.state, HeadState::Detached { .. })
    }

    pub(crate) fn branch_name(&self) -> Option<&str> {
        match &self.state {
            HeadState::Attached { reference } => {
                reference.strip_prefix("refs/heads/").or(Some(reference))
            }
            HeadState::Detached { .. } => None,
        }
    }
}

impl Repository {
    pub(crate) fn read_index(&self) -> Result<Index> {
        Ok(self.read_index_versioned()?.index)
    }

    pub(crate) fn read_index_versioned(&self) -> Result<VersionedIndex> {
        let reader = self.metadata_store.read_index()?;
        let index = Index {
            format_version: reader.format_version(),
            files: collect_file_set(self.metadata_store.as_ref(), reader.as_ref())?,
        };
        self.validate_index_snapshot(&index)?;
        let version = reader.version().clone();
        Ok(VersionedIndex { index, version })
    }

    /// Read only the opaque Index version for an end-of-scan consistency check.
    pub(crate) fn current_index_version(&self) -> Result<IndexVersion> {
        Ok(self.metadata_store.read_index()?.version().clone())
    }

    pub(crate) fn write_index(&self, index: &Index) -> Result<()> {
        let reader = self.metadata_store.read_index()?;
        let expected = reader.version().clone();
        drop(reader);
        self.write_index_if_version(index, &expected)?;
        Ok(())
    }

    /// Publish `index` only if the metadata store still has `expected` as its current version.
    /// Immutable manifests may be published before the CAS; on a mismatch they remain harmless
    /// unreachable objects and the Index itself is left untouched.
    pub(crate) fn write_index_if_version(
        &self,
        index: &Index,
        expected: &IndexVersion,
    ) -> Result<IndexVersion> {
        self.validate_index_snapshot(index)?;
        let mut records = Vec::with_capacity(index.files.len());
        for file in &index.files {
            let mut chunks = file.chunks.iter().cloned().map(Ok);
            let manifest =
                self.metadata_store
                    .put_manifest(file.total_size, file.chunking, &mut chunks)?;
            let record = FileRecord::from_manifest(file.path.clone(), file.total_size, &manifest);
            records.push(record);
        }
        let mut transaction = self
            .metadata_store
            .begin_index_transaction(Some(expected))?;
        transaction.delete_prefix("")?;
        for record in &records {
            transaction.upsert_file(record)?;
        }
        transaction.commit()
    }

    #[cfg(test)]
    pub(crate) fn store_tree(&self, tree: &Tree) -> Result<String> {
        validate_files(&tree.files)?;
        self.validate_chunking_files(&tree.files)?;
        let mut builder = DirectoryDagBuilder::new(self.metadata_store.as_ref())?;
        for file in &tree.files {
            let mut chunks = file.chunks.iter().cloned().map(Ok);
            let manifest =
                self.metadata_store
                    .put_manifest(file.total_size, file.chunking, &mut chunks)?;
            let record = FileRecord::from_manifest(file.path.clone(), file.total_size, &manifest);
            builder.push(record)?;
        }
        builder.finish()
    }

    /// Publishes the current ordered Index as a Directory DAG without materializing a flat Tree.
    pub(crate) fn store_index_tree(&self, require_nonempty: bool) -> Result<(String, u64)> {
        let index = self.metadata_store.read_index()?;
        ensure!(
            index.format_version() == neoengram_core::INDEX_FORMAT_VERSION,
            "不支持的 index 格式版本 {}",
            index.format_version()
        );
        let mut request = PageRequest::first(STORAGE_SCAN_PAGE_SIZE)?;
        let mut page = index.scan_files(None, &request)?;
        if require_nonempty {
            ensure!(
                !page.items.is_empty(),
                "没有可提交的文件；请先运行 `neoengram add`"
            );
        }

        let mut builder = DirectoryDagBuilder::new(self.metadata_store.as_ref())?;
        let mut file_count = 0_u64;
        loop {
            ensure!(
                page.items.len() <= request.limit_usize()?,
                "元数据后端返回的 Index 分页超过请求上限"
            );
            for record in page.items {
                self.verify_index_manifest(&record)?;
                builder.push(record)?;
                file_count = file_count.checked_add(1).context("Index 文件数量溢出")?;
            }
            let Some(next) = page.next else {
                break;
            };
            ensure!(
                request.after.as_deref() != Some(next.as_str()),
                "元数据后端返回了不前进的 Index 分页游标"
            );
            request.after = Some(next);
            page = index.scan_files(None, &request)?;
        }
        self.object_store.durability_barrier()?;
        Ok((builder.finish()?, file_count))
    }

    fn verify_index_manifest(&self, record: &FileRecord) -> Result<()> {
        self.validate_logical_path(&record.path)?;
        let manifest = self.open_manifest(&record.manifest_id)?;
        ensure!(
            manifest.total_size() == record.total_size,
            "Index 文件 {} 的大小与 Manifest 不一致",
            record.path
        );
        ensure!(
            manifest.chunk_count() == record.chunk_count,
            "Index 文件 {} 的 Chunk 数量与 Manifest 不一致",
            record.path
        );
        self.validate_chunking_strategy(manifest.chunking())
            .with_context(|| format!("Index 文件分块策略不符合仓库配置: {}", record.path))?;
        let mut request = PageRequest::first(STORAGE_SCAN_PAGE_SIZE)?;
        let mut chunk_count = 0_u64;
        loop {
            let page = manifest.scan_chunks(&request)?;
            ensure!(
                page.items.len() <= request.limit_usize()?,
                "元数据后端返回的 Manifest 分页超过请求上限"
            );
            for chunk in page.items {
                self.object_store
                    .verify(&crate::local::objects::ObjectSpec::new(
                        chunk.hash, chunk.size,
                    )?)?;
                chunk_count = chunk_count.checked_add(1).context("Chunk 数量溢出")?;
            }
            let Some(next) = page.next else {
                break;
            };
            ensure!(
                request.after.as_deref() != Some(next.as_str()),
                "元数据后端返回了不前进的 Manifest 分页游标"
            );
            request.after = Some(next);
        }
        ensure!(
            chunk_count == record.chunk_count,
            "Index 文件 {} 的 Chunk 数量与 Manifest 分页不一致",
            record.path
        );
        Ok(())
    }

    pub(crate) fn read_tree(&self, id: &str) -> Result<Tree> {
        validate_hash(id, "Tree")?;
        let root = self
            .metadata_store
            .open_directory(id)?
            .with_context(|| format!("根 Directory 不存在: {id}"))?;
        let expected_file_count = root.file_count();
        drop(root);
        let mut files = Vec::new();
        let mut visiting = HashSet::new();
        self.collect_directory(id, "", &mut visiting, &mut files)?;
        let tree = Tree { files };
        ensure!(
            u64::try_from(tree.files.len()).context("Tree 文件数量超出 u64")?
                == expected_file_count,
            "Tree 文件数量与存储记录不一致: {id}"
        );
        validate_files(&tree.files)?;
        ensure!(self.tree_id(&tree)? == id, "Tree 内容与其 ID 不匹配: {id}");
        Ok(tree)
    }

    #[cfg(test)]
    pub(crate) fn list_tree_ids(&self) -> Result<Vec<String>> {
        let snapshot = self.metadata_store.snapshot()?;
        let ids = collect_pages(|request| snapshot.scan_directory_ids(request))?;
        validate_sorted_ids(&ids, "Directory")?;
        Ok(ids)
    }

    pub(crate) fn list_manifest_ids(&self) -> Result<Vec<String>> {
        let snapshot = self.metadata_store.snapshot()?;
        let ids = collect_pages(|request| snapshot.scan_manifest_ids(request))?;
        validate_sorted_ids(&ids, "Manifest")?;
        Ok(ids)
    }

    pub(crate) fn visit_index_files(
        &self,
        mut visitor: impl FnMut(&FileRecord) -> Result<()>,
    ) -> Result<u64> {
        let reader = self.metadata_store.read_index()?;
        let mut request = PageRequest::first(STORAGE_SCAN_PAGE_SIZE)?;
        let mut count = 0_u64;
        loop {
            let page = reader.scan_files(None, &request)?;
            ensure!(
                page.items.len() <= request.limit_usize()?,
                "元数据后端返回的 Index 分页超过请求上限"
            );
            for record in &page.items {
                visitor(record)?;
                count = count.checked_add(1).context("Index 文件数量溢出")?;
            }
            let Some(next) = page.next else {
                break;
            };
            ensure!(
                request.after.as_deref() != Some(next.as_str()),
                "元数据后端返回了不前进的 Index 分页游标"
            );
            request.after = Some(next);
        }
        Ok(count)
    }

    pub(crate) fn visit_all_workspace_index_files(
        &self,
        mut visitor: impl FnMut(&FileRecord) -> Result<()>,
    ) -> Result<u64> {
        let mut total = 0_u64;
        for workspace in self.list_workspaces()? {
            let repository = self.open_workspace(workspace.id)?;
            total = total
                .checked_add(repository.visit_index_files(&mut visitor)?)
                .context("全部 Workspace 的 Index 文件数量溢出")?;
        }
        Ok(total)
    }

    pub(crate) fn visit_manifest_chunks(
        &self,
        manifest_id: &str,
        expected_size: u64,
        mut visitor: impl FnMut(&neoengram_core::Chunk) -> Result<()>,
    ) -> Result<u64> {
        let reader = self.open_manifest(manifest_id)?;
        self.validate_chunking_strategy(reader.chunking())
            .with_context(|| format!("Manifest 分块策略不符合仓库配置: {manifest_id}"))?;
        ensure!(
            reader.total_size() == expected_size,
            "Manifest {manifest_id} 的逻辑大小与引用不一致"
        );
        let expected_count = reader.chunk_count();
        let mut request = PageRequest::first(STORAGE_SCAN_PAGE_SIZE)?;
        let mut count = 0_u64;
        loop {
            let page = reader.scan_chunks(&request)?;
            ensure!(
                page.items.len() <= request.limit_usize()?,
                "元数据后端返回的 Manifest 分页超过请求上限"
            );
            for chunk in &page.items {
                visitor(chunk)?;
                count = count.checked_add(1).context("Manifest Chunk 数量溢出")?;
            }
            let Some(next) = page.next else {
                break;
            };
            ensure!(
                request.after.as_deref() != Some(next.as_str()),
                "元数据后端返回了不前进的 Manifest 分页游标"
            );
            request.after = Some(next);
        }
        ensure!(
            count == expected_count,
            "Manifest {manifest_id} 的 Chunk 数量与 header 不一致"
        );
        Ok(count)
    }

    /// Walks unique Directory objects depth-first while retaining only one page per active depth.
    pub(crate) fn visit_directory_dag(
        &self,
        roots: &[String],
        mut visitor: impl FnMut(&DirectoryEntry) -> Result<()>,
    ) -> Result<usize> {
        let mut visited = HashSet::new();
        let mut active = HashSet::new();
        for root in roots {
            if !visited.insert(root.clone()) {
                continue;
            }
            active.insert(root.clone());
            let mut stack = vec![DirectoryWalkFrame::open(self, root.clone())?];
            while let Some(frame) = stack.last_mut() {
                if let Some(entry) = frame.entries.pop_front() {
                    frame.seen = frame
                        .seen
                        .checked_add(1)
                        .context("Directory 子项数量溢出")?;
                    visitor(&entry)?;
                    if entry.kind == DirectoryEntryKind::Directory {
                        ensure!(
                            !active.contains(&entry.target_id),
                            "Directory DAG 包含环: {}",
                            entry.target_id
                        );
                        if visited.insert(entry.target_id.clone()) {
                            active.insert(entry.target_id.clone());
                            stack.push(DirectoryWalkFrame::open(self, entry.target_id)?);
                        }
                    }
                    continue;
                }
                if let Some(next) = frame.next.take() {
                    frame.load_page(self, Some(next))?;
                    continue;
                }
                ensure!(
                    frame.seen == frame.expected_entries,
                    "Directory {} 的子项数量与 header 不一致",
                    frame.id
                );
                let finished = stack.pop().expect("Directory walker 栈非空");
                active.remove(&finished.id);
            }
        }
        Ok(visited.len())
    }

    pub(crate) fn open_directory(
        &self,
        id: &str,
    ) -> Result<Box<dyn crate::local::metadata::DirectoryReader + '_>> {
        validate_hash(id, "Directory")?;
        self.metadata_store
            .open_directory(id)?
            .with_context(|| format!("Directory 不存在: {id}"))
    }

    pub(crate) fn open_manifest(
        &self,
        id: &str,
    ) -> Result<Box<dyn crate::local::metadata::ManifestReader + '_>> {
        validate_hash(id, "Manifest")?;
        self.metadata_store
            .open_manifest(id)?
            .with_context(|| format!("Manifest 不存在: {id}"))
    }

    fn collect_directory(
        &self,
        id: &str,
        prefix: &str,
        visiting: &mut HashSet<String>,
        files: &mut Vec<FileNode>,
    ) -> Result<()> {
        ensure!(visiting.insert(id.to_owned()), "Directory DAG 包含环: {id}");
        let reader = self.open_directory(id)?;
        let entries = collect_pages(|request| reader.scan_entries(request))?;
        ensure!(
            u64::try_from(entries.len()).context("Directory 子项数量超出 u64")?
                == reader.entry_count(),
            "Directory 子项数量与 header 不一致: {id}"
        );
        drop(reader);
        for entry in entries {
            let path = if prefix.is_empty() {
                entry.name.clone()
            } else {
                format!("{prefix}/{}", entry.name)
            };
            match entry.kind {
                DirectoryEntryKind::File => {
                    let manifest = self.open_manifest(&entry.target_id)?;
                    ensure!(
                        manifest.total_size() == entry.total_size,
                        "Directory 文件大小与 Manifest 不一致: {path}"
                    );
                    let chunk_count = manifest.chunk_count();
                    let chunking = manifest.chunking();
                    let chunks = collect_pages(|request| manifest.scan_chunks(request))?;
                    ensure!(
                        u64::try_from(chunks.len()).context("Chunk 数量超出 u64")? == chunk_count,
                        "Manifest Chunk 数量与 header 不一致: {}",
                        entry.target_id
                    );
                    files.push(FileNode {
                        path,
                        total_size: entry.total_size,
                        chunking,
                        chunks,
                    });
                }
                DirectoryEntryKind::Directory => {
                    self.collect_directory(&entry.target_id, &path, visiting, files)?;
                }
            }
        }
        visiting.remove(id);
        Ok(())
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

    pub(crate) fn current_head(&self) -> Result<RepositoryHead> {
        let state = self.metadata_store.read_head_state()?;
        self.resolve_head_state(state)
    }

    pub(crate) fn current_commit_id(&self) -> Result<Option<String>> {
        Ok(self.current_head()?.commit_id)
    }

    pub(crate) fn default_branch_head(&self) -> Result<RepositoryHead> {
        self.resolve_head_state(HeadState::Attached {
            reference: DEFAULT_HEAD_REFERENCE.to_owned(),
        })
    }

    pub(crate) fn resolve_target_id(&self, target: &str) -> Result<String> {
        let target = target.trim();
        ensure!(!target.is_empty(), "TARGET 不能为空");
        if target.eq_ignore_ascii_case("HEAD") {
            return self
                .current_head()?
                .commit_id()
                .map(str::to_owned)
                .context("当前 Workspace 还没有 Commit");
        }
        let reference = if target.starts_with("refs/") {
            Some(target.to_owned())
        } else if target.len() != 64 {
            Some(format!("refs/heads/{target}"))
        } else {
            None
        };
        if let Some(reference) = reference {
            return self
                .metadata_store
                .get_reference(&reference)?
                .with_context(|| format!("引用不存在或尚未提交: {reference}"));
        }
        validate_hash(target, "Commit")?;
        self.read_commit(target)?;
        Ok(target.to_owned())
    }

    pub(crate) fn advance_head(&self, expected: &RepositoryHead, id: &str) -> Result<()> {
        validate_hash(id, "Commit")?;
        if let Some(expected_id) = expected.commit_id() {
            validate_hash(expected_id, "期望父 Commit")?;
        }

        match expected.state() {
            HeadState::Attached { reference } => {
                let actual_head = self.metadata_store.read_head_state()?;
                ensure!(
                    &actual_head == expected.state(),
                    "HEAD 在提交期间发生变化（期望 {:?}，实际 {:?}），拒绝覆盖并发提交",
                    expected.state(),
                    actual_head
                );
                match self.metadata_store.compare_exchange_reference(
                    reference,
                    expected.commit_id(),
                    Some(id),
                )? {
                    ReferenceCas::Updated => Ok(()),
                    ReferenceCas::Mismatch { actual } => bail!(
                        "分支在提交期间发生变化（期望 {:?}，实际 {:?}），拒绝覆盖并发提交",
                        expected.commit_id(),
                        actual
                    ),
                }
            }
            HeadState::Detached { commit_id } => {
                ensure!(
                    expected.commit_id() == Some(commit_id.as_str()),
                    "Detached HEAD 状态与解析出的 Commit 不一致"
                );
                let new_head = HeadState::Detached {
                    commit_id: id.to_owned(),
                };
                match self
                    .metadata_store
                    .compare_exchange_head(expected.state(), &new_head)?
                {
                    HeadCas::Updated => Ok(()),
                    HeadCas::Mismatch { actual } => bail!(
                        "Detached HEAD 在提交期间发生变化（期望 {:?}，实际 {:?}），拒绝覆盖并发提交",
                        expected.state(),
                        actual
                    ),
                }
            }
        }
    }

    /// 幂等切换 HEAD。恢复事务可能在 durable HEAD 写入后、journal 阶段更新前崩溃，
    /// 因此实际值已经等于目标时也视为成功；任何第三种状态都拒绝覆盖。
    pub(crate) fn transition_head(&self, expected: &HeadState, target: &HeadState) -> Result<()> {
        match self
            .metadata_store
            .compare_exchange_head(expected, target)?
        {
            HeadCas::Updated => Ok(()),
            HeadCas::Mismatch { actual } if actual == *target => Ok(()),
            HeadCas::Mismatch { actual } => bail!(
                "HEAD 状态与 Checkout 事务不一致（期望 {:?}，实际 {:?}），拒绝覆盖",
                expected,
                actual
            ),
        }
    }

    pub(crate) fn resolve_head_state(&self, state: HeadState) -> Result<RepositoryHead> {
        let commit_id = match &state {
            HeadState::Attached { reference } => self.metadata_store.get_reference(reference)?,
            HeadState::Detached { commit_id } => Some(commit_id.clone()),
        };
        if let Some(commit_id) = &commit_id {
            validate_hash(commit_id, "HEAD Commit")?;
        }
        Ok(RepositoryHead { state, commit_id })
    }
}

fn read_commit<R: MetadataReader + ?Sized>(reader: &R, id: &str) -> Result<Commit> {
    validate_hash(id, "Commit")?;
    let commit = reader
        .get_commit(id)?
        .with_context(|| format!("Commit 不存在: {id}"))?;
    validate_commit(&commit)?;
    ensure!(
        commit_content_id(&commit)? == id,
        "Commit 内容与其 ID 不匹配: {id}"
    );
    Ok(commit)
}

struct DirectoryFrame<'a> {
    name_in_parent: Option<String>,
    next_ordinal: u64,
    writer: Box<dyn DirectoryWriter + 'a>,
}

struct DirectoryWalkFrame {
    id: String,
    entries: std::collections::VecDeque<DirectoryEntry>,
    next: Option<String>,
    seen: u64,
    expected_entries: u64,
}

impl DirectoryWalkFrame {
    fn open(repository: &Repository, id: String) -> Result<Self> {
        let expected_entries = repository.open_directory(&id)?.entry_count();
        let mut frame = Self {
            id,
            entries: std::collections::VecDeque::new(),
            next: None,
            seen: 0,
            expected_entries,
        };
        frame.load_page(repository, None)?;
        Ok(frame)
    }

    fn load_page(&mut self, repository: &Repository, after: Option<String>) -> Result<()> {
        let request = PageRequest::new(after.clone(), STORAGE_SCAN_PAGE_SIZE)?;
        let page = repository
            .open_directory(&self.id)?
            .scan_entries(&request)?;
        ensure!(
            page.items.len() <= request.limit_usize()?,
            "元数据后端返回的 Directory 分页超过请求上限"
        );
        if let Some(next) = &page.next {
            let next = next.parse::<u64>().context("Directory cursor 无效")?;
            if let Some(after) = after.as_deref() {
                let after = after.parse::<u64>().context("Directory cursor 无效")?;
                ensure!(next > after, "元数据后端返回了不前进的 Directory 分页游标");
            }
        }
        self.entries = page.items.into();
        self.next = page.next;
        Ok(())
    }
}

struct DirectoryDagBuilder<'a> {
    store: &'a dyn MetadataStore,
    frames: Vec<DirectoryFrame<'a>>,
    open_path: Vec<String>,
    previous_path: Option<String>,
}

impl<'a> DirectoryDagBuilder<'a> {
    fn new(store: &'a dyn MetadataStore) -> Result<Self> {
        Ok(Self {
            store,
            frames: vec![DirectoryFrame {
                name_in_parent: None,
                next_ordinal: 0,
                writer: store.begin_directory_write()?,
            }],
            open_path: Vec::new(),
            previous_path: None,
        })
    }

    fn push(&mut self, record: FileRecord) -> Result<()> {
        if let Some(previous) = &self.previous_path {
            ensure!(
                previous.as_str() < record.path.as_str(),
                "Index 文件路径必须严格升序且唯一: {}",
                record.path
            );
            ensure!(
                !record.path.starts_with(&format!("{previous}/")),
                "Tree 同时包含文件及其子路径: {}",
                record.path
            );
        }
        let components = record.path.split('/').collect::<Vec<_>>();
        let (file_name, directory_components) =
            components.split_last().context("Tree 路径不能为空")?;
        let common = self
            .open_path
            .iter()
            .zip(directory_components.iter())
            .take_while(|(left, right)| left.as_str() == **right)
            .count();
        while self.open_path.len() > common {
            self.close_child()?;
        }
        for component in &directory_components[common..] {
            self.frames.push(DirectoryFrame {
                name_in_parent: Some((*component).to_owned()),
                next_ordinal: 0,
                writer: self.store.begin_directory_write()?,
            });
            self.open_path.push((*component).to_owned());
        }
        let current = self.frames.last_mut().context("Directory 构造栈为空")?;
        let ordinal = current.next_ordinal;
        current.writer.append_entry(&DirectoryEntry {
            ordinal,
            name: (*file_name).to_owned(),
            kind: DirectoryEntryKind::File,
            target_id: record.manifest_id,
            total_size: record.total_size,
        })?;
        current.next_ordinal = ordinal.checked_add(1).context("Directory ordinal 溢出")?;
        self.previous_path = Some(record.path);
        Ok(())
    }

    fn close_child(&mut self) -> Result<()> {
        ensure!(self.frames.len() > 1, "不能提前关闭根 Directory");
        let frame = self.frames.pop().context("Directory 构造栈为空")?;
        let directory = frame.writer.finish()?;
        let name = frame.name_in_parent.context("子 Directory 缺少父级名称")?;
        self.open_path.pop().context("Directory 路径栈为空")?;
        let parent = self.frames.last_mut().context("Directory 父级不存在")?;
        let ordinal = parent.next_ordinal;
        parent.writer.append_entry(&DirectoryEntry {
            ordinal,
            name,
            kind: DirectoryEntryKind::Directory,
            target_id: directory.id,
            total_size: 0,
        })?;
        parent.next_ordinal = ordinal.checked_add(1).context("Directory ordinal 溢出")?;
        Ok(())
    }

    fn finish(mut self) -> Result<String> {
        while self.frames.len() > 1 {
            self.close_child()?;
        }
        self.frames
            .pop()
            .context("根 Directory 不存在")?
            .writer
            .finish()
            .map(|directory| directory.id)
    }
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
        let chunking = manifest.chunking();
        ensure!(
            u64::try_from(chunks.len()).context("Chunk 数量超出 u64")? == record.chunk_count,
            "文件 {} 的 Chunk 数量与存储记录不一致",
            record.path
        );
        files.push(FileNode {
            path: record.path,
            total_size: record.total_size,
            chunking,
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

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use neoengram_core::{ChunkingStrategy, FileNode, Index};

    use super::Repository;

    #[test]
    fn stale_versioned_index_write_is_rejected() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("sqlite");
        fs::create_dir(&root)?;
        let repository = Repository::open_or_create(root)?;
        repository.initialize_layout()?;

        let initial = repository.read_index_versioned()?;
        repository.write_index_if_version(initial.index(), initial.version())?;

        let stale_update = Index {
            format_version: initial.index().format_version,
            files: vec![FileNode {
                path: "stale.bin".to_owned(),
                total_size: 0,
                chunking: ChunkingStrategy::FastCdc,
                chunks: Vec::new(),
            }],
        };
        assert!(repository
            .write_index_if_version(&stale_update, initial.version())
            .is_err());
        assert_eq!(repository.read_index()?, *initial.index());
        Ok(())
    }

    #[test]
    fn identical_subdirectories_are_deduplicated() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("sqlite-dag");
        fs::create_dir(&root)?;
        let repository = Repository::open_or_create(root)?;
        repository.initialize_layout()?;
        let tree = neoengram_core::Tree {
            files: vec![
                FileNode {
                    path: "left/file".to_owned(),
                    total_size: 0,
                    chunking: ChunkingStrategy::FastCdc,
                    chunks: Vec::new(),
                },
                FileNode {
                    path: "right/file".to_owned(),
                    total_size: 0,
                    chunking: ChunkingStrategy::FastCdc,
                    chunks: Vec::new(),
                },
            ],
        };
        let root_id = repository.store_tree(&tree)?;
        let root = repository.open_directory(&root_id)?;
        let left = root.get_entry("left")?.expect("left directory");
        let right = root.get_entry("right")?.expect("right directory");
        assert_eq!(left.target_id, right.target_id);
        assert_eq!(repository.list_tree_ids()?.len(), 2);
        Ok(())
    }
}
