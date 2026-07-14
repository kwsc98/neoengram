use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{Commit, FileNode, Index, Tree};

use crate::local::metadata::{
    FileRecord, FileSetReader, HeadCas, HeadState, MetadataReader, Page, PageRequest, ReferenceCas,
    StoredReference,
};

use super::{
    validation::{
        typed_json_hash, validate_commit, validate_files, validate_hash, validate_index,
        validate_sorted_ids, COMMIT_HASH_DOMAIN,
    },
    Repository, DEFAULT_HEAD_REFERENCE,
};

const STORAGE_SCAN_PAGE_SIZE: u32 = 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryHead {
    state: HeadState,
    commit_id: Option<String>,
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
