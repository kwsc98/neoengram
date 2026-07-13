use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{Commit, FileNode, Index, Tree};

use crate::local::metadata::{
    FileRecord, FileSetReader, MetadataReader, Page, PageRequest, ReferenceCas, StoredReference,
};

use super::{
    validation::{
        typed_json_hash, validate_commit, validate_files, validate_hash, validate_index,
        validate_sorted_ids, COMMIT_HASH_DOMAIN,
    },
    Repository, DEFAULT_HEAD_REFERENCE,
};

const STORAGE_SCAN_PAGE_SIZE: u32 = 1_024;

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
