use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{Chunk, Commit, FileNode};
use tempfile::TempDir;

use crate::local::{objects::ObjectSpec, repository::Repository};

pub(crate) async fn execute() -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let report = tokio::task::spawn_blocking(move || check_repository(&repository))
        .await
        .context("Fsck 任务异常终止")??;

    println!(
        "Fsck OK: {} refs, {} commits, {} trees, {} objects",
        report.refs, report.commits, report.trees, report.objects
    );
    Ok(())
}

struct FsckReport {
    refs: usize,
    commits: usize,
    trees: usize,
    objects: usize,
}

fn check_repository(repository: &Repository) -> Result<FsckReport> {
    // fsck 会扫描多个相互引用的文件。持有仓库写锁可阻止并发 add/commit/checkout
    // 在扫描中途发布新状态，从而让本次检查对应一个一致的本地元数据快照。
    let _lock = repository.acquire_write_lock()?;
    repository.verify_metadata_store_integrity()?;
    let index = repository.read_index()?;
    let commits = read_all_commits(repository)?;
    let tree_ids = repository.list_tree_ids()?;
    let tree_set: HashSet<&str> = tree_ids.iter().map(String::as_str).collect();
    let refs = read_all_refs(repository, &commits)?;
    let head = repository.current_head()?;
    if let Some(commit_id) = head.commit_id() {
        ensure!(
            commits.contains_key(commit_id),
            "HEAD 指向不存在的 Commit: {commit_id}"
        );
    }

    validate_commit_graph(&commits, &tree_set)?;

    // Chunk 引用可能达到数百万条。把每个文件的 Chunk ID 追加到有界的外部排序器，
    // 避免 fsck 为了最后一次对象校验长期保留完整 HashMap。Tree 也只保留当前正在
    // 校验的一棵，读完后立即释放其文件列表。
    let mut marks = ChunkMarkBuilder::new()?;
    collect_file_chunks(&index.files, &mut marks)?;
    drop(index);
    for tree_id in &tree_ids {
        let tree = repository.read_tree(tree_id)?;
        collect_file_chunks(&tree.files, &mut marks)?;
    }

    let mut referenced = marks.finish()?.into_cursor()?;
    let objects = verify_all_objects(repository, &mut referenced)?;
    if let Some(hash) = referenced.remaining_id() {
        anyhow::bail!("元数据引用了不存在的 Chunk 对象: {hash}");
    }

    Ok(FsckReport {
        refs,
        commits: commits.len(),
        trees: tree_ids.len(),
        objects,
    })
}

/// The graph validator only needs these two links. Keeping commit messages and timestamps out of
/// the map materially lowers fsck's peak memory for repositories with long histories.
#[derive(Debug, Clone)]
struct CommitLink {
    tree_hash: String,
    parent: Option<String>,
}

fn read_all_commits(repository: &Repository) -> Result<BTreeMap<String, CommitLink>> {
    let mut commits = BTreeMap::new();
    for id in repository.list_commit_ids()? {
        let commit = repository.read_commit(&id)?;
        let Commit {
            tree_hash,
            parent,
            message: _,
            created_at_unix_ms: _,
        } = commit;
        ensure!(
            commits
                .insert(id, CommitLink { tree_hash, parent })
                .is_none(),
            "Commit ID 重复"
        );
    }
    Ok(commits)
}

fn read_all_refs(repository: &Repository, commits: &BTreeMap<String, CommitLink>) -> Result<usize> {
    let references = repository.list_references("refs")?;
    for reference in &references {
        let id = reference.target.trim();
        validate_hash(id, "分支引用")?;
        ensure!(
            commits.contains_key(id),
            "分支引用指向不存在的 Commit: {} -> {id}",
            reference.name
        );
    }
    Ok(references.len())
}

fn validate_commit_graph(
    commits: &BTreeMap<String, CommitLink>,
    trees: &HashSet<&str>,
) -> Result<()> {
    for (id, commit) in commits {
        ensure!(
            trees.contains(commit.tree_hash.as_str()),
            "Commit {id} 引用了不存在的 Tree: {}",
            commit.tree_hash
        );
        if let Some(parent) = &commit.parent {
            ensure!(
                commits.contains_key(parent),
                "Commit {id} 引用了不存在的父 Commit: {parent}"
            );
        }
    }

    // Commit 只有一个父节点，因此逐链检查即可。resolved 中的节点已确认最终到达根，
    // current_chain 用来发现当前链回到自身的情况。
    let mut resolved = HashSet::new();
    for start in commits.keys() {
        if resolved.contains(start) {
            continue;
        }
        let mut current = Some(start.as_str());
        let mut current_chain = HashSet::new();
        while let Some(id) = current {
            if resolved.contains(id) {
                break;
            }
            ensure!(
                current_chain.insert(id.to_owned()),
                "Commit 历史包含环，重复节点: {id}"
            );
            current = commits
                .get(id)
                .with_context(|| format!("Commit 图引用不存在的节点: {id}"))?
                .parent
                .as_deref();
        }
        resolved.extend(current_chain);
    }
    Ok(())
}

fn collect_file_chunks(files: &[FileNode], chunks: &mut ChunkMarkBuilder) -> Result<()> {
    for file in files {
        for chunk in &file.chunks {
            chunks.push(chunk)?;
        }
    }
    Ok(())
}

fn verify_all_objects(repository: &Repository, referenced: &mut ChunkMarkCursor) -> Result<usize> {
    const PAGE_SIZE: usize = 1_024;

    let store = repository.object_store();
    let limit = NonZeroUsize::new(PAGE_SIZE).expect("对象页大小必须大于零");
    let mut cursor = None;
    let mut count = 0_usize;
    let mut previous_id: Option<String> = None;
    loop {
        let page = store.list_page(cursor.as_deref(), limit)?;
        ensure!(page.objects.len() <= PAGE_SIZE, "对象后端返回了超大分页");
        for object in page.objects {
            if let Some(previous) = &previous_id {
                ensure!(
                    previous.as_str() < object.id.as_str(),
                    "对象后端返回了未排序或重复的对象 ID: {}",
                    object.id
                );
            }
            previous_id = Some(object.id.clone());
            let spec = ObjectSpec::new(object.id.clone(), object.size)?;
            store.verify(&spec)?;
            if let Some(expected_size) = referenced.match_object(&object.id)? {
                ensure!(
                    expected_size == object.size,
                    "Chunk {} 的对象大小与元数据不一致: {} 与 {}",
                    object.id,
                    object.size,
                    expected_size
                );
            }
            count = count.checked_add(1).context("对象数量超出 usize")?;
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        ensure!(
            cursor.as_deref() != Some(next.as_str()),
            "对象后端返回了不前进的分页游标"
        );
        cursor = Some(next);
    }
    Ok(count)
}

// Keep the in-memory mark run deliberately small. The resulting temporary files are private to
// one fsck invocation and are removed with the owning TempDir, so a crash cannot damage metadata.
const MARK_RUN_SIZE: usize = 4_096;
const MARK_MERGE_FAN_IN: usize = 64;

struct ChunkMarkBuilder {
    temporary: TempDir,
    pending: Vec<(String, u64)>,
    runs: Vec<PathBuf>,
}

impl ChunkMarkBuilder {
    fn new() -> Result<Self> {
        Ok(Self {
            temporary: tempfile::tempdir().context("无法创建 fsck Chunk 标记临时目录")?,
            pending: Vec::with_capacity(MARK_RUN_SIZE),
            runs: Vec::new(),
        })
    }

    fn push(&mut self, chunk: &Chunk) -> Result<()> {
        validate_hash(&chunk.hash, "Chunk")?;
        self.pending.push((chunk.hash.clone(), chunk.size));
        if self.pending.len() >= MARK_RUN_SIZE {
            self.flush_run()?;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.pending
            .sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut unique = Vec::with_capacity(self.pending.len());
        for (id, size) in self.pending.drain(..) {
            if let Some((previous_id, previous_size)) = unique.last() {
                if previous_id == &id {
                    ensure!(
                        *previous_size == size,
                        "Chunk {} 在元数据中具有冲突的大小: {} 与 {}",
                        id,
                        previous_size,
                        size
                    );
                    continue;
                }
            }
            unique.push((id, size));
        }

        let path = self
            .temporary
            .path()
            .join(format!("run-{:08}.marks", self.runs.len()));
        let file = File::create(&path)
            .with_context(|| format!("无法创建 fsck Chunk 标记分片: {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        for (id, size) in unique {
            write_mark_record(&mut writer, &id, size)?;
        }
        writer.flush().context("无法刷新 fsck Chunk 标记分片")?;
        self.runs.push(path);
        Ok(())
    }

    fn finish(mut self) -> Result<ChunkMarkSet> {
        self.flush_run()?;
        let mut runs = self.runs;
        let mut round = 0_usize;
        while runs.len() > 1 {
            let mut merged = Vec::new();
            for (batch_index, batch) in runs.chunks(MARK_MERGE_FAN_IN).enumerate() {
                let path = self
                    .temporary
                    .path()
                    .join(format!("merge-{round:04}-{batch_index:08}.marks"));
                merge_mark_runs(batch, &path)?;
                for old in batch {
                    fs::remove_file(old).with_context(|| {
                        format!("无法删除 fsck Chunk 标记分片: {}", old.display())
                    })?;
                }
                merged.push(path);
            }
            runs = merged;
            round = round
                .checked_add(1)
                .context("fsck Chunk 标记归并轮次溢出")?;
        }

        let path = runs
            .pop()
            .unwrap_or_else(|| self.temporary.path().join("marks-00000000.marks"));
        if !path.exists() {
            File::create(&path)
                .with_context(|| format!("无法创建空的 fsck Chunk 标记: {}", path.display()))?;
        }
        Ok(ChunkMarkSet {
            temporary: self.temporary,
            path,
        })
    }
}

struct ChunkMarkSet {
    temporary: TempDir,
    path: PathBuf,
}

impl ChunkMarkSet {
    fn into_cursor(self) -> Result<ChunkMarkCursor> {
        let path = self.path.clone();
        let file = File::open(&path)
            .with_context(|| format!("无法打开 fsck Chunk 标记: {}", path.display()))?;
        let mut cursor = ChunkMarkCursor {
            // Keep the temporary directory alive for the entire object sweep.
            _temporary: self.temporary,
            reader: BufReader::new(file),
            current: None,
        };
        cursor.read_next()?;
        Ok(cursor)
    }
}

struct ChunkMarkCursor {
    // Keep the reader before TempDir so Windows closes the file before removing the directory.
    reader: BufReader<File>,
    current: Option<(String, u64)>,
    _temporary: TempDir,
}

impl ChunkMarkCursor {
    fn read_next(&mut self) -> Result<()> {
        self.current = read_mark_record(&mut self.reader)?;
        Ok(())
    }

    /// Returns and consumes the expected size when `id` is referenced. Because both streams are
    /// sorted, a mark smaller than the current object proves that its payload is missing.
    fn match_object(&mut self, id: &str) -> Result<Option<u64>> {
        let Some((current_id, current_size)) = self.current.clone() else {
            return Ok(None);
        };
        match current_id.as_str().cmp(id) {
            Ordering::Less => bail!("元数据引用了不存在的 Chunk 对象: {current_id}"),
            Ordering::Equal => {
                self.read_next()?;
                Ok(Some(current_size))
            }
            Ordering::Greater => Ok(None),
        }
    }

    fn remaining_id(&self) -> Option<&str> {
        self.current.as_ref().map(|(id, _)| id.as_str())
    }
}

fn write_mark_record(writer: &mut BufWriter<File>, id: &str, size: u64) -> Result<()> {
    writeln!(writer, "{id}\t{size}").context("无法写入 fsck Chunk 标记")?;
    Ok(())
}

fn read_mark_record(reader: &mut BufReader<File>) -> Result<Option<(String, u64)>> {
    let mut line = String::new();
    if reader
        .read_line(&mut line)
        .context("无法读取 fsck Chunk 标记")?
        == 0
    {
        return Ok(None);
    }
    let line = line.strip_suffix('\n').context("fsck Chunk 标记缺少换行")?;
    let (id, size) = line
        .split_once('\t')
        .with_context(|| format!("fsck Chunk 标记格式无效: {line}"))?;
    validate_hash(id, "Chunk 标记")?;
    let size = size
        .parse::<u64>()
        .with_context(|| format!("fsck Chunk 标记大小无效: {line}"))?;
    Ok(Some((id.to_owned(), size)))
}

fn merge_mark_runs(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let mut readers = Vec::with_capacity(inputs.len());
    let mut heap = std::collections::BinaryHeap::new();
    for (index, path) in inputs.iter().enumerate() {
        let file = File::open(path)
            .with_context(|| format!("无法打开 fsck Chunk 标记分片: {}", path.display()))?;
        let mut reader = BufReader::new(file);
        if let Some((id, size)) = read_mark_record(&mut reader)? {
            heap.push(std::cmp::Reverse((id, size, index)));
        }
        readers.push(reader);
    }

    let file = File::create(output)
        .with_context(|| format!("无法创建 fsck Chunk 归并结果: {}", output.display()))?;
    let mut writer = BufWriter::new(file);
    let mut last: Option<(String, u64)> = None;
    while let Some(std::cmp::Reverse((id, size, index))) = heap.pop() {
        if let Some((last_id, last_size)) = &last {
            if last_id == &id {
                ensure!(
                    *last_size == size,
                    "Chunk {} 在元数据中具有冲突的大小: {} 与 {}",
                    id,
                    last_size,
                    size
                );
            } else {
                write_mark_record(&mut writer, last_id, *last_size)?;
                last = Some((id, size));
            }
        } else {
            last = Some((id, size));
        }

        if let Some((next_id, next_size)) = read_mark_record(&mut readers[index])? {
            heap.push(std::cmp::Reverse((next_id, next_size, index)));
        }
    }
    if let Some((last_id, last_size)) = last {
        write_mark_record(&mut writer, &last_id, last_size)?;
    }
    writer.flush().context("无法刷新 fsck Chunk 归并结果")?;
    Ok(())
}

fn validate_hash(hash: &str, kind: &str) -> Result<()> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{kind} 不是有效的小写 BLAKE3 Hash: {hash}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use neoengram_core::Chunk;

    use super::{validate_hash, ChunkMarkBuilder, MARK_RUN_SIZE};

    #[test]
    fn hash_validation_rejects_uppercase_and_wrong_length() {
        assert!(validate_hash(&"a".repeat(64), "test").is_ok());
        assert!(validate_hash(&"A".repeat(64), "test").is_err());
        assert!(validate_hash("abc", "test").is_err());
    }

    #[test]
    fn chunk_marks_are_sorted_and_deduplicated_on_disk() -> Result<()> {
        let first = "b".repeat(64);
        let second = "a".repeat(64);
        let mut marks = ChunkMarkBuilder::new()?;
        marks.push(&Chunk {
            hash: first.clone(),
            offset: 0,
            size: 2,
        })?;
        marks.push(&Chunk {
            hash: second.clone(),
            offset: 0,
            size: 1,
        })?;
        marks.push(&Chunk {
            hash: first.clone(),
            offset: 2,
            size: 2,
        })?;

        let mut cursor = marks.finish()?.into_cursor()?;
        assert_eq!(cursor.match_object(&second)?, Some(1));
        assert_eq!(cursor.match_object(&first)?, Some(2));
        assert_eq!(cursor.remaining_id(), None);
        Ok(())
    }

    #[test]
    fn chunk_mark_size_conflicts_are_rejected() -> Result<()> {
        let id = "c".repeat(64);
        let mut marks = ChunkMarkBuilder::new()?;
        marks.push(&Chunk {
            hash: id.clone(),
            offset: 0,
            size: 1,
        })?;
        marks.push(&Chunk {
            hash: id,
            offset: 0,
            size: 2,
        })?;
        assert!(marks.finish().is_err());
        Ok(())
    }

    #[test]
    fn chunk_mark_runs_merge_without_materializing_all_records() -> Result<()> {
        let mut marks = ChunkMarkBuilder::new()?;
        for ordinal in 0..=MARK_RUN_SIZE {
            marks.push(&Chunk {
                hash: format!("{ordinal:064x}"),
                offset: 0,
                size: 1,
            })?;
        }
        let mut cursor = marks.finish()?.into_cursor()?;
        for ordinal in 0..=MARK_RUN_SIZE {
            assert_eq!(cursor.match_object(&format!("{ordinal:064x}"))?, Some(1));
        }
        assert_eq!(cursor.remaining_id(), None);
        Ok(())
    }
}
