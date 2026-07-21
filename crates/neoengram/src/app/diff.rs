//! File-level comparisons for the mutable worktree/index and immutable history.
//!
//! `diff` deliberately does not produce a line-oriented patch.  NeoEngram stores files as
//! content-addressed CDC chunks, so the useful unit for a large model or dataset is a path,
//! its byte/chunk counts, and the amount of chunk content that can be reused.  The worktree
//! scanner uses the streaming FastCDC implementation and keeps the payload bounded to one CDC
//! buffer (the small per-file recipe is retained for comparison); it never publishes objects as a
//! side effect of a read-only diff.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::{Component, Path},
};

use anyhow::{bail, ensure, Context, Result};
use fastcdc::v2020::StreamCDC;
use neoengram_core::{Chunk, FileNode, Index};
use walkdir::WalkDir;

use crate::local::{
    metadata::{HeadState, IndexVersion},
    repository::{is_managed_container_dir_name, is_neoengram_dir_name, Repository},
    worktree::{lenient_leaf_metadata, workspace_path},
};

// Keep these values in lock-step with local::worktree::import.  StreamCDC uses the same
// FastCDC v2020 algorithm as the mmap based importer, while bounding payload memory to roughly
// the largest chunk rather than the complete file (the per-file recipe remains in memory).
const MIN_CHUNK_SIZE: u32 = 256 * 1024;
const AVG_CHUNK_SIZE: u32 = 1024 * 1024;
const MAX_CHUNK_SIZE: u32 = 4 * 1024 * 1024;

/// Run a diff and print a file-level report.
///
/// `staged == false` compares the worktree with the index by default.  `staged == true`
/// compares the index with `HEAD`.  One target is accepted as a convenient shorthand for
/// comparing that Commit with the worktree (or the index when `staged` is set); two targets
/// compare two immutable Commits.  The CLI owns argument parsing and can pass the target list
/// unchanged here.
pub(crate) async fn execute(staged: bool, targets: Vec<String>, stat_only: bool) -> Result<()> {
    ensure!(targets.len() <= 2, "diff 最多接受两个 TARGET");
    if staged && targets.len() == 2 {
        bail!("`--staged` 不能与两个 Commit TARGET 同时使用");
    }

    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let _worktree_lock = repository.acquire_worktree_shared_lock()?;
    let task_repository = repository.clone();
    let report =
        tokio::task::spawn_blocking(move || build_report(&task_repository, staged, &targets))
            .await
            .context("Diff 任务异常终止")??;

    print_report(&report, stat_only);
    Ok(())
}

/// A compact file statistic used in output and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileStats {
    pub(crate) bytes: u64,
    pub(crate) chunks: u64,
}

impl FileStats {
    fn from_node(node: &FileNode) -> Self {
        Self {
            bytes: node.total_size,
            chunks: u64::try_from(node.chunks.len()).unwrap_or(u64::MAX),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeKind {
    Added,
    Modified,
    Deleted,
}

impl ChangeKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
        }
    }
}

/// Chunk-level accounting for one changed path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ChunkDelta {
    /// Number of chunks on the new side whose `(hash, size)` recipe is reusable from the old side.
    pub(crate) reused: u64,
    /// Number of new-side chunks that are not reusable from the old side.
    pub(crate) added: u64,
    /// Bytes represented by `added` chunks.  This is an upper bound for new object storage;
    /// objects already present elsewhere in the repository may make the physical increase zero.
    pub(crate) added_bytes: u64,
    /// Number of old-side chunks no longer referenced by this path.
    pub(crate) removed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiffEntry {
    pub(crate) path: String,
    pub(crate) kind: ChangeKind,
    pub(crate) before: Option<FileStats>,
    pub(crate) after: Option<FileStats>,
    pub(crate) chunks: ChunkDelta,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DiffSummary {
    pub(crate) added_files: u64,
    pub(crate) modified_files: u64,
    pub(crate) deleted_files: u64,
    pub(crate) added_bytes: u64,
    pub(crate) removed_bytes: u64,
    pub(crate) reused_chunks: u64,
    pub(crate) added_chunks: u64,
    pub(crate) removed_chunks: u64,
    pub(crate) estimated_added_bytes: u64,
}

impl DiffSummary {
    fn add_entry(&mut self, entry: &DiffEntry) -> Result<()> {
        match entry.kind {
            ChangeKind::Added => {
                self.added_files = self
                    .added_files
                    .checked_add(1)
                    .context("diff 新增文件数量溢出")?
            }
            ChangeKind::Modified => {
                self.modified_files = self
                    .modified_files
                    .checked_add(1)
                    .context("diff 修改文件数量溢出")?;
            }
            ChangeKind::Deleted => {
                self.deleted_files = self
                    .deleted_files
                    .checked_add(1)
                    .context("diff 删除文件数量溢出")?;
            }
        }
        if let Some(before) = entry.before {
            self.removed_bytes = self
                .removed_bytes
                .checked_add(before.bytes)
                .context("diff 删除字节数溢出")?;
        }
        if let Some(after) = entry.after {
            self.added_bytes = self
                .added_bytes
                .checked_add(after.bytes)
                .context("diff 新增字节数溢出")?;
        }
        self.reused_chunks = self
            .reused_chunks
            .checked_add(entry.chunks.reused)
            .context("diff 复用 Chunk 数量溢出")?;
        self.added_chunks = self
            .added_chunks
            .checked_add(entry.chunks.added)
            .context("diff 新增 Chunk 数量溢出")?;
        self.removed_chunks = self
            .removed_chunks
            .checked_add(entry.chunks.removed)
            .context("diff 删除 Chunk 数量溢出")?;
        self.estimated_added_bytes = self
            .estimated_added_bytes
            .checked_add(entry.chunks.added_bytes)
            .context("diff 预计新增存储量溢出")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiffReport {
    pub(crate) left_label: String,
    pub(crate) right_label: String,
    pub(crate) entries: Vec<DiffEntry>,
    pub(crate) summary: DiffSummary,
}

/// Build a report for the requested comparison.  Kept separate from printing so callers and
/// tests can consume deterministic, sorted entries without parsing human output.
pub(crate) fn build_report(
    repository: &Repository,
    staged: bool,
    targets: &[String],
) -> Result<DiffReport> {
    ensure!(targets.len() <= 2, "diff 最多接受两个 TARGET");
    if staged && targets.len() == 2 {
        bail!("`--staged` 不能与两个 Commit TARGET 同时使用");
    }
    repository.ensure_no_unfinished_transactions()?;

    let mut observed_index_version = None;
    let mut observed_targets = Vec::new();

    let (left_label, right_label, left, right) = match (staged, targets) {
        (false, []) => {
            let index = read_observed_index(repository, &mut observed_index_version)?;
            let tracked_paths = snapshot_paths(&index.files);
            let worktree = read_worktree_snapshot(repository, Some(&tracked_paths))?;
            (
                "index".to_owned(),
                "worktree".to_owned(),
                index.files,
                worktree,
            )
        }
        (true, []) => {
            let index = read_observed_index(repository, &mut observed_index_version)?;
            let (head, observation) = read_head_snapshot(repository)?;
            observed_targets.push(observation);
            ("HEAD".to_owned(), "index".to_owned(), head, index.files)
        }
        (false, [target]) => {
            let (label, commit_files, observation) = read_commit_snapshot(repository, target)?;
            observed_targets.extend(observation);
            // `diff <COMMIT>` includes files staged since that Commit as well as files present in
            // the Commit, so use the union of the target and current index paths as the worktree
            // comparison scope.  Untracked paths still remain outside this Git-like diff.
            let index = read_observed_index(repository, &mut observed_index_version)?;
            let mut tracked_paths = snapshot_paths(&commit_files);
            tracked_paths.extend(index.files.iter().map(|file| file.path.clone()));
            let worktree = read_worktree_snapshot(repository, Some(&tracked_paths))?;
            (label, "worktree".to_owned(), commit_files, worktree)
        }
        (true, [target]) => {
            let (label, commit_files, observation) = read_commit_snapshot(repository, target)?;
            observed_targets.extend(observation);
            let index = read_observed_index(repository, &mut observed_index_version)?;
            (label, "index".to_owned(), commit_files, index.files)
        }
        (false, [left_target, right_target]) => {
            let (left_label, left, left_observation) =
                read_commit_snapshot(repository, left_target)?;
            observed_targets.extend(left_observation);
            let (right_label, right, right_observation) =
                read_commit_snapshot(repository, right_target)?;
            observed_targets.extend(right_observation);
            (left_label, right_label, left, right)
        }
        (false, _) => bail!("diff 最多接受两个 Commit TARGET"),
        (true, _) => unreachable!("target count was checked above"),
    };

    let entries = compare_snapshots(&left, &right);
    let mut summary = DiffSummary::default();
    for entry in &entries {
        summary.add_entry(entry)?;
    }
    repository.ensure_no_unfinished_transactions()?;
    if let Some(expected) = observed_index_version {
        ensure!(
            repository.current_index_version()? == expected,
            "Index 在 diff 扫描期间发生变化，请重试"
        );
    }
    for observation in &observed_targets {
        observation.ensure_unchanged(repository)?;
    }
    Ok(DiffReport {
        left_label,
        right_label,
        entries,
        summary,
    })
}

fn read_observed_index(
    repository: &Repository,
    observed: &mut Option<IndexVersion>,
) -> Result<Index> {
    let versioned = repository.read_index_versioned()?;
    *observed = Some(versioned.version().clone());
    Ok(versioned.into_parts().0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedHead {
    state: HeadState,
    commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TargetObservation {
    Head(ObservedHead),
    Main(ObservedHead),
}

impl TargetObservation {
    fn ensure_unchanged(&self, repository: &Repository) -> Result<()> {
        let (expected, label, actual) = match self {
            Self::Head(expected) => (expected, "HEAD", repository.current_head()?),
            Self::Main(expected) => (expected, "main", repository.default_branch_head()?),
        };
        ensure!(
            actual.state() == &expected.state
                && actual.commit_id() == expected.commit_id.as_deref(),
            "{label} 在 diff 扫描期间发生变化，请重试"
        );
        Ok(())
    }
}

fn observe_head(repository: &Repository, main: bool) -> Result<ObservedHead> {
    let head = if main {
        repository.default_branch_head()?
    } else {
        repository.current_head()?
    };
    Ok(ObservedHead {
        state: head.state().clone(),
        commit_id: head.commit_id().map(str::to_owned),
    })
}

fn read_head_snapshot(repository: &Repository) -> Result<(Vec<FileNode>, TargetObservation)> {
    let observed = observe_head(repository, false)?;
    let files = match observed.commit_id.as_deref() {
        Some(commit_id) => {
            let commit = repository.read_commit(commit_id)?;
            repository.read_tree(&commit.tree_hash)?.files
        }
        None => Vec::new(),
    };
    Ok((files, TargetObservation::Head(observed)))
}

fn read_commit_snapshot(
    repository: &Repository,
    target: &str,
) -> Result<(String, Vec<FileNode>, Option<TargetObservation>)> {
    let target = target.trim();
    ensure!(!target.is_empty(), "diff TARGET 不能为空");
    let label = target.to_owned();
    let (id, observation) = if target.eq_ignore_ascii_case("HEAD") {
        let observed = observe_head(repository, false)?;
        let id = observed
            .commit_id
            .clone()
            .context("仓库还没有 Commit；请先运行 `neoengram commit`")?;
        (id, Some(TargetObservation::Head(observed)))
    } else if target == "main" {
        let observed = observe_head(repository, true)?;
        let id = observed
            .commit_id
            .as_deref()
            .context("main 分支还没有 Commit；请先运行 `neoengram commit`")?
            .to_owned();
        (id, Some(TargetObservation::Main(observed)))
    } else {
        (target.to_owned(), None)
    };
    let commit = repository.read_commit(&id)?;
    let files = repository.read_tree(&commit.tree_hash)?.files;
    Ok((label, files, observation))
}

/// Compare two complete, sorted file snapshots.  This is used for index/HEAD and Commit/Commit
/// comparisons, and also by the worktree path scanner after it has built current FileNodes.
pub(crate) fn compare_snapshots(base: &[FileNode], current: &[FileNode]) -> Vec<DiffEntry> {
    let base: BTreeMap<&str, &FileNode> =
        base.iter().map(|file| (file.path.as_str(), file)).collect();
    let current: BTreeMap<&str, &FileNode> = current
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    let paths: BTreeSet<&str> = base.keys().chain(current.keys()).copied().collect();

    paths
        .into_iter()
        .filter_map(|path| {
            let before = base.get(path).copied();
            let after = current.get(path).copied();
            let kind = match (before, after) {
                (None, Some(_)) => ChangeKind::Added,
                (Some(_), None) => ChangeKind::Deleted,
                (Some(before), Some(after)) if before != after => ChangeKind::Modified,
                (Some(_), Some(_)) | (None, None) => return None,
            };
            Some(DiffEntry {
                path: path.to_owned(),
                kind,
                before: before.map(FileStats::from_node),
                after: after.map(FileStats::from_node),
                chunks: chunk_delta(before, after),
            })
        })
        .collect()
}

fn chunk_delta(before: Option<&FileNode>, after: Option<&FileNode>) -> ChunkDelta {
    let mut available: BTreeMap<(String, u64), u64> = BTreeMap::new();
    if let Some(before) = before {
        for chunk in &before.chunks {
            let count = available
                .entry((chunk.hash.clone(), chunk.size))
                .or_default();
            *count = count.saturating_add(1);
        }
    }

    let mut delta = ChunkDelta::default();
    if let Some(after) = after {
        for chunk in &after.chunks {
            let key = (chunk.hash.clone(), chunk.size);
            if let Some(count) = available.get_mut(&key) {
                if *count > 0 {
                    *count -= 1;
                    delta.reused = delta.reused.saturating_add(1);
                    continue;
                }
            }
            delta.added = delta.added.saturating_add(1);
            delta.added_bytes = delta.added_bytes.saturating_add(chunk.size);
        }
    }
    if let Some(before) = before {
        let before_count = u64::try_from(before.chunks.len()).unwrap_or(u64::MAX);
        delta.removed = before_count.saturating_sub(delta.reused);
    }
    delta
}

/// Read the relevant regular worktree files into FileNodes.  When `tracked_hint` is present (the
/// normal worktree-vs-index/Commit mode), only paths that have a comparison base are considered,
/// matching Git's convention that untracked files belong to `status` until they are staged.
/// `None` is retained for callers that explicitly want a complete worktree snapshot.
fn read_worktree_snapshot(
    repository: &Repository,
    tracked_hint: Option<&BTreeSet<String>>,
) -> Result<Vec<FileNode>> {
    let root = repository.root();
    let mut paths = BTreeSet::new();
    if let Some(tracked) = tracked_hint {
        paths.extend(tracked.iter().cloned());
    } else {
        let managed_container = root.join(".neoengram/metadata/repository.json").is_file();
        let entries = WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                !is_neoengram_dir_name(entry.file_name())
                    && !(managed_container
                        && entry.depth() == 1
                        && is_managed_container_dir_name(entry.file_name()))
            });
        for entry in entries {
            let entry = entry.with_context(|| format!("遍历工作区失败: {}", root.display()))?;
            if entry.file_type().is_file() {
                paths.insert(repository_path(entry.path(), root)?);
            }
        }
    }

    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let disk_path = workspace_path(root, &path);
        if let Some(file) = read_worktree_file(root, &disk_path, &path)? {
            files.push(file);
        }
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn snapshot_paths(files: &[FileNode]) -> BTreeSet<String> {
    files.iter().map(|file| file.path.clone()).collect()
}

fn read_worktree_file(
    repository_root: &Path,
    path: &Path,
    logical_path: &str,
) -> Result<Option<FileNode>> {
    let Some(metadata) = lenient_leaf_metadata(repository_root, logical_path)? else {
        return Ok(None);
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(None);
    }

    let total_size = metadata.len();
    let mut source =
        File::open(path).with_context(|| format!("无法打开工作区文件: {}", path.display()))?;
    let mut chunks = Vec::new();
    let mut streamed_size = 0_u64;
    let chunker = StreamCDC::new(&mut source, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
    for chunk in chunker {
        let chunk = chunk.with_context(|| format!("无法读取工作区文件: {}", path.display()))?;
        let size = u64::try_from(chunk.length).context("Chunk 大小超出 u64 范围")?;
        let offset = chunk.offset;
        ensure!(
            offset == streamed_size,
            "工作区 Chunk 偏移不连续: {}",
            path.display()
        );
        let hash = blake3::hash(&chunk.data).to_hex().to_string();
        streamed_size = streamed_size
            .checked_add(size)
            .context("工作区文件大小溢出")?;
        chunks.push(Chunk { hash, offset, size });
    }
    ensure!(
        streamed_size == total_size,
        "工作区文件在读取期间发生变化: {}",
        path.display()
    );
    let Some(after) = lenient_leaf_metadata(repository_root, logical_path)? else {
        bail!("工作区文件在读取期间消失: {}", path.display());
    };
    ensure!(
        after.is_file() && !after.file_type().is_symlink() && after.len() == total_size,
        "工作区文件在读取期间发生变化: {}",
        path.display()
    );

    Ok(Some(FileNode {
        path: logical_path.to_owned(),
        total_size,
        chunks,
    }))
}

fn repository_path(path: &Path, repository_root: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(repository_root)
        .with_context(|| format!("工作区文件不在仓库根目录内: {}", path.display()))?;
    let mut names = Vec::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!("工作区路径未规范化: {}", path.display());
        };
        ensure!(
            !is_neoengram_dir_name(name),
            "工作区扫描进入了 NeoEngram 内部路径: {}",
            path.display()
        );
        names.push(
            name.to_str()
                .with_context(|| format!("工作区路径不是有效 UTF-8: {}", path.display()))?,
        );
    }
    ensure!(!names.is_empty(), "工作区文件路径不能为空");
    Ok(names.join("/"))
}

fn print_report(report: &DiffReport, stat_only: bool) {
    println!("Comparing {} -> {}", report.left_label, report.right_label);
    if !stat_only {
        for entry in &report.entries {
            print_entry(entry);
        }
    }
    if report.entries.is_empty() {
        println!("no differences");
    }
    print_summary(&report.summary);
}

fn print_entry(entry: &DiffEntry) {
    let before = format_stats(entry.before);
    let after = format_stats(entry.after);
    println!(
        "{}: {} ({} -> {}; chunks reused {}, new {}, removed {}, +{} bytes)",
        entry.kind.label(),
        entry.path,
        before,
        after,
        entry.chunks.reused,
        entry.chunks.added,
        entry.chunks.removed,
        entry.chunks.added_bytes
    );
}

fn format_stats(stats: Option<FileStats>) -> String {
    match stats {
        Some(stats) => format!("{} bytes/{} chunks", stats.bytes, stats.chunks),
        None => "<absent>".to_owned(),
    }
}

fn print_summary(summary: &DiffSummary) {
    println!(
        "Summary: {} added, {} modified, {} deleted; {} -> {} bytes; {} reused chunks, {} new chunks, {} removed chunks; estimated new storage {} bytes",
        summary.added_files,
        summary.modified_files,
        summary.deleted_files,
        summary.removed_bytes,
        summary.added_bytes,
        summary.reused_chunks,
        summary.added_chunks,
        summary.removed_chunks,
        summary.estimated_added_bytes
    );
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{chunk_delta, compare_snapshots, read_worktree_file, ChangeKind};
    use neoengram_core::{Chunk, FileNode};

    fn node(path: &str, chunks: &[(&str, u64)]) -> FileNode {
        let mut offset = 0;
        let chunks = chunks
            .iter()
            .map(|(hash, size)| {
                let chunk = Chunk {
                    hash: (*hash).to_owned(),
                    offset,
                    size: *size,
                };
                offset += *size;
                chunk
            })
            .collect();
        FileNode {
            path: path.to_owned(),
            total_size: offset,
            chunks,
        }
    }

    #[test]
    fn snapshot_diff_is_sorted_and_reports_chunk_reuse() {
        let before = vec![node("a", &[("a1", 2)]), node("d", &[("d1", 1)])];
        let after = vec![node("a", &[("a1", 2), ("a2", 1)]), node("c", &[("c1", 3)])];
        let diff = compare_snapshots(&before, &after);
        assert_eq!(
            diff.iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            ["a", "c", "d"]
        );
        assert_eq!(diff[0].kind, ChangeKind::Modified);
        assert_eq!(diff[0].chunks.reused, 1);
        assert_eq!(diff[0].chunks.added, 1);
        assert_eq!(diff[0].chunks.added_bytes, 1);
        assert_eq!(diff[1].kind, ChangeKind::Added);
        assert_eq!(diff[2].kind, ChangeKind::Deleted);
    }

    #[test]
    fn duplicate_chunks_are_counted_as_a_multiset() {
        let before = node("a", &[("same", 4), ("same", 4)]);
        let after = node("a", &[("same", 4), ("same", 4), ("new", 2)]);
        let delta = chunk_delta(Some(&before), Some(&after));
        assert_eq!(delta.reused, 2);
        assert_eq!(delta.added, 1);
        assert_eq!(delta.removed, 0);
        assert_eq!(delta.added_bytes, 2);
    }

    #[test]
    fn stream_fingerprint_handles_empty_and_small_files() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let empty = temporary.path().join("empty");
        fs::write(&empty, [])?;
        let node = read_worktree_file(temporary.path(), &empty, "empty")?.expect("empty file");
        assert_eq!(node.total_size, 0);
        assert!(node.chunks.is_empty());

        let small = temporary.path().join("small");
        fs::write(&small, b"small payload")?;
        let node = read_worktree_file(temporary.path(), &small, "small")?.expect("small file");
        assert_eq!(node.total_size, 13);
        assert_eq!(node.chunks.len(), 1);
        assert_eq!(
            node.chunks[0].hash,
            blake3::hash(b"small payload").to_hex().to_string()
        );
        Ok(())
    }
}
