use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

use crate::local::{
    metadata::IndexVersion,
    model::{ChunkingStrategy, FileNode, Index},
    repository::{ChunkingPolicy, Repository},
    worktree::{chunk_file, collect_files_with_ignore, validate_input_path, IgnoreRules},
};
use crate::{AddResult, AddedFile};
use anyhow::{ensure, Context, Result};
use futures::{stream, StreamExt};
use neoengram_core::LogicalPath;
use neoengram_engine::{ProgressEvent, ProgressPhase, ProgressSink, ProgressUnit};

pub(crate) async fn execute(
    cwd: PathBuf,
    path: PathBuf,
    chunking: Option<ChunkingStrategy>,
    progress: Arc<dyn ProgressSink>,
) -> Result<AddResult> {
    execute_with_options(cwd, path, false, chunking, progress).await
}

/// 执行 add，并可选择让 index 与指定路径范围内的工作区保持一致。
///
/// `all` 对应 CLI 的 `-A/--all`。普通 add 只更新仍存在的文件；开启 `all` 后，已在
/// index 中但本次遍历没有发现的路径也会被移除，从而可靠地暂存文件和目录删除。
pub(crate) async fn execute_with_options(
    current_dir: PathBuf,
    path: PathBuf,
    all: bool,
    chunking: Option<ChunkingStrategy>,
    progress: Arc<dyn ProgressSink>,
) -> Result<AddResult> {
    validate_input_path(&path)?;

    let repository = Repository::discover(&current_dir)?;
    let chunking = match repository.chunking_policy() {
        ChunkingPolicy::FastCdc => {
            ensure!(
                chunking.is_none(),
                "固定 fastcdc 仓库不接受 `add --chunking`；只有 mixed 仓库允许逐次选择"
            );
            Some(ChunkingStrategy::FastCdc)
        }
        ChunkingPolicy::WholeFile => {
            ensure!(
                chunking.is_none(),
                "固定 whole-file 仓库不接受 `add --chunking`；只有 mixed 仓库允许逐次选择"
            );
            Some(ChunkingStrategy::WholeFile)
        }
        ChunkingPolicy::Mixed => chunking,
    };
    // Lock order is fixed repository-wide: objects -> worktree -> write. Object publication is
    // serialized with GC for the whole add operation, while the shared worktree guard prevents
    // checkout/rm/restore from changing bytes during traversal and chunking.
    let _object_lock = repository.acquire_object_lock()?;
    let _worktree_lock = repository.acquire_worktree_shared_lock()?;
    let initial_index = repository.read_index_versioned()?;
    let ignore_rules = IgnoreRules::load(repository.root())?;
    let tracked_chunking: Arc<BTreeMap<String, ChunkingStrategy>> = Arc::new(
        initial_index
            .index()
            .files
            .iter()
            .map(|file| (file.path.clone(), file.chunking))
            .collect(),
    );
    let tracked_paths: BTreeSet<String> = initial_index
        .index()
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect();

    // WalkDir、canonicalize 和文件元数据查询都是同步系统调用。大型数据集可能包含
    // 数百万个目录项，因此把完整遍历移到阻塞线程池，避免卡住 Tokio 调度线程。
    let traversal_path = path.clone();
    let traversal_current_dir = current_dir;
    let repository_root = repository.root().to_path_buf();
    let traversal_ignore_rules = ignore_rules.clone();
    let traversal_tracked_paths = tracked_paths.clone();
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Scanning,
        ProgressUnit::Files,
        0,
    ))?;
    let collection = tokio::task::spawn_blocking(move || {
        collect_files_with_ignore(
            &traversal_path,
            &traversal_current_dir,
            &repository_root,
            all,
            &traversal_ignore_rules,
            &traversal_tracked_paths,
        )
    })
    .await
    .with_context(|| format!("目录遍历任务异常终止: {}", path.display()))??;

    // 路径协议错误（NFC、Windows 保留名、非法字符等）应在读取大文件之前失败，避免
    // 为一个永远不能进入 index 的路径留下大量不可达对象。
    for input in collection.files() {
        repository.validate_logical_path(input.repository_path())?;
    }

    let object_store = repository.object_store();
    let staging_dir = repository.staging_dir();
    let concurrency = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    let (input_files, repository_scope) = collection.into_parts();
    let total_files = u64::try_from(input_files.len()).context("文件数量超出 u64 范围")?;
    progress.emit(
        &ProgressEvent::new(ProgressPhase::Scanning, ProgressUnit::Files, total_files)
            .with_total(total_files),
    )?;
    let tasks = input_files.into_iter().map(|input| {
        let task_object_store = Arc::clone(&object_store);
        let task_staging_dir = staging_dir.clone();
        let task_tracked_chunking = Arc::clone(&tracked_chunking);

        async move {
            let (disk_path, repository_path) = input.into_parts();
            let selected_chunking = chunking.unwrap_or_else(|| {
                task_tracked_chunking
                    .get(&repository_path)
                    .copied()
                    .unwrap_or_default()
            });
            let mut file_node = chunk_file(
                disk_path.clone(),
                task_object_store,
                task_staging_dir,
                selected_chunking,
            )
            .await
            .with_context(|| format!("无法处理文件: {}", disk_path.display()))?;
            file_node.path = repository_path;
            Ok::<FileNode, anyhow::Error>(file_node)
        }
    });

    // 等待所有已开始的 spawn_blocking 任务结束，避免某个文件失败后仍有后台写操作。
    // 对象是不可变且内容寻址的，因此失败时留下尚未被 index 引用的对象也是安全的。
    let results: Vec<Result<FileNode>> = stream::iter(tasks)
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let mut staged_files = Vec::with_capacity(results.len());
    let mut processed_files = Vec::with_capacity(results.len());
    for result in results {
        let file = result?;
        let logical_path = file
            .path
            .parse::<LogicalPath>()
            .context("已验证的仓库路径无法转换为 LogicalPath")?;
        let chunk_count =
            u64::try_from(file.chunks.len()).context("文件 Chunk 数量超出 u64 范围")?;
        processed_files.push(AddedFile {
            path: logical_path.clone(),
            total_size: file.total_size,
            chunk_count,
            chunking: file.chunking,
        });
        staged_files.push(file);
        progress.emit(
            &ProgressEvent::new(
                ProgressPhase::Chunking,
                ProgressUnit::Files,
                u64::try_from(staged_files.len()).context("已处理文件数量超出 u64 范围")?,
            )
            .with_total(total_files)
            .at_path(logical_path),
        )?;
    }
    super::test_pause_at("add-before-index-publish");

    if staged_files.is_empty() && !all {
        progress.emit(&ProgressEvent::new(
            ProgressPhase::Completed,
            ProgressUnit::Actions,
            0,
        ))?;
        return Ok(AddResult {
            files: processed_files,
            removed_files: 0,
            index_version: *initial_index.version(),
            index_published: false,
        });
    }

    // 文件切块完成后才短暂持有仓库 state/write 锁，并以扫描前捕获的版本做 CAS。
    // 因此并发的 index-only restore 会让 add 明确失败，而不会被末尾重读混入结果。
    let index_repository = repository.clone();
    let deletion_scope = all.then_some(repository_scope);
    let (index, expected_version) = initial_index.into_parts();
    let update = tokio::task::spawn_blocking(move || {
        update_index(
            &index_repository,
            index,
            &expected_version,
            staged_files,
            deletion_scope.as_deref(),
        )
    })
    .await
    .context("index 更新任务异常终止")??;
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Completed,
        ProgressUnit::Actions,
        1,
    ))?;
    Ok(AddResult {
        files: processed_files,
        removed_files: update.removed_files,
        index_version: update.index_version,
        index_published: true,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexUpdate {
    index_version: IndexVersion,
    removed_files: u64,
}

fn update_index(
    repository: &Repository,
    index: Index,
    expected_version: &IndexVersion,
    staged_files: Vec<FileNode>,
    deletion_scope: Option<&str>,
) -> Result<IndexUpdate> {
    let _lock = repository.acquire_write_lock()?;
    let mut files: BTreeMap<String, FileNode> = index
        .files
        .into_iter()
        .map(|file| (file.path.clone(), file))
        .collect();

    let staged_paths: BTreeSet<&str> = staged_files.iter().map(|file| file.path.as_str()).collect();
    let before_deletion = files.len();
    if let Some(scope) = deletion_scope {
        // 只删除明确 pathspec 范围内、且本次完整遍历没有发现的已跟踪项。普通 add 不会
        // 进入此分支，因此不会因为一次局部 add 意外删除其他暂存内容。
        files.retain(|path, _| {
            !path_is_in_scope(path, scope) || staged_paths.contains(path.as_str())
        });
    }
    let removed_files =
        u64::try_from(before_deletion - files.len()).context("Index 删除文件数量超出 u64 范围")?;

    for file in staged_files {
        files.insert(file.path.clone(), file);
    }

    let index_version = repository
        .write_index_if_version(
            &Index {
                format_version: index.format_version,
                files: files.into_values().collect(),
            },
            expected_version,
        )
        .context("Index 在 add 扫描期间发生变化，请重试")?;
    Ok(IndexUpdate {
        index_version,
        removed_files,
    })
}

fn path_is_in_scope(path: &str, scope: &str) -> bool {
    scope.is_empty()
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|suffix| suffix.starts_with('/'))
}
