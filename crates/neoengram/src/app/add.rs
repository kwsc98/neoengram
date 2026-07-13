use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result};
use futures::{stream, StreamExt};
use neoengram_core::{FileNode, Index};

use crate::local::{
    repository::Repository,
    worktree::{chunk_file, collect_files, validate_input_path},
};

pub(crate) async fn execute(path: PathBuf) -> Result<()> {
    execute_with_options(path, false).await
}

/// 执行 add，并可选择让 index 与指定路径范围内的工作区保持一致。
///
/// `all` 对应 CLI 的 `-A/--all`。普通 add 只更新仍存在的文件；开启 `all` 后，已在
/// index 中但本次遍历没有发现的路径也会被移除，从而可靠地暂存文件和目录删除。
pub(crate) async fn execute_with_options(path: PathBuf, all: bool) -> Result<()> {
    validate_input_path(&path)?;

    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    repository.ensure_no_unfinished_transactions()?;

    // WalkDir、canonicalize 和文件元数据查询都是同步系统调用。大型数据集可能包含
    // 数百万个目录项，因此把完整遍历移到阻塞线程池，避免卡住 Tokio 调度线程。
    let traversal_path = path.clone();
    let traversal_current_dir = current_dir;
    let repository_root = repository.root().to_path_buf();
    let collection = tokio::task::spawn_blocking(move || {
        collect_files(
            &traversal_path,
            &traversal_current_dir,
            &repository_root,
            all,
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
    let tasks = input_files.into_iter().map(|input| {
        let task_object_store = Arc::clone(&object_store);
        let task_staging_dir = staging_dir.clone();

        async move {
            let (disk_path, repository_path) = input.into_parts();
            let mut file_node = chunk_file(disk_path.clone(), task_object_store, task_staging_dir)
                .await
                .with_context(|| format!("无法处理文件: {}", disk_path.display()))?;
            file_node.path = repository_path;
            print_progress(&file_node);
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
    for result in results {
        staged_files.push(result?);
    }

    if staged_files.is_empty() && !all {
        return Ok(());
    }

    // 文件切块完成后才短暂持有仓库写锁。锁覆盖 index 的 read-modify-write，防止
    // 两个并发 add 都从旧 index 开始并互相覆盖；临时文件 + rename 则防止半写 JSON。
    let index_repository = repository.clone();
    let deletion_scope = all.then_some(repository_scope);
    tokio::task::spawn_blocking(move || {
        update_index(&index_repository, staged_files, deletion_scope.as_deref())
    })
    .await
    .context("index 更新任务异常终止")??;
    Ok(())
}

fn update_index(
    repository: &Repository,
    staged_files: Vec<FileNode>,
    deletion_scope: Option<&str>,
) -> Result<()> {
    let _lock = repository.acquire_write_lock()?;
    let index = repository.read_index()?;
    let mut files: BTreeMap<String, FileNode> = index
        .files
        .into_iter()
        .map(|file| (file.path.clone(), file))
        .collect();

    let staged_paths: BTreeSet<&str> = staged_files.iter().map(|file| file.path.as_str()).collect();
    if let Some(scope) = deletion_scope {
        // 只删除明确 pathspec 范围内、且本次完整遍历没有发现的已跟踪项。普通 add 不会
        // 进入此分支，因此不会因为一次局部 add 意外删除其他暂存内容。
        files.retain(|path, _| {
            !path_is_in_scope(path, scope) || staged_paths.contains(path.as_str())
        });
    }

    for file in staged_files {
        files.insert(file.path.clone(), file);
    }

    repository.write_index(&Index {
        format_version: index.format_version,
        files: files.into_values().collect(),
    })
}

fn path_is_in_scope(path: &str, scope: &str) -> bool {
    scope.is_empty()
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn print_progress(file_node: &FileNode) {
    println!(
        "Processed: {}, {} chunks created",
        file_node.path,
        file_node.chunks.len()
    );
}
