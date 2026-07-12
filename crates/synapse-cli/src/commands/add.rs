use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs,
    num::NonZeroUsize,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, ensure, Context, Result};
use futures::{stream, StreamExt};
use synapse::{chunk_file, FileNode, Index};
use walkdir::WalkDir;

use crate::repository::Repository;

pub(crate) async fn execute(path: PathBuf) -> Result<()> {
    execute_with_options(path, false).await
}

/// 执行 add，并可选择让 index 与指定路径范围内的工作区保持一致。
///
/// `all` 对应 CLI 的 `-A/--all`。普通 add 只更新仍存在的文件；开启 `all` 后，已在
/// index 中但本次遍历没有发现的路径也会被移除，从而可靠地暂存文件和目录删除。
pub(crate) async fn execute_with_options(path: PathBuf, all: bool) -> Result<()> {
    if contains_synapse_component(&path) {
        bail!("不能把 Synapse 内部目录加入对象库: {}", path.display());
    }

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
    for input in &collection.files {
        repository.validate_logical_path(&input.repository_path)?;
    }

    let object_store = repository.object_store();
    let staging_dir = repository.staging_dir();
    let concurrency = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    let tasks = collection.files.into_iter().map(|input| {
        let task_object_store = Arc::clone(&object_store);
        let task_staging_dir = staging_dir.clone();

        async move {
            let disk_path = input.disk_path;
            let mut file_node = chunk_file(disk_path.clone(), task_object_store, task_staging_dir)
                .await
                .with_context(|| format!("无法处理文件: {}", disk_path.display()))?;
            file_node.path = input.repository_path;
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
    let deletion_scope = all.then_some(collection.repository_scope);
    tokio::task::spawn_blocking(move || {
        update_index(&index_repository, staged_files, deletion_scope.as_deref())
    })
    .await
    .context("index 更新任务异常终止")??;
    Ok(())
}

struct InputFile {
    disk_path: PathBuf,
    repository_path: String,
}

struct FileCollection {
    files: Vec<InputFile>,
    /// 空字符串表示仓库根目录，其余值均为规范的仓库相对路径。
    repository_scope: String,
}

fn collect_files(
    path: &Path,
    current_dir: &Path,
    repository_root: &Path,
    allow_missing: bool,
) -> Result<FileCollection> {
    let requested_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_dir.join(path)
    };
    let metadata = match fs::symlink_metadata(&requested_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_missing => {
            let normalized = normalize_absolute_path(&requested_path)?;
            ensure_missing_path_is_inside_repository(&normalized, repository_root)?;
            return Ok(FileCollection {
                files: Vec::new(),
                repository_scope: repository_path(&normalized, repository_root, true)?,
            });
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法访问路径: {}", requested_path.display()));
        }
    };
    ensure!(
        !metadata.file_type().is_symlink(),
        "add 不跟随符号链接: {}",
        requested_path.display()
    );
    ensure!(
        metadata.is_file() || metadata.is_dir(),
        "路径不是普通文件或目录: {}",
        requested_path.display()
    );

    let canonical_path = fs::canonicalize(&requested_path)
        .with_context(|| format!("无法解析路径: {}", requested_path.display()))?;
    ensure!(
        canonical_path.starts_with(repository_root),
        "不能添加仓库之外的路径: {}",
        requested_path.display()
    );
    let repository_scope = repository_path(&canonical_path, repository_root, true)?;

    let mut files = Vec::new();
    if metadata.is_file() {
        files.push(input_file(canonical_path, repository_root)?);
    } else {
        let entries = WalkDir::new(&canonical_path)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| !is_synapse_name(entry.file_name()));

        for entry in entries {
            let entry =
                entry.with_context(|| format!("遍历目录失败: {}", canonical_path.display()))?;
            // 不跟随符号链接可防止目录环和意外越过仓库边界。
            if entry.file_type().is_file() {
                files.push(input_file(entry.into_path(), repository_root)?);
            }
        }
    }

    files.sort_unstable_by(|left, right| left.repository_path.cmp(&right.repository_path));
    Ok(FileCollection {
        files,
        repository_scope,
    })
}

fn input_file(disk_path: PathBuf, repository_root: &Path) -> Result<InputFile> {
    let repository_path = repository_path(&disk_path, repository_root, false)?;

    Ok(InputFile {
        disk_path,
        repository_path,
    })
}

fn repository_path(path: &Path, repository_root: &Path, allow_root: bool) -> Result<String> {
    let relative = path.strip_prefix(repository_root).with_context(|| {
        format!(
            "文件不在仓库根目录内: {}（仓库根目录: {}）",
            path.display(),
            repository_root.display()
        )
    })?;
    let mut components = Vec::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!("文件路径未规范化: {}", path.display());
        };
        ensure!(
            !is_synapse_name(name),
            "不能添加 Synapse 内部文件: {}",
            path.display()
        );
        let name = name
            .to_str()
            .with_context(|| format!("文件路径不是有效 UTF-8: {}", path.display()))?;
        components.push(name);
    }
    ensure!(
        allow_root || !components.is_empty(),
        "不能把仓库根目录当作文件添加"
    );
    Ok(components.join("/"))
}

/// 对可能尚不存在的路径做纯词法规范化。调用方随后还会验证所有已存在的祖先，防止
/// `..` 或祖先符号链接把删除范围带到仓库之外。
fn normalize_absolute_path(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "内部错误：待规范化路径不是绝对路径");
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(
                    normalized.pop(),
                    "路径越过了文件系统根目录: {}",
                    path.display()
                );
            }
            Component::Normal(name) => normalized.push(name),
        }
    }
    Ok(normalized)
}

fn ensure_missing_path_is_inside_repository(path: &Path, repository_root: &Path) -> Result<()> {
    ensure!(
        path.starts_with(repository_root),
        "不能添加仓库之外的路径: {}",
        path.display()
    );

    let relative = path
        .strip_prefix(repository_root)
        .context("无法计算待删除路径的仓库相对位置")?;
    let mut current = repository_root.to_path_buf();
    let components: Vec<_> = relative.components().collect();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            bail!("文件路径未规范化: {}", path.display());
        };
        ensure!(
            !is_synapse_name(name),
            "不能添加 Synapse 内部文件: {}",
            path.display()
        );
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                ensure!(
                    !metadata.file_type().is_symlink(),
                    "路径祖先不能是符号链接: {}",
                    current.display()
                );
                if index + 1 < components.len() {
                    ensure!(metadata.is_dir(), "路径祖先不是目录: {}", current.display());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| format!("无法检查路径: {}", current.display()));
            }
        }
    }
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

fn contains_synapse_component(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::Normal(name) if is_synapse_name(name)))
}

fn is_synapse_name(name: &OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.eq_ignore_ascii_case(".synapse"))
}

fn print_progress(file_node: &FileNode) {
    println!(
        "Processed: {}, {} chunks created",
        file_node.path,
        file_node.chunks.len()
    );
}
