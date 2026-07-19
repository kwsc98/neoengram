use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use neoengram_core::{FileNode, Index, Tree};
use tempfile::NamedTempFile;

use crate::local::{
    fs::durable::{create_dir_all_durable, replace_durable, sync_directory},
    objects::ObjectSpec,
    repository::Repository,
    worktree::{file_matches_node, repository_scope, safe_leaf_metadata, workspace_path},
};

/// Restore either the index (`--staged`) or the worktree from the current index.
///
/// The index mode deliberately mirrors the semantics of a safe unstage operation: paths are
/// copied from the current HEAD, while paths that are absent from HEAD are removed from the
/// index.  Worktree mode only writes files that are already tracked in the index and refuses to
/// overwrite a local modification unless `force` is explicitly requested.
pub(crate) async fn execute(paths: Vec<PathBuf>, staged: bool, force: bool) -> Result<()> {
    ensure!(!paths.is_empty(), "restore 至少需要一个 PATH");
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        restore_paths(&task_repository, &current_dir, &paths, staged, force)
    })
    .await
    .context("Restore 任务异常终止")??;

    for path in outcome {
        println!("Restored: {path}");
    }
    Ok(())
}

fn restore_paths(
    repository: &Repository,
    current_dir: &Path,
    requested_paths: &[PathBuf],
    staged: bool,
    force: bool,
) -> Result<Vec<String>> {
    repository.ensure_no_unfinished_transactions()?;
    let _lock = repository.acquire_write_lock()?;
    let scopes = requested_paths
        .iter()
        .map(|path| repository_scope(path, current_dir, repository.root()))
        .collect::<Result<Vec<_>>>()?;

    if staged {
        restore_index(repository, &scopes)
    } else {
        restore_worktree(repository, &scopes, force)
    }
}

fn restore_index(repository: &Repository, scopes: &[String]) -> Result<Vec<String>> {
    let current = repository.read_index()?;
    let format_version = current.format_version;
    let head_tree = current_head_tree(repository)?;
    let mut files: BTreeMap<String, FileNode> = current
        .files
        .into_iter()
        .map(|file| (file.path.clone(), file))
        .collect();
    let mut selected = BTreeMap::new();

    for file in head_tree.files {
        if scopes
            .iter()
            .any(|scope| path_is_in_scope(&file.path, scope))
        {
            let path = file.path.clone();
            selected.insert(path.clone(), file.clone());
            files.insert(path, file);
        }
    }

    let selected_paths: Vec<String> = files
        .keys()
        .filter(|path| scopes.iter().any(|scope| path_is_in_scope(path, scope)))
        .cloned()
        .collect();
    ensure!(
        !selected_paths.is_empty(),
        "指定路径没有已暂存或 HEAD 中的文件"
    );

    // Anything selected that is not present in HEAD is an addition in the index and must be
    // removed when restoring the staged view.
    files.retain(|path, _| {
        !scopes.iter().any(|scope| path_is_in_scope(path, scope)) || selected.contains_key(path)
    });
    let next = Index {
        format_version,
        files: files.into_values().collect(),
    };
    repository.write_index(&next)?;
    Ok(selected_paths)
}

fn restore_worktree(
    repository: &Repository,
    scopes: &[String],
    force: bool,
) -> Result<Vec<String>> {
    let index = repository.read_index()?;
    let files: Vec<FileNode> = index
        .files
        .into_iter()
        .filter(|file| {
            scopes
                .iter()
                .any(|scope| path_is_in_scope(&file.path, scope))
        })
        .collect();
    ensure!(!files.is_empty(), "指定路径没有已暂存文件");

    for file in &files {
        let destination = workspace_path(repository.root(), &file.path);
        let metadata = safe_leaf_metadata(repository.root(), &file.path)?;
        if let Some(metadata) = metadata {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "restore 只覆盖普通文件，路径类型异常: {}",
                file.path
            );
            if file_matches_node(&destination, file)? {
                continue;
            }
            ensure!(
                force,
                "工作区文件包含未暂存修改: {}；使用 `--force`",
                file.path
            );
        }
    }

    for file in &files {
        let destination = workspace_path(repository.root(), &file.path);
        if let Some(metadata) = safe_leaf_metadata(repository.root(), &file.path)? {
            if metadata.is_file()
                && !metadata.file_type().is_symlink()
                && file_matches_node(&destination, file)?
            {
                continue;
            }
        }
        materialize_file(repository, file, &destination)?;
    }
    Ok(files.into_iter().map(|file| file.path).collect())
}

fn current_head_tree(repository: &Repository) -> Result<Tree> {
    let Some(commit_id) = repository.current_commit_id()? else {
        return Ok(Tree::default());
    };
    let commit = repository.read_commit(&commit_id)?;
    repository.read_tree(&commit.tree_hash)
}

fn materialize_file(repository: &Repository, file: &FileNode, destination: &Path) -> Result<()> {
    let parent = destination.parent().context("restore 目标路径没有父目录")?;
    create_dir_all_durable(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)
        .with_context(|| format!("无法创建 restore 临时文件: {}", parent.display()))?;
    let mut written = 0_u64;
    for chunk in &file.chunks {
        let spec = ObjectSpec::new(chunk.hash.clone(), chunk.size)?;
        repository
            .object_store()
            .copy_to(&spec, temporary.as_file_mut())?;
        written = written
            .checked_add(chunk.size)
            .context("restore 文件大小溢出")?;
    }
    ensure!(
        written == file.total_size,
        "restore 文件大小与 index 不一致: {}",
        file.path
    );
    temporary
        .as_file_mut()
        .flush()
        .with_context(|| format!("无法刷新 restore 临时文件: {}", file.path))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("无法同步 restore 临时文件: {}", file.path))?;

    // The destination was checked above.  Replace it with one filesystem rename after the
    // complete payload has been verified and synced; a crash therefore leaves either the old or
    // the new regular file, never a half-written file.
    match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "restore 目标不是普通文件: {}",
                destination.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查 restore 目标: {}", destination.display()));
        }
    }
    let staged_path = temporary.into_temp_path();
    replace_durable(&staged_path, destination)?;
    if let Some(parent) = destination.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn path_is_in_scope(path: &str, scope: &str) -> bool {
    scope.is_empty()
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::path_is_in_scope;

    #[test]
    fn scope_matches_only_path_and_descendants() {
        assert!(path_is_in_scope("a", "a"));
        assert!(path_is_in_scope("a/b", "a"));
        assert!(!path_is_in_scope("ab", "a"));
        assert!(path_is_in_scope("anything", ""));
    }
}
