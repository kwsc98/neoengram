use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{ensure, Context, Result};
use neoengram_engine::ProgressSink;

use crate::local::{
    model::{FileNode, Index, Tree},
    repository::Repository,
    worktree::{repository_scope, restore_worktree_paths},
};
use crate::{RestoreResult, RestoreTarget};

/// Restore either the index (`--staged`) or the worktree from the current index.
///
/// The index mode deliberately mirrors the semantics of a safe unstage operation: paths are
/// copied from the current HEAD, while paths that are absent from HEAD are removed from the
/// index.  Worktree mode only writes files that are already tracked in the index and refuses to
/// overwrite a local modification unless `force` is explicitly requested.
pub(crate) async fn execute(
    current_dir: PathBuf,
    paths: Vec<PathBuf>,
    staged: bool,
    force: bool,
    progress: Arc<dyn ProgressSink>,
) -> Result<RestoreResult> {
    ensure!(!paths.is_empty(), "restore 至少需要一个 PATH");
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        restore_paths(
            &task_repository,
            &current_dir,
            &paths,
            staged,
            force,
            progress.as_ref(),
        )
    })
    .await
    .context("Restore 任务异常终止")??;

    let paths = outcome
        .into_iter()
        .map(neoengram_core::LogicalPath::parse)
        .collect::<Result<Vec<_>, _>>()
        .context("Restore 返回了无效逻辑路径")?;
    Ok(RestoreResult {
        paths,
        target: if staged {
            RestoreTarget::Index
        } else {
            RestoreTarget::Worktree
        },
    })
}

fn restore_paths(
    repository: &Repository,
    current_dir: &Path,
    requested_paths: &[PathBuf],
    staged: bool,
    force: bool,
    progress: &dyn ProgressSink,
) -> Result<Vec<String>> {
    let scopes = requested_paths
        .iter()
        .map(|path| repository_scope(path, current_dir, repository.root()))
        .collect::<Result<Vec<_>>>()?;

    if staged {
        let _lock = repository.acquire_write_lock()?;
        restore_index(repository, &scopes)
    } else {
        // Worktree operations always acquire worktree before state. This excludes checkout/rm
        // while allowing add/status/diff to share a stable read view.
        let _worktree_lock = repository.acquire_worktree_lock()?;
        let _lock = repository.acquire_write_lock()?;
        restore_worktree(repository, &scopes, force, progress)
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
    progress: &dyn ProgressSink,
) -> Result<Vec<String>> {
    restore_worktree_paths(repository, scopes, force, progress)
}

fn current_head_tree(repository: &Repository) -> Result<Tree> {
    let Some(commit_id) = repository.current_commit_id()? else {
        return Ok(Tree::default());
    };
    let commit = repository.read_commit(&commit_id)?;
    repository.read_tree(&commit.root_directory_id)
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
