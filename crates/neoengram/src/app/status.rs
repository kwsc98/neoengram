use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path},
};

use anyhow::{ensure, Context, Result};
use neoengram_core::FileNode;
use walkdir::WalkDir;

use crate::local::{
    repository::{is_neoengram_dir_name, Repository},
    worktree::{file_matches_node, lenient_leaf_metadata, workspace_path},
};

pub(crate) async fn execute() -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let report = tokio::task::spawn_blocking(move || build_report(&task_repository))
        .await
        .context("status 任务异常终止")??;

    print_report(&report);
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeKind {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathChange {
    path: String,
    kind: ChangeKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HeadPosition {
    Branch(String),
    Detached(String),
}

#[derive(Debug, PartialEq, Eq)]
struct StatusReport {
    head: HeadPosition,
    staged: Vec<PathChange>,
    unstaged: Vec<PathChange>,
    untracked: Vec<String>,
}

impl StatusReport {
    fn is_clean(&self) -> bool {
        self.staged.is_empty() && self.unstaged.is_empty() && self.untracked.is_empty()
    }
}

fn build_report(repository: &Repository) -> Result<StatusReport> {
    ensure_no_unfinished_transaction(repository)?;
    let index = repository.read_index()?;
    let head = repository.current_head()?;
    let head_files = match head.commit_id() {
        Some(commit_id) => {
            let commit = repository.read_commit(commit_id)?;
            repository.read_tree(&commit.tree_hash)?.files
        }
        None => Vec::new(),
    };
    let head_position = if head.is_detached() {
        HeadPosition::Detached(
            head.commit_id()
                .context("Detached HEAD 缺少 Commit")?
                .to_owned(),
        )
    } else {
        HeadPosition::Branch(
            head.branch_name()
                .context("Attached HEAD 缺少分支名")?
                .to_owned(),
        )
    };

    let staged = compare_snapshots(&head_files, &index.files);
    let mut unstaged = Vec::new();
    for file in &index.files {
        let path = workspace_path(repository.root(), &file.path);
        match lenient_leaf_metadata(repository.root(), &file.path)? {
            None => unstaged.push(PathChange {
                path: file.path.clone(),
                kind: ChangeKind::Deleted,
            }),
            Some(metadata)
                if !metadata.is_file()
                    || metadata.file_type().is_symlink()
                    || !file_matches_node(&path, file)? =>
            {
                unstaged.push(PathChange {
                    path: file.path.clone(),
                    kind: ChangeKind::Modified,
                });
            }
            Some(_) => {}
        }
    }

    let tracked: BTreeSet<&str> = index.files.iter().map(|file| file.path.as_str()).collect();
    let untracked = collect_untracked_files(repository.root(), &tracked)?;
    // status 没有持有写锁，因此在完成扫描后再次检查，避免输出恰好跨越一次 rm/checkout
    // 工作区事务而混合两个时刻的状态。
    ensure_no_unfinished_transaction(repository)?;
    ensure!(
        repository.current_head()? == head,
        "HEAD 在 status 扫描期间发生变化，请重试"
    );
    Ok(StatusReport {
        head: head_position,
        staged,
        unstaged,
        untracked,
    })
}

fn compare_snapshots(base: &[FileNode], current: &[FileNode]) -> Vec<PathChange> {
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
            let kind = match (base.get(path), current.get(path)) {
                (None, Some(_)) => ChangeKind::Added,
                (Some(_), None) => ChangeKind::Deleted,
                (Some(before), Some(after)) if before != after => ChangeKind::Modified,
                (Some(_), Some(_)) | (None, None) => return None,
            };
            Some(PathChange {
                path: path.to_owned(),
                kind,
            })
        })
        .collect()
}

fn collect_untracked_files(
    repository_root: &Path,
    tracked: &BTreeSet<&str>,
) -> Result<Vec<String>> {
    let entries = WalkDir::new(repository_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| !is_neoengram_dir_name(entry.file_name()));
    let mut untracked = Vec::new();
    for entry in entries {
        let entry =
            entry.with_context(|| format!("遍历工作区失败: {}", repository_root.display()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = repository_path(entry.path(), repository_root)?;
        if !tracked.contains(path.as_str()) {
            untracked.push(path);
        }
    }
    untracked.sort_unstable();
    Ok(untracked)
}

fn repository_path(path: &Path, repository_root: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(repository_root)
        .with_context(|| format!("工作区文件不在仓库根目录内: {}", path.display()))?;
    let mut names = Vec::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            anyhow::bail!("工作区路径未规范化: {}", path.display());
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

fn ensure_no_unfinished_transaction(repository: &Repository) -> Result<()> {
    let mut entries = fs::read_dir(repository.transactions_dir()).with_context(|| {
        format!(
            "无法读取事务目录: {}",
            repository.transactions_dir().display()
        )
    })?;
    if let Some(entry) = entries.next() {
        let entry = entry.context("无法读取事务项")?;
        anyhow::bail!(
            "检测到未完成的工作区事务，请先运行 recover: {}",
            entry.path().display()
        );
    }
    Ok(())
}

fn print_report(report: &StatusReport) {
    match &report.head {
        HeadPosition::Branch(branch) => println!("On branch {branch}"),
        HeadPosition::Detached(commit_id) => println!("HEAD detached at {commit_id}"),
    }
    if report.is_clean() {
        println!("nothing to commit, working tree clean");
        return;
    }

    print_changes("Changes to be committed:", &report.staged);
    print_changes("Changes not staged for commit:", &report.unstaged);
    if !report.untracked.is_empty() {
        println!("Untracked files:");
        for path in &report.untracked {
            println!("  {path}");
        }
    }
}

fn print_changes(title: &str, changes: &[PathChange]) {
    if changes.is_empty() {
        return;
    }
    println!("{title}");
    for change in changes {
        println!("  {}: {}", change.kind.label(), change.path);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{compare_snapshots, ChangeKind, PathChange};
    use crate::local::worktree::lenient_leaf_metadata;
    use neoengram_core::{Chunk, FileNode};

    #[test]
    fn snapshot_diff_is_sorted_and_classifies_changes() {
        let before = vec![node("a", "a1"), node("b", "b1"), node("d", "d1")];
        let after = vec![node("a", "a2"), node("b", "b1"), node("c", "c1")];

        assert_eq!(
            compare_snapshots(&before, &after),
            vec![
                PathChange {
                    path: "a".to_owned(),
                    kind: ChangeKind::Modified,
                },
                PathChange {
                    path: "c".to_owned(),
                    kind: ChangeKind::Added,
                },
                PathChange {
                    path: "d".to_owned(),
                    kind: ChangeKind::Deleted,
                },
            ]
        );
    }

    #[test]
    fn parent_file_makes_tracked_descendant_missing() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        fs::write(temporary.path().join("a"), b"now a file")?;

        assert!(lenient_leaf_metadata(temporary.path(), "a/b")?.is_none());
        Ok(())
    }

    fn node(path: &str, hash: &str) -> FileNode {
        FileNode {
            path: path.to_owned(),
            total_size: 1,
            chunks: vec![Chunk {
                hash: hash.to_owned(),
                offset: 0,
                size: 1,
            }],
        }
    }
}
