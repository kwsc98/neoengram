use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::local::{repository::Repository, worktree};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExportMode {
    Copy,
    HardLink,
}

pub(crate) async fn execute(target: String, destination: PathBuf, mode: ExportMode) -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let worktree_mode = match mode {
        ExportMode::Copy => worktree::ExportMode::Copy,
        ExportMode::HardLink => worktree::ExportMode::HardLink,
    };
    let outcome: worktree::ReadOnlySnapshotOutcome = tokio::task::spawn_blocking(move || {
        worktree::materialize_read_only_snapshot(
            &task_repository,
            target.trim(),
            &destination,
            worktree_mode,
        )
    })
    .await
    .context("只读导出任务异常终止")??;
    match mode {
        ExportMode::Copy => println!(
            "Exported read-only snapshot of {} ({} files) at {}",
            outcome.commit_id(),
            outcome.file_count(),
            outcome.destination().display()
        ),
        ExportMode::HardLink => {
            println!(
                "Exported hard-linked read-only view of {} ({} files) at {}",
                outcome.commit_id(),
                outcome.file_count(),
                outcome.destination().display()
            );
            eprintln!(
                "Warning: hard-linked files share content and permissions with repository objects; modifying the view corrupts every snapshot that references those objects."
            );
        }
    }
    Ok(())
}
