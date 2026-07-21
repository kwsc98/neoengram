use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::local::{repository::Repository, worktree};

pub(crate) async fn execute(target: String, destination: PathBuf) -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let outcome: worktree::ReadOnlySnapshotOutcome = tokio::task::spawn_blocking(move || {
        worktree::materialize_read_only_snapshot(&task_repository, target.trim(), &destination)
    })
    .await
    .context("只读导出任务异常终止")??;
    println!(
        "Exported read-only snapshot of {} ({} files) at {}",
        outcome.commit_id(),
        outcome.file_count(),
        outcome.destination().display()
    );
    Ok(())
}
