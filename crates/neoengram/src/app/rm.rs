use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::local::{repository::Repository, worktree};

pub(crate) async fn execute(path: PathBuf, cached: bool, force: bool) -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();

    let removed = tokio::task::spawn_blocking(move || {
        worktree::remove_tracked_path(&task_repository, &current_dir, &path, cached, force)
    })
    .await
    .context("rm 任务异常终止")??;

    for path in &removed {
        println!("Removed: {path}");
    }
    Ok(())
}
