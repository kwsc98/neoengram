use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use neoengram_engine::ProgressSink;

use crate::{
    local::{repository::Repository, worktree},
    RemoveResult,
};

pub(crate) async fn execute(
    current_dir: PathBuf,
    path: PathBuf,
    cached: bool,
    force: bool,
    progress: Arc<dyn ProgressSink>,
) -> Result<RemoveResult> {
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();

    let removed = tokio::task::spawn_blocking(move || {
        worktree::remove_tracked_path(
            &task_repository,
            &current_dir,
            &path,
            cached,
            force,
            progress.as_ref(),
        )
    })
    .await
    .context("rm 任务异常终止")??;

    let removed_paths = removed
        .into_iter()
        .map(neoengram_core::LogicalPath::parse)
        .collect::<Result<Vec<_>, _>>()
        .context("rm 返回了无效逻辑路径")?;
    Ok(RemoveResult {
        removed_paths,
        cached,
    })
}
