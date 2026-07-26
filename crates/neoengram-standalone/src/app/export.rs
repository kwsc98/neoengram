use std::path::PathBuf;

use anyhow::{Context, Result};
use neoengram_core::CommitId;

use crate::local::{repository::Repository, worktree};
use crate::ExportResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExportMode {
    Copy,
    HardLink,
}

pub(crate) async fn execute(
    cwd: PathBuf,
    target: String,
    destination: PathBuf,
    mode: ExportMode,
) -> Result<ExportResult> {
    let repository = Repository::discover(&cwd)?;
    let destination = if destination.is_absolute() {
        destination
    } else {
        cwd.join(destination)
    };
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
    Ok(ExportResult {
        commit_id: outcome
            .commit_id()
            .parse::<CommitId>()
            .context("只读导出返回无效 Commit ID")?,
        destination: outcome.destination().to_path_buf(),
        file_count: u64::try_from(outcome.file_count()).context("导出文件数量超过 u64")?,
        mode: match mode {
            ExportMode::Copy => crate::ExportMode::Copy,
            ExportMode::HardLink => crate::ExportMode::HardLink,
        },
    })
}
