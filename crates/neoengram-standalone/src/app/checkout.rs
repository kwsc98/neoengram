use std::sync::Arc;

use anyhow::{Context, Result};
use neoengram_engine::ProgressSink;

use crate::{
    local::{repository::Repository, worktree},
    CheckoutHead, CheckoutResult,
};

pub(crate) async fn execute(
    cwd: std::path::PathBuf,
    target: String,
    force: bool,
    progress: Arc<dyn ProgressSink>,
) -> Result<CheckoutResult> {
    let repository = Repository::discover(&cwd)?;

    let task_repository = repository.clone();
    let outcome: worktree::CheckoutOutcome = tokio::task::spawn_blocking(move || {
        worktree::checkout_snapshot_with_progress(
            &task_repository,
            target.trim(),
            force,
            progress.as_ref(),
        )
    })
    .await
    .context("Checkout 任务异常终止")??;

    let commit_id = outcome
        .commit_id()
        .parse()
        .context("Checkout 返回了无效 Commit ID")?;
    let head = if outcome.is_detached() {
        CheckoutHead::Detached(commit_id)
    } else {
        CheckoutHead::Branch(
            outcome
                .branch_name()
                .context("Checkout 返回了无效 HEAD 状态")?
                .to_owned(),
        )
    };
    Ok(CheckoutResult {
        commit_id,
        file_count: u64::try_from(outcome.file_count()).context("Checkout 文件数超出范围")?,
        removed_count: u64::try_from(outcome.removed_count())
            .context("Checkout 删除文件数超出范围")?,
        head,
    })
}
