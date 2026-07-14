use anyhow::{Context, Result};

use crate::local::{repository::Repository, worktree};

pub(crate) async fn execute(target: String, force: bool) -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let outcome: worktree::CheckoutOutcome = tokio::task::spawn_blocking(move || {
        worktree::checkout_snapshot(&task_repository, target.trim(), force)
    })
    .await
    .context("Checkout 任务异常终止")??;

    println!(
        "Checked out {} ({} files, {} removed)",
        outcome.commit_id(),
        outcome.file_count(),
        outcome.removed_count()
    );
    if outcome.is_detached() {
        println!("HEAD is now detached at {}", outcome.commit_id());
    } else if let Some(branch) = outcome.branch_name() {
        println!("HEAD is now on branch {branch}");
    }
    Ok(())
}
