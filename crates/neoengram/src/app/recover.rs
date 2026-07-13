use anyhow::{Context, Result};

use crate::local::{
    repository::Repository,
    worktree::{self, RecoveryReport},
};

pub(crate) async fn execute(abort: bool) -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let outcome =
        tokio::task::spawn_blocking(move || worktree::recover_repository(&repository, abort))
            .await
            .context("Recover 任务异常终止")??;

    match outcome {
        RecoveryReport::Nothing => println!("No unfinished transaction"),
        RecoveryReport::Checkout { target_id, aborted } => {
            if aborted {
                println!("Aborted checkout transaction for {target_id}");
            } else {
                println!("Recovered checkout transaction to {target_id}");
            }
        }
        RecoveryReport::Removal { id, aborted } => {
            if aborted {
                println!("Aborted rm transaction {id}");
            } else {
                println!("Recovered rm transaction {id}");
            }
        }
    }
    Ok(())
}
