use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Result};
use neoengram_core::Commit;

use crate::local::repository::Repository;

pub(crate) async fn execute(message: String) -> Result<()> {
    let message = message.trim().to_owned();
    ensure!(!message.is_empty(), "Commit message 不能为空");

    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let outcome = tokio::task::spawn_blocking(move || create_commit(&task_repository, message))
        .await
        .context("Commit 任务异常终止")??;

    println!("Committed {}", outcome.commit_id);
    println!(
        "Directory: {} ({} files)",
        outcome.tree_id, outcome.file_count
    );
    if outcome.detached {
        println!("HEAD is now detached at {}", outcome.commit_id);
    }
    Ok(())
}

struct CommitOutcome {
    commit_id: String,
    tree_id: String,
    file_count: u64,
    detached: bool,
}

fn create_commit(repository: &Repository, message: String) -> Result<CommitOutcome> {
    // 锁内重新读取 index 和 HEAD，保证 parent 恰好是 attached 分支 tip 或 direct HEAD。
    // Commit 数据模型只有一个 Option parent，因此无法产生 merge commit。
    let _lock = repository.acquire_write_lock()?;
    let head = repository.current_head()?;
    let parent = head.commit_id().map(str::to_owned);

    let parent_tree_id = if let Some(parent_id) = &parent {
        Some(repository.read_commit(parent_id)?.tree_hash)
    } else {
        None
    };
    let (tree_id, file_count) = repository.store_index_tree(parent.is_none())?;
    if let Some(parent_tree_id) = parent_tree_id {
        ensure!(
            parent_tree_id != tree_id,
            "没有可提交的变更；index 与当前 Commit 完全一致"
        );
    }

    let created_at_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("系统时间早于 Unix Epoch")?
            .as_millis(),
    )
    .context("Unix 毫秒时间戳超出 u64 范围")?;
    let commit = Commit {
        tree_hash: tree_id.clone(),
        parent,
        message,
        created_at_unix_ms,
    };

    // Manifest、Tree 和 Commit 追加式发布，branch ref 或 direct HEAD 是最后的线性化点。
    // 任一步失败都不会让 HEAD 指向缺失对象；最多留下后续可由 GC 清理的不可达元数据。
    let commit_id = repository.store_commit(&commit)?;
    repository.advance_head(&head, &commit_id)?;

    Ok(CommitOutcome {
        commit_id,
        tree_id,
        file_count,
        detached: head.is_detached(),
    })
}
