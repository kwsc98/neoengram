use std::collections::HashSet;

use anyhow::{ensure, Context, Result};
use synapse::Commit;

use crate::repository::Repository;

pub(crate) async fn execute(max_count: Option<usize>) -> Result<()> {
    if let Some(max_count) = max_count {
        ensure!(max_count > 0, "--max-count 必须大于 0");
    }

    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let entries = tokio::task::spawn_blocking(move || read_history(&repository, max_count))
        .await
        .context("Log 任务异常终止")??;

    for (position, entry) in entries.iter().enumerate() {
        if position > 0 {
            println!();
        }
        println!("commit {}", entry.id);
        println!("Tree:   {}", entry.commit.tree_hash);
        println!("Time:   {}", entry.commit.created_at_unix_ms);
        println!();
        println!("    {}", entry.commit.message);
    }

    Ok(())
}

struct LogEntry {
    id: String,
    commit: Commit,
}

fn read_history(repository: &Repository, max_count: Option<usize>) -> Result<Vec<LogEntry>> {
    let mut next = repository
        .current_commit_id()?
        .context("仓库还没有 Commit；请先运行 `synapse commit`")?;
    let mut visited = HashSet::new();
    let mut entries = Vec::new();

    loop {
        ensure!(visited.insert(next.clone()), "Commit 历史包含环: {next}");
        let commit = repository.read_commit(&next)?;
        let parent = commit.parent.clone();
        entries.push(LogEntry { id: next, commit });

        if max_count.is_some_and(|limit| entries.len() >= limit) {
            break;
        }
        let Some(parent) = parent else {
            break;
        };
        next = parent;
    }

    Ok(entries)
}
