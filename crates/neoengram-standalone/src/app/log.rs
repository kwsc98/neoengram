use std::collections::HashSet;

use crate::local::{model::Commit as StoredCommit, repository::Repository};
use crate::{CommitRecord, LogResult};
use anyhow::{ensure, Context, Result};
use neoengram_core::Commit;

pub(crate) async fn execute(
    cwd: std::path::PathBuf,
    max_count: Option<usize>,
) -> Result<LogResult> {
    if let Some(max_count) = max_count {
        ensure!(max_count > 0, "--max-count 必须大于 0");
    }

    let repository = Repository::discover(&cwd)?;
    let entries = tokio::task::spawn_blocking(move || read_history(&repository, max_count))
        .await
        .context("Log 任务异常终止")??;

    Ok(LogResult { entries })
}

fn read_history(repository: &Repository, max_count: Option<usize>) -> Result<Vec<CommitRecord>> {
    let mut next = repository
        .current_commit_id()?
        .context("仓库还没有 Commit；请先运行 `neoengram commit`")?;
    let mut visited = HashSet::new();
    let mut entries = Vec::new();

    loop {
        ensure!(visited.insert(next.clone()), "Commit 历史包含环: {next}");
        let commit = repository.read_commit(&next)?;
        let parent = commit.parent.clone();
        entries.push(domain_commit(next, commit)?);

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

fn domain_commit(id: String, commit: StoredCommit) -> Result<CommitRecord> {
    let id = id.parse().context("Log Commit ID 无效")?;
    let root_directory_id = commit
        .root_directory_id
        .parse()
        .context("Log Directory ID 无效")?;
    let parent = commit
        .parent
        .map(|parent| parent.parse())
        .transpose()
        .context("Log parent Commit ID 无效")?;
    let commit = Commit::new(
        root_directory_id,
        parent,
        commit.message,
        commit.created_at_unix_ms,
    )
    .context("Log Commit 无效")?;
    Ok(CommitRecord { id, commit })
}
