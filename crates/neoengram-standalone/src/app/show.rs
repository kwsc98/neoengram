use crate::local::{model::Commit as StoredCommit, repository::Repository};
use crate::{CommitRecord, ShowFileRecord, ShowResult};
use anyhow::{Context, Result};
use neoengram_core::Commit;

pub(crate) async fn execute(cwd: std::path::PathBuf, target: String) -> Result<ShowResult> {
    let target = target.trim().to_owned();
    let repository = Repository::discover(&cwd)?;
    tokio::task::spawn_blocking(move || read_snapshot(&repository, &target))
        .await
        .context("Show 任务异常终止")?
}

fn read_snapshot(repository: &Repository, target: &str) -> Result<ShowResult> {
    let id = if target.eq_ignore_ascii_case("HEAD") {
        repository
            .current_commit_id()?
            .context("仓库还没有 Commit；请先运行 `neoengram commit`")?
    } else {
        anyhow::ensure!(!target.is_empty(), "Show TARGET 不能为空");
        target.to_owned()
    };
    let commit = repository.read_commit(&id)?;
    let tree = repository.read_tree(&commit.root_directory_id)?;
    let record = domain_commit(id, commit)?;
    let files = tree
        .files
        .into_iter()
        .map(|file| {
            Ok(ShowFileRecord {
                path: file.path,
                total_size: file.total_size,
                chunk_count: u64::try_from(file.chunks.len())
                    .context("Show 文件 Chunk 数量超出 u64")?,
                chunking: file.chunking,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ShowResult { record, files })
}

fn domain_commit(id: String, commit: StoredCommit) -> Result<CommitRecord> {
    let id = id.parse().context("Show Commit ID 无效")?;
    let root_directory_id = commit
        .root_directory_id
        .parse()
        .context("Show Directory ID 无效")?;
    let parent = commit
        .parent
        .map(|parent| parent.parse())
        .transpose()
        .context("Show parent Commit ID 无效")?;
    let commit = Commit::new(
        root_directory_id,
        parent,
        commit.message,
        commit.created_at_unix_ms,
    )
    .context("Show Commit 无效")?;
    Ok(CommitRecord { id, commit })
}
