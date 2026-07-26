use std::time::{SystemTime, UNIX_EPOCH};

use crate::local::repository::Repository;
use crate::CommitResult;
use anyhow::{ensure, Context, Result};
use neoengram_core::CommitId;
use neoengram_engine::{
    BuildCommitGraphRequest, ProgressEvent, ProgressPhase, ProgressSink, ProgressUnit,
    PublishCommitRequest,
};
use std::sync::Arc;

pub(crate) async fn execute(
    cwd: std::path::PathBuf,
    message: String,
    progress: Arc<dyn ProgressSink>,
) -> Result<CommitResult> {
    let message = message.trim().to_owned();
    ensure!(!message.is_empty(), "Commit message 不能为空");

    let repository = Repository::discover(&cwd)?;
    let task_repository = repository.clone();
    tokio::task::spawn_blocking(move || create_commit(&task_repository, message, progress.as_ref()))
        .await
        .context("Commit 任务异常终止")?
}

fn create_commit(
    repository: &Repository,
    message: String,
    progress: &dyn ProgressSink,
) -> Result<CommitResult> {
    // 锁内重新读取 index 和 HEAD，保证 parent 恰好是 attached 分支 tip 或 direct HEAD。
    // Commit 数据模型只有一个 Option parent，因此无法产生 merge commit。
    let _lock = repository.acquire_write_lock()?;
    let head = repository.current_head()?;
    let parent_id = head.commit_id().map(str::to_owned);
    let parent = parent_id
        .as_deref()
        .map(str::parse::<CommitId>)
        .transpose()
        .context("当前 HEAD Commit ID 无效")?;

    let parent_tree_id = if let Some(parent_id) = &parent_id {
        Some(repository.read_commit(parent_id)?.root_directory_id)
    } else {
        None
    };
    let created_at_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("系统时间早于 Unix Epoch")?
            .as_millis(),
    )
    .context("Unix 毫秒时间戳超出 u64 范围")?;
    let graph = repository.build_commit_graph(
        &BuildCommitGraphRequest {
            expected_index_version: repository.current_index_version()?,
            parent,
            message,
            created_at_unix_ms,
        },
        parent.is_none(),
        progress,
    )?;
    let root_directory_id = graph.commit.root_directory_id;
    let file_count = graph.file_count;
    if let Some(parent_tree_id) = parent_tree_id {
        ensure!(
            parent_tree_id != root_directory_id.to_string(),
            "没有可提交的变更；index 与当前 Commit 完全一致"
        );
    }

    // Immutable graph objects are persisted first; SQLite HEAD/ref CAS remains the sole
    // authoritative Standalone publication point.
    let published = repository.publish_commit_graph(
        &PublishCommitRequest {
            graph,
            expected_parent: parent,
        },
        &head,
    )?;
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Completed,
        ProgressUnit::Actions,
        1,
    ))?;

    Ok(CommitResult {
        commit_id: published.commit_id,
        root_directory_id,
        file_count,
        detached: head.is_detached(),
    })
}
