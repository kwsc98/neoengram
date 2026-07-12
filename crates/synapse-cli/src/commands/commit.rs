use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{ensure, Context, Result};
use synapse::{Commit, FileNode, ObjectSpec, Tree};

use crate::repository::Repository;

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
    println!("Tree: {} ({} files)", outcome.tree_id, outcome.file_count);
    Ok(())
}

struct CommitOutcome {
    commit_id: String,
    tree_id: String,
    file_count: usize,
}

fn create_commit(repository: &Repository, message: String) -> Result<CommitOutcome> {
    // 锁内重新读取 index 和分支 tip，保证 parent 恰好是当前提交。Commit 数据模型只有
    // 一个 Option parent，发布路径也不接受第二父节点，因此无法产生 merge commit。
    let _lock = repository.acquire_write_lock()?;
    let index = repository.read_index()?;
    validate_staged_objects(repository, &index.files)?;
    let tree = Tree { files: index.files };
    let parent = repository.current_commit_id()?;

    let parent_tree_id = if let Some(parent_id) = &parent {
        Some(repository.read_commit(parent_id)?.tree_hash)
    } else {
        ensure!(
            !tree.files.is_empty(),
            "没有可提交的文件；请先运行 `synapse add`"
        );
        None
    };
    let tree_id = repository.store_tree(&tree)?;
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

    // Manifest、Tree 和 Commit 追加式发布，branch ref 是最后的线性化点。任一步失败都
    // 不会让引用指向缺失对象；最多留下后续可由 GC 清理的不可达元数据。
    let commit_id = repository.store_commit(&commit)?;
    repository.update_current_commit(commit.parent.as_deref(), &commit_id)?;

    Ok(CommitOutcome {
        commit_id,
        tree_id,
        file_count: tree.files.len(),
    })
}

fn validate_staged_objects(repository: &Repository, files: &[FileNode]) -> Result<()> {
    // Commit 会让 index 中的 Chunk 成为历史可达对象。发布 Tree 之前逐个复算 Hash，
    // 保证分支引用不会指向缺失或损坏的 payload；同一 Hash 只校验一次，避免去重对象
    // 被多个文件引用时重复读取。
    let mut chunks = BTreeMap::new();
    for file in files {
        for chunk in &file.chunks {
            if let Some(previous_size) = chunks.insert(chunk.hash.as_str(), chunk.size) {
                ensure!(
                    previous_size == chunk.size,
                    "Chunk {} 在 index 中具有冲突的大小: {} 与 {}",
                    chunk.hash,
                    previous_size,
                    chunk.size
                );
            }
        }
    }

    let object_store = repository.object_store();
    for (hash, size) in chunks {
        object_store.verify(&ObjectSpec::new(hash, size)?)?;
    }
    object_store.durability_barrier()
}
