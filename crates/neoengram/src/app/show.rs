use anyhow::{Context, Result};
use neoengram_core::{ChunkingStrategy, Commit, Tree};

use crate::local::repository::Repository;

pub(crate) async fn execute(target: String) -> Result<()> {
    let target = target.trim().to_owned();
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let shown = tokio::task::spawn_blocking(move || read_snapshot(&repository, &target))
        .await
        .context("Show 任务异常终止")??;

    println!("commit {}", shown.id);
    println!("Tree:   {}", shown.commit.tree_hash);
    println!(
        "Parent: {}",
        shown.commit.parent.as_deref().unwrap_or("<root>")
    );
    println!("Time:   {}", shown.commit.created_at_unix_ms);
    println!();
    println!("    {}", shown.commit.message);
    println!();
    println!("Files ({}):", shown.tree.files.len());
    for file in &shown.tree.files {
        println!(
            "{:>12} bytes  {:>6} chunks  {:>10}  {}",
            file.total_size,
            file.chunks.len(),
            match file.chunking {
                ChunkingStrategy::FastCdc => "fastcdc",
                ChunkingStrategy::WholeFile => "whole-file",
            },
            file.path
        );
    }

    Ok(())
}

struct ShownSnapshot {
    id: String,
    commit: Commit,
    tree: Tree,
}

fn read_snapshot(repository: &Repository, target: &str) -> Result<ShownSnapshot> {
    let id = if target.eq_ignore_ascii_case("HEAD") {
        repository
            .current_commit_id()?
            .context("仓库还没有 Commit；请先运行 `neoengram commit`")?
    } else {
        anyhow::ensure!(!target.is_empty(), "Show TARGET 不能为空");
        target.to_owned()
    };
    let commit = repository.read_commit(&id)?;
    let tree = repository.read_tree(&commit.tree_hash)?;
    Ok(ShownSnapshot { id, commit, tree })
}
