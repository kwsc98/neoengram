use std::{
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
};

use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{Commit, FileNode, Tree};

use crate::local::{objects::ObjectSpec, repository::Repository};

pub(crate) async fn execute() -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let report = tokio::task::spawn_blocking(move || check_repository(&repository))
        .await
        .context("Fsck 任务异常终止")??;

    println!(
        "Fsck OK: {} refs, {} commits, {} trees, {} objects",
        report.refs, report.commits, report.trees, report.objects
    );
    Ok(())
}

struct FsckReport {
    refs: usize,
    commits: usize,
    trees: usize,
    objects: usize,
}

fn check_repository(repository: &Repository) -> Result<FsckReport> {
    // fsck 会扫描多个相互引用的文件。持有仓库写锁可阻止并发 add/commit/checkout
    // 在扫描中途发布新状态，从而让本次检查对应一个一致的本地元数据快照。
    let _lock = repository.acquire_write_lock()?;
    repository.verify_metadata_store_integrity()?;
    let index = repository.read_index()?;
    let commits = read_all_commits(repository)?;
    let trees = read_all_trees(repository)?;
    let refs = read_all_refs(repository, &commits)?;
    let head = repository.current_head()?;
    if let Some(commit_id) = head.commit_id() {
        ensure!(
            commits.contains_key(commit_id),
            "HEAD 指向不存在的 Commit: {commit_id}"
        );
    }

    validate_commit_graph(&commits, &trees)?;

    let mut referenced_chunks = HashMap::new();
    collect_file_chunks(&index.files, &mut referenced_chunks)?;
    for tree in trees.values() {
        collect_file_chunks(&tree.files, &mut referenced_chunks)?;
    }

    let objects = verify_all_objects(repository, &mut referenced_chunks)?;
    if let Some(hash) = referenced_chunks.keys().min() {
        anyhow::bail!("元数据引用了不存在的 Chunk 对象: {hash}");
    }

    Ok(FsckReport {
        refs,
        commits: commits.len(),
        trees: trees.len(),
        objects,
    })
}

fn read_all_commits(repository: &Repository) -> Result<BTreeMap<String, Commit>> {
    let mut commits = BTreeMap::new();
    for id in repository.list_commit_ids()? {
        let commit = repository.read_commit(&id)?;
        commits.insert(id, commit);
    }
    Ok(commits)
}

fn read_all_trees(repository: &Repository) -> Result<BTreeMap<String, Tree>> {
    let mut trees = BTreeMap::new();
    for id in repository.list_tree_ids()? {
        let tree = repository.read_tree(&id)?;
        trees.insert(id, tree);
    }
    Ok(trees)
}

fn read_all_refs(repository: &Repository, commits: &BTreeMap<String, Commit>) -> Result<usize> {
    let references = repository.list_references("refs")?;
    for reference in &references {
        let id = reference.target.trim();
        validate_hash(id, "分支引用")?;
        ensure!(
            commits.contains_key(id),
            "分支引用指向不存在的 Commit: {} -> {id}",
            reference.name
        );
    }
    Ok(references.len())
}

fn validate_commit_graph(
    commits: &BTreeMap<String, Commit>,
    trees: &BTreeMap<String, Tree>,
) -> Result<()> {
    for (id, commit) in commits {
        ensure!(
            trees.contains_key(&commit.tree_hash),
            "Commit {id} 引用了不存在的 Tree: {}",
            commit.tree_hash
        );
        if let Some(parent) = &commit.parent {
            ensure!(
                commits.contains_key(parent),
                "Commit {id} 引用了不存在的父 Commit: {parent}"
            );
        }
    }

    // Commit 只有一个父节点，因此逐链检查即可。resolved 中的节点已确认最终到达根，
    // current_chain 用来发现当前链回到自身的情况。
    let mut resolved = HashSet::new();
    for start in commits.keys() {
        if resolved.contains(start) {
            continue;
        }
        let mut current = Some(start.as_str());
        let mut current_chain = HashSet::new();
        while let Some(id) = current {
            if resolved.contains(id) {
                break;
            }
            ensure!(
                current_chain.insert(id.to_owned()),
                "Commit 历史包含环，重复节点: {id}"
            );
            current = commits
                .get(id)
                .with_context(|| format!("Commit 图引用不存在的节点: {id}"))?
                .parent
                .as_deref();
        }
        resolved.extend(current_chain);
    }
    Ok(())
}

fn collect_file_chunks(files: &[FileNode], chunks: &mut HashMap<String, u64>) -> Result<()> {
    for file in files {
        for chunk in &file.chunks {
            if let Some(previous_size) = chunks.insert(chunk.hash.clone(), chunk.size) {
                ensure!(
                    previous_size == chunk.size,
                    "Chunk {} 在元数据中具有冲突的大小: {} 与 {}",
                    chunk.hash,
                    previous_size,
                    chunk.size
                );
            }
        }
    }
    Ok(())
}

fn verify_all_objects(
    repository: &Repository,
    referenced: &mut HashMap<String, u64>,
) -> Result<usize> {
    const PAGE_SIZE: usize = 1_024;

    let store = repository.object_store();
    let limit = NonZeroUsize::new(PAGE_SIZE).expect("对象页大小必须大于零");
    let mut cursor = None;
    let mut count = 0_usize;
    loop {
        let page = store.list_page(cursor.as_deref(), limit)?;
        ensure!(page.objects.len() <= PAGE_SIZE, "对象后端返回了超大分页");
        for object in page.objects {
            let spec = ObjectSpec::new(object.id.clone(), object.size)?;
            store.verify(&spec)?;
            if let Some(expected_size) = referenced.remove(&object.id) {
                ensure!(
                    expected_size == object.size,
                    "Chunk {} 的对象大小与元数据不一致: {} 与 {}",
                    object.id,
                    object.size,
                    expected_size
                );
            }
            count = count.checked_add(1).context("对象数量超出 usize")?;
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        ensure!(
            cursor.as_deref() != Some(next.as_str()),
            "对象后端返回了不前进的分页游标"
        );
        cursor = Some(next);
    }
    Ok(count)
}

fn validate_hash(hash: &str, kind: &str) -> Result<()> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{kind} 不是有效的小写 BLAKE3 Hash: {hash}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_hash;

    #[test]
    fn hash_validation_rejects_uppercase_and_wrong_length() {
        assert!(validate_hash(&"a".repeat(64), "test").is_ok());
        assert!(validate_hash(&"A".repeat(64), "test").is_err());
        assert!(validate_hash("abc", "test").is_err());
    }
}
