use std::{collections::HashMap, num::NonZeroUsize};

use anyhow::{ensure, Context, Result};

use crate::local::{objects::ObjectSpec, repository::Repository};

/// Remove loose objects that are not referenced by the current index or any persisted Tree.
///
/// Immutable metadata has no deletion operation yet, so every persisted Tree is retained in the
/// mark set; otherwise fsck would correctly report unreachable history as corrupt.  This command
/// therefore reclaims failed/incomplete add payloads, while history pruning remains separate.
pub(crate) async fn execute(dry_run: bool) -> Result<()> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let repository = Repository::discover(&current_dir)?;
    let task_repository = repository.clone();
    let report =
        tokio::task::spawn_blocking(move || collect_and_reclaim(&task_repository, dry_run))
            .await
            .context("GC 任务异常终止")??;

    if dry_run {
        println!(
            "GC dry-run: {} unreferenced objects, {} bytes reclaimable",
            report.unreferenced_objects, report.reclaimable_bytes
        );
    } else {
        println!(
            "GC complete: {} objects removed, {} bytes reclaimed",
            report.removed_objects, report.reclaimed_bytes
        );
    }
    Ok(())
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct GcReport {
    unreferenced_objects: usize,
    reclaimable_bytes: u64,
    removed_objects: usize,
    reclaimed_bytes: u64,
}

fn collect_and_reclaim(repository: &Repository, dry_run: bool) -> Result<GcReport> {
    // Acquire locks in the same order as `add`: object publication first, then the ordinary write
    // lock.  This keeps a concurrent add from publishing a payload between the mark and sweep
    // phases while the write lock freezes refs, HEAD, and index.
    let _object_lock = repository.acquire_object_lock()?;
    let _lock = repository.acquire_write_lock()?;
    let referenced = mark_referenced_chunks(repository)?;
    sweep_objects(repository, &referenced, dry_run)
}

fn mark_referenced_chunks(repository: &Repository) -> Result<HashMap<String, u64>> {
    let mut referenced = HashMap::new();
    let index = repository.read_index()?;
    collect_files(&index.files, &mut referenced)?;

    // Keep all persisted history for now.  A future metadata GC can delete unreachable Trees and
    // then narrow this mark set to refs/HEAD roots in the same transaction.
    for tree_id in repository.list_tree_ids()? {
        let tree = repository.read_tree(&tree_id)?;
        collect_files(&tree.files, &mut referenced)?;
    }
    Ok(referenced)
}

fn collect_files(
    files: &[neoengram_core::FileNode],
    referenced: &mut HashMap<String, u64>,
) -> Result<()> {
    for file in files {
        for chunk in &file.chunks {
            if let Some(previous) = referenced.insert(chunk.hash.clone(), chunk.size) {
                ensure!(
                    previous == chunk.size,
                    "Chunk {} 在持久化元数据中具有冲突大小: {} 与 {}",
                    chunk.hash,
                    previous,
                    chunk.size
                );
            }
        }
    }
    Ok(())
}

fn sweep_objects(
    repository: &Repository,
    referenced: &HashMap<String, u64>,
    dry_run: bool,
) -> Result<GcReport> {
    const PAGE_SIZE: usize = 1_024;
    let store = repository.object_store();
    let limit = NonZeroUsize::new(PAGE_SIZE).expect("对象页大小必须大于零");
    let mut cursor = None;
    let mut report = GcReport::default();

    loop {
        let page = store.list_page(cursor.as_deref(), limit)?;
        ensure!(page.objects.len() <= PAGE_SIZE, "对象后端返回了超大分页");
        for object in page.objects {
            if referenced.contains_key(&object.id) {
                continue;
            }
            report.unreferenced_objects = report
                .unreferenced_objects
                .checked_add(1)
                .context("未引用对象数量溢出")?;
            report.reclaimable_bytes = report
                .reclaimable_bytes
                .checked_add(object.size)
                .context("可回收对象大小溢出")?;
            if !dry_run {
                // Verify the physical object before unlinking it. This catches a concurrent or
                // external replacement instead of silently deleting a path with a reused name.
                store.verify(&ObjectSpec::new(object.id.clone(), object.size)?)?;
                if store.remove(&object.id)? {
                    report.removed_objects = report
                        .removed_objects
                        .checked_add(1)
                        .context("删除对象数量溢出")?;
                    report.reclaimed_bytes = report
                        .reclaimed_bytes
                        .checked_add(object.size)
                        .context("已回收对象大小溢出")?;
                }
            }
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
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use neoengram_core::{Chunk, FileNode};

    use super::collect_files;

    #[test]
    fn mark_keeps_a_single_size_for_deduplicated_chunks() {
        let mut referenced = HashMap::new();
        let file = FileNode {
            path: "a".to_owned(),
            total_size: 1,
            chunks: vec![Chunk {
                hash: "a".repeat(64),
                offset: 0,
                size: 1,
            }],
        };
        assert!(collect_files(&[file], &mut referenced).is_ok());
        assert_eq!(referenced.len(), 1);
    }
}
