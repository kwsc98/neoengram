use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
};

use anyhow::{ensure, Context, Result};

use crate::local::{objects::ObjectSpec, repository::Repository};
use crate::GcResult;

/// Remove loose objects that are not referenced by the current Index or any persisted Commit.
pub(crate) async fn execute(cwd: std::path::PathBuf, dry_run: bool) -> Result<GcResult> {
    let repository = Repository::discover(&cwd)?;
    let task_repository = repository.clone();
    tokio::task::spawn_blocking(move || collect_and_reclaim(&task_repository, dry_run))
        .await
        .context("GC 任务异常终止")?
}

fn collect_and_reclaim(repository: &Repository, dry_run: bool) -> Result<GcResult> {
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
    let mut manifests = HashSet::new();
    repository.visit_all_workspace_index_files(|record| {
        collect_manifest(
            repository,
            &record.manifest_id,
            record.total_size,
            &mut manifests,
            &mut referenced,
        )
    })?;

    // Every persisted Commit remains a GC root until history pruning and mount leases exist.
    let mut roots = Vec::new();
    for commit_id in repository.list_commit_ids()? {
        roots.push(repository.read_commit(&commit_id)?.root_directory_id);
    }
    // Validate and explicitly include HEAD/ref roots as well. This makes GC fail closed on a
    // dangling reference instead of silently treating its payload as reclaimable.
    if let Some(head_id) = repository.current_head()?.commit_id() {
        roots.push(repository.read_commit(head_id)?.root_directory_id);
    }
    for reference in repository.list_references("refs")? {
        roots.push(repository.read_commit(&reference.target)?.root_directory_id);
    }
    repository.visit_directory_dag(&roots, |entry| {
        if entry.kind == crate::local::model::DirectoryEntryKind::File {
            collect_manifest(
                repository,
                &entry.target_id,
                entry.total_size,
                &mut manifests,
                &mut referenced,
            )?;
        }
        Ok(())
    })?;
    Ok(referenced)
}

fn collect_manifest(
    repository: &Repository,
    manifest_id: &str,
    total_size: u64,
    manifests: &mut HashSet<String>,
    referenced: &mut HashMap<String, u64>,
) -> Result<()> {
    if !manifests.insert(manifest_id.to_owned()) {
        return Ok(());
    }
    repository.visit_manifest_chunks(manifest_id, total_size, |chunk| {
        if let Some(previous) = referenced.insert(chunk.hash.clone(), chunk.size) {
            ensure!(
                previous == chunk.size,
                "Chunk {} 在持久化元数据中具有冲突大小: {} 与 {}",
                chunk.hash,
                previous,
                chunk.size
            );
        }
        Ok(())
    })?;
    Ok(())
}

fn sweep_objects(
    repository: &Repository,
    referenced: &HashMap<String, u64>,
    dry_run: bool,
) -> Result<GcResult> {
    const PAGE_SIZE: usize = 1_024;
    let store = repository.object_store();
    let limit = NonZeroUsize::new(PAGE_SIZE).expect("对象页大小必须大于零");
    let mut cursor = None;
    let mut report = GcResult {
        dry_run,
        unreferenced_objects: 0,
        reclaimable_bytes: 0,
        removed_objects: 0,
        reclaimed_bytes: 0,
    };

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

    use crate::local::model::Chunk;
    use anyhow::{ensure, Result};

    fn collect_chunk(chunk: &Chunk, referenced: &mut HashMap<String, u64>) -> Result<()> {
        if let Some(previous) = referenced.insert(chunk.hash.clone(), chunk.size) {
            ensure!(previous == chunk.size, "conflicting Chunk size");
        }
        Ok(())
    }

    #[test]
    fn mark_keeps_a_single_size_for_deduplicated_chunks() {
        let mut referenced = HashMap::new();
        let chunk = Chunk {
            hash: "a".repeat(64),
            offset: 0,
            size: 1,
        };
        assert!(collect_chunk(&chunk, &mut referenced).is_ok());
        assert!(collect_chunk(&chunk, &mut referenced).is_ok());
        assert_eq!(referenced.len(), 1);
    }
}
