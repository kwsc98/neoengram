use std::{
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use neoengram_engine::{JournalStore, ProgressSink};
use neoengram_fs::FileJournalStore;

use crate::local::{
    engine_mutation,
    repository::Repository,
    worktree::{self, RecoveryReport, RemovalRecovery, RestoreRecovery},
};
use crate::{CheckoutRecoveryStatus, MutationRecoveryStatus, RecoverResult, RecoveryOutcome};

pub(crate) async fn execute(
    cwd: std::path::PathBuf,
    abort: bool,
    progress: Arc<dyn ProgressSink>,
) -> Result<RecoverResult> {
    let repository = Repository::discover(&cwd)?;
    let outcome = tokio::task::spawn_blocking(move || {
        recover_with_engine(&repository, abort, progress.as_ref())
    })
    .await
    .context("Recover 任务异常终止")??;

    let outcome = match outcome {
        RecoveryReport::Nothing => RecoveryOutcome::Nothing,
        RecoveryReport::Checkout { target_id, aborted } => RecoveryOutcome::Checkout {
            target: target_id
                .parse()
                .context("Recover 返回了无效 Checkout Commit ID")?,
            status: if aborted {
                CheckoutRecoveryStatus::Aborted
            } else {
                CheckoutRecoveryStatus::Recovered
            },
        },
        RecoveryReport::Removal { id, outcome } => RecoveryOutcome::Removal {
            transaction_id: id,
            status: match outcome {
                RemovalRecovery::Completed => MutationRecoveryStatus::Completed,
                RemovalRecovery::RolledBack => MutationRecoveryStatus::RolledBack,
            },
        },
        RecoveryReport::Restore { id, outcome } => RecoveryOutcome::Restore {
            transaction_id: id,
            status: match outcome {
                RestoreRecovery::Completed => MutationRecoveryStatus::Completed,
                RestoreRecovery::RolledBack => MutationRecoveryStatus::RolledBack,
            },
        },
    };
    Ok(RecoverResult { outcome })
}

fn recover_with_engine(
    repository: &Repository,
    abort: bool,
    progress: &dyn ProgressSink,
) -> Result<RecoveryReport> {
    // Keep the original repository locks through Engine receipt finalization and stale-journal
    // cleanup. This prevents two recover commands from retiring each other's recovery records.
    let _worktree_lock = repository.acquire_worktree_recovery_lock()?;
    let mut state_lock = repository.acquire_recovery_lock()?;
    let transactions = repository.unfinished_transactions()?;
    if transactions.is_empty() {
        cleanup_recovery_journals(repository)?;
        return worktree::recover_repository_locked(repository, abort, &mut state_lock, progress);
    }
    let transaction_name = transactions[0]
        .file_name()
        .and_then(|name| name.to_str())
        .context("恢复事务目录名不是有效 UTF-8")?;
    let operation = std::fs::read_to_string(transactions[0].join("OPERATION"))
        .context("无法读取恢复事务类型")?;
    if !abort && operation.trim() == "checkout" {
        // Checkout replay owns an inner Engine mutation because it must return a Worktree receipt
        // before publishing Index/HEAD. A second outer mutation would re-enter the same fencing
        // lock and adds no ordering guarantee.
        let outcome =
            worktree::recover_repository_locked(repository, false, &mut state_lock, progress)?;
        cleanup_recovery_journals(repository)?;
        return Ok(outcome);
    }
    let plan = engine_mutation::build_plan(
        repository,
        format!("recover-{transaction_name}-{}", next_recovery_attempt()),
        repository.current_index_version()?,
        Vec::new(),
    )?;
    let journal_directory = repository
        .workspace_admin_dir()
        .join("engine-recovery-journals");
    let mut executed = engine_mutation::execute(
        repository,
        plan,
        &journal_directory,
        format!("recover:{transaction_name}:abort={abort}").into_bytes(),
        progress,
        |_| worktree::recover_repository_locked(repository, abort, &mut state_lock, progress),
    )?;
    let outcome = executed.take_value();
    executed.finalize_and_remove()?;
    cleanup_recovery_journals(repository)?;
    Ok(outcome)
}

fn cleanup_recovery_journals(repository: &Repository) -> Result<()> {
    let directory = repository
        .workspace_admin_dir()
        .join("engine-recovery-journals");
    let journal = FileJournalStore::open_or_create(&directory)?;
    for record in journal.list_all()? {
        journal.remove(&record.id)?;
    }
    for entry in std::fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_string_lossy().ends_with(".journal.lock") {
            continue;
        }
        let metadata = std::fs::symlink_metadata(entry.path())?;
        anyhow::ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "Engine recovery journal lock 不是普通文件: {}",
            entry.path().display()
        );
        std::fs::remove_file(entry.path())?;
    }
    crate::local::fs::durable::sync_directory(&directory)?;
    Ok(())
}

fn next_recovery_attempt() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    format!(
        "{}-{millis}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use neoengram_core::ContentDigest;
    use neoengram_engine::{JournalId, JournalPhase, JournalRecord, MutationIdentity};

    use super::*;

    #[test]
    fn cleanup_retires_active_finalized_and_lock_files() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let repository = Repository::at(temporary.path().to_path_buf())?;
        repository.initialize_layout()?;
        let directory = repository
            .workspace_admin_dir()
            .join("engine-recovery-journals");
        let journals = FileJournalStore::open_or_create(&directory)?;
        for (name, phase) in [
            ("active-recovery", JournalPhase::RecoveryRequired),
            ("finalized-recovery", JournalPhase::Finalized),
        ] {
            journals.create(&JournalRecord {
                id: JournalId::new(name)?,
                revision: 0,
                phase,
                identity: MutationIdentity {
                    tenant_id: "standalone".to_owned(),
                    artifact_id: "repository".to_owned(),
                    playground_id: "workspace".to_owned(),
                    job_id: name.to_owned(),
                    storage_volume_id: "filesystem".to_owned(),
                    owner_generation: 1,
                    fencing_token: 1,
                },
                plan_id: name.to_owned(),
                plan_digest: ContentDigest::hash(name),
                recovery_payload: Vec::new(),
                updated_at_unix_ms: 1,
            })?;
        }

        cleanup_recovery_journals(&repository)?;
        anyhow::ensure!(journals.list_all()?.is_empty());
        anyhow::ensure!(std::fs::read_dir(directory)?.next().is_none());
        Ok(())
    }
}
