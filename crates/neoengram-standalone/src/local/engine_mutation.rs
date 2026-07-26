use std::{
    fmt,
    io::Read,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result};
use neoengram_core::{ContentDigest, IndexVersion, LogicalPath};
use neoengram_engine::{
    execute_mutation, finalize_mutation, EngineError, EngineResult, ErrorCode,
    ExecuteMutationRequest, FailureInjector, FinalizeMutationRequest, ImmutableCatalogReader,
    JournalStore, MutationAction, MutationApplication, MutationIdentity, MutationPlan, ObjectStore,
    ProgressEvent, ProgressPhase, ProgressSink, ProgressUnit, Worktree, WorktreeEntry,
    WorktreeScanRequest,
};
use neoengram_fs::{
    FileJournalStore, LocalLockManager, MemoryCatalog, MemoryObjectStore, SystemClock,
};

use super::repository::Repository;

/// Result of applying a Worktree transaction, retained until the caller publishes authority.
pub(crate) struct ExecutedMutation<T> {
    value: Option<T>,
    journal: FileJournalStore,
    journal_id: neoengram_engine::JournalId,
    plan_digest: ContentDigest,
}

impl<T> ExecutedMutation<T> {
    /// Acknowledges the Engine journal only after the caller has published Index/ref authority.
    pub(crate) fn finalize(&self) -> Result<()> {
        finalize_mutation(
            &FinalizeMutationRequest {
                journal_id: self.journal_id.clone(),
                plan_digest: self.plan_digest,
            },
            &self.journal,
            &SystemClock,
        )
        .context("无法完成 Engine 工作区事务 journal")?;
        Ok(())
    }

    /// Finalizes and retires a journal whose lifetime is not owned by an outer transaction.
    pub(crate) fn finalize_and_remove(&self) -> Result<()> {
        self.finalize()?;
        let removed = self
            .journal
            .remove(&self.journal_id)
            .context("无法清理已完成的 Engine 工作区 journal")?;
        anyhow::ensure!(removed, "已完成的 Engine 工作区 journal 不存在");
        Ok(())
    }

    /// Runs the authoritative publisher after receipt creation and finalizes only on success.
    pub(crate) fn publish_and_finalize<R>(
        &mut self,
        publisher: impl FnOnce(&mut T) -> Result<R>,
    ) -> Result<R> {
        let result = publisher(
            self.value
                .as_mut()
                .context("Engine 工作区适配器结果已被取走")?,
        )?;
        self.finalize()?;
        Ok(result)
    }

    pub(crate) fn take_value(&mut self) -> T {
        self.value
            .take()
            .expect("executed mutation retains its adapter result")
    }
}

/// Builds a standalone mutation identity without leaking physical paths into Engine use cases.
pub(crate) fn build_plan(
    repository: &Repository,
    plan_id: impl Into<String>,
    expected_index_version: IndexVersion,
    actions: Vec<MutationAction>,
) -> Result<MutationPlan> {
    let plan_id = plan_id.into();
    let workspace_id = repository
        .workspace_id()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    MutationPlan::new(
        plan_id.clone(),
        MutationIdentity {
            tenant_id: "standalone".to_owned(),
            artifact_id: "local-repository".to_owned(),
            playground_id: workspace_id,
            job_id: plan_id,
            storage_volume_id: "local-filesystem".to_owned(),
            owner_generation: 1,
            fencing_token: 1,
        },
        expected_index_version,
        actions,
    )
    .context("无法构造 Engine 工作区 MutationPlan")
}

/// Executes an operation-wide Standalone transaction inside the Engine journal/lock lifecycle.
pub(crate) fn execute<T, F>(
    repository: &Repository,
    plan: MutationPlan,
    journal_directory: &std::path::Path,
    recovery_payload: Vec<u8>,
    progress: &dyn ProgressSink,
    operation: F,
) -> Result<ExecutedMutation<T>>
where
    T: Send,
    F: FnOnce(&MutationPlan) -> Result<T> + Send,
{
    let journal = FileJournalStore::open_or_create(journal_directory)
        .context("无法打开 Engine 工作区 journal")?;
    let locks = LocalLockManager::open_or_create(repository.workspace_locks_dir())
        .context("无法打开 Engine 工作区锁")?;
    let worktree = TransactionalWorktree::new(operation);
    let result = execute_mutation(
        &ExecuteMutationRequest {
            plan: plan.clone(),
            recovery_payload,
        },
        &worktree,
        &MemoryCatalog::default(),
        &MemoryObjectStore::default(),
        &journal,
        &locks,
        &SystemClock,
        progress,
        &neoengram_engine::NoopFailureInjector,
    )
    .context("Engine 工作区 MutationPlan 执行失败")?;
    let value = worktree
        .take_result()?
        .context("Engine 工作区适配器没有返回执行结果")?;
    Ok(ExecutedMutation {
        value: Some(value),
        journal,
        journal_id: result.receipt.journal_id,
        plan_digest: plan.digest(),
    })
}

struct TransactionalWorktree<F, T> {
    operation: Mutex<Option<F>>,
    result: Mutex<Option<T>>,
}

impl<F, T> TransactionalWorktree<F, T> {
    fn new(operation: F) -> Self {
        Self {
            operation: Mutex::new(Some(operation)),
            result: Mutex::new(None),
        }
    }

    fn take_result(&self) -> Result<Option<T>> {
        Ok(lock(&self.result)?.take())
    }
}

impl<F, T> fmt::Debug for TransactionalWorktree<F, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionalWorktree")
            .finish_non_exhaustive()
    }
}

impl<F, T> Worktree for TransactionalWorktree<F, T>
where
    T: Send,
    F: FnOnce(&MutationPlan) -> Result<T> + Send,
{
    fn apply_mutation(
        &self,
        plan: &MutationPlan,
        _catalog: &dyn ImmutableCatalogReader,
        _objects: &dyn ObjectStore,
        progress: &dyn ProgressSink,
        failures: &dyn FailureInjector,
    ) -> EngineResult<MutationApplication> {
        for _ in &plan.actions {
            failures.check(neoengram_engine::FailurePoint::BeforeMutation)?;
        }
        let operation = self
            .operation
            .lock()
            .map_err(|_| poisoned("standalone mutation operation"))?
            .take()
            .ok_or_else(|| {
                EngineError::new(
                    ErrorCode::Conflict,
                    "standalone mutation operation was already executed",
                )
            })?;
        let value = operation(plan)
            .map_err(|error| EngineError::new(ErrorCode::Io, format!("{error:#}")))?;
        *self
            .result
            .lock()
            .map_err(|_| poisoned("standalone mutation result"))? = Some(value);

        let actions_applied = u64::try_from(plan.actions.len()).map_err(|_| {
            EngineError::new(ErrorCode::Internal, "mutation action count exceeds u64")
        })?;
        let bytes_written = plan.actions.iter().try_fold(0_u64, |total, action| {
            let bytes = match action {
                MutationAction::WriteFile { total_size, .. } => *total_size,
                MutationAction::CreateDirectory { .. } | MutationAction::RemoveFile { .. } => 0,
            };
            total.checked_add(bytes).ok_or_else(|| {
                EngineError::new(ErrorCode::Internal, "mutation byte count exceeds u64")
            })
        })?;
        progress.emit(
            &ProgressEvent::new(
                ProgressPhase::ApplyingMutation,
                ProgressUnit::Actions,
                actions_applied,
            )
            .with_total(actions_applied),
        )?;
        Ok(MutationApplication {
            actions_applied,
            bytes_written,
        })
    }

    fn inspect(&self, _path: &LogicalPath) -> EngineResult<Option<WorktreeEntry>> {
        Err(unsupported_primitive())
    }

    fn scan_files(
        &self,
        _request: &WorktreeScanRequest,
        _visitor: &mut dyn FnMut(WorktreeEntry) -> EngineResult<()>,
    ) -> EngineResult<()> {
        Err(unsupported_primitive())
    }

    fn open_file(&self, _path: &LogicalPath) -> EngineResult<Box<dyn Read + Send>> {
        Err(unsupported_primitive())
    }

    fn create_dir_all(&self, _path: &LogicalPath) -> EngineResult<()> {
        Err(unsupported_primitive())
    }

    fn write_file_atomic(
        &self,
        _path: &LogicalPath,
        _expected_size: u64,
        _source: &mut dyn Read,
    ) -> EngineResult<()> {
        Err(unsupported_primitive())
    }

    fn remove_file(&self, _path: &LogicalPath) -> EngineResult<bool> {
        Err(unsupported_primitive())
    }

    fn rename_no_replace(
        &self,
        _source: &LogicalPath,
        _destination: &LogicalPath,
    ) -> EngineResult<()> {
        Err(unsupported_primitive())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| anyhow::Error::new(poisoned("standalone mutation adapter")))
}

fn poisoned(component: &str) -> EngineError {
    EngineError::new(ErrorCode::Internal, format!("{component} lock is poisoned"))
}

fn unsupported_primitive() -> EngineError {
    EngineError::new(
        ErrorCode::Internal,
        "transactional Worktree primitive cannot be called outside apply_mutation",
    )
}

#[cfg(test)]
mod tests {
    use neoengram_engine::{JournalId, JournalPhase};

    use super::*;
    use crate::local::{model::Index, repository::Repository};

    #[test]
    fn journal_precedes_mutation_and_cas_precedes_finalize() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let repository = Repository::at(temporary.path().to_path_buf())?;
        repository.initialize_layout()?;
        let expected_index_version = repository.current_index_version()?;
        let plan_id = "boundary-order";
        let plan = build_plan(&repository, plan_id, expected_index_version, Vec::new())?;
        let journal_directory = repository
            .workspace_admin_dir()
            .join("boundary-order-journal");
        let observer = FileJournalStore::open_or_create(&journal_directory)?;
        let journal_id = JournalId::new(plan_id)?;

        let callback_observer = observer.clone();
        let callback_id = journal_id.clone();
        let mut executed = execute(
            &repository,
            plan,
            &journal_directory,
            b"boundary-order".to_vec(),
            &neoengram_engine::NoopProgressSink,
            move |_| {
                let record = callback_observer
                    .load(&callback_id)?
                    .context("mutation journal must exist before the Worktree callback")?;
                anyhow::ensure!(record.phase == JournalPhase::Applying);
                Ok(())
            },
        )?;
        anyhow::ensure!(
            observer
                .load(&journal_id)?
                .context("mutation journal disappeared before publication")?
                .phase
                == JournalPhase::AwaitingFinalize
        );

        executed.publish_and_finalize(|()| {
            anyhow::ensure!(
                observer
                    .load(&journal_id)?
                    .context("mutation journal disappeared during publication")?
                    .phase
                    == JournalPhase::AwaitingFinalize
            );
            repository.write_index_if_version(&Index::default(), &expected_index_version)?;
            Ok(())
        })?;
        anyhow::ensure!(
            observer
                .load(&journal_id)?
                .context("mutation journal disappeared after finalization")?
                .phase
                == JournalPhase::Finalized
        );
        Ok(())
    }
}
