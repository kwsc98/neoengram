use std::{
    fs::{self, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::PathBuf,
};

use anyhow::{bail, ensure, Context, Result};
use fs2::FileExt;
use serde::Serialize;

use crate::local::fs::durable::{json_bytes, sync_parent};

use super::Repository;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

const WRITE_LOCK_FILE_NAME: &str = "write.lock";
const OBJECT_LOCK_FILE_NAME: &str = "objects.lock";
const WORKTREE_LOCK_FILE_NAME: &str = "worktree.lock";
const LOCK_FORMAT_VERSION: u32 = 1;

// Repository commands must acquire nested locks in this order:
// objects.lock -> worktree.lock -> write.lock.
// Keeping the order here, next to every acquisition entry point, makes it possible for callers
// to hold a long-lived object/worktree lock while taking the state lock only for a short CAS.

/// 写锁文件只用于诊断，互斥语义由操作系统 advisory lock 提供。
///
/// 因此进程被 kill 或机器重启后，锁会由内核自动释放；持久存在的文件不再等同于活锁。
#[derive(Debug, Serialize)]
struct RepositoryLockInfo<'a> {
    format_version: u32,
    pid: u32,
    operation: &'a str,
    checkout_transaction: Option<&'a str>,
}

pub(crate) struct RepositoryWriteLock {
    file: fs::File,
    path: PathBuf,
}

/// A shared worktree guard. Shared holders deliberately never rewrite the lock file: doing so
/// would race with other readers and make the diagnostic payload unreliable.
pub(crate) struct RepositoryWorktreeReadLock {
    file: fs::File,
}

impl Repository {
    /// 获取普通仓库写锁。
    ///
    /// 所有会改变 index、ref 或工作区的普通命令都必须走这个入口。存在未完成的 Checkout
    /// 时会统一拒绝操作，避免 add/commit 绕过恢复流程继续写元数据。
    pub(crate) fn acquire_write_lock(&self) -> Result<RepositoryWriteLock> {
        let lock = self.acquire_exclusive_lock(WRITE_LOCK_FILE_NAME, "repository-write")?;
        self.ensure_no_unfinished_transactions()?;
        Ok(lock)
    }

    /// Serialize object publication with garbage collection.  The ordinary write lock is
    /// intentionally acquired only for the short index/ref commit phase of `add`; this second
    /// advisory lock covers the longer chunking phase so GC cannot delete an object just before
    /// the index starts referring to it.
    pub(crate) fn acquire_object_lock(&self) -> Result<RepositoryWriteLock> {
        self.acquire_exclusive_lock(OBJECT_LOCK_FILE_NAME, "object-publish")
    }

    /// Hold a stable worktree view for add/status/diff. This is the second lock in the fixed
    /// objects -> worktree -> write order. Readers fail fast when an exclusive worktree command
    /// is active and never alter the diagnostic contents of `worktree.lock`.
    pub(crate) fn acquire_worktree_shared_lock(&self) -> Result<RepositoryWorktreeReadLock> {
        let lock = self.acquire_shared_lock(WORKTREE_LOCK_FILE_NAME)?;
        self.ensure_no_unfinished_transactions()?;
        Ok(lock)
    }

    /// Acquire the exclusive worktree lock used by checkout/rm/worktree restore. Callers that
    /// also need the state lock must acquire it only after this guard.
    pub(crate) fn acquire_worktree_lock(&self) -> Result<RepositoryWriteLock> {
        let lock = self.acquire_exclusive_lock(WORKTREE_LOCK_FILE_NAME, "worktree-write")?;
        self.cleanup_transaction_drafts()?;
        self.ensure_no_unfinished_transactions()?;
        Ok(lock)
    }

    /// Recovery must be able to lock a repository that already has a published transaction.
    pub(crate) fn acquire_worktree_recovery_lock(&self) -> Result<RepositoryWriteLock> {
        let lock = self.acquire_exclusive_lock(WORKTREE_LOCK_FILE_NAME, "worktree-recover")?;
        self.cleanup_transaction_drafts()?;
        Ok(lock)
    }

    /// 恢复命令必须能在事务目录存在时取得锁，因此使用独立入口。
    pub(crate) fn acquire_recovery_lock(&self) -> Result<RepositoryWriteLock> {
        self.acquire_exclusive_lock(WRITE_LOCK_FILE_NAME, "checkout-recover")
    }

    fn acquire_exclusive_lock(
        &self,
        file_name: &str,
        operation: &str,
    ) -> Result<RepositoryWriteLock> {
        let directory = if file_name == WORKTREE_LOCK_FILE_NAME {
            self.workspace_locks_dir()
        } else {
            self.metadata_dir()
        };
        let (file, path, existed) = self.open_lock_file(&directory, file_name)?;
        if let Err(error) = FileExt::try_lock_exclusive(&file) {
            let diagnostic = lock_diagnostic(&path);
            bail!(
                "另一个 NeoEngram 操作仍持有排他锁（{}；锁文件 {}）：{error}",
                diagnostic.trim(),
                path.display()
            );
        }
        let mut guard = RepositoryWriteLock { file, path };
        guard.set_operation(operation, None)?;
        if !existed {
            sync_parent(&guard.path)?;
        }
        Ok(guard)
    }

    fn acquire_shared_lock(&self, file_name: &str) -> Result<RepositoryWorktreeReadLock> {
        let (file, path, existed) = self.open_lock_file(&self.workspace_locks_dir(), file_name)?;
        if let Err(error) = FileExt::try_lock_shared(&file) {
            let diagnostic = lock_diagnostic(&path);
            bail!(
                "另一个 NeoEngram 工作区操作仍在执行（{}；锁文件 {}）：{error}",
                diagnostic.trim(),
                path.display()
            );
        }
        if !existed {
            sync_parent(&path)?;
        }
        Ok(RepositoryWorktreeReadLock { file })
    }

    fn open_lock_file(
        &self,
        directory: &std::path::Path,
        file_name: &str,
    ) -> Result<(fs::File, PathBuf, bool)> {
        let path = directory.join(file_name);
        let existed = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "仓库写锁路径不是普通文件: {}",
                    path.display()
                );
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| format!("无法检查仓库写锁: {}", path.display()));
            }
        };

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options
            .open(&path)
            .with_context(|| format!("无法打开仓库写锁: {}", path.display()))?;
        let opened_metadata = file
            .metadata()
            .with_context(|| format!("无法检查已打开的仓库写锁: {}", path.display()))?;
        let path_metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("无法再次检查仓库写锁: {}", path.display()))?;
        ensure!(
            opened_metadata.is_file()
                && path_metadata.is_file()
                && !path_metadata.file_type().is_symlink(),
            "仓库写锁路径不是普通文件: {}",
            path.display()
        );
        #[cfg(unix)]
        ensure!(
            opened_metadata.dev() == path_metadata.dev()
                && opened_metadata.ino() == path_metadata.ino(),
            "仓库写锁在打开期间被替换: {}",
            path.display()
        );
        Ok((file, path, existed))
    }
}

fn lock_diagnostic(path: &std::path::Path) -> String {
    fs::read_to_string(path)
        .ok()
        .filter(|contents| !contents.trim().is_empty())
        .unwrap_or_else(|| "锁持有者信息不可用".to_owned())
}

impl RepositoryWriteLock {
    pub(crate) fn set_operation(
        &mut self,
        operation: &str,
        checkout_transaction: Option<&str>,
    ) -> Result<()> {
        let bytes = json_bytes(&RepositoryLockInfo {
            format_version: LOCK_FORMAT_VERSION,
            pid: std::process::id(),
            operation,
            checkout_transaction,
        })?;
        self.file.set_len(0).context("无法清空仓库写锁信息")?;
        self.file
            .seek(SeekFrom::Start(0))
            .context("无法定位仓库写锁信息")?;
        self.file
            .write_all(&bytes)
            .context("无法写入仓库写锁信息")?;
        self.file.sync_all().context("无法同步仓库写锁信息")
    }
}

impl Drop for RepositoryWriteLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

impl Drop for RepositoryWorktreeReadLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::Repository;

    #[test]
    fn worktree_lock_allows_readers_and_excludes_writers() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let repository = Repository::at(temporary.path().to_path_buf())?;
        repository.initialize_layout()?;

        let first_reader = repository.acquire_worktree_shared_lock()?;
        let second_repository = Repository::discover(temporary.path())?;
        let second_reader = second_repository.acquire_worktree_shared_lock()?;
        assert!(repository.acquire_worktree_lock().is_err());

        drop(second_reader);
        drop(first_reader);
        let writer = repository.acquire_worktree_lock()?;
        assert!(second_repository.acquire_worktree_shared_lock().is_err());
        drop(writer);

        repository.acquire_worktree_shared_lock()?;
        Ok(())
    }
}
