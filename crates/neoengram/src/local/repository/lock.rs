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
const LOCK_FORMAT_VERSION: u32 = 1;

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

impl Repository {
    /// 获取普通仓库写锁。
    ///
    /// 所有会改变 index、ref 或工作区的普通命令都必须走这个入口。存在未完成的 Checkout
    /// 时会统一拒绝操作，避免 add/commit 绕过恢复流程继续写元数据。
    pub(crate) fn acquire_write_lock(&self) -> Result<RepositoryWriteLock> {
        let lock = self.acquire_lock(WRITE_LOCK_FILE_NAME, "repository-write")?;
        self.ensure_no_unfinished_transactions()?;
        Ok(lock)
    }

    /// Serialize object publication with garbage collection.  The ordinary write lock is
    /// intentionally acquired only for the short index/ref commit phase of `add`; this second
    /// advisory lock covers the longer chunking phase so GC cannot delete an object just before
    /// the index starts referring to it.
    pub(crate) fn acquire_object_lock(&self) -> Result<RepositoryWriteLock> {
        self.acquire_lock(OBJECT_LOCK_FILE_NAME, "object-publish")
    }

    /// 恢复命令必须能在事务目录存在时取得锁，因此使用独立入口。
    pub(crate) fn acquire_recovery_lock(&self) -> Result<RepositoryWriteLock> {
        self.acquire_lock(WRITE_LOCK_FILE_NAME, "checkout-recover")
    }

    fn acquire_lock(&self, file_name: &str, operation: &str) -> Result<RepositoryWriteLock> {
        let path = self.metadata_dir().join(file_name);
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
        if let Err(error) = FileExt::try_lock_exclusive(&file) {
            let owner = fs::read_to_string(&path)
                .ok()
                .filter(|contents| !contents.trim().is_empty())
                .unwrap_or_else(|| "锁持有者信息不可用".to_owned());
            bail!(
                "另一个 NeoEngram 写操作仍在执行（{}；锁文件 {}）：{error}",
                owner.trim(),
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
