//! 本地存储初始化、配置文件与工作区恢复流程共用的 crash-safe 文件原语。

use std::{
    ffi::OsStr,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use tempfile::Builder;

use crate::local::STORAGE_TEMP_FILE_PREFIX;

pub(crate) const TRANSACTION_DRAFT_PREFIX: &str = ".neoengram-tmp-";
pub(crate) const CHECKOUT_TRANSACTION_DRAFT_PREFIX: &str = ".neoengram-tmp-checkout-";
pub(crate) const RM_TRANSACTION_DRAFT_PREFIX: &str = ".neoengram-tmp-rm-";
const DIRECTORY_CLEANUP_DRAFT_PREFIX: &str = ".neoengram-tmp-cleanup-";

pub(crate) fn ensure_or_create_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "仓库内部路径不是普通目录: {}",
                path.display()
            );
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path)
                .with_context(|| format!("无法创建仓库目录: {}", path.display()))?;
            sync_parent(path)
        }
        Err(error) => Err(error).with_context(|| format!("无法检查仓库目录: {}", path.display())),
    }
}

pub(crate) fn create_dir_all_durable(path: &Path) -> Result<()> {
    let mut missing = Vec::<PathBuf>::new();
    let mut current = path.to_path_buf();
    loop {
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "仓库目录的祖先不是普通目录: {}",
                    current.display()
                );
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current.clone());
                current = current
                    .parent()
                    .with_context(|| format!("仓库路径没有现有祖先: {}", path.display()))?
                    .to_path_buf();
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查仓库目录祖先: {}", current.display()));
            }
        }
    }

    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&directory)
                    .with_context(|| format!("无法检查并发创建的目录: {}", directory.display()))?;
                ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "并发创建的仓库路径不是普通目录: {}",
                    directory.display()
                );
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法创建仓库目录: {}", directory.display()));
            }
        }
        sync_parent(&directory)?;
    }

    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("无法检查新建仓库目录: {}", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "初始化路径不是普通目录: {}",
        path.display()
    );
    Ok(())
}

pub(crate) fn remove_dir_all_durable(path: &Path) -> Result<()> {
    remove_dir_all_durable_with(path, |retired| fs::remove_dir_all(retired))
}

fn remove_dir_all_durable_with<R>(path: &Path, remove: R) -> Result<()>
where
    R: FnOnce(&Path) -> io::Result<()>,
{
    let parent = path
        .parent()
        .with_context(|| format!("待删除目录没有父级: {}", path.display()))?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("无法检查待删除目录: {}", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "待删除路径不是普通目录: {}",
        path.display()
    );

    // Recursive removal can fail after deleting the journal that makes a formal transaction
    // recoverable. Retire the complete directory under an ignored draft name first, and make that
    // rename durable. A partial cleanup then leaves only a retryable draft, never a damaged formal
    // transaction that blocks the repository forever.
    let prefix = directory_cleanup_prefix(path);
    let reservation = Builder::new()
        .prefix(prefix)
        .tempdir_in(parent)
        .with_context(|| format!("无法保留目录清理名称: {}", parent.display()))?;
    let retired = reservation.path().to_path_buf();
    reservation
        .close()
        .with_context(|| format!("无法释放目录清理名称: {}", retired.display()))?;
    rename_noreplace(path, &retired).with_context(|| {
        format!(
            "无法把待删除目录转为清理 draft: {} -> {}",
            path.display(),
            retired.display()
        )
    })?;
    sync_directory(parent)?;

    remove(&retired).with_context(|| format!("无法删除目录: {}", retired.display()))?;
    sync_directory(parent)
}

fn directory_cleanup_prefix(path: &Path) -> &'static str {
    let name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
    if name.starts_with("checkout-") || name.starts_with(CHECKOUT_TRANSACTION_DRAFT_PREFIX) {
        CHECKOUT_TRANSACTION_DRAFT_PREFIX
    } else if name.starts_with("rm-") || name.starts_with(RM_TRANSACTION_DRAFT_PREFIX) {
        RM_TRANSACTION_DRAFT_PREFIX
    } else {
        DIRECTORY_CLEANUP_DRAFT_PREFIX
    }
}

pub(crate) fn is_transaction_draft_name(name: &OsStr) -> Result<bool> {
    let Some(name) = name.to_str() else {
        return Ok(false);
    };
    if !name.starts_with(TRANSACTION_DRAFT_PREFIX) {
        return Ok(false);
    }
    let suffix = name
        .strip_prefix(CHECKOUT_TRANSACTION_DRAFT_PREFIX)
        .or_else(|| name.strip_prefix(RM_TRANSACTION_DRAFT_PREFIX));
    ensure!(
        suffix.is_some_and(|suffix| !suffix.is_empty()),
        "事务 draft 使用了未知或无效的保留名称: {name}"
    );
    Ok(true)
}

pub(crate) fn publish_transaction_draft(draft: &Path, operation: &str) -> Result<PathBuf> {
    publish_transaction_draft_observed(draft, operation, |_| {})
}

pub(crate) fn publish_transaction_draft_observed<F>(
    draft: &Path,
    operation: &str,
    on_published: F,
) -> Result<PathBuf>
where
    F: FnOnce(&Path),
{
    let expected_prefix = match operation {
        "checkout" => CHECKOUT_TRANSACTION_DRAFT_PREFIX,
        "rm" => RM_TRANSACTION_DRAFT_PREFIX,
        _ => anyhow::bail!("不支持的事务 draft 类型: {operation}"),
    };
    let parent = draft
        .parent()
        .with_context(|| format!("事务 draft 没有父目录: {}", draft.display()))?;
    let draft_name = draft
        .file_name()
        .and_then(OsStr::to_str)
        .with_context(|| format!("事务 draft 名称不是有效 UTF-8: {}", draft.display()))?;
    let suffix = draft_name
        .strip_prefix(expected_prefix)
        .with_context(|| format!("事务 draft 名称不匹配 {operation}: {}", draft.display()))?;
    ensure!(!suffix.is_empty(), "事务 draft 名称缺少唯一后缀");
    let destination = parent.join(format!("{operation}-{suffix}"));

    sync_directory(draft)?;
    rename_noreplace(draft, &destination)?;
    on_published(&destination);
    sync_directory(parent)?;
    Ok(destination)
}

pub(crate) fn ensure_directory(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!(
            "NeoEngram {kind}目录不存在，请重新运行 `neoengram init`: {}",
            path.display()
        )
    })?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "NeoEngram {kind}路径不是普通目录: {}",
        path.display()
    );
    Ok(())
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let contents = fs::read(path).with_context(|| format!("无法读取 JSON: {}", path.display()))?;
    serde_json::from_slice(&contents).with_context(|| format!("JSON 格式无效: {}", path.display()))
}

pub(crate) fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value).context("无法序列化 JSON")?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn publish_json_if_absent<T: Serialize>(path: &Path, value: &T) -> Result<bool> {
    publish_bytes_if_absent(path, &json_bytes(value)?)
}

pub(crate) fn publish_bytes_if_absent(path: &Path, bytes: &[u8]) -> Result<bool> {
    let parent = path
        .parent()
        .with_context(|| format!("目标路径没有父目录: {}", path.display()))?;
    let mut temporary = Builder::new()
        .prefix(STORAGE_TEMP_FILE_PREFIX)
        .tempfile_in(parent)
        .with_context(|| format!("无法创建临时文件: {}", parent.display()))?;
    temporary
        .write_all(bytes)
        .with_context(|| format!("无法写入临时文件: {}", path.display()))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("无法同步临时文件: {}", path.display()))?;

    let (temporary_file, temporary_path) = temporary
        .keep()
        .map_err(|error| error.error)
        .with_context(|| format!("无法保留待发布临时文件: {}", path.display()))?;
    let publication = rename_noreplace_durable(&temporary_path, path);
    drop(temporary_file);
    match publication {
        Ok(()) => Ok(true),
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::AlreadyExists) =>
        {
            let _ = fs::remove_file(&temporary_path);
            Ok(false)
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary_path);
            Err(error).with_context(|| format!("无法发布文件: {}", path.display()))
        }
    }
}

pub(crate) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_bytes_atomic(path, &json_bytes(value)?)
}

pub(crate) fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("目标路径没有父目录: {}", path.display()))?;
    let mut temporary = Builder::new()
        .prefix(STORAGE_TEMP_FILE_PREFIX)
        .tempfile_in(parent)
        .with_context(|| format!("无法创建临时文件: {}", parent.display()))?;
    temporary
        .write_all(bytes)
        .with_context(|| format!("无法写入临时文件: {}", path.display()))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("无法同步临时文件: {}", path.display()))?;
    let (temporary_file, temporary_path) = temporary
        .keep()
        .map_err(|error| error.error)
        .with_context(|| format!("无法保留待替换临时文件: {}", path.display()))?;
    let publication = replace_durable(&temporary_path, path);
    drop(temporary_file);
    if publication.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    publication
}

pub(crate) fn rename_durable_observed<F>(
    source: &Path,
    destination: &Path,
    on_renamed: F,
) -> Result<()>
where
    F: FnOnce(),
{
    rename_durable_observed_with_sync(source, destination, on_renamed, sync_directory)
}

fn rename_durable_observed_with_sync<F, S>(
    source: &Path,
    destination: &Path,
    on_renamed: F,
    mut sync: S,
) -> Result<()>
where
    F: FnOnce(),
    S: FnMut(&Path) -> Result<()>,
{
    let source_parent = source
        .parent()
        .with_context(|| format!("源路径没有父目录: {}", source.display()))?;
    let destination_parent = destination
        .parent()
        .with_context(|| format!("目标路径没有父目录: {}", destination.display()))?;
    rename_noreplace(source, destination).with_context(|| {
        format!(
            "无法重命名 {} -> {}",
            source.display(),
            destination.display()
        )
    })?;
    on_renamed();
    if cfg!(debug_assertions)
        && std::env::var("NEOENGRAM_TEST_FAIL_AFTER_RENAME")
            .is_ok_and(|configured| configured == "before-sync")
    {
        anyhow::bail!("injected failure after rename and before directory sync");
    }
    if source_parent == destination_parent {
        sync(destination_parent)?;
    } else {
        // Persist the new name before the old name's removal to avoid a no-name crash window.
        sync(destination_parent)?;
        sync(source_parent)?;
    }
    Ok(())
}

/// Atomically replace a regular-file destination with a fully synced temporary file.
///
/// Unlike `rename_noreplace_durable`, replacement is intentional here: callers have already
/// checked the destination type and verified the source contents.  Keeping the replacement in a
/// single filesystem rename avoids the data-loss window caused by remove-then-rename.
pub(crate) fn replace_durable(source: &Path, destination: &Path) -> Result<()> {
    let source_parent = source
        .parent()
        .with_context(|| format!("源路径没有父目录: {}", source.display()))?;
    let destination_parent = destination
        .parent()
        .with_context(|| format!("目标路径没有父目录: {}", destination.display()))?;

    replace_path(source, destination)?;
    if source_parent == destination_parent {
        sync_directory(destination_parent)?;
    } else {
        sync_directory(destination_parent)?;
        sync_directory(source_parent)?;
    }
    Ok(())
}

#[cfg(any(
    target_vendor = "apple",
    target_os = "linux",
    target_os = "android",
    target_os = "redox"
))]
fn replace_path(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination).with_context(|| {
        format!(
            "无法原子替换 {} -> {}",
            source.display(),
            destination.display()
        )
    })
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn replace_path(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let source_wide = source
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let destination_wide = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    ensure!(
        !source_wide[..source_wide.len() - 1].contains(&0)
            && !destination_wide[..destination_wide.len() - 1].contains(&0),
        "替换路径包含 NUL"
    );
    let moved = unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            destination_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    ensure!(
        moved != 0,
        "无法原子替换 {} -> {}: {}",
        source.display(),
        destination.display(),
        io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(not(any(
    target_vendor = "apple",
    target_os = "linux",
    target_os = "android",
    target_os = "redox",
    windows
)))]
fn replace_path(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination).with_context(|| {
        format!(
            "无法原子替换 {} -> {}",
            source.display(),
            destination.display()
        )
    })
}

pub(crate) fn rename_noreplace_durable(source: &Path, destination: &Path) -> Result<()> {
    let source_parent = source
        .parent()
        .with_context(|| format!("源路径没有父目录: {}", source.display()))?;
    let destination_parent = destination
        .parent()
        .with_context(|| format!("目标路径没有父目录: {}", destination.display()))?;

    rename_noreplace(source, destination)?;

    if source_parent == destination_parent {
        sync_directory(destination_parent)?;
    } else {
        sync_directory(destination_parent)?;
        sync_directory(source_parent)?;
    }
    Ok(())
}

#[cfg(any(
    target_vendor = "apple",
    target_os = "linux",
    target_os = "android",
    target_os = "redox"
))]
fn rename_noreplace(source: &Path, destination: &Path) -> Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(io::Error::from)
    .with_context(|| {
        format!(
            "无法以 no-replace 方式重命名 {} -> {}",
            source.display(),
            destination.display()
        )
    })
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn rename_noreplace(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

    let source_wide = source
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let destination_wide = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    ensure!(
        !source_wide[..source_wide.len() - 1].contains(&0)
            && !destination_wide[..destination_wide.len() - 1].contains(&0),
        "重命名路径包含 NUL"
    );

    // 不包含 MOVEFILE_REPLACE_EXISTING，目标存在时 Windows 会原子失败。WRITE_THROUGH 让
    // transaction publish 和工作区 rename 在返回前跨过 Windows 的持久化屏障。
    // SAFETY: 两个缓冲区都以 NUL 结尾、已拒绝内嵌 NUL，并在只读调用期间保持存活。
    let moved = unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            destination_wide.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "无法以 no-replace 方式重命名 {} -> {}",
                source.display(),
                destination.display()
            )
        });
    }
    Ok(())
}

#[cfg(not(any(
    target_vendor = "apple",
    target_os = "linux",
    target_os = "android",
    target_os = "redox",
    windows
)))]
fn rename_noreplace(_source: &Path, _destination: &Path) -> Result<()> {
    anyhow::bail!("当前平台不支持原子 no-replace rename，无法安全发布新仓库")
}

pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("路径没有父目录: {}", path.display()))?;
    sync_directory(parent)
}

/// Unix 需要显式同步目录项，才能让 rename/create 在掉电后持久化。Windows 的 Rust
/// 标准库不能以可移植方式打开目录句柄；该平台上的文件 rename 已由系统 API 提交。
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)
            .with_context(|| format!("无法打开目录进行同步: {}", path.display()))?
            .sync_all()
            .with_context(|| format!("无法同步目录: {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, fs};

    use anyhow::Result;

    use super::{
        is_transaction_draft_name, publish_bytes_if_absent, remove_dir_all_durable,
        remove_dir_all_durable_with, rename_durable_observed_with_sync, rename_noreplace_durable,
        replace_durable, write_bytes_atomic,
    };

    #[test]
    fn atomic_file_publication_preserves_noclobber_and_replaces_intentionally() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("state");

        assert!(publish_bytes_if_absent(&destination, b"first")?);
        assert!(!publish_bytes_if_absent(&destination, b"ignored")?);
        assert_eq!(fs::read(&destination)?, b"first");

        write_bytes_atomic(&destination, b"second")?;
        assert_eq!(fs::read(&destination)?, b"second");
        Ok(())
    }

    #[test]
    fn failed_recursive_cleanup_leaves_an_ignored_retryable_draft() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let transaction = temporary.path().join("checkout-test");
        fs::create_dir(&transaction)?;
        fs::write(transaction.join("OPERATION"), b"checkout\n")?;
        fs::write(transaction.join("journal.json"), b"{}\n")?;

        let result = remove_dir_all_durable_with(&transaction, |retired| {
            fs::remove_file(retired.join("OPERATION"))?;
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected partial recursive cleanup",
            ))
        });
        assert!(result.is_err());
        assert!(!transaction.exists());

        let entries = fs::read_dir(temporary.path())?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(entries.len(), 1);
        let retired = entries[0].path();
        assert!(is_transaction_draft_name(&entries[0].file_name())?);
        assert_eq!(fs::read(retired.join("journal.json"))?, b"{}\n");

        remove_dir_all_durable(&retired)?;
        assert_eq!(fs::read_dir(temporary.path())?.count(), 0);
        Ok(())
    }

    #[test]
    fn rename_observer_runs_after_rename_and_before_sync_error() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let destination = temporary.path().join("destination");
        fs::write(&source, b"payload")?;
        let observed = Cell::new(false);

        let result = rename_durable_observed_with_sync(
            &source,
            &destination,
            || {
                assert!(!source.exists());
                assert_eq!(fs::read(&destination).expect("renamed payload"), b"payload");
                observed.set(true);
            },
            |_| {
                assert!(observed.get());
                anyhow::bail!("injected directory sync failure")
            },
        );

        assert!(result.is_err());
        assert!(observed.get());
        assert!(!source.exists());
        assert_eq!(fs::read(&destination)?, b"payload");
        Ok(())
    }

    #[test]
    fn noreplace_rename_preserves_an_existing_directory() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let destination = temporary.path().join("destination");
        fs::create_dir(&source)?;
        fs::create_dir(&destination)?;
        fs::write(source.join("source.txt"), b"source")?;

        assert!(rename_noreplace_durable(&source, &destination).is_err());
        assert_eq!(fs::read(source.join("source.txt"))?, b"source");
        assert_eq!(fs::read_dir(&destination)?.count(), 0);
        Ok(())
    }

    #[test]
    fn replace_durable_replaces_an_existing_regular_file() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let destination = temporary.path().join("destination");
        fs::write(&source, b"new")?;
        fs::write(&destination, b"old")?;

        replace_durable(&source, &destination)?;
        assert_eq!(fs::read(&destination)?, b"new");
        assert!(!source.exists());
        Ok(())
    }
}
