//! JSON 元数据与工作区恢复流程共用的 crash-safe 本地文件原语。

use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use anyhow::{ensure, Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use tempfile::Builder;

use super::STORAGE_TEMP_FILE_PREFIX;

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

pub(crate) fn ensure_directory(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!(
            "Synapse {kind}目录不存在，请重新运行 `synapse init`: {}",
            path.display()
        )
    })?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "Synapse {kind}路径不是普通目录: {}",
        path.display()
    );
    Ok(())
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let contents = fs::read(path).with_context(|| format!("无法读取 JSON: {}", path.display()))?;
    serde_json::from_slice(&contents).with_context(|| format!("JSON 格式无效: {}", path.display()))
}

pub(crate) fn read_json_optional<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("无法读取 JSON: {}", path.display()));
        }
    };
    serde_json::from_slice(&contents)
        .map(Some)
        .with_context(|| format!("JSON 格式无效: {}", path.display()))
}

pub(crate) fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value).context("无法序列化 JSON")?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn publish_json_if_absent<T: Serialize>(path: &Path, value: &T) -> Result<bool> {
    publish_bytes_if_absent(path, &json_bytes(value)?)
}

pub(crate) fn publish_immutable_json<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize + DeserializeOwned + PartialEq,
{
    if publish_json_if_absent(path, value)? {
        return Ok(());
    }
    let existing: T = read_json(path)?;
    ensure!(
        &existing == value,
        "不可变 JSON 发生内容冲突或文件已损坏: {}",
        path.display()
    );
    Ok(())
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

    match temporary.persist_noclobber(path) {
        Ok(_) => {
            sync_directory(parent)?;
            Ok(true)
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error.error).with_context(|| format!("无法发布文件: {}", path.display())),
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
    temporary
        .persist(path)
        .map(|_| ())
        .map_err(|error| error.error)
        .with_context(|| format!("无法原子替换文件: {}", path.display()))?;
    sync_directory(parent)
}

pub(crate) fn rename_durable(source: &Path, destination: &Path) -> Result<()> {
    let source_parent = source
        .parent()
        .with_context(|| format!("源路径没有父目录: {}", source.display()))?;
    let destination_parent = destination
        .parent()
        .with_context(|| format!("目标路径没有父目录: {}", destination.display()))?;
    fs::rename(source, destination).with_context(|| {
        format!(
            "无法重命名 {} -> {}",
            source.display(),
            destination.display()
        )
    })?;
    if source_parent == destination_parent {
        sync_directory(destination_parent)?;
    } else {
        // Persist the new name before the old name's removal to avoid a no-name crash window.
        sync_directory(destination_parent)?;
        sync_directory(source_parent)?;
    }
    Ok(())
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
