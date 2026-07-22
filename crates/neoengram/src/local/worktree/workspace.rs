//! checkout、rm 和 status 共用的工作区路径与内容校验原语。
//!
//! 这些函数只做只读检查。逻辑路径必须已通过仓库路径校验；符号链接祖先在这里统一
//! 处理，避免多份实现漂移出不一致的安全语义。

use std::{
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use neoengram_core::FileNode;

const IO_BUFFER_SIZE: usize = 64 * 1024;

/// 把仓库相对逻辑路径映射到工作区物理路径。
pub(crate) fn workspace_path(repository_root: &Path, logical_path: &str) -> PathBuf {
    logical_join(repository_root, logical_path)
}

/// 按 `/` 分段拼接逻辑路径。不做 `..` 归一化，调用方必须已验证逻辑路径合法。
pub(crate) fn logical_join(base: &Path, logical_path: &str) -> PathBuf {
    let mut path = base.to_path_buf();
    for component in logical_path.split('/') {
        path.push(component);
    }
    path
}

/// 逐组件检查逻辑路径的叶子元数据；任何祖先是符号链接或非目录时直接报错。
///
/// 会修改工作区的 checkout/rm 使用该严格版本：祖先异常说明工作区结构被外部改写，
/// 继续 rename 可能把内容带到仓库之外。
pub(crate) fn safe_leaf_metadata(
    repository_root: &Path,
    logical_path: &str,
) -> Result<Option<fs::Metadata>> {
    leaf_metadata(repository_root, logical_path, true)
}

/// 与 [`safe_leaf_metadata`] 相同的遍历，但祖先异常按“路径不存在”处理。
///
/// 只读的 status 使用该宽松版本：index 中有 `a/b` 而工作区把 `a` 换成普通文件时，
/// `a/b` 应报告为 deleted，`a` 由后续工作区扫描单独报告为 untracked。symlink 祖先
/// 同样在这里停止，绝不会继续跟随到仓库之外。
pub(crate) fn lenient_leaf_metadata(
    repository_root: &Path,
    logical_path: &str,
) -> Result<Option<fs::Metadata>> {
    leaf_metadata(repository_root, logical_path, false)
}

fn leaf_metadata(
    repository_root: &Path,
    logical_path: &str,
    strict: bool,
) -> Result<Option<fs::Metadata>> {
    let components: Vec<&str> = logical_path.split('/').collect();
    let mut current = repository_root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if index + 1 < components.len() => {
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    ensure!(
                        !strict,
                        "工作区路径的父级不是普通目录: {}",
                        current.display()
                    );
                    return Ok(None);
                }
            }
            Ok(metadata) => return Ok(Some(metadata)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查工作区路径: {}", current.display()));
            }
        }
    }
    Ok(None)
}

/// 校验文件与 `FileNode` 完全一致：大小、逐 Chunk BLAKE3 以及没有多余的尾部内容。
///
/// 路径不存在、类型不是普通文件、是符号链接或内容不一致时返回 `Ok(false)`；只有
/// I/O 失败才返回错误。
pub(crate) fn file_matches_node(path: &Path, file_node: &FileNode) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("无法检查文件: {}", path.display()));
        }
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != file_node.total_size
    {
        return Ok(false);
    }

    let mut reader =
        File::open(path).with_context(|| format!("无法打开文件: {}", path.display()))?;
    for chunk in &file_node.chunks {
        let Some(actual_hash) = read_exact_hash(&mut reader, chunk.size)? else {
            return Ok(false);
        };
        if actual_hash != chunk.hash {
            return Ok(false);
        }
    }

    let mut trailing = [0_u8; 1];
    Ok(reader
        .read(&mut trailing)
        .with_context(|| format!("无法检查文件结尾: {}", path.display()))?
        == 0)
}

fn read_exact_hash(reader: &mut File, size: u64) -> Result<Option<String>> {
    let mut remaining = size;
    let mut buffer = [0_u8; IO_BUFFER_SIZE];
    let mut hasher = blake3::Hasher::new();
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(IO_BUFFER_SIZE as u64))
            .context("当前平台无法表示读取缓冲区大小")?;
        let read = reader
            .read(&mut buffer[..limit])
            .context("无法读取文件内容")?;
        if read == 0 {
            return Ok(None);
        }
        hasher.update(&buffer[..read]);
        remaining = remaining
            .checked_sub(u64::try_from(read).context("读取长度超出 u64 范围")?)
            .context("文件读取长度溢出")?;
    }
    Ok(Some(hasher.finalize().to_hex().to_string()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{file_matches_node, lenient_leaf_metadata, safe_leaf_metadata};
    use neoengram_core::{Chunk, ChunkingStrategy, FileNode};

    #[test]
    fn file_ancestor_is_missing_for_lenient_and_an_error_for_strict(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        fs::write(temporary.path().join("a"), b"now a file")?;

        assert!(lenient_leaf_metadata(temporary.path(), "a/b")?.is_none());
        assert!(safe_leaf_metadata(temporary.path(), "a/b").is_err());
        Ok(())
    }

    #[test]
    fn missing_or_mismatched_files_do_not_match_their_node(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("payload");
        let contents = b"payload";
        fs::write(&path, contents)?;
        let node = FileNode {
            path: "payload".to_owned(),
            total_size: contents.len() as u64,
            chunking: ChunkingStrategy::FastCdc,
            chunks: vec![Chunk {
                hash: blake3::hash(contents).to_hex().to_string(),
                offset: 0,
                size: contents.len() as u64,
            }],
        };

        assert!(file_matches_node(&path, &node)?);
        assert!(!file_matches_node(
            &temporary.path().join("missing"),
            &node
        )?);
        fs::write(&path, b"pay1oad")?;
        assert!(!file_matches_node(&path, &node)?);
        Ok(())
    }
}
