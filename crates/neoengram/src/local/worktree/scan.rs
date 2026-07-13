use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use walkdir::WalkDir;

use crate::local::repository::is_neoengram_dir_name;

pub(crate) struct InputFile {
    disk_path: PathBuf,
    repository_path: String,
}

impl InputFile {
    pub(crate) fn repository_path(&self) -> &str {
        &self.repository_path
    }

    pub(crate) fn into_parts(self) -> (PathBuf, String) {
        (self.disk_path, self.repository_path)
    }
}

pub(crate) struct FileCollection {
    files: Vec<InputFile>,
    /// 空字符串表示仓库根目录，其余值均为规范的仓库相对路径。
    repository_scope: String,
}

impl FileCollection {
    pub(crate) fn files(&self) -> &[InputFile] {
        &self.files
    }

    pub(crate) fn into_parts(self) -> (Vec<InputFile>, String) {
        (self.files, self.repository_scope)
    }
}

pub(crate) fn validate_input_path(path: &Path) -> Result<()> {
    if contains_repository_component(path) {
        bail!("不能把 NeoEngram 内部目录加入对象库: {}", path.display());
    }
    Ok(())
}

pub(crate) fn collect_files(
    path: &Path,
    current_dir: &Path,
    repository_root: &Path,
    allow_missing: bool,
) -> Result<FileCollection> {
    let requested_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_dir.join(path)
    };
    let metadata = match fs::symlink_metadata(&requested_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_missing => {
            let normalized = normalize_absolute_path(&requested_path)?;
            ensure_missing_path_is_inside_repository(&normalized, repository_root)?;
            return Ok(FileCollection {
                files: Vec::new(),
                repository_scope: repository_path(&normalized, repository_root, true)?,
            });
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法访问路径: {}", requested_path.display()));
        }
    };
    ensure!(
        !metadata.file_type().is_symlink(),
        "add 不跟随符号链接: {}",
        requested_path.display()
    );
    ensure!(
        metadata.is_file() || metadata.is_dir(),
        "路径不是普通文件或目录: {}",
        requested_path.display()
    );

    let canonical_path = fs::canonicalize(&requested_path)
        .with_context(|| format!("无法解析路径: {}", requested_path.display()))?;
    ensure!(
        canonical_path.starts_with(repository_root),
        "不能添加仓库之外的路径: {}",
        requested_path.display()
    );
    let repository_scope = repository_path(&canonical_path, repository_root, true)?;

    let mut files = Vec::new();
    if metadata.is_file() {
        files.push(input_file(canonical_path, repository_root)?);
    } else {
        let entries = WalkDir::new(&canonical_path)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| !is_neoengram_dir_name(entry.file_name()));

        for entry in entries {
            let entry =
                entry.with_context(|| format!("遍历目录失败: {}", canonical_path.display()))?;
            // 不跟随符号链接可防止目录环和意外越过仓库边界。
            if entry.file_type().is_file() {
                files.push(input_file(entry.into_path(), repository_root)?);
            }
        }
    }

    files.sort_unstable_by(|left, right| left.repository_path.cmp(&right.repository_path));
    Ok(FileCollection {
        files,
        repository_scope,
    })
}

fn input_file(disk_path: PathBuf, repository_root: &Path) -> Result<InputFile> {
    let repository_path = repository_path(&disk_path, repository_root, false)?;

    Ok(InputFile {
        disk_path,
        repository_path,
    })
}

fn repository_path(path: &Path, repository_root: &Path, allow_root: bool) -> Result<String> {
    let relative = path.strip_prefix(repository_root).with_context(|| {
        format!(
            "文件不在仓库根目录内: {}（仓库根目录: {}）",
            path.display(),
            repository_root.display()
        )
    })?;
    let mut components = Vec::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!("文件路径未规范化: {}", path.display());
        };
        ensure!(
            !is_neoengram_dir_name(name),
            "不能添加 NeoEngram 内部文件: {}",
            path.display()
        );
        let name = name
            .to_str()
            .with_context(|| format!("文件路径不是有效 UTF-8: {}", path.display()))?;
        components.push(name);
    }
    ensure!(
        allow_root || !components.is_empty(),
        "不能把仓库根目录当作文件添加"
    );
    Ok(components.join("/"))
}

/// 对可能尚不存在的路径做纯词法规范化。调用方随后还会验证所有已存在的祖先，防止
/// `..` 或祖先符号链接把删除范围带到仓库之外。
fn normalize_absolute_path(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "内部错误：待规范化路径不是绝对路径");
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(
                    normalized.pop(),
                    "路径越过了文件系统根目录: {}",
                    path.display()
                );
            }
            Component::Normal(name) => normalized.push(name),
        }
    }
    Ok(normalized)
}

fn ensure_missing_path_is_inside_repository(path: &Path, repository_root: &Path) -> Result<()> {
    ensure!(
        path.starts_with(repository_root),
        "不能添加仓库之外的路径: {}",
        path.display()
    );

    let relative = path
        .strip_prefix(repository_root)
        .context("无法计算待删除路径的仓库相对位置")?;
    let mut current = repository_root.to_path_buf();
    let components: Vec<_> = relative.components().collect();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            bail!("文件路径未规范化: {}", path.display());
        };
        ensure!(
            !is_neoengram_dir_name(name),
            "不能添加 NeoEngram 内部文件: {}",
            path.display()
        );
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                ensure!(
                    !metadata.file_type().is_symlink(),
                    "路径祖先不能是符号链接: {}",
                    current.display()
                );
                if index + 1 < components.len() {
                    ensure!(metadata.is_dir(), "路径祖先不是目录: {}", current.display());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| format!("无法检查路径: {}", current.display()));
            }
        }
    }
    Ok(())
}

fn contains_repository_component(path: &Path) -> bool {
    path.components().any(
        |component| matches!(component, Component::Normal(name) if is_neoengram_dir_name(name)),
    )
}
