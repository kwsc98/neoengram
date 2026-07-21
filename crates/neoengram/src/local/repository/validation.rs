use std::{collections::BTreeMap, ffi::OsStr};

use anyhow::{ensure, Context, Result};
use neoengram_core::{
    Commit, DirectoryEntry, DirectoryEntryKind, FileNode, Index, Tree, INDEX_FORMAT_VERSION,
};
use unicode_normalization::UnicodeNormalization;

use crate::local::{
    metadata::{describe_manifest, DirectoryHasher},
    STORAGE_TEMP_FILE_PREFIX,
};

use super::{Repository, NEOENGRAM_DIR_NAME};

pub(super) const COMMIT_HASH_DOMAIN: &[u8] = b"neoengram-commit-v3";

pub(crate) fn is_neoengram_dir_name(name: &OsStr) -> bool {
    name.to_str().is_some_and(|name| {
        name.eq_ignore_ascii_case(NEOENGRAM_DIR_NAME)
            || name
                .as_bytes()
                .get(..STORAGE_TEMP_FILE_PREFIX.len())
                .is_some_and(|prefix| {
                    prefix.eq_ignore_ascii_case(STORAGE_TEMP_FILE_PREFIX.as_bytes())
                })
    })
}

impl Repository {
    pub(crate) fn validate_index_snapshot(&self, index: &Index) -> Result<()> {
        validate_index(index)
    }

    pub(crate) fn validate_file_snapshot(&self, file: &FileNode) -> Result<()> {
        validate_files(std::slice::from_ref(file))
    }

    pub(crate) fn validate_logical_path(&self, path: &str) -> Result<()> {
        validate_repository_path(path)
    }

    pub(crate) fn tree_id(&self, tree: &Tree) -> Result<String> {
        validate_files(&tree.files)?;
        directory_id_for_files(&tree.files)
    }

    pub(crate) fn commit_id(&self, commit: &Commit) -> Result<String> {
        validate_commit(commit)?;
        commit_content_id(commit)
    }

    pub(crate) fn validate_commit_id(&self, id: &str) -> Result<()> {
        validate_hash(id, "Commit")
    }
}

#[derive(Default)]
struct HashDirectory {
    children: BTreeMap<String, HashNode>,
}

enum HashNode {
    File {
        manifest_id: String,
        total_size: u64,
    },
    Directory(HashDirectory),
}

fn directory_id_for_files(files: &[FileNode]) -> Result<String> {
    let mut root = HashDirectory::default();
    for file in files {
        let manifest = describe_manifest(file.total_size, &file.chunks)?;
        insert_hash_file(&mut root, &file.path, manifest.id, file.total_size)?;
    }
    hash_directory(&root)
}

fn insert_hash_file(
    directory: &mut HashDirectory,
    path: &str,
    manifest_id: String,
    total_size: u64,
) -> Result<()> {
    let mut components = path.split('/').peekable();
    let mut current = directory;
    while let Some(component) = components.next() {
        if components.peek().is_none() {
            ensure!(
                current
                    .children
                    .insert(
                        component.to_owned(),
                        HashNode::File {
                            manifest_id,
                            total_size,
                        },
                    )
                    .is_none(),
                "Tree 路径重复: {path}"
            );
            return Ok(());
        }
        let node = current
            .children
            .entry(component.to_owned())
            .or_insert_with(|| HashNode::Directory(HashDirectory::default()));
        let HashNode::Directory(child) = node else {
            anyhow::bail!("Tree 同时包含文件及其子路径: {path}");
        };
        current = child;
    }
    anyhow::bail!("Tree 路径不能为空")
}

fn hash_directory(directory: &HashDirectory) -> Result<String> {
    let mut hasher = DirectoryHasher::new();
    for (ordinal, (name, node)) in directory.children.iter().enumerate() {
        let (kind, target_id, total_size) = match node {
            HashNode::File {
                manifest_id,
                total_size,
            } => (DirectoryEntryKind::File, manifest_id.clone(), *total_size),
            HashNode::Directory(child) => {
                (DirectoryEntryKind::Directory, hash_directory(child)?, 0)
            }
        };
        hasher.push(&DirectoryEntry {
            ordinal: u64::try_from(ordinal).context("Directory ordinal 超出 u64")?,
            name: name.clone(),
            kind,
            target_id,
            total_size,
        })?;
    }
    Ok(hasher.finish().0)
}

pub(super) fn validate_index(index: &Index) -> Result<()> {
    ensure!(
        index.format_version == INDEX_FORMAT_VERSION,
        "不支持的 index 格式版本 {}（当前支持 {}）",
        index.format_version,
        INDEX_FORMAT_VERSION
    );
    validate_files(&index.files)
}

pub(super) fn validate_files(files: &[FileNode]) -> Result<()> {
    let mut previous_path: Option<&str> = None;
    let mut portable_paths: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for file in files {
        validate_repository_path(&file.path)?;
        if let Some(previous) = previous_path {
            ensure!(
                previous < file.path.as_str(),
                "文件路径必须严格升序且唯一: {}",
                file.path
            );
        }
        previous_path = Some(&file.path);

        // Windows 默认大小写不敏感，并且路径前缀冲突不一定在字节序排序后相邻，例如
        // `a`、`a-b`、`a/b`。用可移植 key 检查所有祖先和潜在子孙，避免生成只能在
        // 某些平台读取、或同时把一个路径解释为文件和目录的 Tree。
        let portable_path = portable_path_key(&file.path);
        ensure!(
            !portable_paths.contains(&portable_path),
            "Tree 包含跨平台大小写冲突的路径: {}",
            file.path
        );
        for (offset, _) in portable_path.match_indices('/') {
            ensure!(
                !portable_paths.contains(&portable_path[..offset]),
                "Tree 同时包含文件及其子路径: {}",
                file.path
            );
        }
        let descendant_prefix = format!("{portable_path}/");
        if let Some(descendant) = portable_paths.range(descendant_prefix.clone()..).next() {
            ensure!(
                !descendant.starts_with(&descendant_prefix),
                "Tree 同时包含文件及其子路径: {}",
                file.path
            );
        }
        portable_paths.insert(portable_path);

        let mut expected_offset = 0_u64;
        for chunk in &file.chunks {
            validate_hash(&chunk.hash, "Chunk")?;
            ensure!(chunk.size > 0, "Chunk 大小必须大于零: {}", chunk.hash);
            ensure!(
                chunk.offset == expected_offset,
                "文件 {} 的 Chunk 偏移不连续",
                file.path
            );
            expected_offset = expected_offset
                .checked_add(chunk.size)
                .context("累计 Chunk 大小溢出")?;
        }
        ensure!(
            expected_offset == file.total_size,
            "文件 {} 的 Chunk 总大小与文件大小不一致",
            file.path
        );
    }
    Ok(())
}

fn validate_repository_path(path: &str) -> Result<()> {
    ensure!(!path.is_empty(), "仓库文件路径不能为空");
    ensure!(!path.starts_with('/'), "仓库文件路径不能是绝对路径: {path}");
    ensure!(
        !path.contains('\\'),
        "仓库文件路径必须使用 `/` 分隔: {path}"
    );
    ensure!(!path.contains('\0'), "仓库文件路径包含 NUL: {path}");

    for component in path.split('/') {
        ensure!(
            !component.is_empty() && component != "." && component != "..",
            "仓库文件路径未规范化: {path}"
        );
        ensure!(
            !is_neoengram_dir_name(OsStr::new(component)),
            "不能记录 NeoEngram 内部路径: {path}"
        );
        validate_portable_component(component, path)?;
    }
    Ok(())
}

fn portable_path_key(path: &str) -> String {
    path.split('/')
        .map(|component| {
            component
                .nfc()
                .flat_map(char::to_lowercase)
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn validate_portable_component(component: &str, path: &str) -> Result<()> {
    ensure!(
        component.nfc().eq(component.chars()),
        "仓库路径必须使用 Unicode NFC 规范形式: {path}"
    );
    ensure!(
        !component.ends_with(' ') && !component.ends_with('.'),
        "仓库路径组件不能以空格或句点结尾: {path}"
    );
    ensure!(
        !component.chars().any(|character| {
            character <= '\u{1f}' || matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*')
        }),
        "仓库路径包含跨平台不支持的字符: {path}"
    );

    let device_name = component
        .split('.')
        .next()
        .unwrap_or(component)
        .to_ascii_uppercase();
    let reserved = matches!(
        device_name.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$" | "CONIN$" | "CONOUT$"
    ) || device_name.strip_prefix("COM").is_some_and(|suffix| {
        matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
    }) || device_name.strip_prefix("LPT").is_some_and(|suffix| {
        matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
    });
    ensure!(!reserved, "仓库路径使用 Windows 保留设备名: {path}");
    Ok(())
}

pub(super) fn validate_commit(commit: &Commit) -> Result<()> {
    validate_hash(&commit.tree_hash, "根 Directory")?;
    if let Some(parent) = &commit.parent {
        validate_hash(parent, "父 Commit")?;
    }
    ensure!(!commit.message.trim().is_empty(), "Commit message 不能为空");
    Ok(())
}

pub(super) fn validate_hash(hash: &str, kind: &str) -> Result<()> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "{kind} ID 不是有效的小写 BLAKE3 Hash: {hash}"
    );
    Ok(())
}

pub(super) fn validate_sorted_ids(ids: &[String], kind: &str) -> Result<()> {
    let mut previous: Option<&str> = None;
    for id in ids {
        validate_hash(id, kind)?;
        if let Some(previous) = previous {
            ensure!(
                previous < id.as_str(),
                "元数据后端返回了未排序或重复的 {kind} ID: {id}"
            );
        }
        previous = Some(id);
    }
    Ok(())
}

pub(super) fn commit_content_id(commit: &Commit) -> Result<String> {
    validate_commit(commit)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(COMMIT_HASH_DOMAIN.len() as u64).to_le_bytes());
    hasher.update(COMMIT_HASH_DOMAIN);
    hasher.update(&1_u32.to_le_bytes());
    hasher.update(blake3::Hash::from_hex(&commit.tree_hash)?.as_bytes());
    match &commit.parent {
        Some(parent) => {
            hasher.update(&[1]);
            hasher.update(blake3::Hash::from_hex(parent)?.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    let message = commit.message.as_bytes();
    hasher.update(&(message.len() as u64).to_le_bytes());
    hasher.update(message);
    hasher.update(&commit.created_at_unix_ms.to_le_bytes());
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use neoengram_core::{Commit, FileNode, Tree};

    use super::{validate_files, validate_repository_path};
    use crate::local::repository::Repository;

    #[test]
    fn rejects_non_portable_repository_paths() {
        for path in [
            "/absolute.bin",
            "C:drive-relative.bin",
            "dir\\windows.bin",
            "dir/../escape.bin",
            "CON.txt",
            "aux",
            "LPT9.log",
            "name.",
            "name ",
            "bad?.bin",
            ".NeoEngram/private.bin",
            ".neoengram-tmp-orphan/private.bin",
            "cafe\u{301}.bin",
        ] {
            assert!(
                validate_repository_path(path).is_err(),
                "unexpected portable path: {path}"
            );
        }
        assert!(validate_repository_path("caf\u{e9}/model.bin").is_ok());
    }

    #[test]
    fn rejects_case_and_file_directory_collisions() {
        assert!(validate_files(&[empty_node("Model.bin"), empty_node("model.bin")]).is_err());
        assert!(
            validate_files(&[empty_node("a"), empty_node("a-b"), empty_node("a/child"),]).is_err()
        );
    }

    #[test]
    fn current_tree_and_commit_ids_are_stable() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let repository = Repository::at(temporary.path().to_path_buf())?;
        let tree = Tree::default();
        let tree_id = repository.tree_id(&tree)?;
        assert_eq!(
            tree_id,
            "be674f5161f4ba55cc9028fdca4e8e219e8470191ee749afacb0bac8ab53be99"
        );

        let commit = Commit {
            tree_hash: tree_id,
            parent: None,
            message: "snapshot".to_owned(),
            created_at_unix_ms: 1,
        };
        assert_eq!(
            repository.commit_id(&commit)?,
            "720575956eb8d66cc4b0bc4e038013b1df17f2e170a6ad844184efefcfe2aaf6"
        );

        let deep_tree = Tree {
            files: vec![empty_node("a/b/c")],
        };
        assert_eq!(
            repository.tree_id(&deep_tree)?,
            "559d66308dd1c7769aaf180517f3562d5f89cd2c654ff8a982cf6e4dc86c2fb5"
        );
        Ok(())
    }

    fn empty_node(path: &str) -> FileNode {
        FileNode {
            path: path.to_owned(),
            total_size: 0,
            chunks: Vec::new(),
        }
    }
}
