//! Loose-file implementation of immutable chunk payload storage.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Read, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use tempfile::NamedTempFile;

use super::contract::{
    validate_object_id, validate_object_spec, ObjectCheck, ObjectMeta, ObjectPage, ObjectSpec,
    ObjectStore, PutOutcome, MAX_OBJECT_CHECK_BATCH, MAX_OBJECT_LIST_PAGE_SIZE,
};

const IO_BUFFER_SIZE: usize = 64 * 1024;
/// Loose stores reserve this directory for unpublished objects and stable input snapshots.
pub(crate) const LOOSE_OBJECT_TEMP_DIR_NAME: &str = ".tmp";

/// The development backend: one immutable file named by BLAKE3 under a local directory.
#[derive(Debug, Clone)]
pub(crate) struct LooseObjectStore {
    root: PathBuf,
}

impl LooseObjectStore {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn temporary_dir(&self) -> PathBuf {
        self.root.join(LOOSE_OBJECT_TEMP_DIR_NAME)
    }

    fn object_path(&self, id: &str) -> Result<PathBuf> {
        validate_object_id(id)?;
        Ok(self.root.join(id))
    }

    fn stat_validated(&self, id: &str) -> Result<Option<ObjectMeta>> {
        let path = self.root.join(id);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "Chunk 对象不是普通文件: {}",
                    path.display()
                );
                Ok(Some(ObjectMeta {
                    id: id.to_owned(),
                    size: metadata.len(),
                }))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("无法检查 Chunk 对象: {}", path.display()))
            }
        }
    }
}

impl ObjectStore for LooseObjectStore {
    fn initialize(&self) -> Result<()> {
        if ensure_or_create_directory(&self.root, "对象")? {
            let parent = self
                .root
                .parent()
                .with_context(|| format!("对象目录没有父目录: {}", self.root.display()))?;
            sync_directory(parent)?;
        }
        if ensure_or_create_directory(&self.temporary_dir(), "对象临时")? {
            sync_directory(&self.root)?;
        }
        Ok(())
    }

    fn validate_layout(&self) -> Result<()> {
        ensure_existing_directory(&self.root, "对象")?;
        ensure_existing_directory(&self.temporary_dir(), "对象临时")
    }

    fn put_from(&self, expected: &ObjectSpec, source: &mut dyn Read) -> Result<PutOutcome> {
        validate_object_spec(expected)?;
        self.initialize()?;
        let object_path = self.object_path(&expected.id)?;

        if self.stat_validated(&expected.id)?.is_some() {
            self.verify(expected)?;
            return Ok(PutOutcome::AlreadyPresent);
        }

        let temporary_dir = self.temporary_dir();
        let mut temporary = NamedTempFile::new_in(&temporary_dir)
            .with_context(|| format!("无法在对象临时目录创建文件: {}", temporary_dir.display()))?;
        copy_and_hash_exact(source, temporary.as_file_mut(), expected, "写入临时对象")?;
        temporary
            .as_file()
            .sync_all()
            .with_context(|| format!("无法同步临时对象: {}", expected.id))?;

        match temporary.persist_noclobber(&object_path) {
            Ok(_) => Ok(PutOutcome::Created),
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                self.verify(expected)?;
                Ok(PutOutcome::AlreadyPresent)
            }
            Err(error) => {
                Err(error.error).with_context(|| format!("无法发布对象: {}", object_path.display()))
            }
        }
    }

    fn copy_to(&self, expected: &ObjectSpec, target: &mut dyn Write) -> Result<()> {
        validate_object_spec(expected)?;
        self.validate_layout()?;
        let object_path = self.object_path(&expected.id)?;
        let path_metadata = fs::symlink_metadata(&object_path).with_context(|| {
            format!("缺少 Chunk 对象 {}: {}", expected.id, object_path.display())
        })?;
        ensure!(
            path_metadata.is_file() && !path_metadata.file_type().is_symlink(),
            "Chunk 对象不是普通文件: {}",
            object_path.display()
        );
        ensure!(
            path_metadata.len() == expected.size,
            "Chunk 对象大小损坏: {}（期望 {}，实际 {}）",
            expected.id,
            expected.size,
            path_metadata.len()
        );

        let mut input = File::open(&object_path)
            .with_context(|| format!("无法打开 Chunk 对象: {}", object_path.display()))?;
        let opened_metadata = input
            .metadata()
            .with_context(|| format!("无法读取 Chunk 对象元数据: {}", object_path.display()))?;
        ensure!(
            opened_metadata.is_file() && opened_metadata.len() == expected.size,
            "Chunk 对象在读取前发生变化: {}",
            object_path.display()
        );
        copy_and_hash_exact(&mut input, target, expected, "读取 Chunk 对象")
    }

    fn stat(&self, id: &str) -> Result<Option<ObjectMeta>> {
        validate_object_id(id)?;
        self.validate_layout()?;
        self.stat_validated(id)
    }

    fn check_many(&self, expected: &[ObjectSpec]) -> Result<Vec<ObjectCheck>> {
        ensure!(
            expected.len() <= MAX_OBJECT_CHECK_BATCH,
            "对象批量检查超过上限 {}: {}",
            MAX_OBJECT_CHECK_BATCH,
            expected.len()
        );
        self.validate_layout()?;

        let mut checks = Vec::with_capacity(expected.len());
        for spec in expected {
            validate_object_spec(spec)?;
            match self.stat_validated(&spec.id)? {
                None => checks.push(ObjectCheck::Missing(spec.clone())),
                Some(meta) if meta.size == spec.size => {
                    checks.push(ObjectCheck::Present(meta));
                }
                Some(meta) => checks.push(ObjectCheck::SizeMismatch {
                    expected: spec.clone(),
                    actual_size: meta.size,
                }),
            }
        }
        Ok(checks)
    }

    fn list_page(&self, cursor: Option<&str>, limit: NonZeroUsize) -> Result<ObjectPage> {
        self.validate_layout()?;
        if let Some(cursor) = cursor {
            validate_object_id(cursor).context("对象枚举游标无效")?;
        }
        let limit = limit.get();
        ensure!(
            limit <= MAX_OBJECT_LIST_PAGE_SIZE,
            "对象枚举页超过上限 {}: {}",
            MAX_OBJECT_LIST_PAGE_SIZE,
            limit
        );

        // The loose backend uses one flat directory. Retaining only the lowest `limit + 1`
        // candidates bounds
        // memory even though later pages must rescan that directory. Fanout and pack backends can
        // implement the same cursor contract without the rescan.
        let retained_limit = limit.checked_add(1).context("对象枚举页大小溢出")?;
        let mut retained = BTreeMap::new();
        for entry in fs::read_dir(&self.root)
            .with_context(|| format!("无法读取对象目录: {}", self.root.display()))?
        {
            let entry = entry.context("无法读取对象目录项")?;
            let file_name = entry.file_name().into_string().map_err(|_| {
                anyhow::anyhow!("对象文件名不是有效 UTF-8: {}", entry.path().display())
            })?;

            if file_name == LOOSE_OBJECT_TEMP_DIR_NAME {
                let file_type = entry
                    .file_type()
                    .with_context(|| format!("无法检查对象临时目录: {}", entry.path().display()))?;
                ensure!(
                    file_type.is_dir() && !file_type.is_symlink(),
                    "对象临时路径不是普通目录: {}",
                    entry.path().display()
                );
                continue;
            }

            validate_object_id(&file_name)
                .with_context(|| format!("对象目录包含非法对象名: {}", entry.path().display()))?;
            let metadata = fs::symlink_metadata(entry.path())
                .with_context(|| format!("无法检查对象目录项: {}", entry.path().display()))?;
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "对象目录包含非普通文件: {}",
                entry.path().display()
            );
            if cursor.is_some_and(|cursor| file_name.as_str() <= cursor) {
                continue;
            }

            ensure!(
                retained.insert(file_name, metadata.len()).is_none(),
                "对象目录包含重复对象 ID"
            );
            if retained.len() > retained_limit {
                retained.pop_last();
            }
        }

        let has_more = retained.len() > limit;
        if has_more {
            retained.pop_last();
        }
        let objects: Vec<_> = retained
            .into_iter()
            .map(|(id, size)| ObjectMeta { id, size })
            .collect();
        let next_cursor = has_more.then(|| {
            objects
                .last()
                .expect("a page with more objects cannot be empty")
                .id
                .clone()
        });

        Ok(ObjectPage {
            objects,
            next_cursor,
        })
    }

    fn remove(&self, id: &str) -> Result<bool> {
        validate_object_id(id)?;
        self.validate_layout()?;
        let path = self.object_path(id)?;
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => {
                Err(error).with_context(|| format!("无法检查待删除对象: {}", path.display()))
            }
            Ok(metadata) => {
                ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "待删除对象不是普通文件: {}",
                    path.display()
                );
                fs::remove_file(&path)
                    .with_context(|| format!("无法删除不可达对象: {}", path.display()))?;
                sync_directory(&self.root)?;
                Ok(true)
            }
        }
    }

    fn durability_barrier(&self) -> Result<()> {
        self.validate_layout()?;
        sync_directory(&self.root)
    }
}

fn copy_and_hash_exact(
    source: &mut dyn Read,
    target: &mut dyn Write,
    expected: &ObjectSpec,
    operation: &str,
) -> Result<()> {
    let mut hasher = blake3::Hasher::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; IO_BUFFER_SIZE];
    loop {
        let read = match source.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("{operation}: {}", expected.id));
            }
        };
        let read_u64 = u64::try_from(read).context("对象读取大小超出 u64 范围")?;
        total = total.checked_add(read_u64).context("对象大小溢出")?;
        ensure!(
            total <= expected.size,
            "对象 {} 包含超出声明大小的内容（期望 {}）",
            expected.id,
            expected.size
        );
        hasher.update(&buffer[..read]);
        target
            .write_all(&buffer[..read])
            .with_context(|| format!("{operation}: {}", expected.id))?;
    }

    ensure!(
        total == expected.size,
        "对象 {} 提前结束（期望 {}，实际 {}）",
        expected.id,
        expected.size,
        total
    );
    let actual = hasher.finalize().to_hex().to_string();
    ensure!(
        actual == expected.id,
        "对象 Hash 损坏: {}（实际 {}）",
        expected.id,
        actual
    );
    Ok(())
}

fn ensure_or_create_directory(path: &Path, kind: &str) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "{kind}路径不是普通目录: {}",
                path.display()
            );
            Ok(false)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("无法创建{kind}目录: {}", path.display()));
                }
            }
            ensure_existing_directory(path, kind)?;
            Ok(true)
        }
        Err(error) => Err(error).with_context(|| format!("无法检查{kind}目录: {}", path.display())),
    }
}

fn ensure_existing_directory(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("无法检查{kind}目录: {}", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "{kind}路径不是普通目录: {}",
        path.display()
    );
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("无法打开目录进行同步: {}", path.display()))?
        .sync_all()
        .with_context(|| format!("无法同步目录: {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    // std cannot portably flush a directory on Windows. Object files are still synced before
    // atomic publication; a platform-specific backend may strengthen this barrier later.
    Ok(())
}
