use std::{
    fs::{self, File},
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt as UnixMetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt as WindowsMetadataExt;

use anyhow::{ensure, Context, Result};
use fastcdc::v2020::FastCDC;
use memmap2::{Mmap, MmapOptions};
use tempfile::{Builder, TempDir};

use crate::{
    models::{Chunk, FileNode},
    object_store::{ObjectSpec, ObjectStore},
};

#[cfg(test)]
use crate::object_store::{LooseObjectStore, LOOSE_OBJECT_TEMP_DIR_NAME};

/// FastCDC 的默认块大小：256 KiB / 1 MiB / 4 MiB。
///
/// 这组参数让大型模型权重拥有较高吞吐，同时仍能在文件中部发生插入时复用绝大多数块。
const MIN_CHUNK_SIZE: u32 = 256 * 1024;
const AVG_CHUNK_SIZE: u32 = 1024 * 1024;
const MAX_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
#[cfg(test)]
const TEMPORARY_DIR_NAME: &str = LOOSE_OBJECT_TEMP_DIR_NAME;

/// 对单个文件执行 CDC，并把数据块流式写入抽象对象存储。
///
/// `staging_dir` 必须位于本地文件系统，只保存切块期间的稳定输入快照。它与对象后端的
/// 物理布局无关，因此远端和 pack 后端也无需向切块器暴露路径。
/// 同步的内存映射、切块、哈希和落盘操作会在 Tokio 阻塞线程池中执行；调用方仍应限制
/// 同时处理的文件数量。
pub async fn chunk_file(
    path: PathBuf,
    object_store: Arc<dyn ObjectStore>,
    staging_dir: PathBuf,
) -> Result<FileNode> {
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || {
        chunk_file_with_store_blocking(&task_path, object_store.as_ref(), &staging_dir)
    })
    .await
    .with_context(|| format!("文件处理任务异常终止: {}", path.display()))?
}

fn chunk_file_with_store_blocking(
    path: &Path,
    object_store: &dyn ObjectStore,
    staging_dir: &Path,
) -> Result<FileNode> {
    let snapshot_path = metadata_path(path)?;
    object_store.initialize()?;
    ensure_staging_directory(staging_dir)?;

    // mmap 直接映射工作区文件时，另一个进程截断文件可能触发 SIGBUS，普通错误处理无法
    // 捕获。先在对象目录创建内部快照：同一文件系统优先 Reflink，无法 Reflink 时退回
    // 普通复制；之后只映射这个由 Synapse 独占的文件。
    let stable_snapshot = StableSnapshot::create(path, staging_dir)?;
    let total_size = stable_snapshot.total_size;
    if total_size == 0 {
        // memmap2 无法映射长度为零的文件。空文件本身不需要 payload 对象，
        // 元数据中的空 chunks 列表已经能够无歧义地表示它。
        return Ok(FileNode {
            path: snapshot_path,
            total_size,
            chunks: Vec::new(),
        });
    }

    let mapped = map_read_only(&stable_snapshot.file, &stable_snapshot.path)?;
    let mapped_size = u64::try_from(mapped.len()).context("当前平台无法表示映射文件的大小")?;
    ensure!(
        mapped_size == total_size,
        "文件在映射期间发生了大小变化: {}",
        path.display()
    );

    let mut chunks = Vec::new();
    let mut chunked_size = 0_u64;
    for chunk in FastCDC::new(&mapped, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE) {
        let end = chunk
            .offset
            .checked_add(chunk.length)
            .context("FastCDC 返回的数据块范围溢出")?;
        let bytes = mapped
            .get(chunk.offset..end)
            .context("FastCDC 返回的数据块超出文件范围")?;
        let hash = blake3::hash(bytes).to_hex().to_string();
        let offset = u64::try_from(chunk.offset).context("数据块偏移量超出 u64 范围")?;
        let size = u64::try_from(chunk.length).context("数据块大小超出 u64 范围")?;
        let spec = ObjectSpec::new(hash.clone(), size)?;
        let mut source = Cursor::new(bytes);
        object_store.put_from(&spec, &mut source)?;

        chunked_size = chunked_size
            .checked_add(size)
            .context("累计数据块大小溢出")?;
        chunks.push(Chunk { hash, offset, size });
    }

    ensure!(
        chunked_size == total_size,
        "切块后的总大小与原文件不一致: {}",
        path.display()
    );

    // 即使本次全部命中已有对象也执行 barrier：另一个并发写入者可能刚发布同一 ID，
    // 但尚未来得及同步其目录项。元数据只能在本次 barrier 成功后引用这些对象。
    object_store.durability_barrier()?;

    Ok(FileNode {
        path: snapshot_path,
        total_size,
        chunks,
    })
}

fn ensure_staging_directory(staging_dir: &Path) -> Result<()> {
    match fs::symlink_metadata(staging_dir) {
        Ok(metadata) => ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "切块暂存路径不是普通目录: {}",
            staging_dir.display()
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(staging_dir)
                .with_context(|| format!("无法创建切块暂存目录: {}", staging_dir.display()))?;
            let metadata = fs::symlink_metadata(staging_dir)
                .with_context(|| format!("无法检查切块暂存目录: {}", staging_dir.display()))?;
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "切块暂存路径不是普通目录: {}",
                staging_dir.display()
            );
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查切块暂存目录: {}", staging_dir.display()));
        }
    }
    Ok(())
}

#[cfg(test)]
fn chunk_file_blocking(path: &Path, objects_dir: &Path) -> Result<FileNode> {
    let store = LooseObjectStore::new(objects_dir.to_path_buf());
    chunk_file_with_store_blocking(path, &store, &objects_dir.join(TEMPORARY_DIR_NAME))
}

#[derive(Debug, PartialEq, Eq)]
struct SourceFingerprint {
    size: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
    #[cfg(windows)]
    creation_time: u64,
    #[cfg(windows)]
    last_write_time: u64,
    #[cfg(windows)]
    file_attributes: u32,
}

impl SourceFingerprint {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            size: metadata.len(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            modified_seconds: metadata.mtime(),
            #[cfg(unix)]
            modified_nanoseconds: metadata.mtime_nsec(),
            #[cfg(unix)]
            changed_seconds: metadata.ctime(),
            #[cfg(unix)]
            changed_nanoseconds: metadata.ctime_nsec(),
            #[cfg(windows)]
            creation_time: metadata.creation_time(),
            #[cfg(windows)]
            last_write_time: metadata.last_write_time(),
            #[cfg(windows)]
            file_attributes: metadata.file_attributes(),
        }
    }
}

struct StableSnapshot {
    // 字段按声明顺序析构。Windows 必须先关闭文件，才能删除包含它的临时目录。
    file: File,
    path: PathBuf,
    _directory: TempDir,
    total_size: u64,
}

impl StableSnapshot {
    fn create(source_path: &Path, temporary_dir: &Path) -> Result<Self> {
        let path_metadata = fs::symlink_metadata(source_path)
            .with_context(|| format!("无法读取文件元数据: {}", source_path.display()))?;
        ensure!(
            path_metadata.is_file() && !path_metadata.file_type().is_symlink(),
            "路径不是普通文件: {}",
            source_path.display()
        );

        let source = File::open(source_path)
            .with_context(|| format!("无法打开文件: {}", source_path.display()))?;
        let opened_metadata = source
            .metadata()
            .with_context(|| format!("无法读取文件元数据: {}", source_path.display()))?;
        let fingerprint = SourceFingerprint::from_metadata(&opened_metadata);
        ensure!(
            SourceFingerprint::from_metadata(&path_metadata) == fingerprint,
            "文件在打开期间发生了变化: {}",
            source_path.display()
        );

        let directory = Builder::new()
            .prefix(".snapshot-")
            .tempdir_in(temporary_dir)
            .with_context(|| format!("无法创建稳定快照目录: {}", temporary_dir.display()))?;
        let snapshot_path = directory.path().join("payload");
        let copied = reflink_copy::reflink_or_copy(source_path, &snapshot_path)
            .with_context(|| format!("无法创建文件稳定快照: {}", source_path.display()))?;
        if let Some(copied_size) = copied {
            ensure!(
                copied_size == fingerprint.size,
                "复制的快照大小与源文件不一致: {}",
                source_path.display()
            );
        }

        let opened_after = source
            .metadata()
            .with_context(|| format!("无法再次读取源文件元数据: {}", source_path.display()))?;
        let path_after = fs::symlink_metadata(source_path)
            .with_context(|| format!("无法再次读取源文件路径: {}", source_path.display()))?;
        ensure!(
            path_after.is_file()
                && !path_after.file_type().is_symlink()
                && SourceFingerprint::from_metadata(&opened_after) == fingerprint
                && SourceFingerprint::from_metadata(&path_after) == fingerprint,
            "文件在创建稳定快照期间发生了变化，请重试: {}",
            source_path.display()
        );

        let snapshot_file = open_and_sync_snapshot(&snapshot_path)?;
        let snapshot_size = snapshot_file
            .metadata()
            .with_context(|| format!("无法读取稳定快照元数据: {}", snapshot_path.display()))?
            .len();
        ensure!(
            snapshot_size == fingerprint.size,
            "稳定快照大小与源文件不一致: {}",
            source_path.display()
        );

        Ok(Self {
            file: snapshot_file,
            path: snapshot_path,
            _directory: directory,
            total_size: snapshot_size,
        })
    }
}

#[cfg(not(windows))]
fn open_and_sync_snapshot(snapshot_path: &Path) -> Result<File> {
    let snapshot = File::open(snapshot_path)
        .with_context(|| format!("无法打开稳定快照: {}", snapshot_path.display()))?;
    snapshot
        .sync_all()
        .with_context(|| format!("无法同步稳定快照: {}", snapshot_path.display()))?;
    Ok(snapshot)
}

#[cfg(windows)]
fn open_and_sync_snapshot(snapshot_path: &Path) -> Result<File> {
    // Windows 的 FlushFileBuffers 需要可写 handle。Reflink/fs::copy 会继承源文件的
    // readonly 属性，因此只对私有临时快照清除该属性，再以读写方式打开并同步。
    let mut permissions = fs::metadata(snapshot_path)
        .with_context(|| format!("无法读取稳定快照权限: {}", snapshot_path.display()))?
        .permissions();
    if permissions.readonly() {
        permissions.set_readonly(false);
        fs::set_permissions(snapshot_path, permissions)
            .with_context(|| format!("无法更新稳定快照权限: {}", snapshot_path.display()))?;
    }
    let snapshot = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(snapshot_path)
        .with_context(|| format!("无法打开稳定快照: {}", snapshot_path.display()))?;
    snapshot
        .sync_all()
        .with_context(|| format!("无法同步稳定快照: {}", snapshot_path.display()))?;
    Ok(snapshot)
}

fn metadata_path(path: &Path) -> Result<String> {
    let path = path
        .to_str()
        .with_context(|| format!("文件路径不是有效 UTF-8: {}", path.display()))?;
    Ok(path.replace(std::path::MAIN_SEPARATOR, "/"))
}

/// 创建只读内存映射。
///
/// 此函数是整个核心库唯一允许使用 unsafe 的位置，便于集中审计。
#[allow(unsafe_code)]
fn map_read_only(file: &File, path: &Path) -> Result<Mmap> {
    // SAFETY: 映射的是本模块刚创建、并由私有 TempDir 持有的稳定快照，而不是可能被
    // 训练进程并发截断的工作区文件。映射覆盖已成功打开且长度非零的普通文件；快照和
    // Mmap 的生命周期都局限在单次 chunk_file_blocking 调用内，本模块绝不修改它。
    unsafe { MmapOptions::new().map(file) }
        .with_context(|| format!("无法创建文件内存映射: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Read};

    use anyhow::Result;

    use super::{chunk_file_blocking, StableSnapshot, TEMPORARY_DIR_NAME};

    #[test]
    fn stable_snapshot_survives_source_truncation() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("checkpoint.bin");
        let objects = temporary.path().join("objects");
        let snapshots = objects.join(TEMPORARY_DIR_NAME);
        let original = vec![0x5a; 128 * 1024];
        fs::create_dir(&objects)?;
        fs::create_dir(&snapshots)?;
        fs::write(&source, &original)?;

        let snapshot = StableSnapshot::create(&source, &snapshots)?;
        fs::write(&source, b"truncated")?;

        let mut contents = Vec::new();
        fs::File::open(&snapshot.path)?.read_to_end(&mut contents)?;
        assert_eq!(contents, original);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlinked_objects_directory() -> Result<()> {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source.bin");
        let outside = temporary.path().join("outside");
        let objects = temporary.path().join("objects");
        fs::write(&source, b"payload")?;
        fs::create_dir(&outside)?;
        symlink(&outside, &objects)?;

        assert!(chunk_file_blocking(&source, &objects).is_err());
        assert_eq!(fs::read_dir(outside)?.count(), 0);
        Ok(())
    }
}
