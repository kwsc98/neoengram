//! Fixed-Commit read-only mount namespace and verified Chunk cache.

#![cfg_attr(
    not(all(feature = "fuse-mount", any(target_os = "linux", target_os = "macos"))),
    allow(dead_code)
)]

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    num::NonZeroU32,
    path::{Component, Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

use crate::local::{
    model::{ChunkingStrategy, DirectoryEntry, DirectoryEntryKind},
    objects::ObjectSpec,
    repository::Repository,
};
use anyhow::{bail, ensure, Context, Result};

#[cfg(all(feature = "fuse-mount", any(target_os = "linux", target_os = "macos")))]
mod platform;

pub(crate) const ROOT_INODE: u64 = 1;
const NAMESPACE_HASH_DOMAIN: &[u8] = b"neoengram-fuse-inode-v1";
const DIRECTORY_PAGE_SIZE: u32 = 512;
const CHUNK_RANGE_LIMIT: u32 = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub(crate) struct MountNode {
    pub(crate) inode: u64,
    pub(crate) parent: u64,
    pub(crate) path: String,
    pub(crate) kind: NodeKind,
    pub(crate) target_id: String,
    pub(crate) size: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct DirectoryPage {
    pub(crate) entries: Vec<(MountNode, u64)>,
    pub(crate) next: Option<u64>,
}

#[derive(Debug)]
struct ActiveNode {
    node: MountNode,
    lookups: u64,
    opens: u64,
}

#[derive(Debug)]
struct OpenFile {
    inode: u64,
    manifest_id: String,
    size: u64,
}

#[derive(Debug)]
struct OpenDirectory {
    inode: u64,
}

#[derive(Debug)]
struct InodeTable {
    by_inode: HashMap<u64, ActiveNode>,
    by_path: HashMap<String, u64>,
    handles: HashMap<u64, OpenFile>,
    directory_handles: HashMap<u64, OpenDirectory>,
    next_handle: u64,
}

impl InodeTable {
    fn new(root_id: String) -> Self {
        let root = MountNode {
            inode: ROOT_INODE,
            parent: ROOT_INODE,
            path: String::new(),
            kind: NodeKind::Directory,
            target_id: root_id,
            size: 0,
        };
        let mut by_inode = HashMap::new();
        by_inode.insert(
            ROOT_INODE,
            ActiveNode {
                node: root,
                lookups: u64::MAX,
                opens: 0,
            },
        );
        let mut by_path = HashMap::new();
        by_path.insert(String::new(), ROOT_INODE);
        Self {
            by_inode,
            by_path,
            handles: HashMap::new(),
            directory_handles: HashMap::new(),
            next_handle: 1,
        }
    }
}

#[derive(Debug)]
struct CacheState {
    entries: HashMap<String, Arc<[u8]>>,
    lru: VecDeque<String>,
    bytes: u64,
    loading: HashSet<String>,
}

#[derive(Debug)]
struct VerifiedChunkCache {
    capacity: u64,
    state: Mutex<CacheState>,
    loaded: Condvar,
}

impl VerifiedChunkCache {
    fn new(capacity: u64) -> Result<Self> {
        ensure!(capacity > 0, "FUSE Chunk 缓存必须大于零");
        Ok(Self {
            capacity,
            state: Mutex::new(CacheState {
                entries: HashMap::new(),
                lru: VecDeque::new(),
                bytes: 0,
                loading: HashSet::new(),
            }),
            loaded: Condvar::new(),
        })
    }

    fn get_or_load(&self, repository: &Repository, spec: &ObjectSpec) -> Result<Arc<[u8]>> {
        loop {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("Chunk 缓存锁已损坏"))?;
            if let Some(bytes) = state.entries.get(&spec.id).cloned() {
                ensure!(
                    bytes.len() as u64 == spec.size,
                    "同一 Chunk ID 在 Manifest 中具有冲突大小: {}",
                    spec.id
                );
                touch_lru(&mut state.lru, &spec.id);
                return Ok(bytes);
            }
            if state.loading.insert(spec.id.clone()) {
                break;
            }
            state = self
                .loaded
                .wait(state)
                .map_err(|_| anyhow::anyhow!("Chunk 缓存锁已损坏"))?;
            drop(state);
        }

        let loaded = (|| {
            let capacity = usize::try_from(spec.size).context("当前平台无法分配 Chunk 缓存")?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(capacity)
                .map_err(|error| anyhow::anyhow!("无法分配 Chunk 缓存: {error}"))?;
            repository
                .object_store()
                .copy_to(spec, &mut bytes)
                .with_context(|| format!("无法读取挂载 Chunk {}", spec.id))?;
            ensure!(
                bytes.len() == capacity,
                "挂载 Chunk 读取长度不一致: {}",
                spec.id
            );
            Ok::<Arc<[u8]>, anyhow::Error>(bytes.into())
        })();

        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Chunk 缓存锁已损坏"))?;
        state.loading.remove(&spec.id);
        if let Ok(bytes) = &loaded {
            if spec.size <= self.capacity {
                while state.bytes.saturating_add(spec.size) > self.capacity {
                    let Some(evicted) = state.lru.pop_front() else {
                        break;
                    };
                    if let Some(value) = state.entries.remove(&evicted) {
                        state.bytes = state.bytes.saturating_sub(value.len() as u64);
                    }
                }
                let Some(new_size) = state.bytes.checked_add(spec.size) else {
                    self.loaded.notify_all();
                    bail!("Chunk 缓存字节计数溢出");
                };
                state.bytes = new_size;
                state.lru.push_back(spec.id.clone());
                state.entries.insert(spec.id.clone(), Arc::clone(bytes));
            }
        }
        self.loaded.notify_all();
        loaded
    }
}

fn touch_lru(lru: &mut VecDeque<String>, id: &str) {
    if let Some(index) = lru.iter().position(|entry| entry == id) {
        lru.remove(index);
    }
    lru.push_back(id.to_owned());
}

#[derive(Debug)]
pub(crate) struct ReadOnlyMount {
    repository: Repository,
    commit_id: String,
    created_at_unix_ms: u64,
    root_file_count: u64,
    root_total_size: u64,
    mountpoint: PathBuf,
    cache: VerifiedChunkCache,
    inodes: InodeTable,
}

impl ReadOnlyMount {
    pub(crate) fn prepare(
        repository: Repository,
        target: &str,
        mountpoint: &Path,
        cache_bytes: u64,
    ) -> Result<Self> {
        let mountpoint = validate_mountpoint(&repository, mountpoint)?;
        let commit_id = resolve_target(&repository, target)?;
        let commit = repository.read_commit(&commit_id)?;
        let root = repository.open_directory(&commit.root_directory_id)?;
        let root_file_count = root.file_count();
        let root_total_size = root.total_size();
        drop(root);
        Ok(Self {
            repository,
            commit_id,
            created_at_unix_ms: commit.created_at_unix_ms,
            root_file_count,
            root_total_size,
            mountpoint,
            cache: VerifiedChunkCache::new(cache_bytes)?,
            inodes: InodeTable::new(commit.root_directory_id),
        })
    }

    pub(crate) fn commit_id(&self) -> &str {
        &self.commit_id
    }

    pub(crate) fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }

    pub(crate) fn root_file_count(&self) -> u64 {
        self.root_file_count
    }

    pub(crate) fn root_total_size(&self) -> u64 {
        self.root_total_size
    }

    pub(crate) fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    pub(crate) fn node(&self, inode: u64) -> Option<MountNode> {
        self.inodes
            .by_inode
            .get(&inode)
            .map(|entry| entry.node.clone())
    }

    pub(crate) fn lookup(&mut self, parent: u64, name: &str) -> Result<Option<MountNode>> {
        if name == "." {
            return Ok(self.node(parent));
        }
        if name == ".." {
            let Some(parent_node) = self.node(parent) else {
                return Ok(None);
            };
            return Ok(self.node(parent_node.parent));
        }
        let Some(parent_node) = self.node(parent) else {
            return Ok(None);
        };
        ensure!(parent_node.kind == NodeKind::Directory, "父 inode 不是目录");
        let reader = self.repository.open_directory(&parent_node.target_id)?;
        let Some(entry) = reader.get_entry(name)? else {
            return Ok(None);
        };
        drop(reader);
        let node = self.activate_entry(&parent_node, &entry)?;
        Ok(Some(node))
    }

    pub(crate) fn read_directory(
        &self,
        inode: u64,
        after_ordinal: Option<u64>,
    ) -> Result<DirectoryPage> {
        let node = self.node(inode).context("Directory inode 不存在")?;
        ensure!(node.kind == NodeKind::Directory, "inode 不是目录");
        let reader = self.repository.open_directory(&node.target_id)?;
        let request = crate::local::metadata::PageRequest::new(
            after_ordinal.map(|value| value.to_string()),
            DIRECTORY_PAGE_SIZE,
        )?;
        let page = reader.scan_entries(&request)?;
        let mut entries = Vec::with_capacity(page.items.len());
        for entry in page.items {
            let cookie = entry
                .ordinal
                .checked_add(3)
                .context("Directory cookie 溢出")?;
            entries.push((self.node_from_entry(&node, &entry)?, cookie));
        }
        let next = page
            .next
            .map(|value| value.parse::<u64>().context("Directory cursor 无效"))
            .transpose()?;
        Ok(DirectoryPage { entries, next })
    }

    pub(crate) fn open_file(&mut self, inode: u64) -> Result<u64> {
        let active = self
            .inodes
            .by_inode
            .get_mut(&inode)
            .context("文件 inode 不存在")?;
        ensure!(active.node.kind == NodeKind::File, "inode 不是文件");
        active.opens = active.opens.checked_add(1).context("文件打开计数溢出")?;
        let handle = self.inodes.next_handle;
        self.inodes.next_handle = self
            .inodes
            .next_handle
            .checked_add(1)
            .context("文件 handle 溢出")?;
        self.inodes.handles.insert(
            handle,
            OpenFile {
                inode,
                manifest_id: active.node.target_id.clone(),
                size: active.node.size,
            },
        );
        Ok(handle)
    }

    pub(crate) fn open_directory_handle(&mut self, inode: u64) -> Result<u64> {
        let active = self
            .inodes
            .by_inode
            .get_mut(&inode)
            .context("Directory inode 不存在")?;
        ensure!(active.node.kind == NodeKind::Directory, "inode 不是目录");
        active.opens = active.opens.checked_add(1).context("目录打开计数溢出")?;
        let handle = self.inodes.next_handle;
        self.inodes.next_handle = self
            .inodes
            .next_handle
            .checked_add(1)
            .context("目录 handle 溢出")?;
        self.inodes
            .directory_handles
            .insert(handle, OpenDirectory { inode });
        Ok(handle)
    }

    pub(crate) fn directory_handle_inode(&self, handle: u64) -> Result<u64> {
        self.inodes
            .directory_handles
            .get(&handle)
            .map(|open| open.inode)
            .context("目录 handle 不存在")
    }

    pub(crate) fn release_file(&mut self, handle: u64) -> Result<()> {
        let open = self
            .inodes
            .handles
            .remove(&handle)
            .context("文件 handle 不存在")?;
        if let Some(active) = self.inodes.by_inode.get_mut(&open.inode) {
            active.opens = active.opens.saturating_sub(1);
        }
        self.reap_inode(open.inode);
        Ok(())
    }

    pub(crate) fn file_handle_inode(&self, handle: u64) -> Result<u64> {
        self.inodes
            .handles
            .get(&handle)
            .map(|open| open.inode)
            .context("文件 handle 不存在")
    }

    pub(crate) fn release_directory(&mut self, handle: u64) -> Result<()> {
        let open = self
            .inodes
            .directory_handles
            .remove(&handle)
            .context("目录 handle 不存在")?;
        if let Some(active) = self.inodes.by_inode.get_mut(&open.inode) {
            active.opens = active.opens.saturating_sub(1);
        }
        self.reap_inode(open.inode);
        Ok(())
    }

    pub(crate) fn read_file(&self, handle: u64, offset: u64, size: u32) -> Result<Vec<u8>> {
        let open = self
            .inodes
            .handles
            .get(&handle)
            .context("文件 handle 不存在")?;
        if offset >= open.size || size == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(u64::from(size))
            .context("FUSE 读取范围溢出")?
            .min(open.size);
        let manifest = self.repository.open_manifest(&open.manifest_id)?;
        ensure!(
            manifest.total_size() == open.size,
            "Manifest 文件大小发生变化"
        );
        let whole_file = manifest.chunking() == ChunkingStrategy::WholeFile;
        let mut result = Vec::with_capacity(usize::try_from(end - offset)?);
        let mut cursor = offset;
        let mut range_offset = offset;
        loop {
            let page = manifest.scan_chunks_from_offset(
                range_offset,
                NonZeroU32::new(CHUNK_RANGE_LIMIT).expect("range limit is nonzero"),
            )?;
            for chunk in page.items {
                if chunk.offset >= end {
                    break;
                }
                let chunk_end = chunk
                    .offset
                    .checked_add(chunk.size)
                    .context("Chunk 范围溢出")?;
                ensure!(
                    chunk.offset <= cursor && chunk_end > cursor,
                    "Manifest range 存在空洞"
                );
                ensure!(
                    !whole_file || chunk.size <= self.cache.capacity,
                    "WholeFile 对象 {} bytes 超过 FUSE Chunk 缓存上限 {} bytes；请增大 --cache-size-mib 或使用 export --mode hardlink",
                    chunk.size,
                    self.cache.capacity
                );
                let bytes = self
                    .cache
                    .get_or_load(&self.repository, &ObjectSpec::new(chunk.hash, chunk.size)?)?;
                let start = usize::try_from(cursor - chunk.offset)?;
                let slice_end = usize::try_from((end.min(chunk_end)) - chunk.offset)?;
                result.extend_from_slice(&bytes[start..slice_end]);
                cursor = end.min(chunk_end);
                if cursor == end {
                    break;
                }
            }
            if cursor == end {
                break;
            }
            let next = page.next.context("Manifest range 未覆盖完整读取请求")?;
            let next = next.parse::<u64>().context("Manifest range cursor 无效")?;
            ensure!(next > range_offset, "Manifest range cursor 未前进");
            range_offset = next;
        }
        Ok(result)
    }

    pub(crate) fn forget(&mut self, inode: u64, count: u64) {
        if inode == ROOT_INODE {
            return;
        }
        if let Some(active) = self.inodes.by_inode.get_mut(&inode) {
            active.lookups = active.lookups.saturating_sub(count);
        }
        self.reap_inode(inode);
    }

    fn reap_inode(&mut self, inode: u64) {
        let should_remove = self
            .inodes
            .by_inode
            .get(&inode)
            .is_some_and(|active| active.lookups == 0 && active.opens == 0);
        if should_remove {
            if let Some(active) = self.inodes.by_inode.remove(&inode) {
                self.inodes.by_path.remove(&active.node.path);
            }
        }
    }

    fn node_from_entry(&self, parent: &MountNode, entry: &DirectoryEntry) -> Result<MountNode> {
        let path = if parent.path.is_empty() {
            entry.name.clone()
        } else {
            format!("{}/{}", parent.path, entry.name)
        };
        let kind = match entry.kind {
            DirectoryEntryKind::File => NodeKind::File,
            DirectoryEntryKind::Directory => NodeKind::Directory,
        };
        let inode = self.preview_inode(&path, kind);
        Ok(MountNode {
            inode,
            parent: parent.inode,
            path,
            kind,
            target_id: entry.target_id.clone(),
            size: entry.total_size,
        })
    }

    fn activate_entry(&mut self, parent: &MountNode, entry: &DirectoryEntry) -> Result<MountNode> {
        let path = if parent.path.is_empty() {
            entry.name.clone()
        } else {
            format!("{}/{}", parent.path, entry.name)
        };
        if let Some(inode) = self.inodes.by_path.get(&path).copied() {
            let active = self
                .inodes
                .by_inode
                .get_mut(&inode)
                .context("inode 索引损坏")?;
            active.lookups = active
                .lookups
                .checked_add(1)
                .context("inode lookup 计数溢出")?;
            return Ok(active.node.clone());
        }
        let kind = match entry.kind {
            DirectoryEntryKind::File => NodeKind::File,
            DirectoryEntryKind::Directory => NodeKind::Directory,
        };
        let inode = self.preview_inode(&path, kind);
        let node = MountNode {
            inode,
            parent: parent.inode,
            path: path.clone(),
            kind,
            target_id: entry.target_id.clone(),
            size: entry.total_size,
        };
        self.inodes.by_path.insert(path, inode);
        self.inodes.by_inode.insert(
            inode,
            ActiveNode {
                node: node.clone(),
                lookups: 1,
                opens: 0,
            },
        );
        Ok(node)
    }

    fn preview_inode(&self, path: &str, kind: NodeKind) -> u64 {
        for salt in 0_u64.. {
            let inode = derive_inode(&self.commit_id, path, kind, salt);
            match self.inodes.by_inode.get(&inode) {
                Some(active) if active.node.path != path => continue,
                _ => return inode,
            }
        }
        unreachable!("inode salt space is exhaustive")
    }
}

fn derive_inode(commit_id: &str, path: &str, kind: NodeKind, salt: u64) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(NAMESPACE_HASH_DOMAIN);
    hasher.update(commit_id.as_bytes());
    hasher.update(&[match kind {
        NodeKind::File => 1,
        NodeKind::Directory => 2,
    }]);
    hasher.update(&(path.len() as u64).to_le_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&salt.to_le_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    let inode = u64::from_le_bytes(bytes) & i64::MAX as u64;
    inode.max(2)
}

fn resolve_target(repository: &Repository, target: &str) -> Result<String> {
    repository.resolve_target_id(target)
}

fn validate_mountpoint(repository: &Repository, requested: &Path) -> Result<PathBuf> {
    ensure!(requested.is_absolute(), "FUSE 挂载点必须是绝对路径");
    let absolute = requested.to_path_buf();
    ensure_no_symlink_ancestors(&absolute)?;
    let mountpoint = fs::canonicalize(&absolute)
        .with_context(|| format!("无法解析 FUSE 挂载点: {}", requested.display()))?;
    let metadata = fs::symlink_metadata(&mountpoint)
        .with_context(|| format!("无法检查 FUSE 挂载点: {}", mountpoint.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "FUSE 挂载点不是普通目录: {}",
        mountpoint.display()
    );
    ensure!(
        fs::read_dir(&mountpoint)?.next().is_none(),
        "FUSE 挂载点必须为空: {}",
        mountpoint.display()
    );
    let storage_root = fs::canonicalize(repository.repository_dir())?;
    ensure!(
        !mountpoint.starts_with(&storage_root) && !storage_root.starts_with(&mountpoint),
        "FUSE 挂载点必须与仓库根目录完全分离: {}",
        mountpoint.display()
    );
    let managed_mounts = fs::canonicalize(repository.container_root().join("mounts"))?;
    let is_managed_mount = mountpoint != managed_mounts && mountpoint.starts_with(&managed_mounts);
    if !is_managed_mount {
        for workspace in repository.list_workspaces()? {
            ensure!(
                !mountpoint.starts_with(&workspace.root)
                    && !workspace.root.starts_with(&mountpoint),
                "FUSE 挂载点必须与仓库根目录完全分离: {}",
                mountpoint.display()
            );
        }
    }
    Ok(mountpoint)
}

fn ensure_no_symlink_ancestors(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str());
            }
            Component::CurDir => continue,
            Component::ParentDir => bail!("FUSE 挂载点不能包含 `..`: {}", path.display()),
        }
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("无法检查 FUSE 挂载点祖先: {}", current.display()))?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "FUSE 挂载点祖先不能是符号链接: {}",
            current.display()
        );
    }
    Ok(())
}

#[cfg(all(feature = "fuse-mount", any(target_os = "linux", target_os = "macos")))]
pub(crate) use platform::{mount_foreground, unmount};

#[cfg(not(feature = "fuse-mount"))]
pub(crate) fn mount_foreground(
    _mount: ReadOnlyMount,
    _shutdown: std::sync::mpsc::Receiver<()>,
) -> Result<()> {
    bail!("当前二进制未启用 FUSE；请使用 `--features fuse-mount` 重新构建")
}

#[cfg(not(feature = "fuse-mount"))]
pub(crate) fn unmount(_mountpoint: &Path) -> Result<()> {
    bail!("当前二进制未启用 FUSE；请使用 `--features fuse-mount` 重新构建")
}

#[cfg(all(
    feature = "fuse-mount",
    not(any(target_os = "linux", target_os = "macos"))
))]
pub(crate) fn mount_foreground(
    _mount: ReadOnlyMount,
    _shutdown: std::sync::mpsc::Receiver<()>,
) -> Result<()> {
    bail!("当前平台不支持 NeoEngram FUSE；仅支持 Linux FUSE3 和 macOS macFUSE")
}

#[cfg(all(
    feature = "fuse-mount",
    not(any(target_os = "linux", target_os = "macos"))
))]
pub(crate) fn unmount(_mountpoint: &Path) -> Result<()> {
    bail!("当前平台不支持 NeoEngram FUSE；仅支持 Linux FUSE3 和 macOS macFUSE")
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor};

    use crate::local::model::{Chunk, ChunkingStrategy, Commit, FileNode, Tree};
    use anyhow::Result;

    use crate::local::{objects::ObjectSpec, repository::Repository};

    use super::{derive_inode, ActiveNode, MountNode, NodeKind, ReadOnlyMount, ROOT_INODE};

    struct TestMount {
        _temporary: tempfile::TempDir,
        mount: ReadOnlyMount,
    }

    fn create_test_mount(cache_bytes: u64) -> Result<TestMount> {
        let temporary = tempfile::tempdir_in(std::env::current_dir()?)?;
        let repository_root = temporary.path().join("repository");
        let mountpoint = temporary.path().join("mount");
        fs::create_dir(&repository_root)?;
        fs::create_dir(&mountpoint)?;
        let repository = Repository::open_or_create(repository_root)?;
        repository.initialize_layout()?;

        let mut offset = 0_u64;
        let mut chunks = Vec::new();
        for payload in [b"abc".as_slice(), b"defgh".as_slice()] {
            let id = blake3::hash(payload).to_hex().to_string();
            let spec = ObjectSpec::new(id.clone(), payload.len() as u64)?;
            repository
                .object_store()
                .put_from(&spec, &mut Cursor::new(payload))?;
            chunks.push(Chunk {
                hash: id,
                offset,
                size: payload.len() as u64,
            });
            offset += payload.len() as u64;
        }
        let tree = Tree {
            files: vec![
                FileNode {
                    path: "empty".to_owned(),
                    total_size: 0,
                    chunking: ChunkingStrategy::FastCdc,
                    chunks: Vec::new(),
                },
                FileNode {
                    path: "nested/file.bin".to_owned(),
                    total_size: offset,
                    chunking: ChunkingStrategy::FastCdc,
                    chunks,
                },
            ],
        };
        let tree_hash = repository.store_tree(&tree)?;
        let commit_id = repository.store_commit(&Commit {
            root_directory_id: tree_hash,
            parent: None,
            message: "mount fixture".to_owned(),
            created_at_unix_ms: 1_700_000_000_000,
        })?;
        let mount = ReadOnlyMount::prepare(repository, &commit_id, &mountpoint, cache_bytes)?;
        Ok(TestMount {
            _temporary: temporary,
            mount,
        })
    }

    #[test]
    fn derived_inodes_are_stable_and_reserved_root_is_avoided() {
        let commit = "a".repeat(64);
        let first = derive_inode(&commit, "models/a.bin", NodeKind::File, 0);
        assert_eq!(
            first,
            derive_inode(&commit, "models/a.bin", NodeKind::File, 0)
        );
        assert_ne!(
            first,
            derive_inode(&commit, "models/b.bin", NodeKind::File, 0)
        );
        assert_ne!(first, ROOT_INODE);
    }

    #[test]
    fn inode_collision_uses_a_salted_fallback() -> Result<()> {
        let mut fixture = create_test_mount(16)?;
        let path = "collision-target";
        let colliding = derive_inode(fixture.mount.commit_id(), path, NodeKind::File, 0);
        fixture.mount.inodes.by_inode.insert(
            colliding,
            ActiveNode {
                node: MountNode {
                    inode: colliding,
                    parent: ROOT_INODE,
                    path: "different-path".to_owned(),
                    kind: NodeKind::File,
                    target_id: "a".repeat(64),
                    size: 0,
                },
                lookups: 1,
                opens: 0,
            },
        );
        assert_eq!(
            fixture.mount.preview_inode(path, NodeKind::File),
            derive_inode(fixture.mount.commit_id(), path, NodeKind::File, 1)
        );
        Ok(())
    }

    #[test]
    fn nested_lookup_and_directory_cookies_are_stable() -> Result<()> {
        let mut fixture = create_test_mount(16)?;
        assert_eq!(fixture.mount.root_file_count(), 2);
        assert_eq!(fixture.mount.root_total_size(), 8);

        let page = fixture.mount.read_directory(ROOT_INODE, None)?;
        let names = page
            .entries
            .iter()
            .map(|(node, cookie)| (node.path.as_str(), *cookie))
            .collect::<Vec<_>>();
        assert_eq!(names, vec![("empty", 3), ("nested", 4)]);
        assert!(page.next.is_none());

        let nested = fixture
            .mount
            .lookup(ROOT_INODE, "nested")?
            .expect("nested directory");
        assert_eq!(nested.kind, NodeKind::Directory);
        let repeated = fixture
            .mount
            .lookup(ROOT_INODE, "nested")?
            .expect("nested directory");
        assert_eq!(nested.inode, repeated.inode);
        let file = fixture
            .mount
            .lookup(nested.inode, "file.bin")?
            .expect("nested file");
        assert_eq!(file.kind, NodeKind::File);
        assert_eq!(file.size, 8);
        Ok(())
    }

    #[test]
    fn range_reads_cross_chunks_and_handle_eof_and_empty_files() -> Result<()> {
        let mut fixture = create_test_mount(16)?;
        let nested = fixture
            .mount
            .lookup(ROOT_INODE, "nested")?
            .expect("nested directory");
        let file = fixture
            .mount
            .lookup(nested.inode, "file.bin")?
            .expect("nested file");
        let handle = fixture.mount.open_file(file.inode)?;
        assert_eq!(fixture.mount.read_file(handle, 2, 4)?, b"cdef");
        assert_eq!(fixture.mount.read_file(handle, 8, 100)?, b"");
        assert_eq!(fixture.mount.read_file(handle, 7, 100)?, b"h");
        fixture.mount.release_file(handle)?;

        let empty = fixture
            .mount
            .lookup(ROOT_INODE, "empty")?
            .expect("empty file");
        let handle = fixture.mount.open_file(empty.inode)?;
        assert_eq!(fixture.mount.read_file(handle, 0, 4096)?, b"");
        fixture.mount.release_file(handle)?;
        Ok(())
    }

    #[test]
    fn oversized_whole_file_is_rejected_before_cache_allocation() -> Result<()> {
        let temporary = tempfile::tempdir_in(std::env::current_dir()?)?;
        let repository_root = temporary.path().join("repository");
        let mountpoint = temporary.path().join("mount");
        fs::create_dir(&repository_root)?;
        fs::create_dir(&mountpoint)?;
        let repository = Repository::at_with_chunking(
            repository_root,
            crate::local::repository::ChunkingPolicy::WholeFile,
        )?;
        repository.initialize_layout()?;
        let payload = b"whole file exceeds cache";
        let id = blake3::hash(payload).to_hex().to_string();
        repository.object_store().put_from(
            &ObjectSpec::new(id.clone(), payload.len() as u64)?,
            &mut Cursor::new(payload),
        )?;
        let root = repository.store_tree(&Tree {
            files: vec![FileNode {
                path: "model.bin".to_owned(),
                total_size: payload.len() as u64,
                chunking: ChunkingStrategy::WholeFile,
                chunks: vec![Chunk {
                    hash: id,
                    offset: 0,
                    size: payload.len() as u64,
                }],
            }],
        })?;
        let commit = repository.store_commit(&Commit {
            root_directory_id: root,
            parent: None,
            message: "whole file".to_owned(),
            created_at_unix_ms: 1,
        })?;
        let mut readable = ReadOnlyMount::prepare(
            repository.clone(),
            &commit,
            &mountpoint,
            payload.len() as u64,
        )?;
        let readable_file = readable
            .lookup(ROOT_INODE, "model.bin")?
            .expect("readable model file");
        let readable_handle = readable.open_file(readable_file.inode)?;
        assert_eq!(
            readable.read_file(readable_handle, 0, payload.len() as u32)?,
            payload
        );

        let mut mount = ReadOnlyMount::prepare(repository, &commit, &mountpoint, 4)?;
        let file = mount.lookup(ROOT_INODE, "model.bin")?.expect("model file");
        let handle = mount.open_file(file.inode)?;
        let error = mount
            .read_file(handle, 0, 1)
            .expect_err("oversized WholeFile was loaded");
        assert!(format!("{error:#}").contains("超过 FUSE Chunk 缓存上限"));
        Ok(())
    }

    #[test]
    fn cache_is_byte_bounded_and_evicts_least_recently_used_chunk() -> Result<()> {
        let mut fixture = create_test_mount(5)?;
        let nested = fixture
            .mount
            .lookup(ROOT_INODE, "nested")?
            .expect("nested directory");
        let file = fixture
            .mount
            .lookup(nested.inode, "file.bin")?
            .expect("nested file");
        let handle = fixture.mount.open_file(file.inode)?;
        assert_eq!(fixture.mount.read_file(handle, 0, 8)?, b"abcdefgh");
        let state = fixture.mount.cache.state.lock().expect("cache lock");
        assert_eq!(state.bytes, 5);
        assert_eq!(state.entries.len(), 1);
        assert!(state
            .entries
            .contains_key(&blake3::hash(b"defgh").to_hex().to_string()));
        drop(state);
        fixture.mount.release_file(handle)?;
        Ok(())
    }

    #[test]
    fn forget_reaps_inode_only_after_the_last_open_handle_closes() -> Result<()> {
        let mut fixture = create_test_mount(16)?;
        let file = fixture
            .mount
            .lookup(ROOT_INODE, "empty")?
            .expect("empty file");
        let handle = fixture.mount.open_file(file.inode)?;
        fixture.mount.forget(file.inode, 1);
        assert!(fixture.mount.node(file.inode).is_some());
        fixture.mount.release_file(handle)?;
        assert!(fixture.mount.node(file.inode).is_none());
        Ok(())
    }

    #[test]
    fn directory_pages_resume_after_persistent_ordinals() -> Result<()> {
        let temporary = tempfile::tempdir_in(std::env::current_dir()?)?;
        let repository_root = temporary.path().join("repository");
        let mountpoint = temporary.path().join("mount");
        fs::create_dir(&repository_root)?;
        fs::create_dir(&mountpoint)?;
        let repository = Repository::open_or_create(repository_root)?;
        repository.initialize_layout()?;
        let tree = Tree {
            files: (0..513)
                .map(|index| FileNode {
                    path: format!("files/{index:04}"),
                    total_size: 0,
                    chunking: ChunkingStrategy::FastCdc,
                    chunks: Vec::new(),
                })
                .collect(),
        };
        let root_id = repository.store_tree(&tree)?;
        let commit_id = repository.store_commit(&Commit {
            root_directory_id: root_id,
            parent: None,
            message: "paged directory".to_owned(),
            created_at_unix_ms: 1,
        })?;
        let mut mount = ReadOnlyMount::prepare(repository, &commit_id, &mountpoint, 16)?;
        let directory = mount.lookup(ROOT_INODE, "files")?.expect("files directory");
        let first = mount.read_directory(directory.inode, None)?;
        assert_eq!(first.entries.len(), 512);
        assert_eq!(first.entries.first().expect("first entry").1, 3);
        assert_eq!(first.entries.last().expect("last entry").1, 514);
        assert_eq!(first.next, Some(511));
        let second = mount.read_directory(directory.inode, first.next)?;
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].1, 515);
        assert!(second.next.is_none());
        Ok(())
    }

    #[test]
    fn prepared_head_remains_fixed_after_the_branch_advances() -> Result<()> {
        let temporary = tempfile::tempdir_in(std::env::current_dir()?)?;
        let repository_root = temporary.path().join("repository");
        let mountpoint = temporary.path().join("mount");
        fs::create_dir(&repository_root)?;
        fs::create_dir(&mountpoint)?;
        let repository = Repository::open_or_create(repository_root)?;
        repository.initialize_layout()?;

        let first_root = repository.store_tree(&Tree {
            files: vec![FileNode {
                path: "first".to_owned(),
                total_size: 0,
                chunking: ChunkingStrategy::FastCdc,
                chunks: Vec::new(),
            }],
        })?;
        let first_id = repository.store_commit(&Commit {
            root_directory_id: first_root,
            parent: None,
            message: "first".to_owned(),
            created_at_unix_ms: 1,
        })?;
        let empty_head = repository.current_head()?;
        repository.advance_head(&empty_head, &first_id)?;
        let mut mount = ReadOnlyMount::prepare(repository.clone(), "HEAD", &mountpoint, 16)?;

        let second_root = repository.store_tree(&Tree {
            files: vec![FileNode {
                path: "second".to_owned(),
                total_size: 0,
                chunking: ChunkingStrategy::FastCdc,
                chunks: Vec::new(),
            }],
        })?;
        let second_id = repository.store_commit(&Commit {
            root_directory_id: second_root,
            parent: Some(first_id.clone()),
            message: "second".to_owned(),
            created_at_unix_ms: 2,
        })?;
        let first_head = repository.current_head()?;
        repository.advance_head(&first_head, &second_id)?;

        assert_eq!(mount.commit_id(), first_id);
        assert!(mount.lookup(ROOT_INODE, "first")?.is_some());
        assert!(mount.lookup(ROOT_INODE, "second")?.is_none());
        Ok(())
    }
}
