use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, ensure, Context, Result};
use fuser::{
    consts::FOPEN_KEEP_CACHE, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite,
    ReplyXattr, Request, TimeOrNow,
};
use libc::{
    EACCES, EBADF, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, ENOTSUP, EROFS, O_ACCMODE, O_RDONLY,
    O_TRUNC, R_OK, W_OK, X_OK,
};

use super::{ensure_no_symlink_ancestors, MountNode, NodeKind, ReadOnlyMount};

const ATTRIBUTE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const BLOCK_SIZE: u64 = 4_096;

#[derive(Debug)]
struct NeoEngramFuse {
    mount: ReadOnlyMount,
    uid: u32,
    gid: u32,
    timestamp: SystemTime,
}

impl NeoEngramFuse {
    fn new(mount: ReadOnlyMount) -> Result<Self> {
        let metadata = fs::metadata(mount.mountpoint()).with_context(|| {
            format!(
                "无法读取 FUSE 挂载点所有者: {}",
                mount.mountpoint().display()
            )
        })?;
        let (uid, gid) = mountpoint_owner(&metadata);
        let timestamp = UNIX_EPOCH
            .checked_add(Duration::from_millis(mount.created_at_unix_ms()))
            .context("Commit 时间超出当前平台 SystemTime 范围")?;
        Ok(Self {
            mount,
            uid,
            gid,
            timestamp,
        })
    }

    fn attr(&self, node: &MountNode) -> FileAttr {
        let (kind, perm, size, nlink) = match node.kind {
            NodeKind::File => (FileType::RegularFile, 0o444, node.size, 1),
            NodeKind::Directory => (FileType::Directory, 0o555, 0, 2),
        };
        FileAttr {
            ino: node.inode,
            size,
            blocks: size.div_ceil(512),
            atime: self.timestamp,
            mtime: self.timestamp,
            ctime: self.timestamp,
            crtime: self.timestamp,
            kind,
            perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: u32::try_from(BLOCK_SIZE).expect("BLOCK_SIZE fits u32"),
            flags: 0,
        }
    }

    fn storage_error(operation: &str, error: &anyhow::Error) {
        eprintln!("NeoEngram FUSE {operation} failed: {error:#}");
    }
}

impl Filesystem for NeoEngramFuse {
    fn destroy(&mut self) {}

    fn lookup(&mut self, _request: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(parent_node) = self.mount.node(parent) else {
            reply.error(ENOENT);
            return;
        };
        if parent_node.kind != NodeKind::Directory {
            reply.error(ENOTDIR);
            return;
        }
        let Some(name) = name.to_str() else {
            reply.error(ENOENT);
            return;
        };
        match self.mount.lookup(parent, name) {
            Ok(Some(node)) => reply.entry(&ATTRIBUTE_TTL, &self.attr(&node), 0),
            Ok(None) => reply.error(ENOENT),
            Err(error) => {
                Self::storage_error("lookup", &error);
                reply.error(EIO);
            }
        }
    }

    fn forget(&mut self, _request: &Request<'_>, inode: u64, lookups: u64) {
        self.mount.forget(inode, lookups);
    }

    fn getattr(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        _handle: Option<u64>,
        reply: ReplyAttr,
    ) {
        match self.mount.node(inode) {
            Some(node) => reply.attr(&ATTRIBUTE_TTL, &self.attr(&node)),
            None => reply.error(ENOENT),
        }
    }

    fn setattr(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _handle: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(EROFS);
    }

    fn readlink(&mut self, _request: &Request<'_>, _inode: u64, reply: ReplyData) {
        reply.error(EINVAL);
    }

    fn mknod(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn mkdir(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn unlink(&mut self, _request: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    fn rmdir(&mut self, _request: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    fn symlink(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn rename(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _new_parent: u64,
        _new_name: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(EROFS);
    }

    fn link(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _new_parent: u64,
        _new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn open(&mut self, _request: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
        let Some(node) = self.mount.node(inode) else {
            reply.error(ENOENT);
            return;
        };
        if node.kind == NodeKind::Directory {
            reply.error(EISDIR);
            return;
        }
        if flags & O_ACCMODE != O_RDONLY || flags & O_TRUNC != 0 {
            reply.error(EROFS);
            return;
        }
        match self.mount.open_file(inode) {
            Ok(handle) => reply.opened(handle, FOPEN_KEEP_CACHE),
            Err(error) => {
                Self::storage_error("open", &error);
                reply.error(EIO);
            }
        }
    }

    fn read(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        handle: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        match self.mount.file_handle_inode(handle) {
            Ok(open_inode) if open_inode == inode => {}
            Ok(_) | Err(_) => {
                reply.error(EBADF);
                return;
            }
        }
        match self.mount.read_file(handle, offset as u64, size) {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => {
                Self::storage_error("read", &error);
                reply.error(EIO);
            }
        }
    }

    fn write(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _handle: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        reply.error(EROFS);
    }

    fn flush(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _handle: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        handle: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if self.mount.file_handle_inode(handle).ok() != Some(inode) {
            reply.error(EBADF);
            return;
        }
        match self.mount.release_file(handle) {
            Ok(()) => reply.ok(),
            Err(error) => {
                Self::storage_error("release", &error);
                reply.error(EIO);
            }
        }
    }

    fn fsync(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _handle: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn opendir(&mut self, _request: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
        let Some(node) = self.mount.node(inode) else {
            reply.error(ENOENT);
            return;
        };
        if node.kind != NodeKind::Directory {
            reply.error(ENOTDIR);
            return;
        }
        if flags & O_ACCMODE != O_RDONLY {
            reply.error(EROFS);
            return;
        }
        match self.mount.open_directory_handle(inode) {
            Ok(handle) => reply.opened(handle, 0),
            Err(error) => {
                Self::storage_error("opendir", &error);
                reply.error(EIO);
            }
        }
    }

    fn readdir(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        handle: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        if self.mount.directory_handle_inode(handle).ok() != Some(inode) {
            reply.error(EBADF);
            return;
        }
        let Some(node) = self.mount.node(inode) else {
            reply.error(ENOENT);
            return;
        };

        if offset == 0 && reply.add(inode, 1, FileType::Directory, ".") {
            reply.ok();
            return;
        }
        if offset <= 1 && reply.add(node.parent, 2, FileType::Directory, "..") {
            reply.ok();
            return;
        }

        let mut after = if offset <= 2 {
            None
        } else {
            Some((offset as u64) - 3)
        };
        loop {
            let page = match self.mount.read_directory(inode, after) {
                Ok(page) => page,
                Err(error) => {
                    Self::storage_error("readdir", &error);
                    reply.error(EIO);
                    return;
                }
            };
            for (entry, cookie) in page.entries {
                let Ok(cookie) = i64::try_from(cookie) else {
                    reply.error(EIO);
                    return;
                };
                let kind = match entry.kind {
                    NodeKind::File => FileType::RegularFile,
                    NodeKind::Directory => FileType::Directory,
                };
                let name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
                if reply.add(entry.inode, cookie, kind, name) {
                    reply.ok();
                    return;
                }
            }
            let Some(next) = page.next else {
                break;
            };
            after = Some(next);
        }
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        handle: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        if self.mount.directory_handle_inode(handle).ok() != Some(inode) {
            reply.error(EBADF);
            return;
        }
        match self.mount.release_directory(handle) {
            Ok(()) => reply.ok(),
            Err(error) => {
                Self::storage_error("releasedir", &error);
                reply.error(EIO);
            }
        }
    }

    fn fsyncdir(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _handle: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _request: &Request<'_>, _inode: u64, reply: ReplyStatfs) {
        let blocks = self.mount.root_total_size().div_ceil(BLOCK_SIZE);
        reply.statfs(
            blocks,
            0,
            0,
            self.mount.root_file_count().saturating_add(1),
            0,
            u32::try_from(BLOCK_SIZE).expect("BLOCK_SIZE fits u32"),
            255,
            u32::try_from(BLOCK_SIZE).expect("BLOCK_SIZE fits u32"),
        );
    }

    fn setxattr(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(ENOTSUP);
    }

    fn getxattr(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(ENOTSUP);
    }

    fn listxattr(&mut self, _request: &Request<'_>, _inode: u64, _size: u32, reply: ReplyXattr) {
        reply.error(ENOTSUP);
    }

    fn removexattr(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _name: &OsStr,
        reply: ReplyEmpty,
    ) {
        reply.error(ENOTSUP);
    }

    fn access(&mut self, _request: &Request<'_>, inode: u64, mask: i32, reply: ReplyEmpty) {
        let Some(node) = self.mount.node(inode) else {
            reply.error(ENOENT);
            return;
        };
        if mask & W_OK != 0 {
            reply.error(EROFS);
        } else if (mask & X_OK != 0 && node.kind == NodeKind::File) || mask & !(R_OK | X_OK) != 0 {
            reply.error(EACCES);
        } else {
            reply.ok();
        }
    }

    fn create(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(EROFS);
    }

    fn fallocate(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _handle: u64,
        _offset: i64,
        _length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(EROFS);
    }

    fn copy_file_range(
        &mut self,
        _request: &Request<'_>,
        _inode_in: u64,
        _handle_in: u64,
        _offset_in: i64,
        _inode_out: u64,
        _handle_out: u64,
        _offset_out: i64,
        _length: u64,
        _flags: u32,
        reply: ReplyWrite,
    ) {
        reply.error(EROFS);
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&mut self, _request: &Request<'_>, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    #[cfg(target_os = "macos")]
    fn exchange(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _new_parent: u64,
        _new_name: &OsStr,
        _options: u64,
        reply: ReplyEmpty,
    ) {
        reply.error(EROFS);
    }
}

#[cfg(unix)]
fn mountpoint_owner(metadata: &fs::Metadata) -> (u32, u32) {
    use std::os::unix::fs::MetadataExt;
    (metadata.uid(), metadata.gid())
}

pub(crate) fn mount_foreground(mount: ReadOnlyMount, shutdown: Receiver<()>) -> Result<()> {
    #[cfg(target_os = "macos")]
    ensure!(
        Path::new("/Library/Filesystems/macfuse.fs").exists(),
        "未安装 macFUSE；请先安装并允许其系统扩展，再使用 `--features fuse-mount` 构建"
    );

    let mountpoint = mount.mountpoint().to_path_buf();
    let filesystem = NeoEngramFuse::new(mount)?;
    let options = [
        MountOption::FSName("neoengram".to_owned()),
        MountOption::Subtype("neoengram".to_owned()),
        MountOption::RO,
        MountOption::DefaultPermissions,
        MountOption::NoDev,
        MountOption::NoSuid,
        MountOption::NoExec,
        MountOption::NoAtime,
    ];
    let mut session = fuser::Session::new(filesystem, &mountpoint, &options)
        .with_context(|| format!("无法挂载 NeoEngram FUSE: {}", mountpoint.display()))?;
    let mut unmounter = session.unmount_callable();
    let (done_tx, done_rx) = mpsc::channel();
    let watcher = thread::spawn(move || loop {
        match shutdown.recv_timeout(Duration::from_millis(100)) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                let _ = unmounter.unmount();
                break;
            }
            Err(RecvTimeoutError::Timeout) => {
                if done_rx.try_recv().is_ok() {
                    break;
                }
            }
        }
    });
    let result = session
        .run()
        .with_context(|| format!("NeoEngram FUSE session 失败: {}", mountpoint.display()));
    let _ = done_tx.send(());
    watcher
        .join()
        .map_err(|_| anyhow::anyhow!("FUSE 信号监听线程异常终止"))?;
    result
}

pub(crate) fn unmount(requested: &Path) -> Result<()> {
    let absolute = absolute_path(requested)?;
    ensure_no_symlink_ancestors(&absolute)?;
    let mountpoint = fs::canonicalize(&absolute)
        .with_context(|| format!("无法解析卸载路径: {}", requested.display()))?;
    let metadata = fs::symlink_metadata(&mountpoint)
        .with_context(|| format!("无法检查卸载路径: {}", mountpoint.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "卸载路径不是普通目录: {}",
        mountpoint.display()
    );
    verify_neoengram_mount(&mountpoint)?;

    #[cfg(target_os = "linux")]
    let mut command = Command::new("fusermount3");
    #[cfg(target_os = "linux")]
    command.arg("-u").arg(&mountpoint);

    #[cfg(target_os = "macos")]
    let mut command = Command::new("/sbin/umount");
    #[cfg(target_os = "macos")]
    command.arg(&mountpoint);

    let output = command
        .output()
        .with_context(|| format!("无法启动 FUSE 卸载命令: {}", mountpoint.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "无法卸载 NeoEngram FUSE {}: {}",
            mountpoint.display(),
            stderr.trim()
        );
    }
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("无法确定当前目录")?
            .join(path))
    }
}

#[cfg(target_os = "linux")]
fn verify_neoengram_mount(mountpoint: &Path) -> Result<()> {
    let contents =
        fs::read_to_string("/proc/self/mountinfo").context("无法读取 Linux mount table")?;
    let target = mountpoint.to_string_lossy();
    let record = parse_linux_mountinfo(&contents, &target)
        .with_context(|| format!("路径不是挂载点: {}", mountpoint.display()))?;
    ensure!(
        record == "fuse.neoengram",
        "拒绝卸载非 NeoEngram 文件系统 {}（类型 {record}）",
        mountpoint.display()
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_neoengram_mount(mountpoint: &Path) -> Result<()> {
    let output = Command::new("/sbin/mount")
        .output()
        .context("无法读取 macOS mount table")?;
    ensure!(output.status.success(), "macOS mount table 查询失败");
    let contents = String::from_utf8(output.stdout).context("macOS mount table 不是 UTF-8")?;
    let target = mountpoint.to_string_lossy();
    let source = parse_macos_mounts(&contents, &target)
        .with_context(|| format!("路径不是挂载点: {}", mountpoint.display()))?;
    ensure!(
        source == "neoengram",
        "拒绝卸载非 NeoEngram 文件系统 {}（来源 {source}）",
        mountpoint.display()
    );
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_mountinfo<'a>(contents: &'a str, target: &str) -> Option<&'a str> {
    contents.lines().find_map(|line| {
        let (left, right) = line.split_once(" - ")?;
        let encoded_mountpoint = left.split_whitespace().nth(4)?;
        (decode_mount_field(encoded_mountpoint) == target)
            .then(|| right.split_whitespace().next())
            .flatten()
    })
}

#[cfg(any(target_os = "linux", test))]
fn decode_mount_field(field: &str) -> String {
    field
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

#[cfg(any(target_os = "macos", test))]
fn parse_macos_mounts<'a>(contents: &'a str, target: &str) -> Option<&'a str> {
    contents.lines().find_map(|line| {
        let (source, remainder) = line.split_once(" on ")?;
        let end = remainder.rfind(" (")?;
        (&remainder[..end] == target).then_some(source)
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_linux_mountinfo, parse_macos_mounts};

    #[test]
    fn linux_mountinfo_requires_exact_decoded_mountpoint() {
        let mounts = "42 31 0:52 / /tmp/neo\\040mount ro - fuse.neoengram neoengram ro\n\
                      43 31 0:53 / /tmp/other rw - ext4 /dev/sda rw\n";
        assert_eq!(
            parse_linux_mountinfo(mounts, "/tmp/neo mount"),
            Some("fuse.neoengram")
        );
        assert_eq!(parse_linux_mountinfo(mounts, "/tmp/neo"), None);
    }

    #[test]
    fn macos_mounts_require_exact_source_and_mountpoint() {
        let mounts = "neoengram on /tmp/neo mount (macfuse, local, read-only)\n\
                      /dev/disk3s1 on /Volumes/Data (apfs, local)\n";
        assert_eq!(
            parse_macos_mounts(mounts, "/tmp/neo mount"),
            Some("neoengram")
        );
        assert_eq!(parse_macos_mounts(mounts, "/tmp/neo"), None);
    }
}
