#[cfg(not(target_os = "macos"))]
use super::{MountOption, fuse2_sys::*, with_fuse_args};
#[cfg(target_os = "macos")]
use super::{MountOption, mount_options::option_to_string};
#[cfg(not(target_os = "macos"))]
use log::warn;
use std::{ffi::CString, io, path::Path};
#[cfg(not(target_os = "macos"))]
use std::{
    fs::File,
    os::unix::prelude::{FromRawFd, OsStrExt},
    sync::Arc,
};

#[cfg(target_os = "macos")]
use crate::channel::{Channel, MacFuseMount};

#[derive(Debug)]
pub struct Mount {
    #[cfg(not(target_os = "macos"))]
    mountpoint: CString,
    #[cfg(target_os = "macos")]
    inner: MacFuseMount,
}

impl Mount {
    #[cfg(not(target_os = "macos"))]
    pub fn new(mountpoint: &Path, options: &[MountOption]) -> io::Result<(Arc<File>, Mount)> {
        let mountpoint = CString::new(mountpoint.as_os_str().as_bytes()).unwrap();
        with_fuse_args(options, |args| {
            let fd = unsafe { fuse_mount_compat25(mountpoint.as_ptr(), args) };
            if fd < 0 {
                Err(ensure_last_os_error())
            } else {
                let file = unsafe { File::from_raw_fd(fd) };
                Ok((Arc::new(file), Mount { mountpoint }))
            }
        })
    }

    #[cfg(target_os = "macos")]
    pub fn new(mountpoint: &Path, options: &[MountOption]) -> io::Result<(Channel, Mount)> {
        use std::os::unix::ffi::OsStrExt;

        let mountpoint = CString::new(mountpoint.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "macFUSE mountpoint contains an interior NUL",
            )
        })?;
        let quiet = options
            .iter()
            .any(|option| matches!(option, MountOption::CUSTOM(value) if value == "quiet"));
        let options = macfuse_mount_options(options);
        let options = CString::new(options).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "macFUSE mount options contain an interior NUL",
            )
        })?;
        let (channel, inner) = Channel::mount_macfuse(&mountpoint, &options, quiet)?;
        Ok((channel, Mount { inner }))
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn wait_until_ready(&self) -> io::Result<()> {
        self.inner.wait_until_ready()
    }
}

#[cfg(not(target_os = "macos"))]
impl Drop for Mount {
    fn drop(&mut self) {
        use std::io::ErrorKind::PermissionDenied;

        if let Err(error) = super::libc_umount(&self.mountpoint) {
            // Linux always returns EPERM for non-root users. We have to let the
            // library go through the setuid-root helper to unmount.
            if error.kind() == PermissionDenied {
                #[cfg(not(any(
                    target_os = "freebsd",
                    target_os = "dragonfly",
                    target_os = "openbsd",
                    target_os = "netbsd"
                )))]
                unsafe {
                    fuse_unmount_compat22(self.mountpoint.as_ptr());
                    return;
                }
            }
            warn!("umount failed with {error:?}");
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn ensure_last_os_error() -> io::Error {
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(0) => io::Error::other("Unspecified Error"),
        _ => error,
    }
}

#[cfg(target_os = "macos")]
fn macfuse_mount_options(options: &[MountOption]) -> String {
    options
        .iter()
        .filter_map(|option| match option {
            // libfuse consumes these before invoking MFMount.
            MountOption::Subtype(_) | MountOption::AutoUnmount => None,
            MountOption::CUSTOM(value) if value == "quiet" || value.starts_with("backend=") => None,
            option => Some(option_to_string(option)),
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::{MountOption, macfuse_mount_options};

    #[test]
    fn fskit_backend_and_libfuse_only_options_are_not_forwarded() {
        let options = macfuse_mount_options(&[
            MountOption::FSName("neoengram".to_owned()),
            MountOption::Subtype("neoengram".to_owned()),
            MountOption::RO,
            MountOption::AutoUnmount,
            MountOption::CUSTOM("backend=fskit".to_owned()),
        ]);

        assert_eq!(options, "fsname=neoengram,ro");
    }
}
