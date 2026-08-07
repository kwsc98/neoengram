use std::{fs::File, io, sync::Arc};

#[cfg(not(target_os = "macos"))]
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::prelude::AsRawFd;

use libc::{c_int, c_void, size_t};

#[cfg(feature = "abi-7-40")]
use crate::passthrough::BackingId;
use crate::reply::ReplySender;

#[cfg(target_os = "macos")]
mod macfuse {
    use std::{
        ffi::{CStr, c_char, c_void},
        io, ptr,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    };

    const MOUNT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
    const MF_MOUNT_SUCCESS: i32 = 0;
    const MF_MOUNT_UNSUPPORTED_OS: i32 = 1;
    const MF_MOUNT_HELPER_INSTALLATION_FAILED: i32 = 2;
    const MF_MOUNT_EXTENSION_NOT_FOUND: i32 = 3;
    const MF_MOUNT_EXTENSION_REQUIRES_APPROVAL: i32 = 4;

    #[link(name = "MFMount", kind = "framework")]
    unsafe extern "C" {
        #[link_name = "MFChannelCreate"]
        fn mf_channel_create() -> *mut c_void;
        #[link_name = "MFChannelCopyNextMessage"]
        fn mf_channel_copy_next_message(channel: *mut c_void) -> *mut c_void;
        #[link_name = "MFChannelSendMessage"]
        fn mf_channel_send_message(
            channel: *mut c_void,
            buffers: *const libc::iovec,
            count: usize,
        ) -> isize;
        #[link_name = "MFChannelClose"]
        fn mf_channel_close(channel: *mut c_void) -> bool;
        #[link_name = "MFMessageGetBodySize"]
        fn mf_message_get_body_size(message: *mut c_void) -> isize;
        #[link_name = "MFMessageGetBodyBuffers"]
        fn mf_message_get_body_buffers(
            message: *mut c_void,
            buffers: *mut *const libc::iovec,
        ) -> isize;
        #[link_name = "MFMount"]
        fn mf_mount(
            channel: *mut c_void,
            mountpoint: *const c_char,
            options: *const c_char,
            quiet: bool,
        ) -> i32;
        #[link_name = "MFRelease"]
        fn mf_release(reference: *mut c_void);
    }

    #[derive(Debug, Clone, Copy)]
    enum MountOutcome {
        Pending,
        Finished { result: i32, os_error: i32 },
    }

    pub(super) struct ChannelInner {
        reference: *mut c_void,
        closed: AtomicBool,
        mount_outcome: Mutex<MountOutcome>,
        mount_completed: Condvar,
    }

    // MFChannel is explicitly designed for a receive thread plus concurrent reply senders.
    unsafe impl Send for ChannelInner {}
    unsafe impl Sync for ChannelInner {}

    impl std::fmt::Debug for ChannelInner {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("MacFuseChannel")
                .field("closed", &self.closed.load(Ordering::Acquire))
                .finish_non_exhaustive()
        }
    }

    impl ChannelInner {
        pub(super) fn mount(
            mountpoint: &CStr,
            options: &CStr,
            quiet: bool,
        ) -> io::Result<(Arc<Self>, JoinHandle<()>)> {
            let reference = unsafe { mf_channel_create() };
            if reference.is_null() {
                return Err(io::Error::other("MFChannelCreate() failed"));
            }

            let channel = Arc::new(Self {
                reference,
                closed: AtomicBool::new(false),
                mount_outcome: Mutex::new(MountOutcome::Pending),
                mount_completed: Condvar::new(),
            });
            let mountpoint = mountpoint.to_owned();
            let options = options.to_owned();
            let mounting_channel = Arc::clone(&channel);
            let thread = thread::Builder::new()
                .name("fuser-macfuse-mount".to_owned())
                .spawn(move || {
                    unsafe {
                        *libc::__error() = 0;
                    }
                    let result = unsafe {
                        mf_mount(
                            mounting_channel.reference,
                            mountpoint.as_ptr(),
                            options.as_ptr(),
                            quiet,
                        )
                    };
                    let os_error = io::Error::last_os_error().raw_os_error().unwrap_or(0);
                    if let Ok(mut outcome) = mounting_channel.mount_outcome.lock() {
                        *outcome = MountOutcome::Finished { result, os_error };
                        mounting_channel.mount_completed.notify_all();
                    }
                })?;

            Ok((channel, thread))
        }

        pub(super) fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
            let message = unsafe { mf_channel_copy_next_message(self.reference) };
            if message.is_null() {
                return Err(io::Error::last_os_error());
            }
            let message = Message(message);

            let body_size = unsafe { mf_message_get_body_size(message.0) };
            if body_size < 0 {
                return Err(io::Error::last_os_error());
            }
            let body_size = usize::try_from(body_size)
                .map_err(|_| io::Error::other("macFUSE returned an invalid message size"))?;
            if body_size > buffer.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "macFUSE message exceeds the fuser receive buffer",
                ));
            }

            let mut body_buffers = ptr::null();
            let body_count = unsafe { mf_message_get_body_buffers(message.0, &mut body_buffers) };
            if body_count <= 0 || body_buffers.is_null() {
                return Err(io::Error::last_os_error());
            }

            let body_buffers =
                unsafe { std::slice::from_raw_parts(body_buffers, body_count as usize) };
            let mut offset = 0usize;
            for body in body_buffers {
                let end = offset.checked_add(body.iov_len).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "macFUSE message size overflow")
                })?;
                if end > body_size || (body.iov_base.is_null() && body.iov_len != 0) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "macFUSE returned malformed message buffers",
                    ));
                }
                unsafe {
                    ptr::copy_nonoverlapping(
                        body.iov_base.cast::<u8>(),
                        buffer[offset..end].as_mut_ptr(),
                        body.iov_len,
                    );
                }
                offset = end;
            }
            if offset != body_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "macFUSE message buffers do not match the declared size",
                ));
            }
            Ok(body_size)
        }

        pub(super) fn send(&self, buffers: &[io::IoSlice<'_>]) -> io::Result<()> {
            let sent = unsafe {
                mf_channel_send_message(
                    self.reference,
                    buffers.as_ptr().cast::<libc::iovec>(),
                    buffers.len(),
                )
            };
            if sent < 0 {
                return Err(io::Error::last_os_error());
            }
            let expected = buffers.iter().map(|buffer| buffer.len()).sum::<usize>();
            if sent as usize != expected {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "macFUSE sent a partial FUSE reply",
                ));
            }
            Ok(())
        }

        pub(super) fn wait_until_mounted(&self) -> io::Result<()> {
            let outcome = self
                .mount_outcome
                .lock()
                .map_err(|_| io::Error::other("macFUSE mount state lock is poisoned"))?;
            let (outcome, timeout) = self
                .mount_completed
                .wait_timeout_while(outcome, MOUNT_WAIT_TIMEOUT, |outcome| {
                    matches!(outcome, MountOutcome::Pending)
                })
                .map_err(|_| io::Error::other("macFUSE mount state lock is poisoned"))?;
            match *outcome {
                MountOutcome::Pending if timeout.timed_out() => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "macFUSE FSKit mount did not become ready within 30 seconds",
                )),
                MountOutcome::Pending => Err(io::Error::other(
                    "macFUSE FSKit mount completion was not reported",
                )),
                MountOutcome::Finished {
                    result: MF_MOUNT_SUCCESS,
                    ..
                } => Ok(()),
                MountOutcome::Finished { result, os_error } => Err(mount_failure(result, os_error)),
            }
        }

        pub(super) fn close(&self) {
            if !self.closed.swap(true, Ordering::AcqRel) {
                unsafe {
                    let _ = mf_channel_close(self.reference);
                }
            }
        }
    }

    impl Drop for ChannelInner {
        fn drop(&mut self) {
            self.close();
            unsafe {
                mf_release(self.reference);
            }
        }
    }

    fn mount_failure(result: i32, os_error: i32) -> io::Error {
        let (kind, message) = match result {
            MF_MOUNT_UNSUPPORTED_OS => (
                io::ErrorKind::Unsupported,
                "macFUSE FSKit does not support this macOS version".to_owned(),
            ),
            MF_MOUNT_HELPER_INSTALLATION_FAILED => (
                io::ErrorKind::PermissionDenied,
                "macFUSE helper installation failed".to_owned(),
            ),
            MF_MOUNT_EXTENSION_NOT_FOUND => (
                io::ErrorKind::NotFound,
                "macFUSE FSKit extension is not registered".to_owned(),
            ),
            MF_MOUNT_EXTENSION_REQUIRES_APPROVAL => (
                io::ErrorKind::PermissionDenied,
                "macFUSE FSKit extension requires user approval".to_owned(),
            ),
            -1 if os_error != 0 => (
                io::Error::from_raw_os_error(os_error).kind(),
                format!("macFUSE FSKit mount failed with os_error={os_error}"),
            ),
            _ => (
                io::ErrorKind::Other,
                format!("macFUSE FSKit mount failed with result={result}"),
            ),
        };
        io::Error::new(kind, message)
    }

    struct Message(*mut c_void);

    impl Drop for Message {
        fn drop(&mut self) {
            unsafe {
                mf_release(self.0);
            }
        }
    }

    #[derive(Debug)]
    pub(crate) struct Mount {
        channel: Arc<ChannelInner>,
        thread: Option<JoinHandle<()>>,
    }

    impl Mount {
        pub(super) fn new(channel: Arc<ChannelInner>, thread: JoinHandle<()>) -> Self {
            Self {
                channel,
                thread: Some(thread),
            }
        }

        pub(crate) fn wait_until_ready(&self) -> io::Result<()> {
            self.channel.wait_until_mounted()
        }
    }

    impl Drop for Mount {
        fn drop(&mut self) {
            self.channel.close();
            if let Some(thread) = self.thread.take() {
                // MFChannelClose normally wakes MFMount immediately. If the framework itself is
                // wedged, do not turn the documented mount timeout into an unbounded join; the
                // worker owns an Arc and can release the channel safely when it eventually exits.
                if thread.is_finished() {
                    let _ = thread.join();
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
pub(crate) use macfuse::Mount as MacFuseMount;

/// A raw communication channel to the FUSE kernel driver.
#[derive(Debug)]
pub struct Channel(ChannelKind);

#[derive(Clone, Debug)]
enum ChannelKind {
    Device(Arc<File>),
    #[cfg(target_os = "macos")]
    MacFuse(Arc<macfuse::ChannelInner>),
}

#[cfg(not(target_os = "macos"))]
impl AsFd for Channel {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match &self.0 {
            ChannelKind::Device(device) => device.as_fd(),
        }
    }
}

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel.
    pub(crate) fn new(device: Arc<File>) -> Self {
        Self(ChannelKind::Device(device))
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn mount_macfuse(
        mountpoint: &std::ffi::CStr,
        options: &std::ffi::CStr,
        quiet: bool,
    ) -> io::Result<(Self, MacFuseMount)> {
        let (channel, thread) = macfuse::ChannelInner::mount(mountpoint, options, quiet)?;
        Ok((
            Self(ChannelKind::MacFuse(Arc::clone(&channel))),
            MacFuseMount::new(channel, thread),
        ))
    }

    /// Receives data up to the capacity of the given buffer (can block).
    pub fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
        match &self.0 {
            ChannelKind::Device(device) => {
                let rc = unsafe {
                    libc::read(
                        device.as_raw_fd(),
                        buffer.as_mut_ptr().cast::<c_void>(),
                        buffer.len() as size_t,
                    )
                };
                if rc < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(rc as usize)
                }
            }
            #[cfg(target_os = "macos")]
            ChannelKind::MacFuse(channel) => channel.receive(buffer),
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub fn sender(&self) -> ChannelSender {
        ChannelSender(self.0.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ChannelSender(ChannelKind);

impl ReplySender for ChannelSender {
    fn send(&self, buffers: &[io::IoSlice<'_>]) -> io::Result<()> {
        match &self.0 {
            ChannelKind::Device(device) => {
                let rc = unsafe {
                    libc::writev(
                        device.as_raw_fd(),
                        buffers.as_ptr().cast::<libc::iovec>(),
                        buffers.len() as c_int,
                    )
                };
                if rc < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    debug_assert_eq!(
                        buffers.iter().map(|buffer| buffer.len()).sum::<usize>(),
                        rc as usize
                    );
                    Ok(())
                }
            }
            #[cfg(target_os = "macos")]
            ChannelKind::MacFuse(channel) => channel.send(buffers),
        }
    }

    #[cfg(feature = "abi-7-40")]
    fn open_backing(&self, fd: std::os::fd::BorrowedFd<'_>) -> std::io::Result<BackingId> {
        match &self.0 {
            ChannelKind::Device(device) => BackingId::create(device, fd),
            #[cfg(target_os = "macos")]
            ChannelKind::MacFuse(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "macFUSE FSKit does not expose a backing file descriptor",
            )),
        }
    }
}
