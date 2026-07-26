//! Local filesystem adapters for [`neoengram_engine`].
//!
//! Physical paths remain inside this crate. Engine callers use validated logical paths and typed
//! content identifiers.

mod local;
mod memory;
mod objects;
mod path;

pub use local::{FileJournalStore, LocalLockManager, LocalWorktree, SystemClock};
pub use memory::{
    FixedClock, MemoryCatalog, MemoryIndexSnapshot, MemoryJournalStore, MemoryObjectStore,
    MemoryWorktree, TriggeredFailureInjector,
};
pub use objects::LooseObjectStore;
pub use path::VerifiedRoot;

use std::{fs::File, io, path::Path};

use neoengram_engine::{EngineError, ErrorCode};

fn io_error(error: io::Error, message: impl Into<String>) -> EngineError {
    EngineError::new(ErrorCode::Io, message).with_source(error)
}

fn rename_error(error: io::Error) -> EngineError {
    if matches!(
        error.kind(),
        io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty
    ) {
        EngineError::new(
            ErrorCode::AlreadyExists,
            "rename destination already exists",
        )
        .with_source(error)
    } else {
        io_error(error, "no-replace rename failed")
    }
}

fn sync_directory(path: &Path) -> Result<(), EngineError> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error(error, "failed to synchronize directory"))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn rename_no_replace(source: &Path, destination: &Path) -> Result<(), EngineError> {
    #[cfg(any(
        target_vendor = "apple",
        target_os = "linux",
        target_os = "android",
        target_os = "redox"
    ))]
    {
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            source,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(io::Error::from)
        .map_err(rename_error)?;
    }

    #[cfg(windows)]
    rename_no_replace_windows(source, destination)?;

    #[cfg(not(any(
        target_vendor = "apple",
        target_os = "linux",
        target_os = "android",
        target_os = "redox",
        windows
    )))]
    return Err(EngineError::new(
        ErrorCode::StorageUnavailable,
        "atomic no-replace rename is unsupported on this platform",
    ));

    let source_parent = source
        .parent()
        .ok_or_else(|| EngineError::new(ErrorCode::InvalidPath, "rename source has no parent"))?;
    let destination_parent = destination.parent().ok_or_else(|| {
        EngineError::new(ErrorCode::InvalidPath, "rename destination has no parent")
    })?;
    sync_directory(destination_parent)?;
    if source_parent != destination_parent {
        sync_directory(source_parent)?;
    }
    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn rename_no_replace_windows(source: &Path, destination: &Path) -> Result<(), EngineError> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    if source[..source.len() - 1].contains(&0) || destination[..destination.len() - 1].contains(&0)
    {
        return Err(EngineError::new(
            ErrorCode::InvalidPath,
            "rename path contains NUL",
        ));
    }
    // SAFETY: both buffers are NUL-terminated, contain no interior NUL, and remain alive.
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(rename_error(io::Error::last_os_error()));
    }
    Ok(())
}
