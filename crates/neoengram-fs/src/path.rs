use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use neoengram_core::LogicalPath;
use neoengram_engine::{EngineError, EngineResult, ErrorCode};

use crate::io_error;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

/// A canonical ordinary directory used as the root of safe logical-path resolution.
///
/// Resolution rejects symlink components and rechecks the root identity. The resulting path is
/// suitable for a cooperative Agent runtime; storage-side fencing remains a separate requirement.
#[derive(Debug, Clone)]
pub struct VerifiedRoot {
    path: Arc<PathBuf>,
    identity: RootIdentity,
}

impl VerifiedRoot {
    pub fn open(path: impl AsRef<Path>) -> EngineResult<Self> {
        let requested = path.as_ref();
        let metadata = fs::symlink_metadata(requested)
            .map_err(|error| io_error(error, "failed to inspect filesystem root"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::InvalidPath,
                "filesystem root must be an ordinary directory",
            ));
        }
        let canonical = fs::canonicalize(requested)
            .map_err(|error| io_error(error, "failed to canonicalize filesystem root"))?;
        let canonical_metadata = fs::symlink_metadata(&canonical)
            .map_err(|error| io_error(error, "failed to inspect canonical filesystem root"))?;
        if !canonical_metadata.is_dir() || canonical_metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::InvalidPath,
                "canonical filesystem root must be an ordinary directory",
            ));
        }
        Ok(Self {
            path: Arc::new(canonical),
            identity: RootIdentity {
                #[cfg(unix)]
                device: canonical_metadata.dev(),
                #[cfg(unix)]
                inode: canonical_metadata.ino(),
            },
        })
    }

    pub fn create(path: impl AsRef<Path>) -> EngineResult<Self> {
        fs::create_dir_all(path.as_ref())
            .map_err(|error| io_error(error, "failed to create filesystem root"))?;
        Self::open(path)
    }

    #[must_use]
    pub fn as_path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn verify_identity(&self) -> EngineResult<()> {
        let metadata = fs::symlink_metadata(self.as_path())
            .map_err(|error| io_error(error, "failed to recheck filesystem root"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::FenceRejected,
                "filesystem root identity is no longer an ordinary directory",
            ));
        }
        #[cfg(unix)]
        if metadata.dev() != self.identity.device || metadata.ino() != self.identity.inode {
            return Err(EngineError::new(
                ErrorCode::FenceRejected,
                "filesystem root identity changed",
            ));
        }
        Ok(())
    }

    /// Resolves an existing logical path and rejects every symlink in the traversal.
    pub fn resolve_existing(&self, logical: &LogicalPath) -> EngineResult<PathBuf> {
        self.verify_identity()?;
        let mut current = self.as_path().to_path_buf();
        let component_count = logical.components().count();
        for (index, component) in logical.components().enumerate() {
            current.push(component);
            let metadata = fs::symlink_metadata(&current).map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    EngineError::new(ErrorCode::ResourceNotFound, "logical path does not exist")
                } else {
                    io_error(error, "failed to inspect logical path")
                }
            })?;
            if metadata.file_type().is_symlink() {
                return Err(EngineError::new(
                    ErrorCode::InvalidPath,
                    "logical path traverses a symlink",
                ));
            }
            if index + 1 < component_count && !metadata.is_dir() {
                return Err(EngineError::new(
                    ErrorCode::InvalidPath,
                    "logical path ancestor is not a directory",
                ));
            }
        }
        Ok(current)
    }

    /// Resolves a destination whose parent must already be a safe ordinary directory.
    pub fn resolve_for_create(&self, logical: &LogicalPath) -> EngineResult<PathBuf> {
        self.verify_identity()?;
        let mut current = self.as_path().to_path_buf();
        let components = logical.components().collect::<Vec<_>>();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            current.push(component);
            let metadata = fs::symlink_metadata(&current).map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    EngineError::new(
                        ErrorCode::ResourceNotFound,
                        "logical path parent does not exist",
                    )
                } else {
                    io_error(error, "failed to inspect logical path parent")
                }
            })?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(EngineError::new(
                    ErrorCode::InvalidPath,
                    "logical path parent is not a safe directory",
                ));
            }
        }
        current.push(components.last().expect("LogicalPath is non-empty"));
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(EngineError::new(
                ErrorCode::InvalidPath,
                "logical destination is a symlink",
            )),
            Ok(_) => Ok(current),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(current),
            Err(error) => Err(io_error(error, "failed to inspect logical destination")),
        }
    }

    pub fn create_dir_all(&self, logical: &LogicalPath) -> EngineResult<PathBuf> {
        self.verify_identity()?;
        let mut current = self.as_path().to_path_buf();
        for component in logical.components() {
            current.push(component);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => {
                    return Err(EngineError::new(
                        ErrorCode::InvalidPath,
                        "logical directory component is not an ordinary directory",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match fs::create_dir(&current) {
                        Ok(()) => {
                            let parent = current.parent().ok_or_else(|| {
                                EngineError::new(
                                    ErrorCode::InvalidPath,
                                    "logical directory has no parent",
                                )
                            })?;
                            crate::sync_directory(parent)?;
                        }
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                            let metadata = fs::symlink_metadata(&current).map_err(|error| {
                                io_error(error, "failed to inspect concurrently created directory")
                            })?;
                            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                                return Err(EngineError::new(
                                    ErrorCode::InvalidPath,
                                    "concurrently created path is not a safe directory",
                                ));
                            }
                        }
                        Err(error) => {
                            return Err(io_error(error, "failed to create logical directory"));
                        }
                    }
                }
                Err(error) => {
                    return Err(io_error(error, "failed to inspect logical directory"));
                }
            }
        }
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use neoengram_core::LogicalPath;

    use super::*;

    #[test]
    fn rejects_escape_and_symlink_traversal() {
        let temporary = tempfile::tempdir().unwrap();
        fs::create_dir(temporary.path().join("safe")).unwrap();
        let root = VerifiedRoot::open(temporary.path()).unwrap();
        assert!(LogicalPath::parse("../escape").is_err());

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(temporary.path(), temporary.path().join("safe/link"))
                .unwrap();
            let path = LogicalPath::parse("safe/link/file").unwrap();
            assert_eq!(
                root.resolve_existing(&path).unwrap_err().code(),
                ErrorCode::InvalidPath
            );
        }
    }

    #[test]
    fn creates_only_validated_directory_components() {
        let temporary = tempfile::tempdir().unwrap();
        let root = VerifiedRoot::open(temporary.path()).unwrap();
        let path = LogicalPath::parse("a/b/c").unwrap();
        assert_eq!(
            root.create_dir_all(&path).unwrap(),
            root.as_path().join("a/b/c")
        );
        assert!(root.resolve_existing(&path).unwrap().is_dir());
    }
}
