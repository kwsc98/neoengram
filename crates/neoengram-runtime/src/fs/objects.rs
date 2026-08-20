use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::engine::{
    ContentAddressedObject, EngineError, EngineResult, ErrorCode, ObjectMetadata, ObjectPage,
    ObjectPutOutcome, ObjectSpec, ObjectStore, PageCursor, PageRequest,
};
use neoengram_domain::core::ObjectId;
use tempfile::NamedTempFile;

use super::{io_error, sync_directory, VerifiedRoot};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

const TEMPORARY_DIRECTORY: &str = ".tmp";

/// One immutable file per typed object ID under a verified local root.
#[derive(Debug, Clone)]
pub struct LooseObjectStore {
    root: VerifiedRoot,
}

impl LooseObjectStore {
    #[must_use]
    pub const fn new(root: VerifiedRoot) -> Self {
        Self { root }
    }

    pub fn open_or_create(path: impl AsRef<Path>) -> EngineResult<Self> {
        Ok(Self::new(VerifiedRoot::create(path)?))
    }

    fn temporary_dir(&self) -> PathBuf {
        self.root.as_path().join(TEMPORARY_DIRECTORY)
    }

    fn object_path(&self, id: &ObjectId) -> PathBuf {
        self.root.as_path().join(id.to_hex())
    }

    /// Seals a CAS object as read-only after verifying its content-addressed identity.
    ///
    /// The permission change is applied to the opened inode rather than to an unchecked path,
    /// so a concurrent path replacement cannot cause an unrelated file to be sealed.
    pub fn seal_read_only(&self, expected: &ObjectSpec) -> EngineResult<()> {
        self.validate_layout()?;
        let source = self.object_path(&expected.id);
        let source_path_metadata = fs::symlink_metadata(&source).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                EngineError::new(ErrorCode::ObjectMissing, "loose object is missing")
            } else {
                io_error(error, "failed to inspect loose object for sealing")
            }
        })?;
        if source_path_metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object path is a symbolic link",
            ));
        }
        let mut input = File::open(&source).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                EngineError::new(ErrorCode::ObjectMissing, "loose object is missing")
            } else {
                io_error(error, "failed to open loose object for sealing")
            }
        })?;
        let metadata = input
            .metadata()
            .map_err(|error| io_error(error, "failed to inspect loose object for sealing"))?;
        if !metadata.is_file() || metadata.len() != expected.size {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object is not a valid WholeFile object",
            ));
        }
        #[cfg(unix)]
        let source_identity = (metadata.dev(), metadata.ino());
        copy_and_hash_exact(
            &mut input,
            &mut io::sink(),
            expected,
            ErrorCode::ObjectCorrupt,
        )?;
        let checked = input.metadata().map_err(|error| {
            io_error(error, "failed to recheck loose object after sealing proof")
        })?;
        if checked.len() != expected.size {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object changed during sealing proof",
            ));
        }
        #[cfg(unix)]
        if (checked.dev(), checked.ino()) != source_identity {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object inode changed during sealing proof",
            ));
        }
        #[cfg(unix)]
        {
            input
                .set_permissions(fs::Permissions::from_mode(0o444))
                .map_err(|error| io_error(error, "failed to seal loose object read-only"))?;
            input
                .sync_all()
                .map_err(|error| io_error(error, "failed to synchronize sealed loose object"))?;
            let sealed = input
                .metadata()
                .map_err(|error| io_error(error, "failed to verify sealed loose object"))?;
            if sealed.permissions().mode() & 0o222 != 0 {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "loose object remained writable after sealing",
                ));
            }
            let source_after = fs::symlink_metadata(&source)
                .map_err(|error| io_error(error, "failed to recheck sealed loose object path"))?;
            if source_after.file_type().is_symlink()
                || source_after.dev() != sealed.dev()
                || source_after.ino() != sealed.ino()
                || source_after.len() != expected.size
                || source_after.permissions().mode() & 0o222 != 0
            {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "sealed loose object path no longer identifies the verified inode",
                ));
            }
        }
        #[cfg(not(unix))]
        {
            let mut permissions = checked.permissions();
            permissions.set_readonly(true);
            input
                .set_permissions(permissions)
                .map_err(|error| io_error(error, "failed to seal loose object read-only"))?;
            input
                .sync_all()
                .map_err(|error| io_error(error, "failed to synchronize sealed loose object"))?;
            let sealed = input
                .metadata()
                .map_err(|error| io_error(error, "failed to verify sealed loose object"))?;
            let source_after = fs::symlink_metadata(&source)
                .map_err(|error| io_error(error, "failed to recheck sealed loose object path"))?;
            if !sealed.permissions().readonly()
                || !source_after.is_file()
                || source_after.file_type().is_symlink()
                || source_after.len() != expected.size
                || !source_after.permissions().readonly()
            {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "sealed loose object path is not read-only",
                ));
            }
        }
        Ok(())
    }

    /// Creates a hard link to a sealed WholeFile object after rechecking its identity and size.
    /// The destination is never replaced and must already have an ordinary parent directory.
    ///
    /// This entry point implements `SealedAcl` and deliberately rejects writable CAS inodes. A
    /// `TrustedLocal` caller uses [`Self::hard_link_to_trusted`], which seals the verified inode
    /// before linking it.
    pub fn hard_link_to(&self, expected: &ObjectSpec, destination: &Path) -> EngineResult<()> {
        self.hard_link_to_with_seal(expected, destination, false)
    }

    /// Seals and links one object using the same opened source inode proof.
    pub fn hard_link_to_trusted(
        &self,
        expected: &ObjectSpec,
        destination: &Path,
    ) -> EngineResult<()> {
        self.hard_link_to_with_seal(expected, destination, true)
    }

    /// Verifies that `destination` is the immutable hard link for `expected`.
    pub fn verify_hard_link_to(
        &self,
        expected: &ObjectSpec,
        destination: &Path,
    ) -> EngineResult<()> {
        self.hard_link_proof(expected, destination)
    }

    fn hard_link_to_with_seal(
        &self,
        expected: &ObjectSpec,
        destination: &Path,
        seal: bool,
    ) -> EngineResult<()> {
        ensure_hardlink_identity_supported()?;
        self.validate_layout()?;
        let source = self.object_path(&expected.id);
        let source_path_metadata = fs::symlink_metadata(&source).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                EngineError::new(ErrorCode::ObjectMissing, "loose object is missing")
            } else {
                io_error(error, "failed to inspect loose object for hardlink proof")
            }
        })?;
        if source_path_metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object path is a symbolic link",
            ));
        }
        let mut input = File::open(&source).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                EngineError::new(ErrorCode::ObjectMissing, "loose object is missing")
            } else {
                io_error(error, "failed to open loose object for hardlink proof")
            }
        })?;
        let metadata = input
            .metadata()
            .map_err(|error| io_error(error, "failed to inspect loose object"))?;
        if !metadata.is_file() || metadata.len() != expected.size {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object is not a sealed WholeFile object",
            ));
        }
        // A hard link keeps the source inode alive, so verify the content-addressed identity
        // before linking and re-check the inode after the read. This closes the window where a
        // mutable or replaced CAS file could otherwise become the public read-only view.
        #[cfg(unix)]
        let source_identity = (metadata.dev(), metadata.ino());
        copy_and_hash_exact(
            &mut input,
            &mut io::sink(),
            expected,
            ErrorCode::ObjectCorrupt,
        )?;
        let after_read = input.metadata().map_err(|error| {
            io_error(
                error,
                "failed to re-check loose object after hardlink proof",
            )
        })?;
        #[cfg(unix)]
        if (after_read.dev(), after_read.ino()) != source_identity
            || after_read.len() != expected.size
        {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object changed during hardlink proof",
            ));
        }
        #[cfg(unix)]
        if !seal && after_read.permissions().mode() & 0o222 != 0 {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "HARDLINK_OBJECT_NOT_SEALED: loose object inode is writable",
            ));
        }
        #[cfg(unix)]
        if seal {
            input
                .set_permissions(fs::Permissions::from_mode(0o444))
                .map_err(|error| io_error(error, "failed to seal loose object before hardlink"))?;
            input
                .sync_all()
                .map_err(|error| io_error(error, "failed to synchronize sealed loose object"))?;
            input
                .seek(SeekFrom::Start(0))
                .map_err(|error| io_error(error, "failed to rewind sealed loose object"))?;
            copy_and_hash_exact(
                &mut input,
                &mut io::sink(),
                expected,
                ErrorCode::ObjectCorrupt,
            )?;
        }
        let sealed = input
            .metadata()
            .map_err(|error| io_error(error, "failed to inspect sealed loose object"))?;
        #[cfg(unix)]
        if sealed.permissions().mode() & 0o222 != 0 {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "HARDLINK_OBJECT_NOT_SEALED: loose object inode is writable",
            ));
        }
        let parent = destination.parent().ok_or_else(|| {
            EngineError::new(ErrorCode::InvalidPath, "hardlink destination has no parent")
        })?;
        let parent_metadata = fs::symlink_metadata(parent)
            .map_err(|error| io_error(error, "failed to inspect hardlink destination parent"))?;
        if !parent_metadata.is_dir() || parent_metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::InvalidPath,
                "hardlink destination parent is not an ordinary directory",
            ));
        }
        #[cfg(unix)]
        if parent_metadata.dev() != sealed.dev() {
            return Err(EngineError::new(
                ErrorCode::Conflict,
                "HARDLINK_CROSS_FILESYSTEM: CAS object and Delivery target are on different filesystems",
            ));
        }
        fs::hard_link(&source, destination).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                EngineError::new(ErrorCode::AlreadyExists, "hardlink destination exists")
            } else if is_cross_filesystem_error(&error) {
                EngineError::new(
                    ErrorCode::Conflict,
                    "HARDLINK_CROSS_FILESYSTEM: CAS object and Delivery target are on different filesystems",
                )
            } else {
                io_error(error, "failed to create hardlink")
            }
        })?;

        let linked = match fs::symlink_metadata(destination) {
            Ok(metadata) => metadata,
            Err(error) => {
                return Err(reject_created_hardlink(
                    destination,
                    format!("failed to inspect newly created hardlink: {error}"),
                ));
            }
        };
        #[cfg(unix)]
        let linked_identity_matches = linked.is_file()
            && linked.dev() == sealed.dev()
            && linked.ino() == sealed.ino()
            && linked.len() == expected.size
            && linked.permissions().mode() & 0o222 == 0;
        #[cfg(not(unix))]
        let linked_identity_matches = linked.is_file() && linked.len() == expected.size;
        if !linked_identity_matches {
            return Err(reject_created_hardlink(
                destination,
                "HARDLINK_OBJECT_NOT_SEALED: linked destination identity or mode mismatch",
            ));
        }

        let source_after = match fs::symlink_metadata(&source) {
            Ok(metadata) => metadata,
            Err(error) => {
                return Err(reject_created_hardlink(
                    destination,
                    format!("failed to re-check loose object path after hardlink: {error}"),
                ));
            }
        };
        #[cfg(unix)]
        if !source_after.is_file()
            || source_after.file_type().is_symlink()
            || source_after.dev() != sealed.dev()
            || source_after.ino() != sealed.ino()
            || source_after.len() != expected.size
            || source_after.permissions().mode() & 0o222 != 0
        {
            return Err(reject_created_hardlink(
                destination,
                "HARDLINK_OBJECT_NOT_SEALED: source path changed after hardlink",
            ));
        }
        Ok(())
    }

    fn hard_link_proof(&self, expected: &ObjectSpec, destination: &Path) -> EngineResult<()> {
        ensure_hardlink_identity_supported()?;
        self.validate_layout()?;
        let source = self.object_path(&expected.id);
        let source_path_metadata = fs::symlink_metadata(&source).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                EngineError::new(ErrorCode::ObjectMissing, "loose object is missing")
            } else {
                io_error(error, "failed to inspect loose object for hardlink proof")
            }
        })?;
        if source_path_metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object path is a symbolic link",
            ));
        }
        let mut input = File::open(&source)
            .map_err(|error| io_error(error, "failed to open loose object for hardlink proof"))?;
        let source_metadata = input.metadata().map_err(|error| {
            io_error(error, "failed to inspect loose object for hardlink proof")
        })?;
        if !source_metadata.is_file() || source_metadata.len() != expected.size {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object is not a valid WholeFile object",
            ));
        }
        copy_and_hash_exact(
            &mut input,
            &mut io::sink(),
            expected,
            ErrorCode::ObjectCorrupt,
        )?;
        let source_after = input
            .metadata()
            .map_err(|error| io_error(error, "failed to recheck loose object proof"))?;
        let source_path_after = fs::symlink_metadata(&source)
            .map_err(|error| io_error(error, "failed to recheck loose object path proof"))?;
        let destination_metadata = fs::symlink_metadata(destination)
            .map_err(|error| io_error(error, "failed to inspect hardlink destination"))?;
        #[cfg(unix)]
        {
            if source_after.permissions().mode() & 0o222 != 0 {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "HARDLINK_OBJECT_NOT_SEALED: loose object inode is writable",
                ));
            }
            if source_path_after.file_type().is_symlink()
                || source_path_after.dev() != source_after.dev()
                || source_path_after.ino() != source_after.ino()
                || source_path_after.len() != expected.size
            {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "HARDLINK_OBJECT_NOT_SEALED: source path changed during proof",
                ));
            }
            if !destination_metadata.is_file()
                || destination_metadata.dev() != source_after.dev()
                || destination_metadata.ino() != source_after.ino()
                || destination_metadata.len() != expected.size
                || destination_metadata.permissions().mode() & 0o222 != 0
            {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "HARDLINK_OBJECT_NOT_SEALED: linked destination identity or mode mismatch",
                ));
            }
        }
        #[cfg(not(unix))]
        if !destination_metadata.is_file() || destination_metadata.len() != expected.size {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "hardlink destination identity or mode mismatch",
            ));
        }
        Ok(())
    }

    fn stat_object(&self, id: &ObjectId) -> EngineResult<Option<ObjectMetadata>> {
        self.root.verify_identity()?;
        let path = self.object_path(id);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(EngineError::new(
                        ErrorCode::ObjectCorrupt,
                        "loose object is not an ordinary file",
                    ));
                }
                Ok(Some(ObjectMetadata {
                    id: *id,
                    size: metadata.len(),
                }))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error(error, "failed to inspect loose object")),
        }
    }
}

#[cfg(unix)]
fn ensure_hardlink_identity_supported() -> EngineResult<()> {
    Ok(())
}

#[cfg(not(unix))]
fn ensure_hardlink_identity_supported() -> EngineResult<()> {
    Err(EngineError::new(
        ErrorCode::Conflict,
        "HARDLINK_UNSAFE_VOLUME: this platform cannot prove device and inode identity",
    ))
}

fn reject_created_hardlink(destination: &Path, detail: impl Into<String>) -> EngineError {
    let detail = detail.into();
    match fs::remove_file(destination) {
        Ok(()) => EngineError::new(ErrorCode::IntegrityViolation, detail),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            EngineError::new(ErrorCode::IntegrityViolation, detail)
        }
        Err(error) => EngineError::new(
            ErrorCode::IntegrityViolation,
            format!("{detail}; failed to remove unsafe hardlink destination: {error}"),
        )
        .with_source(error),
    }
}

#[cfg(unix)]
fn is_cross_filesystem_error(error: &io::Error) -> bool {
    error.raw_os_error() == Some(rustix::io::Errno::XDEV.raw_os_error())
}

#[cfg(not(unix))]
fn is_cross_filesystem_error(_error: &io::Error) -> bool {
    false
}

impl ObjectStore for LooseObjectStore {
    fn initialize(&self) -> EngineResult<()> {
        self.root.verify_identity()?;
        match fs::symlink_metadata(self.temporary_dir()) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
            Ok(_) => Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "loose object temporary path is not an ordinary directory",
            )),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(self.temporary_dir()) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let metadata =
                            fs::symlink_metadata(self.temporary_dir()).map_err(|error| {
                                io_error(error, "failed to inspect loose object temp directory")
                            })?;
                        if !metadata.is_dir() || metadata.file_type().is_symlink() {
                            return Err(EngineError::new(
                                ErrorCode::IntegrityViolation,
                                "loose object temporary path is invalid",
                            ));
                        }
                    }
                    Err(error) => {
                        return Err(io_error(
                            error,
                            "failed to create loose object temp directory",
                        ));
                    }
                }
                sync_directory(self.root.as_path())
            }
            Err(error) => Err(io_error(
                error,
                "failed to inspect loose object temp directory",
            )),
        }
    }

    fn validate_layout(&self) -> EngineResult<()> {
        self.root.verify_identity()?;
        let metadata = fs::symlink_metadata(self.temporary_dir())
            .map_err(|error| io_error(error, "loose object store is not initialized"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "loose object temporary path is invalid",
            ));
        }
        Ok(())
    }

    fn put_from(
        &self,
        expected: &ObjectSpec,
        source: &mut dyn Read,
    ) -> EngineResult<ObjectPutOutcome> {
        self.initialize()?;
        if self.stat_object(&expected.id)?.is_some() {
            self.verify(expected)?;
            return Ok(ObjectPutOutcome::AlreadyPresent);
        }

        let mut temporary = NamedTempFile::new_in(self.temporary_dir())
            .map_err(|error| io_error(error, "failed to create temporary loose object"))?;
        copy_and_hash_exact(
            source,
            temporary.as_file_mut(),
            expected,
            ErrorCode::InvalidArgument,
        )?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| io_error(error, "failed to synchronize temporary loose object"))?;

        match temporary.persist_noclobber(self.object_path(&expected.id)) {
            Ok(_) => Ok(ObjectPutOutcome::Created),
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                self.verify(expected)?;
                Ok(ObjectPutOutcome::AlreadyPresent)
            }
            Err(error) => Err(io_error(error.error, "failed to publish loose object")),
        }
    }

    fn put_content_addressed(&self, source: &mut dyn Read) -> EngineResult<ContentAddressedObject> {
        self.initialize()?;
        let mut temporary = NamedTempFile::new_in(self.temporary_dir())
            .map_err(|error| io_error(error, "failed to create temporary loose object"))?;
        let mut hasher = blake3::Hasher::new();
        let mut size = 0_u64;
        let mut buffer = [0_u8; 128 * 1024];
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|error| io_error(error, "failed to read content-addressed object"))?;
            if read == 0 {
                break;
            }
            temporary
                .as_file_mut()
                .write_all(&buffer[..read])
                .map_err(|error| io_error(error, "failed to write temporary loose object"))?;
            hasher.update(&buffer[..read]);
            size = size
                .checked_add(u64::try_from(read).map_err(|_| {
                    EngineError::new(ErrorCode::Internal, "object read size exceeds u64")
                })?)
                .ok_or_else(|| EngineError::new(ErrorCode::Internal, "object size exceeds u64"))?;
        }
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| io_error(error, "failed to synchronize temporary loose object"))?;
        let spec = ObjectSpec::new(ObjectId::from_bytes(*hasher.finalize().as_bytes()), size);
        let outcome = match temporary.persist_noclobber(self.object_path(&spec.id)) {
            Ok(_) => ObjectPutOutcome::Created,
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                self.verify(&spec)?;
                ObjectPutOutcome::AlreadyPresent
            }
            Err(error) => return Err(io_error(error.error, "failed to publish loose object")),
        };
        Ok(ContentAddressedObject { spec, outcome })
    }

    fn copy_to(&self, expected: &ObjectSpec, target: &mut dyn Write) -> EngineResult<()> {
        self.validate_layout()?;
        let path = self.object_path(&expected.id);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                EngineError::new(ErrorCode::ObjectMissing, "loose object is missing")
            } else {
                io_error(error, "failed to inspect loose object")
            }
        })?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != expected.size
        {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "loose object metadata does not match its specification",
            ));
        }
        let mut source =
            File::open(path).map_err(|error| io_error(error, "failed to open loose object"))?;
        copy_and_hash_exact(&mut source, target, expected, ErrorCode::ObjectCorrupt)
    }

    fn stat(&self, id: &ObjectId) -> EngineResult<Option<ObjectMetadata>> {
        self.stat_object(id)
    }

    fn list_page(&self, request: &PageRequest) -> EngineResult<ObjectPage> {
        request.validate()?;
        self.validate_layout()?;
        let mut objects = Vec::new();
        for entry in fs::read_dir(self.root.as_path())
            .map_err(|error| io_error(error, "failed to enumerate loose objects"))?
        {
            let entry =
                entry.map_err(|error| io_error(error, "failed to read loose object entry"))?;
            let name = entry.file_name();
            if name == TEMPORARY_DIRECTORY {
                continue;
            }
            let name = name.to_str().ok_or_else(|| {
                EngineError::new(ErrorCode::ObjectCorrupt, "loose object name is not UTF-8")
            })?;
            let id = name.parse::<ObjectId>().map_err(|error| {
                EngineError::new(ErrorCode::ObjectCorrupt, error.to_string()).with_source(error)
            })?;
            let metadata = self.stat_object(&id)?.ok_or_else(|| {
                EngineError::new(
                    ErrorCode::ObjectMissing,
                    "loose object vanished during listing",
                )
            })?;
            objects.push(metadata);
        }
        objects.sort_unstable_by_key(|object| object.id);

        let start = request.after.as_ref().map_or(Ok(0), |cursor| {
            let after = cursor.as_str().parse::<ObjectId>().map_err(|error| {
                EngineError::new(ErrorCode::InvalidArgument, error.to_string()).with_source(error)
            })?;
            Ok::<_, EngineError>(objects.partition_point(|object| object.id <= after))
        })?;
        let limit = usize::try_from(request.limit).map_err(|_| {
            EngineError::new(ErrorCode::InvalidArgument, "page limit cannot fit usize")
        })?;
        let end = start.saturating_add(limit).min(objects.len());
        let page = objects[start..end].to_vec();
        let next = if end < objects.len() {
            page.last()
                .map(|object| PageCursor::new(object.id.to_hex()))
                .transpose()?
        } else {
            None
        };
        Ok(ObjectPage {
            objects: page,
            next,
        })
    }

    fn remove(&self, id: &ObjectId) -> EngineResult<bool> {
        let Some(_) = self.stat_object(id)? else {
            return Ok(false);
        };
        fs::remove_file(self.object_path(id))
            .map_err(|error| io_error(error, "failed to remove loose object"))?;
        sync_directory(self.root.as_path())?;
        Ok(true)
    }

    fn durability_barrier(&self) -> EngineResult<()> {
        self.validate_layout()?;
        sync_directory(self.root.as_path())
    }
}

pub(crate) fn copy_and_hash_exact(
    source: &mut dyn Read,
    target: &mut dyn Write,
    expected: &ObjectSpec,
    mismatch_code: ErrorCode,
) -> EngineResult<()> {
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|error| io_error(error, "failed to read object stream"))?;
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).map_err(|_| {
            EngineError::new(ErrorCode::Internal, "object read length cannot fit u64")
        })?;
        total = total.checked_add(read_u64).ok_or_else(|| {
            EngineError::new(ErrorCode::InvalidArgument, "object stream size overflow")
        })?;
        if total > expected.size {
            return Err(EngineError::new(
                mismatch_code,
                "object stream exceeds its declared size",
            ));
        }
        hasher.update(&buffer[..read]);
        target
            .write_all(&buffer[..read])
            .map_err(|error| io_error(error, "failed to write object stream"))?;
    }
    let actual = ObjectId::from_bytes(*hasher.finalize().as_bytes());
    if total != expected.size || actual != expected.id {
        return Err(EngineError::new(
            mismatch_code,
            "object stream size or digest does not match its specification",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn publishes_idempotently_and_detects_corruption() {
        let temporary = tempfile::tempdir().unwrap();
        let store = LooseObjectStore::open_or_create(temporary.path()).unwrap();
        let bytes = b"managed object";
        let spec = ObjectSpec::new(ObjectId::for_bytes(bytes), bytes.len() as u64);
        assert_eq!(
            store.put_from(&spec, &mut Cursor::new(bytes)).unwrap(),
            ObjectPutOutcome::Created
        );
        assert_eq!(
            store.put_from(&spec, &mut Cursor::new(bytes)).unwrap(),
            ObjectPutOutcome::AlreadyPresent
        );
        let mut output = Vec::new();
        store.copy_to(&spec, &mut output).unwrap();
        assert_eq!(output, bytes);

        fs::write(store.object_path(&spec.id), b"wrong").unwrap();
        assert_eq!(
            store.verify(&spec).unwrap_err().code(),
            ErrorCode::ObjectCorrupt
        );
    }

    #[test]
    fn publishes_content_addressed_stream_without_a_known_object_id() {
        let temporary = tempfile::tempdir().unwrap();
        let store = LooseObjectStore::open_or_create(temporary.path()).unwrap();
        let bytes = b"whole file streamed directly into CAS";

        let first = store
            .put_content_addressed(&mut Cursor::new(bytes))
            .unwrap();
        assert_eq!(first.spec.id, ObjectId::for_bytes(bytes));
        assert_eq!(first.spec.size, bytes.len() as u64);
        assert_eq!(first.outcome, ObjectPutOutcome::Created);

        let replay = store
            .put_content_addressed(&mut Cursor::new(bytes))
            .unwrap();
        assert_eq!(replay.spec, first.spec);
        assert_eq!(replay.outcome, ObjectPutOutcome::AlreadyPresent);
        let mut output = Vec::new();
        store.copy_to(&first.spec, &mut output).unwrap();
        assert_eq!(output, bytes);
    }

    #[test]
    fn lists_with_typed_keyset_cursor() {
        let temporary = tempfile::tempdir().unwrap();
        let store = LooseObjectStore::open_or_create(temporary.path()).unwrap();
        for bytes in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            let spec = ObjectSpec::new(ObjectId::for_bytes(bytes), bytes.len() as u64);
            store.put_from(&spec, &mut Cursor::new(bytes)).unwrap();
        }
        let first = store.list_page(&PageRequest::first(2).unwrap()).unwrap();
        assert_eq!(first.objects.len(), 2);
        assert!(first.next.is_some());
        let second = store
            .list_page(&PageRequest::new(first.next, 2).unwrap())
            .unwrap();
        assert_eq!(second.objects.len(), 1);
        assert!(second.next.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn hardlink_proof_verifies_digest_and_inode() {
        use std::os::unix::fs::MetadataExt;

        let temporary = tempfile::tempdir().unwrap();
        let store = LooseObjectStore::open_or_create(temporary.path().join("objects")).unwrap();
        let destination_root = tempfile::tempdir().unwrap();
        let bytes = b"immutable WholeFile payload";
        let spec = ObjectSpec::new(ObjectId::for_bytes(bytes), bytes.len() as u64);
        store.put_from(&spec, &mut Cursor::new(bytes)).unwrap();
        store.seal_read_only(&spec).unwrap();
        let destination = destination_root.path().join("view");
        store.hard_link_to(&spec, &destination).unwrap();
        let source_metadata = fs::symlink_metadata(store.object_path(&spec.id)).unwrap();
        let destination_metadata = fs::symlink_metadata(&destination).unwrap();
        assert_eq!(source_metadata.dev(), destination_metadata.dev());
        assert_eq!(source_metadata.ino(), destination_metadata.ino());

        fs::remove_file(store.object_path(&spec.id)).unwrap();
        fs::write(store.object_path(&spec.id), b"corrupt").unwrap();
        let second_destination = destination_root.path().join("corrupt-view");
        let error = store.hard_link_to(&spec, &second_destination).unwrap_err();
        assert_eq!(error.code(), ErrorCode::ObjectCorrupt);
    }

    #[cfg(unix)]
    #[test]
    fn hardlink_rejects_writable_cas_and_trusted_mode_seals_it() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().unwrap();
        let store = LooseObjectStore::open_or_create(temporary.path().join("objects")).unwrap();
        let destination_root = tempfile::tempdir().unwrap();
        let bytes = b"must be sealed before linking";
        let spec = ObjectSpec::new(ObjectId::for_bytes(bytes), bytes.len() as u64);
        store.put_from(&spec, &mut Cursor::new(bytes)).unwrap();
        let destination = destination_root.path().join("view");
        let error = store.hard_link_to(&spec, &destination).unwrap_err();
        assert!(error.message().contains("HARDLINK_OBJECT_NOT_SEALED"));

        store.hard_link_to_trusted(&spec, &destination).unwrap();
        let mode = fs::symlink_metadata(&destination)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o222, 0);
        store.verify_hard_link_to(&spec, &destination).unwrap();
    }
}
