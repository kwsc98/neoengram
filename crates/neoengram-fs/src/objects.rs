use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use neoengram_core::ObjectId;
use neoengram_engine::{
    EngineError, EngineResult, ErrorCode, ObjectMetadata, ObjectPage, ObjectPutOutcome, ObjectSpec,
    ObjectStore, PageCursor, PageRequest,
};
use tempfile::NamedTempFile;

use crate::{io_error, sync_directory, VerifiedRoot};

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
}
