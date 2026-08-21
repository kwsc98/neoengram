//! Placement-first object storage and transfer ports.
//!
//! A backend is a physical copy target. Logical Commit and Snapshot records never appear here:
//! callers select a backend through placement metadata, then address immutable objects by tenant
//! and digest. Partial transfer state is additionally scoped by transfer ID so concurrent copies
//! of the same object cannot corrupt each other's resume offsets.

use std::{
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use neoengram_domain::protocol::CommitObject;
use neoengram_domain::{ObjectId, TenantId, TransferId, TransferTicket};

use crate::{
    sync_directory, EngineError, EngineResult, ErrorCode, LooseObjectStore, ObjectMetadata,
    ObjectPutOutcome, ObjectSpec, ObjectStore, VerifiedRoot,
};

const OBJECTS_DIRECTORY: &str = "objects";
const STAGING_DIRECTORY: &str = "staging";
const STAGED_OBJECT_SUFFIX: &str = ".partial";

/// Maximum bytes accepted by one staged write or returned by one range read.
///
/// This matches the current binary `ObjectChunk` bound. Keeping the limit at the storage port
/// prevents an in-memory or test transport from bypassing the data-plane allocation fence.
pub const MAX_OBJECT_TRANSFER_CHUNK_BYTES: usize = 1024 * 1024;

/// A non-empty, bounded byte range within an immutable object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectRange {
    pub offset: u64,
    pub length: u64,
}

impl ObjectRange {
    pub fn new(offset: u64, length: u64) -> EngineResult<Self> {
        if length == 0 {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "object range length must be greater than zero",
            ));
        }
        let length_usize = usize::try_from(length).map_err(|_| {
            EngineError::new(
                ErrorCode::InvalidArgument,
                "object range length cannot fit in memory",
            )
        })?;
        if length_usize > MAX_OBJECT_TRANSFER_CHUNK_BYTES {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "object range exceeds the {} byte transfer chunk limit",
                    MAX_OBJECT_TRANSFER_CHUNK_BYTES
                ),
            ));
        }
        offset.checked_add(length).ok_or_else(|| {
            EngineError::new(ErrorCode::InvalidArgument, "object range end exceeds u64")
        })?;
        Ok(Self { offset, length })
    }

    #[must_use]
    pub const fn end(self) -> u64 {
        self.offset + self.length
    }

    fn validate_for(self, expected: &ObjectSpec) -> EngineResult<()> {
        Self::new(self.offset, self.length)?;
        if self.end() > expected.size {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "object range exceeds the expected object size",
            ));
        }
        Ok(())
    }
}

/// Result of durably appending one transfer chunk to store-owned staging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageWriteOutcome {
    pub accepted_bytes: u64,
    pub staged_size: u64,
    pub complete: bool,
}

/// Placement data-plane boundary for immutable objects.
///
/// Implementations must scope published objects by tenant, stage writes by transfer, reject
/// sparse/out-of-order writes, verify exact size and BLAKE3 identity before publication, and never
/// replace an existing object. A successful publication must be crash durable before it returns.
pub trait ObjectBackend: Debug + Send + Sync {
    fn initialize(&self) -> EngineResult<()>;
    fn validate_layout(&self) -> EngineResult<()>;

    fn inspect(
        &self,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<Option<ObjectMetadata>>;

    fn read_range(
        &self,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        range: ObjectRange,
        target: &mut dyn Write,
    ) -> EngineResult<u64>;

    /// Returns the durable resume offset, or `None` when this transfer has not staged the object.
    fn staged_size(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<Option<u64>>;

    fn stage_write(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        offset: u64,
        bytes: &[u8],
    ) -> EngineResult<StageWriteOutcome>;

    fn verify_and_publish(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
    ) -> EngineResult<ObjectPutOutcome>;

    fn discard_staged(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<bool>;

    /// Deletes one published copy only after the authority has proved it is eligible for GC.
    fn delete(&self, tenant_id: &TenantId, object_id: &ObjectId) -> EngineResult<bool>;
}

/// A selected, readable placement used by a replication executor.
pub trait TransferSource: Debug + Send + Sync {
    fn open_object(
        &self,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        range: ObjectRange,
        target: &mut dyn Write,
    ) -> EngineResult<u64>;
}

/// A selected target placement used by a replication executor.
pub trait TransferSink: Debug + Send + Sync {
    fn resume_offset(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<u64>;

    fn accept_object(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        offset: u64,
        bytes: &[u8],
    ) -> EngineResult<StageWriteOutcome>;

    fn commit_object(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
    ) -> EngineResult<ObjectPutOutcome>;
}

/// Transport session used by the Agent replication executor.
///
/// Implementations may use Quinn, an in-process relay, or a deterministic test transport. The
/// ticket is the complete authorization and fencing input; no implementation may discover a
/// different source or target while a session is open.
#[async_trait]
pub trait TransferTransport: Debug + Send + Sync {
    async fn open(&self, ticket: &TransferTicket) -> EngineResult<()>;

    async fn pull_object(&self, object_id: ObjectId, ranges: &[ObjectRange]) -> EngineResult<()>;

    async fn finish(&self) -> EngineResult<()>;
}

/// Result of copying one immutable object through a [`TransferSource`] and [`TransferSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectTransferOutcome {
    pub object_id: ObjectId,
    pub bytes_transferred: u64,
    pub resumed_from: u64,
    pub published: ObjectPutOutcome,
}

/// Synchronous object-level replication kernel shared by Agent workers and deterministic tests.
///
/// The transport is deliberately outside this type: a QUIC relay, an in-process same-Gateway
/// channel, and a future archive backend can all feed the same source/sink boundaries. The
/// executor never publishes a PlacementSet; callers publish that authority fence only after every
/// object in the immutable set has returned successfully.
#[derive(Debug, Clone, Copy)]
pub struct ObjectSetTransferExecutor {
    chunk_bytes: usize,
}

impl Default for ObjectSetTransferExecutor {
    fn default() -> Self {
        Self {
            chunk_bytes: MAX_OBJECT_TRANSFER_CHUNK_BYTES,
        }
    }
}

impl ObjectSetTransferExecutor {
    pub fn new(chunk_bytes: usize) -> EngineResult<Self> {
        if chunk_bytes == 0 || chunk_bytes > MAX_OBJECT_TRANSFER_CHUNK_BYTES {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                format!("replication chunk size must be in 1..={MAX_OBJECT_TRANSFER_CHUNK_BYTES}"),
            ));
        }
        Ok(Self { chunk_bytes })
    }

    /// Copies one object, resuming from the sink's durable staged offset.
    pub fn copy_object(
        &self,
        ticket: &TransferTicket,
        tenant_id: &TenantId,
        transfer_id: &TransferId,
        source: &dyn TransferSource,
        sink: &dyn TransferSink,
        object: &CommitObject,
    ) -> EngineResult<ObjectTransferOutcome> {
        validate_transfer_ticket(ticket)?;
        if ticket.tenant_id != *tenant_id
            || ticket.transfer_id != *transfer_id
            || !ticket.allows(object.object_id)
        {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "transfer ticket does not authorize this object",
            ));
        }
        let expected = ObjectSpec::new(object.object_id, object.size.get());
        let mut offset = sink.resume_offset(transfer_id, tenant_id, &object.object_id)?;
        if offset > expected.size {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "durable transfer offset exceeds the expected object size",
            ));
        }
        let resumed_from = offset;
        let mut bytes_transferred = 0_u64;
        while offset < expected.size {
            let remaining = expected.size - offset;
            let length = remaining.min(self.chunk_bytes as u64);
            let range = ObjectRange::new(offset, length)?;
            let mut bytes = Vec::with_capacity(length as usize);
            let copied = source.open_object(tenant_id, &expected, range, &mut bytes)?;
            if copied != length || bytes.len() != length as usize {
                return Err(EngineError::new(
                    ErrorCode::ObjectCorrupt,
                    "transfer source returned a range with the wrong length",
                ));
            }
            let staged = sink.accept_object(transfer_id, tenant_id, &expected, offset, &bytes)?;
            let expected_offset = offset.checked_add(length).ok_or_else(|| {
                EngineError::new(ErrorCode::ObjectCorrupt, "transfer offset exceeds u64")
            })?;
            if staged.accepted_bytes != length || staged.staged_size != expected_offset {
                return Err(EngineError::new(
                    ErrorCode::ObjectCorrupt,
                    "transfer sink acknowledged an unexpected staged offset",
                ));
            }
            offset = expected_offset;
            bytes_transferred = bytes_transferred.checked_add(length).ok_or_else(|| {
                EngineError::new(ErrorCode::ObjectCorrupt, "transfer byte count exceeds u64")
            })?;
        }
        let published = sink.commit_object(transfer_id, tenant_id, &expected)?;
        Ok(ObjectTransferOutcome {
            object_id: object.object_id,
            bytes_transferred,
            resumed_from,
            published,
        })
    }

    /// Copies every object in canonical ObjectSet order. A failure leaves staged objects intact
    /// so the caller can retry from the returned object's durable offset.
    pub fn copy_object_set(
        &self,
        ticket: &TransferTicket,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
        transfer_id: &TransferId,
        source: &dyn TransferSource,
        sink: &dyn TransferSink,
    ) -> EngineResult<Vec<ObjectTransferOutcome>> {
        let mut total = 0_u64;
        let max_bytes = ticket.max_bytes.get();
        let mut results = Vec::with_capacity(object_set.len());
        for object in object_set {
            let expected = ObjectSpec::new(object.object_id, object.size.get());
            let resumed = sink.resume_offset(transfer_id, tenant_id, &object.object_id)?;
            if resumed > expected.size {
                return Err(EngineError::new(
                    ErrorCode::ObjectCorrupt,
                    "durable transfer offset exceeds the expected object size",
                ));
            }
            let remaining = expected.size - resumed;
            let allowed = max_bytes.checked_sub(total).ok_or_else(|| {
                EngineError::new(
                    ErrorCode::InvalidArgument,
                    "transfer exceeded the ticket byte limit",
                )
            })?;
            if remaining > allowed {
                return Err(EngineError::new(
                    ErrorCode::InvalidArgument,
                    "transfer would exceed the ticket byte limit",
                ));
            }
            let result = self.copy_object(ticket, tenant_id, transfer_id, source, sink, object)?;
            total = total.checked_add(result.bytes_transferred).ok_or_else(|| {
                EngineError::new(
                    ErrorCode::InvalidArgument,
                    "transfer byte count exceeds u64",
                )
            })?;
            if total > max_bytes {
                return Err(EngineError::new(
                    ErrorCode::InvalidArgument,
                    "transfer exceeded the ticket byte limit",
                ));
            }
            results.push(result);
        }
        Ok(results)
    }
}

fn validate_transfer_ticket(ticket: &TransferTicket) -> EngineResult<()> {
    ticket
        .validate()
        .map_err(|error| EngineError::new(ErrorCode::InvalidArgument, error.to_string()))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            EngineError::new(ErrorCode::LeaseExpired, "system clock is before Unix epoch")
        })?
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    if ticket.deadline_unix_ms.get() <= now {
        return Err(EngineError::new(
            ErrorCode::LeaseExpired,
            "transfer ticket deadline has elapsed",
        ));
    }
    Ok(())
}

impl<T> TransferSource for T
where
    T: ObjectBackend + ?Sized,
{
    fn open_object(
        &self,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        range: ObjectRange,
        target: &mut dyn Write,
    ) -> EngineResult<u64> {
        self.read_range(tenant_id, expected, range, target)
    }
}

impl<T> TransferSink for T
where
    T: ObjectBackend + ?Sized,
{
    fn resume_offset(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<u64> {
        Ok(self
            .staged_size(transfer_id, tenant_id, object_id)?
            .unwrap_or(0))
    }

    fn accept_object(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        offset: u64,
        bytes: &[u8],
    ) -> EngineResult<StageWriteOutcome> {
        self.stage_write(transfer_id, tenant_id, expected, offset, bytes)
    }

    fn commit_object(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
    ) -> EngineResult<ObjectPutOutcome> {
        self.verify_and_publish(transfer_id, tenant_id, expected)
    }
}

/// Tenant-scoped loose CAS for a single mounted Volume.
///
/// Published paths are `objects/<tenant_id>/<object_id>`. Staged paths are
/// `staging/<tenant_id>/<transfer_id>/<object_id>.partial`. The final object operations delegate
/// to the runtime's existing [`LooseObjectStore`], preserving its verified no-replace publication
/// and durability contract.
#[derive(Debug, Clone)]
pub struct VolumeCasBackend {
    root: VerifiedRoot,
}

impl VolumeCasBackend {
    pub fn open_or_create(path: impl AsRef<Path>) -> EngineResult<Self> {
        let backend = Self {
            root: VerifiedRoot::create(path)?,
        };
        backend.initialize()?;
        Ok(backend)
    }

    pub fn open(path: impl AsRef<Path>) -> EngineResult<Self> {
        let backend = Self {
            root: VerifiedRoot::open(path)?,
        };
        backend.validate_layout()?;
        Ok(backend)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    fn objects_root(&self) -> PathBuf {
        self.root.as_path().join(OBJECTS_DIRECTORY)
    }

    fn staging_root(&self) -> PathBuf {
        self.root.as_path().join(STAGING_DIRECTORY)
    }

    fn tenant_objects_path(&self, tenant_id: &TenantId) -> PathBuf {
        self.objects_root().join(tenant_id.as_str())
    }

    fn object_path(&self, tenant_id: &TenantId, object_id: &ObjectId) -> PathBuf {
        self.tenant_objects_path(tenant_id).join(object_id.to_hex())
    }

    fn transfer_staging_path(&self, transfer_id: &TransferId, tenant_id: &TenantId) -> PathBuf {
        self.staging_root()
            .join(tenant_id.as_str())
            .join(transfer_id.as_str())
    }

    fn staged_object_path(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> PathBuf {
        self.transfer_staging_path(transfer_id, tenant_id)
            .join(format!("{}{STAGED_OBJECT_SUFFIX}", object_id.to_hex()))
    }

    fn ensure_object_store(&self, tenant_id: &TenantId) -> EngineResult<LooseObjectStore> {
        self.initialize()?;
        let path = self.tenant_objects_path(tenant_id);
        ensure_directory(&path)?;
        let store = LooseObjectStore::new(VerifiedRoot::open(path)?);
        store.initialize()?;
        Ok(store)
    }

    fn existing_object_store(
        &self,
        tenant_id: &TenantId,
    ) -> EngineResult<Option<LooseObjectStore>> {
        self.validate_layout()?;
        let path = self.tenant_objects_path(tenant_id);
        match ordinary_file_metadata(&path, "tenant object directory")? {
            None => Ok(None),
            Some(metadata) if metadata.is_dir() => {
                let store = LooseObjectStore::new(VerifiedRoot::open(path)?);
                store.validate_layout()?;
                Ok(Some(store))
            }
            Some(_) => Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "tenant object path is not an ordinary directory",
            )),
        }
    }

    fn ensure_transfer_staging(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
    ) -> EngineResult<PathBuf> {
        self.initialize()?;
        let tenant_staging = self.staging_root().join(tenant_id.as_str());
        ensure_directory(&tenant_staging)?;
        let transfer_staging = tenant_staging.join(transfer_id.as_str());
        ensure_directory(&transfer_staging)?;
        Ok(transfer_staging)
    }
}

impl ObjectBackend for VolumeCasBackend {
    fn initialize(&self) -> EngineResult<()> {
        self.root.verify_identity()?;
        let objects_created = ensure_directory(&self.objects_root())?;
        let staging_created = ensure_directory(&self.staging_root())?;
        if objects_created || staging_created {
            sync_directory(self.root.as_path())?;
        }
        Ok(())
    }

    fn validate_layout(&self) -> EngineResult<()> {
        self.root.verify_identity()?;
        ensure_existing_directory(&self.objects_root(), "object backend CAS")?;
        ensure_existing_directory(&self.staging_root(), "object backend staging")
    }

    fn inspect(
        &self,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<Option<ObjectMetadata>> {
        self.validate_layout()?;
        let path = self.object_path(tenant_id, object_id);
        match ordinary_file_metadata(&path, "published object")? {
            None => Ok(None),
            Some(metadata) if metadata.is_file() => Ok(Some(ObjectMetadata {
                id: *object_id,
                size: metadata.len(),
            })),
            Some(_) => Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "published object path is not an ordinary file",
            )),
        }
    }

    fn read_range(
        &self,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        range: ObjectRange,
        target: &mut dyn Write,
    ) -> EngineResult<u64> {
        range.validate_for(expected)?;
        let Some(metadata) = self.inspect(tenant_id, &expected.id)? else {
            return Err(EngineError::new(
                ErrorCode::ObjectMissing,
                "published object is missing",
            ));
        };
        if metadata.size != expected.size {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "published object size differs from its specification",
            ));
        }

        let path = self.object_path(tenant_id, &expected.id);
        let mut source = File::open(&path)
            .map_err(|error| io_error(error, "failed to open published object"))?;
        ensure_opened_ordinary_file(&source, &path, expected.size, "published object")?;
        source
            .seek(SeekFrom::Start(range.offset))
            .map_err(|error| io_error(error, "failed to seek published object"))?;
        let mut limited = source.take(range.length);
        let copied = std::io::copy(&mut limited, target)
            .map_err(|error| io_error(error, "failed to read published object range"))?;
        if copied != range.length {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "published object ended before the requested range",
            ));
        }
        Ok(copied)
    }

    fn staged_size(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<Option<u64>> {
        self.validate_layout()?;
        let path = self.staged_object_path(transfer_id, tenant_id, object_id);
        match ordinary_file_metadata(&path, "staged object")? {
            None => Ok(None),
            Some(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
            Some(_) => Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "staged object path is not an ordinary file",
            )),
        }
    }

    fn stage_write(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
        offset: u64,
        bytes: &[u8],
    ) -> EngineResult<StageWriteOutcome> {
        if bytes.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "staged object chunk cannot be empty",
            ));
        }
        if bytes.len() > MAX_OBJECT_TRANSFER_CHUNK_BYTES {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "staged object chunk exceeds the {} byte limit",
                    MAX_OBJECT_TRANSFER_CHUNK_BYTES
                ),
            ));
        }
        let accepted_bytes = u64::try_from(bytes.len()).map_err(|_| {
            EngineError::new(
                ErrorCode::InvalidArgument,
                "staged object chunk length cannot fit u64",
            )
        })?;
        let staged_size = offset.checked_add(accepted_bytes).ok_or_else(|| {
            EngineError::new(
                ErrorCode::InvalidArgument,
                "staged object offset exceeds u64",
            )
        })?;
        if staged_size > expected.size {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "staged object chunk exceeds the expected object size",
            ));
        }

        let staging_dir = self.ensure_transfer_staging(transfer_id, tenant_id)?;
        let path = self.staged_object_path(transfer_id, tenant_id, &expected.id);
        let current_size = ordinary_file_metadata(&path, "staged object")?
            .map(|metadata| {
                if !metadata.is_file() {
                    return Err(EngineError::new(
                        ErrorCode::ObjectCorrupt,
                        "staged object path is not an ordinary file",
                    ));
                }
                Ok(metadata.len())
            })
            .transpose()?
            .unwrap_or(0);
        if offset != current_size {
            if current_size == 0 && ordinary_file_metadata(&path, "staged object")?.is_none() {
                File::create(&path)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| io_error(error, "failed to initialize staged object"))?;
                sync_directory(&staging_dir)?;
            }
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                format!(
                    "staged object chunk offset {offset} does not match the durable resume offset {current_size}"
                ),
            ));
        }
        let existed = ordinary_file_metadata(&path, "staged object")?.is_some();
        let mut staged = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| io_error(error, "failed to open staged object"))?;
        ensure_opened_ordinary_file(&staged, &path, offset, "staged object")?;
        staged
            .seek(SeekFrom::Start(offset))
            .map_err(|error| io_error(error, "failed to seek staged object"))?;
        staged
            .write_all(bytes)
            .map_err(|error| io_error(error, "failed to write staged object"))?;
        staged
            .sync_data()
            .map_err(|error| io_error(error, "failed to synchronize staged object"))?;
        ensure_opened_ordinary_file(&staged, &path, staged_size, "staged object")?;
        if !existed {
            sync_directory(&staging_dir)?;
        }
        Ok(StageWriteOutcome {
            accepted_bytes,
            staged_size,
            complete: staged_size == expected.size,
        })
    }

    fn verify_and_publish(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        expected: &ObjectSpec,
    ) -> EngineResult<ObjectPutOutcome> {
        let store = self.ensure_object_store(tenant_id)?;
        if store.stat(&expected.id)?.is_some() {
            store.verify(expected)?;
            self.discard_staged(transfer_id, tenant_id, &expected.id)?;
            return Ok(ObjectPutOutcome::AlreadyPresent);
        }

        let staging_dir = self.ensure_transfer_staging(transfer_id, tenant_id)?;
        let path = self.staged_object_path(transfer_id, tenant_id, &expected.id);
        if expected.size == 0 && ordinary_file_metadata(&path, "staged object")?.is_none() {
            File::create(&path)
                .and_then(|file| file.sync_all())
                .map_err(|error| io_error(error, "failed to create empty staged object"))?;
            sync_directory(&staging_dir)?;
        }
        let mut staged = File::open(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                EngineError::new(ErrorCode::ObjectMissing, "staged object is missing")
            } else {
                io_error(error, "failed to open staged object for publication")
            }
        })?;
        ensure_opened_ordinary_file(&staged, &path, expected.size, "staged object")?;
        verify_reader(&mut staged, expected)?;
        staged
            .seek(SeekFrom::Start(0))
            .map_err(|error| io_error(error, "failed to rewind staged object"))?;
        let outcome = store.put_from(expected, &mut staged)?;
        store.durability_barrier()?;
        fs::remove_file(&path)
            .map_err(|error| io_error(error, "failed to remove published staging file"))?;
        sync_directory(&staging_dir)?;
        Ok(outcome)
    }

    fn discard_staged(
        &self,
        transfer_id: &TransferId,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> EngineResult<bool> {
        self.validate_layout()?;
        let path = self.staged_object_path(transfer_id, tenant_id, object_id);
        let Some(metadata) = ordinary_file_metadata(&path, "staged object")? else {
            return Ok(false);
        };
        if !metadata.is_file() {
            return Err(EngineError::new(
                ErrorCode::ObjectCorrupt,
                "staged object path is not an ordinary file",
            ));
        }
        fs::remove_file(&path)
            .map_err(|error| io_error(error, "failed to discard staged object"))?;
        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }
        Ok(true)
    }

    fn delete(&self, tenant_id: &TenantId, object_id: &ObjectId) -> EngineResult<bool> {
        let Some(store) = self.existing_object_store(tenant_id)? else {
            return Ok(false);
        };
        store.remove(object_id)
    }
}

fn ensure_directory(path: &Path) -> EngineResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(false),
        Ok(_) => Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            "object backend path is not an ordinary directory",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(io_error(error, "failed to create object backend directory"));
                }
            }
            ensure_existing_directory(path, "object backend")?;
            Ok(true)
        }
        Err(error) => Err(io_error(
            error,
            "failed to inspect object backend directory",
        )),
    }
}

fn ensure_existing_directory(path: &Path, description: &str) -> EngineResult<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error(error, format!("failed to inspect {description} directory")))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            format!("{description} path is not an ordinary directory"),
        ));
    }
    Ok(())
}

fn ordinary_file_metadata(path: &Path, description: &str) -> EngineResult<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            format!("{description} path is a symbolic link"),
        )),
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error, format!("failed to inspect {description}"))),
    }
}

fn ensure_opened_ordinary_file(
    file: &File,
    path: &Path,
    expected_size: u64,
    description: &str,
) -> EngineResult<()> {
    let opened = file
        .metadata()
        .map_err(|error| io_error(error, format!("failed to inspect opened {description}")))?;
    let path_metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error(error, format!("failed to recheck {description} path")))?;
    if !opened.is_file()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || opened.len() != expected_size
        || path_metadata.len() != expected_size
    {
        return Err(EngineError::new(
            ErrorCode::ObjectCorrupt,
            format!("{description} identity or size changed"),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.dev() != path_metadata.dev() || opened.ino() != path_metadata.ino() {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                format!("{description} path no longer identifies the opened file"),
            ));
        }
    }
    Ok(())
}

fn verify_reader(source: &mut dyn Read, expected: &ObjectSpec) -> EngineResult<()> {
    let mut hasher = blake3::Hasher::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|error| io_error(error, "failed to read staged object for verification"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total = total
            .checked_add(u64::try_from(read).map_err(|_| {
                EngineError::new(ErrorCode::Internal, "object read length cannot fit u64")
            })?)
            .ok_or_else(|| {
                EngineError::new(ErrorCode::ObjectCorrupt, "staged object size exceeds u64")
            })?;
    }
    let actual_id = ObjectId::from_bytes(*hasher.finalize().as_bytes());
    if total != expected.size || actual_id != expected.id {
        return Err(EngineError::new(
            ErrorCode::ObjectCorrupt,
            "staged object size or BLAKE3 identity does not match its specification",
        ));
    }
    Ok(())
}

fn io_error(error: std::io::Error, message: impl Into<String>) -> EngineError {
    EngineError::new(ErrorCode::Io, message).with_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(value: &str) -> TenantId {
        TenantId::new(value).unwrap()
    }

    fn transfer(value: &str) -> TransferId {
        TransferId::new(value).unwrap()
    }

    #[test]
    fn stages_resumes_publishes_and_reads_a_tenant_object() {
        let temporary = tempfile::tempdir().unwrap();
        let backend = VolumeCasBackend::open_or_create(temporary.path()).unwrap();
        let payload = b"placement-first-object";
        let expected = ObjectSpec::for_bytes(payload);
        let tenant_a = tenant("tenant-a");
        let transfer_a = transfer("transfer-a");

        let first = backend
            .stage_write(&transfer_a, &tenant_a, &expected, 0, &payload[..9])
            .unwrap();
        assert_eq!(first.staged_size, 9);
        assert!(!first.complete);
        assert_eq!(
            backend
                .staged_size(&transfer_a, &tenant_a, &expected.id)
                .unwrap(),
            Some(9)
        );
        let second = backend
            .stage_write(&transfer_a, &tenant_a, &expected, 9, &payload[9..])
            .unwrap();
        assert!(second.complete);
        assert_eq!(
            backend
                .verify_and_publish(&transfer_a, &tenant_a, &expected)
                .unwrap(),
            ObjectPutOutcome::Created
        );
        assert_eq!(
            backend.inspect(&tenant_a, &expected.id).unwrap(),
            Some(ObjectMetadata {
                id: expected.id,
                size: expected.size,
            })
        );
        assert_eq!(
            backend.inspect(&tenant("tenant-b"), &expected.id).unwrap(),
            None
        );

        let mut selected = Vec::new();
        backend
            .read_range(
                &tenant_a,
                &expected,
                ObjectRange::new(10, 5).unwrap(),
                &mut selected,
            )
            .unwrap();
        assert_eq!(selected, &payload[10..15]);
    }

    #[test]
    fn resume_offset_survives_backend_reopen() {
        let temporary = tempfile::tempdir().unwrap();
        let payload = b"resumable";
        let expected = ObjectSpec::for_bytes(payload);
        let tenant_id = tenant("tenant-a");
        let transfer_id = transfer("transfer-a");
        {
            let backend = VolumeCasBackend::open_or_create(temporary.path()).unwrap();
            backend
                .stage_write(&transfer_id, &tenant_id, &expected, 0, &payload[..4])
                .unwrap();
        }
        let backend = VolumeCasBackend::open(temporary.path()).unwrap();
        assert_eq!(
            backend
                .staged_size(&transfer_id, &tenant_id, &expected.id)
                .unwrap(),
            Some(4)
        );
        backend
            .stage_write(&transfer_id, &tenant_id, &expected, 4, &payload[4..])
            .unwrap();
        backend
            .verify_and_publish(&transfer_id, &tenant_id, &expected)
            .unwrap();
    }

    #[test]
    fn rejects_out_of_order_and_corrupt_staging_without_publication() {
        let temporary = tempfile::tempdir().unwrap();
        let backend = VolumeCasBackend::open_or_create(temporary.path()).unwrap();
        let expected = ObjectSpec::for_bytes(b"correct");
        let tenant_id = tenant("tenant-a");
        let transfer_id = transfer("transfer-a");

        let error = backend
            .stage_write(&transfer_id, &tenant_id, &expected, 1, b"wrong!")
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::ObjectCorrupt);
        assert_eq!(
            backend
                .staged_size(&transfer_id, &tenant_id, &expected.id)
                .unwrap(),
            Some(0)
        );
        backend
            .stage_write(&transfer_id, &tenant_id, &expected, 0, b"wrong!!")
            .unwrap();
        let error = backend
            .verify_and_publish(&transfer_id, &tenant_id, &expected)
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::ObjectCorrupt);
        assert_eq!(backend.inspect(&tenant_id, &expected.id).unwrap(), None);
    }

    #[test]
    fn publication_is_idempotent_across_transfers() {
        let temporary = tempfile::tempdir().unwrap();
        let backend = VolumeCasBackend::open_or_create(temporary.path()).unwrap();
        let payload = b"deduplicated";
        let expected = ObjectSpec::for_bytes(payload);
        let tenant_id = tenant("tenant-a");
        let first = transfer("transfer-a");
        let second = transfer("transfer-b");
        backend
            .stage_write(&first, &tenant_id, &expected, 0, payload)
            .unwrap();
        assert_eq!(
            backend
                .verify_and_publish(&first, &tenant_id, &expected)
                .unwrap(),
            ObjectPutOutcome::Created
        );
        backend
            .stage_write(&second, &tenant_id, &expected, 0, payload)
            .unwrap();
        assert_eq!(
            backend
                .verify_and_publish(&second, &tenant_id, &expected)
                .unwrap(),
            ObjectPutOutcome::AlreadyPresent
        );
        assert_eq!(
            backend
                .staged_size(&second, &tenant_id, &expected.id)
                .unwrap(),
            None
        );
    }
}
