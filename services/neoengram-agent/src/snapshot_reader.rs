//! Immutable Snapshot/CAS reader used by the S3 data plane.
//!
//! The reader is deliberately independent from any local Delivery projection.  It resolves the
//! frozen Index/Manifest snapshot through the session bridge and reads sealed objects from the
//! Volume-local CAS.  A Delivery may therefore be removed without changing S3 semantics.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use crate::{AgentError, AgentErrorCode, AgentResult};
use neoengram_domain::core::{LogicalPath, ObjectId};
use neoengram_domain::protocol::{
    AgentId, MountGeneration, OwnerGeneration, S3ReadTicket, SessionGeneration, StorageVolumeId,
    TenantId,
};
use neoengram_runtime::engine::{ObjectSpec, ObjectStore};
use neoengram_runtime::fs::LooseObjectStore;

use crate::{execution::artifact_object_store, ExecutionBridge, WorkspaceMaterializationSnapshot};

/// Metadata returned by the immutable Snapshot/CAS reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmutableObjectHead {
    pub size_bytes: u64,
}

/// A half-open byte range. One read is capped at the S3 binary frame size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmutableByteRange {
    pub start: u64,
    pub end_exclusive: u64,
}

impl ImmutableByteRange {
    pub fn new(start: u64, end_exclusive: u64) -> AgentResult<Self> {
        if end_exclusive < start
            || end_exclusive.saturating_sub(start)
                > neoengram_domain::protocol::S3_READ_FRAME_MAX_BYTES as u64
        {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "Snapshot byte range is invalid or exceeds the S3 frame limit",
            ));
        }
        Ok(Self {
            start,
            end_exclusive,
        })
    }

    #[must_use]
    pub const fn len(self) -> u64 {
        self.end_exclusive.saturating_sub(self.start)
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.start == self.end_exclusive
    }
}

/// Read-only boundary over the exact frozen Index and Manifest set named by an S3 ticket.
pub trait ImmutableSnapshotReader: Send + Sync {
    fn head(&self, path: &LogicalPath) -> AgentResult<ImmutableObjectHead>;
    fn read_range(&self, path: &LogicalPath, range: ImmutableByteRange) -> AgentResult<Vec<u8>>;
}

/// Source boundary used by S3.  Implementations must resolve only the exact signed Snapshot
/// scope from the ticket and must not accept a caller-provided filesystem path.
pub trait S3SnapshotSource: Send + Sync {
    fn immutable_reader_for_ticket(
        &self,
        ticket: &S3ReadTicket,
    ) -> AgentResult<Arc<dyn ImmutableSnapshotReader>>;
}

/// Direct immutable reader over one frozen Snapshot and the Volume-local sealed CAS.
#[derive(Debug, Clone)]
pub struct SnapshotCasReader {
    snapshot: Arc<WorkspaceMaterializationSnapshot>,
    objects: LooseObjectStore,
}

impl SnapshotCasReader {
    pub fn new(
        snapshot: WorkspaceMaterializationSnapshot,
        objects: LooseObjectStore,
    ) -> AgentResult<Self> {
        validate_snapshot_files(snapshot.files(), &objects)?;
        Ok(Self {
            snapshot: Arc::new(snapshot),
            objects,
        })
    }

    fn file(&self, path: &LogicalPath) -> AgentResult<&crate::WorkspaceMaterializationFile> {
        self.snapshot
            .files()
            .iter()
            .find(|file| file.record.path == *path)
            .ok_or_else(|| {
                AgentError::new(
                    AgentErrorCode::AssignmentNotFound,
                    "Snapshot object was not found",
                )
            })
    }
}

fn validate_snapshot_files(
    files: &[crate::WorkspaceMaterializationFile],
    objects: &LooseObjectStore,
) -> AgentResult<()> {
    let mut specs = BTreeMap::<ObjectId, u64>::new();
    for file in files {
        file.manifest.validate().map_err(|error| {
            AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                format!(
                    "Snapshot Manifest for {} is invalid: {error}",
                    file.record.path
                ),
            )
        })?;
        let manifest_id = file.manifest.canonical_id().map_err(|error| {
            AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                format!(
                    "Snapshot Manifest identity for {} is invalid: {error}",
                    file.record.path
                ),
            )
        })?;
        let chunk_count = file.manifest.chunk_count().map_err(|error| {
            AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                format!(
                    "Snapshot Manifest chunk count for {} is invalid: {error}",
                    file.record.path
                ),
            )
        })?;
        if manifest_id != file.record.manifest_id
            || file.manifest.total_size != file.record.total_size
            || chunk_count != file.record.chunk_count
        {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                format!(
                    "Snapshot Manifest metadata differs from Index record {}",
                    file.record.path
                ),
            ));
        }
        for chunk in &file.manifest.chunks {
            if let Some(previous) = specs.insert(chunk.object_id, chunk.size) {
                if previous != chunk.size {
                    return Err(AgentError::new(
                        AgentErrorCode::ProtocolInvalid,
                        "Snapshot Manifests disagree about an Object size",
                    ));
                }
            }
        }
    }
    for (id, size) in specs {
        objects
            .verify(&ObjectSpec::new(id, size))
            .map_err(|error| {
                AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    format!("Snapshot requires missing or corrupt Volume object {id}: {error}"),
                )
            })?;
    }
    Ok(())
}

impl ImmutableSnapshotReader for SnapshotCasReader {
    fn head(&self, path: &LogicalPath) -> AgentResult<ImmutableObjectHead> {
        Ok(ImmutableObjectHead {
            size_bytes: self.file(path)?.record.total_size,
        })
    }

    fn read_range(&self, path: &LogicalPath, range: ImmutableByteRange) -> AgentResult<Vec<u8>> {
        let file = self.file(path)?;
        if range.end_exclusive > file.record.total_size {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "Snapshot byte range exceeds object size",
            ));
        }
        if range.start == range.end_exclusive {
            return Ok(Vec::new());
        }
        let mut output = Vec::with_capacity(usize::try_from(range.len()).map_err(|_| {
            AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "Snapshot byte range is too large",
            )
        })?);
        for chunk in &file.manifest.chunks {
            let chunk_end = chunk.offset.saturating_add(chunk.size);
            if chunk_end <= range.start {
                continue;
            }
            if chunk.offset >= range.end_exclusive {
                break;
            }
            let mut bytes = Vec::with_capacity(usize::try_from(chunk.size).map_err(|_| {
                AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    "Snapshot object is too large",
                )
            })?);
            self.objects
                .copy_to(&ObjectSpec::new(chunk.object_id, chunk.size), &mut bytes)
                .map_err(|error| {
                    AgentError::new(AgentErrorCode::ObjectTransferFailed, error.to_string())
                })?;
            let start = range.start.max(chunk.offset) - chunk.offset;
            let end = range.end_exclusive.min(chunk_end) - chunk.offset;
            output.extend_from_slice(
                &bytes[usize::try_from(start).unwrap()..usize::try_from(end).unwrap()],
            );
        }
        if output.len() != usize::try_from(range.len()).unwrap_or(usize::MAX) {
            return Err(AgentError::new(
                AgentErrorCode::ObjectTransferFailed,
                "Snapshot Manifest did not provide complete byte coverage",
            ));
        }
        Ok(output)
    }
}

/// Ticket-bound factory for direct Snapshot/CAS reads.
#[derive(Debug, Clone)]
pub struct SnapshotCasReaderFactory {
    mount_root: PathBuf,
    agent_id: AgentId,
    tenant_id: TenantId,
    mount_generation: MountGeneration,
    owner_generation: OwnerGeneration,
    session_generation: SessionGeneration,
    bridge: Arc<dyn ExecutionBridge>,
}

impl SnapshotCasReaderFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn for_binding(
        mount_root: impl Into<PathBuf>,
        agent_id: AgentId,
        tenant_id: TenantId,
        _storage_volume_id: StorageVolumeId,
        mount_generation: MountGeneration,
        owner_generation: OwnerGeneration,
        session_generation: SessionGeneration,
        bridge: Arc<dyn ExecutionBridge>,
    ) -> Self {
        Self {
            mount_root: mount_root.into(),
            agent_id,
            tenant_id,
            mount_generation,
            owner_generation,
            session_generation,
            bridge,
        }
    }

    fn validate_ticket(&self, ticket: &S3ReadTicket) -> AgentResult<()> {
        if ticket.agent_id != self.agent_id
            || ticket.tenant_id != self.tenant_id.as_str()
            || ticket.owner_generation != self.owner_generation
            || ticket.mount_generation != self.mount_generation
            || ticket.session_generation != self.session_generation
        {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "S3 read ticket does not match the live Agent Snapshot binding",
            ));
        }
        Ok(())
    }
}

impl S3SnapshotSource for SnapshotCasReaderFactory {
    fn immutable_reader_for_ticket(
        &self,
        ticket: &S3ReadTicket,
    ) -> AgentResult<Arc<dyn ImmutableSnapshotReader>> {
        self.validate_ticket(ticket)?;
        let snapshot = self.bridge.snapshot_read_snapshot(ticket)?;
        let objects = artifact_object_store(
            &self.mount_root,
            &self.tenant_id,
            &neoengram_domain::protocol::ArtifactId::new(ticket.artifact_id.clone()).map_err(
                |_| {
                    AgentError::new(
                        AgentErrorCode::ProtocolInvalid,
                        "S3 ticket Artifact ID is invalid",
                    )
                },
            )?,
        )?;
        Ok(Arc::new(SnapshotCasReader::new(snapshot, objects)?))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor, path::Path};

    use neoengram_domain::core::{
        ChunkRef, ChunkingStrategy, FileRecord, IndexVersion, Manifest, ManifestId,
    };

    use super::*;
    use crate::WorkspaceMaterializationFile;

    fn materialization_file(
        objects: &LooseObjectStore,
        path: &str,
        chunking: ChunkingStrategy,
        payloads: &[&[u8]],
    ) -> WorkspaceMaterializationFile {
        let mut offset = 0_u64;
        let mut chunks = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let object_id = ObjectId::for_bytes(payload);
            let size = payload.len() as u64;
            objects
                .put_from(&ObjectSpec::new(object_id, size), &mut Cursor::new(payload))
                .unwrap();
            chunks.push(ChunkRef::new(object_id, offset, size).unwrap());
            offset += size;
        }
        let manifest = Manifest::new(offset, chunking, chunks).unwrap();
        let record =
            FileRecord::from_manifest(LogicalPath::parse(path).unwrap(), &manifest).unwrap();
        WorkspaceMaterializationFile { record, manifest }
    }

    fn snapshot(files: Vec<WorkspaceMaterializationFile>) -> WorkspaceMaterializationSnapshot {
        let records = files
            .iter()
            .map(|file| file.record.clone())
            .collect::<Vec<_>>();
        let version = IndexVersion::from_snapshot(1, &records).unwrap();
        WorkspaceMaterializationSnapshot::new(version, files).unwrap()
    }

    fn object_store(root: &Path) -> LooseObjectStore {
        LooseObjectStore::open_or_create(root).unwrap()
    }

    #[test]
    fn reads_a_range_across_fast_cdc_chunk_boundaries() {
        let temporary = tempfile::tempdir().unwrap();
        let objects = object_store(&temporary.path().join("objects"));
        let file = materialization_file(
            &objects,
            "nested/chunked.bin",
            ChunkingStrategy::FastCdc,
            &[b"abcde", b"FGHIJK"],
        );
        let reader = SnapshotCasReader::new(snapshot(vec![file]), objects).unwrap();
        let path = LogicalPath::parse("nested/chunked.bin").unwrap();

        assert_eq!(reader.head(&path).unwrap().size_bytes, 11);
        assert_eq!(
            reader
                .read_range(&path, ImmutableByteRange::new(3, 9).unwrap())
                .unwrap(),
            b"deFGHI"
        );
    }

    #[test]
    fn reads_a_range_from_a_whole_file_object() {
        let temporary = tempfile::tempdir().unwrap();
        let objects = object_store(&temporary.path().join("objects"));
        let file = materialization_file(
            &objects,
            "whole.bin",
            ChunkingStrategy::WholeFile,
            &[b"whole-file-payload"],
        );
        let reader = SnapshotCasReader::new(snapshot(vec![file]), objects).unwrap();
        let path = LogicalPath::parse("whole.bin").unwrap();

        assert_eq!(
            reader
                .read_range(&path, ImmutableByteRange::new(6, 10).unwrap())
                .unwrap(),
            b"file"
        );
    }

    #[test]
    fn missing_logical_path_is_not_found() {
        let temporary = tempfile::tempdir().unwrap();
        let objects = object_store(&temporary.path().join("objects"));
        let reader = SnapshotCasReader::new(snapshot(Vec::new()), objects).unwrap();
        let path = LogicalPath::parse("missing.bin").unwrap();

        let head_error = reader.head(&path).unwrap_err();
        assert_eq!(head_error.code(), AgentErrorCode::AssignmentNotFound);
        let read_error = reader
            .read_range(&path, ImmutableByteRange::new(0, 0).unwrap())
            .unwrap_err();
        assert_eq!(read_error.code(), AgentErrorCode::AssignmentNotFound);
    }

    #[test]
    fn constructor_validation_rejects_incomplete_manifest_coverage() {
        let temporary = tempfile::tempdir().unwrap();
        let objects = object_store(&temporary.path().join("objects"));
        let payload = b"abc";
        let object_id = ObjectId::for_bytes(payload);
        let file = WorkspaceMaterializationFile {
            record: FileRecord {
                path: LogicalPath::parse("incomplete.bin").unwrap(),
                manifest_id: ManifestId::from_bytes([0x41; 32]),
                total_size: 4,
                chunk_count: 1,
            },
            manifest: Manifest {
                total_size: 4,
                chunking: ChunkingStrategy::FastCdc,
                chunks: vec![ChunkRef::new(object_id, 0, payload.len() as u64).unwrap()],
            },
        };

        let error = validate_snapshot_files(&[file], &objects).unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ProtocolInvalid);
        assert!(error
            .message()
            .contains("do not cover the declared total size"));
    }

    #[test]
    fn constructor_rejects_a_corrupt_cas_object() {
        let temporary = tempfile::tempdir().unwrap();
        let objects_root = temporary.path().join("objects");
        let objects = object_store(&objects_root);
        let file = materialization_file(
            &objects,
            "corrupt.bin",
            ChunkingStrategy::WholeFile,
            &[b"sealed-object"],
        );
        let object_id = file.manifest.chunks[0].object_id;
        let frozen = snapshot(vec![file]);
        fs::write(objects_root.join(object_id.to_hex()), b"broken-object").unwrap();

        let error = SnapshotCasReader::new(frozen, objects).unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
        assert!(error.message().contains("missing or corrupt Volume object"));
    }
}
