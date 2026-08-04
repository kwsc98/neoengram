use std::{
    collections::BTreeMap,
    io::Cursor,
    path::Path,
    sync::{Arc, RwLock},
};

use neoengram_core::{LogicalPath, ObjectId, ObjectSpec};
use neoengram_engine::{ObjectPutOutcome, ObjectStore};
use neoengram_fs::{LooseObjectStore, VerifiedRoot};
use neoengram_protocol::{ArtifactId, TenantId, WireObjectSpec};

/// FastCDC's maximum chunk size is the development object-upload ceiling.
pub const MAX_FILESYSTEM_OBJECT_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub object: WireObjectSpec,
    pub storage_version: String,
    pub replayed: bool,
}

/// Central object durability boundary used by the Agent action transport.
pub trait CentralObjectStoreBackend: Send + Sync {
    fn missing(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        objects: &[WireObjectSpec],
    ) -> Result<(Vec<WireObjectSpec>, Vec<WireObjectSpec>), FilesystemObjectStoreError>;

    fn put(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object: &WireObjectSpec,
        payload: &[u8],
    ) -> Result<StoredObject, FilesystemObjectStoreError>;

    fn durable_version(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object: &WireObjectSpec,
    ) -> Result<Option<String>, FilesystemObjectStoreError>;
}

#[derive(Debug, thiserror::Error)]
pub enum FilesystemObjectStoreError {
    #[error("invalid filesystem object-store scope: {0}")]
    InvalidScope(String),
    #[error("object exceeds the {MAX_FILESYSTEM_OBJECT_BYTES}-byte filesystem backend limit")]
    ObjectTooLarge,
    #[error("object payload length does not match its declaration")]
    LengthMismatch,
    #[error("object payload hash does not match its ObjectId")]
    HashMismatch,
    #[error("filesystem object-store operation failed: {0}")]
    Storage(String),
    #[error("filesystem object-store lock is poisoned")]
    LockPoisoned,
}

/// Development central object backend. Each Tenant/Artifact scope owns an independent verified
/// loose-object root, so a valid object ID in one scope never grants access to another scope.
#[derive(Debug)]
pub struct FilesystemObjectStore {
    root: VerifiedRoot,
    stores: RwLock<BTreeMap<(TenantId, ArtifactId), Arc<LooseObjectStore>>>,
}

impl FilesystemObjectStore {
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self, FilesystemObjectStoreError> {
        let root = VerifiedRoot::create(path).map_err(storage_error)?;
        Ok(Self {
            root,
            stores: RwLock::new(BTreeMap::new()),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    pub fn missing(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        objects: &[WireObjectSpec],
    ) -> Result<(Vec<WireObjectSpec>, Vec<WireObjectSpec>), FilesystemObjectStoreError> {
        let store = self.store(tenant_id, artifact_id)?;
        let mut missing = Vec::new();
        let mut durable = Vec::new();
        for object in objects {
            validate_object(object)?;
            let expected = object_spec(object);
            match store.stat(&expected.id).map_err(storage_error)? {
                Some(metadata) if metadata.size == expected.size => {
                    store.verify(&expected).map_err(storage_error)?;
                    durable.push(object.clone());
                }
                Some(_) => {
                    return Err(FilesystemObjectStoreError::Storage(format!(
                        "stored object {} has another length",
                        expected.id
                    )));
                }
                None => missing.push(object.clone()),
            }
        }
        Ok((missing, durable))
    }

    pub fn put(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object: &WireObjectSpec,
        payload: &[u8],
    ) -> Result<StoredObject, FilesystemObjectStoreError> {
        validate_object(object)?;
        if payload.len() as u64 != object.size.get() {
            return Err(FilesystemObjectStoreError::LengthMismatch);
        }
        if ObjectId::for_bytes(payload) != object.object_id {
            return Err(FilesystemObjectStoreError::HashMismatch);
        }
        let store = self.store(tenant_id, artifact_id)?;
        let expected = object_spec(object);
        let outcome = store
            .put_from(&expected, &mut Cursor::new(payload))
            .map_err(storage_error)?;
        store.durability_barrier().map_err(storage_error)?;
        store.verify(&expected).map_err(storage_error)?;
        Ok(StoredObject {
            object: object.clone(),
            storage_version: expected.id.to_hex(),
            replayed: outcome == ObjectPutOutcome::AlreadyPresent,
        })
    }

    fn store(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
    ) -> Result<Arc<LooseObjectStore>, FilesystemObjectStoreError> {
        let key = (tenant_id.clone(), artifact_id.clone());
        if let Some(store) = self
            .stores
            .read()
            .map_err(|_| FilesystemObjectStoreError::LockPoisoned)?
            .get(&key)
            .cloned()
        {
            return Ok(store);
        }

        let logical = scope_path(tenant_id, artifact_id)?;
        let path = self.root.create_dir_all(&logical).map_err(storage_error)?;
        let candidate = Arc::new(LooseObjectStore::open_or_create(path).map_err(storage_error)?);
        candidate.initialize().map_err(storage_error)?;
        let mut stores = self
            .stores
            .write()
            .map_err(|_| FilesystemObjectStoreError::LockPoisoned)?;
        Ok(stores.entry(key).or_insert(candidate).clone())
    }
}

impl CentralObjectStoreBackend for FilesystemObjectStore {
    fn missing(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        objects: &[WireObjectSpec],
    ) -> Result<(Vec<WireObjectSpec>, Vec<WireObjectSpec>), FilesystemObjectStoreError> {
        Self::missing(self, tenant_id, artifact_id, objects)
    }

    fn put(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object: &WireObjectSpec,
        payload: &[u8],
    ) -> Result<StoredObject, FilesystemObjectStoreError> {
        Self::put(self, tenant_id, artifact_id, object, payload)
    }

    fn durable_version(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object: &WireObjectSpec,
    ) -> Result<Option<String>, FilesystemObjectStoreError> {
        let (_, durable) =
            Self::missing(self, tenant_id, artifact_id, std::slice::from_ref(object))?;
        Ok((!durable.is_empty()).then(|| object.object_id.to_hex()))
    }
}

fn scope_path(
    tenant_id: &TenantId,
    artifact_id: &ArtifactId,
) -> Result<LogicalPath, FilesystemObjectStoreError> {
    LogicalPath::parse(format!(
        "tenants/{tenant_id}/artifacts/{artifact_id}/objects"
    ))
    .map_err(|error| FilesystemObjectStoreError::InvalidScope(error.to_string()))
}

fn validate_object(object: &WireObjectSpec) -> Result<(), FilesystemObjectStoreError> {
    if object.size.get() > MAX_FILESYSTEM_OBJECT_BYTES {
        Err(FilesystemObjectStoreError::ObjectTooLarge)
    } else {
        Ok(())
    }
}

fn object_spec(object: &WireObjectSpec) -> ObjectSpec {
    ObjectSpec::new(object.object_id, object.size.get())
}

fn storage_error(error: impl std::fmt::Display) -> FilesystemObjectStoreError {
    FilesystemObjectStoreError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use neoengram_protocol::{DecimalU64, Extensions};

    use super::*;

    fn object(payload: &[u8]) -> WireObjectSpec {
        WireObjectSpec {
            object_id: neoengram_core::ObjectId::for_bytes(payload),
            size: DecimalU64::new(payload.len() as u64),
            extensions: Extensions::new(),
        }
    }

    #[test]
    fn stores_verifies_and_partitions_objects_by_scope() {
        let temporary = tempfile::tempdir().unwrap();
        let backend = FilesystemObjectStore::open_or_create(temporary.path()).unwrap();
        let tenant_a = TenantId::new("tenant-a").unwrap();
        let tenant_b = TenantId::new("tenant-b").unwrap();
        let artifact = ArtifactId::new("artifact-a").unwrap();
        let payload = b"central object payload";
        let object = object(payload);

        let (missing, durable) = backend
            .missing(&tenant_a, &artifact, std::slice::from_ref(&object))
            .unwrap();
        assert_eq!(missing.as_slice(), std::slice::from_ref(&object));
        assert!(durable.is_empty());

        let first = backend.put(&tenant_a, &artifact, &object, payload).unwrap();
        assert!(!first.replayed);
        assert!(
            backend
                .put(&tenant_a, &artifact, &object, payload)
                .unwrap()
                .replayed
        );

        let (missing, durable) = backend
            .missing(&tenant_a, &artifact, std::slice::from_ref(&object))
            .unwrap();
        assert!(missing.is_empty());
        assert_eq!(durable.as_slice(), std::slice::from_ref(&object));
        assert_eq!(
            backend
                .missing(&tenant_b, &artifact, std::slice::from_ref(&object))
                .unwrap()
                .0,
            [object]
        );
    }

    #[test]
    fn rejects_wrong_length_and_oversized_objects() {
        let temporary = tempfile::tempdir().unwrap();
        let backend = FilesystemObjectStore::open_or_create(temporary.path()).unwrap();
        let tenant = TenantId::new("tenant-a").unwrap();
        let artifact = ArtifactId::new("artifact-a").unwrap();
        let declared = object(b"expected");
        assert!(matches!(
            backend.put(&tenant, &artifact, &declared, b"wrong"),
            Err(FilesystemObjectStoreError::LengthMismatch)
        ));
        assert!(matches!(
            backend.put(&tenant, &artifact, &declared, b"tampered"),
            Err(FilesystemObjectStoreError::HashMismatch)
        ));
        assert_eq!(
            backend
                .missing(&tenant, &artifact, std::slice::from_ref(&declared))
                .unwrap()
                .0,
            [declared]
        );

        let oversized = WireObjectSpec {
            object_id: neoengram_core::ObjectId::for_bytes(b"oversized"),
            size: DecimalU64::new(MAX_FILESYSTEM_OBJECT_BYTES + 1),
            extensions: Extensions::new(),
        };
        assert!(matches!(
            backend.missing(&tenant, &artifact, &[oversized]),
            Err(FilesystemObjectStoreError::ObjectTooLarge)
        ));
    }
}
