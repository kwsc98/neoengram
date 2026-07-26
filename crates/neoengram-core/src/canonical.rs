//! Canonical BLAKE3 encoders shared by standalone, Agent, and central control-plane components.
//!
//! Manifest v4, Directory v1, and Commit v3 preserve the byte layout used by NeoEngram's existing
//! local repository implementation. Index snapshot and delta v1 establish the backend-independent
//! format introduced with repository format v8.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ChunkRef, ChunkingStrategy, Commit, CommitId, ContentDigest, Directory, DirectoryEntry,
    DirectoryEntryKind, DirectoryId, FileRecord, IndexDelta, IndexMutation, IndexVersion, Manifest,
    ManifestId, ObjectId, ObjectSpec, ValidationError, ValidationErrorKind, ValidationResult,
    INDEX_FORMAT_VERSION,
};

/// Domain separator for canonical Manifest v4 identities.
pub const MANIFEST_HASH_DOMAIN: &[u8] = b"neoengram-manifest-v4";
/// Domain separator for canonical Directory v1 identities.
pub const DIRECTORY_HASH_DOMAIN: &[u8] = b"neoengram-directory-v1";
/// Domain separator for canonical Commit v3 identities.
pub const COMMIT_HASH_DOMAIN: &[u8] = b"neoengram-commit-v3";
/// Domain separator for canonical Index record v1 digests.
pub const INDEX_RECORD_HASH_DOMAIN: &[u8] = b"neoengram-index-record-v1";
/// Domain separator for canonical Index snapshot v1 digests.
pub const INDEX_SNAPSHOT_HASH_DOMAIN: &[u8] = b"neoengram-index-snapshot-v1";
/// Domain separator for canonical Index delta v1 digests.
pub const INDEX_DELTA_HASH_DOMAIN: &[u8] = b"neoengram-index-delta-v1";
/// Domain separator for the logical content of one managed Add publication.
pub const ADD_PUBLICATION_HASH_DOMAIN: &[u8] = b"neoengram-add-publication-v1";
/// Version prefix included in every canonical encoding in this module.
pub const CANONICAL_ENCODING_VERSION: u32 = 1;

/// Incremental canonical Manifest v4 identity builder.
///
/// Callers must append chunks in file-offset order. This supports paged Manifest readers without
/// materializing every chunk reference in memory.
pub struct ManifestHasher {
    hasher: blake3::Hasher,
    total_size: u64,
    chunking: ChunkingStrategy,
    next_offset: u64,
    chunk_count: u64,
}

impl ManifestHasher {
    /// Starts a canonical Manifest identity with its declared strategy and logical size.
    pub fn new(total_size: u64, chunking: ChunkingStrategy) -> Self {
        let mut hasher = canonical_hasher(MANIFEST_HASH_DOMAIN);
        hasher.update(&[chunking_tag(chunking)]);
        hasher.update(&total_size.to_le_bytes());
        Self {
            hasher,
            total_size,
            chunking,
            next_offset: 0,
            chunk_count: 0,
        }
    }

    /// Appends the next contiguous chunk reference.
    pub fn push(&mut self, chunk: &ChunkRef) -> ValidationResult<()> {
        chunk.validate().map_err(|error| {
            ValidationError::new(
                ValidationErrorKind::InvalidManifest,
                format!("invalid Manifest chunk: {error}"),
            )
        })?;
        if chunk.offset != self.next_offset {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidManifest,
                format!(
                    "Manifest chunk offset is not contiguous at object {}",
                    chunk.object_id
                ),
            ));
        }
        self.hasher.update(chunk.object_id.as_bytes());
        self.hasher.update(&chunk.offset.to_le_bytes());
        self.hasher.update(&chunk.size.to_le_bytes());
        self.next_offset = self.next_offset.checked_add(chunk.size).ok_or_else(|| {
            ValidationError::new(
                ValidationErrorKind::Overflow,
                "Manifest total chunk size exceeds u64",
            )
        })?;
        self.chunk_count = self.chunk_count.checked_add(1).ok_or_else(|| {
            ValidationError::new(
                ValidationErrorKind::Overflow,
                "Manifest chunk count exceeds u64",
            )
        })?;
        Ok(())
    }

    /// Finishes validation and returns the canonical Manifest v4 identity.
    pub fn finish(mut self) -> ValidationResult<ManifestId> {
        if self.next_offset != self.total_size {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidManifest,
                "Manifest chunks do not cover the declared total size",
            ));
        }
        self.hasher.update(&self.chunk_count.to_le_bytes());
        if self.chunking == ChunkingStrategy::WholeFile
            && !((self.total_size == 0 && self.chunk_count == 0)
                || (self.total_size > 0 && self.chunk_count == 1))
        {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidManifest,
                "WholeFile Manifest must contain zero chunks for an empty file or one complete chunk",
            ));
        }
        Ok(ManifestId::from_bytes(*self.hasher.finalize().as_bytes()))
    }
}

/// Incremental canonical Directory v1 identity builder.
pub struct DirectoryHasher {
    hasher: blake3::Hasher,
    previous_name: Option<crate::PathComponent>,
    portable_names: BTreeSet<String>,
    entry_count: u64,
}

impl DirectoryHasher {
    /// Starts an empty canonical Directory identity.
    pub fn new() -> Self {
        Self {
            hasher: canonical_hasher(DIRECTORY_HASH_DOMAIN),
            previous_name: None,
            portable_names: BTreeSet::new(),
            entry_count: 0,
        }
    }

    /// Appends one direct child in canonical name order.
    pub fn push(&mut self, entry: &DirectoryEntry) -> ValidationResult<()> {
        entry.validate()?;
        if entry.ordinal != self.entry_count {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidDirectory,
                format!("Directory entry ordinal is not contiguous: {}", entry.name),
            ));
        }
        if self
            .previous_name
            .as_ref()
            .is_some_and(|previous| previous >= &entry.name)
        {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidDirectory,
                format!(
                    "Directory entries must be strictly ordered by name: {}",
                    entry.name
                ),
            ));
        }
        if !self.portable_names.insert(entry.name.portable_key()) {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidDirectory,
                format!(
                    "Directory entry names collide under portable case rules: {}",
                    entry.name
                ),
            ));
        }

        self.hasher.update(&[directory_kind_tag(entry.kind)]);
        update_bytes_with_len(&mut self.hasher, entry.name.as_str().as_bytes())?;
        self.hasher.update(entry.target_id.as_bytes());
        self.hasher.update(&entry.total_size.to_le_bytes());
        self.entry_count = self.entry_count.checked_add(1).ok_or_else(|| {
            ValidationError::new(
                ValidationErrorKind::Overflow,
                "Directory entry count exceeds u64",
            )
        })?;
        self.previous_name = Some(entry.name.clone());
        Ok(())
    }

    /// Finishes the Directory by encoding its entry count and returns its canonical identity.
    pub fn finish(mut self) -> DirectoryId {
        self.hasher.update(&self.entry_count.to_le_bytes());
        DirectoryId::from_bytes(*self.hasher.finalize().as_bytes())
    }

    /// Returns the number of entries appended so far.
    pub const fn entry_count(&self) -> u64 {
        self.entry_count
    }
}

impl Default for DirectoryHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Incremental backend-independent Index snapshot v1 digest builder.
pub struct IndexSnapshotHasher {
    hasher: blake3::Hasher,
    previous_path: Option<crate::LogicalPath>,
    portable_paths: BTreeSet<String>,
    record_count: u64,
}

impl IndexSnapshotHasher {
    /// Starts an empty format-v8 Index snapshot digest.
    pub fn new() -> Self {
        let mut hasher = canonical_hasher(INDEX_SNAPSHOT_HASH_DOMAIN);
        hasher.update(&INDEX_FORMAT_VERSION.to_le_bytes());
        Self {
            hasher,
            previous_path: None,
            portable_paths: BTreeSet::new(),
            record_count: 0,
        }
    }

    /// Appends one file record in strictly increasing logical-path order.
    pub fn push(&mut self, record: &FileRecord) -> ValidationResult<()> {
        record.validate()?;
        validate_next_index_path(
            self.previous_path.as_ref(),
            &mut self.portable_paths,
            &record.path,
        )?;
        update_file_record(&mut self.hasher, record)?;
        self.record_count = self.record_count.checked_add(1).ok_or_else(|| {
            ValidationError::new(
                ValidationErrorKind::Overflow,
                "Index record count exceeds u64",
            )
        })?;
        self.previous_path = Some(record.path.clone());
        Ok(())
    }

    /// Finishes the canonical snapshot digest without including a backend revision.
    pub fn finish(mut self) -> ContentDigest {
        self.hasher.update(&self.record_count.to_le_bytes());
        ContentDigest::from_bytes(*self.hasher.finalize().as_bytes())
    }

    /// Finishes the snapshot and associates it with a backend CAS revision.
    pub fn finish_version(self, revision: u64) -> IndexVersion {
        IndexVersion::new(revision, self.finish())
    }

    /// Returns the number of records appended so far.
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }
}

impl Default for IndexSnapshotHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Computes the canonical Manifest v4 identity after validating the complete model.
pub fn manifest_id(manifest: &Manifest) -> ValidationResult<ManifestId> {
    manifest.validate()?;
    let mut hasher = ManifestHasher::new(manifest.total_size, manifest.chunking);
    for chunk in &manifest.chunks {
        hasher.push(chunk)?;
    }
    hasher.finish()
}

/// Computes the canonical Directory v1 identity after validating direct entry order.
pub fn directory_id(directory: &Directory) -> ValidationResult<DirectoryId> {
    directory.validate()?;
    let mut hasher = DirectoryHasher::new();
    for entry in &directory.entries {
        hasher.push(entry)?;
    }
    Ok(hasher.finish())
}

/// Computes the canonical Commit v3 identity.
pub fn commit_id(commit: &Commit) -> ValidationResult<CommitId> {
    commit.validate()?;
    let mut hasher = canonical_hasher(COMMIT_HASH_DOMAIN);
    hasher.update(commit.root_directory_id.as_bytes());
    match commit.parent {
        Some(parent) => {
            hasher.update(&[1]);
            hasher.update(parent.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    update_bytes_with_len(&mut hasher, commit.message.as_bytes())?;
    hasher.update(&commit.created_at_unix_ms.to_le_bytes());
    Ok(CommitId::from_bytes(*hasher.finalize().as_bytes()))
}

/// Computes the canonical digest of one Index file record.
pub fn index_record_digest(record: &FileRecord) -> ValidationResult<ContentDigest> {
    record.validate()?;
    let mut hasher = canonical_hasher(INDEX_RECORD_HASH_DOMAIN);
    update_file_record(&mut hasher, record)?;
    Ok(ContentDigest::from_bytes(*hasher.finalize().as_bytes()))
}

/// Computes the canonical digest of a complete sorted Index snapshot.
pub fn index_snapshot_digest(records: &[FileRecord]) -> ValidationResult<ContentDigest> {
    let mut hasher = IndexSnapshotHasher::new();
    for record in records {
        hasher.push(record)?;
    }
    Ok(hasher.finish())
}

/// Computes the canonical digest of a logical Index delta, independent of page boundaries.
pub fn index_delta_digest(delta: &IndexDelta) -> ValidationResult<ContentDigest> {
    delta.validate()?;
    let mut hasher = canonical_hasher(INDEX_DELTA_HASH_DOMAIN);
    hasher.update(&INDEX_FORMAT_VERSION.to_le_bytes());
    hasher.update(&delta.base_version.revision.to_le_bytes());
    hasher.update(delta.base_version.digest.as_bytes());
    for mutation in delta.iter() {
        match mutation {
            IndexMutation::Upsert { record } => {
                hasher.update(&[1]);
                update_file_record(&mut hasher, record)?;
            }
            IndexMutation::Delete { path } => {
                hasher.update(&[2]);
                update_bytes_with_len(&mut hasher, path.as_str().as_bytes())?;
            }
        }
    }
    let mutation_count = u64::try_from(delta.mutation_count()).map_err(|_| {
        ValidationError::new(
            ValidationErrorKind::Overflow,
            "Index mutation count exceeds u64",
        )
    })?;
    hasher.update(&mutation_count.to_le_bytes());
    hasher.update(delta.result_digest.as_bytes());
    Ok(ContentDigest::from_bytes(*hasher.finalize().as_bytes()))
}

/// Computes the canonical digest of the exact logical material proposed by managed Add.
///
/// Transport page boundaries, upload receipts, statistics, timestamps, and storage placement are
/// deliberately excluded. The logical resource scope, CAS base, complete IndexDelta (including
/// its expected result digest), canonical Manifests, and exact ObjectSpecs are included. The
/// function validates that these collections form one closed publication graph before hashing.
#[allow(clippy::too_many_arguments)]
pub fn add_publication_digest(
    tenant_id: &str,
    project_id: &str,
    artifact_id: &str,
    playground_id: &str,
    job_id: &str,
    base_index_version: &IndexVersion,
    index_delta: &IndexDelta,
    manifests: &[Manifest],
    object_specs: &[ObjectSpec],
) -> ValidationResult<ContentDigest> {
    for (field, value) in [
        ("tenant_id", tenant_id),
        ("project_id", project_id),
        ("artifact_id", artifact_id),
        ("playground_id", playground_id),
        ("job_id", job_id),
    ] {
        if value.is_empty() {
            return Err(invalid_publication(format!(
                "publication {field} cannot be empty"
            )));
        }
    }
    if &index_delta.base_version != base_index_version {
        return Err(invalid_publication(
            "publication IndexDelta base differs from its declared base IndexVersion",
        ));
    }
    index_delta.validate()?;

    let mut canonical_manifests = BTreeMap::<ManifestId, &Manifest>::new();
    let mut referenced_objects = BTreeMap::<ObjectId, u64>::new();
    for manifest in manifests {
        manifest.validate()?;
        let manifest_id = manifest.canonical_id()?;
        if canonical_manifests.insert(manifest_id, manifest).is_some() {
            return Err(invalid_publication(format!(
                "publication repeats Manifest {manifest_id}"
            )));
        }
        for chunk in &manifest.chunks {
            if referenced_objects
                .insert(chunk.object_id, chunk.size)
                .is_some_and(|size| size != chunk.size)
            {
                return Err(invalid_publication(format!(
                    "publication Manifests disagree about object {} size",
                    chunk.object_id
                )));
            }
        }
    }

    let mut referenced_manifests = BTreeSet::new();
    for mutation in index_delta.iter() {
        if let IndexMutation::Upsert { record } = mutation {
            referenced_manifests.insert(record.manifest_id);
            let manifest = canonical_manifests
                .get(&record.manifest_id)
                .ok_or_else(|| {
                    invalid_publication(format!(
                        "Index upsert references absent Manifest {}",
                        record.manifest_id
                    ))
                })?;
            if manifest.total_size != record.total_size
                || manifest.chunk_count()? != record.chunk_count
            {
                return Err(invalid_publication(format!(
                    "Index upsert metadata differs from Manifest {}",
                    record.manifest_id
                )));
            }
        }
    }
    if canonical_manifests.keys().copied().collect::<BTreeSet<_>>() != referenced_manifests {
        return Err(invalid_publication(
            "publication Manifests are not the exact set referenced by Index upserts",
        ));
    }

    let mut canonical_objects = BTreeMap::new();
    for object in object_specs {
        if canonical_objects.insert(object.id, object.size).is_some() {
            return Err(invalid_publication(format!(
                "publication repeats ObjectSpec {}",
                object.id
            )));
        }
    }
    if canonical_objects != referenced_objects {
        return Err(invalid_publication(
            "publication ObjectSpecs are not the exact Manifest object set",
        ));
    }

    let mut hasher = canonical_hasher(ADD_PUBLICATION_HASH_DOMAIN);
    for value in [tenant_id, project_id, artifact_id, playground_id, job_id] {
        update_bytes_with_len(&mut hasher, value.as_bytes())?;
    }
    hasher.update(&base_index_version.revision.to_le_bytes());
    hasher.update(base_index_version.digest.as_bytes());
    hasher.update(index_delta_digest(index_delta)?.as_bytes());

    let manifest_count = u64::try_from(canonical_manifests.len()).map_err(|_| {
        ValidationError::new(
            ValidationErrorKind::Overflow,
            "publication Manifest count exceeds u64",
        )
    })?;
    hasher.update(&manifest_count.to_le_bytes());
    for manifest_id in canonical_manifests.keys() {
        hasher.update(manifest_id.as_bytes());
    }

    let object_count = u64::try_from(canonical_objects.len()).map_err(|_| {
        ValidationError::new(
            ValidationErrorKind::Overflow,
            "publication ObjectSpec count exceeds u64",
        )
    })?;
    hasher.update(&object_count.to_le_bytes());
    for (object_id, size) in canonical_objects {
        hasher.update(object_id.as_bytes());
        hasher.update(&size.to_le_bytes());
    }
    Ok(ContentDigest::from_bytes(*hasher.finalize().as_bytes()))
}

fn invalid_publication(message: impl Into<String>) -> ValidationError {
    ValidationError::new(ValidationErrorKind::InvalidPublication, message)
}

fn canonical_hasher(domain: &[u8]) -> blake3::Hasher {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(domain.len() as u64).to_le_bytes());
    hasher.update(domain);
    hasher.update(&CANONICAL_ENCODING_VERSION.to_le_bytes());
    hasher
}

const fn chunking_tag(chunking: ChunkingStrategy) -> u8 {
    match chunking {
        ChunkingStrategy::FastCdc => 1,
        ChunkingStrategy::WholeFile => 2,
    }
}

const fn directory_kind_tag(kind: DirectoryEntryKind) -> u8 {
    match kind {
        DirectoryEntryKind::File => 1,
        DirectoryEntryKind::Directory => 2,
    }
}

fn update_file_record(hasher: &mut blake3::Hasher, record: &FileRecord) -> ValidationResult<()> {
    update_bytes_with_len(hasher, record.path.as_str().as_bytes())?;
    hasher.update(record.manifest_id.as_bytes());
    hasher.update(&record.total_size.to_le_bytes());
    hasher.update(&record.chunk_count.to_le_bytes());
    Ok(())
}

fn update_bytes_with_len(hasher: &mut blake3::Hasher, bytes: &[u8]) -> ValidationResult<()> {
    let length = u64::try_from(bytes.len()).map_err(|_| {
        ValidationError::new(
            ValidationErrorKind::Overflow,
            "canonical string length exceeds u64",
        )
    })?;
    hasher.update(&length.to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

fn validate_next_index_path(
    previous: Option<&crate::LogicalPath>,
    portable_paths: &mut BTreeSet<String>,
    path: &crate::LogicalPath,
) -> ValidationResult<()> {
    if previous.is_some_and(|previous| previous >= path) {
        return Err(ValidationError::new(
            ValidationErrorKind::InvalidIndex,
            format!("Index records must be strictly ordered by path: {path}"),
        ));
    }
    let portable_path = path.portable_key();
    if portable_paths.contains(&portable_path) {
        return Err(ValidationError::new(
            ValidationErrorKind::InvalidIndex,
            format!("Index paths collide under portable case rules: {path}"),
        ));
    }
    for (offset, _) in portable_path.match_indices('/') {
        if portable_paths.contains(&portable_path[..offset]) {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidIndex,
                format!("an Index file path is also an ancestor of another path: {path}"),
            ));
        }
    }
    let descendant_prefix = format!("{portable_path}/");
    if portable_paths
        .range(descendant_prefix.clone()..)
        .next()
        .is_some_and(|candidate| candidate.starts_with(&descendant_prefix))
    {
        return Err(ValidationError::new(
            ValidationErrorKind::InvalidIndex,
            format!("an Index file path is also an ancestor of another path: {path}"),
        ));
    }
    portable_paths.insert(portable_path);
    Ok(())
}
