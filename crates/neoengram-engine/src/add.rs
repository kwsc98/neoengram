use std::collections::{BTreeMap, BTreeSet};

use neoengram_core::{ChunkingStrategy, ContentDigest, IndexDelta, Manifest, ObjectId};
use serde::{Deserialize, Serialize};

use crate::{
    canonical::CanonicalHasher, EngineError, EngineResult, ErrorCode, IndexMutation, IndexVersion,
    ObjectSpec, PathScope,
};

/// Domain separator for the canonical identity of a complete [`PreparedAdd`] candidate.
pub const PREPARED_ADD_HASH_DOMAIN: &[u8] = b"neoengram-engine-prepared-add-v1";

/// Logical managed resource identity. No physical mount path is allowed here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedResource {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub job_id: String,
}

impl ManagedResource {
    pub fn validate(&self) -> EngineResult<()> {
        for (name, value) in [
            ("tenant_id", &self.tenant_id),
            ("project_id", &self.project_id),
            ("artifact_id", &self.artifact_id),
            ("playground_id", &self.playground_id),
            ("job_id", &self.job_id),
        ] {
            if value.is_empty() {
                return Err(EngineError::new(
                    ErrorCode::InvalidArgument,
                    format!("{name} cannot be empty"),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", content = "strategy", rename_all = "snake_case")]
pub enum ChunkingSelection {
    /// Uses the core format default (`FastCdc`). Adapters for artifacts with another persisted
    /// policy must resolve that policy to [`Self::Explicit`] before invoking the Engine.
    ArtifactDefault,
    /// Uses the exact strategy supplied by the assignment or Standalone policy adapter.
    Explicit(ChunkingStrategy),
}

/// Input to the non-authoritative Agent side of managed `add`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareAddRequest {
    pub resource: ManagedResource,
    pub expected_index_version: IndexVersion,
    pub scopes: Vec<PathScope>,
    pub stage_deletions: bool,
    pub chunking: ChunkingSelection,
    pub candidate_batch_id: String,
}

impl PrepareAddRequest {
    pub fn validate(&self) -> EngineResult<()> {
        self.resource.validate()?;
        if self.scopes.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "managed add requires at least one logical scope",
            ));
        }
        if self.candidate_batch_id.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "candidate_batch_id cannot be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddStatistics {
    pub files_scanned: u64,
    pub files_upserted: u64,
    pub paths_deleted: u64,
    pub bytes_scanned: u64,
    pub chunks_observed: u64,
    pub objects_created: u64,
    pub objects_reused: u64,
    pub bytes_published: u64,
}

/// Non-authoritative candidate returned by an Agent. Only the control plane may publish it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedAdd {
    pub resource: ManagedResource,
    pub base_index_version: IndexVersion,
    pub candidate_batch_id: String,
    /// Complete paged candidate prepared against `base_index_version`.
    pub index_delta: IndexDelta,
    /// Immutable metadata required by upserted file records.
    pub manifests: Vec<Manifest>,
    /// Exact payload objects referenced by the prepared Manifests.
    pub object_specs: Vec<ObjectSpec>,
    pub statistics: AddStatistics,
    candidate_digest: ContentDigest,
    pub prepared_at_unix_ms: u64,
}

impl PreparedAdd {
    /// Constructs a validated candidate and computes its canonical digest.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        resource: ManagedResource,
        base_index_version: IndexVersion,
        candidate_batch_id: impl Into<String>,
        index_delta: IndexDelta,
        manifests: Vec<Manifest>,
        object_specs: Vec<ObjectSpec>,
        statistics: AddStatistics,
        prepared_at_unix_ms: u64,
    ) -> EngineResult<Self> {
        let mut prepared = Self {
            resource,
            base_index_version,
            candidate_batch_id: candidate_batch_id.into(),
            index_delta,
            manifests,
            object_specs,
            statistics,
            candidate_digest: ContentDigest::from_bytes([0; 32]),
            prepared_at_unix_ms,
        };
        prepared.validate_content()?;
        prepared.candidate_digest = prepared.compute_canonical_digest()?;
        Ok(prepared)
    }

    /// Returns the canonical digest stored with this candidate.
    #[must_use]
    pub const fn candidate_digest(&self) -> ContentDigest {
        self.candidate_digest
    }

    /// Returns the backend-independent identity of the exact material proposed for publication.
    ///
    /// Unlike [`Self::candidate_digest`], this excludes execution statistics and timestamps so
    /// Agent metadata pages and the central staging validator can recompute the same value.
    pub fn publication_digest(&self) -> EngineResult<ContentDigest> {
        self.validate_content()?;
        Ok(neoengram_core::canonical::add_publication_digest(
            &self.resource.tenant_id,
            &self.resource.project_id,
            &self.resource.artifact_id,
            &self.resource.playground_id,
            &self.resource.job_id,
            &self.base_index_version,
            &self.index_delta,
            &self.manifests,
            &self.object_specs,
        )?)
    }

    /// Recomputes the canonical digest from all candidate fields except the digest itself.
    pub fn canonical_digest(&self) -> EngineResult<ContentDigest> {
        self.validate_content()?;
        self.compute_canonical_digest()
    }

    pub fn validate_for(&self, request: &PrepareAddRequest) -> EngineResult<()> {
        request.validate()?;
        self.validate()?;
        if self.resource != request.resource
            || self.base_index_version != request.expected_index_version
            || self.candidate_batch_id != request.candidate_batch_id
        {
            return Err(EngineError::new(
                ErrorCode::Conflict,
                "prepared add does not match its request",
            ));
        }
        Ok(())
    }

    /// Validates the candidate independently of a transport assignment.
    pub fn validate(&self) -> EngineResult<()> {
        self.validate_content()?;
        let canonical_digest = self.compute_canonical_digest()?;
        if self.candidate_digest != canonical_digest {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "prepared add digest does not match its canonical content",
            )
            .with_context("stored_digest", self.candidate_digest.to_string())
            .with_context("canonical_digest", canonical_digest.to_string()));
        }
        Ok(())
    }

    fn validate_content(&self) -> EngineResult<()> {
        self.resource.validate()?;
        if self.candidate_batch_id.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "prepared candidate_batch_id cannot be empty",
            ));
        }
        if self.index_delta.base_version != self.base_index_version {
            return Err(EngineError::new(
                ErrorCode::Conflict,
                "prepared IndexDelta base does not match base_index_version",
            ));
        }
        self.index_delta.validate()?;
        let mut objects = BTreeMap::<ObjectId, u64>::new();
        for object in &self.object_specs {
            if objects.insert(object.id, object.size).is_some() {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "prepared add contains a duplicate ObjectSpec",
                )
                .with_context("object_id", object.id.to_string()));
            }
        }
        let mut manifests = BTreeMap::new();
        let mut referenced_objects = BTreeMap::<ObjectId, u64>::new();
        for manifest in &self.manifests {
            manifest.validate()?;
            let manifest_id = manifest.canonical_id()?;
            if manifests.insert(manifest_id, manifest).is_some() {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "prepared add contains a duplicate Manifest",
                )
                .with_context("manifest_id", manifest_id.to_string()));
            }
            for chunk in &manifest.chunks {
                if let Some(previous_size) = referenced_objects.insert(chunk.object_id, chunk.size)
                {
                    if previous_size != chunk.size {
                        return Err(EngineError::new(
                            ErrorCode::IntegrityViolation,
                            "prepared Manifests disagree about an object size",
                        )
                        .with_context("object_id", chunk.object_id.to_string()));
                    }
                }
                if objects.get(&chunk.object_id) != Some(&chunk.size) {
                    return Err(EngineError::new(
                        ErrorCode::ObjectMissing,
                        "prepared Manifest references an absent ObjectSpec",
                    )
                    .with_context("object_id", chunk.object_id.to_string()));
                }
            }
        }
        let mut referenced_manifests = BTreeSet::new();
        for mutation in self.index_delta.iter() {
            if let IndexMutation::Upsert { record } = mutation {
                referenced_manifests.insert(record.manifest_id);
                let manifest = manifests.get(&record.manifest_id).ok_or_else(|| {
                    EngineError::new(
                        ErrorCode::ObjectMissing,
                        "Index upsert references an absent prepared Manifest",
                    )
                    .with_context("manifest_id", record.manifest_id.to_string())
                })?;
                if manifest.total_size != record.total_size
                    || manifest.chunk_count()? != record.chunk_count
                {
                    return Err(EngineError::new(
                        ErrorCode::IntegrityViolation,
                        "Index upsert metadata differs from its prepared Manifest",
                    )
                    .with_context("logical_path", record.path.to_string()));
                }
            }
        }
        if manifests.keys().copied().collect::<BTreeSet<_>>() != referenced_manifests {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "prepared add contains a Manifest not referenced by its IndexDelta",
            ));
        }
        if objects != referenced_objects {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "prepared ObjectSpecs are not the exact Manifest object set",
            ));
        }
        Ok(())
    }

    fn compute_canonical_digest(&self) -> EngineResult<ContentDigest> {
        let mut hasher = CanonicalHasher::new(PREPARED_ADD_HASH_DOMAIN)?;
        hasher.write_str(&self.resource.tenant_id)?;
        hasher.write_str(&self.resource.project_id)?;
        hasher.write_str(&self.resource.artifact_id)?;
        hasher.write_str(&self.resource.playground_id)?;
        hasher.write_str(&self.resource.job_id)?;
        hasher.write_u64(self.base_index_version.revision);
        hasher.write_digest(self.base_index_version.digest)?;
        hasher.write_str(&self.candidate_batch_id)?;

        hasher.write_u64(self.index_delta.base_version.revision);
        hasher.write_digest(self.index_delta.base_version.digest)?;
        hasher.write_len(self.index_delta.mutation_count())?;
        for mutation in self.index_delta.iter() {
            match mutation {
                IndexMutation::Upsert { record } => {
                    hasher.write_u8(1);
                    hasher.write_str(record.path.as_str())?;
                    hasher.write_bytes(record.manifest_id.as_bytes())?;
                    hasher.write_u64(record.total_size);
                    hasher.write_u64(record.chunk_count);
                }
                IndexMutation::Delete { path } => {
                    hasher.write_u8(2);
                    hasher.write_str(path.as_str())?;
                }
            }
        }
        hasher.write_digest(self.index_delta.result_digest)?;

        let mut manifests = self
            .manifests
            .iter()
            .map(|manifest| Ok((manifest.canonical_id()?, manifest)))
            .collect::<EngineResult<Vec<_>>>()?;
        manifests.sort_by_key(|(id, _)| *id);
        hasher.write_len(manifests.len())?;
        for (manifest_id, manifest) in manifests {
            hasher.write_bytes(manifest_id.as_bytes())?;
            hasher.write_u64(manifest.total_size);
            hasher.write_u8(match manifest.chunking {
                ChunkingStrategy::FastCdc => 1,
                ChunkingStrategy::WholeFile => 2,
            });
            hasher.write_len(manifest.chunks.len())?;
            for chunk in &manifest.chunks {
                hasher.write_bytes(chunk.object_id.as_bytes())?;
                hasher.write_u64(chunk.offset);
                hasher.write_u64(chunk.size);
            }
        }

        let mut object_specs = self.object_specs.iter().collect::<Vec<_>>();
        object_specs.sort_by_key(|object| object.id);
        hasher.write_len(object_specs.len())?;
        for object in object_specs {
            hasher.write_bytes(object.id.as_bytes())?;
            hasher.write_u64(object.size);
        }

        hasher.write_u64(self.statistics.files_scanned);
        hasher.write_u64(self.statistics.files_upserted);
        hasher.write_u64(self.statistics.paths_deleted);
        hasher.write_u64(self.statistics.bytes_scanned);
        hasher.write_u64(self.statistics.chunks_observed);
        hasher.write_u64(self.statistics.objects_created);
        hasher.write_u64(self.statistics.objects_reused);
        hasher.write_u64(self.statistics.bytes_published);
        hasher.write_u64(self.prepared_at_unix_ms);
        Ok(hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    use neoengram_core::{
        ChunkRef, ContentDigest, FileRecord, IndexDelta, IndexDeltaPage, IndexVersion, LogicalPath,
        Manifest, ObjectId, MAX_INDEX_MUTATIONS_PER_PAGE,
    };

    use super::*;

    fn resource(job_id: &str) -> ManagedResource {
        ManagedResource {
            tenant_id: "tenant-a".to_owned(),
            project_id: "project-a".to_owned(),
            artifact_id: "artifact-a".to_owned(),
            playground_id: "playground-a".to_owned(),
            job_id: job_id.to_owned(),
        }
    }

    #[test]
    fn prepared_add_is_bound_to_the_exact_request() {
        let version = IndexVersion::new(7, ContentDigest::from_bytes([7; 32]));
        let request = PrepareAddRequest {
            resource: resource("job-a"),
            expected_index_version: version,
            scopes: vec![PathScope::Root],
            stage_deletions: true,
            chunking: ChunkingSelection::ArtifactDefault,
            candidate_batch_id: "batch-a".to_owned(),
        };
        request.validate().unwrap();
        let prepared = PreparedAdd::new(
            request.resource.clone(),
            version,
            request.candidate_batch_id.clone(),
            IndexDelta::new(version, Vec::new(), ContentDigest::from_bytes([8; 32])).unwrap(),
            Vec::new(),
            Vec::new(),
            AddStatistics::default(),
            10,
        )
        .unwrap();
        prepared.validate_for(&request).unwrap();
        assert_eq!(
            prepared.candidate_digest(),
            prepared.canonical_digest().unwrap()
        );

        let mut tampered = prepared.clone();
        tampered.prepared_at_unix_ms += 1;
        assert_eq!(
            tampered.validate_for(&request).unwrap_err().code(),
            ErrorCode::IntegrityViolation
        );

        let mut forged_digest = prepared.clone();
        forged_digest.candidate_digest = ContentDigest::from_bytes([9; 32]);
        assert_eq!(
            forged_digest.validate().unwrap_err().code(),
            ErrorCode::IntegrityViolation
        );

        let mut mismatched = request;
        mismatched.resource = resource("job-b");
        assert_eq!(
            prepared.validate_for(&mismatched).unwrap_err().code(),
            ErrorCode::Conflict
        );
    }

    #[test]
    fn prepared_add_requires_an_exact_closed_metadata_graph() {
        let version = IndexVersion::new(7, ContentDigest::from_bytes([7; 32]));
        let object_id = ObjectId::for_bytes(b"payload");
        let manifest = Manifest::new(
            7,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(object_id, 0, 7).unwrap()],
        )
        .unwrap();
        let record =
            FileRecord::from_manifest(LogicalPath::parse("dataset.bin").unwrap(), &manifest)
                .unwrap();
        let mut prepared = PreparedAdd::new(
            resource("job-a"),
            version,
            "batch-a",
            IndexDelta::new(
                version,
                vec![IndexMutation::Upsert { record }],
                ContentDigest::from_bytes([8; 32]),
            )
            .unwrap(),
            vec![manifest],
            vec![ObjectSpec::new(object_id, 7)],
            AddStatistics::default(),
            10,
        )
        .unwrap();
        prepared.object_specs.clear();
        assert_eq!(
            prepared.validate().unwrap_err().code(),
            ErrorCode::ObjectMissing
        );

        prepared.object_specs.push(ObjectSpec::new(object_id, 7));
        prepared.validate().unwrap();
        prepared
            .object_specs
            .push(ObjectSpec::for_bytes(b"unreferenced"));
        assert_eq!(
            prepared.validate().unwrap_err().code(),
            ErrorCode::IntegrityViolation
        );
    }

    #[test]
    fn prepared_add_digest_rejects_tampering_and_normalizes_set_order() {
        let version = IndexVersion::new(7, ContentDigest::from_bytes([7; 32]));
        let first_object = ObjectId::for_bytes(b"first");
        let second_object = ObjectId::for_bytes(b"second");
        let first_manifest = Manifest::new(
            5,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(first_object, 0, 5).unwrap()],
        )
        .unwrap();
        let second_manifest = Manifest::new(
            6,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(second_object, 0, 6).unwrap()],
        )
        .unwrap();
        let first_record =
            FileRecord::from_manifest(LogicalPath::parse("first.bin").unwrap(), &first_manifest)
                .unwrap();
        let second_record =
            FileRecord::from_manifest(LogicalPath::parse("second.bin").unwrap(), &second_manifest)
                .unwrap();
        let prepared = PreparedAdd::new(
            resource("job-a"),
            version,
            "batch-a",
            IndexDelta::new(
                version,
                vec![
                    IndexMutation::Upsert {
                        record: first_record,
                    },
                    IndexMutation::Upsert {
                        record: second_record,
                    },
                ],
                ContentDigest::from_bytes([8; 32]),
            )
            .unwrap(),
            vec![first_manifest, second_manifest],
            vec![
                ObjectSpec::new(first_object, 5),
                ObjectSpec::new(second_object, 6),
            ],
            AddStatistics {
                files_scanned: 2,
                files_upserted: 2,
                bytes_scanned: 11,
                chunks_observed: 2,
                objects_created: 2,
                bytes_published: 11,
                ..AddStatistics::default()
            },
            10,
        )
        .unwrap();
        assert_eq!(
            prepared.candidate_digest().to_string(),
            "9b93385a0833c93f21de1c4cd6e7c23f19ca1b5270b5f977a75cb8ad8bb18ce9"
        );

        let mut reordered = prepared.clone();
        reordered.manifests.reverse();
        reordered.object_specs.reverse();
        reordered.validate().unwrap();
        assert_eq!(
            reordered.canonical_digest().unwrap(),
            prepared.candidate_digest()
        );

        let mut tampered = prepared;
        tampered.statistics.bytes_published += 1;
        assert_eq!(
            tampered.validate().unwrap_err().code(),
            ErrorCode::IntegrityViolation
        );
        assert_ne!(
            tampered.canonical_digest().unwrap(),
            tampered.candidate_digest()
        );
    }

    #[test]
    fn prepared_add_digest_is_independent_of_index_page_boundaries() {
        let version = IndexVersion::new(7, ContentDigest::from_bytes([7; 32]));
        let result_digest = ContentDigest::from_bytes([8; 32]);
        let mutations = (0..=MAX_INDEX_MUTATIONS_PER_PAGE)
            .map(|index| IndexMutation::Delete {
                path: LogicalPath::parse(format!("files/file-{index:05}.bin")).unwrap(),
            })
            .collect::<Vec<_>>();
        let deterministic = IndexDelta::new(version, mutations.clone(), result_digest).unwrap();
        let alternate = IndexDelta::from_pages(
            version,
            vec![
                IndexDeltaPage::new(0, mutations[..1].to_vec()).unwrap(),
                IndexDeltaPage::new(1, mutations[1..].to_vec()).unwrap(),
            ],
            result_digest,
        )
        .unwrap();

        let first = PreparedAdd::new(
            resource("job-paged"),
            version,
            "batch-paged",
            deterministic,
            Vec::new(),
            Vec::new(),
            AddStatistics::default(),
            10,
        )
        .unwrap();
        let second = PreparedAdd::new(
            resource("job-paged"),
            version,
            "batch-paged",
            alternate,
            Vec::new(),
            Vec::new(),
            AddStatistics::default(),
            10,
        )
        .unwrap();

        assert_eq!(first.index_delta.page_count(), 2);
        assert_eq!(second.index_delta.page_count(), 2);
        assert_ne!(first.index_delta.pages, second.index_delta.pages);
        assert_eq!(first.candidate_digest(), second.candidate_digest());
    }
}
