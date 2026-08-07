use std::collections::{BTreeMap, BTreeSet};

use neoengram_core::{
    validate_index_mutation_paths, ChunkRef, ChunkingStrategy, FileRecord, IndexVersion, Manifest,
    ManifestId, ObjectId,
};
use neoengram_protocol::{
    AddAssignment, AssignmentGeneration, AssignmentId, IndexDeltaRecord, JobPrepared, JobState,
    LeaseMode, ManifestRecord, MetadataBatchDescriptor, MetadataBatchKind, MetadataBatchRecords,
    MetadataPublication, WireIndexVersion,
};

use crate::{
    AddJobSpec, AssignmentTarget, CentralError, CentralErrorCode, CentralResult, JobRecord,
    MetadataBatchStager, ValidatedMetadata,
};

pub(crate) fn invalid(code: CentralErrorCode, message: impl Into<String>) -> CentralError {
    CentralError::new(code, message)
}

pub(crate) fn same_index_version(left: &WireIndexVersion, right: &WireIndexVersion) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

pub(crate) fn validate_initial_index_snapshot(
    version: &WireIndexVersion,
    records: &[FileRecord],
) -> CentralResult<()> {
    version.validate()?;
    let computed =
        IndexVersion::from_snapshot(version.revision.get(), records).map_err(|error| {
            invalid(
                CentralErrorCode::MetadataInvalid,
                format!("invalid initial Index snapshot: {error}"),
            )
        })?;
    if computed.digest != version.digest {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            format!(
                "initial Index digest {} differs from canonical records {}",
                version.digest, computed.digest
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_job_spec(spec: &AddJobSpec, now_ms: u64) -> CentralResult<()> {
    let computed_request_digest = spec.computed_request_digest()?;
    if spec.request_digest != computed_request_digest {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            format!(
                "managed Add request digest mismatch: expected {}, observed {}",
                spec.request_digest, computed_request_digest
            ),
        ));
    }
    if spec.deadline_unix_ms.get() <= now_ms {
        return Err(invalid(
            CentralErrorCode::DeadlineExceeded,
            "managed Add deadline has already elapsed",
        ));
    }
    Ok(())
}

pub(crate) fn validate_assignment_target(
    target: &AssignmentTarget,
    now_ms: u64,
) -> CentralResult<()> {
    for (name, generation) in [
        ("assignment_generation", target.assignment_generation.get()),
        ("placement_generation", target.placement_generation.get()),
        ("mount_generation", target.mount_generation.get()),
        ("owner_generation", target.owner_generation.get()),
    ] {
        if generation == 0 {
            return Err(invalid(
                CentralErrorCode::GenerationMismatch,
                format!("{name} must be greater than zero"),
            ));
        }
    }
    if let Some(lease) = &target.lease {
        if lease.expires_at_unix_ms.get() <= now_ms {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "assignment lease has already elapsed",
            ));
        }
        match (lease.mode, lease.fencing_token) {
            (LeaseMode::ExclusiveWrite, Some(token)) if token.get() > 0 => {}
            (LeaseMode::SharedRead, None) => {}
            (LeaseMode::ExclusiveWrite, _) => {
                return Err(invalid(
                    CentralErrorCode::GenerationMismatch,
                    "exclusive assignment lease requires a non-zero fencing token",
                ));
            }
            (LeaseMode::SharedRead, Some(_)) => {
                return Err(invalid(
                    CentralErrorCode::MetadataInvalid,
                    "shared assignment lease cannot carry a fencing token",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_prepared(job: &JobRecord, prepared: &JobPrepared) -> CentralResult<()> {
    prepared.validate()?;
    let assignment = job.assignment.as_ref().ok_or_else(|| {
        invalid(
            CentralErrorCode::InvalidState,
            "job has no persisted assignment",
        )
    })?;
    validate_report_identity(
        assignment,
        &prepared.job_id,
        &prepared.assignment_id,
        prepared.assignment_generation,
    )?;
    if !same_index_version(
        &prepared.base_index_version,
        &assignment.expected_index_version,
    ) {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            "prepared base IndexVersion differs from the assignment CAS expectation",
        ));
    }
    let mut batch_ids = BTreeSet::new();
    let mut kinds = [false; 3];
    for descriptor in &prepared.metadata_batches {
        descriptor.validate()?;
        validate_descriptor_scope(assignment, descriptor)?;
        if !batch_ids.insert(descriptor.batch_id.clone()) {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "prepared report repeats a metadata batch ID",
            ));
        }
        let kind = kind_index(descriptor.kind);
        if kinds[kind] {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "prepared Add declares more than one batch of the same kind",
            ));
        }
        kinds[kind] = true;
    }
    if kinds != [true, true, true] {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            "prepared Add must declare exactly one Manifest, IndexDelta, and ObjectReceipt batch",
        ));
    }
    Ok(())
}

pub(crate) fn validate_descriptor_scope(
    assignment: &AddAssignment,
    descriptor: &MetadataBatchDescriptor,
) -> CentralResult<()> {
    let scope = &descriptor.scope;
    if scope.tenant_id != assignment.tenant_id
        || scope.project_id != assignment.project_id
        || scope.artifact_id != assignment.artifact_id
        || scope.playground_id != assignment.playground_id
        || scope.job_id != assignment.job_id
        || !same_index_version(
            &scope.base_index_version,
            &assignment.expected_index_version,
        )
    {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            "metadata batch scope differs from its persisted assignment",
        ));
    }
    Ok(())
}

pub(crate) fn validate_report_identity(
    assignment: &AddAssignment,
    job_id: &neoengram_protocol::JobId,
    assignment_id: &AssignmentId,
    generation: AssignmentGeneration,
) -> CentralResult<()> {
    if job_id != &assignment.job_id || assignment_id != &assignment.assignment_id {
        return Err(invalid(
            CentralErrorCode::AssignmentMismatch,
            "agent report does not identify the persisted job assignment",
        ));
    }
    if generation != assignment.assignment_generation {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "agent report carries a stale assignment generation",
        ));
    }
    Ok(())
}

pub(crate) fn validate_terminal_state(state: JobState) -> CentralResult<()> {
    if matches!(
        state,
        JobState::Failed
            | JobState::Cancelled
            | JobState::TimedOut
            | JobState::RecoveryRequired
            | JobState::Rejected
    ) {
        Ok(())
    } else {
        Err(invalid(
            CentralErrorCode::InvalidState,
            "terminal agent report carries a state not accepted as a failure outcome",
        ))
    }
}

pub(crate) async fn validate_staged_metadata(
    job: &JobRecord,
    stager: &dyn MetadataBatchStager,
) -> CentralResult<ValidatedMetadata> {
    let assignment = job.assignment.as_ref().ok_or_else(|| {
        invalid(
            CentralErrorCode::InvalidState,
            "job has no persisted assignment",
        )
    })?;
    let prepared = job.prepared.as_ref().ok_or_else(|| {
        invalid(
            CentralErrorCode::InvalidState,
            "job has no persisted prepared report",
        )
    })?;

    let mut manifest_fragments = BTreeMap::new();
    let mut manifest_objects: BTreeMap<ObjectId, u64> = BTreeMap::new();
    let mut receipt_objects: BTreeMap<ObjectId, u64> = BTreeMap::new();
    let mut receipt_ids = BTreeSet::new();
    let mut placements = Vec::new();
    let mut mutations = Vec::new();
    let mut publication_pages = Vec::new();

    for declared in &prepared.metadata_batches {
        let staged = stager
            .get(&assignment.tenant_id, &declared.batch_id)
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::BatchIncomplete,
                    format!(
                        "metadata batch {} has no staged descriptor",
                        declared.batch_id
                    ),
                )
            })?;
        if staged.descriptor != *declared {
            return Err(invalid(
                CentralErrorCode::BatchTampered,
                format!("metadata batch {} descriptor changed", declared.batch_id),
            ));
        }
        if !staged.is_complete() {
            return Err(invalid(
                CentralErrorCode::BatchIncomplete,
                format!(
                    "metadata batch {} has {}/{} pages",
                    declared.batch_id,
                    staged.pages.len(),
                    declared.page_count
                ),
            ));
        }
        for page_number in 0..declared.page_count {
            let page = staged.pages.get(&page_number).ok_or_else(|| {
                invalid(
                    CentralErrorCode::BatchIncomplete,
                    format!(
                        "metadata batch {} is missing page {page_number}",
                        declared.batch_id
                    ),
                )
            })?;
            declared.validate_page(page)?;
            publication_pages.push(page.clone());
            match &page.records {
                MetadataBatchRecords::Manifest(records) => {
                    for record in records {
                        collect_manifest_fragment(&mut manifest_fragments, record)?;
                    }
                }
                MetadataBatchRecords::IndexDelta(records) => {
                    mutations.extend(records.iter().cloned());
                }
                MetadataBatchRecords::ObjectReceipt(records) => {
                    for record in records {
                        if record.tenant_id != assignment.tenant_id
                            || record.artifact_id != assignment.artifact_id
                            || record.job_id != assignment.job_id
                            || record.storage_volume_id != assignment.storage_volume_id
                            || record.artifact_placement_id != assignment.artifact_placement_id
                        {
                            return Err(invalid(
                                CentralErrorCode::MetadataInvalid,
                                "object receipt scope or placement differs from the assignment",
                            ));
                        }
                        if !receipt_ids.insert(record.receipt_id.clone()) {
                            return Err(invalid(
                                CentralErrorCode::MetadataInvalid,
                                format!(
                                    "object receipt {} was declared more than once",
                                    record.receipt_id
                                ),
                            ));
                        }
                        if receipt_objects.contains_key(&record.object_id) {
                            return Err(invalid(
                                CentralErrorCode::MetadataInvalid,
                                format!(
                                    "object {} has more than one receipt declaration",
                                    record.object_id
                                ),
                            ));
                        }
                        insert_object_requirement(
                            &mut receipt_objects,
                            record.object_id,
                            record.size.get(),
                        )?;
                        placements.push(record.clone());
                    }
                }
            }
        }
    }

    let manifests = assemble_manifest_fragments(manifest_fragments, &mut manifest_objects)?;

    if manifest_objects != receipt_objects {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            "Manifest object declarations and ObjectReceipt declarations differ",
        ));
    }

    validate_index_delta_order(&mutations)?;
    let mutation_paths = mutations.iter().map(index_delta_path).collect::<Vec<_>>();
    if !assignment.all {
        for path in &mutation_paths {
            if !assignment
                .paths
                .iter()
                .any(|scope| scope == *path || scope.is_ancestor_of(path))
            {
                return Err(invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("IndexDelta path {path} is outside the Add assignment scope"),
                ));
            }
        }
    }
    let mut referenced_manifests = BTreeSet::new();
    for mutation in &mutations {
        if let IndexDeltaRecord::Upsert {
            manifest_id,
            total_size,
            chunk_count,
            ..
        } = mutation
        {
            referenced_manifests.insert(*manifest_id);
            let expected = manifests.get(manifest_id).ok_or_else(|| {
                invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("IndexDelta references undeclared Manifest {manifest_id}"),
                )
            })?;
            if *expected != (total_size.get(), chunk_count.get()) {
                return Err(invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("IndexDelta metadata differs from Manifest {manifest_id}"),
                ));
            }
        }
    }
    if manifests.keys().copied().collect::<BTreeSet<_>>() != referenced_manifests {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            "Manifest batch is not the exact set referenced by IndexDelta upserts",
        ));
    }

    let publication = MetadataPublication::from_pages(&publication_pages).map_err(|error| {
        invalid(
            CentralErrorCode::MetadataInvalid,
            format!("invalid staged publication material: {error}"),
        )
    })?;
    let scope = &prepared
        .metadata_batches
        .first()
        .ok_or_else(|| {
            invalid(
                CentralErrorCode::MetadataInvalid,
                "prepared publication has no metadata scope",
            )
        })?
        .scope;
    let observed_publication_digest = publication
        .publication_digest(scope, prepared.result_index_digest)
        .map_err(|error| {
            invalid(
                CentralErrorCode::MetadataInvalid,
                format!("cannot canonicalize staged publication: {error}"),
            )
        })?;
    if observed_publication_digest != prepared.publication_digest {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            format!(
                "staged publication digest {observed_publication_digest} differs from prepared {}",
                prepared.publication_digest
            ),
        ));
    }
    Ok(ValidatedMetadata {
        manifests: publication.manifests,
        mutations,
        placements,
    })
}

struct ManifestFragments {
    total_size: u64,
    chunking: ChunkingStrategy,
    chunks_by_start: BTreeMap<u64, Vec<ChunkRef>>,
}

fn collect_manifest_fragment(
    manifests: &mut BTreeMap<ManifestId, ManifestFragments>,
    record: &ManifestRecord,
) -> CentralResult<()> {
    let chunks = record
        .chunks
        .iter()
        .map(|chunk| ChunkRef::new(chunk.object_id, chunk.offset.get(), chunk.size.get()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            invalid(
                CentralErrorCode::MetadataInvalid,
                format!("invalid Manifest chunk: {error}"),
            )
        })?;
    let total_size = record.total_size.get();
    let chunking = record.chunking.into();
    let manifest = &mut manifests
        .entry(record.manifest_id)
        .or_insert_with(|| ManifestFragments {
            total_size,
            chunking,
            chunks_by_start: BTreeMap::new(),
        });
    if manifest.total_size != total_size || manifest.chunking != chunking {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            format!(
                "Manifest {} fragments disagree on total_size or chunking",
                record.manifest_id
            ),
        ));
    }
    if manifest
        .chunks_by_start
        .insert(record.chunk_start.get(), chunks)
        .is_some()
    {
        return Err(invalid(
            CentralErrorCode::MetadataInvalid,
            format!(
                "Manifest {} repeats chunk ordinal {}",
                record.manifest_id, record.chunk_start
            ),
        ));
    }
    Ok(())
}

fn assemble_manifest_fragments(
    fragments: BTreeMap<ManifestId, ManifestFragments>,
    objects: &mut BTreeMap<ObjectId, u64>,
) -> CentralResult<BTreeMap<ManifestId, (u64, u64)>> {
    let mut manifests = BTreeMap::new();
    for (manifest_id, fragments) in fragments {
        let mut next_chunk_start = 0_u64;
        let mut chunks = Vec::new();
        for (chunk_start, fragment) in fragments.chunks_by_start {
            if chunk_start != next_chunk_start {
                return Err(invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!(
                        "Manifest {manifest_id} has a chunk ordinal gap or overlap at {chunk_start}; expected {next_chunk_start}"
                    ),
                ));
            }
            let fragment_len = u64::try_from(fragment.len()).map_err(|_| {
                invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("Manifest {manifest_id} chunk count does not fit u64"),
                )
            })?;
            next_chunk_start = next_chunk_start.checked_add(fragment_len).ok_or_else(|| {
                invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("Manifest {manifest_id} chunk ordinal exceeds u64"),
                )
            })?;
            chunks.extend(fragment);
        }

        let manifest =
            Manifest::new(fragments.total_size, fragments.chunking, chunks).map_err(|error| {
                invalid(
                    CentralErrorCode::MetadataInvalid,
                    format!("invalid Manifest {manifest_id} metadata: {error}"),
                )
            })?;
        let observed = manifest
            .canonical_id()
            .map_err(|error| invalid(CentralErrorCode::MetadataInvalid, error.to_string()))?;
        if observed != manifest_id {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                format!("Manifest {manifest_id} does not match its canonical content {observed}"),
            ));
        }
        for chunk in &manifest.chunks {
            insert_object_requirement(objects, chunk.object_id, chunk.size)?;
        }
        manifests.insert(manifest_id, (manifest.total_size, next_chunk_start));
    }
    Ok(manifests)
}

fn validate_index_delta_order(mutations: &[IndexDeltaRecord]) -> CentralResult<()> {
    validate_index_mutation_paths(mutations.iter().map(index_delta_path)).map_err(|error| {
        invalid(
            CentralErrorCode::MetadataInvalid,
            format!("invalid IndexDelta order: {error}"),
        )
    })
}

fn index_delta_path(mutation: &IndexDeltaRecord) -> &neoengram_core::LogicalPath {
    match mutation {
        IndexDeltaRecord::Upsert { path, .. } | IndexDeltaRecord::Delete { path, .. } => path,
    }
}

fn insert_object_requirement(
    objects: &mut BTreeMap<ObjectId, u64>,
    object_id: ObjectId,
    size: u64,
) -> CentralResult<()> {
    if objects
        .insert(object_id, size)
        .is_some_and(|existing| existing != size)
    {
        Err(invalid(
            CentralErrorCode::MetadataInvalid,
            format!("object {object_id} was declared with inconsistent lengths"),
        ))
    } else {
        Ok(())
    }
}

const fn kind_index(kind: MetadataBatchKind) -> usize {
    match kind {
        MetadataBatchKind::Manifest => 0,
        MetadataBatchKind::IndexDelta => 1,
        MetadataBatchKind::ObjectReceipt => 2,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use neoengram_core::{ChunkRef, ChunkingStrategy, LogicalPath, Manifest, ManifestId, ObjectId};
    use neoengram_protocol::{
        DecimalU64, Extensions, IndexDeltaRecord, ManifestRecord, WireChunkRef,
        WireChunkingStrategy,
    };

    use super::{
        assemble_manifest_fragments, collect_manifest_fragment, validate_index_delta_order,
    };

    fn upsert(path: &str) -> IndexDeltaRecord {
        IndexDeltaRecord::Upsert {
            path: LogicalPath::parse(path).expect("test path is valid"),
            manifest_id: ManifestId::from_bytes([7; 32]),
            total_size: DecimalU64::new(0),
            chunk_count: DecimalU64::new(0),
            extensions: Extensions::new(),
        }
    }

    fn delete(path: &str) -> IndexDeltaRecord {
        IndexDeltaRecord::Delete {
            path: LogicalPath::parse(path).expect("test path is valid"),
            extensions: Extensions::new(),
        }
    }

    fn manifest_record(
        manifest_id: ManifestId,
        total_size: u64,
        chunking: WireChunkingStrategy,
        chunk_start: u64,
        chunks: &[(ObjectId, u64, u64)],
    ) -> ManifestRecord {
        ManifestRecord {
            manifest_id,
            total_size: DecimalU64::new(total_size),
            chunking,
            chunk_start: DecimalU64::new(chunk_start),
            chunks: chunks
                .iter()
                .map(|(object_id, offset, size)| WireChunkRef {
                    object_id: *object_id,
                    offset: DecimalU64::new(*offset),
                    size: DecimalU64::new(*size),
                    extensions: Extensions::new(),
                })
                .collect(),
            extensions: Extensions::new(),
        }
    }

    fn two_chunk_manifest() -> (Manifest, ObjectId, ObjectId) {
        let first = ObjectId::from_bytes([0x11; 32]);
        let second = ObjectId::from_bytes([0x22; 32]);
        let manifest = Manifest::new(
            5,
            ChunkingStrategy::FastCdc,
            vec![
                ChunkRef::new(first, 0, 2).unwrap(),
                ChunkRef::new(second, 2, 3).unwrap(),
            ],
        )
        .unwrap();
        (manifest, first, second)
    }

    #[test]
    fn manifest_fragments_from_separate_pages_reassemble_canonically() {
        let (manifest, first, second) = two_chunk_manifest();
        let manifest_id = manifest.canonical_id().unwrap();
        let mut fragments = BTreeMap::new();
        collect_manifest_fragment(
            &mut fragments,
            &manifest_record(
                manifest_id,
                5,
                WireChunkingStrategy::FastCdc,
                0,
                &[(first, 0, 2)],
            ),
        )
        .unwrap();
        collect_manifest_fragment(
            &mut fragments,
            &manifest_record(
                manifest_id,
                5,
                WireChunkingStrategy::FastCdc,
                1,
                &[(second, 2, 3)],
            ),
        )
        .unwrap();

        let mut objects = BTreeMap::new();
        let summaries = assemble_manifest_fragments(fragments, &mut objects).unwrap();

        assert_eq!(summaries.get(&manifest_id), Some(&(5, 2)));
        assert_eq!(objects.get(&first), Some(&2));
        assert_eq!(objects.get(&second), Some(&3));
    }

    #[test]
    fn manifest_fragments_reject_chunk_ordinal_gaps_and_duplicates() {
        let (manifest, first, second) = two_chunk_manifest();
        let manifest_id = manifest.canonical_id().unwrap();

        let mut gap = BTreeMap::new();
        collect_manifest_fragment(
            &mut gap,
            &manifest_record(
                manifest_id,
                5,
                WireChunkingStrategy::FastCdc,
                0,
                &[(first, 0, 2)],
            ),
        )
        .unwrap();
        collect_manifest_fragment(
            &mut gap,
            &manifest_record(
                manifest_id,
                5,
                WireChunkingStrategy::FastCdc,
                2,
                &[(second, 2, 3)],
            ),
        )
        .unwrap();
        let gap_error = assemble_manifest_fragments(gap, &mut BTreeMap::new()).unwrap_err();
        assert_eq!(gap_error.stable_code(), "METADATA_INVALID");

        let first_fragment = manifest_record(
            manifest_id,
            5,
            WireChunkingStrategy::FastCdc,
            0,
            &[(first, 0, 2)],
        );
        let mut duplicate = BTreeMap::new();
        collect_manifest_fragment(&mut duplicate, &first_fragment).unwrap();
        let duplicate_error =
            collect_manifest_fragment(&mut duplicate, &first_fragment).unwrap_err();
        assert_eq!(duplicate_error.stable_code(), "METADATA_INVALID");
    }

    #[test]
    fn manifest_fragments_reject_inconsistent_manifest_metadata() {
        let (manifest, first, second) = two_chunk_manifest();
        let manifest_id = manifest.canonical_id().unwrap();
        let first_fragment = manifest_record(
            manifest_id,
            5,
            WireChunkingStrategy::FastCdc,
            0,
            &[(first, 0, 2)],
        );

        for inconsistent in [
            manifest_record(
                manifest_id,
                6,
                WireChunkingStrategy::FastCdc,
                1,
                &[(second, 2, 3)],
            ),
            manifest_record(
                manifest_id,
                5,
                WireChunkingStrategy::WholeFile,
                1,
                &[(second, 2, 3)],
            ),
        ] {
            let mut fragments = BTreeMap::new();
            collect_manifest_fragment(&mut fragments, &first_fragment).unwrap();
            let error = collect_manifest_fragment(&mut fragments, &inconsistent).unwrap_err();
            assert_eq!(error.stable_code(), "METADATA_INVALID");
        }
    }

    #[test]
    fn manifest_fragments_reject_a_tampered_canonical_id() {
        let (_, first, second) = two_chunk_manifest();
        let tampered_id = ManifestId::from_bytes([0x77; 32]);
        let mut fragments = BTreeMap::new();
        collect_manifest_fragment(
            &mut fragments,
            &manifest_record(
                tampered_id,
                5,
                WireChunkingStrategy::FastCdc,
                0,
                &[(first, 0, 2)],
            ),
        )
        .unwrap();
        collect_manifest_fragment(
            &mut fragments,
            &manifest_record(
                tampered_id,
                5,
                WireChunkingStrategy::FastCdc,
                1,
                &[(second, 2, 3)],
            ),
        )
        .unwrap();

        let error = assemble_manifest_fragments(fragments, &mut BTreeMap::new()).unwrap_err();
        assert_eq!(error.stable_code(), "METADATA_INVALID");
    }

    #[test]
    fn index_delta_order_accepts_both_file_directory_transitions() {
        validate_index_delta_order(&[delete("a"), upsert("a/b")])
            .expect("file-to-directory transition is a valid mutation sequence");
        validate_index_delta_order(&[upsert("a"), delete("a/b")])
            .expect("directory-to-file transition is a valid mutation sequence");
    }

    #[test]
    fn index_delta_order_still_rejects_portable_duplicates() {
        let error = validate_index_delta_order(&[delete("A"), upsert("a")])
            .expect_err("portable duplicate mutation paths must be rejected");
        assert_eq!(error.stable_code(), "METADATA_INVALID");
    }
}
