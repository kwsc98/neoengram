use std::{fmt, str::FromStr, sync::Arc};

use std::collections::BTreeMap;

use crate::{
    canonical_commit_id_with_layout, AdvanceWorkspaceCommitRequest, AuthorityStore, CentralError,
    CentralErrorCode, CentralResult, Clock, CommitRecord, ControlCatalogRepository, IndexKey,
    IndexPublisher, JobKey, JobRepository, PreCommitCommitRequest, PreCommitId, PreCommitKey,
    PreCommitRecord, PreCommitRepository, PreCommitState, WorkspaceRecord, WorkspaceState,
};
use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::{
    ChunkingStrategy, CommitId, ContentDigest, FileRecord, IndexVersion, LogicalPath, ObjectId,
};
use neoengram_domain::protocol::{
    ArtifactId, CommitDataLayout, CommitObject, CommitObjectSet, IndexRevision, JobState,
    ObjectEncoding, PlacementGeneration, ProjectId, RequestId, TenantId, WireIndexVersion,
    WorkspaceId,
};
use neoengram_runtime::engine::{
    build_commit_graph, BuildCommitGraphRequest, EngineError, EngineResult, IndexSnapshotReader,
    NoopProgressSink, Page, PageCursor, PageRequest,
};
use tokio::sync::Mutex;

use crate::{
    dto::{CommitWorkspaceRequest, IndexVersionBody},
    error::{application_error, invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy},
};

#[derive(Debug, Clone)]
pub struct WorkspaceCommitResult {
    pub commit: CommitRecord,
    pub workspace: WorkspaceRecord,
    pub consumed_precommit: PreCommitRecord,
    pub replayed: bool,
}

/// Single-instance publication boundary from a frozen Pre-commit candidate to Artifact authority.
pub struct WorkspaceCommitService {
    catalog: Arc<dyn ControlCatalogRepository>,
    precommits: Arc<dyn PreCommitRepository>,
    jobs: Arc<dyn JobRepository>,
    indexes: Arc<dyn IndexPublisher>,
    placement: Option<Arc<dyn crate::PlacementRepository>>,
    policy: Arc<StaticRbacPolicy>,
    clock: Arc<dyn Clock>,
    publication_lock: Mutex<()>,
}

impl WorkspaceCommitService {
    pub fn from_authority(
        authority: &AuthorityStore,
        policy: Arc<StaticRbacPolicy>,
        clock: Arc<dyn Clock>,
    ) -> CentralResult<Self> {
        let catalog = authority.control_catalog().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no control catalog composition",
            )
        })?;
        let precommits = authority.precommits().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no Pre-commit repository composition",
            )
        })?;
        Ok(Self {
            catalog,
            precommits,
            jobs: authority.jobs(),
            indexes: authority.publisher(),
            placement: authority.placement(),
            policy,
            clock,
            publication_lock: Mutex::new(()),
        })
    }

    pub async fn commit_workspace(
        &self,
        identity: &AuthenticatedIdentity,
        request: CommitWorkspaceRequest,
    ) -> Result<WorkspaceCommitResult, Error> {
        let tenant_id = parse_id("tenant_id", request.tenant_id, TenantId::new)?;
        if !self.policy.is_allowed(
            identity.principal(),
            Permission::WorkspaceCreate,
            &tenant_id,
        ) {
            return Err(resource_not_found("workspace"));
        }
        let project_id = parse_id("project_id", request.project_id, ProjectId::new)?;
        let artifact_id = parse_id("artifact_id", request.artifact_id, ArtifactId::new)?;
        let workspace_id = parse_id("workspace_id", request.workspace_id, WorkspaceId::new)?;
        let precommit_id = parse_id("precommit_id", request.precommit_id, PreCommitId::new)?;
        let commit_request_id = parse_id(
            "commit_request_id",
            request.commit_request_id,
            RequestId::new,
        )?;
        let expected_candidate = parse_index_version(
            "expected_candidate_index_version",
            request.expected_candidate_index_version,
        )?;
        let data_layout = match request.data_layout {
            crate::dto::DataLayout::FastCdc => CommitDataLayout::FastCdc,
            crate::dto::DataLayout::WholeFile => CommitDataLayout::WholeFile,
        };
        validate_commit_text(
            &request.message,
            request.description.as_deref(),
            &request.tag_names,
        )?;

        // All Commit mutations share this boundary in the supported single-Server profile. It
        // closes the race between the authority transaction and the control-catalog transaction;
        // a process crash remains recoverable by replaying the stable commit_request_id.
        let _publication = self.publication_lock.lock().await;
        // Keep the Artifact scope check, but do not use its mutable Head as a parent fence.
        let artifact = self
            .catalog
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        if !artifact.lifecycle.is_active() {
            return Err(commit_conflict(
                "resource_not_active",
                "RESOURCE_NOT_ACTIVE",
                "the Artifact is not active and cannot accept commits",
            ));
        }
        let workspace = self
            .catalog
            .get_workspace(&tenant_id, &project_id, &artifact_id, &workspace_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("workspace"))?;
        if !workspace.lifecycle.is_active() {
            return Err(commit_conflict(
                "resource_not_active",
                "RESOURCE_NOT_ACTIVE",
                "the Workspace is not active and cannot be committed",
            ));
        }
        if workspace.state != WorkspaceState::Ready {
            return Err(commit_conflict(
                "workspace_not_ready",
                "WORKSPACE_NOT_READY",
                "only a Ready Workspace can be committed",
            ));
        }

        let key = PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
        let mut precommit = self
            .precommits
            .get(&key)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        if precommit.project_id != project_id
            || precommit.artifact_id != artifact_id
            || precommit.workspace_id != workspace_id
        {
            return Err(resource_not_found("precommit"));
        }
        if precommit.state == PreCommitState::Running {
            let job = self
                .jobs
                .get(&JobKey::new(tenant_id.clone(), precommit.job_id.clone()))
                .await
                .map_err(map_central_error)?;
            if let Some(job) = job {
                let published_index = if job.state == JobState::Succeeded {
                    Some(
                        self.indexes
                            .published_index(&job.index_key())
                            .await
                            .map_err(map_central_error)?,
                    )
                } else {
                    None
                };
                if let Some(synchronized) = self
                    .precommits
                    .sync_job(job, published_index, self.clock.now())
                    .await
                    .map_err(map_central_error)?
                {
                    precommit = synchronized;
                }
            }
        }
        let candidate = precommit.candidate_index_version.as_ref().ok_or_else(|| {
            commit_conflict(
                "precommit_not_ready",
                "PRECOMMIT_NOT_READY",
                "Commit requires a ready Pre-commit candidate",
            )
        })?;
        if !same_index_version(candidate, &expected_candidate) {
            return Err(commit_conflict(
                "candidate_index_version_mismatch",
                "CANDIDATE_INDEX_VERSION_MISMATCH",
                "the Pre-commit candidate IndexVersion changed",
            ));
        }
        if precommit.data_layout != data_layout {
            return Err(commit_conflict(
                "data_layout_mismatch",
                "DATA_LAYOUT_MISMATCH",
                "Commit data_layout must match the layout frozen by Pre-commit",
            ));
        }
        if data_layout == CommitDataLayout::WholeFile {
            let volume = self
                .catalog
                .get_storage_volume(&tenant_id, &workspace.storage_volume_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| resource_not_found("storage volume"))?;
            if precommit.candidate_records.as_ref().is_some_and(|records| {
                records
                    .iter()
                    .any(|record| record.total_size > volume.max_whole_file_bytes.get())
            }) {
                return Err(commit_conflict(
                    "whole_file_size_limit_exceeded",
                    "WHOLE_FILE_SIZE_LIMIT_EXCEEDED",
                    "the Commit contains a file that exceeds the StorageVolume WholeFile size policy",
                ));
            }
        }

        let authority_outcome = if precommit.state == PreCommitState::Committed {
            let commit_id = precommit
                .committed_commit_id
                .ok_or_else(|| internal_error("a committed Pre-commit lost its Commit identity"))?;
            let stored = self
                .precommits
                .get_commit(&tenant_id, &project_id, &artifact_id, commit_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| internal_error("the committed Commit record is missing"))?;
            if stored.commit_request_id != commit_request_id
                || stored.source_workspace_id != workspace_id
                || stored.source_precommit_id != precommit_id
                || !same_index_version(&stored.index_version, &expected_candidate)
                || stored.message != request.message
                || stored.description != request.description
                || stored.tag_names != request.tag_names
                || stored.data_layout != data_layout
            {
                return Err(commit_conflict(
                    "commit_request_id_reused",
                    "COMMIT_REQUEST_ID_REUSED",
                    "commit_request_id is already bound to another Commit payload",
                ));
            }
            self.precommits
                .commit(PreCommitCommitRequest {
                    key: key.clone(),
                    expected_candidate_index_version: expected_candidate.clone(),
                    data_layout,
                    commit: stored,
                })
                .await
                .map_err(map_central_error)?
        } else {
            let frozen_head = precommit.frozen_head_commit_id.map(Into::into);
            if workspace.head_commit_id != frozen_head {
                return Err(commit_conflict(
                    // Preserve the published wire code while narrowing the fence to the
                    // branch-local Workspace Head.
                    "artifact_head_mismatch",
                    "ARTIFACT_HEAD_MISMATCH",
                    "Workspace Head changed after Pre-commit",
                ));
            }
            let index_key = IndexKey {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                workspace_id: workspace_id.clone(),
            };
            let published = self
                .indexes
                .published_index(&index_key)
                .await
                .map_err(map_central_error)?;
            if !same_index_version(&published.version, &expected_candidate) {
                return Err(commit_conflict(
                    "candidate_index_version_mismatch",
                    "CANDIDATE_INDEX_VERSION_MISMATCH",
                    "the published Workspace Index no longer matches this Pre-commit",
                ));
            }
            let created_at_unix_ms = self.clock.now();
            let candidate_records = precommit.candidate_records.clone().ok_or_else(|| {
                internal_error("a ready Pre-commit lost its frozen Index snapshot")
            })?;
            validate_candidate_layout(
                self.indexes.as_ref(),
                &tenant_id,
                &artifact_id,
                &candidate_records,
                data_layout,
            )
            .await?;
            let reader = PublishedIndexReader::new(crate::PublishedIndex {
                version: expected_candidate.clone(),
                records: candidate_records,
            });
            let graph = build_commit_graph(
                &BuildCommitGraphRequest {
                    expected_index_version: reader.version,
                    parent: precommit.frozen_head_commit_id,
                    message: request.message.clone(),
                    created_at_unix_ms: created_at_unix_ms.get(),
                },
                &reader,
                &NoopProgressSink,
            )
            .map_err(commit_graph_error)?;
            let object_set_digest = self
                .build_object_set_for_records(&tenant_id, &artifact_id, &reader.records)
                .await?
                .object_set_digest;
            self.precommits
                .commit(PreCommitCommitRequest {
                    key: key.clone(),
                    expected_candidate_index_version: expected_candidate.clone(),
                    data_layout,
                    commit: CommitRecord {
                        tenant_id: tenant_id.clone(),
                        project_id: project_id.clone(),
                        artifact_id: artifact_id.clone(),
                        source_workspace_id: workspace_id.clone(),
                        source_precommit_id: precommit_id,
                        commit_request_id,
                        commit_id: canonical_commit_id_with_layout(graph.commit_id, data_layout),
                        object_set_digest,
                        root_directory_id: graph.commit.root_directory_id,
                        parent_commit_id: graph.commit.parent,
                        index_version: expected_candidate,
                        data_layout,
                        records: reader.records.clone(),
                        message: request.message,
                        description: request.description,
                        tag_names: request.tag_names,
                        created_at_unix_ms,
                    },
                })
                .await
                .map_err(map_central_error)?
        };

        // A Commit is logical metadata only. The initial physical evidence is recorded as
        // namespace-scoped, verified object placements on the Workspace's Volume. This operation
        // is idempotent so a retry after a process interruption converges without changing the
        // immutable Commit record.
        self.publish_initial_placement(&authority_outcome.commit, &workspace)
            .await?;

        if authority_outcome
            .consumed_precommit
            .head_published_at_unix_ms
            .is_some()
        {
            return Ok(WorkspaceCommitResult {
                commit: authority_outcome.commit,
                workspace,
                consumed_precommit: authority_outcome.consumed_precommit,
                replayed: true,
            });
        }
        let (published_workspace, head_replayed) = publish_committed_workspace_head(
            self.catalog.as_ref(),
            self.precommits.as_ref(),
            &authority_outcome.commit,
        )
        .await
        .map_err(map_central_error)?;
        let consumed_precommit = self
            .precommits
            .acknowledge_head_publication(
                &key,
                authority_outcome.commit.commit_id,
                self.clock.now(),
            )
            .await
            .map_err(map_central_error)?;
        Ok(WorkspaceCommitResult {
            commit: authority_outcome.commit,
            workspace: published_workspace,
            consumed_precommit,
            replayed: authority_outcome.replayed || head_replayed,
        })
    }

    async fn publish_initial_placement(
        &self,
        commit: &CommitRecord,
        workspace: &WorkspaceRecord,
    ) -> Result<(), Error> {
        let Some(placement) = &self.placement else {
            // Standalone/unit compositions may intentionally omit the placement authority. The
            // durable Commit remains valid; production composition always installs this port.
            return Ok(());
        };

        let object_set = self.build_commit_object_set(commit).await?;
        if object_set.object_set.object_set_digest != commit.object_set_digest {
            return Err(commit_integrity_error(
                "Commit ObjectSet digest differs from its immutable Commit identity",
            ));
        }
        let storage_volume_id = workspace.storage_volume_id.clone();
        let _volume = self
            .catalog
            .get_storage_volume(&commit.tenant_id, &storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        let namespace =
            neoengram_domain::protocol::ObjectNamespaceId::from_artifact(&commit.artifact_id);
        placement
            .insert_commit_object_set(object_set.clone())
            .await
            .map_err(map_central_error)?;

        // Preserve the existing target generation on replay. A generation change is an explicit
        // Volume-owner operation; a Commit retry must never create a second generation merely
        // because the process was interrupted between two object receipts.
        let mut generation: Option<PlacementGeneration> = None;
        for object in &object_set.object_set.objects {
            let existing = placement
                .object_placements_v2(&commit.tenant_id, &namespace, &object.object_id)
                .await
                .map_err(map_central_error)?;
            if let Some(existing_generation) = existing.into_iter().find_map(|entry| {
                (entry.storage_volume_id.as_ref() == Some(&storage_volume_id)
                    && entry.state
                        == neoengram_domain::protocol::materialization::ObjectPlacementState::Verified)
                    .then_some(entry.placement_generation)
            }) {
                generation = Some(generation.map_or(existing_generation, |current| {
                    current.max(existing_generation)
                }));
            }
        }
        let generation = generation.unwrap_or_else(|| PlacementGeneration::new(1));

        for object in &object_set.object_set.objects {
            let placement_digest = blake3::hash(
                format!(
                    "v2-placement\0{}\0{}\0{}\0{}",
                    commit.tenant_id, commit.artifact_id, storage_volume_id, object.object_id
                )
                .as_bytes(),
            );
            let placement_id = neoengram_domain::protocol::PlacementId::new(format!(
                "placement-v2-{}",
                &placement_digest.to_hex()[..32]
            ))
            .map_err(|_| internal_error("invalid v2 placement identity"))?;
            placement
                .insert_object_placement_v2(
                    neoengram_domain::protocol::materialization::ObjectPlacement {
                        placement_id,
                        tenant_id: commit.tenant_id.clone(),
                        object_namespace_id: namespace.clone(),
                        object_id: object.object_id,
                        size: object.size,
                        encoding: object.encoding,
                        verified_digest: object.object_id.digest(),
                        storage_volume_id: Some(storage_volume_id.clone()),
                        archive_id: None,
                        placement_generation: generation,
                        state: neoengram_domain::protocol::materialization::ObjectPlacementState::Verified,
                        failure_domain: format!("volume:{}", storage_volume_id),
                    },
                )
                .await
                .map_err(map_central_error)?;
        }
        let mut placements = Vec::with_capacity(object_set.object_set.objects.len());
        for object in &object_set.object_set.objects {
            let placement = placement
                .object_placements_v2(&commit.tenant_id, &namespace, &object.object_id)
                .await
                .map_err(map_central_error)?
                .into_iter()
                .find(|entry| {
                    entry.storage_volume_id.as_ref() == Some(&storage_volume_id)
                        && entry.placement_generation == generation
                })
                .ok_or_else(|| {
                    commit_integrity_error(format!(
                        "initial object placement for {} was not persisted",
                        object.object_id
                    ))
                })?;
            placements.push(placement);
        }
        let coverage =
            neoengram_domain::protocol::materialization::VolumeCommitCoverage::from_placements(
                commit.tenant_id.clone(),
                namespace,
                commit.commit_id,
                storage_volume_id,
                generation,
                &object_set.object_set,
                &placements,
            )
            .map_err(|error| commit_integrity_error(format!("initial coverage: {error}")))?;
        placement
            .upsert_volume_commit_coverage(coverage)
            .await
            .map_err(map_central_error)?;
        Ok(())
    }

    async fn build_commit_object_set(
        &self,
        commit: &CommitRecord,
    ) -> Result<CommitObjectSet, Error> {
        let object_set = self
            .build_object_set_for_records(&commit.tenant_id, &commit.artifact_id, &commit.records)
            .await?;
        Ok(CommitObjectSet {
            tenant_id: commit.tenant_id.clone(),
            commit_id: commit.commit_id,
            object_set,
        })
    }

    async fn build_object_set_for_records(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        records: &[FileRecord],
    ) -> Result<neoengram_domain::protocol::ObjectSet, Error> {
        let mut objects = BTreeMap::<ObjectId, u64>::new();
        for record in records {
            let manifest = self
                .indexes
                .manifest(tenant_id, artifact_id, record.manifest_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| {
                    commit_integrity_error(format!(
                        "Commit file {} references a missing immutable Manifest",
                        record.path
                    ))
                })?;
            manifest.validate().map_err(|error| {
                commit_integrity_error(format!(
                    "Commit file {} references an invalid Manifest: {error}",
                    record.path
                ))
            })?;
            for chunk in manifest.chunks {
                if let Some(existing) = objects.insert(chunk.object_id, chunk.size) {
                    if existing != chunk.size {
                        return Err(commit_integrity_error(format!(
                            "Object {} has conflicting sizes in Commit manifests",
                            chunk.object_id
                        )));
                    }
                }
            }
        }
        let object_list = objects
            .into_iter()
            .enumerate()
            .map(|(ordinal, (object_id, size))| {
                CommitObject::new(object_id, size, ObjectEncoding::Raw, ordinal as u64)
            })
            .collect();
        let object_set =
            neoengram_domain::protocol::ObjectSet::new(object_list).map_err(|error| {
                commit_integrity_error(format!("invalid Commit ObjectSet: {error}"))
            })?;
        Ok(object_set)
    }
}

async fn validate_candidate_layout(
    indexes: &dyn IndexPublisher,
    tenant_id: &TenantId,
    artifact_id: &ArtifactId,
    records: &[FileRecord],
    data_layout: CommitDataLayout,
) -> Result<(), Error> {
    let expected_chunking = match data_layout {
        CommitDataLayout::FastCdc => ChunkingStrategy::FastCdc,
        CommitDataLayout::WholeFile => ChunkingStrategy::WholeFile,
    };
    for record in records {
        let manifest = indexes
            .manifest(tenant_id, artifact_id, record.manifest_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| {
                commit_integrity_error(format!(
                    "Commit file {} references a missing immutable Manifest",
                    record.path
                ))
            })?;
        manifest.validate().map_err(|error| {
            commit_integrity_error(format!(
                "Commit file {} references an invalid Manifest: {error}",
                record.path
            ))
        })?;
        let manifest_id = manifest.canonical_id().map_err(|error| {
            commit_integrity_error(format!(
                "Commit file {} Manifest identity is invalid: {error}",
                record.path
            ))
        })?;
        let chunk_count = manifest.chunk_count().map_err(|error| {
            commit_integrity_error(format!(
                "Commit file {} Manifest chunk count is invalid: {error}",
                record.path
            ))
        })?;
        if manifest_id != record.manifest_id
            || manifest.total_size != record.total_size
            || chunk_count != record.chunk_count
        {
            return Err(commit_integrity_error(format!(
                "Commit file {} differs from its immutable Manifest metadata",
                record.path
            )));
        }
        if manifest.chunking != expected_chunking {
            return Err(commit_conflict(
                "mixed_commit_layout",
                "MIXED_COMMIT_LAYOUT",
                "every Manifest in a Commit must use the Commit data_layout",
            ));
        }
    }
    Ok(())
}

pub(super) async fn publish_committed_workspace_head(
    catalog: &dyn ControlCatalogRepository,
    precommits: &dyn PreCommitRepository,
    commit: &CommitRecord,
) -> CentralResult<(WorkspaceRecord, bool)> {
    let current = load_commit_workspace(catalog, commit).await?;
    if workspace_contains_commit(precommits, commit, &current).await? {
        return Ok((current, true));
    }

    match catalog
        .advance_workspace_commit(AdvanceWorkspaceCommitRequest {
            tenant_id: commit.tenant_id.clone(),
            project_id: commit.project_id.clone(),
            artifact_id: commit.artifact_id.clone(),
            workspace_id: commit.source_workspace_id.clone(),
            expected_head_commit_id: commit.parent_commit_id.map(Into::into),
            commit_id: commit.commit_id.into(),
            updated_at_unix_ms: commit.created_at_unix_ms,
        })
        .await
    {
        Ok(outcome) => Ok((outcome.workspace, outcome.replayed)),
        Err(error) if error.code() == CentralErrorCode::ArtifactHeadMismatch => {
            // Another publisher may have advanced this same Workspace between the observation
            // and CAS. A descendant proves this Commit was already published; never move Head
            // backwards merely to complete its recovery acknowledgement.
            let current = load_commit_workspace(catalog, commit).await?;
            if workspace_contains_commit(precommits, commit, &current).await? {
                Ok((current, true))
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

async fn load_commit_workspace(
    catalog: &dyn ControlCatalogRepository,
    commit: &CommitRecord,
) -> CentralResult<WorkspaceRecord> {
    catalog
        .get_workspace(
            &commit.tenant_id,
            &commit.project_id,
            &commit.artifact_id,
            &commit.source_workspace_id,
        )
        .await?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ArtifactNotFound,
                "committed Pre-commit source Workspace no longer exists",
            )
        })
}

async fn workspace_contains_commit(
    precommits: &dyn PreCommitRepository,
    commit: &CommitRecord,
    workspace: &WorkspaceRecord,
) -> CentralResult<bool> {
    let Some(head) = workspace.head_commit_id else {
        return Ok(false);
    };
    is_commit_ancestor(
        precommits,
        &commit.tenant_id,
        &commit.project_id,
        &commit.artifact_id,
        commit.commit_id,
        CommitId::from_digest(head),
    )
    .await
}

async fn is_commit_ancestor(
    precommits: &dyn PreCommitRepository,
    tenant_id: &TenantId,
    project_id: &ProjectId,
    artifact_id: &ArtifactId,
    ancestor: CommitId,
    mut descendant: CommitId,
) -> CentralResult<bool> {
    let mut visited = std::collections::BTreeSet::new();
    loop {
        if descendant == ancestor {
            return Ok(true);
        }
        if !visited.insert(descendant) {
            return Err(CentralError::new(
                CentralErrorCode::Internal,
                "Commit parent chain contains a cycle",
            ));
        }
        let commit = precommits
            .get_commit(tenant_id, project_id, artifact_id, descendant)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "published Workspace Head lost its immutable Commit row",
                )
            })?;
        let Some(parent) = commit.parent_commit_id else {
            return Ok(false);
        };
        descendant = parent;
    }
}

#[derive(Debug)]
struct PublishedIndexReader {
    version: IndexVersion,
    records: Vec<FileRecord>,
}

impl PublishedIndexReader {
    fn new(index: crate::PublishedIndex) -> Self {
        let mut records = index.records;
        records.sort_by(|left, right| left.path.cmp(&right.path));
        Self {
            version: index.version.into(),
            records,
        }
    }
}

impl IndexSnapshotReader for PublishedIndexReader {
    fn version(&self) -> &IndexVersion {
        &self.version
    }

    fn get_file(&self, path: &LogicalPath) -> EngineResult<Option<FileRecord>> {
        Ok(self
            .records
            .binary_search_by(|record| record.path.cmp(path))
            .ok()
            .map(|index| self.records[index].clone()))
    }

    fn scan_files(
        &self,
        prefix: Option<&LogicalPath>,
        request: &PageRequest,
    ) -> EngineResult<Page<FileRecord>> {
        request.validate()?;
        let records = self
            .records
            .iter()
            .filter(|record| {
                prefix.is_none_or(|prefix| {
                    record.path == *prefix || prefix.is_ancestor_of(&record.path)
                })
            })
            .filter(|record| {
                request
                    .after
                    .as_ref()
                    .is_none_or(|after| record.path.as_str() > after.as_str())
            })
            .take(request.limit as usize + 1)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = records.len() > request.limit as usize;
        let mut items = records;
        items.truncate(request.limit as usize);
        let next = if has_more {
            items
                .last()
                .map(|record| PageCursor::new(record.path.as_str()))
                .transpose()?
        } else {
            None
        };
        Ok(Page { items, next })
    }
}

fn parse_id<T, E>(
    field: &'static str,
    value: String,
    parser: impl FnOnce(String) -> Result<T, E>,
) -> Result<T, Error>
where
    E: fmt::Display,
{
    parser(value).map_err(|error| invalid_request(format!("{field}: {error}")))
}

fn parse_index_version(
    field: &'static str,
    value: IndexVersionBody,
) -> Result<WireIndexVersion, Error> {
    let revision = value.revision.parse::<u64>().map_err(|_| {
        invalid_request(format!(
            "{field}.revision must be a canonical unsigned integer"
        ))
    })?;
    if revision.to_string() != value.revision {
        return Err(invalid_request(format!(
            "{field}.revision must be a canonical unsigned integer"
        )));
    }
    let digest = ContentDigest::from_str(&value.digest)
        .map_err(|_| invalid_request(format!("{field}.digest must be a BLAKE3 hex digest")))?;
    Ok(WireIndexVersion {
        revision: IndexRevision::new(revision),
        digest,
        extensions: Default::default(),
    })
}

fn validate_commit_text(
    message: &str,
    description: Option<&str>,
    tag_names: &[String],
) -> Result<(), Error> {
    if message.trim().is_empty() || message.chars().count() > 4_096 {
        return Err(invalid_request(
            "message must contain between 1 and 4096 characters",
        ));
    }
    if description.is_some_and(|value| value.chars().count() > 2_048) {
        return Err(invalid_request(
            "description must not exceed 2048 characters",
        ));
    }
    if tag_names.len() > 20
        || tag_names.iter().any(|tag| {
            tag.is_empty()
                || tag.len() > 128
                || tag.starts_with("refs/")
                || !tag.bytes().enumerate().all(|(index, byte)| {
                    byte.is_ascii_alphanumeric()
                        || (index > 0 && matches!(byte, b'.' | b'_' | b'/' | b'-'))
                })
        })
    {
        return Err(invalid_request(
            "tag_names must be non-empty, unique, and contain at most 20 entries",
        ));
    }
    let unique = tag_names.iter().collect::<std::collections::BTreeSet<_>>();
    if unique.len() != tag_names.len() {
        return Err(invalid_request("tag_names must be unique"));
    }
    Ok(())
}

fn same_index_version(left: &WireIndexVersion, right: &WireIndexVersion) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn resource_not_found(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::NotFound,
        "resource_not_found",
        "RESOURCE_NOT_FOUND",
        format!("{resource} not found"),
        false,
    )
}

fn commit_conflict(code: &'static str, neo_code: &'static str, message: &'static str) -> Error {
    application_error(ErrorCategory::Conflict, code, neo_code, message, false)
}

fn internal_error(message: &'static str) -> Error {
    application_error(
        ErrorCategory::Internal,
        "commit_authority_invalid",
        "COMMIT_AUTHORITY_INVALID",
        message,
        false,
    )
}

fn commit_integrity_error(message: impl Into<String>) -> Error {
    application_error(
        ErrorCategory::Internal,
        "commit_manifest_invalid",
        "COMMIT_MANIFEST_INVALID",
        message,
        false,
    )
}

fn commit_graph_error(error: EngineError) -> Error {
    application_error(
        ErrorCategory::Internal,
        "commit_graph_invalid",
        "COMMIT_GRAPH_INVALID",
        format!("the authoritative Index could not form a canonical Commit: {error}"),
        false,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        ArtifactInitialization, ArtifactRecord, CatalogPvcReference, InMemoryControlCatalog,
        PreCommitCancelRequest, PreCommitCommitOutcome, PreCommitCommitRequest, PreCommitId,
        PreCommitMutationOutcome, PreCommitRecord, PreCommitRestartRequest, PreCommitStartRequest,
        PublishedIndex, StorageAccessMode, StorageBackendType, StorageVolumeRecord,
        StorageVolumeState, TenantRecord,
    };
    use async_trait::async_trait;
    use neoengram_domain::core::DirectoryId;
    use neoengram_domain::protocol::{
        EdgeClusterId, Extensions, RequestId, ResourceLifecycle, StorageVolumeId, UnixMillis,
    };

    use super::*;

    #[tokio::test]
    async fn recovery_accepts_descendant_workspace_head_without_rolling_it_back() {
        let target = CommitId::from_bytes([1; 32]);
        let descendant = CommitId::from_bytes([2; 32]);
        let commits = CommitLookupRepository::new([
            commit_record(target, None),
            commit_record(descendant, Some(target)),
        ]);
        let catalog = catalog_with_head(descendant).await;

        let (workspace, replayed) =
            publish_committed_workspace_head(&catalog, &commits, &commit_record(target, None))
                .await
                .unwrap();

        assert!(replayed);
        assert_eq!(workspace.head_commit_id, Some(descendant.into()));
        assert_catalog_heads(&catalog, descendant).await;
    }

    #[tokio::test]
    async fn recovery_rejects_an_unrelated_workspace_head() {
        let target = CommitId::from_bytes([1; 32]);
        let unrelated = CommitId::from_bytes([3; 32]);
        let commits = CommitLookupRepository::new([
            commit_record(target, None),
            commit_record(unrelated, None),
        ]);
        let catalog = catalog_with_head(unrelated).await;

        let error =
            publish_committed_workspace_head(&catalog, &commits, &commit_record(target, None))
                .await
                .unwrap_err();

        assert_eq!(error.code(), CentralErrorCode::ArtifactHeadMismatch);
        assert_catalog_heads(&catalog, unrelated).await;
    }

    #[tokio::test]
    async fn recovery_reports_a_missing_parent_as_an_internal_consistency_error() {
        let target = CommitId::from_bytes([1; 32]);
        let descendant = CommitId::from_bytes([4; 32]);
        let missing_parent = CommitId::from_bytes([5; 32]);
        let commits = CommitLookupRepository::new([
            commit_record(target, None),
            commit_record(descendant, Some(missing_parent)),
        ]);
        let catalog = catalog_with_head(descendant).await;

        let error =
            publish_committed_workspace_head(&catalog, &commits, &commit_record(target, None))
                .await
                .unwrap_err();

        assert_eq!(error.code(), CentralErrorCode::Internal);
        assert_eq!(
            error.message(),
            "published Workspace Head lost its immutable Commit row"
        );
        assert_catalog_heads(&catalog, descendant).await;
    }

    #[tokio::test]
    async fn recovery_reports_a_parent_cycle_as_an_internal_consistency_error() {
        let target = CommitId::from_bytes([1; 32]);
        let cycle_left = CommitId::from_bytes([6; 32]);
        let cycle_right = CommitId::from_bytes([7; 32]);
        let commits = CommitLookupRepository::new([
            commit_record(target, None),
            commit_record(cycle_left, Some(cycle_right)),
            commit_record(cycle_right, Some(cycle_left)),
        ]);
        let catalog = catalog_with_head(cycle_left).await;

        let error =
            publish_committed_workspace_head(&catalog, &commits, &commit_record(target, None))
                .await
                .unwrap_err();

        assert_eq!(error.code(), CentralErrorCode::Internal);
        assert_eq!(error.message(), "Commit parent chain contains a cycle");
        assert_catalog_heads(&catalog, cycle_left).await;
    }

    async fn catalog_with_head(head: CommitId) -> InMemoryControlCatalog {
        let catalog = InMemoryControlCatalog::default();
        let tenant_id = tenant_id();
        let project_id = project_id();
        let artifact_id = artifact_id();
        catalog
            .insert_tenant(TenantRecord {
                tenant_id: tenant_id.clone(),
                display_name: "Tenant".to_owned(),
                description: None,
                resource_version: 1,
                created_at_unix_ms: UnixMillis::new(1),
                updated_at_unix_ms: UnixMillis::new(1),
            })
            .await
            .unwrap();
        catalog
            .insert_artifact(ArtifactRecord {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                display_name: "Artifact".to_owned(),
                description: None,
                initialization: ArtifactInitialization::Empty,
                head_commit_id: Some(head.into()),
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: UnixMillis::new(2),
                updated_at_unix_ms: UnixMillis::new(2),
            })
            .await
            .unwrap();
        catalog
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id: tenant_id.clone(),
                storage_volume_id: storage_volume_id(),
                display_name: "Volume".to_owned(),
                edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
                region: "local".to_owned(),
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteMany,
                allowed_delivery_modes: vec![
                    neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
                    neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
                ],
                hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
                max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
                copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
                pvc_reference: Some(CatalogPvcReference {
                    namespace: "default".to_owned(),
                    claim_name: "workspace".to_owned(),
                }),
                nfs_reference: None,
                state: StorageVolumeState::Ready,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                created_at_unix_ms: UnixMillis::new(3),
                updated_at_unix_ms: UnixMillis::new(3),
            })
            .await
            .unwrap();
        catalog
            .insert_workspace(WorkspaceRecord {
                tenant_id,
                project_id,
                artifact_id,
                workspace_id: workspace_id(),
                storage_volume_id: storage_volume_id(),
                region: "local".to_owned(),
                display_name: "Workspace".to_owned(),
                base_commit_id: Some(head.into()),
                head_commit_id: Some(head.into()),
                state: WorkspaceState::Ready,
                resource_version: 1,
                lifecycle: ResourceLifecycle::active(),
                relative_root: "workspaces/project-a/artifact-a/workspace-a".to_owned(),
                created_at_unix_ms: UnixMillis::new(4),
                updated_at_unix_ms: UnixMillis::new(4),
            })
            .await
            .unwrap();
        catalog
    }

    async fn assert_catalog_heads(catalog: &InMemoryControlCatalog, expected: CommitId) {
        let artifact = catalog
            .get_artifact(&tenant_id(), &project_id(), &artifact_id())
            .await
            .unwrap()
            .unwrap();
        let workspace = catalog
            .get_workspace(&tenant_id(), &project_id(), &artifact_id(), &workspace_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(artifact.head_commit_id, Some(expected.into()));
        assert_eq!(workspace.head_commit_id, Some(expected.into()));
    }

    fn commit_record(commit_id: CommitId, parent_commit_id: Option<CommitId>) -> CommitRecord {
        CommitRecord {
            tenant_id: tenant_id(),
            project_id: project_id(),
            artifact_id: artifact_id(),
            source_workspace_id: workspace_id(),
            source_precommit_id: PreCommitId::new(format!("precommit-{commit_id}")).unwrap(),
            commit_request_id: RequestId::new(format!("request-{commit_id}")).unwrap(),
            commit_id,
            object_set_digest: ContentDigest::from_bytes([0; 32]),
            data_layout: neoengram_domain::protocol::CommitDataLayout::FastCdc,
            root_directory_id: DirectoryId::from_bytes([8; 32]),
            parent_commit_id,
            index_version: WireIndexVersion {
                revision: IndexRevision::new(1),
                digest: ContentDigest::from_bytes([9; 32]),
                extensions: Extensions::new(),
            },
            records: Vec::new(),
            message: "Commit".to_owned(),
            description: None,
            tag_names: Vec::new(),
            created_at_unix_ms: UnixMillis::new(10),
        }
    }

    fn tenant_id() -> TenantId {
        TenantId::new("tenant-a").unwrap()
    }

    fn project_id() -> ProjectId {
        ProjectId::new("project-a").unwrap()
    }

    fn artifact_id() -> ArtifactId {
        ArtifactId::new("artifact-a").unwrap()
    }

    fn workspace_id() -> WorkspaceId {
        WorkspaceId::new("workspace-a").unwrap()
    }

    fn storage_volume_id() -> StorageVolumeId {
        StorageVolumeId::new("volume-a").unwrap()
    }

    struct CommitLookupRepository {
        commits: BTreeMap<CommitId, CommitRecord>,
    }

    impl CommitLookupRepository {
        fn new(commits: impl IntoIterator<Item = CommitRecord>) -> Self {
            Self {
                commits: commits
                    .into_iter()
                    .map(|commit| (commit.commit_id, commit))
                    .collect(),
            }
        }

        fn unused<T>() -> CentralResult<T> {
            panic!("unexpected PreCommitRepository operation in recovery test")
        }
    }

    #[async_trait]
    impl PreCommitRepository for CommitLookupRepository {
        async fn start(
            &self,
            _request: PreCommitStartRequest,
        ) -> CentralResult<PreCommitMutationOutcome> {
            Self::unused()
        }

        async fn get(&self, _key: &PreCommitKey) -> CentralResult<Option<PreCommitRecord>> {
            Self::unused()
        }

        async fn get_active(
            &self,
            _tenant_id: &TenantId,
            _project_id: &ProjectId,
            _artifact_id: &ArtifactId,
            _workspace_id: &WorkspaceId,
        ) -> CentralResult<Option<PreCommitRecord>> {
            Self::unused()
        }

        async fn list_running(
            &self,
            _after: Option<&PreCommitKey>,
            _limit: usize,
        ) -> CentralResult<Vec<PreCommitRecord>> {
            Self::unused()
        }

        async fn list_unpublished_commits(
            &self,
            _after: Option<&PreCommitKey>,
            _limit: usize,
        ) -> CentralResult<Vec<PreCommitRecord>> {
            Self::unused()
        }

        async fn find_restart_result(
            &self,
            _tenant_id: &TenantId,
            _restart_request_id: &RequestId,
        ) -> CentralResult<Option<PreCommitRecord>> {
            Self::unused()
        }

        async fn restart(
            &self,
            _request: PreCommitRestartRequest,
        ) -> CentralResult<PreCommitMutationOutcome> {
            Self::unused()
        }

        async fn cancel(
            &self,
            _request: PreCommitCancelRequest,
        ) -> CentralResult<PreCommitMutationOutcome> {
            Self::unused()
        }

        async fn sync_job(
            &self,
            _job: crate::JobRecord,
            _published_index: Option<PublishedIndex>,
            _observed_at_unix_ms: UnixMillis,
        ) -> CentralResult<Option<PreCommitRecord>> {
            Self::unused()
        }

        async fn commit(
            &self,
            _request: PreCommitCommitRequest,
        ) -> CentralResult<PreCommitCommitOutcome> {
            Self::unused()
        }

        async fn get_commit(
            &self,
            tenant_id: &TenantId,
            project_id: &ProjectId,
            artifact_id: &ArtifactId,
            commit_id: CommitId,
        ) -> CentralResult<Option<CommitRecord>> {
            Ok(self
                .commits
                .get(&commit_id)
                .filter(|commit| {
                    &commit.tenant_id == tenant_id
                        && &commit.project_id == project_id
                        && &commit.artifact_id == artifact_id
                })
                .cloned())
        }

        async fn list_published_commits(
            &self,
            _tenant_id: &TenantId,
            _project_id: &ProjectId,
            _artifact_id: &ArtifactId,
        ) -> CentralResult<Vec<CommitRecord>> {
            Self::unused()
        }

        async fn acknowledge_head_publication(
            &self,
            _key: &PreCommitKey,
            _commit_id: CommitId,
            _published_at_unix_ms: UnixMillis,
        ) -> CentralResult<PreCommitRecord> {
            Self::unused()
        }
    }
}
