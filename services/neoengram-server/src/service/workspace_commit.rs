use std::{fmt, str::FromStr, sync::Arc};

use fusen_rs::{Error, ErrorCategory};
use neoengram_core::{ContentDigest, FileRecord, IndexVersion, LogicalPath};
use neoengram_engine::{
    build_commit_graph, BuildCommitGraphRequest, EngineError, EngineResult, IndexSnapshotReader,
    NoopProgressSink, Page, PageCursor, PageRequest,
};
use neoengram_protocol::{
    ArtifactId, IndexRevision, JobState, PlaygroundId, ProjectId, RequestId, TenantId,
    WireIndexVersion,
};
use neoengramd::{
    AdvancePlaygroundCommitRequest, AuthorityStore, CentralError, CentralErrorCode, CentralResult,
    Clock, CommitRecord, ControlCatalogRepository, IndexKey, IndexPublisher, JobKey, JobRepository,
    PlaygroundRecord, PlaygroundState, PreCommitCommitRequest, PreCommitId, PreCommitKey,
    PreCommitRecord, PreCommitRepository, PreCommitState,
};
use tokio::sync::Mutex;

use crate::{
    dto::{CommitPlaygroundRequest, IndexVersionBody},
    error::{application_error, invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy},
};

const RESERVED_COMMIT_FIELDS: &[&str] = &[
    "actor",
    "expected_head_commit_id",
    "principal",
    "request_digest",
    "source_head_commit_id",
];

#[derive(Debug, Clone)]
pub struct WorkspaceCommitResult {
    pub commit: CommitRecord,
    pub playground: PlaygroundRecord,
    pub consumed_precommit: PreCommitRecord,
    pub replayed: bool,
}

/// Single-instance publication boundary from a frozen Pre-commit candidate to Artifact authority.
pub struct WorkspaceCommitService {
    catalog: Arc<dyn ControlCatalogRepository>,
    precommits: Arc<dyn PreCommitRepository>,
    jobs: Arc<dyn JobRepository>,
    indexes: Arc<dyn IndexPublisher>,
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
            policy,
            clock,
            publication_lock: Mutex::new(()),
        })
    }

    pub async fn commit_playground(
        &self,
        identity: &AuthenticatedIdentity,
        request: CommitPlaygroundRequest,
    ) -> Result<WorkspaceCommitResult, Error> {
        if let Some(field) = request.extensions.first_reserved(RESERVED_COMMIT_FIELDS) {
            return Err(invalid_request(format!(
                "request body must not contain server-owned field {field:?}"
            )));
        }
        let tenant_id = parse_id("tenant_id", request.tenant_id, TenantId::new)?;
        if !self.policy.is_allowed(
            identity.principal(),
            Permission::PlaygroundCreate,
            &tenant_id,
        ) {
            return Err(resource_not_found("playground"));
        }
        let project_id = parse_id("project_id", request.project_id, ProjectId::new)?;
        let artifact_id = parse_id("artifact_id", request.artifact_id, ArtifactId::new)?;
        let playground_id = parse_id("playground_id", request.playground_id, PlaygroundId::new)?;
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
        validate_commit_text(
            &request.message,
            request.description.as_deref(),
            &request.tag_names,
        )?;

        // All Commit mutations share this boundary in the supported single-Server profile. It
        // closes the race between the authority transaction and the control-catalog transaction;
        // a process crash remains recoverable by replaying the stable commit_request_id.
        let _publication = self.publication_lock.lock().await;
        let artifact = self
            .catalog
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let playground = self
            .catalog
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("playground"))?;
        if playground.state != PlaygroundState::Ready {
            return Err(commit_conflict(
                "playground_not_ready",
                "PLAYGROUND_NOT_READY",
                "only a Ready Playground can be committed",
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
            || precommit.playground_id != playground_id
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
                || stored.source_playground_id != playground_id
                || stored
                    .source_storage_volume_id
                    .as_ref()
                    .is_some_and(|volume_id| volume_id != &playground.storage_volume_id)
                || stored.source_precommit_id != precommit_id
                || !same_index_version(&stored.index_version, &expected_candidate)
                || stored.message != request.message
                || stored.description != request.description
                || stored.tag_names != request.tag_names
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
                    commit: stored,
                })
                .await
                .map_err(map_central_error)?
        } else {
            let frozen_head = precommit.frozen_head_commit_id.map(Into::into);
            if artifact.head_commit_id != frozen_head || playground.head_commit_id != frozen_head {
                return Err(commit_conflict(
                    "artifact_head_mismatch",
                    "ARTIFACT_HEAD_MISMATCH",
                    "Artifact or Playground Head changed after Pre-commit",
                ));
            }
            let index_key = IndexKey {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: playground_id.clone(),
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
                    "the published Playground Index no longer matches this Pre-commit",
                ));
            }
            let created_at_unix_ms = self.clock.now();
            let candidate_records = precommit.candidate_records.clone().ok_or_else(|| {
                internal_error("a ready Pre-commit lost its frozen Index snapshot")
            })?;
            let reader = PublishedIndexReader::new(neoengramd::PublishedIndex {
                version: expected_candidate.clone(),
                records: candidate_records,
            });
            let graph = build_commit_graph(
                &BuildCommitGraphRequest {
                    expected_index_version: reader.version.clone(),
                    parent: precommit.frozen_head_commit_id.map(Into::into),
                    message: request.message.clone(),
                    created_at_unix_ms: created_at_unix_ms.get(),
                },
                &reader,
                &NoopProgressSink,
            )
            .map_err(commit_graph_error)?;
            self.precommits
                .commit(PreCommitCommitRequest {
                    key: key.clone(),
                    expected_candidate_index_version: expected_candidate.clone(),
                    commit: CommitRecord {
                        tenant_id: tenant_id.clone(),
                        project_id: project_id.clone(),
                        artifact_id: artifact_id.clone(),
                        source_playground_id: playground_id.clone(),
                        source_storage_volume_id: Some(playground.storage_volume_id.clone()),
                        source_precommit_id: precommit_id,
                        commit_request_id,
                        commit_id: graph.commit_id,
                        root_directory_id: graph.commit.root_directory_id,
                        parent_commit_id: graph.commit.parent,
                        index_version: expected_candidate,
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

        if authority_outcome
            .consumed_precommit
            .head_published_at_unix_ms
            .is_some()
        {
            return Ok(WorkspaceCommitResult {
                commit: authority_outcome.commit,
                playground,
                consumed_precommit: authority_outcome.consumed_precommit,
                replayed: true,
            });
        }
        let heads = self
            .catalog
            .advance_playground_commit(AdvancePlaygroundCommitRequest {
                tenant_id,
                project_id,
                artifact_id,
                playground_id,
                expected_head_commit_id: authority_outcome.commit.parent_commit_id.map(Into::into),
                commit_id: authority_outcome.commit.commit_id.into(),
                updated_at_unix_ms: authority_outcome.commit.created_at_unix_ms,
            })
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
            playground: heads.playground,
            consumed_precommit,
            replayed: authority_outcome.replayed || heads.replayed,
        })
    }
}

#[derive(Debug)]
struct PublishedIndexReader {
    version: IndexVersion,
    records: Vec<FileRecord>,
}

impl PublishedIndexReader {
    fn new(index: neoengramd::PublishedIndex) -> Self {
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

fn commit_graph_error(error: EngineError) -> Error {
    application_error(
        ErrorCategory::Internal,
        "commit_graph_invalid",
        "COMMIT_GRAPH_INVALID",
        format!("the authoritative Index could not form a canonical Commit: {error}"),
        false,
    )
}
