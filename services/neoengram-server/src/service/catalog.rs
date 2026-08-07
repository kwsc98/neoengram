use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use fusen_rs::{Error, ErrorCategory};
use neoengram_core::{CommitId, ContentDigest, FileRecord, LogicalPath};
use neoengram_protocol::{
    ArtifactId, EdgeClusterId, IndexRevision, JobId, PlaygroundId, ProjectId, PvcIdentityDigest,
    RequestId, SnapshotId, StorageVolumeId, TenantId, UnixMillis, WireIndexVersion,
};
use neoengramd::{
    diff_index_snapshots, summarize_index_changes, ArtifactHeadExpectation,
    ArtifactInitialization as CatalogArtifactInitialization, ArtifactListCursor,
    ArtifactListRequest, ArtifactRecord, CatalogInsertOutcome, CatalogNfsReference,
    CatalogPvcReference, Clock, CommitRecord, ControlCatalogRepository, IndexKey, IndexPublisher,
    IndexSnapshotChangeKind, PlaygroundInsertRequest, PlaygroundListCursor, PlaygroundListRequest,
    PlaygroundRecord, PlaygroundState, PreCommitCancelRequest as DomainPreCommitCancelRequest,
    PreCommitId, PreCommitKey, PreCommitPhase, PreCommitRecord, PreCommitRepository,
    PreCommitRestartRequest as DomainPreCommitRestartRequest,
    PreCommitStartRequest as DomainPreCommitStartRequest, PreCommitState, SnapshotInsertOutcome,
    SnapshotInsertRequest, SnapshotListCursor, SnapshotListRequest, SnapshotPhase, SnapshotRecord,
    SnapshotState, StorageAccessMode, StorageBackendType, StorageVolumeListCursor,
    StorageVolumeListRequest, StorageVolumeRecord, StorageVolumeState, TenantListCursor,
    TenantListRequest, TenantRecord,
};
use serde::{Deserialize, Serialize};

use crate::{
    dto::{
        ArtifactInitialization as ArtifactInitializationBody, ArtifactView, CancelPreCommitRequest,
        CancelPreCommitResponse, CommitGraphView, CommitNodeView, CommitPlaygroundRequest,
        CommitPlaygroundResponse, CreateArtifactRequest, CreateArtifactResponse,
        CreatePlaygroundRequest, CreatePlaygroundResponse, CreateSnapshotRequest,
        CreateSnapshotResponse, CreateStorageVolumeRequest, CreateStorageVolumeResponse,
        CreateTenantRequest, CreateTenantResponse, DatasetProfileSummary, DatasetProfileView,
        FileMetadataView, IndexVersionBody, LogicalFileEntry, PlaygroundChangeEntry,
        PlaygroundChangeSummary, PlaygroundView, PreCommitCheckView, PreCommitDiffSummaryView,
        PreCommitNoticeView, PreCommitProgressView, PreCommitView, PvcReference,
        QueryArtifactCommitGraphRequest, QueryArtifactCommitGraphResponse,
        QueryArtifactListRequest, QueryArtifactListResponse, QueryArtifactRequest,
        QueryArtifactResponse, QueryPlaygroundChangeListRequest, QueryPlaygroundChangeListResponse,
        QueryPlaygroundDatasetProfileRequest, QueryPlaygroundDatasetProfileResponse,
        QueryPlaygroundFileListRequest, QueryPlaygroundFileListResponse,
        QueryPlaygroundFileMetadataRequest, QueryPlaygroundFileMetadataResponse,
        QueryPlaygroundListRequest, QueryPlaygroundListResponse, QueryPlaygroundRequest,
        QueryPlaygroundResponse, QueryPreCommitRequest, QueryPreCommitResponse,
        QuerySnapshotListRequest, QuerySnapshotListResponse, QuerySnapshotRequest,
        QuerySnapshotResponse, QueryStorageVolumeListRequest, QueryStorageVolumeListResponse,
        QueryStorageVolumeRequest, QueryStorageVolumeResponse, QueryTenantListRequest,
        QueryTenantListResponse, QueryTenantRequest, QueryTenantResponse, ResourceIssueSummary,
        RestartPreCommitRequest, RestartPreCommitResponse, SnapshotIntegritySummary, SnapshotView,
        StartPreCommitRequest, StartPreCommitResponse, StorageVolumeView, TenantView,
    },
    error::{application_error, invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy, TenantVisibility},
};

const DEFAULT_PAGE_SIZE: u16 = 50;
const MAX_PAGE_SIZE: u16 = 100;
const CURSOR_PREFIX: &str = "ngcat_v1_";
const MAX_QUERY_CHARS: usize = 256;
const RESERVED_CREATE_FIELDS: &[&str] = &["actor", "principal"];
const RESERVED_VOLUME_FIELDS: &[&str] = &["actor", "credentials", "mount_path", "principal"];
const RESERVED_PRECOMMIT_FIELDS: &[&str] = &[
    "actor",
    "expected_head_commit_id",
    "principal",
    "request_digest",
    "source_head_commit_id",
];
const RESERVED_SNAPSHOT_FIELDS: &[&str] = &["actor", "principal", "request_digest"];

/// Public Tenant, StorageVolume, and minimal Playground application service.
pub struct CatalogService {
    repository: Arc<dyn ControlCatalogRepository>,
    indexes: Arc<dyn IndexPublisher>,
    policy: Arc<StaticRbacPolicy>,
    clock: Arc<dyn Clock>,
    coordinator: Option<Arc<super::JobCoordinator>>,
    precommits: Option<Arc<dyn PreCommitRepository>>,
    workspace_commits: Option<Arc<super::WorkspaceCommitService>>,
}

impl CatalogService {
    #[must_use]
    pub fn new(
        repository: Arc<dyn ControlCatalogRepository>,
        indexes: Arc<dyn IndexPublisher>,
        policy: Arc<StaticRbacPolicy>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            repository,
            indexes,
            policy,
            clock,
            coordinator: None,
            precommits: None,
            workspace_commits: None,
        }
    }

    #[must_use]
    pub fn with_coordinator(mut self, coordinator: Arc<super::JobCoordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
    }

    #[must_use]
    pub fn with_precommits(mut self, precommits: Arc<dyn PreCommitRepository>) -> Self {
        self.precommits = Some(precommits);
        self
    }

    #[must_use]
    pub fn with_workspace_commits(
        mut self,
        workspace_commits: Arc<super::WorkspaceCommitService>,
    ) -> Self {
        self.workspace_commits = Some(workspace_commits);
        self
    }

    pub async fn list_tenants(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTenantListRequest,
    ) -> Result<QueryTenantListResponse, Error> {
        let visible_tenant_ids = match self
            .policy
            .tenant_visibility(identity.principal(), Permission::TenantRead)
        {
            TenantVisibility::None => Some(Vec::new()),
            TenantVisibility::All => None,
            TenantVisibility::Explicit(ids) => Some(ids),
        };
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Tenant {
            visible: visible_tenant_ids
                .as_ref()
                .map(|ids| ids.iter().map(ToString::to_string).collect()),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_tenant_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_tenants(&TenantListRequest {
                visible_tenant_ids,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let items = page
            .records
            .iter()
            .map(|record| self.tenant_view(identity, record))
            .collect();
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_tenant_cursor(&scope, cursor))
            .transpose()?;
        let can_create_tenant = !matches!(
            self.policy
                .tenant_visibility(identity.principal(), Permission::TenantCreate),
            TenantVisibility::None
        );
        Ok(QueryTenantListResponse {
            items,
            next_cursor,
            can_create_tenant,
        })
    }

    pub async fn query_tenant(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryTenantRequest,
    ) -> Result<QueryTenantResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::TenantRead, &tenant_id)
        {
            return Err(resource_not_found("tenant"));
        }
        let record = self
            .repository
            .get_tenant(&tenant_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("tenant"))?;
        Ok(QueryTenantResponse {
            tenant: self.tenant_view(identity, &record),
        })
    }

    pub async fn create_tenant(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateTenantRequest,
    ) -> Result<CreateTenantResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_CREATE_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.policy
            .authorize_identity(identity, Permission::TenantCreate, &tenant_id)?;
        let display_name = validate_display_name(request.display_name)?;
        let description = request.description.map(validate_description).transpose()?;
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_tenant(TenantRecord {
                tenant_id,
                display_name,
                description,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "tenant_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        Ok(CreateTenantResponse {
            tenant: self.tenant_view(identity, &record),
            replayed,
        })
    }

    pub async fn list_storage_volumes(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryStorageVolumeListRequest,
    ) -> Result<QueryStorageVolumeListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::StorageRead, &tenant_id)
            .await?;
        let region = request.region.map(validate_region).transpose()?;
        let backend_type = request
            .backend_type
            .as_deref()
            .map(parse_backend_type)
            .transpose()?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Volume {
            tenant_id: tenant_id.to_string(),
            region: region.clone(),
            backend_type: backend_type.map(backend_name).map(str::to_owned),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_volume_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_storage_volumes(&StorageVolumeListRequest {
                tenant_id,
                region,
                backend_type,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_volume_cursor(&scope, cursor))
            .transpose()?;
        Ok(QueryStorageVolumeListResponse {
            items: page.records.iter().map(storage_volume_view).collect(),
            next_cursor,
        })
    }

    pub async fn query_storage_volume(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryStorageVolumeRequest,
    ) -> Result<QueryStorageVolumeResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::StorageRead, &tenant_id)
        {
            return Err(resource_not_found("storage volume"));
        }
        let storage_volume_id = parse_volume_id(request.storage_volume_id)?;
        let record = self
            .repository
            .get_storage_volume(&tenant_id, &storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        Ok(QueryStorageVolumeResponse {
            storage_volume: storage_volume_view(&record),
        })
    }

    pub async fn create_storage_volume(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateStorageVolumeRequest,
    ) -> Result<CreateStorageVolumeResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_VOLUME_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::StorageCreate, &tenant_id)
            .await?;
        let storage_volume_id = parse_volume_id(request.storage_volume_id)?;
        let display_name = validate_display_name(request.display_name)?;
        let edge_cluster_id = EdgeClusterId::new(request.edge_cluster_id)
            .map_err(|error| invalid_request(format!("edge_cluster_id: {error}")))?;
        let region = validate_region(request.region)?;
        let backend_type = parse_backend_type(&request.backend_type)?;
        let access_mode = parse_access_mode(&request.access_mode)?;
        let (pvc_reference, nfs_reference) = match backend_type {
            StorageBackendType::Pvc => {
                if request.nfs_reference.is_some() {
                    return Err(invalid_request(
                        "nfs_reference is forbidden when backend_type is pvc",
                    ));
                }
                let reference = request.pvc_reference.ok_or_else(|| {
                    invalid_request("pvc_reference is required when backend_type is pvc")
                })?;
                PvcIdentityDigest::derive(&reference.namespace, &reference.claim_name)
                    .map_err(|error| invalid_request(error.to_string()))?;
                (
                    Some(CatalogPvcReference {
                        namespace: reference.namespace,
                        claim_name: reference.claim_name,
                    }),
                    None,
                )
            }
            StorageBackendType::Nfs => {
                if request.pvc_reference.is_some() {
                    return Err(invalid_request(
                        "pvc_reference is forbidden when backend_type is nfs",
                    ));
                }
                let reference = request.nfs_reference.ok_or_else(|| {
                    invalid_request("nfs_reference is required when backend_type is nfs")
                })?;
                let server = validate_nfs_server(reference.server)?;
                let export_path = validate_nfs_export_path(reference.export_path)?;
                (
                    None,
                    Some(CatalogNfsReference {
                        server,
                        export_path,
                    }),
                )
            }
        };
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id,
                storage_volume_id,
                display_name,
                edge_cluster_id,
                region,
                backend_type,
                access_mode,
                pvc_reference,
                nfs_reference,
                state: StorageVolumeState::Unavailable,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "storage_volume_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        Ok(CreateStorageVolumeResponse {
            storage_volume: storage_volume_view(&record),
            replayed,
        })
    }

    pub async fn list_artifacts(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryArtifactListRequest,
    ) -> Result<QueryArtifactListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactRead, &tenant_id)
            .await?;
        let project_id = request.project_id.map(parse_project_id).transpose()?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Artifact {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.as_ref().map(ToString::to_string),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_artifact_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_artifacts(&ArtifactListRequest {
                tenant_id,
                project_id,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_artifact_cursor(&scope, cursor))
            .transpose()?;
        Ok(QueryArtifactListResponse {
            items: page.records.iter().map(artifact_view).collect(),
            next_cursor,
        })
    }

    pub async fn query_artifact(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryArtifactRequest,
    ) -> Result<QueryArtifactResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::ArtifactRead, &tenant_id)
        {
            return Err(resource_not_found("artifact"));
        }
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let record = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        Ok(QueryArtifactResponse {
            artifact: artifact_view(&record),
        })
    }

    pub async fn query_artifact_commit_graph(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryArtifactCommitGraphRequest,
    ) -> Result<QueryArtifactCommitGraphResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::ArtifactRead, &tenant_id)
        {
            return Err(resource_not_found("artifact"));
        }
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::CommitGraph {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
            artifact_id: artifact_id.to_string(),
            graph_version: artifact.resource_version.to_string(),
            head_commit_id: artifact
                .head_commit_id
                .map(|commit_id| commit_id.to_string()),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_commit_cursor(cursor, &scope))
            .transpose()?;
        let precommits = self.precommit_repository()?;
        let mut current = artifact.head_commit_id.map(CommitId::from_digest);
        let mut seeking_after = after.is_some();
        let mut visited = BTreeSet::new();
        let mut records = Vec::with_capacity(usize::from(page_size));
        let mut next = None;
        while let Some(commit_id) = current {
            if !visited.insert(commit_id) {
                return Err(internal_catalog_error());
            }
            let commit = precommits
                .get_commit(&tenant_id, &project_id, &artifact_id, commit_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(internal_catalog_error)?;
            current = commit.parent_commit_id;
            if seeking_after {
                let cursor = after.as_ref().expect("cursor exists while seeking");
                if commit.commit_id == cursor.commit_id {
                    if commit.created_at_unix_ms != cursor.created_at_unix_ms {
                        return Err(cursor_conflict());
                    }
                    seeking_after = false;
                }
                continue;
            }
            records.push(commit);
            if records.len() == usize::from(page_size) {
                if current.is_some() {
                    let last = records.last().expect("non-empty Commit graph page");
                    next = Some(CommitGraphCursor {
                        created_at_unix_ms: last.created_at_unix_ms,
                        commit_id: last.commit_id,
                    });
                }
                break;
            }
        }
        if seeking_after {
            return Err(cursor_conflict());
        }
        let next_cursor = next
            .as_ref()
            .map(|cursor| encode_commit_cursor(&scope, cursor))
            .transpose()?;
        Ok(QueryArtifactCommitGraphResponse {
            graph: CommitGraphView {
                graph_version: artifact.resource_version.to_string(),
                head_commit_id: artifact
                    .head_commit_id
                    .map(|commit_id| commit_id.to_string()),
                nodes: records.iter().map(commit_node_view).collect(),
                next_cursor,
            },
        })
    }

    pub async fn create_artifact(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateArtifactRequest,
    ) -> Result<CreateArtifactResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_CREATE_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCreate, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let display_name = validate_display_name(request.display_name)?;
        let description = request.description.map(validate_description).transpose()?;
        let initialization = match request.initialization {
            ArtifactInitializationBody::Empty => CatalogArtifactInitialization::Empty,
            ArtifactInitializationBody::Derived {
                source_project_id,
                source_artifact_id,
                source_commit_id,
            } => {
                parse_project_id(source_project_id)?;
                parse_artifact_id(source_artifact_id)?;
                ContentDigest::from_str(&source_commit_id).map_err(|_| {
                    invalid_request("initialization.source_commit_id must be a 64-character digest")
                })?;
                return Err(catalog_conflict(
                    "artifact_derived_initialization_unsupported",
                    "ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED",
                    "derived Artifact initialization requires authoritative Commit validation",
                ));
            }
        };
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_artifact(ArtifactRecord {
                tenant_id,
                project_id,
                artifact_id,
                display_name,
                description,
                initialization,
                head_commit_id: None,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "artifact_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        Ok(CreateArtifactResponse {
            artifact: artifact_view(&record),
            replayed,
        })
    }

    pub async fn list_playgrounds(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundListRequest,
    ) -> Result<QueryPlaygroundListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundRead, &tenant_id)
            .await?;
        if request.artifact_id.is_some() && request.project_id.is_none() {
            return Err(invalid_request(
                "project_id is required when artifact_id is provided",
            ));
        }
        let project_id = request.project_id.map(parse_project_id).transpose()?;
        let artifact_id = request.artifact_id.map(parse_artifact_id).transpose()?;
        let region = request.region.map(validate_region).transpose()?;
        let state = request
            .state
            .as_deref()
            .map(parse_playground_state)
            .transpose()?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Playground {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.as_ref().map(ToString::to_string),
            artifact_id: artifact_id.as_ref().map(ToString::to_string),
            region: region.clone(),
            state: state.map(playground_state_name).map(str::to_owned),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_playground_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_playgrounds(&PlaygroundListRequest {
                tenant_id,
                project_id,
                artifact_id,
                region,
                state,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_playground_cursor(&scope, cursor))
            .transpose()?;
        let mut items = Vec::with_capacity(page.records.len());
        for record in &page.records {
            items.push(self.playground_view(record).await?);
        }
        Ok(QueryPlaygroundListResponse { items, next_cursor })
    }

    pub async fn query_playground(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundRequest,
    ) -> Result<QueryPlaygroundResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::PlaygroundRead, &tenant_id)
        {
            return Err(resource_not_found("playground"));
        }
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let playground_id = parse_playground_id(request.playground_id)?;
        let record = self
            .repository
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("playground"))?;
        Ok(QueryPlaygroundResponse {
            playground: self.playground_view(&record).await?,
        })
    }

    pub async fn create_playground(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreatePlaygroundRequest,
    ) -> Result<CreatePlaygroundResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_CREATE_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let playground_id = parse_playground_id(request.playground_id)?;
        let storage_volume_id = parse_volume_id(request.storage_volume_id)?;
        let display_name = validate_display_name(request.display_name)?;
        let requested_base_commit_id = request
            .base_commit_id
            .map(|value| {
                ContentDigest::from_str(&value)
                    .map_err(|_| invalid_request("base_commit_id must be a 64-character digest"))
            })
            .transpose()?;
        let relative_root = format!(
            "playgrounds/{}/{}/{}",
            project_id, artifact_id, playground_id
        );
        if let Some(existing) = self
            .repository
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .map_err(map_central_error)?
        {
            if existing.storage_volume_id == storage_volume_id
                && existing.display_name == display_name
                && existing.relative_root == relative_root
                && requested_base_commit_id
                    .as_ref()
                    .is_none_or(|requested| existing.base_commit_id.as_ref() == Some(requested))
            {
                self.ensure_workspace_materialization(&existing).await;
                return Ok(CreatePlaygroundResponse {
                    playground: self.playground_view(&existing).await?,
                    replayed: true,
                });
            }
            return Err(catalog_conflict(
                "playground_id_reused",
                "PLAYGROUND_ID_REUSED",
                "Playground ID is already bound to another create request",
            ));
        }
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        // An explicit base may select any immutable Commit owned by this exact Artifact scope.
        // Omitting it still means "freeze the current authoritative Head", not an empty baseline.
        let (base_commit_id, artifact_head) = match requested_base_commit_id {
            Some(commit_id) => {
                let exists = self
                    .precommit_repository()?
                    .get_commit(
                        &tenant_id,
                        &project_id,
                        &artifact_id,
                        CommitId::from_digest(commit_id),
                    )
                    .await
                    .map_err(map_central_error)?
                    .is_some();
                if !exists {
                    return Err(resource_not_found("commit"));
                }
                (Some(commit_id), ArtifactHeadExpectation::Any)
            }
            None => (
                artifact.head_commit_id,
                ArtifactHeadExpectation::Exact(artifact.head_commit_id),
            ),
        };
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        if volume.state != StorageVolumeState::Ready {
            return Err(catalog_conflict(
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "the selected StorageVolume is not ready for placement",
            ));
        }
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_playground_fenced(PlaygroundInsertRequest {
                record: PlaygroundRecord {
                    tenant_id,
                    project_id,
                    artifact_id,
                    playground_id,
                    storage_volume_id,
                    region: volume.region,
                    display_name,
                    base_commit_id,
                    head_commit_id: base_commit_id,
                    // Physical materialization is asynchronous. The record must remain Creating
                    // until the selected owner Agent confirms the directory is ready.
                    state: PlaygroundState::Creating,
                    relative_root,
                    created_at_unix_ms: now,
                    updated_at_unix_ms: now,
                },
                artifact_head,
            })
            .await
            .map_err(playground_mutation_error)?;
        let (record, replayed) = split_outcome(outcome);
        self.ensure_workspace_materialization(&record).await;
        Ok(CreatePlaygroundResponse {
            playground: self.playground_view(&record).await?,
            replayed,
        })
    }

    pub async fn list_snapshots(
        &self,
        identity: &AuthenticatedIdentity,
        request: QuerySnapshotListRequest,
    ) -> Result<QuerySnapshotListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotRead, &tenant_id)
            .await?;
        if request.artifact_id.is_some() && request.project_id.is_none() {
            return Err(invalid_request(
                "project_id is required when artifact_id is provided",
            ));
        }
        let project_id = request.project_id.map(parse_project_id).transpose()?;
        let artifact_id = request.artifact_id.map(parse_artifact_id).transpose()?;
        let commit_id = request
            .commit_id
            .map(|value| {
                ContentDigest::from_str(&value)
                    .map_err(|_| invalid_request("commit_id must be a 64-character digest"))
            })
            .transpose()?;
        let storage_volume_id = request.storage_volume_id.map(parse_volume_id).transpose()?;
        let region = request.region.map(validate_region).transpose()?;
        let state = request
            .state
            .as_deref()
            .map(parse_snapshot_state)
            .transpose()?;
        let page_size = page_size(request.page_size)?;
        let scope = CursorScope::Snapshot {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.as_ref().map(ToString::to_string),
            artifact_id: artifact_id.as_ref().map(ToString::to_string),
            commit_id: commit_id.map(|id| id.to_string()),
            storage_volume_id: storage_volume_id.as_ref().map(ToString::to_string),
            region: region.clone(),
            state: state.map(snapshot_state_name).map(str::to_owned),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_snapshot_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .repository
            .list_snapshots(&SnapshotListRequest {
                tenant_id,
                project_id,
                artifact_id,
                commit_id,
                storage_volume_id,
                region,
                state,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_central_error)?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|cursor| encode_snapshot_cursor(&scope, cursor))
            .transpose()?;
        let mut items = Vec::with_capacity(page.records.len());
        for record in &page.records {
            items.push(self.snapshot_view(record).await?);
        }
        Ok(QuerySnapshotListResponse { items, next_cursor })
    }

    pub async fn query_snapshot(
        &self,
        identity: &AuthenticatedIdentity,
        request: QuerySnapshotRequest,
    ) -> Result<QuerySnapshotResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::SnapshotRead, &tenant_id)
        {
            return Err(resource_not_found("snapshot"));
        }
        let snapshot_id = parse_snapshot_id(request.snapshot_id)?;
        let record = self
            .repository
            .get_snapshot(&tenant_id, &snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        Ok(QuerySnapshotResponse {
            snapshot: self.snapshot_view(&record).await?,
        })
    }

    pub async fn create_snapshot(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateSnapshotRequest,
    ) -> Result<CreateSnapshotResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_SNAPSHOT_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let storage_volume_id = parse_volume_id(request.storage_volume_id)?;
        let snapshot_request_id = RequestId::new(request.snapshot_request_id)
            .map_err(|error| invalid_request(format!("snapshot_request_id: {error}")))?;
        let commit_id = ContentDigest::from_str(&request.commit_id)
            .map_err(|_| invalid_request("commit_id must be a 64-character digest"))?;
        let snapshot_id = deterministic_snapshot_id(&tenant_id, &snapshot_request_id)?;
        if let Some(existing) = self
            .repository
            .get_snapshot(&tenant_id, &snapshot_id)
            .await
            .map_err(map_central_error)?
        {
            if existing.project_id != project_id
                || existing.artifact_id != artifact_id
                || existing.commit_id != commit_id
                || existing.storage_volume_id != storage_volume_id
                || existing.snapshot_request_id != snapshot_request_id
            {
                return Err(catalog_conflict(
                    "snapshot_request_id_reused",
                    "SNAPSHOT_REQUEST_ID_REUSED",
                    "Snapshot request ID is already bound to another create payload",
                ));
            }
            self.ensure_snapshot_mount(&existing).await;
            return Ok(CreateSnapshotResponse {
                snapshot: self.snapshot_view(&existing).await?,
                replayed: true,
                placement_reused: false,
            });
        }
        let _artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let artifact_head = ArtifactHeadExpectation::Any;
        let commit = self
            .precommit_repository()?
            .get_commit(
                &tenant_id,
                &project_id,
                &artifact_id,
                CommitId::from_digest(commit_id),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("commit"))?;
        let source_storage_volume_id =
            if let Some(source_storage_volume_id) = commit.source_storage_volume_id.as_ref() {
                source_storage_volume_id.clone()
            } else {
                // Compatibility for immutable Commit payloads written before placement provenance was
                // persisted. New Commits never depend on the source Playground for Snapshot placement.
                self.repository
                    .get_playground(
                        &tenant_id,
                        &project_id,
                        &artifact_id,
                        &commit.source_playground_id,
                    )
                    .await
                    .map_err(map_central_error)?
                    .ok_or_else(|| resource_not_found("commit"))?
                    .storage_volume_id
            };
        if source_storage_volume_id != storage_volume_id {
            return Err(catalog_conflict(
                "snapshot_volume_has_no_commit_data",
                "SNAPSHOT_VOLUME_HAS_NO_COMMIT_DATA",
                "the selected StorageVolume does not hold this Commit's immutable objects",
            ));
        }
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        if volume.state != StorageVolumeState::Ready {
            return Err(catalog_conflict(
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "the selected StorageVolume is not ready for Snapshot placement",
            ));
        }
        let relative_root = neoengram_protocol::SnapshotMountAssignment::canonical_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
        )
        .map_err(|error| invalid_request(format!("snapshot path: {error}")))?
        .to_string();
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_snapshot_fenced(SnapshotInsertRequest {
                record: SnapshotRecord {
                    tenant_id,
                    project_id,
                    artifact_id,
                    snapshot_id,
                    snapshot_request_id,
                    commit_id,
                    storage_volume_id,
                    region: volume.region,
                    state: SnapshotState::Creating,
                    phase: SnapshotPhase::Planning,
                    relative_root,
                    created_at_unix_ms: now,
                    updated_at_unix_ms: now,
                },
                artifact_head,
            })
            .await
            .map_err(snapshot_mutation_error)?;
        let (record, replayed, placement_reused) = match outcome {
            SnapshotInsertOutcome::Inserted(record) => (record, false, false),
            SnapshotInsertOutcome::ExistingRequest(record) => (record, true, false),
            SnapshotInsertOutcome::ExistingPlacement(record) => (record, false, true),
        };
        // Commit was loaded before the fenced insert. Retain this assertion so a corrupted
        // repository cannot dispatch a different immutable Index than the public response.
        if ContentDigest::from(commit.commit_id) != record.commit_id {
            return Err(internal_catalog_error());
        }
        self.ensure_snapshot_mount(&record).await;
        Ok(CreateSnapshotResponse {
            snapshot: self.snapshot_view(&record).await?,
            replayed,
            placement_reused,
        })
    }

    async fn ensure_snapshot_mount(&self, snapshot: &SnapshotRecord) {
        if snapshot.state != SnapshotState::Creating {
            return;
        }
        let Some(coordinator) = &self.coordinator else {
            return;
        };
        if let Err(error) = coordinator.ensure_snapshot_mount(snapshot).await {
            tracing::warn!(
                tenant_id = %snapshot.tenant_id,
                snapshot_id = %snapshot.snapshot_id,
                %error,
                "immediate Snapshot mount dispatch failed; recovery will retry"
            );
        }
    }

    async fn ensure_workspace_materialization(&self, playground: &PlaygroundRecord) {
        if playground.state != PlaygroundState::Creating {
            return;
        }
        let Some(coordinator) = &self.coordinator else {
            return;
        };
        if let Err(error) = coordinator
            .ensure_workspace_materialization(playground)
            .await
        {
            tracing::warn!(
                tenant_id = %playground.tenant_id,
                project_id = %playground.project_id,
                artifact_id = %playground.artifact_id,
                playground_id = %playground.playground_id,
                %error,
                "immediate Playground materialization dispatch failed; recovery will retry"
            );
        }
    }

    pub async fn start_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: StartPreCommitRequest,
    ) -> Result<StartPreCommitResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_PRECOMMIT_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        self.require_tenant(identity, Permission::CreateAddJob, &tenant_id)
            .await?;
        let project_id = parse_project_id(request.project_id)?;
        let artifact_id = parse_artifact_id(request.artifact_id)?;
        let playground_id = parse_playground_id(request.playground_id)?;
        let precommit_request_id = RequestId::new(request.precommit_request_id)
            .map_err(|error| invalid_request(format!("precommit_request_id: {error}")))?;
        let source_index_version = parse_index_version(request.expected_index_version)?;
        let precommit_id = deterministic_precommit_id(&tenant_id, &precommit_request_id)?;
        let key = PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
        let (precommits, coordinator) = self.precommit_execution()?;
        let now = self.clock.now();

        // The proposed ID is derived from the idempotency key. Resolve a persisted replay before
        // consulting mutable Playground/Index state, which may already have advanced.
        if let Some(existing) = precommits.get(&key).await.map_err(map_central_error)? {
            let outcome = precommits
                .start(DomainPreCommitStartRequest {
                    tenant_id,
                    project_id,
                    artifact_id,
                    playground_id,
                    precommit_id,
                    precommit_request_id,
                    source_index_version,
                    frozen_head_commit_id: existing.frozen_head_commit_id,
                    job_id: existing.job_id.clone(),
                    created_at_unix_ms: now,
                })
                .await
                .map_err(precommit_mutation_error)?;
            if outcome.precommit.state == PreCommitState::Running {
                coordinator
                    .ensure_precommit_job(&outcome.precommit)
                    .await
                    .map_err(map_central_error)?;
            }
            let playground = self
                .load_precommit_playground(identity, &outcome.precommit, Permission::PlaygroundRead)
                .await?;
            return Ok(StartPreCommitResponse {
                precommit: precommit_view(&outcome.precommit),
                playground: self.playground_view(&playground).await?,
                replayed: outcome.replayed,
            });
        }

        let playground = self
            .load_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await?;
        self.validate_precommit_source(&playground, &source_index_version)
            .await?;
        let artifact = self
            .repository
            .get_artifact(&tenant_id, &project_id, &artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let job_id = deterministic_precommit_job_id(&tenant_id, &precommit_id, 1)?;
        let outcome = precommits
            .start(DomainPreCommitStartRequest {
                tenant_id,
                project_id,
                artifact_id,
                playground_id,
                precommit_id,
                precommit_request_id,
                source_index_version,
                frozen_head_commit_id: artifact.head_commit_id.map(Into::into),
                job_id,
                created_at_unix_ms: now,
            })
            .await
            .map_err(precommit_mutation_error)?;
        coordinator
            .ensure_precommit_job(&outcome.precommit)
            .await
            .map_err(map_central_error)?;
        Ok(StartPreCommitResponse {
            precommit: precommit_view(&outcome.precommit),
            playground: self.playground_view(&playground).await?,
            replayed: outcome.replayed,
        })
    }

    pub async fn query_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPreCommitRequest,
    ) -> Result<QueryPreCommitResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundRead, &tenant_id)
            .await?;
        let precommit_id = PreCommitId::new(request.precommit_id)
            .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
        let precommits = self.precommit_repository()?;
        let stored = precommits
            .get(&PreCommitKey::new(tenant_id, precommit_id))
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        self.load_precommit_playground(identity, &stored, Permission::PlaygroundRead)
            .await?;
        let synchronized = match &self.coordinator {
            Some(coordinator) => coordinator
                .synchronize_precommit(&stored)
                .await
                .map_err(map_central_error)?,
            None => stored,
        };
        Ok(QueryPreCommitResponse {
            precommit: precommit_view(&synchronized),
        })
    }

    pub async fn restart_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: RestartPreCommitRequest,
    ) -> Result<RestartPreCommitResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_PRECOMMIT_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        self.require_tenant(identity, Permission::CreateAddJob, &tenant_id)
            .await?;
        let precommit_id = PreCommitId::new(request.precommit_id)
            .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
        let restart_request_id = RequestId::new(request.restart_request_id)
            .map_err(|error| invalid_request(format!("restart_request_id: {error}")))?;
        let source_index_version = parse_index_version(request.expected_index_version)?;
        let key = PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
        let (precommits, coordinator) = self.precommit_execution()?;
        let stored = precommits
            .get(&key)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        let playground = self
            .load_precommit_playground(identity, &stored, Permission::PlaygroundCreate)
            .await?;

        if matches!(
            stored.state,
            PreCommitState::Abnormal | PreCommitState::Cancelled
        ) {
            self.validate_precommit_source(&playground, &source_index_version)
                .await?;
        }
        let artifact = self
            .repository
            .get_artifact(&stored.tenant_id, &stored.project_id, &stored.artifact_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("artifact"))?;
        let next_attempt = stored
            .attempt
            .checked_add(1)
            .ok_or_else(|| precommit_conflict("Pre-commit attempt is exhausted"))?;
        let job_id = deterministic_precommit_job_id(&tenant_id, &precommit_id, next_attempt)?;
        let outcome = precommits
            .restart(DomainPreCommitRestartRequest {
                key,
                restart_request_id,
                source_index_version,
                frozen_head_commit_id: artifact.head_commit_id.map(Into::into),
                job_id,
                restarted_at_unix_ms: self.clock.now(),
            })
            .await
            .map_err(precommit_mutation_error)?;
        if outcome.precommit.state == PreCommitState::Running {
            coordinator
                .ensure_precommit_job(&outcome.precommit)
                .await
                .map_err(map_central_error)?;
        }
        Ok(RestartPreCommitResponse {
            precommit: precommit_view(&outcome.precommit),
            playground: self.playground_view(&playground).await?,
            replayed: outcome.replayed,
        })
    }

    pub async fn cancel_playground_precommit(
        &self,
        identity: &AuthenticatedIdentity,
        request: CancelPreCommitRequest,
    ) -> Result<CancelPreCommitResponse, Error> {
        reject_reserved(&request.extensions, RESERVED_PRECOMMIT_FIELDS)?;
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        let precommit_id = PreCommitId::new(request.precommit_id)
            .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
        let cancel_request_id = RequestId::new(request.cancel_request_id)
            .map_err(|error| invalid_request(format!("cancel_request_id: {error}")))?;
        let precommits = self.precommit_repository()?;
        let key = PreCommitKey::new(tenant_id, precommit_id);
        let stored = precommits
            .get(&key)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("precommit"))?;
        let playground = self
            .load_precommit_playground(identity, &stored, Permission::PlaygroundCreate)
            .await?;
        let outcome = precommits
            .cancel(DomainPreCommitCancelRequest {
                key,
                cancel_request_id,
                cancelled_at_unix_ms: self.clock.now(),
            })
            .await
            .map_err(precommit_mutation_error)?;
        Ok(CancelPreCommitResponse {
            precommit: precommit_view(&outcome.precommit),
            playground: self.playground_view(&playground).await?,
            replayed: outcome.replayed,
        })
    }

    pub async fn commit_playground(
        &self,
        identity: &AuthenticatedIdentity,
        request: CommitPlaygroundRequest,
    ) -> Result<CommitPlaygroundResponse, Error> {
        let service = self.workspace_commits.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "workspace_commit_unavailable",
                "WORKSPACE_COMMIT_UNAVAILABLE",
                "Workspace Commit authority is unavailable",
                true,
            )
        })?;
        let result = service.commit_playground(identity, request).await?;
        Ok(CommitPlaygroundResponse {
            commit: CommitNodeView {
                commit_id: result.commit.commit_id.to_string(),
                parent_commit_id: result.commit.parent_commit_id.map(|id| id.to_string()),
                message: result.commit.message,
                description: result.commit.description,
                tag_names: result.commit.tag_names,
                created_at_unix_ms: result.commit.created_at_unix_ms.to_string(),
            },
            playground: self.playground_view(&result.playground).await?,
            consumed_precommit: precommit_view(&result.consumed_precommit),
            replayed: result.replayed,
        })
    }

    /// Lists the logical contents of the authoritative Playground Index. No Agent or mount
    /// locator participates in this read path.
    pub async fn query_playground_file_list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundFileListRequest,
    ) -> Result<QueryPlaygroundFileListResponse, Error> {
        let path_prefix = request
            .path_prefix
            .map(|value| parse_logical_path("path_prefix", value))
            .transpose()?;
        let format = request.format.map(validate_file_format).transpose()?;
        let page_size = page_size(request.page_size)?;
        let (_, index) = self
            .load_browse_index(
                identity,
                request.tenant_id,
                request.project_id,
                request.artifact_id,
                request.playground_id,
            )
            .await?;
        let scope = BrowseCursorScope {
            kind: "files",
            source_id: None,
            baseline_commit_id: None,
            index_revision: index.version.revision.to_string(),
            index_digest: index.version.digest.to_string(),
            path_prefix: path_prefix.as_ref().map(ToString::to_string),
            format: format.clone(),
            change_type: None,
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| decode_browse_cursor(cursor, &scope))
            .transpose()?;
        let mut entries = logical_entries(&index.records, path_prefix.as_ref(), format.as_deref());
        if let Some(after) = after {
            entries.retain(|entry| entry.path > after);
        }
        let (items, next_cursor) = take_browse_page(entries, page_size, &scope)?;
        Ok(QueryPlaygroundFileListResponse {
            index_version: index_version_body(&index.version),
            items,
            next_cursor,
        })
    }

    /// Returns the public, logical metadata available in the central Index for one file.
    pub async fn query_playground_file_metadata(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundFileMetadataRequest,
    ) -> Result<QueryPlaygroundFileMetadataResponse, Error> {
        let path = parse_logical_path("path", request.path)?;
        let (_, index) = self
            .load_browse_index(
                identity,
                request.tenant_id,
                request.project_id,
                request.artifact_id,
                request.playground_id,
            )
            .await?;
        let record = index
            .records
            .iter()
            .find(|record| record.path == path)
            .ok_or_else(|| resource_not_found("playground file"))?;
        let format = file_format(record.path.as_str());
        Ok(QueryPlaygroundFileMetadataResponse {
            index_version: index_version_body(&index.version),
            metadata: FileMetadataView {
                path: record.path.to_string(),
                size_bytes: record.total_size.to_string(),
                media_type: media_type(&format).map(str::to_owned),
                format,
                row_count: None,
            },
        })
    }

    /// Computes a minimal Dataset Profile from public Index attributes. Rich schema and quality
    /// fields remain absent until an Agent publishes a dedicated, validated profile batch.
    pub async fn query_playground_dataset_profile(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundDatasetProfileRequest,
    ) -> Result<QueryPlaygroundDatasetProfileResponse, Error> {
        let (_, index) = self
            .load_browse_index(
                identity,
                request.tenant_id,
                request.project_id,
                request.artifact_id,
                request.playground_id,
            )
            .await?;
        let logical_size_bytes = index.records.iter().try_fold(0_u64, |total, record| {
            total.checked_add(record.total_size).ok_or_else(|| {
                application_error(
                    ErrorCategory::Internal,
                    "profile_size_overflow",
                    "PROFILE_SIZE_OVERFLOW",
                    "the logical Dataset size cannot be represented",
                    false,
                )
            })
        })?;
        let format_count = index
            .records
            .iter()
            .map(|record| file_format(record.path.as_str()))
            .collect::<BTreeSet<_>>()
            .len();
        Ok(QueryPlaygroundDatasetProfileResponse {
            index_version: index_version_body(&index.version),
            profile: DatasetProfileView {
                state: "ready".to_owned(),
                summary: Some(DatasetProfileSummary {
                    format_count: u32::try_from(format_count).unwrap_or(u32::MAX),
                    logical_file_count: index.records.len().to_string(),
                    logical_size_bytes: logical_size_bytes.to_string(),
                    row_count: None,
                    field_count: None,
                }),
            },
        })
    }

    /// Lists the current workspace delta against its Head Commit, or a frozen Pre-commit delta
    /// against the Head captured by that exact attempt.
    pub async fn query_playground_change_list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryPlaygroundChangeListRequest,
    ) -> Result<QueryPlaygroundChangeListResponse, Error> {
        let QueryPlaygroundChangeListRequest {
            tenant_id,
            project_id,
            artifact_id,
            playground_id,
            precommit_id,
            change_type,
            path_prefix,
            cursor,
            page_size: requested_page_size,
        } = request;
        let change_type = change_type.map(validate_change_type).transpose()?;
        let path_prefix = path_prefix
            .map(|value| parse_logical_path("path_prefix", value))
            .transpose()?;
        let page_size = page_size(requested_page_size)?;

        if let Some(precommit_id) = precommit_id {
            let tenant_id = parse_tenant(tenant_id)?;
            if !self
                .policy
                .is_allowed(identity.principal(), Permission::PlaygroundRead, &tenant_id)
            {
                return Err(resource_not_found("precommit"));
            }
            let project_id = parse_project_id(project_id)?;
            let artifact_id = parse_artifact_id(artifact_id)?;
            let playground_id = parse_playground_id(playground_id)?;
            let precommit_id = PreCommitId::new(precommit_id)
                .map_err(|error| invalid_request(format!("precommit_id: {error}")))?;
            let precommits = self.precommit_repository()?;
            let stored = precommits
                .get(&PreCommitKey::new(tenant_id, precommit_id.clone()))
                .await
                .map_err(map_central_error)?
                .filter(|record| {
                    record.project_id == project_id
                        && record.artifact_id == artifact_id
                        && record.playground_id == playground_id
                })
                .ok_or_else(|| resource_not_found("precommit"))?;
            let synchronized = match &self.coordinator {
                Some(coordinator) => coordinator
                    .synchronize_precommit(&stored)
                    .await
                    .map_err(map_central_error)?,
                None => stored,
            };
            let version = synchronized
                .candidate_index_version
                .as_ref()
                .ok_or_else(|| {
                    catalog_conflict(
                        "precommit_not_ready",
                        "PRECOMMIT_NOT_READY",
                        "the Pre-commit has no frozen candidate yet",
                    )
                })?;
            let records = synchronized.candidate_records.as_deref().ok_or_else(|| {
                catalog_conflict(
                    "precommit_candidate_unavailable",
                    "PRECOMMIT_CANDIDATE_UNAVAILABLE",
                    "the Pre-commit candidate snapshot is unavailable",
                )
            })?;
            let baseline_commit_id = synchronized.frozen_head_commit_id;
            let base_records = self
                .load_commit_records(
                    &synchronized.tenant_id,
                    &synchronized.project_id,
                    &synchronized.artifact_id,
                    baseline_commit_id,
                )
                .await?;
            return build_change_list_response(
                "precommit",
                Some(precommit_id.to_string()),
                baseline_commit_id,
                version,
                &base_records,
                records,
                change_type,
                path_prefix,
                cursor.as_deref(),
                page_size,
            );
        }

        let (playground, index) = self
            .load_browse_index(identity, tenant_id, project_id, artifact_id, playground_id)
            .await?;
        let baseline_commit_id = playground.head_commit_id.map(Into::into);
        let base_records = self
            .load_commit_records(
                &playground.tenant_id,
                &playground.project_id,
                &playground.artifact_id,
                baseline_commit_id,
            )
            .await?;
        build_change_list_response(
            "workspace",
            None,
            baseline_commit_id,
            &index.version,
            &base_records,
            &index.records,
            change_type,
            path_prefix,
            cursor.as_deref(),
            page_size,
        )
    }

    async fn load_browse_index(
        &self,
        identity: &AuthenticatedIdentity,
        tenant_id: String,
        project_id: String,
        artifact_id: String,
        playground_id: String,
    ) -> Result<(PlaygroundRecord, neoengramd::PublishedIndex), Error> {
        let tenant_id = parse_tenant(tenant_id)?;
        if !self
            .policy
            .is_allowed(identity.principal(), Permission::PlaygroundRead, &tenant_id)
        {
            return Err(resource_not_found("playground"));
        }
        let project_id = parse_project_id(project_id)?;
        let artifact_id = parse_artifact_id(artifact_id)?;
        let playground_id = parse_playground_id(playground_id)?;
        let playground = self
            .repository
            .get_playground(&tenant_id, &project_id, &artifact_id, &playground_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("playground"))?;
        if playground.state != PlaygroundState::Ready {
            return Err(catalog_conflict(
                "playground_not_ready",
                "PLAYGROUND_NOT_READY",
                "the Playground is not ready for browsing",
            ));
        }
        let index = self
            .indexes
            .published_index(&IndexKey {
                tenant_id,
                project_id,
                artifact_id,
                playground_id,
            })
            .await
            .map_err(map_central_error)?;
        Ok((playground, index))
    }

    fn precommit_repository(&self) -> Result<Arc<dyn PreCommitRepository>, Error> {
        self.precommits.clone().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "precommit_unavailable",
                "PRECOMMIT_UNAVAILABLE",
                "Pre-commit authority is not configured",
                true,
            )
        })
    }

    async fn load_commit_records(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        commit_id: Option<CommitId>,
    ) -> Result<Vec<FileRecord>, Error> {
        let Some(commit_id) = commit_id else {
            return Ok(Vec::new());
        };
        self.precommit_repository()?
            .get_commit(tenant_id, project_id, artifact_id, commit_id)
            .await
            .map_err(map_central_error)?
            .map(|commit| commit.records)
            .ok_or_else(|| {
                catalog_conflict(
                    "workspace_base_diff_unavailable",
                    "WORKSPACE_BASE_DIFF_UNAVAILABLE",
                    "the Head Commit Index is unavailable for this Playground",
                )
            })
    }

    fn precommit_execution(
        &self,
    ) -> Result<(Arc<dyn PreCommitRepository>, Arc<super::JobCoordinator>), Error> {
        let precommits = self.precommit_repository()?;
        let coordinator = self.coordinator.clone().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "precommit_execution_unavailable",
                "PRECOMMIT_EXECUTION_UNAVAILABLE",
                "Pre-commit execution is not configured",
                true,
            )
        })?;
        Ok((precommits, coordinator))
    }

    async fn load_playground(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &PlaygroundId,
    ) -> Result<PlaygroundRecord, Error> {
        self.repository
            .get_playground(tenant_id, project_id, artifact_id, playground_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("playground"))
    }

    async fn load_precommit_playground(
        &self,
        identity: &AuthenticatedIdentity,
        precommit: &PreCommitRecord,
        permission: Permission,
    ) -> Result<PlaygroundRecord, Error> {
        if !self
            .policy
            .is_allowed(identity.principal(), permission, &precommit.tenant_id)
        {
            return Err(resource_not_found("precommit"));
        }
        self.load_playground(
            &precommit.tenant_id,
            &precommit.project_id,
            &precommit.artifact_id,
            &precommit.playground_id,
        )
        .await
        .map_err(|_| resource_not_found("precommit"))
    }

    async fn validate_precommit_source(
        &self,
        playground: &PlaygroundRecord,
        expected: &WireIndexVersion,
    ) -> Result<(), Error> {
        if playground.state != PlaygroundState::Ready {
            return Err(catalog_conflict(
                "playground_not_ready",
                "PLAYGROUND_NOT_READY",
                "only a Ready Playground can start Pre-commit",
            ));
        }
        let current = self
            .indexes
            .current_version(&IndexKey {
                tenant_id: playground.tenant_id.clone(),
                project_id: playground.project_id.clone(),
                artifact_id: playground.artifact_id.clone(),
                playground_id: playground.playground_id.clone(),
            })
            .await
            .map_err(map_central_error)?;
        if !same_index_version(&current, expected) {
            return Err(catalog_conflict(
                "index_version_conflict",
                "INDEX_VERSION_CONFLICT",
                "expected_index_version no longer matches the Playground",
            ));
        }
        Ok(())
    }

    async fn require_tenant(
        &self,
        identity: &AuthenticatedIdentity,
        permission: Permission,
        tenant_id: &TenantId,
    ) -> Result<(), Error> {
        if !self
            .policy
            .is_allowed(identity.principal(), permission, tenant_id)
        {
            return Err(resource_not_found("tenant"));
        }
        self.repository
            .get_tenant(tenant_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("tenant"))?;
        Ok(())
    }

    fn tenant_view(&self, identity: &AuthenticatedIdentity, record: &TenantRecord) -> TenantView {
        TenantView {
            tenant_id: record.tenant_id.to_string(),
            display_name: record.display_name.clone(),
            description: record.description.clone(),
            resource_version: record.resource_version.to_string(),
            created_at_unix_ms: record.created_at_unix_ms.to_string(),
            updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
            permissions: self
                .policy
                .permission_names(identity.principal(), &record.tenant_id),
        }
    }

    async fn playground_view(&self, record: &PlaygroundRecord) -> Result<PlaygroundView, Error> {
        let index_version = self
            .indexes
            .current_version(&IndexKey {
                tenant_id: record.tenant_id.clone(),
                project_id: record.project_id.clone(),
                artifact_id: record.artifact_id.clone(),
                playground_id: record.playground_id.clone(),
            })
            .await
            .map_err(map_central_error)?;
        let active = match &self.precommits {
            Some(precommits) => precommits
                .get_active(
                    &record.tenant_id,
                    &record.project_id,
                    &record.artifact_id,
                    &record.playground_id,
                )
                .await
                .map_err(map_central_error)?,
            None => None,
        };
        Ok(playground_view(record, &index_version, active.as_ref()))
    }

    async fn snapshot_view(&self, record: &SnapshotRecord) -> Result<SnapshotView, Error> {
        let commit = self
            .precommit_repository()?
            .get_commit(
                &record.tenant_id,
                &record.project_id,
                &record.artifact_id,
                CommitId::from_digest(record.commit_id),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        let logical_file_count = commit.records.len().to_string();
        let logical_size_bytes = commit
            .records
            .iter()
            .fold(0_u64, |total, file| total.saturating_add(file.total_size))
            .to_string();
        let (integrity_state, verified_at_unix_ms) = match record.state {
            SnapshotState::Creating => ("pending", None),
            SnapshotState::Ready => ("verified", Some(record.updated_at_unix_ms.to_string())),
            SnapshotState::Abnormal => ("failed", None),
        };
        let verified = record.state == SnapshotState::Ready;
        Ok(SnapshotView {
            snapshot_id: record.snapshot_id.to_string(),
            tenant_id: record.tenant_id.to_string(),
            project_id: record.project_id.to_string(),
            artifact_id: record.artifact_id.to_string(),
            commit_id: record.commit_id.to_string(),
            storage_volume_id: record.storage_volume_id.to_string(),
            region: record.region.clone(),
            message: commit.message,
            tag_names: commit.tag_names,
            state: snapshot_state_name(record.state).to_owned(),
            phase: snapshot_phase_name(record.phase).to_owned(),
            issue: (record.state == SnapshotState::Abnormal).then(|| ResourceIssueSummary {
                code: "SNAPSHOT_MOUNT_ABNORMAL".to_owned(),
                message: "the read-only Snapshot mount is unavailable".to_owned(),
                retryable: true,
                occurred_at_unix_ms: Some(record.updated_at_unix_ms.to_string()),
            }),
            integrity: SnapshotIntegritySummary {
                state: integrity_state.to_owned(),
                files_verified: if verified {
                    logical_file_count.clone()
                } else {
                    "0".to_owned()
                },
                bytes_verified: if verified {
                    logical_size_bytes.clone()
                } else {
                    "0".to_owned()
                },
                verified_at_unix_ms,
            },
            logical_file_count,
            logical_size_bytes,
            created_at_unix_ms: record.created_at_unix_ms.to_string(),
            updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
        })
    }
}

pub(crate) fn storage_volume_view(record: &StorageVolumeRecord) -> StorageVolumeView {
    StorageVolumeView {
        tenant_id: record.tenant_id.to_string(),
        storage_volume_id: record.storage_volume_id.to_string(),
        display_name: record.display_name.clone(),
        edge_cluster_id: record.edge_cluster_id.to_string(),
        region: record.region.clone(),
        backend_type: backend_name(record.backend_type).to_owned(),
        access_mode: access_mode_name(record.access_mode).to_owned(),
        pvc_reference: record.pvc_reference.as_ref().map(|reference| PvcReference {
            namespace: reference.namespace.clone(),
            claim_name: reference.claim_name.clone(),
        }),
        state: volume_state_name(record.state).to_owned(),
        resource_version: record.resource_version.to_string(),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn artifact_view(record: &ArtifactRecord) -> ArtifactView {
    ArtifactView {
        tenant_id: record.tenant_id.to_string(),
        project_id: record.project_id.to_string(),
        artifact_id: record.artifact_id.to_string(),
        display_name: record.display_name.clone(),
        description: record.description.clone(),
        initialization: match &record.initialization {
            CatalogArtifactInitialization::Empty => ArtifactInitializationBody::Empty,
            CatalogArtifactInitialization::Derived {
                source_project_id,
                source_artifact_id,
                source_commit_id,
            } => ArtifactInitializationBody::Derived {
                source_project_id: source_project_id.to_string(),
                source_artifact_id: source_artifact_id.to_string(),
                source_commit_id: source_commit_id.to_string(),
            },
        },
        head_commit_id: record.head_commit_id.map(|id| id.to_string()),
        resource_version: record.resource_version.to_string(),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn commit_node_view(record: &CommitRecord) -> CommitNodeView {
    CommitNodeView {
        commit_id: record.commit_id.to_string(),
        parent_commit_id: record.parent_commit_id.map(|id| id.to_string()),
        message: record.message.clone(),
        description: record.description.clone(),
        tag_names: record.tag_names.clone(),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
    }
}

fn playground_view(
    record: &PlaygroundRecord,
    index_version: &WireIndexVersion,
    active_precommit: Option<&PreCommitRecord>,
) -> PlaygroundView {
    PlaygroundView {
        tenant_id: record.tenant_id.to_string(),
        project_id: record.project_id.to_string(),
        artifact_id: record.artifact_id.to_string(),
        playground_id: record.playground_id.to_string(),
        storage_volume_id: record.storage_volume_id.to_string(),
        region: record.region.clone(),
        display_name: record.display_name.clone(),
        base_commit_id: record.base_commit_id.map(|id| id.to_string()),
        head_commit_id: record.head_commit_id.map(|id| id.to_string()),
        index_version: IndexVersionBody {
            revision: index_version.revision.to_string(),
            digest: index_version.digest.to_string(),
        },
        state: playground_state_name(record.state).to_owned(),
        active_precommit_id: active_precommit.map(|record| record.precommit_id.to_string()),
        issue: None,
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

pub(crate) fn precommit_view(record: &PreCommitRecord) -> PreCommitView {
    PreCommitView {
        tenant_id: record.tenant_id.to_string(),
        project_id: record.project_id.to_string(),
        artifact_id: record.artifact_id.to_string(),
        playground_id: record.playground_id.to_string(),
        precommit_id: record.precommit_id.to_string(),
        precommit_request_id: record.precommit_request_id.to_string(),
        attempt: record.attempt,
        state: precommit_state_name(record.state).to_owned(),
        phase: precommit_phase_name(record.phase).to_owned(),
        progress: PreCommitProgressView {
            percent: record.progress.percent,
            files_completed: record.progress.files_completed.to_string(),
            files_total: record.progress.files_total.map(|value| value.to_string()),
            bytes_completed: record.progress.bytes_completed.to_string(),
            bytes_total: record.progress.bytes_total.map(|value| value.to_string()),
        },
        checks: record
            .checks
            .iter()
            .map(|check| PreCommitCheckView {
                check_id: check.check_id.to_string(),
                status: match check.status {
                    neoengramd::PreCommitCheckStatus::Pending => "pending",
                    neoengramd::PreCommitCheckStatus::Passed => "passed",
                    neoengramd::PreCommitCheckStatus::Warning => "warning",
                    neoengramd::PreCommitCheckStatus::Failed => "failed",
                }
                .to_owned(),
                summary: check.summary.clone(),
            })
            .collect(),
        warnings: record.warnings.iter().map(precommit_notice_view).collect(),
        blockers: record.blockers.iter().map(precommit_notice_view).collect(),
        source_index_version: index_version_body(&record.source_index_version),
        candidate_index_version: record
            .candidate_index_version
            .as_ref()
            .map(index_version_body),
        diff_summary: record
            .diff_summary
            .as_ref()
            .map(|summary| PreCommitDiffSummaryView {
                files_added: summary.files_added.to_string(),
                files_modified: summary.files_modified.to_string(),
                files_deleted: summary.files_deleted.to_string(),
                files_renamed: summary.files_renamed.to_string(),
                bytes_added: summary.bytes_added.to_string(),
                bytes_removed: summary.bytes_removed.to_string(),
            }),
        issue: record.issue.as_ref().map(|issue| ResourceIssueSummary {
            code: issue.code.clone(),
            message: issue.message.clone(),
            retryable: record.state == PreCommitState::Abnormal,
            occurred_at_unix_ms: Some(record.updated_at_unix_ms.to_string()),
        }),
        committed_commit_id: record.committed_commit_id.map(|id| id.to_string()),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn precommit_notice_view(notice: &neoengramd::PreCommitNotice) -> PreCommitNoticeView {
    PreCommitNoticeView {
        code: notice.code.clone(),
        message: notice.message.clone(),
        path: notice.path.clone(),
    }
}

const fn precommit_state_name(state: PreCommitState) -> &'static str {
    match state {
        PreCommitState::Running => "running",
        PreCommitState::Ready => "ready",
        PreCommitState::Abnormal => "abnormal",
        PreCommitState::Cancelled => "cancelled",
        PreCommitState::Committed => "committed",
    }
}

const fn precommit_phase_name(phase: PreCommitPhase) -> &'static str {
    match phase {
        PreCommitPhase::Queued => "queued",
        PreCommitPhase::Scanning => "scanning",
        PreCommitPhase::Hashing => "hashing",
        PreCommitPhase::Uploading => "uploading",
        PreCommitPhase::Validating => "validating",
        PreCommitPhase::Idle => "idle",
    }
}

fn parse_index_version(body: IndexVersionBody) -> Result<WireIndexVersion, Error> {
    let revision = parse_canonical_u64(&body.revision).map_err(|_| {
        invalid_request("expected_index_version.revision must be a canonical unsigned integer")
    })?;
    let digest = ContentDigest::from_str(&body.digest).map_err(|_| {
        invalid_request("expected_index_version.digest must be a BLAKE3 hex digest")
    })?;
    Ok(WireIndexVersion {
        revision: IndexRevision::new(revision),
        digest,
        extensions: Default::default(),
    })
}

fn deterministic_precommit_id(
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> Result<PreCommitId, Error> {
    let input = format!("{}\0{}", tenant_id, request_id);
    PreCommitId::new(format!(
        "precommit-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(|_| internal_catalog_error())
}

fn deterministic_snapshot_id(
    tenant_id: &TenantId,
    request_id: &RequestId,
) -> Result<SnapshotId, Error> {
    let input = format!("{}\0{}", tenant_id, request_id);
    SnapshotId::new(format!(
        "snapshot-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(|_| internal_catalog_error())
}

fn deterministic_precommit_job_id(
    tenant_id: &TenantId,
    precommit_id: &PreCommitId,
    attempt: u32,
) -> Result<JobId, Error> {
    let input = format!("{}\0{}\0{}", tenant_id, precommit_id, attempt);
    JobId::new(format!(
        "precommit-{}",
        blake3::hash(input.as_bytes()).to_hex()
    ))
    .map_err(|_| internal_catalog_error())
}

fn same_index_version(left: &WireIndexVersion, right: &WireIndexVersion) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn split_outcome<T>(outcome: CatalogInsertOutcome<T>) -> (T, bool) {
    match outcome {
        CatalogInsertOutcome::Inserted(record) => (record, false),
        CatalogInsertOutcome::Existing(record) => (record, true),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CursorScope {
    Tenant {
        visible: Option<Vec<String>>,
        query: Option<String>,
    },
    Volume {
        tenant_id: String,
        region: Option<String>,
        backend_type: Option<String>,
        query: Option<String>,
    },
    Artifact {
        tenant_id: String,
        project_id: Option<String>,
        query: Option<String>,
    },
    CommitGraph {
        tenant_id: String,
        project_id: String,
        artifact_id: String,
        graph_version: String,
        head_commit_id: Option<String>,
    },
    Playground {
        tenant_id: String,
        project_id: Option<String>,
        artifact_id: Option<String>,
        region: Option<String>,
        state: Option<String>,
        query: Option<String>,
    },
    Snapshot {
        tenant_id: String,
        project_id: Option<String>,
        artifact_id: Option<String>,
        commit_id: Option<String>,
        storage_volume_id: Option<String>,
        region: Option<String>,
        state: Option<String>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorDocument {
    version: u8,
    scope_digest: String,
    created_at_unix_ms: String,
    keys: Vec<String>,
}

fn encode_tenant_cursor(scope: &CursorScope, cursor: &TenantListCursor) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.tenant_id.to_string()],
    )
}

fn decode_tenant_cursor(encoded: &str, scope: &CursorScope) -> Result<TenantListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(TenantListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        tenant_id: TenantId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
    })
}

fn encode_volume_cursor(
    scope: &CursorScope,
    cursor: &StorageVolumeListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.storage_volume_id.to_string()],
    )
}

fn decode_volume_cursor(
    encoded: &str,
    scope: &CursorScope,
) -> Result<StorageVolumeListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(StorageVolumeListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        storage_volume_id: StorageVolumeId::new(cursor.keys[0].clone())
            .map_err(|_| cursor_conflict())?,
    })
}

fn encode_artifact_cursor(
    scope: &CursorScope,
    cursor: &ArtifactListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![
            cursor.project_id.to_string(),
            cursor.artifact_id.to_string(),
        ],
    )
}

fn decode_artifact_cursor(encoded: &str, scope: &CursorScope) -> Result<ArtifactListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 2)?;
    Ok(ArtifactListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        project_id: ProjectId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
        artifact_id: ArtifactId::new(cursor.keys[1].clone()).map_err(|_| cursor_conflict())?,
    })
}

#[derive(Debug, Clone, Copy)]
struct CommitGraphCursor {
    created_at_unix_ms: UnixMillis,
    commit_id: CommitId,
}

fn encode_commit_cursor(scope: &CursorScope, cursor: &CommitGraphCursor) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.commit_id.to_string()],
    )
}

fn decode_commit_cursor(encoded: &str, scope: &CursorScope) -> Result<CommitGraphCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(CommitGraphCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        commit_id: CommitId::from_str(&cursor.keys[0]).map_err(|_| cursor_conflict())?,
    })
}

fn encode_playground_cursor(
    scope: &CursorScope,
    cursor: &PlaygroundListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![
            cursor.project_id.to_string(),
            cursor.artifact_id.to_string(),
            cursor.playground_id.to_string(),
        ],
    )
}

fn decode_playground_cursor(
    encoded: &str,
    scope: &CursorScope,
) -> Result<PlaygroundListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 3)?;
    Ok(PlaygroundListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        project_id: ProjectId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
        artifact_id: ArtifactId::new(cursor.keys[1].clone()).map_err(|_| cursor_conflict())?,
        playground_id: PlaygroundId::new(cursor.keys[2].clone()).map_err(|_| cursor_conflict())?,
    })
}

fn encode_snapshot_cursor(
    scope: &CursorScope,
    cursor: &SnapshotListCursor,
) -> Result<String, Error> {
    encode_cursor(
        scope,
        cursor.created_at_unix_ms,
        vec![cursor.snapshot_id.to_string()],
    )
}

fn decode_snapshot_cursor(encoded: &str, scope: &CursorScope) -> Result<SnapshotListCursor, Error> {
    let cursor = decode_cursor(encoded, scope, 1)?;
    Ok(SnapshotListCursor {
        created_at_unix_ms: cursor.created_at_unix_ms,
        snapshot_id: SnapshotId::new(cursor.keys[0].clone()).map_err(|_| cursor_conflict())?,
    })
}

struct DecodedCursor {
    created_at_unix_ms: UnixMillis,
    keys: Vec<String>,
}

fn encode_cursor(
    scope: &CursorScope,
    created_at_unix_ms: UnixMillis,
    keys: Vec<String>,
) -> Result<String, Error> {
    let document = CursorDocument {
        version: 1,
        scope_digest: scope_digest(scope)?,
        created_at_unix_ms: created_at_unix_ms.to_string(),
        keys,
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| internal_catalog_error())?;
    Ok(format!("{CURSOR_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn decode_cursor(
    encoded: &str,
    scope: &CursorScope,
    key_count: usize,
) -> Result<DecodedCursor, Error> {
    if encoded.len() > 2_048 {
        return Err(cursor_conflict());
    }
    let bytes = encoded
        .strip_prefix(CURSOR_PREFIX)
        .ok_or_else(cursor_conflict)
        .and_then(|value| {
            URL_SAFE_NO_PAD
                .decode(value.as_bytes())
                .map_err(|_| cursor_conflict())
        })?;
    let document: CursorDocument = serde_json::from_slice(&bytes).map_err(|_| cursor_conflict())?;
    if document.version != 1
        || document.scope_digest != scope_digest(scope)?
        || document.keys.len() != key_count
    {
        return Err(cursor_conflict());
    }
    let created_at_unix_ms = parse_canonical_u64(&document.created_at_unix_ms)
        .map(UnixMillis::new)
        .map_err(|_| cursor_conflict())?;
    Ok(DecodedCursor {
        created_at_unix_ms,
        keys: document.keys,
    })
}

fn scope_digest(scope: &CursorScope) -> Result<String, Error> {
    serde_json::to_vec(scope)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|_| internal_catalog_error())
}

fn parse_tenant(value: String) -> Result<TenantId, Error> {
    TenantId::new(value).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}

fn parse_project_id(value: String) -> Result<ProjectId, Error> {
    ProjectId::new(value).map_err(|error| invalid_request(format!("project_id: {error}")))
}

fn parse_artifact_id(value: String) -> Result<ArtifactId, Error> {
    ArtifactId::new(value).map_err(|error| invalid_request(format!("artifact_id: {error}")))
}

fn parse_playground_id(value: String) -> Result<PlaygroundId, Error> {
    PlaygroundId::new(value).map_err(|error| invalid_request(format!("playground_id: {error}")))
}

fn parse_snapshot_id(value: String) -> Result<SnapshotId, Error> {
    SnapshotId::new(value).map_err(|error| invalid_request(format!("snapshot_id: {error}")))
}

fn parse_volume_id(value: String) -> Result<StorageVolumeId, Error> {
    StorageVolumeId::new(value)
        .map_err(|error| invalid_request(format!("storage_volume_id: {error}")))
}

fn parse_backend_type(value: &str) -> Result<StorageBackendType, Error> {
    match value {
        "pvc" => Ok(StorageBackendType::Pvc),
        "nfs" => Ok(StorageBackendType::Nfs),
        _ => Err(invalid_request("backend_type must be pvc or nfs")),
    }
}

fn parse_access_mode(value: &str) -> Result<StorageAccessMode, Error> {
    match value {
        "read_write_once" => Ok(StorageAccessMode::ReadWriteOnce),
        "read_write_many" => Ok(StorageAccessMode::ReadWriteMany),
        "read_only_many" => Ok(StorageAccessMode::ReadOnlyMany),
        _ => Err(invalid_request(
            "access_mode must be read_write_once, read_write_many, or read_only_many",
        )),
    }
}

fn parse_playground_state(value: &str) -> Result<PlaygroundState, Error> {
    match value {
        "creating" => Ok(PlaygroundState::Creating),
        "ready" => Ok(PlaygroundState::Ready),
        "abnormal" => Ok(PlaygroundState::Abnormal),
        _ => Err(invalid_request(
            "state must be creating, ready, or abnormal",
        )),
    }
}

fn parse_snapshot_state(value: &str) -> Result<SnapshotState, Error> {
    match value {
        "creating" => Ok(SnapshotState::Creating),
        "ready" => Ok(SnapshotState::Ready),
        "abnormal" => Ok(SnapshotState::Abnormal),
        _ => Err(invalid_request(
            "state must be creating, ready, or abnormal",
        )),
    }
}

fn backend_name(value: StorageBackendType) -> &'static str {
    match value {
        StorageBackendType::Pvc => "pvc",
        StorageBackendType::Nfs => "nfs",
    }
}

fn access_mode_name(value: StorageAccessMode) -> &'static str {
    match value {
        StorageAccessMode::ReadWriteOnce => "read_write_once",
        StorageAccessMode::ReadWriteMany => "read_write_many",
        StorageAccessMode::ReadOnlyMany => "read_only_many",
    }
}

fn volume_state_name(value: StorageVolumeState) -> &'static str {
    match value {
        StorageVolumeState::Ready => "ready",
        StorageVolumeState::Degraded => "degraded",
        StorageVolumeState::Unavailable => "unavailable",
    }
}

fn playground_state_name(value: PlaygroundState) -> &'static str {
    match value {
        PlaygroundState::Creating => "creating",
        PlaygroundState::Ready => "ready",
        PlaygroundState::Abnormal => "abnormal",
    }
}

fn snapshot_state_name(value: SnapshotState) -> &'static str {
    match value {
        SnapshotState::Creating => "creating",
        SnapshotState::Ready => "ready",
        SnapshotState::Abnormal => "abnormal",
    }
}

fn snapshot_phase_name(value: SnapshotPhase) -> &'static str {
    match value {
        SnapshotPhase::Planning => "planning",
        SnapshotPhase::Materializing => "materializing",
        SnapshotPhase::Verifying => "verifying",
        SnapshotPhase::Idle => "idle",
    }
}

const BROWSE_CURSOR_PREFIX: &str = "ngbrowse_v1_";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BrowseCursorScope {
    kind: &'static str,
    source_id: Option<String>,
    baseline_commit_id: Option<String>,
    index_revision: String,
    index_digest: String,
    path_prefix: Option<String>,
    format: Option<String>,
    change_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowseCursorDocument {
    version: u8,
    scope_digest: String,
    path: String,
}

fn index_version_body(version: &WireIndexVersion) -> IndexVersionBody {
    IndexVersionBody {
        revision: version.revision.to_string(),
        digest: version.digest.to_string(),
    }
}

fn parse_logical_path(field: &'static str, value: String) -> Result<LogicalPath, Error> {
    LogicalPath::parse(value).map_err(|error| invalid_request(format!("{field}: {error}")))
}

fn validate_file_format(value: String) -> Result<String, Error> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(invalid_request("format is invalid"));
    }
    Ok(value.to_ascii_lowercase())
}

fn validate_change_type(value: String) -> Result<String, Error> {
    match value.as_str() {
        "added" | "modified" | "deleted" | "renamed" => Ok(value),
        _ => Err(invalid_request(
            "change_type must be added, modified, deleted, or renamed",
        )),
    }
}

fn file_format(path: &str) -> String {
    path.rsplit_once('.')
        .filter(|(_, extension)| !extension.is_empty() && !extension.contains('/'))
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn media_type(format: &str) -> Option<&'static str> {
    match format {
        "json" => Some("application/json"),
        "csv" => Some("text/csv"),
        "parquet" => Some("application/vnd.apache.parquet"),
        "txt" => Some("text/plain"),
        "yaml" | "yml" => Some("application/yaml"),
        "xml" => Some("application/xml"),
        _ => None,
    }
}

fn path_matches_prefix(path: &LogicalPath, prefix: Option<&LogicalPath>) -> bool {
    prefix.is_none_or(|prefix| path == prefix || prefix.is_ancestor_of(path))
}

#[allow(clippy::too_many_arguments)]
fn build_change_list_response(
    source: &'static str,
    precommit_id: Option<String>,
    baseline_commit_id: Option<CommitId>,
    version: &WireIndexVersion,
    base_records: &[FileRecord],
    candidate_records: &[FileRecord],
    change_type: Option<String>,
    path_prefix: Option<LogicalPath>,
    cursor: Option<&str>,
    page_size: u16,
) -> Result<QueryPlaygroundChangeListResponse, Error> {
    let scope = BrowseCursorScope {
        kind: if source == "precommit" {
            "precommit_changes"
        } else {
            "changes"
        },
        source_id: precommit_id.clone(),
        baseline_commit_id: baseline_commit_id.map(|commit_id| commit_id.to_string()),
        index_revision: version.revision.to_string(),
        index_digest: version.digest.to_string(),
        path_prefix: path_prefix.as_ref().map(ToString::to_string),
        format: None,
        change_type: change_type.clone(),
    };
    let after = cursor
        .map(|cursor| decode_browse_cursor(cursor, &scope))
        .transpose()?;
    let diff = diff_index_snapshots(base_records, candidate_records).map_err(map_central_error)?;
    let matching_changes = diff
        .changes
        .into_iter()
        .filter(|change| path_matches_prefix(&change.path, path_prefix.as_ref()))
        .collect::<Vec<_>>();
    let summary = summarize_index_changes(&matching_changes).map_err(map_central_error)?;
    let mut entries = matching_changes
        .into_iter()
        .filter(|change| {
            change_type
                .as_deref()
                .is_none_or(|requested| requested == index_change_kind_name(change.kind))
        })
        .map(|change| PlaygroundChangeEntry {
            change_type: index_change_kind_name(change.kind).to_owned(),
            format: Some(file_format(change.path.as_str())),
            path: change.path.to_string(),
            previous_path: change.previous_path.map(|path| path.to_string()),
            old_size_bytes: change.old_size.map(|size| size.to_string()),
            new_size_bytes: change.new_size.map(|size| size.to_string()),
        })
        .collect::<Vec<_>>();
    if let Some(after) = after {
        entries.retain(|entry| entry.path > after);
    }
    let (items, next_cursor) = take_change_page(entries, page_size, &scope)?;
    Ok(QueryPlaygroundChangeListResponse {
        source: source.to_owned(),
        precommit_id,
        index_version: index_version_body(version),
        summary: PlaygroundChangeSummary {
            files_added: summary.files_added.to_string(),
            files_modified: summary.files_modified.to_string(),
            files_deleted: summary.files_deleted.to_string(),
            files_renamed: summary.files_renamed.to_string(),
            bytes_added: summary.bytes_added.to_string(),
            bytes_removed: summary.bytes_removed.to_string(),
        },
        items,
        next_cursor,
    })
}

const fn index_change_kind_name(kind: IndexSnapshotChangeKind) -> &'static str {
    match kind {
        IndexSnapshotChangeKind::Added => "added",
        IndexSnapshotChangeKind::Modified => "modified",
        IndexSnapshotChangeKind::Deleted => "deleted",
        IndexSnapshotChangeKind::Renamed => "renamed",
    }
}

fn logical_entries(
    records: &[FileRecord],
    prefix: Option<&LogicalPath>,
    format: Option<&str>,
) -> Vec<LogicalFileEntry> {
    let matching = records
        .iter()
        .filter(|record| path_matches_prefix(&record.path, prefix))
        .filter(|record| format.is_none_or(|wanted| file_format(record.path.as_str()) == wanted));
    let mut files = BTreeMap::<String, LogicalFileEntry>::new();
    for record in matching {
        files.insert(
            record.path.to_string(),
            LogicalFileEntry {
                path: record.path.to_string(),
                entry_type: "file".to_owned(),
                size_bytes: Some(record.total_size.to_string()),
                format: Some(file_format(record.path.as_str())),
                row_count: None,
                updated_at_unix_ms: None,
            },
        );
        let mut current = record.path.parent();
        while let Some(directory) = current {
            if prefix.is_none_or(|wanted| directory == *wanted || wanted.is_ancestor_of(&directory))
            {
                files
                    .entry(directory.to_string())
                    .or_insert_with(|| LogicalFileEntry {
                        path: directory.to_string(),
                        entry_type: "directory".to_owned(),
                        size_bytes: None,
                        format: None,
                        row_count: None,
                        updated_at_unix_ms: None,
                    });
            }
            current = directory.parent();
        }
    }
    files.into_values().collect()
}

fn take_browse_page(
    mut entries: Vec<LogicalFileEntry>,
    page_size: u16,
    scope: &BrowseCursorScope,
) -> Result<(Vec<LogicalFileEntry>, Option<String>), Error> {
    let limit = usize::from(page_size);
    let next = entries.len() > limit;
    if next {
        entries.truncate(limit);
    }
    let next_cursor = next
        .then(|| {
            entries
                .last()
                .map(|entry| encode_browse_cursor(scope, &entry.path))
        })
        .flatten()
        .transpose()?;
    Ok((entries, next_cursor))
}

fn take_change_page(
    mut entries: Vec<PlaygroundChangeEntry>,
    page_size: u16,
    scope: &BrowseCursorScope,
) -> Result<(Vec<PlaygroundChangeEntry>, Option<String>), Error> {
    let limit = usize::from(page_size);
    let next = entries.len() > limit;
    if next {
        entries.truncate(limit);
    }
    let next_cursor = next
        .then(|| {
            entries
                .last()
                .map(|entry| encode_browse_cursor(scope, &entry.path))
        })
        .flatten()
        .transpose()?;
    Ok((entries, next_cursor))
}

fn encode_browse_cursor(scope: &BrowseCursorScope, path: &str) -> Result<String, Error> {
    let document = BrowseCursorDocument {
        version: 1,
        scope_digest: browse_scope_digest(scope)?,
        path: path.to_owned(),
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| internal_catalog_error())?;
    Ok(format!(
        "{BROWSE_CURSOR_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(bytes)
    ))
}

fn decode_browse_cursor(encoded: &str, scope: &BrowseCursorScope) -> Result<String, Error> {
    if encoded.len() > 2_048 {
        return Err(cursor_conflict());
    }
    let value = encoded
        .strip_prefix(BROWSE_CURSOR_PREFIX)
        .ok_or_else(cursor_conflict)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|_| cursor_conflict())?;
    let document: BrowseCursorDocument =
        serde_json::from_slice(&bytes).map_err(|_| cursor_conflict())?;
    if document.version != 1 || document.scope_digest != browse_scope_digest(scope)? {
        return Err(cursor_conflict());
    }
    LogicalPath::parse(document.path.clone()).map_err(|_| cursor_conflict())?;
    Ok(document.path)
}

fn browse_scope_digest(scope: &BrowseCursorScope) -> Result<String, Error> {
    serde_json::to_vec(scope)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|_| internal_catalog_error())
}

fn validate_display_name(value: String) -> Result<String, Error> {
    validate_text("display_name", value, 1, 128)
}

fn validate_description(value: String) -> Result<String, Error> {
    validate_text("description", value, 0, 2_048)
}

fn validate_region(value: String) -> Result<String, Error> {
    let mut bytes = value.bytes();
    if value.len() <= 64
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Ok(value)
    } else {
        Err(invalid_request("region is invalid"))
    }
}

fn validate_query(value: String) -> Result<String, Error> {
    validate_text("query", value, 1, MAX_QUERY_CHARS)
}

fn validate_nfs_server(value: String) -> Result<String, Error> {
    validate_text("nfs_reference.server", value, 1, 253)
}

fn validate_nfs_export_path(value: String) -> Result<String, Error> {
    if value.starts_with('/') && value.len() <= 1_024 && !value.contains('\0') {
        Ok(value)
    } else {
        Err(invalid_request(
            "nfs_reference.export_path must be an absolute NFS export path",
        ))
    }
}

fn validate_text(
    field: &'static str,
    value: String,
    min_chars: usize,
    max_chars: usize,
) -> Result<String, Error> {
    let count = value.chars().count();
    if (min_chars..=max_chars).contains(&count) && !value.chars().any(char::is_control) {
        Ok(value)
    } else {
        Err(invalid_request(format!(
            "{field} must contain {min_chars} to {max_chars} non-control characters"
        )))
    }
}

fn page_size(value: Option<u16>) -> Result<u16, Error> {
    let value = value.unwrap_or(DEFAULT_PAGE_SIZE);
    if (1..=MAX_PAGE_SIZE).contains(&value) {
        Ok(value)
    } else {
        Err(invalid_request("page_size must be between 1 and 100"))
    }
}

fn parse_canonical_u64(value: &str) -> Result<u64, ()> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(());
    }
    value.parse().map_err(|_| ())
}

fn reject_reserved(
    extensions: &crate::dto::JsonExtensions,
    reserved: &[&str],
) -> Result<(), Error> {
    if let Some(field) = extensions.first_reserved(reserved) {
        Err(invalid_request(format!(
            "request body must not contain server-owned field {field:?}"
        )))
    } else {
        Ok(())
    }
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

fn cursor_conflict() -> Error {
    catalog_conflict(
        "cursor_scope_conflict",
        "CURSOR_SCOPE_CONFLICT",
        "cursor is invalid or belongs to another catalog query",
    )
}

fn catalog_conflict(code: &'static str, neo_code: &'static str, message: &'static str) -> Error {
    application_error(ErrorCategory::Conflict, code, neo_code, message, false)
}

fn catalog_mutation_error(error: neoengramd::CentralError, reused_code: &'static str) -> Error {
    match error.code() {
        neoengramd::CentralErrorCode::InvalidState => catalog_conflict(
            reused_code,
            match reused_code {
                "tenant_id_reused" => "TENANT_ID_REUSED",
                "artifact_id_reused" => "ARTIFACT_ID_REUSED",
                "storage_volume_id_reused" => "STORAGE_VOLUME_ID_REUSED",
                "playground_id_reused" => "PLAYGROUND_ID_REUSED",
                _ => "MUTATION_CONFLICT",
            },
            "resource ID is already bound to another create request",
        ),
        neoengramd::CentralErrorCode::VolumeOwnerConflict => catalog_conflict(
            "storage_volume_binding_conflict",
            "STORAGE_VOLUME_BINDING_CONFLICT",
            "StorageVolume or PVC identity is already registered",
        ),
        _ => map_central_error(error),
    }
}

fn playground_mutation_error(error: neoengramd::CentralError) -> Error {
    match error.code() {
        neoengramd::CentralErrorCode::ArtifactNotFound => resource_not_found("artifact"),
        neoengramd::CentralErrorCode::StorageVolumeNotFound => resource_not_found("storage volume"),
        neoengramd::CentralErrorCode::ArtifactHeadMismatch if error.retryable() => {
            application_error(
                ErrorCategory::Conflict,
                "artifact_head_changed",
                "ARTIFACT_HEAD_MISMATCH",
                "the Artifact Head changed before Playground creation; retry the request",
                true,
            )
        }
        neoengramd::CentralErrorCode::ArtifactHeadMismatch => catalog_conflict(
            "artifact_base_commit_mismatch",
            "ARTIFACT_BASE_COMMIT_MISMATCH",
            "the Playground base Commit must match its initial head Commit",
        ),
        neoengramd::CentralErrorCode::StorageVolumeNotReady => catalog_conflict(
            "storage_volume_not_ready",
            "STORAGE_VOLUME_NOT_READY",
            "the selected StorageVolume is not ready for placement",
        ),
        neoengramd::CentralErrorCode::StorageVolumeRegionMismatch => catalog_conflict(
            "storage_volume_region_mismatch",
            "STORAGE_VOLUME_REGION_MISMATCH",
            "the selected StorageVolume region changed before placement",
        ),
        _ => catalog_mutation_error(error, "playground_id_reused"),
    }
}

fn snapshot_mutation_error(error: neoengramd::CentralError) -> Error {
    match error.code() {
        neoengramd::CentralErrorCode::ArtifactNotFound => resource_not_found("artifact"),
        neoengramd::CentralErrorCode::StorageVolumeNotFound => resource_not_found("storage volume"),
        neoengramd::CentralErrorCode::ArtifactHeadMismatch if error.retryable() => {
            application_error(
                ErrorCategory::Conflict,
                "artifact_head_changed",
                "ARTIFACT_HEAD_MISMATCH",
                "the Artifact Head changed before Snapshot creation; retry the request",
                true,
            )
        }
        neoengramd::CentralErrorCode::StorageVolumeNotReady => catalog_conflict(
            "storage_volume_not_ready",
            "STORAGE_VOLUME_NOT_READY",
            "the selected StorageVolume is not ready for Snapshot placement",
        ),
        neoengramd::CentralErrorCode::StorageVolumeRegionMismatch => catalog_conflict(
            "storage_volume_region_mismatch",
            "STORAGE_VOLUME_REGION_MISMATCH",
            "the selected StorageVolume region changed before Snapshot placement",
        ),
        _ => catalog_mutation_error(error, "snapshot_request_id_reused"),
    }
}

fn precommit_mutation_error(error: neoengramd::CentralError) -> Error {
    match error.code() {
        neoengramd::CentralErrorCode::ConcurrentUpdate
        | neoengramd::CentralErrorCode::InvalidState
        | neoengramd::CentralErrorCode::JobIdReused => precommit_conflict(
            "the Pre-commit mutation conflicts with its current state or idempotency identity",
        ),
        neoengramd::CentralErrorCode::JobNotFound => resource_not_found("precommit"),
        _ => map_central_error(error),
    }
}

fn precommit_conflict(message: impl Into<String>) -> Error {
    application_error(
        ErrorCategory::Conflict,
        "precommit_conflict",
        "PRECOMMIT_CONFLICT",
        message,
        false,
    )
}

fn internal_catalog_error() -> Error {
    application_error(
        ErrorCategory::Internal,
        "catalog_internal",
        "INTERNAL",
        "the server could not complete the catalog operation",
        false,
    )
}
