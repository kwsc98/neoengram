use std::{str::FromStr, sync::Arc};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use fusen_rs::{Error, ErrorCategory};
use neoengram_core::{ContentDigest, IndexVersion};
use neoengram_protocol::{
    ArtifactId, EdgeClusterId, PlaygroundId, ProjectId, PvcIdentityDigest, StorageVolumeId,
    TenantId, UnixMillis, WireIndexVersion,
};
use neoengramd::{
    CatalogInsertOutcome, CatalogNfsReference, CatalogPvcReference, Clock,
    ControlCatalogRepository, IndexKey, IndexPublisher, PlaygroundListCursor,
    PlaygroundListRequest, PlaygroundRecord, PlaygroundState, StorageAccessMode,
    StorageBackendType, StorageVolumeListCursor, StorageVolumeListRequest, StorageVolumeRecord,
    StorageVolumeState, TenantListCursor, TenantListRequest, TenantRecord,
};
use serde::{Deserialize, Serialize};

use crate::{
    dto::{
        CreatePlaygroundRequest, CreatePlaygroundResponse, CreateStorageVolumeRequest,
        CreateStorageVolumeResponse, CreateTenantRequest, CreateTenantResponse, IndexVersionBody,
        PlaygroundView, PvcReference, QueryPlaygroundListRequest, QueryPlaygroundListResponse,
        QueryPlaygroundRequest, QueryPlaygroundResponse, QueryStorageVolumeListRequest,
        QueryStorageVolumeListResponse, QueryStorageVolumeRequest, QueryStorageVolumeResponse,
        QueryTenantListRequest, QueryTenantListResponse, QueryTenantRequest, QueryTenantResponse,
        StorageVolumeView, TenantView,
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

/// Public Tenant, StorageVolume, and minimal Playground application service.
pub struct CatalogService {
    repository: Arc<dyn ControlCatalogRepository>,
    indexes: Arc<dyn IndexPublisher>,
    policy: Arc<StaticRbacPolicy>,
    clock: Arc<dyn Clock>,
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
        }
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
        let base_commit_id = request
            .base_commit_id
            .map(|value| {
                ContentDigest::from_str(&value)
                    .map_err(|_| invalid_request("base_commit_id must be a 64-character digest"))
            })
            .transpose()?;
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
        let relative_root = format!(
            "playgrounds/{}/{}/{}",
            project_id, artifact_id, playground_id
        );
        let initial_index = IndexVersion::from_snapshot(0, &[])
            .map(WireIndexVersion::from)
            .map_err(|_| internal_catalog_error())?;
        let now = self.clock.now();
        let outcome = self
            .repository
            .insert_playground(PlaygroundRecord {
                tenant_id,
                project_id,
                artifact_id,
                playground_id,
                storage_volume_id,
                region: volume.region,
                display_name,
                base_commit_id,
                head_commit_id: base_commit_id,
                index_version: initial_index,
                state: PlaygroundState::Ready,
                relative_root,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| catalog_mutation_error(error, "playground_id_reused"))?;
        let (record, replayed) = split_outcome(outcome);
        Ok(CreatePlaygroundResponse {
            playground: self.playground_view(&record).await?,
            replayed,
        })
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
        Ok(playground_view(record, &index_version))
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

fn playground_view(record: &PlaygroundRecord, index_version: &WireIndexVersion) -> PlaygroundView {
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
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
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
    Playground {
        tenant_id: String,
        project_id: Option<String>,
        artifact_id: Option<String>,
        region: Option<String>,
        state: Option<String>,
        query: Option<String>,
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

fn internal_catalog_error() -> Error {
    application_error(
        ErrorCategory::Internal,
        "catalog_internal",
        "INTERNAL",
        "the server could not complete the catalog operation",
        false,
    )
}
