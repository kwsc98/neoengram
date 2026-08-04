use std::sync::Arc;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use fusen_rs::{Error, ErrorCategory};
use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentBootstrapProbe, AgentEnrollmentApprovalRequest, AgentEnrollmentDecision,
    AgentEnrollmentId, AgentEnrollmentTokenCreateRequest, AgentEnrollmentTokenId, AgentId,
    AgentMountId, EdgeClusterId, Extensions, MountAccessMode, PvcIdentityDigest, RequestId,
    ResourceHealth, ResourceVersion, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId,
    PROTOCOL_VERSION_V1,
};
use neoengramd::{
    AgentEnrollmentListCursor, AgentEnrollmentListRequest, AgentProofOfPossessionStatus,
    AgentRegistryRecord, AgentRegistryService, BootstrapTokenMetadata, CentralError,
    CentralErrorCode, Clock, ControlCatalogRepository, CreateStorageEnrollmentIntentRequest,
    FrozenPvcReference, FrozenStorageDescriptor, StorageEnrollmentAccessMode,
    StorageEnrollmentRegistrationKind, StorageEnrollmentState,
};
use serde::{Deserialize, Serialize};

use crate::{
    dto::{
        ApproveStorageEnrollmentRequest, ApproveStorageEnrollmentResponse,
        CreateStorageEnrollmentTokenRequest, CreateStorageEnrollmentTokenResponse, PvcReference,
        QueryStorageEnrollmentListRequest, QueryStorageEnrollmentListResponse,
        QueryStorageEnrollmentRequest, QueryStorageEnrollmentResponse,
        RejectStorageEnrollmentRequest, RejectStorageEnrollmentResponse,
        StorageEnrollmentProbeSummary, StorageEnrollmentView,
    },
    error::{application_error, invalid_request, map_central_error, storage_enrollment_not_found},
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy},
};

use super::{catalog::storage_volume_view, EnrollmentKeyring};

const TOKEN_TTL_MS: u64 = 15 * 60 * 1_000;
const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 100;
const CURSOR_DOMAIN: &[u8] = b"neoengram-storage-enrollment-cursor-v1\0";
const CURSOR_PREFIX: &str = "ngcur_v1_";

/// Public enrollment application service. HTTP types are converted at this boundary only.
pub struct EnrollmentService {
    registry: Arc<AgentRegistryService>,
    catalog: Arc<dyn ControlCatalogRepository>,
    policy: Arc<StaticRbacPolicy>,
    keyring: Arc<EnrollmentKeyring>,
    clock: Arc<dyn Clock>,
}

impl EnrollmentService {
    #[must_use]
    pub fn new(
        registry: Arc<AgentRegistryService>,
        catalog: Arc<dyn ControlCatalogRepository>,
        policy: Arc<StaticRbacPolicy>,
        keyring: Arc<EnrollmentKeyring>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            registry,
            catalog,
            policy,
            keyring,
            clock,
        }
    }

    pub async fn create_token(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateStorageEnrollmentTokenRequest,
    ) -> Result<CreateStorageEnrollmentTokenResponse, Error> {
        let parsed = ParsedCreateRequest::parse(request)?;
        self.policy.authorize_identity(
            identity,
            Permission::StorageEnrollmentCreate,
            &parsed.tenant_id,
        )?;

        if let Some(existing) = self
            .registry
            .find_by_token_request_id(&parsed.tenant_id, &parsed.token_request_id)
            .await
            .map_err(map_storage_enrollment_error)?
        {
            if !parsed.matches(&existing) {
                return Err(map_storage_enrollment_error(CentralError::new(
                    CentralErrorCode::EnrollmentIdReused,
                    "token request identity was reused with another enrollment intent",
                )));
            }
            return self.token_response(&existing, true);
        }

        let now = self.clock.now();
        let expires_at = now
            .get()
            .checked_add(TOKEN_TTL_MS)
            .ok_or_else(internal_enrollment_error)?;
        let token_id = AgentEnrollmentTokenId::new(random_resource_id("ngtok")?)
            .map_err(|_| internal_enrollment_error())?;
        let enrollment_id = AgentEnrollmentId::new(random_resource_id("ngenr")?)
            .map_err(|_| internal_enrollment_error())?;
        let agent_id = AgentId::new(random_resource_id("ngagent")?)
            .map_err(|_| internal_enrollment_error())?;
        let agent_mount_id = AgentMountId::new(random_resource_id("ngmount")?)
            .map_err(|_| internal_enrollment_error())?;
        let key_id = self.keyring.active_key_id().to_owned();

        let material = TokenMaterial::from_values(
            &key_id,
            &token_id,
            &parsed.token_request_id,
            &enrollment_id,
            &parsed.tenant_id,
            &parsed.edge_cluster_id,
            &parsed.storage_volume_id,
            &parsed.volume_descriptor_digest,
            &parsed.pvc_identity_digest,
            &agent_id,
            &agent_mount_id,
            now,
            UnixMillis::new(expires_at),
            &parsed.descriptor,
        );
        let token = self
            .keyring
            .derive_token(&key_id, &canonical_bytes(&material)?)
            .map_err(|_| internal_enrollment_error())?;
        let domain_request = AgentEnrollmentTokenCreateRequest {
            token_id,
            token_request_id: parsed.token_request_id,
            enrollment_id,
            tenant_id: parsed.tenant_id,
            edge_cluster_id: parsed.edge_cluster_id,
            storage_volume_id: parsed.storage_volume_id.clone(),
            volume_descriptor_digest: parsed.volume_descriptor_digest,
            pvc_identity_digest: parsed.pvc_identity_digest,
            agent_id,
            agent_mount_id,
            expected_volume_marker: VolumeMarkerId::new(parsed.storage_volume_id.to_string())
                .map_err(|error| invalid_request(format!("storage_volume_id: {error}")))?,
            desired_access_mode: MountAccessMode::ReadWrite,
            bootstrap_token: token,
            created_at_unix_ms: now,
            expires_at_unix_ms: UnixMillis::new(expires_at),
            extensions: Extensions::new(),
        };
        let result = self
            .registry
            .create_storage_enrollment_intent(CreateStorageEnrollmentIntentRequest {
                request: domain_request,
                descriptor: parsed.descriptor,
                token: BootstrapTokenMetadata { key_id },
            })
            .await
            .map_err(map_storage_enrollment_error)?;
        self.token_response(&result.record, result.replayed)
    }

    pub async fn list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryStorageEnrollmentListRequest,
    ) -> Result<QueryStorageEnrollmentListResponse, Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        if !self.policy.is_allowed(
            identity.principal(),
            Permission::StorageEnrollmentRead,
            &tenant_id,
        ) {
            return Err(storage_enrollment_not_found());
        }
        let state = request
            .state
            .as_deref()
            .map(parse_public_state)
            .transpose()?;
        let registration_kind = request
            .registration_kind
            .as_deref()
            .map(parse_registration_kind)
            .transpose()?;
        let query = request.query.map(validate_query).transpose()?;
        let page_size = usize::from(request.page_size.unwrap_or(DEFAULT_PAGE_SIZE as u16));
        if !(1..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(invalid_request("page_size must be between 1 and 100"));
        }
        let scope = CursorScope {
            tenant_id: tenant_id.to_string(),
            state: state.map(public_state_name).map(str::to_owned),
            registration_kind: registration_kind
                .map(registration_kind_name)
                .map(str::to_owned),
            query: query.clone(),
        };
        let after = request
            .cursor
            .as_deref()
            .map(|cursor| self.decode_cursor(cursor, &scope))
            .transpose()?;
        let page = self
            .registry
            .list_enrollments(AgentEnrollmentListRequest {
                tenant_id,
                state,
                registration_kind,
                query,
                after,
                limit: page_size,
            })
            .await
            .map_err(map_storage_enrollment_error)?;
        let items = page
            .records
            .iter()
            .map(record_to_view)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = page
            .next
            .as_ref()
            .map(|position| self.encode_cursor(&scope, position))
            .transpose()?;
        Ok(QueryStorageEnrollmentListResponse { items, next_cursor })
    }

    pub async fn query(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryStorageEnrollmentRequest,
    ) -> Result<QueryStorageEnrollmentResponse, Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        if !self.policy.is_allowed(
            identity.principal(),
            Permission::StorageEnrollmentRead,
            &tenant_id,
        ) {
            return Err(storage_enrollment_not_found());
        }
        let enrollment_id = parse_enrollment_id(&request.storage_enrollment_id)?;
        let record = self
            .registry
            .query_enrollment(&tenant_id, &enrollment_id)
            .await
            .map_err(map_storage_enrollment_error)?;
        Ok(QueryStorageEnrollmentResponse {
            enrollment: record_to_view(&record)?,
        })
    }

    pub async fn approve(
        &self,
        identity: &AuthenticatedIdentity,
        request: ApproveStorageEnrollmentRequest,
    ) -> Result<ApproveStorageEnrollmentResponse, Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.policy.authorize_identity(
            identity,
            Permission::StorageEnrollmentReview,
            &tenant_id,
        )?;
        let enrollment_id = parse_enrollment_id(&request.storage_enrollment_id)?;
        self.registry
            .query_enrollment(&tenant_id, &enrollment_id)
            .await
            .map_err(map_storage_enrollment_error)?;
        let decision = AgentEnrollmentApprovalRequest {
            enrollment_id,
            decision_request_id: parse_request_id(
                "approval_request_id",
                &request.approval_request_id,
            )?,
            expected_resource_version: ResourceVersion::new(parse_canonical_u64(
                "expected_resource_version",
                &request.expected_resource_version,
            )?),
            decision: AgentEnrollmentDecision::Approve,
            confirm_replacement: request.confirm_replacement,
            extensions: Extensions::new(),
        };
        let result = self
            .registry
            .decide_storage_enrollment(decision, identity.principal().clone(), None)
            .await
            .map_err(map_storage_enrollment_error)?;
        let enrollment = record_to_view(&result.record)?;
        let storage_volume = self
            .catalog
            .get_storage_volume(&tenant_id, &result.record.enrollment.storage_volume_id)
            .await
            .map_err(map_storage_enrollment_error)?
            .ok_or_else(internal_enrollment_error)?;
        Ok(ApproveStorageEnrollmentResponse {
            enrollment,
            storage_volume: storage_volume_view(&storage_volume),
            replayed: result.replayed,
        })
    }

    pub async fn reject(
        &self,
        identity: &AuthenticatedIdentity,
        request: RejectStorageEnrollmentRequest,
    ) -> Result<RejectStorageEnrollmentResponse, Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        self.policy.authorize_identity(
            identity,
            Permission::StorageEnrollmentReview,
            &tenant_id,
        )?;
        let enrollment_id = parse_enrollment_id(&request.storage_enrollment_id)?;
        self.registry
            .query_enrollment(&tenant_id, &enrollment_id)
            .await
            .map_err(map_storage_enrollment_error)?;
        let reason = request.reason.map(validate_rejection_reason).transpose()?;
        let decision = AgentEnrollmentApprovalRequest {
            enrollment_id,
            decision_request_id: parse_request_id(
                "rejection_request_id",
                &request.rejection_request_id,
            )?,
            expected_resource_version: ResourceVersion::new(parse_canonical_u64(
                "expected_resource_version",
                &request.expected_resource_version,
            )?),
            decision: AgentEnrollmentDecision::Reject,
            confirm_replacement: false,
            extensions: Extensions::new(),
        };
        let result = self
            .registry
            .decide_storage_enrollment(decision, identity.principal().clone(), reason)
            .await
            .map_err(map_storage_enrollment_error)?;
        Ok(RejectStorageEnrollmentResponse {
            enrollment: record_to_view(&result.record)?,
            replayed: result.replayed,
        })
    }

    fn token_response(
        &self,
        record: &AgentRegistryRecord,
        replayed: bool,
    ) -> Result<CreateStorageEnrollmentTokenResponse, Error> {
        let token = derive_record_token(&self.keyring, record)?;
        if ContentDigest::hash(token.as_bytes()) != record.enrollment.bootstrap_token_digest {
            return Err(internal_enrollment_error());
        }
        Ok(CreateStorageEnrollmentTokenResponse {
            token_id: record.enrollment.token_id.to_string(),
            bootstrap_token: token,
            volume_descriptor_digest: record.enrollment.volume_descriptor_digest.to_string(),
            expires_at_unix_ms: record.enrollment.expires_at_unix_ms.to_string(),
            replayed,
        })
    }

    fn encode_cursor(
        &self,
        scope: &CursorScope,
        position: &AgentEnrollmentListCursor,
    ) -> Result<String, Error> {
        let document = CursorDocument {
            version: 1,
            key_id: self.keyring.active_key_id().to_owned(),
            scope: scope.clone(),
            created_at_unix_ms: position.created_at_unix_ms.to_string(),
            enrollment_id: position.enrollment_id.to_string(),
        };
        let payload = serde_json::to_vec(&document).map_err(|_| internal_enrollment_error())?;
        let mac = self
            .keyring
            .mac(&document.key_id, CURSOR_DOMAIN, &payload)
            .map_err(|_| internal_enrollment_error())?;
        Ok(format!(
            "{CURSOR_PREFIX}{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(mac)
        ))
    }

    fn decode_cursor(
        &self,
        cursor: &str,
        expected_scope: &CursorScope,
    ) -> Result<AgentEnrollmentListCursor, Error> {
        if cursor.is_empty() || cursor.len() > 2_048 {
            return Err(cursor_conflict());
        }
        let encoded = cursor
            .strip_prefix(CURSOR_PREFIX)
            .ok_or_else(cursor_conflict)?;
        let (payload, mac) = encoded.split_once('.').ok_or_else(cursor_conflict)?;
        if mac.contains('.') {
            return Err(cursor_conflict());
        }
        let payload = URL_SAFE_NO_PAD
            .decode(payload.as_bytes())
            .map_err(|_| cursor_conflict())?;
        let mac = URL_SAFE_NO_PAD
            .decode(mac.as_bytes())
            .map_err(|_| cursor_conflict())?;
        let document: CursorDocument =
            serde_json::from_slice(&payload).map_err(|_| cursor_conflict())?;
        if document.version != 1
            || &document.scope != expected_scope
            || !self
                .keyring
                .verify_mac(&document.key_id, CURSOR_DOMAIN, &payload, &mac)
                .unwrap_or(false)
        {
            return Err(cursor_conflict());
        }
        Ok(AgentEnrollmentListCursor {
            created_at_unix_ms: UnixMillis::new(parse_canonical_u64(
                "cursor.created_at_unix_ms",
                &document.created_at_unix_ms,
            )?),
            enrollment_id: parse_enrollment_id(&document.enrollment_id)?,
        })
    }
}

struct ParsedCreateRequest {
    tenant_id: TenantId,
    token_request_id: RequestId,
    storage_volume_id: StorageVolumeId,
    edge_cluster_id: EdgeClusterId,
    descriptor: FrozenStorageDescriptor,
    pvc_identity_digest: PvcIdentityDigest,
    volume_descriptor_digest: ContentDigest,
}

impl ParsedCreateRequest {
    fn parse(request: CreateStorageEnrollmentTokenRequest) -> Result<Self, Error> {
        let tenant_id = parse_tenant(&request.tenant_id)?;
        let token_request_id = parse_request_id("token_request_id", &request.token_request_id)?;
        let storage_volume_id = StorageVolumeId::new(request.storage_volume_id)
            .map_err(|error| invalid_request(format!("storage_volume_id: {error}")))?;
        let edge_cluster_id = EdgeClusterId::new(request.edge_cluster_id)
            .map_err(|error| invalid_request(format!("edge_cluster_id: {error}")))?;
        validate_display_name(&request.display_name)?;
        validate_region(&request.region)?;
        let access_mode = parse_access_mode(&request.access_mode)?;
        let pvc_identity_digest = PvcIdentityDigest::derive(
            &request.pvc_reference.namespace,
            &request.pvc_reference.claim_name,
        )
        .map_err(|error| invalid_request(error.to_string()))?;
        let descriptor = FrozenStorageDescriptor {
            display_name: request.display_name,
            region: request.region,
            access_mode,
            pvc_reference: FrozenPvcReference {
                namespace: request.pvc_reference.namespace,
                claim_name: request.pvc_reference.claim_name,
            },
        };
        let digest_material = DescriptorDigestMaterial {
            version: 1,
            tenant_id: tenant_id.to_string(),
            storage_volume_id: storage_volume_id.to_string(),
            edge_cluster_id: edge_cluster_id.to_string(),
            descriptor: &descriptor,
        };
        let volume_descriptor_digest = ContentDigest::hash(canonical_bytes(&digest_material)?);
        Ok(Self {
            tenant_id,
            token_request_id,
            storage_volume_id,
            edge_cluster_id,
            descriptor,
            pvc_identity_digest,
            volume_descriptor_digest,
        })
    }

    fn matches(&self, record: &AgentRegistryRecord) -> bool {
        record.enrollment.tenant_id == self.tenant_id
            && record.enrollment.token_request_id == self.token_request_id
            && record.enrollment.storage_volume_id == self.storage_volume_id
            && record.enrollment.edge_cluster_id == self.edge_cluster_id
            && record.enrollment.pvc_identity_digest == self.pvc_identity_digest
            && record.enrollment.volume_descriptor_digest == self.volume_descriptor_digest
            && record.storage_enrollment.descriptor.as_ref() == Some(&self.descriptor)
    }
}

#[derive(Serialize)]
struct DescriptorDigestMaterial<'a> {
    version: u32,
    tenant_id: String,
    storage_volume_id: String,
    edge_cluster_id: String,
    descriptor: &'a FrozenStorageDescriptor,
}

#[derive(Serialize)]
struct TokenMaterial {
    version: u32,
    key_id: String,
    token_id: String,
    token_request_id: String,
    enrollment_id: String,
    tenant_id: String,
    edge_cluster_id: String,
    storage_volume_id: String,
    volume_descriptor_digest: String,
    pvc_identity_digest: String,
    agent_id: String,
    agent_mount_id: String,
    created_at_unix_ms: String,
    expires_at_unix_ms: String,
    descriptor: FrozenStorageDescriptor,
}

impl TokenMaterial {
    #[allow(clippy::too_many_arguments)]
    fn from_values(
        key_id: &str,
        token_id: &AgentEnrollmentTokenId,
        token_request_id: &RequestId,
        enrollment_id: &AgentEnrollmentId,
        tenant_id: &TenantId,
        edge_cluster_id: &EdgeClusterId,
        storage_volume_id: &StorageVolumeId,
        volume_descriptor_digest: &ContentDigest,
        pvc_identity_digest: &PvcIdentityDigest,
        agent_id: &AgentId,
        agent_mount_id: &AgentMountId,
        created_at_unix_ms: UnixMillis,
        expires_at_unix_ms: UnixMillis,
        descriptor: &FrozenStorageDescriptor,
    ) -> Self {
        Self {
            version: 1,
            key_id: key_id.to_owned(),
            token_id: token_id.to_string(),
            token_request_id: token_request_id.to_string(),
            enrollment_id: enrollment_id.to_string(),
            tenant_id: tenant_id.to_string(),
            edge_cluster_id: edge_cluster_id.to_string(),
            storage_volume_id: storage_volume_id.to_string(),
            volume_descriptor_digest: volume_descriptor_digest.to_string(),
            pvc_identity_digest: pvc_identity_digest.as_digest().to_string(),
            agent_id: agent_id.to_string(),
            agent_mount_id: agent_mount_id.to_string(),
            created_at_unix_ms: created_at_unix_ms.to_string(),
            expires_at_unix_ms: expires_at_unix_ms.to_string(),
            descriptor: descriptor.clone(),
        }
    }

    fn from_record(record: &AgentRegistryRecord) -> Result<Self, Error> {
        let key_id = record
            .storage_enrollment
            .token_key_id
            .as_deref()
            .ok_or_else(legacy_reissue_required)?;
        let descriptor = record
            .storage_enrollment
            .descriptor
            .as_ref()
            .ok_or_else(legacy_reissue_required)?;
        Ok(Self::from_values(
            key_id,
            &record.enrollment.token_id,
            &record.enrollment.token_request_id,
            &record.enrollment.enrollment_id,
            &record.enrollment.tenant_id,
            &record.enrollment.edge_cluster_id,
            &record.enrollment.storage_volume_id,
            &record.enrollment.volume_descriptor_digest,
            &record.enrollment.pvc_identity_digest,
            &record.enrollment.reserved_agent_id,
            &record.enrollment.reserved_agent_mount_id,
            record.enrollment.created_at_unix_ms,
            record.enrollment.expires_at_unix_ms,
            descriptor,
        ))
    }
}

fn derive_record_token(
    keyring: &EnrollmentKeyring,
    record: &AgentRegistryRecord,
) -> Result<String, Error> {
    let material = TokenMaterial::from_record(record)?;
    keyring
        .derive_token(&material.key_id, &canonical_bytes(&material)?)
        .map_err(|_| {
            application_error(
                ErrorCategory::Unavailable,
                "enrollment_key_unavailable",
                "SERVICE_UNAVAILABLE",
                "enrollment token key is unavailable",
                true,
            )
        })
}

fn record_to_view(record: &AgentRegistryRecord) -> Result<StorageEnrollmentView, Error> {
    let descriptor = record
        .storage_enrollment
        .descriptor
        .as_ref()
        .ok_or_else(legacy_reissue_required)?;
    let candidate = record
        .candidate
        .as_ref()
        .ok_or_else(legacy_reissue_required)?;
    let state = record
        .storage_enrollment
        .state
        .ok_or_else(legacy_reissue_required)?;
    let registration_kind = record
        .storage_enrollment
        .registration_kind
        .ok_or_else(legacy_reissue_required)?;
    let updated_at = record
        .storage_enrollment
        .updated_at_unix_ms
        .ok_or_else(legacy_reissue_required)?;
    let created_at = record
        .enrollment
        .bootstrapped_at_unix_ms
        .ok_or_else(legacy_reissue_required)?;
    let expires_at = record
        .enrollment
        .review_expires_at_unix_ms
        .ok_or_else(legacy_reissue_required)?;
    let descriptor_matches =
        probe_matches_descriptor(&candidate.probe, &record.mount.expected_volume_marker);
    if candidate.proof_of_possession_status != Some(AgentProofOfPossessionStatus::Verified) {
        return Err(legacy_reissue_required());
    }
    Ok(StorageEnrollmentView {
        tenant_id: record.enrollment.tenant_id.to_string(),
        storage_enrollment_id: record.enrollment.enrollment_id.to_string(),
        storage_volume_id: record.enrollment.storage_volume_id.to_string(),
        display_name: descriptor.display_name.clone(),
        edge_cluster_id: record.enrollment.edge_cluster_id.to_string(),
        region: descriptor.region.clone(),
        access_mode: access_mode_name(descriptor.access_mode).to_owned(),
        pvc_reference: PvcReference {
            namespace: descriptor.pvc_reference.namespace.clone(),
            claim_name: descriptor.pvc_reference.claim_name.clone(),
        },
        registration_kind: registration_kind_name(registration_kind).to_owned(),
        state: public_state_name(state).to_owned(),
        agent_version: candidate.agent_version.clone(),
        identity_fingerprint: candidate.public_key_fingerprint.to_string(),
        proof_of_possession_status: "verified".to_owned(),
        probe: StorageEnrollmentProbeSummary {
            observed_access_mode: match candidate.probe.access_mode {
                Some(MountAccessMode::ReadWrite) => "read_write",
                Some(MountAccessMode::ReadOnly) | None => "read_only",
            }
            .to_owned(),
            descriptor_matches,
            protocol_compatible: candidate
                .supported_protocol_versions
                .contains(&PROTOCOL_VERSION_V1),
            observed_at_unix_ms: candidate.probe.observed_at_unix_ms.to_string(),
        },
        resource_version: record.resource_version.to_string(),
        created_at_unix_ms: created_at.to_string(),
        updated_at_unix_ms: updated_at.to_string(),
        expires_at_unix_ms: expires_at.to_string(),
        reviewed_at_unix_ms: record
            .enrollment
            .decided_at_unix_ms
            .map(|value| value.to_string()),
    })
}

fn probe_matches_descriptor(
    probe: &AgentBootstrapProbe,
    expected_volume_marker: &VolumeMarkerId,
) -> bool {
    probe.marker_matches
        && probe.mount_boundary_detected
        && probe.observed_volume_marker.as_ref() == Some(expected_volume_marker)
        && probe.access_mode == Some(MountAccessMode::ReadWrite)
        && probe.rename_supported
        && probe.fsync_supported
        && matches!(
            probe.health,
            ResourceHealth::Ready | ResourceHealth::Degraded
        )
}

fn parse_tenant(value: &str) -> Result<TenantId, Error> {
    TenantId::new(value.to_owned()).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}

fn parse_enrollment_id(value: &str) -> Result<AgentEnrollmentId, Error> {
    AgentEnrollmentId::new(value.to_owned())
        .map_err(|error| invalid_request(format!("storage_enrollment_id: {error}")))
}

fn parse_request_id(field: &'static str, value: &str) -> Result<RequestId, Error> {
    RequestId::new(value.to_owned()).map_err(|error| invalid_request(format!("{field}: {error}")))
}

fn parse_access_mode(value: &str) -> Result<StorageEnrollmentAccessMode, Error> {
    match value {
        "read_write_many" => Ok(StorageEnrollmentAccessMode::ReadWriteMany),
        "read_write_once" => Ok(StorageEnrollmentAccessMode::ReadWriteOnce),
        _ => Err(invalid_request(
            "access_mode must be read_write_many or read_write_once",
        )),
    }
}

fn parse_public_state(value: &str) -> Result<StorageEnrollmentState, Error> {
    match value {
        "pending_approval" => Ok(StorageEnrollmentState::PendingApproval),
        "approved" => Ok(StorageEnrollmentState::Approved),
        "enrolled" => Ok(StorageEnrollmentState::Enrolled),
        "rejected" => Ok(StorageEnrollmentState::Rejected),
        "expired" => Ok(StorageEnrollmentState::Expired),
        _ => Err(invalid_request("state is not a public enrollment state")),
    }
}

fn parse_registration_kind(value: &str) -> Result<StorageEnrollmentRegistrationKind, Error> {
    match value {
        "initial" => Ok(StorageEnrollmentRegistrationKind::Initial),
        "replacement" => Ok(StorageEnrollmentRegistrationKind::Replacement),
        _ => Err(invalid_request(
            "registration_kind must be initial or replacement",
        )),
    }
}

fn public_state_name(value: StorageEnrollmentState) -> &'static str {
    match value {
        StorageEnrollmentState::PendingApproval => "pending_approval",
        StorageEnrollmentState::Approved => "approved",
        StorageEnrollmentState::Enrolled => "enrolled",
        StorageEnrollmentState::Rejected => "rejected",
        StorageEnrollmentState::Expired => "expired",
    }
}

fn registration_kind_name(value: StorageEnrollmentRegistrationKind) -> &'static str {
    match value {
        StorageEnrollmentRegistrationKind::Initial => "initial",
        StorageEnrollmentRegistrationKind::Replacement => "replacement",
    }
}

fn access_mode_name(value: StorageEnrollmentAccessMode) -> &'static str {
    match value {
        StorageEnrollmentAccessMode::ReadWriteMany => "read_write_many",
        StorageEnrollmentAccessMode::ReadWriteOnce => "read_write_once",
    }
}

fn validate_display_name(value: &str) -> Result<(), Error> {
    let length = value.chars().count();
    if (1..=128).contains(&length) {
        Ok(())
    } else {
        Err(invalid_request(
            "display_name must contain between 1 and 128 characters",
        ))
    }
}

fn validate_region(value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > 64
        || !value.is_ascii()
        || !value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(invalid_request("region has an invalid format"));
    }
    Ok(())
}

fn validate_query(value: String) -> Result<String, Error> {
    let length = value.chars().count();
    if (1..=256).contains(&length) {
        Ok(value)
    } else {
        Err(invalid_request(
            "query must contain between 1 and 256 characters",
        ))
    }
}

fn validate_rejection_reason(value: String) -> Result<String, Error> {
    let length = value.chars().count();
    if (1..=2_048).contains(&length) {
        Ok(value)
    } else {
        Err(invalid_request(
            "reason must contain between 1 and 2048 characters",
        ))
    }
}

fn parse_canonical_u64(field: &'static str, value: &str) -> Result<u64, Error> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| invalid_request(format!("{field} must be a canonical unsigned integer")))?;
    if parsed.to_string() != value {
        return Err(invalid_request(format!(
            "{field} must be a canonical unsigned integer"
        )));
    }
    Ok(parsed)
}

fn random_resource_id(prefix: &str) -> Result<String, Error> {
    let mut random = [0_u8; 18];
    getrandom::fill(&mut random).map_err(|_| internal_enrollment_error())?;
    Ok(format!("{prefix}_{}", URL_SAFE_NO_PAD.encode(random)))
}

fn canonical_bytes(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    serde_json_canonicalizer::to_vec(value).map_err(|_| internal_enrollment_error())
}

fn internal_enrollment_error() -> Error {
    application_error(
        ErrorCategory::Internal,
        "internal",
        "INTERNAL",
        "the server could not complete the enrollment request",
        false,
    )
}

fn legacy_reissue_required() -> Error {
    application_error(
        ErrorCategory::Conflict,
        "legacy_enrollment_requires_reissue",
        "LEGACY_ENROLLMENT_REQUIRES_REISSUE",
        "legacy enrollment requires a replacement enrollment",
        false,
    )
}

fn cursor_conflict() -> Error {
    application_error(
        ErrorCategory::Conflict,
        "cursor_scope_conflict",
        "CURSOR_SCOPE_CONFLICT",
        "cursor is invalid or belongs to another enrollment query",
        false,
    )
}

fn map_storage_enrollment_error(error: CentralError) -> Error {
    match error.code() {
        CentralErrorCode::EnrollmentNotFound => storage_enrollment_not_found(),
        CentralErrorCode::LegacyEnrollmentRequiresReissue => legacy_reissue_required(),
        CentralErrorCode::EnrollmentIdReused => application_error(
            ErrorCategory::Conflict,
            "storage_enrollment_token_request_id_reused",
            "STORAGE_ENROLLMENT_TOKEN_REQUEST_ID_REUSED",
            "the token request ID already belongs to a different enrollment intent",
            false,
        ),
        CentralErrorCode::EnrollmentDecisionConflict => application_error(
            ErrorCategory::Conflict,
            "storage_enrollment_decision_id_reused",
            "STORAGE_ENROLLMENT_DECISION_ID_REUSED",
            "the decision request ID already belongs to another review payload",
            false,
        ),
        CentralErrorCode::ConcurrentUpdate => application_error(
            ErrorCategory::Conflict,
            "storage_enrollment_version_conflict",
            "STORAGE_ENROLLMENT_VERSION_CONFLICT",
            "the storage enrollment resource version changed before review",
            false,
        ),
        CentralErrorCode::EnrollmentExpired => application_error(
            ErrorCategory::Conflict,
            "storage_enrollment_expired",
            "STORAGE_ENROLLMENT_EXPIRED",
            "the storage enrollment review window expired",
            false,
        ),
        CentralErrorCode::VolumeOwnerConflict => application_error(
            ErrorCategory::Conflict,
            "pvc_already_enrolled",
            "PVC_ALREADY_ENROLLED",
            "the PVC is already bound to another active enrollment",
            false,
        ),
        CentralErrorCode::EnrollmentProbeFailed | CentralErrorCode::AgentIdentityMismatch => {
            application_error(
                ErrorCategory::Conflict,
                "storage_volume_descriptor_mismatch",
                "STORAGE_VOLUME_DESCRIPTOR_MISMATCH",
                "the Agent probe does not match the frozen storage descriptor",
                false,
            )
        }
        CentralErrorCode::ApprovalRequired | CentralErrorCode::InvalidState => application_error(
            ErrorCategory::Conflict,
            "storage_enrollment_state_conflict",
            "STORAGE_ENROLLMENT_STATE_CONFLICT",
            "the storage enrollment is not in a reviewable state",
            false,
        ),
        _ => map_central_error(error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorScope {
    tenant_id: String,
    state: Option<String>,
    registration_kind: Option<String>,
    query: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorDocument {
    version: u32,
    key_id: String,
    scope: CursorScope,
    created_at_unix_ms: String,
    enrollment_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use neoengram_protocol::AgentMountIdentityDigest;

    #[test]
    fn public_state_and_access_mode_are_closed_sets() {
        assert!(parse_public_state("token_issued").is_err());
        assert!(parse_access_mode("read_only_many").is_err());
        assert_eq!(
            access_mode_name(StorageEnrollmentAccessMode::ReadWriteOnce),
            "read_write_once"
        );
    }

    #[test]
    fn identifiers_are_csprng_shaped_and_protocol_valid() {
        let id = random_resource_id("ngenr").unwrap();
        assert!(AgentEnrollmentId::new(id).is_ok());
    }

    #[test]
    fn degraded_probe_still_matches_the_frozen_descriptor() {
        let expected_marker = VolumeMarkerId::new("volume-a").unwrap();
        let mut probe = AgentBootstrapProbe {
            observed_volume_marker: Some(expected_marker.clone()),
            marker_matches: true,
            mount_boundary_detected: true,
            mount_identity_digest: AgentMountIdentityDigest::new(ContentDigest::hash(b"mount-a")),
            access_mode: Some(MountAccessMode::ReadWrite),
            rename_supported: true,
            fsync_supported: true,
            health: ResourceHealth::Ready,
            observed_at_unix_ms: UnixMillis::new(1),
            extensions: Extensions::new(),
        };

        assert!(probe_matches_descriptor(&probe, &expected_marker));
        probe.health = ResourceHealth::Degraded;
        assert!(probe_matches_descriptor(&probe, &expected_marker));

        for health in [ResourceHealth::Unavailable, ResourceHealth::Unknown] {
            probe.health = health;
            assert!(!probe_matches_descriptor(&probe, &expected_marker));
        }
    }
}
