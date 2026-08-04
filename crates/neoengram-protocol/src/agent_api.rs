use base64::{engine::general_purpose::STANDARD, Engine as _};
use neoengram_core::ObjectId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::validation::{
    validate_collection_limit, validate_extension_keys, validate_positive, CONTENT_DIGEST_PATTERN,
};
use crate::{
    domain_separated_jcs_bytes, jcs_blake3, AgentBootId, AgentBootstrapProof, AgentHeartbeat,
    AgentId, AgentInstallationId, AgentMountIdentityDigest, AgentMountStatusReport,
    ControlEnvelope, DecimalU64, Extensions, IndexDeltaRecord, JobId, MetadataBatchDescriptor,
    MetadataBatchPage, ProtocolError, ProtocolResult, ProtocolVersion, RequestId, ResourceVersion,
    SessionGeneration, SessionId, TenantId, UnixMillis, WireIndexVersion, WireObjectSpec,
    MAX_RECORDS_PER_PAGE, PROTOCOL_VERSION_V1,
};

pub const AGENT_REQUEST_SIGNING_DOMAIN_V1: &str = "neoengram-agent-request-v1";
pub const AGENT_SESSION_OPEN_PATH: &str = "/agent/session/open";
pub const AGENT_SESSION_HEARTBEAT_REPORT_PATH: &str = "/agent/session/heartbeat/report";
pub const AGENT_SESSION_MESSAGE_LIST_QUERY_PATH: &str = "/agent/session/message/list/query";
pub const AGENT_JOB_REPORT_CREATE_PATH: &str = "/agent/job/report/create";
pub const AGENT_JOB_METADATA_BATCH_STAGE_PATH: &str = "/agent/job/metadata/batch/stage";
pub const AGENT_JOB_METADATA_PAGE_STAGE_PATH: &str = "/agent/job/metadata/page/stage";
pub const AGENT_JOB_INDEX_PAGE_QUERY_PATH: &str = "/agent/job/index/page/query";
pub const AGENT_JOB_OBJECT_MISSING_QUERY_PATH: &str = "/agent/job/object/missing/query";
pub const AGENT_JOB_OBJECT_UPLOAD_PATH: &str = "/agent/job/object/upload";
pub const AGENT_SESSION_CLOSE_PATH: &str = "/agent/session/close";
pub const AGENT_ENROLLMENT_BOOTSTRAP_PATH: &str = "/agent/enrollment/bootstrap";
pub const AGENT_ENROLLMENT_STATUS_QUERY_PATH: &str = "/agent/enrollment/status/query";

pub const MAX_AGENT_POLL_MESSAGES: usize = 32;
pub const MAX_INLINE_OBJECT_BYTES: usize = 4 * 1024 * 1024;

/// Ed25519 proof over one complete unsigned action request.
///
/// `unsigned_body_digest` is BLAKE3 over RFC 8785 canonical JSON for every request member except
/// `proof`. The signature binds that digest to POST and one exact canonical action path.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentRequestProof {
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub unsigned_body_digest: crate::ContentDigest,
    #[serde(flatten)]
    pub possession: AgentBootstrapProof,
}

impl std::fmt::Debug for AgentRequestProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentRequestProof")
            .field("unsigned_body_digest", &self.unsigned_body_digest)
            .field("possession", &self.possession)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentAuthenticatedRequest<T> {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub agent_id: AgentId,
    pub installation_id: AgentInstallationId,
    pub boot_id: AgentBootId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_generation: Option<SessionGeneration>,
    pub signed_at_unix_ms: UnixMillis,
    pub payload: T,
    pub proof: AgentRequestProof,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl<T: Serialize> AgentAuthenticatedRequest<T> {
    pub fn computed_unsigned_body_digest(&self) -> ProtocolResult<crate::ContentDigest> {
        jcs_blake3(&self.unsigned_body())
    }

    pub fn signing_bytes(&self, canonical_path: &'static str) -> ProtocolResult<Vec<u8>> {
        validate_agent_action_path(canonical_path)?;
        let computed = self.computed_unsigned_body_digest()?;
        if computed != self.proof.unsigned_body_digest {
            return Err(ProtocolError::InvalidDigest(
                "Agent request unsigned body digest does not match".to_owned(),
            ));
        }
        domain_separated_jcs_bytes(
            AGENT_REQUEST_SIGNING_DOMAIN_V1,
            &AgentRequestSignatureInput {
                method: "POST",
                canonical_path,
                unsigned_body_digest: computed,
            },
        )
    }

    pub fn verify(&self, canonical_path: &'static str) -> ProtocolResult<()> {
        self.validate_common()?;
        self.proof
            .possession
            .verify(&self.signing_bytes(canonical_path)?)
    }

    pub fn validate_open(&self) -> ProtocolResult<()> {
        self.validate_common()?;
        if self.session_id.is_some() || self.session_generation.is_some() {
            return Err(ProtocolError::InvalidField {
                field: "session_id",
                reason: "session open must not claim a pre-existing session".to_owned(),
            });
        }
        Ok(())
    }

    pub fn validate_session(&self) -> ProtocolResult<()> {
        self.validate_common()?;
        if self.session_id.is_none() {
            return Err(ProtocolError::InvalidField {
                field: "session_id",
                reason: "an established Agent action requires session_id".to_owned(),
            });
        }
        let generation = self
            .session_generation
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "session_generation",
                reason: "an established Agent action requires session_generation".to_owned(),
            })?;
        validate_positive("session_generation", generation.get())
    }

    fn validate_common(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        self.proof.possession.validate()?;
        validate_extension_keys(
            &self.extensions,
            &[
                "protocol_version",
                "request_id",
                "agent_id",
                "installation_id",
                "boot_id",
                "session_id",
                "session_generation",
                "signed_at_unix_ms",
                "payload",
                "proof",
            ],
        )
    }

    fn unsigned_body(&self) -> AgentUnsignedRequest<'_, T> {
        AgentUnsignedRequest {
            protocol_version: self.protocol_version,
            request_id: &self.request_id,
            agent_id: &self.agent_id,
            installation_id: &self.installation_id,
            boot_id: &self.boot_id,
            session_id: self.session_id.as_ref(),
            session_generation: self.session_generation,
            signed_at_unix_ms: self.signed_at_unix_ms,
            payload: &self.payload,
            extensions: &self.extensions,
        }
    }
}

#[derive(Serialize)]
struct AgentUnsignedRequest<'a, T> {
    protocol_version: ProtocolVersion,
    request_id: &'a RequestId,
    agent_id: &'a AgentId,
    installation_id: &'a AgentInstallationId,
    boot_id: &'a AgentBootId,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<&'a SessionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_generation: Option<SessionGeneration>,
    signed_at_unix_ms: UnixMillis,
    payload: &'a T,
    #[serde(flatten)]
    extensions: &'a Extensions,
}

#[derive(Serialize)]
struct AgentRequestSignatureInput<'a> {
    method: &'static str,
    canonical_path: &'a str,
    unsigned_body_digest: crate::ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentSessionOpenPayload {
    pub mount_identity_digest: AgentMountIdentityDigest,
    pub expected_resource_version: ResourceVersion,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentSessionOpenResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub agent_id: AgentId,
    pub session_id: SessionId,
    pub session_generation: SessionGeneration,
    pub agent_mount_id: crate::AgentMountId,
    pub mount_generation: crate::MountGeneration,
    pub owner_generation: crate::OwnerGeneration,
    pub resource_version: ResourceVersion,
    pub opened_at_unix_ms: UnixMillis,
    pub replayed: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentHeartbeatReportPayload {
    pub heartbeat: AgentHeartbeat,
    pub mount_report: AgentMountStatusReport,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentHeartbeatReportResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub resource_version: ResourceVersion,
    pub received_at_unix_ms: UnixMillis,
    pub replayed: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMessageListQueryPayload {
    pub max_messages: u16,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentMessageListQueryPayload {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.max_messages == 0 || usize::from(self.max_messages) > MAX_AGENT_POLL_MESSAGES {
            return Err(ProtocolError::InvalidField {
                field: "max_messages",
                reason: format!("must be in 1..={MAX_AGENT_POLL_MESSAGES}"),
            });
        }
        validate_extension_keys(&self.extensions, &["max_messages"])
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMessageListQueryResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    #[schemars(length(max = 32))]
    pub messages: Vec<ControlEnvelope>,
    pub retry_after_ms: DecimalU64,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentJobReportCreatePayload {
    pub tenant_id: TenantId,
    pub report: ControlEnvelope,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentActionAcceptedResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub resource_version: ResourceVersion,
    pub replayed: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMetadataBatchStagePayload {
    pub tenant_id: TenantId,
    pub job_id: JobId,
    pub descriptor: MetadataBatchDescriptor,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMetadataPageStagePayload {
    pub tenant_id: TenantId,
    pub job_id: JobId,
    pub page: MetadataBatchPage,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMetadataStageResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub batch_id: crate::MetadataBatchId,
    pub complete: bool,
    pub replayed: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentIndexPageQueryPayload {
    pub tenant_id: TenantId,
    pub job_id: JobId,
    pub artifact_id: crate::ArtifactId,
    pub playground_id: crate::PlaygroundId,
    pub index_version: WireIndexVersion,
    pub page_number: u32,
    pub max_records: u16,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentIndexPageQueryPayload {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.index_version.validate()?;
        if self.max_records == 0 || usize::from(self.max_records) > MAX_RECORDS_PER_PAGE {
            return Err(ProtocolError::InvalidField {
                field: "max_records",
                reason: format!("must be in 1..={MAX_RECORDS_PER_PAGE}"),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "job_id",
                "artifact_id",
                "playground_id",
                "index_version",
                "page_number",
                "max_records",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentIndexPageQueryResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub index_version: WireIndexVersion,
    pub page_number: u32,
    pub page_count: u32,
    #[schemars(length(max = 4096))]
    pub records: Vec<IndexDeltaRecord>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentObjectMissingQueryPayload {
    pub tenant_id: TenantId,
    pub project_id: crate::ProjectId,
    pub artifact_id: crate::ArtifactId,
    pub job_id: JobId,
    #[schemars(length(max = 4096))]
    pub objects: Vec<WireObjectSpec>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentObjectMissingQueryPayload {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_collection_limit("objects", self.objects.len(), MAX_RECORDS_PER_PAGE)?;
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "project_id",
                "artifact_id",
                "job_id",
                "objects",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentObjectMissingQueryResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    #[schemars(length(max = 4096))]
    pub missing: Vec<WireObjectSpec>,
    #[schemars(length(max = 4096))]
    pub already_durable: Vec<WireObjectSpec>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentObjectUploadPayload {
    pub tenant_id: TenantId,
    pub project_id: crate::ProjectId,
    pub artifact_id: crate::ArtifactId,
    pub job_id: JobId,
    pub object: WireObjectSpec,
    #[schemars(with = "String", length(min = 4, max = 5592408))]
    pub content_base64: String,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentObjectUploadPayload {
    pub fn decode_content(&self) -> ProtocolResult<Vec<u8>> {
        let content =
            STANDARD
                .decode(&self.content_base64)
                .map_err(|_| ProtocolError::InvalidField {
                    field: "content_base64",
                    reason: "must be canonical padded base64".to_owned(),
                })?;
        if STANDARD.encode(&content) != self.content_base64 {
            return Err(ProtocolError::InvalidField {
                field: "content_base64",
                reason: "must be canonical padded base64".to_owned(),
            });
        }
        if content.len() > MAX_INLINE_OBJECT_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "inline object bytes",
                limit: MAX_INLINE_OBJECT_BYTES,
                actual: content.len(),
            });
        }
        if self.object.size.get() != content.len() as u64 {
            return Err(ProtocolError::InvalidField {
                field: "object.size",
                reason: "does not match decoded content length".to_owned(),
            });
        }
        let observed = ObjectId::from_bytes(*blake3::hash(&content).as_bytes());
        if observed != self.object.object_id {
            return Err(ProtocolError::InvalidDigest(
                "uploaded content digest does not match object_id".to_owned(),
            ));
        }
        Ok(content)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentObjectUploadResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub receipt: crate::ObjectDurabilityReceipt,
    pub replayed: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentSessionClosePayload {
    pub expected_resource_version: ResourceVersion,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentSessionCloseResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub resource_version: ResourceVersion,
    pub closed_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

/// Schema-only union that keeps every Agent action DTO reachable from one root.
#[derive(JsonSchema)]
#[allow(dead_code)]
pub enum AgentApiSchema {
    SessionOpen(AgentAuthenticatedRequest<AgentSessionOpenPayload>),
    Heartbeat(AgentAuthenticatedRequest<AgentHeartbeatReportPayload>),
    MessageList(AgentAuthenticatedRequest<AgentMessageListQueryPayload>),
    JobReport(AgentAuthenticatedRequest<AgentJobReportCreatePayload>),
    MetadataBatch(AgentAuthenticatedRequest<AgentMetadataBatchStagePayload>),
    MetadataPage(AgentAuthenticatedRequest<AgentMetadataPageStagePayload>),
    IndexPage(AgentAuthenticatedRequest<AgentIndexPageQueryPayload>),
    ObjectMissing(AgentAuthenticatedRequest<AgentObjectMissingQueryPayload>),
    ObjectUpload(AgentAuthenticatedRequest<AgentObjectUploadPayload>),
    SessionClose(AgentAuthenticatedRequest<AgentSessionClosePayload>),
}

fn validate_agent_action_path(path: &'static str) -> ProtocolResult<()> {
    if matches!(
        path,
        AGENT_SESSION_OPEN_PATH
            | AGENT_SESSION_HEARTBEAT_REPORT_PATH
            | AGENT_SESSION_MESSAGE_LIST_QUERY_PATH
            | AGENT_JOB_REPORT_CREATE_PATH
            | AGENT_JOB_METADATA_BATCH_STAGE_PATH
            | AGENT_JOB_METADATA_PAGE_STAGE_PATH
            | AGENT_JOB_INDEX_PAGE_QUERY_PATH
            | AGENT_JOB_OBJECT_MISSING_QUERY_PATH
            | AGENT_JOB_OBJECT_UPLOAD_PATH
            | AGENT_SESSION_CLOSE_PATH
    ) {
        Ok(())
    } else {
        Err(ProtocolError::InvalidField {
            field: "canonical_path",
            reason: "is not a supported Agent action path".to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use neoengram_core::ContentDigest;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    use super::*;
    use crate::{AgentSignatureAlgorithm, Ed25519PublicKeySpki, Ed25519Signature};

    fn digest() -> ContentDigest {
        "11".repeat(32).parse().unwrap()
    }

    fn unsigned_request() -> AgentAuthenticatedRequest<AgentSessionOpenPayload> {
        AgentAuthenticatedRequest {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id: RequestId::new("request-a").unwrap(),
            agent_id: AgentId::new("agent-a").unwrap(),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            boot_id: AgentBootId::new("boot-a").unwrap(),
            session_id: None,
            session_generation: None,
            signed_at_unix_ms: UnixMillis::new(10),
            payload: AgentSessionOpenPayload {
                mount_identity_digest: AgentMountIdentityDigest::new(digest()),
                expected_resource_version: ResourceVersion::new(7),
                extensions: Extensions::new(),
            },
            proof: AgentRequestProof {
                unsigned_body_digest: digest(),
                possession: AgentBootstrapProof {
                    algorithm: AgentSignatureAlgorithm::Ed25519,
                    public_key_spki: Ed25519PublicKeySpki::from_public_key_bytes([0; 32]),
                    signature: Ed25519Signature::new(vec![0; 64]).unwrap(),
                    extensions: Extensions::new(),
                },
            },
            extensions: Extensions::new(),
        }
    }

    #[test]
    fn proof_binds_path_and_every_unsigned_request_member() {
        let document = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
        let key_pair = Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap();
        let mut request = unsigned_request();
        request.proof.possession.public_key_spki = Ed25519PublicKeySpki::from_public_key_bytes(
            key_pair.public_key().as_ref().try_into().unwrap(),
        );
        request.proof.unsigned_body_digest = request.computed_unsigned_body_digest().unwrap();
        let signing_bytes = request.signing_bytes(AGENT_SESSION_OPEN_PATH).unwrap();
        request.proof.possession.signature =
            Ed25519Signature::new(key_pair.sign(&signing_bytes).as_ref().to_vec()).unwrap();

        request.verify(AGENT_SESSION_OPEN_PATH).unwrap();
        assert!(request.verify(AGENT_SESSION_CLOSE_PATH).is_err());
        request.payload.expected_resource_version = ResourceVersion::new(8);
        assert!(request.verify(AGENT_SESSION_OPEN_PATH).is_err());
    }

    #[test]
    fn session_open_response_carries_authoritative_mount_and_owner_fences() {
        let response = AgentSessionOpenResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id: RequestId::new("request-open").unwrap(),
            agent_id: AgentId::new("agent-a").unwrap(),
            session_id: SessionId::new("session-a").unwrap(),
            session_generation: SessionGeneration::new(2),
            agent_mount_id: crate::AgentMountId::new("mount-a").unwrap(),
            mount_generation: crate::MountGeneration::new(3),
            owner_generation: crate::OwnerGeneration::new(4),
            resource_version: ResourceVersion::new(5),
            opened_at_unix_ms: UnixMillis::new(6),
            replayed: false,
            extensions: Extensions::new(),
        };
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["agent_mount_id"], "mount-a");
        assert_eq!(json["mount_generation"], "3");
        assert_eq!(json["owner_generation"], "4");
    }
}
