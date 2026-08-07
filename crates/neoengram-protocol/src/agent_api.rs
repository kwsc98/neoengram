use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::validation::{
    parse_unique_json, validate_extension_keys, validate_positive, CONTENT_DIGEST_PATTERN,
};
use crate::{
    domain_separated_jcs_bytes, jcs_blake3, AgentBootId, AgentBootstrapProof, AgentHeartbeat,
    AgentId, AgentInstallationId, AgentMountIdentityDigest, AgentMountStatusReport,
    ControlEnvelope, ControlError, ControlMessage, DecimalU64, Extensions, IndexDeltaRecord,
    JobAssignment, JobDecision, JobId, MessageId, MetadataBatchDescriptor, MetadataBatchPage,
    ProtocolError, ProtocolResult, ProtocolVersion, RequestId, ResourceVersion, SequenceNumber,
    SessionGeneration, SessionId, TenantId, UnixMillis, WireChunkRef, WireChunkingStrategy,
    WireIndexVersion, MAX_CONTROL_MESSAGE_BYTES, MAX_RECORDS_PER_PAGE, PROTOCOL_VERSION_V1,
};
use neoengram_core::ManifestId;

pub const AGENT_REQUEST_SIGNING_DOMAIN_V1: &str = "neoengram-agent-request-v1";
pub const AGENT_SESSION_OPEN_PATH: &str = "/agent/session/open";
pub const AGENT_SESSION_CHANNEL_OPEN_PATH: &str = "/agent/session/channel/open";
pub const AGENT_SESSION_HEARTBEAT_REPORT_PATH: &str = "/agent/session/heartbeat/report";
pub const AGENT_SESSION_MESSAGE_LIST_QUERY_PATH: &str = "/agent/session/message/list/query";
pub const AGENT_JOB_REPORT_CREATE_PATH: &str = "/agent/job/report/create";
pub const AGENT_JOB_METADATA_BATCH_STAGE_PATH: &str = "/agent/job/metadata/batch/stage";
pub const AGENT_JOB_METADATA_PAGE_STAGE_PATH: &str = "/agent/job/metadata/page/stage";
pub const AGENT_JOB_INDEX_PAGE_QUERY_PATH: &str = "/agent/job/index/page/query";
pub const AGENT_JOB_MANIFEST_PAGE_QUERY_PATH: &str = "/agent/job/manifest/page/query";
pub const AGENT_SESSION_CLOSE_PATH: &str = "/agent/session/close";
pub const AGENT_ENROLLMENT_BOOTSTRAP_PATH: &str = "/agent/enrollment/bootstrap";
pub const AGENT_ENROLLMENT_STATUS_QUERY_PATH: &str = "/agent/enrollment/status/query";

pub const MAX_AGENT_POLL_MESSAGES: usize = 32;
/// Maximum JSON bytes in one NDJSON channel frame, excluding its LF delimiter.
pub const MAX_AGENT_CHANNEL_FRAME_BYTES: usize = MAX_CONTROL_MESSAGE_BYTES;

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub playground_id: Option<crate::PlaygroundId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<crate::SnapshotId>,
    pub index_version: WireIndexVersion,
    pub page_number: u32,
    pub max_records: u16,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentIndexPageQueryPayload {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.index_version.validate()?;
        if self.playground_id.is_some() == self.snapshot_id.is_some() {
            return Err(ProtocolError::InvalidField {
                field: "playground_id",
                reason: "exactly one of playground_id or snapshot_id must be present".to_owned(),
            });
        }
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
                "snapshot_id",
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
pub struct AgentManifestPageQueryPayload {
    pub tenant_id: TenantId,
    pub job_id: JobId,
    pub artifact_id: crate::ArtifactId,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub manifest_id: ManifestId,
    pub page_number: u32,
    #[schemars(range(min = 1, max = 4096))]
    pub max_chunks: u16,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentManifestPageQueryPayload {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.max_chunks == 0 || usize::from(self.max_chunks) > MAX_RECORDS_PER_PAGE {
            return Err(ProtocolError::InvalidField {
                field: "max_chunks",
                reason: format!("must be in 1..={MAX_RECORDS_PER_PAGE}"),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "job_id",
                "artifact_id",
                "manifest_id",
                "page_number",
                "max_chunks",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentManifestPageQueryResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub manifest_id: ManifestId,
    pub total_size: DecimalU64,
    pub chunking: WireChunkingStrategy,
    pub page_number: u32,
    #[schemars(range(min = 1))]
    pub page_count: u32,
    #[schemars(length(max = 4096))]
    pub chunks: Vec<WireChunkRef>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentManifestPageQueryResponse {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        if self.page_count == 0 || self.page_number >= self.page_count {
            return Err(ProtocolError::InvalidField {
                field: "page_number",
                reason: format!(
                    "page {} is outside page_count {}",
                    self.page_number, self.page_count
                ),
            });
        }
        if self.chunks.len() > MAX_RECORDS_PER_PAGE {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "Manifest page chunks",
                limit: MAX_RECORDS_PER_PAGE,
                actual: self.chunks.len(),
            });
        }
        validate_manifest_page_chunks(
            self.total_size,
            self.chunking,
            self.page_number,
            self.page_count,
            &self.chunks,
        )?;
        validate_extension_keys(
            &self.extensions,
            &[
                "protocol_version",
                "request_id",
                "manifest_id",
                "total_size",
                "chunking",
                "page_number",
                "page_count",
                "chunks",
            ],
        )
    }
}

fn validate_manifest_page_chunks(
    total_size: DecimalU64,
    chunking: WireChunkingStrategy,
    page_number: u32,
    page_count: u32,
    chunks: &[WireChunkRef],
) -> ProtocolResult<()> {
    if total_size.get() == 0 {
        if page_number != 0 || page_count != 1 || !chunks.is_empty() {
            return Err(ProtocolError::InvalidField {
                field: "chunks",
                reason: "an empty Manifest must be represented by one empty page".to_owned(),
            });
        }
        return Ok(());
    }
    if chunks.is_empty() {
        return Err(ProtocolError::InvalidField {
            field: "chunks",
            reason: "a non-empty Manifest page must contain at least one chunk".to_owned(),
        });
    }
    if chunking == WireChunkingStrategy::WholeFile && (page_count != 1 || chunks.len() != 1) {
        return Err(ProtocolError::InvalidField {
            field: "chunks",
            reason: "a non-empty WholeFile Manifest must contain exactly one chunk on one page"
                .to_owned(),
        });
    }

    let mut next_offset = chunks[0].offset.get();
    if page_number == 0 && next_offset != 0 {
        return Err(ProtocolError::InvalidField {
            field: "chunks",
            reason: "the first Manifest page must begin at byte offset zero".to_owned(),
        });
    }
    for chunk in chunks {
        if chunk.size.get() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "chunks",
                reason: "chunk size must be greater than zero".to_owned(),
            });
        }
        if chunk.offset.get() != next_offset {
            return Err(ProtocolError::InvalidField {
                field: "chunks",
                reason: "chunks within a Manifest page must be contiguous".to_owned(),
            });
        }
        next_offset = next_offset.checked_add(chunk.size.get()).ok_or_else(|| {
            ProtocolError::InvalidField {
                field: "chunks",
                reason: "chunk offset plus size exceeds u64".to_owned(),
            }
        })?;
        validate_extension_keys(&chunk.extensions, &["object_id", "offset", "size"])?;
    }
    if next_offset > total_size.get() {
        return Err(ProtocolError::InvalidField {
            field: "chunks",
            reason: "Manifest page extends beyond total_size".to_owned(),
        });
    }
    if page_number + 1 == page_count && next_offset != total_size.get() {
        return Err(ProtocolError::InvalidField {
            field: "chunks",
            reason: "the final Manifest page must end at total_size".to_owned(),
        });
    }
    Ok(())
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

/// Channel Open deliberately reuses the unary session-open payload. The surrounding authenticated
/// request supplies the complete Agent, installation, boot, request, clock, and proof identity.
pub type AgentChannelOpenPayload = AgentSessionOpenPayload;

/// Generic acknowledgement for an upstream Heartbeat, Report, or Close frame.
///
/// The containing downstream frame's `correlation_id` identifies the acknowledged message and its
/// `session_generation` supplies the fence. `acknowledged_sequence` detects accidental cross-stream
/// correlation when message IDs are replayed from durable storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentChannelAck {
    pub acknowledged_sequence: SequenceNumber,
    pub resource_version: ResourceVersion,
    pub replayed: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentChannelAck {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("acknowledged_sequence", self.acknowledged_sequence.get())?;
        validate_extension_keys(
            &self.extensions,
            &["acknowledged_sequence", "resource_version", "replayed"],
        )
    }
}

/// Agent-to-Server messages carried in one NDJSON request stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "payload")]
pub enum AgentChannelUpstreamMessage {
    #[serde(rename = "channel.open")]
    Open(AgentSessionOpenPayload),
    #[serde(rename = "session.heartbeat")]
    Heartbeat(AgentHeartbeatReportPayload),
    #[serde(rename = "job.report")]
    Report(AgentJobReportCreatePayload),
    #[serde(rename = "channel.close")]
    Close(AgentSessionClosePayload),
}

/// Signed channel material carried as the payload of [`AgentAuthenticatedRequest`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentChannelUpstreamPayload {
    pub sequence: SequenceNumber,
    pub message_id: MessageId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<MessageId>,
    #[serde(flatten)]
    pub message: AgentChannelUpstreamMessage,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentChannelUpstreamMessage {
    #[must_use]
    pub fn supports_type(message_type: &str) -> bool {
        matches!(
            message_type,
            "channel.open" | "session.heartbeat" | "job.report" | "channel.close"
        )
    }
}

/// Server-to-Agent messages carried in one NDJSON response stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "payload")]
pub enum AgentChannelDownstreamMessage {
    #[serde(rename = "channel.opened")]
    Opened(AgentSessionOpenResponse),
    #[serde(rename = "job.assignment")]
    Assignment(Box<JobAssignment>),
    #[serde(rename = "job.decision")]
    Decision(JobDecision),
    #[serde(rename = "channel.ack")]
    Ack(AgentChannelAck),
    #[serde(rename = "protocol.error")]
    Error(ControlError),
}

impl AgentChannelDownstreamMessage {
    #[must_use]
    pub fn supports_type(message_type: &str) -> bool {
        matches!(
            message_type,
            "channel.opened" | "job.assignment" | "job.decision" | "channel.ack" | "protocol.error"
        )
    }
}

/// One Agent-to-Server frame. HTTP/2 DATA boundaries have no protocol meaning; the wire boundary
/// is one LF-terminated JSON object and the LF is not included in the size limit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentChannelUpstreamFrame {
    #[serde(flatten)]
    pub request: AgentAuthenticatedRequest<AgentChannelUpstreamPayload>,
}

impl AgentChannelUpstreamFrame {
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        validate_channel_json_line(bytes)?;
        let value = parse_unique_json(bytes)?;
        let payload = value.get("payload").ok_or_else(|| {
            invalid_channel_field("payload", "signed channel frame has no payload")
        })?;
        validate_supported_channel_type(payload, AgentChannelUpstreamMessage::supports_type)?;
        let frame: Self = serde_json::from_value(value)?;
        frame.validate()?;
        Ok(frame)
    }

    pub fn encode_ndjson(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        encode_channel_ndjson(self)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        let payload = &self.request.payload;
        validate_positive("sequence", payload.sequence.get())?;
        validate_extension_keys(
            &payload.extensions,
            &[
                "sequence",
                "message_id",
                "correlation_id",
                "type",
                "payload",
            ],
        )?;
        match &payload.message {
            AgentChannelUpstreamMessage::Open(open) => {
                self.request.validate_open()?;
                if payload.sequence.get() != 1 {
                    return Err(invalid_channel_field(
                        "sequence",
                        "the signed Open frame must have sequence 1",
                    ));
                }
                if payload.correlation_id.is_some() {
                    return Err(invalid_channel_field(
                        "correlation_id",
                        "the signed Open frame cannot correlate a prior message",
                    ));
                }
                validate_extension_keys(
                    &open.extensions,
                    &["mount_identity_digest", "expected_resource_version"],
                )?;
            }
            AgentChannelUpstreamMessage::Heartbeat(heartbeat) => {
                self.request.validate_session()?;
                heartbeat.heartbeat.validate()?;
                heartbeat.mount_report.validate()?;
                if heartbeat.heartbeat.agent_id != self.request.agent_id
                    || heartbeat.mount_report.agent_id != self.request.agent_id
                    || heartbeat.mount_report.installation_id != self.request.installation_id
                    || heartbeat.mount_report.boot_id != self.request.boot_id
                    || Some(heartbeat.mount_report.session_generation)
                        != self.request.session_generation
                {
                    return Err(invalid_channel_field(
                        "payload",
                        "Heartbeat identity or session fence differs from its signed request",
                    ));
                }
                validate_extension_keys(&heartbeat.extensions, &["heartbeat", "mount_report"])?;
            }
            AgentChannelUpstreamMessage::Report(report) => {
                self.request.validate_session()?;
                report.report.validate()?;
                if Some(report.report.session_generation) != self.request.session_generation {
                    return Err(invalid_channel_field(
                        "session_generation",
                        "Job Report envelope uses a different session generation",
                    ));
                }
                if !matches!(
                    report.report.message,
                    ControlMessage::Accepted(_)
                        | ControlMessage::Progress(_)
                        | ControlMessage::Prepared(_)
                        | ControlMessage::Failed(_)
                        | ControlMessage::Finalized(_)
                ) {
                    return Err(invalid_channel_field(
                        "payload.report.type",
                        "Job Report must carry Accepted, Progress, Prepared, Failed, or Finalized",
                    ));
                }
                validate_extension_keys(&report.extensions, &["tenant_id", "report"])?;
            }
            AgentChannelUpstreamMessage::Close(close) => {
                self.request.validate_session()?;
                validate_extension_keys(&close.extensions, &["expected_resource_version"])?;
            }
        }
        validate_channel_encoded_size(self)
    }

    /// Verifies this frame is the exact successor of `previous` (`None` means stream start).
    pub fn validate_sequence_after(&self, previous: Option<SequenceNumber>) -> ProtocolResult<()> {
        validate_next_channel_sequence(self.request.payload.sequence, previous)
    }

    /// Checks the durable fence after the Opened response has established it.
    pub fn validate_session_generation(&self, expected: SessionGeneration) -> ProtocolResult<()> {
        if self.request.session_generation == Some(expected) {
            Ok(())
        } else {
            Err(invalid_channel_field(
                "session_generation",
                "does not match the established channel generation",
            ))
        }
    }

    /// BLAKE3 over the complete unsigned authenticated request, including every identity and the
    /// channel sequence, message ID, correlation, type, and message payload.
    pub fn computed_unsigned_body_digest(&self) -> ProtocolResult<crate::ContentDigest> {
        self.request.computed_unsigned_body_digest()
    }

    pub fn computed_open_unsigned_body_digest(&self) -> ProtocolResult<crate::ContentDigest> {
        if !matches!(
            &self.request.payload.message,
            AgentChannelUpstreamMessage::Open(_)
        ) {
            return Err(invalid_channel_field(
                "type",
                "only a channel.open frame has an Open proof digest",
            ));
        }
        self.computed_unsigned_body_digest()
    }

    pub fn signing_bytes(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        self.request.signing_bytes(AGENT_SESSION_CHANNEL_OPEN_PATH)
    }

    pub fn verify(&self) -> ProtocolResult<()> {
        self.validate()?;
        self.request.verify(AGENT_SESSION_CHANNEL_OPEN_PATH)
    }

    pub fn open_signing_bytes(&self) -> ProtocolResult<Vec<u8>> {
        if !matches!(
            &self.request.payload.message,
            AgentChannelUpstreamMessage::Open(_)
        ) {
            return Err(invalid_channel_field(
                "type",
                "only a channel.open frame can open the channel",
            ));
        }
        self.signing_bytes()
    }

    pub fn verify_open(&self) -> ProtocolResult<()> {
        self.open_signing_bytes()?;
        self.verify()
    }
}

/// One Server-to-Agent frame in the LF-delimited HTTP/2 response stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentChannelDownstreamFrame {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub sequence: SequenceNumber,
    pub message_id: MessageId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<MessageId>,
    pub session_generation: SessionGeneration,
    pub sent_at_unix_ms: UnixMillis,
    #[serde(flatten)]
    pub message: AgentChannelDownstreamMessage,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentChannelDownstreamFrame {
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        validate_channel_json_line(bytes)?;
        let value = parse_unique_json(bytes)?;
        validate_supported_channel_type(&value, AgentChannelDownstreamMessage::supports_type)?;
        let frame: Self = serde_json::from_value(value)?;
        frame.validate()?;
        Ok(frame)
    }

    pub fn encode_ndjson(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        encode_channel_ndjson(self)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_channel_frame_common(
            self.protocol_version,
            self.sequence,
            Some(self.session_generation),
            &self.extensions,
        )?;
        match &self.message {
            AgentChannelDownstreamMessage::Opened(payload) => {
                if self.sequence.get() != 1 {
                    return Err(invalid_channel_field(
                        "sequence",
                        "the Opened response must have sequence 1",
                    ));
                }
                if self.correlation_id.is_none() {
                    return Err(invalid_channel_field(
                        "correlation_id",
                        "the Opened response must correlate the signed Open frame",
                    ));
                }
                if payload.protocol_version != self.protocol_version
                    || payload.session_generation != self.session_generation
                {
                    return Err(invalid_channel_field(
                        "session_generation",
                        "Opened payload differs from its channel frame fence",
                    ));
                }
                validate_extension_keys(
                    &payload.extensions,
                    &[
                        "protocol_version",
                        "request_id",
                        "agent_id",
                        "session_id",
                        "session_generation",
                        "agent_mount_id",
                        "mount_generation",
                        "owner_generation",
                        "resource_version",
                        "opened_at_unix_ms",
                        "replayed",
                    ],
                )?;
            }
            AgentChannelDownstreamMessage::Assignment(payload) => {
                if self.correlation_id.is_some() {
                    return Err(invalid_channel_field(
                        "correlation_id",
                        "Assignment is a new delivery and cannot correlate an upstream frame",
                    ));
                }
                payload.validate()?;
            }
            AgentChannelDownstreamMessage::Decision(payload) => {
                if self.correlation_id.is_some() {
                    return Err(invalid_channel_field(
                        "correlation_id",
                        "Decision is a new delivery and cannot correlate an upstream frame",
                    ));
                }
                payload.validate()?;
            }
            AgentChannelDownstreamMessage::Ack(payload) => {
                if self.correlation_id.is_none() {
                    return Err(invalid_channel_field(
                        "correlation_id",
                        "Ack must correlate an upstream frame",
                    ));
                }
                payload.validate()?;
            }
            AgentChannelDownstreamMessage::Error(payload) => payload.validate()?,
        }
        validate_channel_encoded_size(self)
    }

    pub fn validate_sequence_after(&self, previous: Option<SequenceNumber>) -> ProtocolResult<()> {
        validate_next_channel_sequence(self.sequence, previous)
    }

    pub fn validate_session_generation(&self, expected: SessionGeneration) -> ProtocolResult<()> {
        if self.session_generation == expected {
            Ok(())
        } else {
            Err(invalid_channel_field(
                "session_generation",
                "does not match the established channel generation",
            ))
        }
    }
}

/// Incremental LF boundary decoder. It deliberately ignores HTTP/2 DATA frame boundaries.
#[derive(Debug, Default)]
pub struct AgentChannelNdjsonDecoder {
    pending: Vec<u8>,
}

impl AgentChannelNdjsonDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends arbitrary H2 DATA bytes and returns every complete JSON line in order.
    pub fn push(&mut self, bytes: &[u8]) -> ProtocolResult<Vec<Vec<u8>>> {
        let mut frames = Vec::new();
        for &byte in bytes {
            match byte {
                b'\n' => {
                    if self.pending.is_empty() {
                        return Err(invalid_channel_field(
                            "frame",
                            "empty NDJSON channel frames are not allowed",
                        ));
                    }
                    frames.push(std::mem::take(&mut self.pending));
                }
                b'\r' => {
                    return Err(invalid_channel_field(
                        "frame",
                        "channel framing requires LF and rejects CR/CRLF",
                    ));
                }
                _ => {
                    if self.pending.len() == MAX_AGENT_CHANNEL_FRAME_BYTES {
                        return Err(ProtocolError::LimitExceeded {
                            limit_name: "Agent channel frame bytes",
                            limit: MAX_AGENT_CHANNEL_FRAME_BYTES,
                            actual: MAX_AGENT_CHANNEL_FRAME_BYTES + 1,
                        });
                    }
                    self.pending.push(byte);
                }
            }
        }
        Ok(frames)
    }

    /// A cleanly ended NDJSON stream cannot retain a partial, non-LF-terminated frame.
    pub fn finish(&self) -> ProtocolResult<()> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(invalid_channel_field(
                "frame",
                "channel ended with a partial NDJSON frame",
            ))
        }
    }

    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }
}

fn validate_channel_json_line(bytes: &[u8]) -> ProtocolResult<()> {
    if bytes.is_empty() {
        return Err(invalid_channel_field(
            "frame",
            "channel frame JSON cannot be empty",
        ));
    }
    if bytes.len() > MAX_AGENT_CHANNEL_FRAME_BYTES {
        return Err(ProtocolError::LimitExceeded {
            limit_name: "Agent channel frame bytes",
            limit: MAX_AGENT_CHANNEL_FRAME_BYTES,
            actual: bytes.len(),
        });
    }
    if bytes.iter().any(|byte| matches!(byte, b'\r' | b'\n')) {
        return Err(invalid_channel_field(
            "frame",
            "decode_json accepts exactly one NDJSON line without its LF delimiter",
        ));
    }
    Ok(())
}

fn validate_supported_channel_type(
    value: &serde_json::Value,
    supports: fn(&str) -> bool,
) -> ProtocolResult<()> {
    let message_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid_channel_field("type", "missing or non-string channel frame type"))?;
    if supports(message_type) {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedMessageType(
            message_type.to_owned(),
        ))
    }
}

fn validate_channel_frame_common(
    protocol_version: ProtocolVersion,
    sequence: SequenceNumber,
    session_generation: Option<SessionGeneration>,
    extensions: &Extensions,
) -> ProtocolResult<()> {
    if protocol_version != PROTOCOL_VERSION_V1 {
        return Err(ProtocolError::UnsupportedProtocolVersion(
            protocol_version.get(),
        ));
    }
    validate_positive("sequence", sequence.get())?;
    if let Some(generation) = session_generation {
        validate_positive("session_generation", generation.get())?;
    }
    validate_extension_keys(
        extensions,
        &[
            "protocol_version",
            "sequence",
            "message_id",
            "correlation_id",
            "session_generation",
            "sent_at_unix_ms",
            "proof",
            "type",
            "payload",
        ],
    )
}

fn validate_next_channel_sequence(
    sequence: SequenceNumber,
    previous: Option<SequenceNumber>,
) -> ProtocolResult<()> {
    let expected = previous.map_or(Some(1), |value| value.get().checked_add(1));
    if expected == Some(sequence.get()) {
        Ok(())
    } else {
        Err(invalid_channel_field(
            "sequence",
            "channel frame sequence is not the exact next value",
        ))
    }
}

fn validate_channel_encoded_size<T: Serialize>(frame: &T) -> ProtocolResult<()> {
    let encoded = serde_json::to_vec(frame)?;
    if encoded.len() > MAX_AGENT_CHANNEL_FRAME_BYTES {
        Err(ProtocolError::LimitExceeded {
            limit_name: "Agent channel frame bytes",
            limit: MAX_AGENT_CHANNEL_FRAME_BYTES,
            actual: encoded.len(),
        })
    } else {
        Ok(())
    }
}

fn encode_channel_ndjson<T: Serialize>(frame: &T) -> ProtocolResult<Vec<u8>> {
    let mut encoded = serde_json::to_vec(frame)?;
    if encoded.len() > MAX_AGENT_CHANNEL_FRAME_BYTES {
        return Err(ProtocolError::LimitExceeded {
            limit_name: "Agent channel frame bytes",
            limit: MAX_AGENT_CHANNEL_FRAME_BYTES,
            actual: encoded.len(),
        });
    }
    encoded.push(b'\n');
    Ok(encoded)
}

fn invalid_channel_field(field: &'static str, reason: impl Into<String>) -> ProtocolError {
    ProtocolError::InvalidField {
        field,
        reason: reason.into(),
    }
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
    ManifestPage(AgentAuthenticatedRequest<AgentManifestPageQueryPayload>),
    SessionClose(AgentAuthenticatedRequest<AgentSessionClosePayload>),
    ChannelUpstream(AgentChannelUpstreamFrame),
    ChannelDownstream(AgentChannelDownstreamFrame),
}

fn validate_agent_action_path(path: &'static str) -> ProtocolResult<()> {
    if matches!(
        path,
        AGENT_SESSION_OPEN_PATH
            | AGENT_SESSION_CHANNEL_OPEN_PATH
            | AGENT_SESSION_HEARTBEAT_REPORT_PATH
            | AGENT_SESSION_MESSAGE_LIST_QUERY_PATH
            | AGENT_JOB_REPORT_CREATE_PATH
            | AGENT_JOB_METADATA_BATCH_STAGE_PATH
            | AGENT_JOB_METADATA_PAGE_STAGE_PATH
            | AGENT_JOB_INDEX_PAGE_QUERY_PATH
            | AGENT_JOB_MANIFEST_PAGE_QUERY_PATH
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

    fn unsigned_channel_open() -> AgentChannelUpstreamFrame {
        AgentChannelUpstreamFrame {
            request: AgentAuthenticatedRequest {
                protocol_version: PROTOCOL_VERSION_V1,
                request_id: RequestId::new("channel-request-a").unwrap(),
                agent_id: AgentId::new("agent-a").unwrap(),
                installation_id: AgentInstallationId::new("installation-a").unwrap(),
                boot_id: AgentBootId::new("boot-a").unwrap(),
                session_id: None,
                session_generation: None,
                signed_at_unix_ms: UnixMillis::new(10),
                payload: AgentChannelUpstreamPayload {
                    sequence: SequenceNumber::new(1),
                    message_id: MessageId::new("channel-open-a").unwrap(),
                    correlation_id: None,
                    message: AgentChannelUpstreamMessage::Open(AgentSessionOpenPayload {
                        mount_identity_digest: AgentMountIdentityDigest::new(digest()),
                        expected_resource_version: ResourceVersion::new(7),
                        extensions: Extensions::new(),
                    }),
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
            },
        }
    }

    fn sign_channel_open(key_pair: &Ed25519KeyPair) -> AgentChannelUpstreamFrame {
        let mut frame = unsigned_channel_open();
        frame.request.proof.possession.public_key_spki =
            Ed25519PublicKeySpki::from_public_key_bytes(
                key_pair.public_key().as_ref().try_into().unwrap(),
            );
        frame.request.proof.unsigned_body_digest = frame.computed_unsigned_body_digest().unwrap();
        let signing_bytes = frame.signing_bytes().unwrap();
        frame.request.proof.possession.signature =
            Ed25519Signature::new(key_pair.sign(&signing_bytes).as_ref().to_vec()).unwrap();
        frame
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
    fn agent_contract_rejects_central_object_payload_actions() {
        assert!(validate_agent_action_path("/agent/job/object/missing/query").is_err());
        assert!(validate_agent_action_path("/agent/job/object/upload").is_err());
        assert!(validate_agent_action_path(AGENT_JOB_INDEX_PAGE_QUERY_PATH).is_ok());
        assert!(validate_agent_action_path(AGENT_JOB_MANIFEST_PAGE_QUERY_PATH).is_ok());
        assert!(validate_agent_action_path(AGENT_JOB_METADATA_PAGE_STAGE_PATH).is_ok());
    }

    #[test]
    fn manifest_page_contract_validates_limits_pagination_and_chunk_coverage() {
        let payload = AgentManifestPageQueryPayload {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            artifact_id: crate::ArtifactId::new("artifact-a").unwrap(),
            manifest_id: ManifestId::from_bytes([0x22; 32]),
            page_number: 0,
            max_chunks: 4096,
            extensions: Extensions::new(),
        };
        payload.validate().unwrap();
        let mut invalid_payload = payload;
        invalid_payload.max_chunks = 0;
        assert!(invalid_payload.validate().is_err());

        let mut response = AgentManifestPageQueryResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id: RequestId::new("request-manifest").unwrap(),
            manifest_id: ManifestId::from_bytes([0x22; 32]),
            total_size: DecimalU64::new(8),
            chunking: WireChunkingStrategy::FastCdc,
            page_number: 0,
            page_count: 1,
            chunks: vec![
                WireChunkRef {
                    object_id: neoengram_core::ObjectId::from_bytes([0x33; 32]),
                    offset: DecimalU64::new(0),
                    size: DecimalU64::new(3),
                    extensions: Extensions::new(),
                },
                WireChunkRef {
                    object_id: neoengram_core::ObjectId::from_bytes([0x44; 32]),
                    offset: DecimalU64::new(3),
                    size: DecimalU64::new(5),
                    extensions: Extensions::new(),
                },
            ],
            extensions: Extensions::new(),
        };
        response.validate().unwrap();

        response.page_count = 0;
        assert!(response.validate().is_err());
        response.page_count = 1;
        response.chunks[1].offset = DecimalU64::new(4);
        assert!(response.validate().is_err());
    }

    #[test]
    fn manifest_page_contract_accepts_only_one_empty_page_for_an_empty_manifest() {
        let mut response = AgentManifestPageQueryResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id: RequestId::new("request-empty-manifest").unwrap(),
            manifest_id: ManifestId::from_bytes([0x55; 32]),
            total_size: DecimalU64::new(0),
            chunking: WireChunkingStrategy::FastCdc,
            page_number: 0,
            page_count: 1,
            chunks: Vec::new(),
            extensions: Extensions::new(),
        };
        response.validate().unwrap();
        response.page_count = 2;
        assert!(response.validate().is_err());
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

    #[test]
    fn channel_signature_binds_identity_fence_sequence_and_message() {
        let document = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
        let key_pair = Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap();
        let frame = sign_channel_open(&key_pair);
        frame.verify().unwrap();

        let mut tampered = frame.clone();
        tampered.request.agent_id = AgentId::new("agent-b").unwrap();
        assert!(tampered.verify().is_err());
        let mut tampered = frame.clone();
        tampered.request.payload.sequence = SequenceNumber::new(2);
        assert!(tampered.verify().is_err());
        let mut tampered = frame.clone();
        tampered.request.payload.message_id = MessageId::new("channel-open-b").unwrap();
        assert!(tampered.verify().is_err());

        let mut established = frame;
        established.request.session_id = Some(SessionId::new("session-a").unwrap());
        established.request.session_generation = Some(SessionGeneration::new(2));
        established.request.payload.sequence = SequenceNumber::new(2);
        established.request.payload.message_id = MessageId::new("channel-close-a").unwrap();
        established.request.payload.message =
            AgentChannelUpstreamMessage::Close(AgentSessionClosePayload {
                expected_resource_version: ResourceVersion::new(8),
                extensions: Extensions::new(),
            });
        established.request.proof.unsigned_body_digest =
            established.computed_unsigned_body_digest().unwrap();
        let signing_bytes = established.signing_bytes().unwrap();
        established.request.proof.possession.signature =
            Ed25519Signature::new(key_pair.sign(&signing_bytes).as_ref().to_vec()).unwrap();
        established.verify().unwrap();
        established
            .validate_session_generation(SessionGeneration::new(2))
            .unwrap();
        established.request.session_generation = Some(SessionGeneration::new(3));
        assert!(established.verify().is_err());
    }

    #[test]
    fn channel_ndjson_decoder_ignores_h2_data_boundaries() {
        let document = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
        let key_pair = Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap();
        let frame = sign_channel_open(&key_pair);
        let encoded = frame.encode_ndjson().unwrap();
        let split_a = encoded.len() / 3;
        let split_b = split_a * 2;
        let mut decoder = AgentChannelNdjsonDecoder::new();
        assert!(decoder.push(&encoded[..split_a]).unwrap().is_empty());
        assert!(decoder.push(&encoded[split_a..split_b]).unwrap().is_empty());
        let lines = decoder.push(&encoded[split_b..]).unwrap();
        assert_eq!(lines.len(), 1);
        let decoded = AgentChannelUpstreamFrame::decode_json(&lines[0]).unwrap();
        decoded.verify().unwrap();
        decoder.finish().unwrap();
    }

    #[test]
    fn channel_boundaries_reject_invalid_or_oversized_lines() {
        let mut decoder = AgentChannelNdjsonDecoder::new();
        assert!(decoder.push(b"\n").is_err());
        let mut decoder = AgentChannelNdjsonDecoder::new();
        assert!(decoder.push(b"{}\r\n").is_err());
        let mut decoder = AgentChannelNdjsonDecoder::new();
        decoder.push(b"{").unwrap();
        assert!(decoder.finish().is_err());
        let mut decoder = AgentChannelNdjsonDecoder::new();
        decoder
            .push(&vec![b' '; MAX_AGENT_CHANNEL_FRAME_BYTES])
            .unwrap();
        assert!(decoder.push(b"x").is_err());

        let duplicate = br#"{"protocol_version":1,"protocol_version":1}"#;
        assert!(AgentChannelUpstreamFrame::decode_json(duplicate).is_err());
    }

    #[test]
    fn downstream_ack_requires_correlation_and_matching_opened_fence() {
        let mut ack = AgentChannelDownstreamFrame {
            protocol_version: PROTOCOL_VERSION_V1,
            sequence: SequenceNumber::new(2),
            message_id: MessageId::new("ack-a").unwrap(),
            correlation_id: Some(MessageId::new("heartbeat-a").unwrap()),
            session_generation: SessionGeneration::new(3),
            sent_at_unix_ms: UnixMillis::new(20),
            message: AgentChannelDownstreamMessage::Ack(AgentChannelAck {
                acknowledged_sequence: SequenceNumber::new(2),
                resource_version: ResourceVersion::new(9),
                replayed: false,
                extensions: Extensions::new(),
            }),
            extensions: Extensions::new(),
        };
        ack.validate().unwrap();
        ack.validate_sequence_after(Some(SequenceNumber::new(1)))
            .unwrap();
        ack.correlation_id = None;
        assert!(ack.validate().is_err());
    }

    #[test]
    fn index_page_query_requires_exactly_one_resource_target() {
        let mut payload = AgentIndexPageQueryPayload {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            artifact_id: crate::ArtifactId::new("artifact-a").unwrap(),
            playground_id: None,
            snapshot_id: Some(crate::SnapshotId::new("snapshot-a").unwrap()),
            index_version: WireIndexVersion {
                revision: crate::IndexRevision::new(1),
                digest: digest(),
                extensions: Extensions::new(),
            },
            page_number: 0,
            max_records: 128,
            extensions: Extensions::new(),
        };
        payload.validate().unwrap();

        payload.playground_id = Some(crate::PlaygroundId::new("playground-a").unwrap());
        assert!(payload.validate().is_err());
        payload.snapshot_id = None;
        payload.validate().unwrap();
        payload.playground_id = None;
        assert!(payload.validate().is_err());
    }
}
