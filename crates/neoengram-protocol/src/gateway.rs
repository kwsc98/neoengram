use std::{collections::BTreeSet, fmt};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ring::signature::{UnparsedPublicKey, ED25519};
use schemars::JsonSchema;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

use crate::validation::{parse_unique_json, CONTENT_DIGEST_PATTERN};
use crate::{
    domain_separated_jcs_bytes, AgentId, CertificateGeneration, ContentDigest,
    Ed25519PublicKeySpki, Ed25519Signature, Extensions, GatewayConnectionId, GatewayPoolId,
    GatewayReplicaId, Generation, ProtocolError, ProtocolResult, ProtocolVersion, RequestId,
    RouteGeneration, SequenceNumber, SessionGeneration, TraceId, UnixMillis, PROTOCOL_VERSION_V1,
};

pub const GATEWAY_CONTROL_CHANNEL_PATH: &str = "/gateway/control/channel/open";
/// Same-pool Replica endpoint for one-hop delivery to the authoritative Agent owner.
pub const GATEWAY_PEER_FORWARD_PATH: &str = "/gateway/peer/forward";
/// Canonical capabilities advertised by the current Gateway control implementation.
pub const GATEWAY_CAPABILITY_AGENT_CONTROL_V1: &str = "agent-control-v1";
pub const GATEWAY_CAPABILITY_PEER_FORWARD_V1: &str = "peer-forward-v1";
pub const GATEWAY_CAPABILITY_ROUTE_LEASE_V1: &str = "route-lease-v1";
/// Central-initiated server-authenticated bootstrap exchange for a pending Replica.
pub const GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH: &str = "/gateway/bootstrap/challenge";
/// One-time delivery endpoint for the workload certificate issued after activation.
pub const GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH: &str = "/gateway/bootstrap/certificate";
pub const MAX_GATEWAY_CONTROL_FRAME_BYTES: usize = 12 * 1024 * 1024;
pub const MAX_GATEWAY_OPAQUE_PAYLOAD_BYTES: usize = 9 * 1024 * 1024;
pub const MAX_GATEWAY_STREAM_CHUNK_BYTES: usize = 256 * 1024;
pub const MAX_GATEWAY_CAPABILITIES: usize = 128;
pub const MAX_GATEWAY_TEXT_BYTES: usize = 1024;
pub const MAX_GATEWAY_PEER_ENDPOINT_BYTES: usize = 2048;
pub const MAX_GATEWAY_HOPS: u8 = 1;
/// A Gateway frame cannot reserve work beyond this lifetime, even when its absolute deadline is
/// still in the future. This bounds queue and replay-resource retention for authenticated peers.
pub const MAX_GATEWAY_FRAME_LIFETIME_MS: u64 = 5 * 60 * 1_000;
/// Small allowance for clock skew when validating a sender's `sent_at_unix_ms`.
pub const GATEWAY_FRAME_CLOCK_SKEW_MS: u64 = 30_000;
pub const AGENT_ROUTE_LEASE_TTL_MS: u64 = 30_000;
pub const AGENT_ROUTE_LEASE_RENEW_INTERVAL_MS: u64 = 10_000;
pub const GATEWAY_BOOTSTRAP_CHALLENGE_TTL_MS: u64 = 60_000;
/// A Central-issued peer directory is deliberately short lived.  A revoked or rotated Replica
/// therefore disappears from every Gateway even when a heartbeat/control connection is delayed.
pub const GATEWAY_PEER_DIRECTORY_TTL_MS: u64 = 30_000;
pub const MAX_GATEWAY_PEER_DIRECTORY_ENTRIES: usize = 256;
const GATEWAY_BOOTSTRAP_NONCE_BYTES: usize = 32;
const GATEWAY_BOOTSTRAP_MAX_ISSUER_CHAIN_DEPTH: usize = 8;
const GATEWAY_BOOTSTRAP_SIGNING_DOMAIN_V1: &str = "neoengram-gateway-replica-activation-v1";

#[must_use]
pub fn gateway_capabilities_v1() -> BTreeSet<String> {
    [
        GATEWAY_CAPABILITY_AGENT_CONTROL_V1,
        GATEWAY_CAPABILITY_PEER_FORWARD_V1,
        GATEWAY_CAPABILITY_ROUTE_LEASE_V1,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// Wire representation of the Central-issued Replica activation challenge. The activation token
/// itself is intentionally absent; only its digest is bound into the proof material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayBootstrapChallenge {
    pub request_id: RequestId,
    pub edge_cluster_id: crate::EdgeClusterId,
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: GatewayReplicaId,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub activation_token_digest: ContentDigest,
    #[schemars(length(min = 43, max = 43))]
    pub nonce: String,
    pub issued_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
}

impl GatewayBootstrapChallenge {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.issued_at_unix_ms.get() == 0
            || self.expires_at_unix_ms.get() <= self.issued_at_unix_ms.get()
            || self
                .expires_at_unix_ms
                .get()
                .saturating_sub(self.issued_at_unix_ms.get())
                > GATEWAY_BOOTSTRAP_CHALLENGE_TTL_MS
        {
            return invalid(
                "expires_at_unix_ms",
                "Gateway bootstrap challenge must have a positive lifetime of at most 60 seconds",
            );
        }
        let nonce = URL_SAFE_NO_PAD.decode(self.nonce.as_bytes()).map_err(|_| {
            ProtocolError::InvalidField {
                field: "nonce",
                reason: "Gateway bootstrap nonce must be canonical base64url".to_owned(),
            }
        })?;
        if nonce.len() != GATEWAY_BOOTSTRAP_NONCE_BYTES
            || URL_SAFE_NO_PAD.encode(&nonce) != self.nonce
        {
            return invalid(
                "nonce",
                "Gateway bootstrap nonce must contain exactly 32 bytes",
            );
        }
        Ok(())
    }

    pub fn signing_bytes(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        #[derive(Serialize)]
        struct SigningInput<'a> {
            version: u8,
            request_id: &'a RequestId,
            edge_cluster_id: &'a crate::EdgeClusterId,
            gateway_pool_id: &'a GatewayPoolId,
            gateway_replica_id: &'a GatewayReplicaId,
            activation_token_digest: ContentDigest,
            nonce: &'a str,
            issued_at_unix_ms: UnixMillis,
            expires_at_unix_ms: UnixMillis,
        }
        domain_separated_jcs_bytes(
            GATEWAY_BOOTSTRAP_SIGNING_DOMAIN_V1,
            &SigningInput {
                version: 1,
                request_id: &self.request_id,
                edge_cluster_id: &self.edge_cluster_id,
                gateway_pool_id: &self.gateway_pool_id,
                gateway_replica_id: &self.gateway_replica_id,
                activation_token_digest: self.activation_token_digest,
                nonce: &self.nonce,
                issued_at_unix_ms: self.issued_at_unix_ms,
                expires_at_unix_ms: self.expires_at_unix_ms,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayBootstrapChallengeRequest {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub challenge: GatewayBootstrapChallenge,
}

impl GatewayBootstrapChallengeRequest {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        self.challenge.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayBootstrapProofResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub proof: crate::AgentBootstrapProof,
}

impl GatewayBootstrapProofResponse {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        self.proof.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayBootstrapCertificateDelivery {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub request_id: RequestId,
    pub certificate_generation: CertificateGeneration,
    pub leaf_certificate_der: GatewayOpaqueBytes,
    pub issuer_chain_der: Vec<GatewayOpaqueBytes>,
}

impl GatewayBootstrapCertificateDelivery {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        if self.certificate_generation.get() == 0 {
            return invalid(
                "certificate_generation",
                "Gateway workload certificate generation must be positive",
            );
        }
        if self.leaf_certificate_der.as_bytes().is_empty() {
            return invalid(
                "leaf_certificate_der",
                "Gateway workload leaf certificate must not be empty",
            );
        }
        if self.issuer_chain_der.is_empty()
            || self.issuer_chain_der.len() > GATEWAY_BOOTSTRAP_MAX_ISSUER_CHAIN_DEPTH
            || self
                .issuer_chain_der
                .iter()
                .any(|certificate| certificate.as_bytes().is_empty())
        {
            return invalid(
                "issuer_chain_der",
                "Gateway workload issuer chain must contain between one and eight certificates",
            );
        }
        Ok(())
    }
}

const CENTRAL_PAYLOAD_SIGNING_DOMAIN_V1: &str = "neoengram-central-payload-v1";
const GATEWAY_BASE64URL_PATTERN: &str = r"^[A-Za-z0-9_-]*$";

/// Opaque bytes encoded as canonical base64url without padding.
#[derive(Clone, PartialEq, Eq, JsonSchema)]
#[schemars(transparent)]
pub struct GatewayOpaqueBytes(
    #[schemars(
        with = "String",
        length(min = 0, max = 12582912),
        regex(pattern = GATEWAY_BASE64URL_PATTERN)
    )]
    Vec<u8>,
);

impl GatewayOpaqueBytes {
    pub fn new(bytes: impl Into<Vec<u8>>) -> ProtocolResult<Self> {
        let bytes = bytes.into();
        if bytes.len() > MAX_GATEWAY_OPAQUE_PAYLOAD_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "gateway payload",
                limit: MAX_GATEWAY_OPAQUE_PAYLOAD_BYTES,
                actual: bytes.len(),
            });
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for GatewayOpaqueBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayOpaqueBytes")
            .field("length", &self.0.len())
            .finish()
    }
}

impl Serialize for GatewayOpaqueBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for GatewayOpaqueBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        if encoded.contains('=') {
            return Err(de::Error::custom(
                "gateway payload must use unpadded base64url",
            ));
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(de::Error::custom)?;
        let value = Self::new(bytes).map_err(de::Error::custom)?;
        if URL_SAFE_NO_PAD.encode(value.as_bytes()) != encoded {
            return Err(de::Error::custom(
                "gateway payload is not canonical base64url",
            ));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayAgentAction {
    EnrollmentBootstrap,
    EnrollmentStatusQuery,
    SessionOpen,
    SessionChannelOpen,
    SessionHeartbeatReport,
    SessionMessageListQuery,
    JobReportCreate,
    JobMetadataBatchStage,
    JobMetadataPageStage,
    JobIndexPageQuery,
    JobManifestPageQuery,
    SessionClose,
}

impl GatewayAgentAction {
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::EnrollmentBootstrap => crate::AGENT_ENROLLMENT_BOOTSTRAP_PATH,
            Self::EnrollmentStatusQuery => crate::AGENT_ENROLLMENT_STATUS_QUERY_PATH,
            Self::SessionOpen => crate::AGENT_SESSION_OPEN_PATH,
            Self::SessionChannelOpen => crate::AGENT_SESSION_CHANNEL_OPEN_PATH,
            Self::SessionHeartbeatReport => crate::AGENT_SESSION_HEARTBEAT_REPORT_PATH,
            Self::SessionMessageListQuery => crate::AGENT_SESSION_MESSAGE_LIST_QUERY_PATH,
            Self::JobReportCreate => crate::AGENT_JOB_REPORT_CREATE_PATH,
            Self::JobMetadataBatchStage => crate::AGENT_JOB_METADATA_BATCH_STAGE_PATH,
            Self::JobMetadataPageStage => crate::AGENT_JOB_METADATA_PAGE_STAGE_PATH,
            Self::JobIndexPageQuery => crate::AGENT_JOB_INDEX_PAGE_QUERY_PATH,
            Self::JobManifestPageQuery => crate::AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
            Self::SessionClose => crate::AGENT_SESSION_CLOSE_PATH,
        }
    }

    #[must_use]
    pub fn from_path(path: &str) -> Option<Self> {
        match path {
            crate::AGENT_ENROLLMENT_BOOTSTRAP_PATH => Some(Self::EnrollmentBootstrap),
            crate::AGENT_ENROLLMENT_STATUS_QUERY_PATH => Some(Self::EnrollmentStatusQuery),
            crate::AGENT_SESSION_OPEN_PATH => Some(Self::SessionOpen),
            crate::AGENT_SESSION_CHANNEL_OPEN_PATH => Some(Self::SessionChannelOpen),
            crate::AGENT_SESSION_HEARTBEAT_REPORT_PATH => Some(Self::SessionHeartbeatReport),
            crate::AGENT_SESSION_MESSAGE_LIST_QUERY_PATH => Some(Self::SessionMessageListQuery),
            crate::AGENT_JOB_REPORT_CREATE_PATH => Some(Self::JobReportCreate),
            crate::AGENT_JOB_METADATA_BATCH_STAGE_PATH => Some(Self::JobMetadataBatchStage),
            crate::AGENT_JOB_METADATA_PAGE_STAGE_PATH => Some(Self::JobMetadataPageStage),
            crate::AGENT_JOB_INDEX_PAGE_QUERY_PATH => Some(Self::JobIndexPageQuery),
            crate::AGENT_JOB_MANIFEST_PAGE_QUERY_PATH => Some(Self::JobManifestPageQuery),
            crate::AGENT_SESSION_CLOSE_PATH => Some(Self::SessionClose),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayReplicaHello {
    pub edge_cluster_id: crate::EdgeClusterId,
    #[schemars(length(min = 1, max = 1024))]
    pub software_version: String,
    #[schemars(length(min = 1, max = 128))]
    pub supported_protocol_versions: BTreeSet<ProtocolVersion>,
    #[schemars(length(max = 128))]
    pub capabilities: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayReplicaHeartbeat {
    pub connected_agents: u32,
    pub active_streams: u32,
    pub queue_depth: u32,
}

/// Central's short-lived allow-list for authenticated Replica-to-Replica forwarding.
///
/// The mTLS CA and URI SAN establish that a peer is *a* Gateway Replica.  They do not establish
/// that the leaf is still the Registry-authoritative credential for that Replica.  Central sends
/// this directory after opening a control session and refreshes it with every accepted heartbeat;
/// a Gateway must reject a peer whose leaf fingerprint is absent, stale, or associated with an old
/// certificate generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayPeerDirectoryEntry {
    pub gateway_replica_id: GatewayReplicaId,
    pub certificate_generation: CertificateGeneration,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub certificate_fingerprint: ContentDigest,
}

/// Versioned, bounded and short-lived peer credential directory delivered by Central.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayPeerDirectory {
    pub directory_generation: Generation,
    pub issued_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
    #[schemars(length(max = MAX_GATEWAY_PEER_DIRECTORY_ENTRIES))]
    pub replicas: Vec<GatewayPeerDirectoryEntry>,
}

impl GatewayPeerDirectory {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.directory_generation.get() == 0 {
            return invalid(
                "directory_generation",
                "Gateway peer directory generation must be positive",
            );
        }
        if self.issued_at_unix_ms.get() == 0
            || self.expires_at_unix_ms.get() <= self.issued_at_unix_ms.get()
            || self
                .expires_at_unix_ms
                .get()
                .saturating_sub(self.issued_at_unix_ms.get())
                > GATEWAY_PEER_DIRECTORY_TTL_MS
        {
            return invalid(
                "expires_at_unix_ms",
                "Gateway peer directory must have a positive lifetime of at most 30 seconds",
            );
        }
        if self.replicas.len() > MAX_GATEWAY_PEER_DIRECTORY_ENTRIES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "Gateway peer directory entries",
                limit: MAX_GATEWAY_PEER_DIRECTORY_ENTRIES,
                actual: self.replicas.len(),
            });
        }
        let mut replica_ids = BTreeSet::new();
        for replica in &self.replicas {
            if replica.certificate_generation.get() == 0 {
                return invalid(
                    "certificate_generation",
                    "Gateway peer directory certificate generation must be positive",
                );
            }
            if !replica_ids.insert(&replica.gateway_replica_id) {
                return invalid(
                    "replicas",
                    "Gateway peer directory cannot contain duplicate Replica IDs",
                );
            }
        }
        Ok(())
    }

    pub fn validate_at(&self, now_unix_ms: UnixMillis) -> ProtocolResult<()> {
        self.validate()?;
        if self.issued_at_unix_ms.get()
            > now_unix_ms
                .get()
                .saturating_add(GATEWAY_FRAME_CLOCK_SKEW_MS)
        {
            return invalid(
                "issued_at_unix_ms",
                "Gateway peer directory issue time is too far in the future",
            );
        }
        if self.expires_at_unix_ms.get() <= now_unix_ms.get() {
            return invalid("expires_at_unix_ms", "Gateway peer directory has expired");
        }
        Ok(())
    }
}

/// Short aliases keep the protocol vocabulary usable by callers that refer to the control
/// message as simply `PeerDirectory`.
pub type PeerDirectory = GatewayPeerDirectory;
pub type PeerDirectoryEntry = GatewayPeerDirectoryEntry;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayDrain {
    pub deadline_unix_ms: UnixMillis,
    #[schemars(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayAgentRequest {
    pub action: GatewayAgentAction,
    pub stream_id: GatewayConnectionId,
    pub body: GatewayOpaqueBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayAgentResponse {
    pub stream_id: GatewayConnectionId,
    #[schemars(range(min = 100, max = 599))]
    pub status: u16,
    #[schemars(length(min = 1, max = 1024))]
    pub content_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub body: GatewayOpaqueBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayAgentStreamOpen {
    pub stream_id: GatewayConnectionId,
    pub action: GatewayAgentAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayAgentStreamData {
    pub stream_id: GatewayConnectionId,
    #[schemars(
        with = "String",
        length(min = 0, max = 349526),
        regex(pattern = GATEWAY_BASE64URL_PATTERN)
    )]
    pub chunk: GatewayOpaqueBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayAgentStreamEnd {
    pub stream_id: GatewayConnectionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayRouteLeaseRequest {
    pub agent_id: AgentId,
    pub owner_replica_id: GatewayReplicaId,
    pub agent_connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_generation: Option<RouteGeneration>,
    pub requested_expires_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayRouteLeaseGranted {
    pub agent_id: AgentId,
    pub owner_replica_id: GatewayReplicaId,
    pub agent_connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    pub route_generation: RouteGeneration,
    pub lease_expires_at_unix_ms: UnixMillis,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayRouteFence {
    pub agent_id: AgentId,
    pub route_generation: RouteGeneration,
    #[schemars(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Central-authoritative route and opaque Agent frame forwarded to the current owner Replica.
///
/// `target_peer_endpoint` is copied from Central's persisted `GatewayReplica` record. A Gateway
/// must never discover alternatives, broadcast this request, or use the endpoint to change owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayPeerForwardRequest {
    pub source_replica_id: GatewayReplicaId,
    pub target_replica_id: GatewayReplicaId,
    #[schemars(length(min = 1, max = 2048))]
    pub target_peer_endpoint: String,
    pub agent_id: AgentId,
    pub agent_connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    pub route_generation: RouteGeneration,
    /// Exactly one LF-terminated `AgentChannelDownstreamFrame`, preserved byte-for-byte.
    #[schemars(
        with = "String",
        length(min = 4, max = 1398103),
        regex(pattern = GATEWAY_BASE64URL_PATTERN)
    )]
    pub frame: GatewayOpaqueBytes,
}

/// Positive acknowledgement for one exact route generation and Agent connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayPeerForwardAccepted {
    pub source_replica_id: GatewayReplicaId,
    pub target_replica_id: GatewayReplicaId,
    pub agent_id: AgentId,
    pub agent_connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    pub route_generation: RouteGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayErrorCode {
    ProtocolInvalid,
    IdentityRejected,
    RouteUnavailable,
    RouteFenced,
    DeadlineExceeded,
    ResourceExhausted,
    Internal,
}

impl GatewayErrorCode {
    /// Whether callers may retry after this class of error without changing identity or fencing.
    #[must_use]
    pub const fn permits_retry(self) -> bool {
        matches!(
            self,
            Self::RouteUnavailable | Self::ResourceExhausted | Self::Internal
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayControlError {
    pub code: GatewayErrorCode,
    #[schemars(length(min = 1, max = 1024))]
    pub detail: String,
    pub retryable: bool,
}

impl GatewayControlError {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_text("detail", &self.detail)?;
        if self.retryable && !self.code.permits_retry() {
            return invalid(
                "retryable",
                "protocol, identity, fencing, and deadline errors must fail closed",
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GatewayBackpressure {
    pub retry_after_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum GatewayControlMessage {
    ReplicaHello(GatewayReplicaHello),
    ReplicaHeartbeat(GatewayReplicaHeartbeat),
    #[serde(rename = "peer_directory")]
    PeerDirectory(GatewayPeerDirectory),
    Drain(GatewayDrain),
    AgentRequest(GatewayAgentRequest),
    AgentResponse(GatewayAgentResponse),
    AgentStreamOpen(GatewayAgentStreamOpen),
    AgentStreamData(GatewayAgentStreamData),
    AgentStreamEnd(GatewayAgentStreamEnd),
    RouteAcquire(GatewayRouteLeaseRequest),
    RouteRenew(GatewayRouteLeaseRequest),
    RouteRelease(GatewayRouteLeaseRequest),
    RouteGranted(GatewayRouteLeaseGranted),
    RouteFence(GatewayRouteFence),
    PeerForward(GatewayPeerForwardRequest),
    PeerForwardAccepted(GatewayPeerForwardAccepted),
    Backpressure(GatewayBackpressure),
    Error(GatewayControlError),
}

impl GatewayControlMessage {
    #[must_use]
    pub fn supports_type(message_type: &str) -> bool {
        matches!(
            message_type,
            "replica_hello"
                | "replica_heartbeat"
                | "peer_directory"
                | "drain"
                | "agent_request"
                | "agent_response"
                | "agent_stream_open"
                | "agent_stream_data"
                | "agent_stream_end"
                | "route_acquire"
                | "route_renew"
                | "route_release"
                | "route_granted"
                | "route_fence"
                | "peer_forward"
                | "peer_forward_accepted"
                | "backpressure"
                | "error"
        )
    }
}

/// One hop-authenticated frame between Central and a Gateway Replica or between two Replicas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GatewayControlFrame {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: GatewayReplicaId,
    pub connection_id: GatewayConnectionId,
    pub sequence: SequenceNumber,
    pub request_id: RequestId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<TraceId>,
    pub sent_at_unix_ms: UnixMillis,
    pub deadline_unix_ms: UnixMillis,
    #[schemars(range(max = 1))]
    pub hop_count: u8,
    #[serde(flatten)]
    pub message: GatewayControlMessage,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

/// Incremental LF-delimited Gateway frame decoder.
///
/// HTTP/2 DATA boundaries are transport details and do not delimit protocol frames. Callers feed
/// every received byte chunk into this decoder and decode each returned JSON line with
/// [`GatewayControlFrame::decode_json`].
#[derive(Debug, Default)]
pub struct GatewayControlNdjsonDecoder {
    pending: Vec<u8>,
}

impl GatewayControlNdjsonDecoder {
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
                        return invalid("frame", "empty Gateway NDJSON frames are not allowed");
                    }
                    frames.push(std::mem::take(&mut self.pending));
                }
                b'\r' => {
                    return invalid("frame", "Gateway framing requires LF and rejects CR/CRLF");
                }
                _ => {
                    if self.pending.len() == MAX_GATEWAY_CONTROL_FRAME_BYTES {
                        return Err(ProtocolError::LimitExceeded {
                            limit_name: "Gateway control frame bytes",
                            limit: MAX_GATEWAY_CONTROL_FRAME_BYTES,
                            actual: MAX_GATEWAY_CONTROL_FRAME_BYTES + 1,
                        });
                    }
                    self.pending.push(byte);
                }
            }
        }
        Ok(frames)
    }

    /// A cleanly ended stream cannot retain a partial, non-LF-terminated frame.
    pub fn finish(&self) -> ProtocolResult<()> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            invalid(
                "frame",
                "Gateway control stream ended with a partial NDJSON frame",
            )
        }
    }

    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }
}

impl GatewayControlFrame {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        if self.sequence.get() == 0 {
            return invalid("sequence", "Gateway frame sequence must be positive");
        }
        if self.sent_at_unix_ms.get() == 0
            || self.deadline_unix_ms.get() <= self.sent_at_unix_ms.get()
        {
            return invalid(
                "deadline_unix_ms",
                "Gateway frame deadline must follow its positive send time",
            );
        }
        if self.hop_count > MAX_GATEWAY_HOPS {
            return invalid("hop_count", "Gateway forwarding is limited to one hop");
        }
        validate_message(&self.message)?;
        match &self.message {
            GatewayControlMessage::PeerForward(request) => {
                if self.gateway_replica_id != request.source_replica_id {
                    return invalid(
                        "gateway_replica_id",
                        "peer forward envelope must identify its source Replica",
                    );
                }
                if self.hop_count == 1 && request.source_replica_id == request.target_replica_id {
                    return invalid(
                        "target_replica_id",
                        "a forwarded peer hop must target another Replica",
                    );
                }
            }
            GatewayControlMessage::PeerForwardAccepted(accepted) => {
                let expected = if self.hop_count == 0 {
                    &accepted.source_replica_id
                } else {
                    &accepted.target_replica_id
                };
                if &self.gateway_replica_id != expected {
                    return invalid(
                        "gateway_replica_id",
                        "peer forward acknowledgement has the wrong reporting Replica",
                    );
                }
            }
            _ => {}
        }
        crate::validation::validate_extension_keys(
            &self.extensions,
            &[
                "protocol_version",
                "gateway_pool_id",
                "gateway_replica_id",
                "connection_id",
                "sequence",
                "request_id",
                "trace_id",
                "sent_at_unix_ms",
                "deadline_unix_ms",
                "hop_count",
                "type",
                "payload",
            ],
        )?;
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > MAX_GATEWAY_CONTROL_FRAME_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "gateway frame bytes",
                limit: MAX_GATEWAY_CONTROL_FRAME_BYTES,
                actual: encoded.len(),
            });
        }
        Ok(())
    }

    /// Validates the frame and rejects work whose deadline has elapsed or whose lifetime/time
    /// origin could be used to retain resources indefinitely.
    pub fn validate_at(&self, now_unix_ms: UnixMillis) -> ProtocolResult<()> {
        self.validate()?;
        if self
            .deadline_unix_ms
            .get()
            .saturating_sub(self.sent_at_unix_ms.get())
            > MAX_GATEWAY_FRAME_LIFETIME_MS
        {
            return invalid(
                "deadline_unix_ms",
                "Gateway frame lifetime exceeds the five-minute maximum",
            );
        }
        if self.sent_at_unix_ms.get()
            > now_unix_ms
                .get()
                .saturating_add(GATEWAY_FRAME_CLOCK_SKEW_MS)
        {
            return invalid(
                "sent_at_unix_ms",
                "Gateway frame send time is too far in the future",
            );
        }
        if self.deadline_unix_ms.get() <= now_unix_ms.get() {
            return invalid(
                "deadline_unix_ms",
                "Gateway frame deadline has already elapsed",
            );
        }
        Ok(())
    }

    pub fn encode_ndjson(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        let mut encoded = serde_json::to_vec(self)?;
        encoded.push(b'\n');
        Ok(encoded)
    }

    /// Decodes one bounded I-JSON frame, rejecting duplicate members and unknown message types.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        if bytes.len() > MAX_GATEWAY_CONTROL_FRAME_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "gateway frame bytes",
                limit: MAX_GATEWAY_CONTROL_FRAME_BYTES,
                actual: bytes.len(),
            });
        }
        let value = parse_unique_json(bytes)?;
        let message_type = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "type",
                reason: "missing or non-string Gateway message type".to_owned(),
            })?;
        if !GatewayControlMessage::supports_type(message_type) {
            return Err(ProtocolError::UnsupportedMessageType(
                message_type.to_owned(),
            ));
        }
        let frame: Self = serde_json::from_value(value)?;
        frame.validate()?;
        Ok(frame)
    }
}

/// Central-signed bytes carried unchanged through Gateway hops.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CentralSignedPayload {
    #[schemars(length(min = 1, max = 1024))]
    pub key_id: String,
    pub certificate_generation: CertificateGeneration,
    pub signed_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub payload_digest: ContentDigest,
    pub payload: GatewayOpaqueBytes,
    pub signature: Ed25519Signature,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl CentralSignedPayload {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.validate_without_signature()
    }

    pub fn signing_bytes(&self) -> ProtocolResult<Vec<u8>> {
        self.validate_without_signature()?;
        domain_separated_jcs_bytes(
            CENTRAL_PAYLOAD_SIGNING_DOMAIN_V1,
            &CentralPayloadSigningInput {
                key_id: &self.key_id,
                certificate_generation: self.certificate_generation,
                signed_at_unix_ms: self.signed_at_unix_ms,
                expires_at_unix_ms: self.expires_at_unix_ms,
                payload_digest: self.payload_digest,
                payload: &self.payload,
                extensions: &self.extensions,
            },
        )
    }

    pub fn verify(&self, public_key: &Ed25519PublicKeySpki) -> ProtocolResult<()> {
        self.validate()?;
        UnparsedPublicKey::new(&ED25519, public_key.as_public_key_bytes())
            .verify(&self.signing_bytes()?, self.signature.as_bytes())
            .map_err(|_| ProtocolError::InvalidField {
                field: "signature",
                reason: "Central payload signature verification failed".to_owned(),
            })
    }

    /// Verifies integrity and rejects payloads outside their signed validity window.
    pub fn verify_at(
        &self,
        public_key: &Ed25519PublicKeySpki,
        now_unix_ms: UnixMillis,
    ) -> ProtocolResult<()> {
        self.verify(public_key)?;
        if now_unix_ms.get() < self.signed_at_unix_ms.get() {
            return invalid(
                "signed_at_unix_ms",
                "signed Central payload is not valid at the supplied time",
            );
        }
        if now_unix_ms.get() >= self.expires_at_unix_ms.get() {
            return invalid("expires_at_unix_ms", "signed Central payload has expired");
        }
        Ok(())
    }

    fn validate_without_signature(&self) -> ProtocolResult<()> {
        validate_text("key_id", &self.key_id)?;
        if self.certificate_generation.get() == 0 {
            return invalid(
                "certificate_generation",
                "signed Central payload certificate generation must be positive",
            );
        }
        if self.signed_at_unix_ms.get() == 0
            || self.expires_at_unix_ms.get() <= self.signed_at_unix_ms.get()
        {
            return invalid(
                "expires_at_unix_ms",
                "signed Central payload expiry must follow its positive signing time",
            );
        }
        if self.payload_digest != ContentDigest::hash(self.payload.as_bytes()) {
            return invalid(
                "payload_digest",
                "signed Central payload digest does not match its bytes",
            );
        }
        crate::validation::validate_extension_keys(
            &self.extensions,
            &[
                "key_id",
                "certificate_generation",
                "signed_at_unix_ms",
                "expires_at_unix_ms",
                "payload_digest",
                "payload",
                "signature",
            ],
        )
    }
}

#[derive(Serialize)]
struct CentralPayloadSigningInput<'a> {
    key_id: &'a str,
    certificate_generation: CertificateGeneration,
    signed_at_unix_ms: UnixMillis,
    expires_at_unix_ms: UnixMillis,
    payload_digest: ContentDigest,
    payload: &'a GatewayOpaqueBytes,
    #[serde(flatten)]
    extensions: &'a Extensions,
}

fn validate_message(message: &GatewayControlMessage) -> ProtocolResult<()> {
    match message {
        GatewayControlMessage::ReplicaHello(hello) => {
            validate_text("software_version", &hello.software_version)?;
            if hello.supported_protocol_versions.is_empty()
                || hello.supported_protocol_versions.len() > MAX_GATEWAY_CAPABILITIES
                || !hello
                    .supported_protocol_versions
                    .contains(&PROTOCOL_VERSION_V1)
            {
                return invalid(
                    "supported_protocol_versions",
                    "Gateway Replica must advertise protocol v1 within the bounded version set",
                );
            }
            validate_capabilities(&hello.capabilities)
        }
        GatewayControlMessage::PeerDirectory(directory) => directory.validate(),
        GatewayControlMessage::Drain(drain) => {
            validate_text("reason", &drain.reason)?;
            if drain.deadline_unix_ms.get() == 0 {
                return invalid("deadline_unix_ms", "drain deadline must be positive");
            }
            Ok(())
        }
        GatewayControlMessage::AgentResponse(response) => {
            if !(100..=599).contains(&response.status) {
                return invalid("status", "Agent response HTTP status is outside 100..=599");
            }
            validate_text("content_type", &response.content_type)
        }
        GatewayControlMessage::AgentStreamOpen(open) => {
            if open.action != GatewayAgentAction::SessionChannelOpen {
                return invalid(
                    "action",
                    "stream open only supports the Agent control-channel action",
                );
            }
            Ok(())
        }
        GatewayControlMessage::AgentStreamData(data) => {
            if data.chunk.as_bytes().len() > MAX_GATEWAY_STREAM_CHUNK_BYTES {
                return Err(ProtocolError::LimitExceeded {
                    limit_name: "gateway stream chunk",
                    limit: MAX_GATEWAY_STREAM_CHUNK_BYTES,
                    actual: data.chunk.as_bytes().len(),
                });
            }
            Ok(())
        }
        GatewayControlMessage::RouteAcquire(request)
        | GatewayControlMessage::RouteRenew(request)
        | GatewayControlMessage::RouteRelease(request) => {
            if request.session_generation.get() == 0 {
                return invalid(
                    "session_generation",
                    "route lease session generation must be positive",
                );
            }
            if request
                .route_generation
                .is_some_and(|generation| generation.get() == 0)
            {
                return invalid(
                    "route_generation",
                    "route generation must be positive when supplied",
                );
            }
            if request.requested_expires_at_unix_ms.get() == 0 {
                return invalid(
                    "requested_expires_at_unix_ms",
                    "route lease expiry must be positive",
                );
            }
            Ok(())
        }
        GatewayControlMessage::RouteGranted(granted) => {
            if granted.session_generation.get() == 0 || granted.route_generation.get() == 0 {
                return invalid(
                    "route_generation",
                    "granted session and route generations must be positive",
                );
            }
            if granted.lease_expires_at_unix_ms.get() == 0 {
                return invalid(
                    "lease_expires_at_unix_ms",
                    "granted route lease expiry must be positive",
                );
            }
            Ok(())
        }
        GatewayControlMessage::RouteFence(fence) => {
            if fence.route_generation.get() == 0 {
                return invalid(
                    "route_generation",
                    "fenced route generation must be positive",
                );
            }
            validate_text("reason", &fence.reason)
        }
        GatewayControlMessage::PeerForward(request) => validate_peer_forward_request(request),
        GatewayControlMessage::PeerForwardAccepted(accepted) => validate_peer_forward_identity(
            &accepted.source_replica_id,
            &accepted.target_replica_id,
            accepted.session_generation,
            accepted.route_generation,
        ),
        GatewayControlMessage::Error(error) => error.validate(),
        GatewayControlMessage::ReplicaHeartbeat(_)
        | GatewayControlMessage::AgentRequest(_)
        | GatewayControlMessage::AgentStreamEnd(_)
        | GatewayControlMessage::Backpressure(_) => Ok(()),
    }
}

fn validate_peer_forward_request(request: &GatewayPeerForwardRequest) -> ProtocolResult<()> {
    validate_peer_forward_identity(
        &request.source_replica_id,
        &request.target_replica_id,
        request.session_generation,
        request.route_generation,
    )?;
    validate_text_max(
        "target_peer_endpoint",
        &request.target_peer_endpoint,
        MAX_GATEWAY_PEER_ENDPOINT_BYTES,
    )?;
    validate_peer_endpoint_origin(&request.target_peer_endpoint)?;
    let frame = request.frame.as_bytes();
    if frame.is_empty()
        || frame.len() > crate::MAX_AGENT_CHANNEL_FRAME_BYTES.saturating_add(1)
        || frame.last() != Some(&b'\n')
        || frame[..frame.len().saturating_sub(1)]
            .iter()
            .any(|byte| matches!(byte, b'\n' | b'\r'))
    {
        return invalid(
            "frame",
            "peer forwarding requires exactly one bounded LF-terminated Agent frame",
        );
    }
    Ok(())
}

/// Production peer endpoints are HTTPS origins. Plain HTTP is available only for literal
/// loopback origins used by the loopback-only development transport; accepting arbitrary HTTP
/// here would let a Central directory downgrade a peer hop to unauthenticated network traffic.
fn validate_peer_endpoint_origin(endpoint: &str) -> ProtocolResult<()> {
    let parsed = Url::parse(endpoint).map_err(|_| ProtocolError::InvalidField {
        field: "target_peer_endpoint",
        reason: "peer endpoint must be a canonical HTTPS origin (or loopback HTTP origin in development)".to_owned(),
    })?;
    let canonical = parsed.as_str().strip_suffix('/').unwrap_or(parsed.as_str());
    let origin_shape_valid = !parsed.cannot_be_a_base()
        && parsed.host_str().is_some()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.path() == "/"
        && parsed.query().is_none()
        && parsed.fragment().is_none()
        && canonical == endpoint;
    if !origin_shape_valid {
        return invalid(
            "target_peer_endpoint",
            "peer endpoint must be a canonical origin without credentials, path, query, or fragment",
        );
    }
    if parsed.scheme() == "https" {
        return Ok(());
    }
    let loopback = parsed.scheme() == "http"
        && parsed.host().is_some_and(|host| match host {
            url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
            url::Host::Ipv4(address) => address.is_loopback(),
            url::Host::Ipv6(address) => address.is_loopback(),
        });
    if loopback {
        Ok(())
    } else {
        invalid(
            "target_peer_endpoint",
            "peer endpoint must use HTTPS unless it is a literal loopback HTTP origin",
        )
    }
}

fn validate_peer_forward_identity(
    source: &GatewayReplicaId,
    target: &GatewayReplicaId,
    session_generation: SessionGeneration,
    route_generation: RouteGeneration,
) -> ProtocolResult<()> {
    if source == target {
        return invalid(
            "target_replica_id",
            "peer forwarding requires distinct source and owner Replicas",
        );
    }
    if session_generation.get() == 0 || route_generation.get() == 0 {
        return invalid(
            "route_generation",
            "peer forwarding requires positive session and route generations",
        );
    }
    Ok(())
}

fn validate_capabilities(capabilities: &BTreeSet<String>) -> ProtocolResult<()> {
    if capabilities.len() > MAX_GATEWAY_CAPABILITIES {
        return invalid("capabilities", "Gateway capability set is too large");
    }
    for capability in capabilities {
        validate_text("capability", capability)?;
    }
    Ok(())
}

fn validate_text(field: &'static str, value: &str) -> ProtocolResult<()> {
    validate_text_max(field, value, MAX_GATEWAY_TEXT_BYTES)
}

fn validate_text_max(field: &'static str, value: &str, max_chars: usize) -> ProtocolResult<()> {
    if value.is_empty()
        || value.chars().count() > max_chars
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return invalid(
            field,
            "value must be non-empty, bounded, and contain no controls",
        );
    }
    Ok(())
}

fn invalid<T>(field: &'static str, reason: impl Into<String>) -> ProtocolResult<T> {
    Err(ProtocolError::InvalidField {
        field,
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };
    use serde_json::json;

    use super::*;

    fn frame(message: GatewayControlMessage) -> GatewayControlFrame {
        GatewayControlFrame {
            protocol_version: ProtocolVersion::V1,
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            connection_id: GatewayConnectionId::new("connection-a").unwrap(),
            sequence: SequenceNumber::new(1),
            request_id: RequestId::new("request-a").unwrap(),
            trace_id: None,
            sent_at_unix_ms: UnixMillis::new(100),
            deadline_unix_ms: UnixMillis::new(200),
            hop_count: 0,
            message,
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn gateway_frame_round_trips_without_rewriting_agent_payload() {
        let body = br#"{"proof":{"signature":"opaque"}}"#.to_vec();
        let original = frame(GatewayControlMessage::AgentRequest(GatewayAgentRequest {
            action: GatewayAgentAction::EnrollmentBootstrap,
            stream_id: GatewayConnectionId::new("stream-a").unwrap(),
            body: GatewayOpaqueBytes::new(body.clone()).unwrap(),
        }));
        let encoded = original.encode_ndjson().unwrap();
        let decoded = GatewayControlFrame::decode_json(&encoded[..encoded.len() - 1]).unwrap();
        assert_eq!(decoded, original);
        let GatewayControlMessage::AgentRequest(request) = decoded.message else {
            panic!("expected Agent request");
        };
        assert_eq!(request.body.as_bytes(), body);
    }

    #[test]
    fn forwarding_rejects_more_than_one_hop() {
        let mut value = frame(GatewayControlMessage::Backpressure(GatewayBackpressure {
            retry_after_ms: 10,
        }));
        value.hop_count = 2;
        assert!(value.validate().is_err());
    }

    #[test]
    fn peer_forwarding_binds_source_target_fences_and_exactly_one_hop() {
        let request = GatewayPeerForwardRequest {
            source_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            target_replica_id: GatewayReplicaId::new("replica-b").unwrap(),
            target_peer_endpoint: "https://replica-b.gateway.example".to_owned(),
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: RouteGeneration::new(5),
            frame: GatewayOpaqueBytes::new(b"{}\n".to_vec()).unwrap(),
        };
        let direct = frame(GatewayControlMessage::PeerForward(request.clone()));
        direct.validate().unwrap();

        let mut forwarded = direct.clone();
        forwarded.hop_count = 1;
        forwarded.validate().unwrap();

        let mut second_hop = forwarded.clone();
        second_hop.hop_count = 2;
        assert!(second_hop.validate().is_err());

        let mut wrong_envelope = forwarded.clone();
        wrong_envelope.gateway_replica_id = GatewayReplicaId::new("replica-b").unwrap();
        assert!(wrong_envelope.validate().is_err());

        let mut self_target = request.clone();
        self_target.target_replica_id = self_target.source_replica_id.clone();
        assert!(frame(GatewayControlMessage::PeerForward(self_target))
            .validate()
            .is_err());

        let mut stale = request.clone();
        stale.route_generation = RouteGeneration::new(0);
        assert!(frame(GatewayControlMessage::PeerForward(stale))
            .validate()
            .is_err());

        let mut ambiguous = request.clone();
        ambiguous.frame = GatewayOpaqueBytes::new(b"{}\n{}\n".to_vec()).unwrap();
        assert!(frame(GatewayControlMessage::PeerForward(ambiguous))
            .validate()
            .is_err());

        let mut insecure = request.clone();
        insecure.target_peer_endpoint = "http://replica-b.gateway.example".to_owned();
        assert!(frame(GatewayControlMessage::PeerForward(insecure))
            .validate()
            .is_err());

        for endpoint in [
            "http://127.0.0.1:8083",
            "http://[::1]:8083",
            "http://localhost:8083",
        ] {
            let mut loopback = request.clone();
            loopback.target_peer_endpoint = endpoint.to_owned();
            frame(GatewayControlMessage::PeerForward(loopback))
                .validate()
                .expect("canonical loopback HTTP is valid for development transport");
        }
        for endpoint in [
            "http://10.0.0.1:8083",
            "http://gateway.internal:8083",
            "http://127.0.0.1:8083/",
        ] {
            let mut non_loopback = request.clone();
            non_loopback.target_peer_endpoint = endpoint.to_owned();
            assert!(
                frame(GatewayControlMessage::PeerForward(non_loopback))
                    .validate()
                    .is_err(),
                "non-canonical or non-loopback HTTP endpoint must fail: {endpoint}"
            );
        }

        let accepted = GatewayPeerForwardAccepted {
            source_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            target_replica_id: GatewayReplicaId::new("replica-b").unwrap(),
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: RouteGeneration::new(5),
        };
        let mut peer_ack = frame(GatewayControlMessage::PeerForwardAccepted(accepted.clone()));
        peer_ack.gateway_replica_id = accepted.target_replica_id.clone();
        peer_ack.hop_count = 1;
        peer_ack.validate().unwrap();
        let control_ack = frame(GatewayControlMessage::PeerForwardAccepted(accepted));
        control_ack.validate().unwrap();
    }

    #[test]
    fn deadline_validation_rejects_expired_frames() {
        let mut value = frame(GatewayControlMessage::Backpressure(GatewayBackpressure {
            retry_after_ms: 10,
        }));
        value.validate_at(UnixMillis::new(199)).unwrap();
        assert!(matches!(
            value.validate_at(UnixMillis::new(200)),
            Err(ProtocolError::InvalidField {
                field: "deadline_unix_ms",
                ..
            })
        ));
        value.deadline_unix_ms = value.sent_at_unix_ms;
        assert!(value.validate().is_err());

        let mut long_lived = frame(GatewayControlMessage::Backpressure(GatewayBackpressure {
            retry_after_ms: 10,
        }));
        long_lived.deadline_unix_ms = UnixMillis::new(
            long_lived
                .sent_at_unix_ms
                .get()
                .saturating_add(MAX_GATEWAY_FRAME_LIFETIME_MS + 1),
        );
        assert!(matches!(
            long_lived.validate_at(UnixMillis::new(100)),
            Err(ProtocolError::InvalidField {
                field: "deadline_unix_ms",
                ..
            })
        ));

        let mut from_the_future = frame(GatewayControlMessage::Backpressure(GatewayBackpressure {
            retry_after_ms: 10,
        }));
        from_the_future.sent_at_unix_ms = UnixMillis::new(
            100_u64
                .saturating_add(GATEWAY_FRAME_CLOCK_SKEW_MS)
                .saturating_add(1),
        );
        from_the_future.deadline_unix_ms =
            UnixMillis::new(from_the_future.sent_at_unix_ms.get().saturating_add(100));
        assert!(matches!(
            from_the_future.validate_at(UnixMillis::new(100)),
            Err(ProtocolError::InvalidField {
                field: "sent_at_unix_ms",
                ..
            })
        ));
    }

    #[test]
    fn decode_rejects_duplicate_members_and_maps_unknown_types() {
        let duplicate = br#"{
            "protocol_version":1,
            "gateway_pool_id":"pool-a",
            "gateway_replica_id":"replica-a",
            "connection_id":"connection-a",
            "sequence":"1",
            "request_id":"request-a",
            "sent_at_unix_ms":"100",
            "deadline_unix_ms":"200",
            "hop_count":0,
            "hop_count":1,
            "type":"backpressure",
            "payload":{"retry_after_ms":10}
        }"#;
        let error = GatewayControlFrame::decode_json(duplicate).unwrap_err();
        assert_eq!(error.stable_code(), "PROTOCOL_INVALID");
        assert!(error.to_string().contains("duplicate JSON object member"));

        let unknown = br#"{
            "protocol_version":1,
            "gateway_pool_id":"pool-a",
            "gateway_replica_id":"replica-a",
            "connection_id":"connection-a",
            "sequence":"1",
            "request_id":"request-a",
            "sent_at_unix_ms":"100",
            "deadline_unix_ms":"200",
            "hop_count":0,
            "type":"future_message",
            "payload":{}
        }"#;
        let error = GatewayControlFrame::decode_json(unknown).unwrap_err();
        assert_eq!(
            error,
            ProtocolError::UnsupportedMessageType("future_message".to_owned())
        );
        assert_eq!(error.stable_code(), "PROTOCOL_UNSUPPORTED");
    }

    #[test]
    fn decode_preserves_unknown_envelope_extensions() {
        let original = frame(GatewayControlMessage::Backpressure(GatewayBackpressure {
            retry_after_ms: 10,
        }));
        let mut value = serde_json::to_value(original).unwrap();
        value["future_envelope"] = json!({"forwarding_mode": "bounded"});

        let decoded =
            GatewayControlFrame::decode_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(
            decoded.extensions.get("future_envelope"),
            Some(&json!({"forwarding_mode": "bounded"}))
        );
        assert_eq!(
            serde_json::to_value(decoded).unwrap()["future_envelope"],
            json!({"forwarding_mode": "bounded"})
        );
    }

    #[test]
    fn decode_rejects_unknown_message_payload_fields() {
        let original = frame(GatewayControlMessage::ReplicaHeartbeat(
            GatewayReplicaHeartbeat {
                connected_agents: 1,
                active_streams: 2,
                queue_depth: 3,
            },
        ));
        let mut value = serde_json::to_value(original).unwrap();
        value["payload"]["unrecognized_counter"] = json!(4);

        let error =
            GatewayControlFrame::decode_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert_eq!(error.stable_code(), "PROTOCOL_INVALID");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn decode_rejects_oversized_frames_before_parsing() {
        let oversized = vec![b' '; MAX_GATEWAY_CONTROL_FRAME_BYTES + 1];
        assert!(matches!(
            GatewayControlFrame::decode_json(&oversized),
            Err(ProtocolError::LimitExceeded {
                limit_name: "gateway frame bytes",
                ..
            })
        ));
    }

    #[test]
    fn gateway_error_retry_mapping_fails_closed() {
        let rejected = frame(GatewayControlMessage::Error(GatewayControlError {
            code: GatewayErrorCode::IdentityRejected,
            detail: "workload identity does not match the route".to_owned(),
            retryable: true,
        }));
        assert!(matches!(
            rejected.validate(),
            Err(ProtocolError::InvalidField {
                field: "retryable",
                ..
            })
        ));

        let unavailable = frame(GatewayControlMessage::Error(GatewayControlError {
            code: GatewayErrorCode::RouteUnavailable,
            detail: "owner Replica is unavailable".to_owned(),
            retryable: true,
        }));
        unavailable.validate().unwrap();
        assert_eq!(
            serde_json::to_value(unavailable).unwrap()["payload"]["code"],
            json!("route_unavailable")
        );
    }

    #[test]
    fn stream_chunks_are_strictly_bounded() {
        let chunk = GatewayOpaqueBytes::new(vec![0; MAX_GATEWAY_STREAM_CHUNK_BYTES + 1]).unwrap();
        let value = frame(GatewayControlMessage::AgentStreamData(
            GatewayAgentStreamData {
                stream_id: GatewayConnectionId::new("stream-a").unwrap(),
                chunk,
            },
        ));
        assert!(value.validate().is_err());
    }

    #[test]
    fn route_and_certificate_generations_fail_closed_at_runtime() {
        let route = frame(GatewayControlMessage::RouteAcquire(
            GatewayRouteLeaseRequest {
                agent_id: AgentId::new("agent-a").unwrap(),
                owner_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
                agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
                session_generation: SessionGeneration::new(0),
                route_generation: None,
                requested_expires_at_unix_ms: UnixMillis::new(200),
            },
        ));
        assert!(matches!(
            route.validate(),
            Err(ProtocolError::InvalidField {
                field: "session_generation",
                ..
            })
        ));

        let payload = GatewayOpaqueBytes::new(b"assignment".to_vec()).unwrap();
        let signed = CentralSignedPayload {
            key_id: "central-key-a".to_owned(),
            certificate_generation: CertificateGeneration::new(0),
            signed_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: UnixMillis::new(200),
            payload_digest: ContentDigest::hash(payload.as_bytes()),
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions: Extensions::new(),
        };
        assert!(matches!(
            signed.signing_bytes(),
            Err(ProtocolError::InvalidField {
                field: "certificate_generation",
                ..
            })
        ));
    }

    #[test]
    fn central_payload_signature_covers_payload_and_expiry() {
        let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap();
        let public_key = Ed25519PublicKeySpki::from_public_key_bytes(
            key.public_key().as_ref().try_into().unwrap(),
        );
        let payload = GatewayOpaqueBytes::new(b"assignment".to_vec()).unwrap();
        let mut signed = CentralSignedPayload {
            key_id: "central-key-a".to_owned(),
            certificate_generation: CertificateGeneration::new(1),
            signed_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: UnixMillis::new(200),
            payload_digest: ContentDigest::hash(payload.as_bytes()),
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions: Extensions::new(),
        };
        signed.signature =
            Ed25519Signature::new(key.sign(&signed.signing_bytes().unwrap()).as_ref().to_vec())
                .unwrap();
        signed.verify(&public_key).unwrap();
        signed.verify_at(&public_key, UnixMillis::new(150)).unwrap();
        assert!(signed.verify_at(&public_key, UnixMillis::new(99)).is_err());
        assert!(signed.verify_at(&public_key, UnixMillis::new(200)).is_err());

        let mut expiry_tampered = signed.clone();
        expiry_tampered.expires_at_unix_ms = UnixMillis::new(201);
        assert!(expiry_tampered.verify(&public_key).is_err());

        let mut payload_tampered = signed;
        payload_tampered.payload =
            GatewayOpaqueBytes::new(b"different-assignment".to_vec()).unwrap();
        payload_tampered.payload_digest = ContentDigest::hash(payload_tampered.payload.as_bytes());
        assert!(payload_tampered.verify(&public_key).is_err());
    }

    #[test]
    fn central_payload_rejects_extension_collisions_before_signing() {
        let payload = GatewayOpaqueBytes::new(b"assignment".to_vec()).unwrap();
        let mut extensions = Extensions::new();
        extensions.insert("payload".to_owned(), json!("shadowed"));
        let signed = CentralSignedPayload {
            key_id: "central-key-a".to_owned(),
            certificate_generation: CertificateGeneration::new(1),
            signed_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: UnixMillis::new(200),
            payload_digest: ContentDigest::hash(payload.as_bytes()),
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions,
        };

        assert!(matches!(
            signed.signing_bytes(),
            Err(ProtocolError::InvalidField {
                field: "extensions",
                ..
            })
        ));
    }

    #[test]
    fn gateway_actions_round_trip_the_agent_api_paths() {
        let actions = [
            GatewayAgentAction::EnrollmentBootstrap,
            GatewayAgentAction::EnrollmentStatusQuery,
            GatewayAgentAction::SessionOpen,
            GatewayAgentAction::SessionChannelOpen,
            GatewayAgentAction::SessionHeartbeatReport,
            GatewayAgentAction::SessionMessageListQuery,
            GatewayAgentAction::JobReportCreate,
            GatewayAgentAction::JobMetadataBatchStage,
            GatewayAgentAction::JobMetadataPageStage,
            GatewayAgentAction::JobIndexPageQuery,
            GatewayAgentAction::JobManifestPageQuery,
            GatewayAgentAction::SessionClose,
        ];

        for action in actions {
            assert_eq!(GatewayAgentAction::from_path(action.path()), Some(action));
        }
        assert_eq!(GatewayAgentAction::from_path("/unknown"), None);
    }

    #[test]
    fn hello_requires_protocol_v1_and_bounded_capabilities() {
        let hello = GatewayReplicaHello {
            edge_cluster_id: crate::EdgeClusterId::new("cluster-a").unwrap(),
            software_version: "0.2.0".to_owned(),
            supported_protocol_versions: BTreeSet::from([ProtocolVersion::V1]),
            capabilities: BTreeSet::from(["agent-control-v1".to_owned()]),
        };
        frame(GatewayControlMessage::ReplicaHello(hello))
            .validate()
            .unwrap();
    }

    #[test]
    fn gateway_ndjson_decoder_ignores_h2_chunk_boundaries() {
        let first = frame(GatewayControlMessage::Backpressure(GatewayBackpressure {
            retry_after_ms: 10,
        }))
        .encode_ndjson()
        .unwrap();
        let second = frame(GatewayControlMessage::Backpressure(GatewayBackpressure {
            retry_after_ms: 20,
        }))
        .encode_ndjson()
        .unwrap();
        let split = first.len() / 2;
        let mut decoder = GatewayControlNdjsonDecoder::new();
        assert!(decoder.push(&first[..split]).unwrap().is_empty());
        let mut joined = first[split..].to_vec();
        joined.extend_from_slice(&second);
        let lines = decoder.push(&joined).unwrap();
        assert_eq!(lines.len(), 2);
        GatewayControlFrame::decode_json(&lines[0]).unwrap();
        GatewayControlFrame::decode_json(&lines[1]).unwrap();
        decoder.finish().unwrap();
    }

    #[test]
    fn gateway_ndjson_decoder_rejects_ambiguous_or_partial_framing() {
        let mut decoder = GatewayControlNdjsonDecoder::new();
        assert!(decoder.push(b"\n").is_err());

        let mut decoder = GatewayControlNdjsonDecoder::new();
        assert!(decoder.push(b"{}\r\n").is_err());

        let mut decoder = GatewayControlNdjsonDecoder::new();
        assert!(decoder.push(b"{").unwrap().is_empty());
        assert_eq!(decoder.pending_bytes(), 1);
        assert!(decoder.finish().is_err());
    }

    #[test]
    fn bootstrap_wire_contract_binds_digest_and_short_challenge_window() {
        let challenge = GatewayBootstrapChallenge {
            request_id: RequestId::new("request-a").unwrap(),
            edge_cluster_id: crate::EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            activation_token_digest: ContentDigest::hash(b"activation-token"),
            nonce: URL_SAFE_NO_PAD.encode([7_u8; GATEWAY_BOOTSTRAP_NONCE_BYTES]),
            issued_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: UnixMillis::new(100 + GATEWAY_BOOTSTRAP_CHALLENGE_TTL_MS),
        };
        let request = GatewayBootstrapChallengeRequest {
            protocol_version: PROTOCOL_VERSION_V1,
            challenge,
        };
        request.validate().unwrap();
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded: GatewayBootstrapChallengeRequest = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, request);

        let mut expired = request;
        expired.challenge.expires_at_unix_ms = UnixMillis::new(100 + 1);
        assert!(expired.validate().is_ok());
        expired.challenge.expires_at_unix_ms =
            UnixMillis::new(100 + GATEWAY_BOOTSTRAP_CHALLENGE_TTL_MS + 1);
        assert!(expired.validate().is_err());
    }
}
