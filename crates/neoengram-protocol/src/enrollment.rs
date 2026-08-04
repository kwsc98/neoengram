use std::{collections::HashSet, fmt};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use neoengram_core::ContentDigest;
use ring::signature::{UnparsedPublicKey, ED25519};
use schemars::JsonSchema;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::validation::{
    decode_bounded_unique_json, validate_collection_limit, validate_extension_keys,
    validate_nonempty_limited, CONTENT_DIGEST_PATTERN,
};
use crate::{
    domain_separated_jcs_bytes, AgentBootId, AgentEnrollmentId, AgentEnrollmentTokenId, AgentId,
    AgentInstallationId, AgentMountId, EdgeClusterId, Extensions, MountAccessMode, MountGeneration,
    OwnerGeneration, ProtocolError, ProtocolResult, ProtocolVersion, RequestId, ResourceHealth,
    ResourceVersion, SequenceNumber, SessionGeneration, StorageVolumeId, TenantId, UnixMillis,
    VolumeMarkerId, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES, PROTOCOL_VERSION_V1,
};

const BOOTSTRAP_TOKEN_MIN_BYTES: usize = 32;
const BOOTSTRAP_TOKEN_MAX_BYTES: usize = 2_048;
const BOOTSTRAP_TOKEN_MAX_TTL_MS: u64 = 15 * 60 * 1_000;
const MAX_CAPABILITIES: usize = 256;
const ED25519_PUBLIC_KEY_BYTES: usize = 32;
const ED25519_SPKI_BYTES: usize = 44;
const ED25519_SIGNATURE_BYTES: usize = 64;
const ED25519_SPKI_BASE64URL_BYTES: usize = 59;
const ED25519_SIGNATURE_BASE64URL_BYTES: usize = 86;
const BASE64URL_PATTERN: &str = r"^[A-Za-z0-9_-]+$";
const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// Domain for proof-of-possession signatures over v1 bootstrap requests.
pub const AGENT_BOOTSTRAP_SIGNING_DOMAIN_V1: &str = "neoengram-agent-bootstrap-v1";

/// Domain for proof-of-possession signatures over v1 bootstrap status requests.
pub const AGENT_BOOTSTRAP_STATUS_SIGNING_DOMAIN_V1: &str = "neoengram-agent-bootstrap-status-v1";

/// Opaque identity of the PVC named by an enrollment intent.
///
/// The canonical digest input contains only the validated namespace and claim name. EdgeCluster is
/// a separate registry scope, while Tenant is deliberately excluded so one cluster-scoped PVC
/// cannot be admitted by multiple Tenants or under multiple StorageVolume IDs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct PvcIdentityDigest(
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    ContentDigest,
);

impl PvcIdentityDigest {
    #[must_use]
    pub const fn new(digest: ContentDigest) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn as_digest(&self) -> &ContentDigest {
        &self.0
    }

    /// Derives the identity from canonical Kubernetes namespace and claim-name strings.
    pub fn derive(namespace: &str, claim_name: &str) -> ProtocolResult<Self> {
        if !valid_dns_1123_label(namespace) {
            return Err(ProtocolError::InvalidField {
                field: "pvc_namespace",
                reason: "must be a Kubernetes DNS-1123 label".to_owned(),
            });
        }
        if claim_name.is_empty()
            || claim_name.len() > 253
            || !claim_name.split('.').all(valid_dns_1123_label)
        {
            return Err(ProtocolError::InvalidField {
                field: "pvc_claim_name",
                reason: "must be a Kubernetes DNS-1123 subdomain".to_owned(),
            });
        }
        let mut material = Vec::with_capacity(namespace.len() + claim_name.len() + 48);
        material.extend_from_slice(b"neoengram-pvc-identity-v1\0");
        material.extend_from_slice(
            &u64::try_from(namespace.len())
                .expect("namespace length fits in u64")
                .to_le_bytes(),
        );
        material.extend_from_slice(namespace.as_bytes());
        material.extend_from_slice(
            &u64::try_from(claim_name.len())
                .expect("claim-name length fits in u64")
                .to_le_bytes(),
        );
        material.extend_from_slice(claim_name.as_bytes());
        Ok(Self(ContentDigest::hash(material)))
    }
}

impl fmt::Debug for PvcIdentityDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PvcIdentityDigest")
            .field(&"[REDACTED]")
            .finish()
    }
}

impl From<ContentDigest> for PvcIdentityDigest {
    fn from(digest: ContentDigest) -> Self {
        Self::new(digest)
    }
}

impl From<PvcIdentityDigest> for ContentDigest {
    fn from(digest: PvcIdentityDigest) -> Self {
        digest.0
    }
}

/// Opaque identity of the filesystem mounted by one volume-scoped Agent.
///
/// Raw mount sources and filesystem identifiers never cross the Agent boundary. The stable digest
/// is bound during bootstrap and checked on every mount status report.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct AgentMountIdentityDigest(
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    ContentDigest,
);

impl AgentMountIdentityDigest {
    #[must_use]
    pub const fn new(digest: ContentDigest) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn as_digest(&self) -> &ContentDigest {
        &self.0
    }
}

impl fmt::Debug for AgentMountIdentityDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("AgentMountIdentityDigest")
            .field(&"[REDACTED]")
            .finish()
    }
}

impl From<ContentDigest> for AgentMountIdentityDigest {
    fn from(digest: ContentDigest) -> Self {
        Self::new(digest)
    }
}

impl From<AgentMountIdentityDigest> for ContentDigest {
    fn from(digest: AgentMountIdentityDigest) -> Self {
        digest.0
    }
}

/// Canonical RFC 8410 Ed25519 SubjectPublicKeyInfo DER, encoded as base64url without padding.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, JsonSchema)]
#[schemars(transparent)]
pub struct Ed25519PublicKeySpki(
    #[schemars(
        with = "String",
        length(equal = 59),
        regex(pattern = BASE64URL_PATTERN)
    )]
    [u8; ED25519_SPKI_BYTES],
);

impl Ed25519PublicKeySpki {
    /// Parses the exact RFC 8410 DER form. Algorithm parameters and alternate OIDs are rejected.
    pub fn new(der: Vec<u8>) -> ProtocolResult<Self> {
        let der: [u8; ED25519_SPKI_BYTES] =
            der.try_into().map_err(|der: Vec<u8>| ProtocolError::InvalidField {
                field: "public_key_spki",
                reason: format!(
                    "Ed25519 SubjectPublicKeyInfo must contain exactly {ED25519_SPKI_BYTES} DER bytes, observed {}",
                    der.len()
                ),
            })?;
        if der[..ED25519_SPKI_PREFIX.len()] != ED25519_SPKI_PREFIX {
            return Err(ProtocolError::InvalidField {
                field: "public_key_spki",
                reason: "must use the parameter-free RFC 8410 Ed25519 SubjectPublicKeyInfo algorithm identifier"
                    .to_owned(),
            });
        }
        Ok(Self(der))
    }

    /// Constructs canonical SPKI DER around one raw Ed25519 public key.
    #[must_use]
    pub fn from_public_key_bytes(public_key: [u8; ED25519_PUBLIC_KEY_BYTES]) -> Self {
        let mut der = [0_u8; ED25519_SPKI_BYTES];
        der[..ED25519_SPKI_PREFIX.len()].copy_from_slice(&ED25519_SPKI_PREFIX);
        der[ED25519_SPKI_PREFIX.len()..].copy_from_slice(&public_key);
        Self(der)
    }

    /// Returns the complete canonical SubjectPublicKeyInfo DER.
    #[must_use]
    pub const fn as_der(&self) -> &[u8; ED25519_SPKI_BYTES] {
        &self.0
    }

    /// Alias for [`Self::as_der`] for generic byte-oriented consumers.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; ED25519_SPKI_BYTES] {
        self.as_der()
    }

    #[must_use]
    pub const fn into_der(self) -> [u8; ED25519_SPKI_BYTES] {
        self.0
    }

    /// Returns the raw 32-byte Ed25519 verification key from the validated SPKI wrapper.
    #[must_use]
    pub fn as_public_key_bytes(&self) -> &[u8; ED25519_PUBLIC_KEY_BYTES] {
        self.0[ED25519_SPKI_PREFIX.len()..]
            .try_into()
            .expect("validated Ed25519 SPKI contains a 32-byte public key")
    }

    /// Returns BLAKE3 over the exact canonical SPKI DER.
    #[must_use]
    pub fn fingerprint(&self) -> ContentDigest {
        ContentDigest::hash(self.as_der())
    }

    fn from_base64url(encoded: &str) -> ProtocolResult<Self> {
        if encoded.len() != ED25519_SPKI_BASE64URL_BYTES {
            return Err(ProtocolError::InvalidField {
                field: "public_key_spki",
                reason: format!(
                    "canonical Ed25519 SubjectPublicKeyInfo must contain exactly {ED25519_SPKI_BASE64URL_BYTES} base64url characters"
                ),
            });
        }
        let decoded = decode_base64url("public_key_spki", encoded)?;
        Self::new(decoded)
    }
}

impl fmt::Debug for Ed25519PublicKeySpki {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Ed25519PublicKeySpki")
            .field(&"[REDACTED]")
            .finish()
    }
}

impl Serialize for Ed25519PublicKeySpki {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(self.as_der()))
    }
}

impl<'de> Deserialize<'de> for Ed25519PublicKeySpki {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        Self::from_base64url(&encoded).map_err(de::Error::custom)
    }
}

impl TryFrom<Vec<u8>> for Ed25519PublicKeySpki {
    type Error = ProtocolError;

    fn try_from(der: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(der)
    }
}

/// Canonical 64-byte Ed25519 signature, encoded as base64url without padding.
#[derive(Clone, PartialEq, Eq, JsonSchema)]
#[schemars(transparent)]
pub struct Ed25519Signature(
    #[schemars(
        with = "String",
        length(equal = 86),
        regex(pattern = BASE64URL_PATTERN)
    )]
    [u8; ED25519_SIGNATURE_BYTES],
);

impl Ed25519Signature {
    /// Constructs a signature from its exact wire-independent byte representation.
    pub fn new(bytes: Vec<u8>) -> ProtocolResult<Self> {
        let bytes = bytes
            .try_into()
            .map_err(|bytes: Vec<u8>| ProtocolError::InvalidField {
                field: "signature",
                reason: format!(
                    "Ed25519 signature must contain exactly {ED25519_SIGNATURE_BYTES} bytes, observed {}",
                    bytes.len()
                ),
            })?;
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn from_bytes(bytes: [u8; ED25519_SIGNATURE_BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; ED25519_SIGNATURE_BYTES] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; ED25519_SIGNATURE_BYTES] {
        self.0
    }

    fn from_base64url(encoded: &str) -> ProtocolResult<Self> {
        if encoded.len() != ED25519_SIGNATURE_BASE64URL_BYTES {
            return Err(ProtocolError::InvalidField {
                field: "signature",
                reason: format!(
                    "canonical Ed25519 signature must contain exactly {ED25519_SIGNATURE_BASE64URL_BYTES} base64url characters"
                ),
            });
        }
        Self::new(decode_base64url("signature", encoded)?)
    }
}

impl fmt::Debug for Ed25519Signature {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Ed25519Signature")
            .field(&"[REDACTED]")
            .finish()
    }
}

impl Serialize for Ed25519Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(self.as_bytes()))
    }
}

impl<'de> Deserialize<'de> for Ed25519Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        Self::from_base64url(&encoded).map_err(de::Error::custom)
    }
}

impl TryFrom<Vec<u8>> for Ed25519Signature {
    type Error = ProtocolError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

/// Signature suite admitted for bootstrap proof of possession.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum AgentSignatureAlgorithm {
    #[serde(rename = "ed25519")]
    Ed25519,
}

/// Proof that a bootstrap or status caller controls the private key matching its SPKI.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentBootstrapProof {
    pub algorithm: AgentSignatureAlgorithm,
    pub public_key_spki: Ed25519PublicKeySpki,
    pub signature: Ed25519Signature,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentBootstrapProof {
    #[must_use]
    pub fn new(public_key_spki: Ed25519PublicKeySpki, signature: Ed25519Signature) -> Self {
        Self {
            algorithm: AgentSignatureAlgorithm::Ed25519,
            public_key_spki,
            signature,
            extensions: Extensions::new(),
        }
    }

    #[must_use]
    pub fn public_key_fingerprint(&self) -> ContentDigest {
        self.public_key_spki.fingerprint()
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_extension_keys(
            &self.extensions,
            &["algorithm", "public_key_spki", "signature"],
        )
    }

    /// Verifies an already domain-separated message with the embedded public key.
    pub fn verify(&self, message: &[u8]) -> ProtocolResult<()> {
        self.validate()?;
        UnparsedPublicKey::new(&ED25519, self.public_key_spki.as_public_key_bytes())
            .verify(message, self.signature.as_bytes())
            .map_err(|_| ProtocolError::InvalidField {
                field: "proof.signature",
                reason: "Ed25519 proof-of-possession signature verification failed".to_owned(),
            })
    }
}

impl fmt::Debug for AgentBootstrapProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentBootstrapProof")
            .field("algorithm", &self.algorithm)
            .field("public_key_spki", &self.public_key_spki)
            .field("signature", &self.signature)
            .field("extensions", &self.extensions)
            .finish()
    }
}

fn decode_base64url(field: &'static str, encoded: &str) -> ProtocolResult<Vec<u8>> {
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ProtocolError::InvalidField {
            field,
            reason: "must use canonical base64url without padding".to_owned(),
        })?;
    if URL_SAFE_NO_PAD.encode(&decoded) != encoded {
        return Err(ProtocolError::InvalidField {
            field,
            reason: "must use canonical base64url without padding".to_owned(),
        });
    }
    Ok(decoded)
}

/// Transport-neutral envelope used by a future bootstrap/admin adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentEnrollmentEnvelope {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    #[serde(flatten)]
    pub message: AgentEnrollmentMessage,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentEnrollmentEnvelope {
    /// Decodes one bounded message through the shared duplicate-member validator.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let value: serde_json::Value =
            decode_bounded_unique_json(bytes, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES)?;
        let message_type = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "type",
                reason: "missing or non-string enrollment message type".to_owned(),
            })?;
        if !AgentEnrollmentMessage::supports_type(message_type) {
            return Err(ProtocolError::UnsupportedMessageType(
                message_type.to_owned(),
            ));
        }
        let message: Self = serde_json::from_value(value)?;
        message.validate()?;
        Ok(message)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        validate_extension_keys(&self.extensions, &["protocol_version", "type", "payload"])?;
        self.message.validate()?;
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > MAX_AGENT_ENROLLMENT_MESSAGE_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "agent enrollment message bytes",
                limit: MAX_AGENT_ENROLLMENT_MESSAGE_BYTES,
                actual: encoded.len(),
            });
        }
        Ok(())
    }
}

/// Messages needed to enroll, bootstrap, approve, and observe one volume-scoped Agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "payload")]
pub enum AgentEnrollmentMessage {
    #[serde(rename = "agent.enrollment.token.create")]
    CreateToken(AgentEnrollmentTokenCreateRequest),
    #[serde(rename = "agent.bootstrap")]
    Bootstrap(AgentBootstrapRequest),
    #[serde(rename = "agent.bootstrap.accepted")]
    BootstrapAccepted(AgentBootstrapAccepted),
    #[serde(rename = "agent.bootstrap.status.request")]
    BootstrapStatusRequest(AgentBootstrapStatusRequest),
    #[serde(rename = "agent.bootstrap.status.response")]
    BootstrapStatusResponse(AgentBootstrapStatusResponse),
    #[serde(rename = "agent.enrollment.approval")]
    Approval(AgentEnrollmentApprovalRequest),
    #[serde(rename = "agent.mount.status")]
    MountStatus(AgentMountStatusReport),
}

impl AgentEnrollmentMessage {
    #[must_use]
    pub fn supports_type(message_type: &str) -> bool {
        matches!(
            message_type,
            "agent.enrollment.token.create"
                | "agent.bootstrap"
                | "agent.bootstrap.accepted"
                | "agent.bootstrap.status.request"
                | "agent.bootstrap.status.response"
                | "agent.enrollment.approval"
                | "agent.mount.status"
        )
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::CreateToken(message) => message.validate(),
            Self::Bootstrap(message) => message.validate(),
            Self::BootstrapAccepted(message) => message.validate(),
            Self::BootstrapStatusRequest(message) => message.validate(),
            Self::BootstrapStatusResponse(message) => message.validate(),
            Self::Approval(message) => message.validate(),
            Self::MountStatus(message) => message.validate(),
        }
    }
}

/// Platform-created token intent. Reserved IDs are central-owned and never chosen by an Agent.
#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentEnrollmentTokenCreateRequest {
    pub token_id: AgentEnrollmentTokenId,
    pub token_request_id: RequestId,
    pub enrollment_id: AgentEnrollmentId,
    pub tenant_id: TenantId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub volume_descriptor_digest: ContentDigest,
    pub pvc_identity_digest: PvcIdentityDigest,
    pub agent_id: AgentId,
    pub agent_mount_id: AgentMountId,
    pub expected_volume_marker: VolumeMarkerId,
    pub desired_access_mode: MountAccessMode,
    #[schemars(length(min = 32, max = 2048))]
    pub bootstrap_token: String,
    pub created_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl fmt::Debug for AgentEnrollmentTokenCreateRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentEnrollmentTokenCreateRequest")
            .field("token_id", &self.token_id)
            .field("token_request_id", &self.token_request_id)
            .field("enrollment_id", &self.enrollment_id)
            .field("tenant_id", &self.tenant_id)
            .field("edge_cluster_id", &self.edge_cluster_id)
            .field("storage_volume_id", &self.storage_volume_id)
            .field("volume_descriptor_digest", &self.volume_descriptor_digest)
            .field("pvc_identity_digest", &self.pvc_identity_digest)
            .field("agent_id", &self.agent_id)
            .field("agent_mount_id", &self.agent_mount_id)
            .field("expected_volume_marker", &self.expected_volume_marker)
            .field("desired_access_mode", &self.desired_access_mode)
            .field("bootstrap_token", &"[REDACTED]")
            .field("created_at_unix_ms", &self.created_at_unix_ms)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("extensions", &self.extensions)
            .finish()
    }
}

impl AgentEnrollmentTokenCreateRequest {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_bootstrap_token(&self.bootstrap_token)?;
        if self.desired_access_mode != MountAccessMode::ReadWrite {
            return Err(ProtocolError::InvalidField {
                field: "desired_access_mode",
                reason: "0.0.1 enrollment requires read-write access".to_owned(),
            });
        }
        if self.expected_volume_marker.as_str() != self.storage_volume_id.as_str() {
            return Err(ProtocolError::InvalidField {
                field: "expected_volume_marker",
                reason: "volume marker must equal the StorageVolume ID".to_owned(),
            });
        }
        if self.expires_at_unix_ms.get() <= self.created_at_unix_ms.get() {
            return Err(ProtocolError::InvalidField {
                field: "expires_at_unix_ms",
                reason: "enrollment expiry must follow creation".to_owned(),
            });
        }
        if self
            .expires_at_unix_ms
            .get()
            .saturating_sub(self.created_at_unix_ms.get())
            > BOOTSTRAP_TOKEN_MAX_TTL_MS
        {
            return Err(ProtocolError::InvalidField {
                field: "expires_at_unix_ms",
                reason: "bootstrap token lifetime cannot exceed 15 minutes".to_owned(),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "token_id",
                "token_request_id",
                "enrollment_id",
                "tenant_id",
                "edge_cluster_id",
                "storage_volume_id",
                "volume_descriptor_digest",
                "pvc_identity_digest",
                "agent_id",
                "agent_mount_id",
                "expected_volume_marker",
                "desired_access_mode",
                "bootstrap_token",
                "created_at_unix_ms",
                "expires_at_unix_ms",
            ],
        )
    }
}

/// First Agent contact. The opaque token resolves all Tenant and Volume scope centrally.
#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentBootstrapRequest {
    pub bootstrap_request_id: RequestId,
    #[schemars(length(min = 32, max = 2048))]
    pub bootstrap_token: String,
    pub installation_id: AgentInstallationId,
    pub tenant_id: TenantId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub volume_descriptor_digest: ContentDigest,
    #[schemars(length(min = 1, max = 128))]
    pub agent_version: String,
    #[schemars(length(max = 256))]
    pub supported_protocol_versions: Vec<ProtocolVersion>,
    #[serde(default)]
    #[schemars(length(max = 256), extend("uniqueItems" = true))]
    pub capabilities: Vec<String>,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub public_key_fingerprint: ContentDigest,
    pub proof: AgentBootstrapProof,
    /// Sanitized mount evidence. Raw paths, filesystem IDs, and mount sources stay Agent-local.
    pub probe: AgentBootstrapProbe,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl fmt::Debug for AgentBootstrapRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentBootstrapRequest")
            .field("bootstrap_request_id", &self.bootstrap_request_id)
            .field("bootstrap_token", &"[REDACTED]")
            .field("installation_id", &self.installation_id)
            .field("tenant_id", &self.tenant_id)
            .field("edge_cluster_id", &self.edge_cluster_id)
            .field("storage_volume_id", &self.storage_volume_id)
            .field("volume_descriptor_digest", &self.volume_descriptor_digest)
            .field("agent_version", &self.agent_version)
            .field(
                "supported_protocol_versions",
                &self.supported_protocol_versions,
            )
            .field("capabilities", &self.capabilities)
            .field("public_key_fingerprint", &self.public_key_fingerprint)
            .field("proof", &self.proof)
            .field("probe", &self.probe)
            .field("extensions", &self.extensions)
            .finish()
    }
}

impl AgentBootstrapRequest {
    /// Decodes a direct bootstrap body without losing duplicate-member evidence.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let request: Self = decode_bounded_unique_json(bytes, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES)?;
        request.validate()?;
        Ok(request)
    }

    /// Returns the domain-separated canonical bytes covered by the proof signature.
    pub fn signing_bytes(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        let mut wire_request = serde_json::to_value(self)?;
        let proof = wire_request
            .get_mut("proof")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| {
                ProtocolError::Serialization("proof must serialize as an object".to_owned())
            })?;
        if proof.remove("signature").is_none() {
            return Err(ProtocolError::Serialization(
                "proof.signature is absent from the serialized bootstrap request".to_owned(),
            ));
        }
        domain_separated_jcs_bytes(AGENT_BOOTSTRAP_SIGNING_DOMAIN_V1, &wire_request)
    }

    /// Verifies proof of possession after all structural and scope fields have been validated.
    pub fn verify(&self) -> ProtocolResult<()> {
        let signing_bytes = self.signing_bytes()?;
        self.proof.verify(&signing_bytes)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_bootstrap_token(&self.bootstrap_token)?;
        validate_nonempty_limited("agent_version", &self.agent_version, 128)?;
        validate_collection_limit(
            "supported_protocol_versions",
            self.supported_protocol_versions.len(),
            MAX_CAPABILITIES,
        )?;
        if !self
            .supported_protocol_versions
            .contains(&PROTOCOL_VERSION_V1)
        {
            return Err(ProtocolError::InvalidField {
                field: "supported_protocol_versions",
                reason: "v1 support is required".to_owned(),
            });
        }
        validate_collection_limit("capabilities", self.capabilities.len(), MAX_CAPABILITIES)?;
        let mut unique_capabilities = HashSet::with_capacity(self.capabilities.len());
        if let Some(duplicate) = self
            .capabilities
            .iter()
            .find(|capability| !unique_capabilities.insert(capability.as_str()))
        {
            return Err(ProtocolError::InvalidField {
                field: "capabilities",
                reason: format!("duplicate capability {duplicate:?}"),
            });
        }
        self.proof.validate()?;
        if self.public_key_fingerprint != self.proof.public_key_spki.fingerprint() {
            return Err(ProtocolError::InvalidField {
                field: "public_key_fingerprint",
                reason: "must equal BLAKE3 over the canonical Ed25519 SubjectPublicKeyInfo DER"
                    .to_owned(),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "bootstrap_request_id",
                "bootstrap_token",
                "installation_id",
                "tenant_id",
                "edge_cluster_id",
                "storage_volume_id",
                "volume_descriptor_digest",
                "agent_version",
                "supported_protocol_versions",
                "capabilities",
                "public_key_fingerprint",
                "proof",
                "probe",
            ],
        )?;
        self.probe.validate()
    }
}

/// Bootstrap-time evidence that can be projected into a public enrollment summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentBootstrapProbe {
    /// Internal opaque marker value used by the center to verify `marker_matches`.
    pub observed_volume_marker: Option<VolumeMarkerId>,
    pub marker_matches: bool,
    pub mount_boundary_detected: bool,
    pub mount_identity_digest: AgentMountIdentityDigest,
    pub access_mode: Option<MountAccessMode>,
    pub rename_supported: bool,
    pub fsync_supported: bool,
    pub health: ResourceHealth,
    pub observed_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentBootstrapProbe {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.marker_matches && self.observed_volume_marker.is_none() {
            return Err(ProtocolError::InvalidField {
                field: "probe.marker_matches",
                reason: "a matching marker requires an observed marker identity".to_owned(),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "observed_volume_marker",
                "marker_matches",
                "mount_boundary_detected",
                "mount_identity_digest",
                "access_mode",
                "rename_supported",
                "fsync_supported",
                "health",
                "observed_at_unix_ms",
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentEnrollmentState {
    TokenIssued,
    PendingApproval,
    Approved,
    Rejected,
    Expired,
    Revoked,
}

/// Stable identity returned after a valid, idempotent bootstrap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentBootstrapAccepted {
    pub bootstrap_request_id: RequestId,
    pub enrollment_id: AgentEnrollmentId,
    pub agent_id: AgentId,
    pub agent_mount_id: AgentMountId,
    pub state: AgentEnrollmentState,
    pub mount_generation: MountGeneration,
    pub resource_version: ResourceVersion,
    pub review_expires_at_unix_ms: UnixMillis,
    pub replayed: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentBootstrapAccepted {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("mount_generation", self.mount_generation.get())?;
        validate_positive("resource_version", self.resource_version.get())?;
        validate_extension_keys(
            &self.extensions,
            &[
                "bootstrap_request_id",
                "enrollment_id",
                "agent_id",
                "agent_mount_id",
                "state",
                "mount_generation",
                "resource_version",
                "review_expires_at_unix_ms",
                "replayed",
            ],
        )
    }
}

/// Signed polling request used before an approved Agent has a long-lived credential.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentBootstrapStatusRequest {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub tenant_id: TenantId,
    pub bootstrap_request_id: RequestId,
    pub installation_id: AgentInstallationId,
    pub signed_at_unix_ms: UnixMillis,
    pub proof: AgentBootstrapProof,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentBootstrapStatusRequest {
    /// Decodes a direct status body without losing duplicate-member evidence.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let request: Self = decode_bounded_unique_json(bytes, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES)?;
        request.validate()?;
        Ok(request)
    }

    /// Returns the domain-separated canonical bytes covered by the proof signature.
    pub fn signing_bytes(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        domain_separated_jcs_bytes(
            AGENT_BOOTSTRAP_STATUS_SIGNING_DOMAIN_V1,
            &AgentBootstrapStatusSigningInput {
                protocol_version: self.protocol_version,
                tenant_id: &self.tenant_id,
                bootstrap_request_id: &self.bootstrap_request_id,
                installation_id: &self.installation_id,
                signed_at_unix_ms: self.signed_at_unix_ms,
                proof: AgentBootstrapProofSigningInput::from(&self.proof),
                extensions: &self.extensions,
            },
        )
    }

    /// Verifies proof of possession. Freshness remains an application-layer clock decision.
    pub fn verify(&self) -> ProtocolResult<()> {
        let signing_bytes = self.signing_bytes()?;
        self.proof.verify(&signing_bytes)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        validate_positive("signed_at_unix_ms", self.signed_at_unix_ms.get())?;
        self.proof.validate()?;
        validate_extension_keys(
            &self.extensions,
            &[
                "protocol_version",
                "tenant_id",
                "bootstrap_request_id",
                "installation_id",
                "signed_at_unix_ms",
                "proof",
            ],
        )
    }
}

impl fmt::Debug for AgentBootstrapStatusRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentBootstrapStatusRequest")
            .field("protocol_version", &self.protocol_version)
            .field("tenant_id", &self.tenant_id)
            .field("bootstrap_request_id", &self.bootstrap_request_id)
            .field("installation_id", &self.installation_id)
            .field("signed_at_unix_ms", &self.signed_at_unix_ms)
            .field("proof", &self.proof)
            .field("extensions", &self.extensions)
            .finish()
    }
}

#[derive(Serialize)]
struct AgentBootstrapProofSigningInput<'a> {
    algorithm: AgentSignatureAlgorithm,
    public_key_spki: &'a Ed25519PublicKeySpki,
    #[serde(flatten)]
    extensions: &'a Extensions,
}

impl<'a> From<&'a AgentBootstrapProof> for AgentBootstrapProofSigningInput<'a> {
    fn from(proof: &'a AgentBootstrapProof) -> Self {
        Self {
            algorithm: proof.algorithm,
            public_key_spki: &proof.public_key_spki,
            extensions: &proof.extensions,
        }
    }
}

#[derive(Serialize)]
struct AgentBootstrapStatusSigningInput<'a> {
    protocol_version: ProtocolVersion,
    tenant_id: &'a TenantId,
    bootstrap_request_id: &'a RequestId,
    installation_id: &'a AgentInstallationId,
    signed_at_unix_ms: UnixMillis,
    proof: AgentBootstrapProofSigningInput<'a>,
    #[serde(flatten)]
    extensions: &'a Extensions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentBootstrapStatusState {
    Pending,
    Approved,
    Rejected,
    Expired,
}

/// Poll result for one stable bootstrap request identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentBootstrapStatusResponse {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub bootstrap_request_id: RequestId,
    pub installation_id: AgentInstallationId,
    pub state: AgentBootstrapStatusState,
    pub enrollment_id: AgentEnrollmentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub resource_version: ResourceVersion,
    pub updated_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentBootstrapStatusResponse {
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let response: Self = decode_bounded_unique_json(bytes, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES)?;
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        validate_positive("resource_version", self.resource_version.get())?;
        match (self.state, self.agent_id.is_some()) {
            (AgentBootstrapStatusState::Approved, true)
            | (
                AgentBootstrapStatusState::Pending
                | AgentBootstrapStatusState::Rejected
                | AgentBootstrapStatusState::Expired,
                false,
            ) => {}
            (AgentBootstrapStatusState::Approved, false) => {
                return Err(ProtocolError::InvalidField {
                    field: "agent_id",
                    reason: "approved bootstrap status requires the bound Agent ID".to_owned(),
                });
            }
            (_, true) => {
                return Err(ProtocolError::InvalidField {
                    field: "agent_id",
                    reason: "only approved bootstrap status may expose a bound Agent ID".to_owned(),
                });
            }
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "protocol_version",
                "bootstrap_request_id",
                "installation_id",
                "state",
                "enrollment_id",
                "agent_id",
                "resource_version",
                "updated_at_unix_ms",
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentEnrollmentDecision {
    Approve,
    Reject,
}

/// Tenant-admin decision; the authenticated actor is supplied by the future adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentEnrollmentApprovalRequest {
    pub enrollment_id: AgentEnrollmentId,
    pub decision_request_id: RequestId,
    pub expected_resource_version: ResourceVersion,
    pub decision: AgentEnrollmentDecision,
    pub confirm_replacement: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentEnrollmentApprovalRequest {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive(
            "expected_resource_version",
            self.expected_resource_version.get(),
        )?;
        validate_extension_keys(
            &self.extensions,
            &[
                "enrollment_id",
                "decision_request_id",
                "expected_resource_version",
                "decision",
                "confirm_replacement",
            ],
        )
    }
}

/// Mount observation submitted on an already-open, center-issued session generation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentMountStatusReport {
    pub agent_id: AgentId,
    pub installation_id: AgentInstallationId,
    pub boot_id: AgentBootId,
    pub session_generation: SessionGeneration,
    pub sequence: SequenceNumber,
    pub agent_mount_id: AgentMountId,
    pub storage_volume_id: StorageVolumeId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
    pub observed_volume_marker: VolumeMarkerId,
    pub mount_identity_digest: AgentMountIdentityDigest,
    pub access_mode: MountAccessMode,
    pub health: ResourceHealth,
    pub observed_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentMountStatusReport {
    pub fn validate(&self) -> ProtocolResult<()> {
        for (field, value) in [
            ("session_generation", self.session_generation.get()),
            ("mount_generation", self.mount_generation.get()),
            ("owner_generation", self.owner_generation.get()),
        ] {
            validate_positive(field, value)?;
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "agent_id",
                "installation_id",
                "boot_id",
                "session_generation",
                "sequence",
                "agent_mount_id",
                "storage_volume_id",
                "mount_generation",
                "owner_generation",
                "observed_volume_marker",
                "mount_identity_digest",
                "access_mode",
                "health",
                "observed_at_unix_ms",
            ],
        )
    }
}

fn validate_bootstrap_token(token: &str) -> ProtocolResult<()> {
    if token.len() < BOOTSTRAP_TOKEN_MIN_BYTES || token.len() > BOOTSTRAP_TOKEN_MAX_BYTES {
        return Err(ProtocolError::InvalidField {
            field: "bootstrap_token",
            reason: format!(
                "bootstrap token must contain {BOOTSTRAP_TOKEN_MIN_BYTES}..={BOOTSTRAP_TOKEN_MAX_BYTES} UTF-8 bytes"
            ),
        });
    }
    if !token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'~' | b'-'))
    {
        return Err(ProtocolError::InvalidField {
            field: "bootstrap_token",
            reason: "bootstrap token contains characters outside the opaque token alphabet"
                .to_owned(),
        });
    }
    Ok(())
}

fn valid_dns_1123_label(value: &str) -> bool {
    if value.is_empty() || value.len() > 63 {
        return false;
    }
    let bytes = value.as_bytes();
    let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    is_alphanumeric(bytes[0])
        && is_alphanumeric(bytes[bytes.len() - 1])
        && bytes
            .iter()
            .all(|byte| is_alphanumeric(*byte) || *byte == b'-')
}

fn validate_positive(field: &'static str, value: u64) -> ProtocolResult<()> {
    if value == 0 {
        Err(ProtocolError::InvalidField {
            field,
            reason: "must be positive".to_owned(),
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ring::signature::{Ed25519KeyPair, KeyPair as _};

    use super::*;

    fn create_request() -> AgentEnrollmentTokenCreateRequest {
        AgentEnrollmentTokenCreateRequest {
            token_id: AgentEnrollmentTokenId::new("token-a").unwrap(),
            token_request_id: RequestId::new("create-token-a").unwrap(),
            enrollment_id: AgentEnrollmentId::new("enroll-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"volume-descriptor-a"),
            pvc_identity_digest: PvcIdentityDigest::new(ContentDigest::hash(b"pvc-a")),
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadWrite,
            bootstrap_token: "t".repeat(32),
            created_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: UnixMillis::new(200),
            extensions: Extensions::new(),
        }
    }

    fn test_key_pair() -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[0x42; 32]).unwrap()
    }

    fn placeholder_proof(key_pair: &Ed25519KeyPair) -> AgentBootstrapProof {
        let public_key = key_pair
            .public_key()
            .as_ref()
            .try_into()
            .expect("ring Ed25519 public keys contain 32 bytes");
        AgentBootstrapProof {
            algorithm: AgentSignatureAlgorithm::Ed25519,
            public_key_spki: Ed25519PublicKeySpki::from_public_key_bytes(public_key),
            signature: Ed25519Signature::from_bytes([0_u8; ED25519_SIGNATURE_BYTES]),
            extensions: Extensions::new(),
        }
    }

    fn bootstrap_request(key_pair: &Ed25519KeyPair) -> AgentBootstrapRequest {
        let proof = placeholder_proof(key_pair);
        AgentBootstrapRequest {
            bootstrap_request_id: RequestId::new("bootstrap-a").unwrap(),
            bootstrap_token: "t".repeat(32),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"volume-descriptor-a"),
            agent_version: "0.0.1".to_owned(),
            supported_protocol_versions: vec![PROTOCOL_VERSION_V1],
            capabilities: Vec::new(),
            public_key_fingerprint: proof.public_key_spki.fingerprint(),
            proof,
            probe: AgentBootstrapProbe {
                observed_volume_marker: Some(VolumeMarkerId::new("volume-a").unwrap()),
                marker_matches: true,
                mount_boundary_detected: true,
                mount_identity_digest: AgentMountIdentityDigest::new(ContentDigest::hash(
                    b"mount-a",
                )),
                access_mode: Some(MountAccessMode::ReadWrite),
                rename_supported: true,
                fsync_supported: true,
                health: ResourceHealth::Ready,
                observed_at_unix_ms: UnixMillis::new(100),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        }
    }

    fn signed_bootstrap_request(key_pair: &Ed25519KeyPair) -> AgentBootstrapRequest {
        let mut request = bootstrap_request(key_pair);
        let signature = key_pair.sign(&request.signing_bytes().unwrap());
        request.proof.signature = Ed25519Signature::new(signature.as_ref().to_vec()).unwrap();
        request
    }

    fn signed_status_request(key_pair: &Ed25519KeyPair) -> AgentBootstrapStatusRequest {
        let mut request = AgentBootstrapStatusRequest {
            protocol_version: PROTOCOL_VERSION_V1,
            tenant_id: TenantId::new("tenant-a").unwrap(),
            bootstrap_request_id: RequestId::new("bootstrap-a").unwrap(),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            signed_at_unix_ms: UnixMillis::new(1_721_821_600_000),
            proof: placeholder_proof(key_pair),
            extensions: Extensions::new(),
        };
        let signature = key_pair.sign(&request.signing_bytes().unwrap());
        request.proof.signature = Ed25519Signature::new(signature.as_ref().to_vec()).unwrap();
        request
    }

    #[test]
    fn enrollment_rejects_short_tokens_and_non_future_expiry() {
        let mut request = create_request();
        request.bootstrap_token = "short".to_owned();
        assert!(request.validate().is_err());
        request.bootstrap_token = "t".repeat(32);
        request.expires_at_unix_ms = request.created_at_unix_ms;
        assert!(request.validate().is_err());
    }

    #[test]
    fn token_intent_requires_the_storage_volume_id_as_marker() {
        let mut request = create_request();
        request.expected_volume_marker = VolumeMarkerId::new("another-volume").unwrap();
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "expected_volume_marker",
                ..
            })
        ));
    }

    #[test]
    fn p0_token_intent_rejects_read_only_access() {
        let mut request = create_request();
        request.desired_access_mode = MountAccessMode::ReadOnly;
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "desired_access_mode",
                ..
            })
        ));
    }

    #[test]
    fn debug_output_redacts_bootstrap_tokens() {
        let token_request = create_request();
        let token = token_request.bootstrap_token.clone();
        let pvc_identity = token_request.pvc_identity_digest;
        let token_debug = format!("{token_request:?}");
        assert!(token_debug.contains("[REDACTED]"));
        assert!(!token_debug.contains(&token));
        assert!(!token_debug.contains(&pvc_identity.as_digest().to_string()));

        let key_pair = test_key_pair();
        let bootstrap_request = bootstrap_request(&key_pair);
        let mount_identity = bootstrap_request.probe.mount_identity_digest;
        let bootstrap_debug = format!("{bootstrap_request:?}");
        assert!(bootstrap_debug.contains("[REDACTED]"));
        assert!(!bootstrap_debug.contains(&token));
        assert!(!bootstrap_debug.contains(&mount_identity.as_digest().to_string()));
    }

    #[test]
    fn internal_identity_digests_are_transparent_canonical_hex() {
        let digest = ContentDigest::from_bytes([0x42; 32]);
        let expected = format!("\"{digest}\"");
        let mount_identity = AgentMountIdentityDigest::new(digest);
        let pvc_identity = PvcIdentityDigest::new(digest);

        assert_eq!(serde_json::to_string(&mount_identity).unwrap(), expected);
        assert_eq!(serde_json::to_string(&pvc_identity).unwrap(), expected);
        assert_eq!(
            serde_json::from_str::<AgentMountIdentityDigest>(&expected).unwrap(),
            mount_identity
        );
        assert_eq!(
            serde_json::from_str::<PvcIdentityDigest>(&expected).unwrap(),
            pvc_identity
        );
        assert!(!format!("{mount_identity:?}").contains(&digest.to_string()));
        assert!(!format!("{pvc_identity:?}").contains(&digest.to_string()));
    }

    #[test]
    fn pvc_identity_is_scoped_only_by_namespace_and_claim() {
        let identity = PvcIdentityDigest::derive("datasets", "training-pvc").unwrap();
        assert_eq!(
            identity,
            PvcIdentityDigest::derive("datasets", "training-pvc").unwrap()
        );
        assert_ne!(
            identity,
            PvcIdentityDigest::derive("other-namespace", "training-pvc").unwrap()
        );
        assert_ne!(
            identity,
            PvcIdentityDigest::derive("datasets", "other-pvc").unwrap()
        );
        assert!(PvcIdentityDigest::derive("UPPERCASE", "training-pvc").is_err());
        assert!(PvcIdentityDigest::derive("datasets", "invalid..claim").is_err());
    }

    #[test]
    fn envelope_rejects_duplicate_members() {
        let bytes = br#"{"protocol_version":1,"protocol_version":1,"type":"agent.enrollment.token.create","payload":{}}"#;
        assert!(AgentEnrollmentEnvelope::decode_json(bytes).is_err());
    }

    #[test]
    fn ed25519_wire_types_require_canonical_algorithm_and_lengths() {
        let key_pair = test_key_pair();
        let proof = placeholder_proof(&key_pair);
        let encoded_key = serde_json::to_string(&proof.public_key_spki).unwrap();
        let encoded_signature = serde_json::to_string(&proof.signature).unwrap();

        assert_eq!(
            encoded_key.trim_matches('"').len(),
            ED25519_SPKI_BASE64URL_BYTES
        );
        assert_eq!(
            encoded_signature.trim_matches('"').len(),
            ED25519_SIGNATURE_BASE64URL_BYTES
        );
        assert!(!encoded_key.contains('='));
        assert!(!encoded_signature.contains('='));
        assert_eq!(
            serde_json::from_str::<Ed25519PublicKeySpki>(&encoded_key).unwrap(),
            proof.public_key_spki
        );
        assert_eq!(
            serde_json::from_str::<Ed25519Signature>(&encoded_signature).unwrap(),
            proof.signature
        );

        let mut wrong_oid = proof.public_key_spki.as_der().to_vec();
        wrong_oid[8] ^= 1;
        assert!(Ed25519PublicKeySpki::new(wrong_oid).is_err());
        assert!(Ed25519Signature::new(vec![0_u8; 63]).is_err());

        let unsupported = format!(
            "{{\"algorithm\":\"rsa_pss\",\"public_key_spki\":{encoded_key},\"signature\":{encoded_signature}}}"
        );
        assert!(serde_json::from_str::<AgentBootstrapProof>(&unsupported).is_err());
    }

    #[test]
    fn bootstrap_proof_detects_tampering() {
        let key_pair = test_key_pair();
        let request = signed_bootstrap_request(&key_pair);
        request.verify().unwrap();
        assert!(request
            .signing_bytes()
            .unwrap()
            .starts_with(b"neoengram-agent-bootstrap-v1\0"));
        assert_eq!(
            ContentDigest::hash(request.signing_bytes().unwrap()).to_string(),
            "9b947fbdccf96f30d93bc3999e18ac5c5a6150504aa34a52c4eec230a773a605"
        );

        let mut tampered = request.clone();
        tampered.storage_volume_id = StorageVolumeId::new("volume-b").unwrap();
        assert!(matches!(
            tampered.verify(),
            Err(ProtocolError::InvalidField {
                field: "proof.signature",
                ..
            })
        ));

        let mut extension_tampered = request.clone();
        extension_tampered
            .extensions
            .insert("x_policy_hint".to_owned(), serde_json::json!("changed"));
        assert!(extension_tampered.verify().is_err());

        let mut wrong_fingerprint = request;
        wrong_fingerprint.public_key_fingerprint = ContentDigest::hash(b"different-key");
        assert!(matches!(
            wrong_fingerprint.validate(),
            Err(ProtocolError::InvalidField {
                field: "public_key_fingerprint",
                ..
            })
        ));
    }

    #[test]
    fn bootstrap_proof_rejects_lossy_binary64_in_nested_extensions() {
        const MAX_CONSECUTIVE_INTEGER: u64 = 1_u64 << 53;

        let key_pair = test_key_pair();
        let mut request = bootstrap_request(&key_pair);
        request.extensions.insert(
            "x_policy_hint".to_owned(),
            serde_json::json!({"nested": {"sequence": MAX_CONSECUTIVE_INTEGER}}),
        );
        let signature = key_pair.sign(&request.signing_bytes().unwrap());
        request.proof.signature = Ed25519Signature::new(signature.as_ref().to_vec()).unwrap();
        request.verify().unwrap();

        request.extensions.insert(
            "x_policy_hint".to_owned(),
            serde_json::json!({"nested": {"sequence": MAX_CONSECUTIVE_INTEGER + 1}}),
        );
        assert!(matches!(
            request.verify(),
            Err(ProtocolError::InvalidField {
                field: "canonical_json_number",
                ..
            })
        ));
    }

    #[test]
    fn bootstrap_signing_bytes_are_the_wire_request_without_the_signature() {
        let key_pair = test_key_pair();
        let request = bootstrap_request(&key_pair);
        let mut wire_request = serde_json::to_value(&request).unwrap();
        assert!(wire_request.get("protocol_version").is_none());
        assert!(wire_request
            .get_mut("proof")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("signature")
            .is_some());

        let mut expected = AGENT_BOOTSTRAP_SIGNING_DOMAIN_V1.as_bytes().to_vec();
        expected.push(0);
        expected.extend(crate::jcs_bytes(&wire_request).unwrap());
        assert_eq!(request.signing_bytes().unwrap(), expected);
    }

    #[test]
    fn bootstrap_capability_order_is_signed_and_duplicates_are_rejected() {
        let key_pair = test_key_pair();
        let mut request = bootstrap_request(&key_pair);
        request.capabilities = vec!["mount_probe_v1".to_owned(), "status_poll_v1".to_owned()];
        let signature = key_pair.sign(&request.signing_bytes().unwrap());
        request.proof.signature = Ed25519Signature::new(signature.as_ref().to_vec()).unwrap();
        request.verify().unwrap();

        let mut reordered = request.clone();
        reordered.capabilities.swap(0, 1);
        assert!(matches!(
            reordered.verify(),
            Err(ProtocolError::InvalidField {
                field: "proof.signature",
                ..
            })
        ));

        let mut duplicated = request;
        duplicated.capabilities[1] = duplicated.capabilities[0].clone();
        assert!(matches!(
            duplicated.validate(),
            Err(ProtocolError::InvalidField {
                field: "capabilities",
                ..
            })
        ));
        let duplicated_wire = serde_json::to_vec(&duplicated).unwrap();
        assert!(matches!(
            AgentBootstrapRequest::decode_json(&duplicated_wire),
            Err(ProtocolError::InvalidField {
                field: "capabilities",
                ..
            })
        ));
    }

    #[test]
    fn status_proof_uses_a_distinct_domain_and_detects_tampering() {
        let key_pair = test_key_pair();
        let status = signed_status_request(&key_pair);
        status.verify().unwrap();
        let status_bytes = status.signing_bytes().unwrap();
        assert!(status_bytes.starts_with(b"neoengram-agent-bootstrap-status-v1\0"));

        let bootstrap = signed_bootstrap_request(&key_pair);
        assert_ne!(status_bytes, bootstrap.signing_bytes().unwrap());

        let mut tampered = status;
        tampered.signed_at_unix_ms = UnixMillis::new(tampered.signed_at_unix_ms.get() + 1);
        assert!(matches!(
            tampered.verify(),
            Err(ProtocolError::InvalidField {
                field: "proof.signature",
                ..
            })
        ));
    }

    #[test]
    fn status_proof_rejects_lossy_binary64_in_nested_proof_extensions() {
        const MAX_CONSECUTIVE_INTEGER: u64 = 1_u64 << 53;

        let key_pair = test_key_pair();
        let mut status = signed_status_request(&key_pair);
        status.proof.extensions.insert(
            "x_key_epoch".to_owned(),
            serde_json::json!({"nested": [MAX_CONSECUTIVE_INTEGER]}),
        );
        let signature = key_pair.sign(&status.signing_bytes().unwrap());
        status.proof.signature = Ed25519Signature::new(signature.as_ref().to_vec()).unwrap();
        status.verify().unwrap();

        status.proof.extensions.insert(
            "x_key_epoch".to_owned(),
            serde_json::json!({"nested": [MAX_CONSECUTIVE_INTEGER + 1]}),
        );
        assert!(matches!(
            status.verify(),
            Err(ProtocolError::InvalidField {
                field: "canonical_json_number",
                ..
            })
        ));
    }

    #[test]
    fn direct_decoders_reject_nested_duplicates_and_oversized_input() {
        let key_pair = test_key_pair();
        let bootstrap = serde_json::to_string(&bootstrap_request(&key_pair)).unwrap();
        let bootstrap_with_duplicate = bootstrap.replacen(
            "\"agent_version\":\"0.0.1\"",
            "\"agent_version\":\"0.0.1\",\"agent_version\":\"0.0.1\"",
            1,
        );
        assert!(matches!(
            AgentBootstrapRequest::decode_json(bootstrap_with_duplicate.as_bytes()),
            Err(ProtocolError::Serialization(message))
                if message.contains("duplicate JSON object member")
        ));

        let nested_duplicate =
            br#"{"protocol_version":1,"proof":{"algorithm":"ed25519","algorithm":"ed25519"}}"#;
        assert!(AgentBootstrapStatusRequest::decode_json(nested_duplicate).is_err());

        let oversized = vec![b' '; MAX_AGENT_ENROLLMENT_MESSAGE_BYTES + 1];
        assert_eq!(
            AgentBootstrapRequest::decode_json(&oversized),
            Err(ProtocolError::LimitExceeded {
                limit_name: "JSON message bytes",
                limit: MAX_AGENT_ENROLLMENT_MESSAGE_BYTES,
                actual: MAX_AGENT_ENROLLMENT_MESSAGE_BYTES + 1,
            })
        );
        assert_eq!(
            AgentEnrollmentEnvelope::decode_json(&oversized),
            Err(ProtocolError::LimitExceeded {
                limit_name: "JSON message bytes",
                limit: MAX_AGENT_ENROLLMENT_MESSAGE_BYTES,
                actual: MAX_AGENT_ENROLLMENT_MESSAGE_BYTES + 1,
            })
        );
    }

    #[test]
    fn approved_status_requires_an_agent_identity() {
        let mut response = AgentBootstrapStatusResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            bootstrap_request_id: RequestId::new("bootstrap-a").unwrap(),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            state: AgentBootstrapStatusState::Approved,
            enrollment_id: AgentEnrollmentId::new("enrollment-a").unwrap(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            resource_version: ResourceVersion::new(1),
            updated_at_unix_ms: UnixMillis::new(100),
            extensions: Extensions::new(),
        };
        response.validate().unwrap();

        response.agent_id = None;
        assert!(response.validate().is_err());
        response.state = AgentBootstrapStatusState::Pending;
        response.validate().unwrap();
        response.agent_id = Some(AgentId::new("agent-a").unwrap());
        assert!(response.validate().is_err());
    }
}
