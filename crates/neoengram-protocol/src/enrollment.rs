use std::{collections::BTreeSet, fmt};

use neoengram_core::ContentDigest;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::validation::{
    parse_unique_json, validate_collection_limit, validate_extension_keys,
    validate_nonempty_limited, CONTENT_DIGEST_PATTERN,
};
use crate::{
    AgentBootId, AgentEnrollmentId, AgentEnrollmentTokenId, AgentId, AgentInstallationId,
    AgentMountId, EdgeClusterId, Extensions, MountAccessMode, MountGeneration, OwnerGeneration,
    ProtocolError, ProtocolResult, ProtocolVersion, RequestId, ResourceHealth, ResourceVersion,
    SequenceNumber, SessionGeneration, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId,
    PROTOCOL_VERSION_V1,
};

const BOOTSTRAP_TOKEN_MIN_BYTES: usize = 32;
const BOOTSTRAP_TOKEN_MAX_BYTES: usize = 2_048;
const BOOTSTRAP_TOKEN_MAX_TTL_MS: u64 = 15 * 60 * 1_000;
const MAX_CAPABILITIES: usize = 256;

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
    /// Decodes through the shared duplicate-member validator.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let value = parse_unique_json(bytes)?;
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
        self.message.validate()
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
    #[serde(rename = "agent.enrollment.approval")]
    Approval(AgentEnrollmentApprovalRequest),
    #[serde(rename = "agent.mount.status")]
    MountStatus(AgentMountStatusReport),
}

impl AgentEnrollmentMessage {
    pub fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::CreateToken(message) => message.validate(),
            Self::Bootstrap(message) => message.validate(),
            Self::BootstrapAccepted(message) => message.validate(),
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
    #[schemars(length(max = 256))]
    pub capabilities: BTreeSet<String>,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub public_key_fingerprint: ContentDigest,
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
            .field("probe", &self.probe)
            .field("extensions", &self.extensions)
            .finish()
    }
}

impl AgentBootstrapRequest {
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

        let mount_identity = AgentMountIdentityDigest::new(ContentDigest::hash(b"mount-a"));
        let bootstrap_request = AgentBootstrapRequest {
            bootstrap_request_id: RequestId::new("bootstrap-a").unwrap(),
            bootstrap_token: token.clone(),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"volume-descriptor-a"),
            agent_version: "0.0.1".to_owned(),
            supported_protocol_versions: vec![PROTOCOL_VERSION_V1],
            capabilities: BTreeSet::new(),
            public_key_fingerprint: ContentDigest::hash(b"key-a"),
            probe: AgentBootstrapProbe {
                observed_volume_marker: Some(VolumeMarkerId::new("volume-a").unwrap()),
                marker_matches: true,
                mount_boundary_detected: true,
                mount_identity_digest: mount_identity,
                access_mode: Some(MountAccessMode::ReadWrite),
                rename_supported: true,
                fsync_supported: true,
                health: ResourceHealth::Ready,
                observed_at_unix_ms: UnixMillis::new(100),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
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
}
