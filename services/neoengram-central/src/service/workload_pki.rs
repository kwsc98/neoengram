use std::{collections::BTreeSet, fmt, net::IpAddr, str::FromStr, sync::Arc};

use async_trait::async_trait;
use neoengram_domain::protocol::{
    AgentId, CertificateGeneration, ContentDigest, Ed25519PublicKeySpki, EdgeClusterId,
    GatewayPoolId, GatewayReplicaId, RequestId, UnixMillis,
};
use thiserror::Error;
use url::{Host, Url};
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer, X509Certificate},
};

/// Default lifetime of a workload leaf certificate: six hours.
pub const DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS: u64 = 6 * 60 * 60 * 1_000;
/// Maximum lifetime accepted at the online Intermediate boundary: six hours.
pub const MAX_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS: u64 =
    DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS;
const MAX_CERTIFICATE_DER_BYTES: usize = 64 * 1024;
const MAX_ISSUER_CHAIN_DEPTH: usize = 8;
const WORKLOAD_PATH_PREFIX: &str = "workloads";

/// Stable workload identity encoded into the sole identity URI SAN of a leaf certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadIdentity {
    /// The logical Central control plane.  Central replicas share this stable identity while
    /// certificate generation and key material remain independently rotatable.
    Central,
    GatewayReplica {
        edge_cluster_id: EdgeClusterId,
        gateway_pool_id: GatewayPoolId,
        gateway_replica_id: GatewayReplicaId,
    },
    Agent {
        edge_cluster_id: EdgeClusterId,
        agent_id: AgentId,
    },
}

/// Extended key usages the Intermediate must place in a workload leaf certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadCertificateUsage {
    ClientAuth,
    ServerAuth,
}

const GATEWAY_REPLICA_USAGES: &[WorkloadCertificateUsage] = &[
    WorkloadCertificateUsage::ClientAuth,
    WorkloadCertificateUsage::ServerAuth,
];
const AGENT_USAGES: &[WorkloadCertificateUsage] = &[WorkloadCertificateUsage::ClientAuth];
const CENTRAL_USAGES: &[WorkloadCertificateUsage] = &[WorkloadCertificateUsage::ClientAuth];

impl WorkloadIdentity {
    /// Gateway replicas accept and initiate mTLS, while Agents only initiate their Gateway link.
    #[must_use]
    pub fn required_extended_key_usages(&self) -> &'static [WorkloadCertificateUsage] {
        match self {
            Self::Central => CENTRAL_USAGES,
            Self::GatewayReplica { .. } => GATEWAY_REPLICA_USAGES,
            Self::Agent { .. } => AGENT_USAGES,
        }
    }
}

/// A canonical SPIFFE URI whose path binds a workload to its EdgeCluster scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadIdentityUri {
    value: String,
    trust_domain: String,
    identity: WorkloadIdentity,
}

impl WorkloadIdentityUri {
    /// Returns the stable identity used by Central when it opens a Gateway control session.
    /// Central is a logical control-plane workload rather than an EdgeCluster-scoped resource.
    pub fn central(trust_domain: &str) -> Result<Self, WorkloadPkiError> {
        validate_trust_domain(trust_domain)?;
        Self::parse(&format!(
            "spiffe://{trust_domain}/{WORKLOAD_PATH_PREFIX}/central"
        ))
    }

    pub fn gateway_replica(
        trust_domain: &str,
        edge_cluster_id: EdgeClusterId,
        gateway_pool_id: GatewayPoolId,
        gateway_replica_id: GatewayReplicaId,
    ) -> Result<Self, WorkloadPkiError> {
        validate_trust_domain(trust_domain)?;
        let value = format!(
            "spiffe://{trust_domain}/{WORKLOAD_PATH_PREFIX}/edge-clusters/{edge_cluster_id}/gateway-pools/{gateway_pool_id}/gateway-replicas/{gateway_replica_id}"
        );
        Self::parse(&value)
    }

    pub fn agent(
        trust_domain: &str,
        edge_cluster_id: EdgeClusterId,
        agent_id: AgentId,
    ) -> Result<Self, WorkloadPkiError> {
        validate_trust_domain(trust_domain)?;
        let value = format!(
            "spiffe://{trust_domain}/{WORKLOAD_PATH_PREFIX}/edge-clusters/{edge_cluster_id}/agents/{agent_id}"
        );
        Self::parse(&value)
    }

    pub fn parse_for_trust_domain(
        value: &str,
        expected_trust_domain: &str,
    ) -> Result<Self, WorkloadPkiError> {
        validate_trust_domain(expected_trust_domain)?;
        let parsed = Self::parse(value)?;
        if parsed.trust_domain != expected_trust_domain {
            return Err(WorkloadPkiError::TrustDomainMismatch);
        }
        Ok(parsed)
    }

    pub fn parse(value: &str) -> Result<Self, WorkloadPkiError> {
        let url = Url::parse(value).map_err(|_| WorkloadPkiError::InvalidIdentityUri)?;
        if url.scheme() != "spiffe"
            || url.username() != ""
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.as_str() != value
        {
            return Err(WorkloadPkiError::InvalidIdentityUri);
        }
        let trust_domain = url.host_str().ok_or(WorkloadPkiError::InvalidIdentityUri)?;
        if !matches!(url.host(), Some(Host::Domain(_))) {
            return Err(WorkloadPkiError::InvalidIdentityUri);
        }
        validate_trust_domain(trust_domain)?;
        let segments = url
            .path_segments()
            .ok_or(WorkloadPkiError::InvalidIdentityUri)?
            .collect::<Vec<_>>();
        let identity = match segments.as_slice() {
            [WORKLOAD_PATH_PREFIX, "central"] => WorkloadIdentity::Central,
            [WORKLOAD_PATH_PREFIX, "edge-clusters", edge_cluster_id, "gateway-pools", gateway_pool_id, "gateway-replicas", gateway_replica_id] => {
                WorkloadIdentity::GatewayReplica {
                    edge_cluster_id: EdgeClusterId::new(*edge_cluster_id)
                        .map_err(|_| WorkloadPkiError::InvalidIdentityUri)?,
                    gateway_pool_id: GatewayPoolId::new(*gateway_pool_id)
                        .map_err(|_| WorkloadPkiError::InvalidIdentityUri)?,
                    gateway_replica_id: GatewayReplicaId::new(*gateway_replica_id)
                        .map_err(|_| WorkloadPkiError::InvalidIdentityUri)?,
                }
            }
            [WORKLOAD_PATH_PREFIX, "edge-clusters", edge_cluster_id, "agents", agent_id] => {
                WorkloadIdentity::Agent {
                    edge_cluster_id: EdgeClusterId::new(*edge_cluster_id)
                        .map_err(|_| WorkloadPkiError::InvalidIdentityUri)?,
                    agent_id: AgentId::new(*agent_id)
                        .map_err(|_| WorkloadPkiError::InvalidIdentityUri)?,
                }
            }
            _ => return Err(WorkloadPkiError::InvalidIdentityUri),
        };
        Ok(Self {
            value: value.to_owned(),
            trust_domain: trust_domain.to_owned(),
            identity,
        })
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub fn trust_domain(&self) -> &str {
        &self.trust_domain
    }

    #[must_use]
    pub fn identity(&self) -> &WorkloadIdentity {
        &self.identity
    }
}

impl fmt::Display for WorkloadIdentityUri {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.value)
    }
}

impl FromStr for WorkloadIdentityUri {
    type Err = WorkloadPkiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Fully scoped request sent to an online Intermediate CA adapter.
///
/// The adapter signs the supplied public key and must not accept SANs from an untrusted CSR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadCertificateRequest {
    request_id: RequestId,
    identity_uri: WorkloadIdentityUri,
    certificate_generation: CertificateGeneration,
    public_key_spki: Ed25519PublicKeySpki,
    not_before_unix_ms: UnixMillis,
    not_after_unix_ms: UnixMillis,
    /// DNS names or IP literals that a server-auth workload certificate must contain.  URI SAN
    /// remains the stable workload identity; these names are only for the TLS endpoint binding.
    server_names: BTreeSet<String>,
}

impl WorkloadCertificateRequest {
    pub fn new(
        request_id: RequestId,
        identity_uri: WorkloadIdentityUri,
        certificate_generation: CertificateGeneration,
        public_key_spki: Ed25519PublicKeySpki,
        not_before_unix_ms: UnixMillis,
    ) -> Result<Self, WorkloadPkiError> {
        Self::with_lifetime_ms(
            request_id,
            identity_uri,
            certificate_generation,
            public_key_spki,
            not_before_unix_ms,
            DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS,
        )
    }

    pub fn new_with_server_names(
        request_id: RequestId,
        identity_uri: WorkloadIdentityUri,
        certificate_generation: CertificateGeneration,
        public_key_spki: Ed25519PublicKeySpki,
        not_before_unix_ms: UnixMillis,
        server_names: BTreeSet<String>,
    ) -> Result<Self, WorkloadPkiError> {
        Self::with_lifetime_and_server_names(
            request_id,
            identity_uri,
            certificate_generation,
            public_key_spki,
            not_before_unix_ms,
            DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS,
            server_names,
        )
    }

    pub fn with_lifetime_ms(
        request_id: RequestId,
        identity_uri: WorkloadIdentityUri,
        certificate_generation: CertificateGeneration,
        public_key_spki: Ed25519PublicKeySpki,
        not_before_unix_ms: UnixMillis,
        lifetime_ms: u64,
    ) -> Result<Self, WorkloadPkiError> {
        Self::with_lifetime_and_server_names(
            request_id,
            identity_uri,
            certificate_generation,
            public_key_spki,
            not_before_unix_ms,
            lifetime_ms,
            BTreeSet::new(),
        )
    }

    pub fn with_lifetime_and_server_names(
        request_id: RequestId,
        identity_uri: WorkloadIdentityUri,
        certificate_generation: CertificateGeneration,
        public_key_spki: Ed25519PublicKeySpki,
        not_before_unix_ms: UnixMillis,
        lifetime_ms: u64,
        server_names: BTreeSet<String>,
    ) -> Result<Self, WorkloadPkiError> {
        if certificate_generation.get() == 0 {
            return Err(WorkloadPkiError::InvalidCertificateGeneration);
        }
        if not_before_unix_ms.get() == 0 {
            return Err(WorkloadPkiError::InvalidValidityWindow);
        }
        if lifetime_ms == 0 || lifetime_ms > MAX_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS {
            return Err(WorkloadPkiError::InvalidLifetime);
        }
        let not_after_unix_ms = not_before_unix_ms
            .get()
            .checked_add(lifetime_ms)
            .map(UnixMillis::new)
            .ok_or(WorkloadPkiError::InvalidValidityWindow)?;
        validate_server_names(&server_names)?;
        Ok(Self {
            request_id,
            identity_uri,
            certificate_generation,
            public_key_spki,
            not_before_unix_ms,
            not_after_unix_ms,
            server_names,
        })
    }

    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub fn identity_uri(&self) -> &WorkloadIdentityUri {
        &self.identity_uri
    }

    #[must_use]
    pub const fn certificate_generation(&self) -> CertificateGeneration {
        self.certificate_generation
    }

    #[must_use]
    pub fn public_key_spki(&self) -> &Ed25519PublicKeySpki {
        &self.public_key_spki
    }

    #[must_use]
    pub fn server_names(&self) -> &BTreeSet<String> {
        &self.server_names
    }

    #[must_use]
    pub fn required_extended_key_usages(&self) -> &'static [WorkloadCertificateUsage] {
        self.identity_uri.identity().required_extended_key_usages()
    }

    #[must_use]
    pub const fn not_before_unix_ms(&self) -> UnixMillis {
        self.not_before_unix_ms
    }

    #[must_use]
    pub const fn not_after_unix_ms(&self) -> UnixMillis {
        self.not_after_unix_ms
    }

    /// Renewal point at half of the requested certificate lifetime.
    #[must_use]
    pub fn renew_at_unix_ms(&self) -> UnixMillis {
        let lifetime = self.not_after_unix_ms.get() - self.not_before_unix_ms.get();
        UnixMillis::new(self.not_before_unix_ms.get() + lifetime / 2)
    }
}

/// Public certificate material returned by an external online Intermediate CA.
#[derive(Clone, PartialEq, Eq)]
pub struct IssuedWorkloadCertificate {
    request: WorkloadCertificateRequest,
    leaf_certificate_der: Vec<u8>,
    issuer_chain_der: Vec<Vec<u8>>,
}

impl IssuedWorkloadCertificate {
    pub fn from_request(
        request: WorkloadCertificateRequest,
        leaf_certificate_der: Vec<u8>,
        issuer_chain_der: Vec<Vec<u8>>,
    ) -> Result<Self, WorkloadPkiError> {
        if !valid_der_size(&leaf_certificate_der) {
            return Err(WorkloadPkiError::InvalidCertificateDer);
        }
        if issuer_chain_der.is_empty()
            || issuer_chain_der.len() > MAX_ISSUER_CHAIN_DEPTH
            || issuer_chain_der
                .iter()
                .any(|certificate| !valid_der_size(certificate))
        {
            return Err(WorkloadPkiError::InvalidCertificateChain);
        }
        Ok(Self {
            request,
            leaf_certificate_der,
            issuer_chain_der,
        })
    }

    #[must_use]
    pub fn request(&self) -> &WorkloadCertificateRequest {
        &self.request
    }

    #[must_use]
    pub fn leaf_certificate_der(&self) -> &[u8] {
        &self.leaf_certificate_der
    }

    #[must_use]
    pub fn leaf_certificate_fingerprint(&self) -> ContentDigest {
        ContentDigest::hash(&self.leaf_certificate_der)
    }

    #[must_use]
    pub fn issuer_chain_der(&self) -> &[Vec<u8>] {
        &self.issuer_chain_der
    }

    fn validate_for(&self, request: &WorkloadCertificateRequest) -> Result<(), WorkloadPkiError> {
        if self.request != *request {
            return Err(WorkloadPkiError::IssuerResponseMismatch);
        }
        Ok(())
    }
}

impl fmt::Debug for IssuedWorkloadCertificate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedWorkloadCertificate")
            .field("request", &self.request)
            .field(
                "leaf_certificate_der_bytes",
                &self.leaf_certificate_der.len(),
            )
            .field("issuer_chain_depth", &self.issuer_chain_der.len())
            .finish()
    }
}

/// Port implemented by an external KMS/HSM-backed online Intermediate CA.
///
/// No local private-key implementation is provided by `neoengram-central`.
#[async_trait]
pub trait WorkloadCertificateIssuer: Send + Sync {
    async fn issue(
        &self,
        request: WorkloadCertificateRequest,
    ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError>;
}

/// Fail-closed facade that validates an external issuer's response against its exact request.
#[derive(Clone)]
pub struct WorkloadCertificateIssuance {
    issuer: Arc<dyn WorkloadCertificateIssuer>,
}

impl WorkloadCertificateIssuance {
    #[must_use]
    pub fn new(issuer: Arc<dyn WorkloadCertificateIssuer>) -> Self {
        Self { issuer }
    }

    pub async fn issue(
        &self,
        request: WorkloadCertificateRequest,
    ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
        let issued = self.issuer.issue(request.clone()).await?;
        issued.validate_for(&request)?;
        Ok(issued)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkloadPkiError {
    #[error("workload identity URI SAN is not canonical or has an unsupported identity path")]
    InvalidIdentityUri,
    #[error("workload trust domain is invalid")]
    InvalidTrustDomain,
    #[error("workload identity URI SAN belongs to a different trust domain")]
    TrustDomainMismatch,
    #[error("certificate generation must be positive")]
    InvalidCertificateGeneration,
    #[error("workload leaf lifetime must be between one millisecond and six hours")]
    InvalidLifetime,
    #[error("workload certificate validity window is invalid")]
    InvalidValidityWindow,
    #[error("workload certificate DER is empty or exceeds the size limit")]
    InvalidCertificateDer,
    #[error("workload certificate issuer chain is empty or exceeds its limits")]
    InvalidCertificateChain,
    #[error("workload certificate server names are invalid")]
    InvalidServerNames,
    #[error("workload certificate material does not match its issuance request")]
    InvalidCertificateMaterial,
    #[error("workload certificate does not contain exactly one canonical URI identity SAN")]
    InvalidCertificateIdentity,
    #[error("external issuer response does not match the complete issuance request")]
    IssuerResponseMismatch,
}

#[derive(Debug, Error)]
pub enum WorkloadCertificateIssuerError {
    #[error(transparent)]
    Invalid(#[from] WorkloadPkiError),
    #[error("workload certificate issuer is unavailable: {0}")]
    Unavailable(String),
    #[error("workload certificate request was rejected: {0}")]
    Rejected(String),
    #[error("workload certificate issuer failed: {0}")]
    Internal(String),
}

fn valid_der_size(der: &[u8]) -> bool {
    !der.is_empty() && der.len() <= MAX_CERTIFICATE_DER_BYTES
}

fn validate_server_names(server_names: &BTreeSet<String>) -> Result<(), WorkloadPkiError> {
    if server_names
        .iter()
        .any(|name| name.is_empty() || name.len() > 253 || name.trim() != name)
    {
        return Err(WorkloadPkiError::InvalidServerNames);
    }
    for name in server_names {
        if name.parse::<IpAddr>().is_ok() {
            continue;
        }
        if name.bytes().any(|byte| byte.is_ascii_uppercase())
            || name.ends_with('.')
            || name.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || !label
                        .bytes()
                        .next()
                        .is_some_and(|byte| byte.is_ascii_alphanumeric())
                    || !label
                        .bytes()
                        .last()
                        .is_some_and(|byte| byte.is_ascii_alphanumeric())
                    || label
                        .bytes()
                        .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'-')
            })
        {
            return Err(WorkloadPkiError::InvalidServerNames);
        }
    }
    Ok(())
}

/// Extracts the workload identity from the leaf certificate's URI Subject Alternative Name.
///
/// Rustls performs the cryptographic chain and validity checks. It intentionally does not
/// interpret application identities, so the Central adapter must still bind the authenticated
/// peer to the Registry record. This parser accepts only a complete X.509 certificate with one
/// `subjectAltName` extension and exactly one URI identity SAN.
pub fn parse_workload_identity_from_certificate_der(
    certificate_der: &[u8],
    expected_trust_domain: &str,
) -> Result<WorkloadIdentityUri, WorkloadPkiError> {
    if !valid_der_size(certificate_der) {
        return Err(WorkloadPkiError::InvalidCertificateDer);
    }
    let (remainder, certificate) = X509Certificate::from_der(certificate_der)
        .map_err(|_| WorkloadPkiError::InvalidCertificateIdentity)?;
    if !remainder.is_empty() {
        return Err(WorkloadPkiError::InvalidCertificateIdentity);
    }
    let san = certificate
        .subject_alternative_name()
        .map_err(|_| WorkloadPkiError::InvalidCertificateIdentity)?
        .ok_or(WorkloadPkiError::InvalidCertificateIdentity)?;
    let mut uris = san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(uri),
            _ => None,
        });
    let uri = uris
        .next()
        .ok_or(WorkloadPkiError::InvalidCertificateIdentity)?;
    if uris.next().is_some() {
        return Err(WorkloadPkiError::InvalidCertificateIdentity);
    }
    WorkloadIdentityUri::parse_for_trust_domain(uri, expected_trust_domain)
        .map_err(|_| WorkloadPkiError::InvalidCertificateIdentity)
}

/// Validates the part of an issuer response that can be checked without the CA private key.
/// Rustls will perform the chain signature and wall-clock checks at connection time, but Central
/// must reject a mismatched public key, URI identity, EKU, validity window, or endpoint SAN before
/// persisting a newly issued Gateway certificate.
pub fn validate_issued_workload_certificate(
    issued: &IssuedWorkloadCertificate,
    now: UnixMillis,
) -> Result<(), WorkloadPkiError> {
    let request = issued.request();
    let identity = parse_workload_identity_from_certificate_der(
        issued.leaf_certificate_der(),
        request.identity_uri().trust_domain(),
    )?;
    if &identity != request.identity_uri() {
        return Err(WorkloadPkiError::InvalidCertificateMaterial);
    }
    let (remainder, certificate) = X509Certificate::from_der(issued.leaf_certificate_der())
        .map_err(|_| WorkloadPkiError::InvalidCertificateMaterial)?;
    if !remainder.is_empty() || certificate.public_key().raw != request.public_key_spki().as_der() {
        return Err(WorkloadPkiError::InvalidCertificateMaterial);
    }

    let not_before_ms = unix_ms_from_timestamp(certificate.validity().not_before.timestamp())?;
    let not_after_ms = unix_ms_from_timestamp(certificate.validity().not_after.timestamp())?;
    if not_before_ms > request.not_before_unix_ms().get()
        || not_after_ms < request.not_after_unix_ms().get()
        || now.get() < not_before_ms
        || now.get() >= not_after_ms
        || not_after_ms.saturating_sub(not_before_ms)
            > MAX_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS.saturating_add(2_000)
    {
        return Err(WorkloadPkiError::InvalidCertificateMaterial);
    }

    let eku = certificate
        .extended_key_usage()
        .map_err(|_| WorkloadPkiError::InvalidCertificateMaterial)?
        .ok_or(WorkloadPkiError::InvalidCertificateMaterial)?;
    let required = request
        .identity_uri()
        .identity()
        .required_extended_key_usages();
    for usage in required {
        let present = match usage {
            WorkloadCertificateUsage::ClientAuth => eku.value.client_auth,
            WorkloadCertificateUsage::ServerAuth => eku.value.server_auth,
        };
        if !present {
            return Err(WorkloadPkiError::InvalidCertificateMaterial);
        }
    }

    let san = certificate
        .subject_alternative_name()
        .map_err(|_| WorkloadPkiError::InvalidCertificateMaterial)?
        .ok_or(WorkloadPkiError::InvalidCertificateMaterial)?;
    let mut server_names = BTreeSet::new();
    for name in &san.value.general_names {
        match name {
            GeneralName::DNSName(value) => {
                server_names.insert((*value).to_owned());
            }
            GeneralName::IPAddress(value) => {
                let address = match value.len() {
                    4 => IpAddr::from(
                        <[u8; 4]>::try_from(*value)
                            .map_err(|_| WorkloadPkiError::InvalidCertificateMaterial)?,
                    ),
                    16 => IpAddr::from(
                        <[u8; 16]>::try_from(*value)
                            .map_err(|_| WorkloadPkiError::InvalidCertificateMaterial)?,
                    ),
                    _ => return Err(WorkloadPkiError::InvalidCertificateMaterial),
                };
                server_names.insert(address.to_string());
            }
            _ => {}
        }
    }
    if server_names != *request.server_names() {
        return Err(WorkloadPkiError::InvalidCertificateMaterial);
    }
    for chain_certificate in issued.issuer_chain_der() {
        let (remainder, _) = X509Certificate::from_der(chain_certificate)
            .map_err(|_| WorkloadPkiError::InvalidCertificateChain)?;
        if !remainder.is_empty() {
            return Err(WorkloadPkiError::InvalidCertificateChain);
        }
    }
    Ok(())
}

fn unix_ms_from_timestamp(timestamp: i64) -> Result<u64, WorkloadPkiError> {
    u64::try_from(timestamp)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or(WorkloadPkiError::InvalidCertificateMaterial)
}

fn validate_trust_domain(value: &str) -> Result<(), WorkloadPkiError> {
    if value.is_empty() || value.len() > 253 || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(WorkloadPkiError::InvalidTrustDomain);
    }
    let valid = value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && label
                .bytes()
                .last()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    });
    if valid {
        Ok(())
    } else {
        Err(WorkloadPkiError::InvalidTrustDomain)
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    fn gateway_uri() -> WorkloadIdentityUri {
        WorkloadIdentityUri::gateway_replica(
            "mesh.example.test",
            EdgeClusterId::new("edge-a").unwrap(),
            GatewayPoolId::new("pool-a").unwrap(),
            GatewayReplicaId::new("replica-a").unwrap(),
        )
        .unwrap()
    }

    fn request(at: u64) -> WorkloadCertificateRequest {
        WorkloadCertificateRequest::new(
            RequestId::new("issue-a").unwrap(),
            gateway_uri(),
            CertificateGeneration::new(3),
            Ed25519PublicKeySpki::from_public_key_bytes([7; 32]),
            UnixMillis::new(at),
        )
        .unwrap()
    }

    #[test]
    fn identity_uri_round_trips_and_enforces_scope() {
        let central = WorkloadIdentityUri::central("mesh.example.test").unwrap();
        assert_eq!(
            central.as_str(),
            "spiffe://mesh.example.test/workloads/central"
        );
        assert!(matches!(central.identity(), WorkloadIdentity::Central));
        assert_eq!(
            central.identity().required_extended_key_usages(),
            &[WorkloadCertificateUsage::ClientAuth]
        );

        let gateway = gateway_uri();
        assert_eq!(
            WorkloadIdentityUri::parse(gateway.as_str()).unwrap(),
            gateway
        );
        assert!(matches!(
            gateway.identity(),
            WorkloadIdentity::GatewayReplica { gateway_replica_id, .. }
                if gateway_replica_id.as_str() == "replica-a"
        ));

        let agent = WorkloadIdentityUri::agent(
            "mesh.example.test",
            EdgeClusterId::new("edge-a").unwrap(),
            AgentId::new("agent-a").unwrap(),
        )
        .unwrap();
        assert!(matches!(agent.identity(), WorkloadIdentity::Agent { .. }));
        assert_eq!(
            agent.identity().required_extended_key_usages(),
            &[WorkloadCertificateUsage::ClientAuth]
        );
        assert!(matches!(
            WorkloadIdentityUri::parse_for_trust_domain(agent.as_str(), "other.example.test"),
            Err(WorkloadPkiError::TrustDomainMismatch)
        ));
    }

    #[test]
    fn identity_uri_rejects_ambiguous_or_unscoped_values() {
        assert!(WorkloadIdentityUri::parse(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/agents/%61gent-a"
        )
        .is_err());
        assert!(WorkloadIdentityUri::parse(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/agents/agent-a?admin=true"
        )
        .is_err());
        assert!(
            WorkloadIdentityUri::parse("spiffe://mesh.example.test/workloads/agents/agent-a")
                .is_err()
        );
        assert!(WorkloadIdentityUri::parse(
            "spiffe://mesh.example.test/workloads/central/replica-a"
        )
        .is_err());
    }

    fn certificate_with_uri_sans(uris: &[&str]) -> Vec<u8> {
        use rcgen::{CertificateParams, KeyPair, SanType};

        let mut parameters = CertificateParams::new(vec!["gateway.example.test".to_owned()])
            .expect("certificate parameters");
        parameters.subject_alt_names.extend(
            uris.iter()
                .map(|uri| SanType::URI((*uri).try_into().expect("URI SAN must be valid IA5"))),
        );
        let key_pair = KeyPair::generate().expect("test key");
        parameters
            .self_signed(&key_pair)
            .expect("test certificate")
            .der()
            .to_vec()
    }

    #[test]
    fn certificate_identity_parser_binds_the_single_uri_san() {
        let expected = gateway_uri();
        let certificate = certificate_with_uri_sans(&[expected.as_str()]);
        assert_eq!(
            parse_workload_identity_from_certificate_der(&certificate, "mesh.example.test")
                .unwrap(),
            expected
        );
        assert!(matches!(
            parse_workload_identity_from_certificate_der(&certificate, "other.example.test"),
            Err(WorkloadPkiError::InvalidCertificateIdentity)
        ));
    }

    #[test]
    fn certificate_identity_parser_rejects_multiple_uri_sans() {
        let gateway = gateway_uri();
        let certificate = certificate_with_uri_sans(&[
            gateway.as_str(),
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/agents/agent-a",
        ]);
        assert!(matches!(
            parse_workload_identity_from_certificate_der(&certificate, "mesh.example.test"),
            Err(WorkloadPkiError::InvalidCertificateIdentity)
        ));

        let certificate = certificate_with_uri_sans(&[]);
        assert!(matches!(
            parse_workload_identity_from_certificate_der(&certificate, "mesh.example.test"),
            Err(WorkloadPkiError::InvalidCertificateIdentity)
        ));
    }

    #[test]
    fn certificate_request_defaults_to_six_hours_and_renews_halfway() {
        let request = request(1_000);
        assert_eq!(
            request.not_after_unix_ms().get(),
            1_000 + DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS
        );
        assert_eq!(
            request.renew_at_unix_ms().get(),
            1_000 + DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS / 2
        );
        assert!(matches!(
            WorkloadCertificateRequest::with_lifetime_ms(
                RequestId::new("issue-b").unwrap(),
                gateway_uri(),
                CertificateGeneration::new(1),
                Ed25519PublicKeySpki::from_public_key_bytes([8; 32]),
                UnixMillis::new(1_000),
                MAX_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS + 1,
            ),
            Err(WorkloadPkiError::InvalidLifetime)
        ));
    }

    #[derive(Clone)]
    struct FixedIssuer {
        result: IssuedWorkloadCertificate,
    }

    #[async_trait]
    impl WorkloadCertificateIssuer for FixedIssuer {
        async fn issue(
            &self,
            _request: WorkloadCertificateRequest,
        ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
            Ok(self.result.clone())
        }
    }

    #[tokio::test]
    async fn issuance_facade_rejects_response_for_another_request() {
        let expected = request(1_000);
        let wrong = request(2_000);
        let result = IssuedWorkloadCertificate::from_request(
            wrong,
            vec![0x30, 0x01],
            vec![vec![0x30, 0x02]],
        )
        .unwrap();
        let issuance = WorkloadCertificateIssuance::new(Arc::new(FixedIssuer { result }));
        assert!(matches!(
            issuance.issue(expected).await,
            Err(WorkloadCertificateIssuerError::Invalid(
                WorkloadPkiError::IssuerResponseMismatch
            ))
        ));
    }
}

/// Builds a structurally valid issuer response for unit tests. Production code must use the
/// external KMS/HSM-backed issuer instead; keeping this helper here prevents activation tests
/// from accidentally bypassing the same leaf validation used by the real path.
#[cfg(test)]
pub(crate) fn test_issue_workload_certificate(
    request: WorkloadCertificateRequest,
) -> IssuedWorkloadCertificate {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
        SubjectPublicKeyInfo,
    };
    use time::OffsetDateTime;

    let issuer_key = KeyPair::generate().expect("issuer key");
    let mut issuer_params = CertificateParams::default();
    issuer_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    issuer_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::DigitalSignature,
    ];
    issuer_params.not_before = OffsetDateTime::from_unix_timestamp(0).unwrap();
    issuer_params.not_after = OffsetDateTime::from_unix_timestamp(4_000_000_000).unwrap();
    let issuer = issuer_params
        .self_signed(&issuer_key)
        .expect("issuer certificate");

    let mut leaf_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    leaf_params.not_before = OffsetDateTime::from_unix_timestamp(
        i64::try_from(request.not_before_unix_ms().get() / 1_000).unwrap(),
    )
    .unwrap();
    leaf_params.not_after = OffsetDateTime::from_unix_timestamp(
        i64::try_from(request.not_after_unix_ms().get().div_ceil(1_000)).unwrap(),
    )
    .unwrap();
    leaf_params.subject_alt_names.push(SanType::URI(
        request
            .identity_uri()
            .as_str()
            .try_into()
            .expect("identity URI SAN"),
    ));
    for name in request.server_names() {
        if let Ok(address) = name.parse() {
            leaf_params
                .subject_alt_names
                .push(SanType::IpAddress(address));
        } else {
            leaf_params
                .subject_alt_names
                .push(SanType::DnsName(name.clone().try_into().expect("DNS SAN")));
        }
    }
    leaf_params.extended_key_usages = request
        .identity_uri()
        .identity()
        .required_extended_key_usages()
        .iter()
        .map(|usage| match usage {
            WorkloadCertificateUsage::ClientAuth => ExtendedKeyUsagePurpose::ClientAuth,
            WorkloadCertificateUsage::ServerAuth => ExtendedKeyUsagePurpose::ServerAuth,
        })
        .collect();
    let public_key = SubjectPublicKeyInfo::from_der(request.public_key_spki().as_der())
        .expect("request public key SPKI");
    let leaf = leaf_params
        .signed_by(&public_key, &issuer, &issuer_key)
        .expect("leaf certificate");
    IssuedWorkloadCertificate::from_request(
        request,
        leaf.der().to_vec(),
        vec![issuer.der().to_vec()],
    )
    .expect("test issuer response")
}
