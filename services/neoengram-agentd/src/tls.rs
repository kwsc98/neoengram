use std::{
    fmt::Write as _,
    fs,
    io::Read,
    path::{Component, Path},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use neoengram_agent::{AgentCertificateState, SystemIdentityRecord};
use neoengram_protocol::{
    AgentWorkloadCertificateBundle, Ed25519PublicKeySpki, GatewayPoolId, GatewayReplicaId,
    MAX_AGENT_WORKLOAD_CERTIFICATE_CHAIN_LENGTH, MAX_AGENT_WORKLOAD_CERTIFICATE_DER_BYTES,
};
use rustls_pki_types::{pem::PemObject, CertificateDer};
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer, X509Certificate},
};

use crate::{AgentDaemonError, AgentDaemonResult};

const MAX_TRUST_BUNDLE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct GatewayTrustBundle {
    certificates: Vec<CertificateDer<'static>>,
}

/// Application identity expected from the Gateway server certificate.  The CA bundle proves the
/// cryptographic issuer; this binding proves that the issuer is the Gateway for this EdgeCluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GatewayServerIdentity {
    pub(crate) trust_domain: String,
    pub(crate) edge_cluster_id: String,
}

pub(crate) fn validate_gateway_server_certificate(
    peer_certificates: Option<&[CertificateDer<'_>]>,
    expected: &GatewayServerIdentity,
) -> AgentDaemonResult<()> {
    let certificates = peer_certificates
        .ok_or_else(|| configuration("Gateway did not present a workload server certificate"))?;
    let leaf = certificates
        .first()
        .ok_or_else(|| configuration("Gateway presented an empty certificate chain"))?;
    let (remainder, certificate) = X509Certificate::from_der(leaf.as_ref())
        .map_err(|_| configuration("Gateway workload certificate is invalid DER"))?;
    if !remainder.is_empty() {
        return Err(configuration(
            "Gateway workload certificate contains trailing DER data",
        ));
    }
    let eku = certificate
        .extended_key_usage()
        .map_err(|_| configuration("Gateway workload certificate EKU is invalid"))?
        .ok_or_else(|| configuration("Gateway workload certificate has no EKU"))?;
    if !eku.value.server_auth {
        return Err(configuration(
            "Gateway workload certificate is not valid for server authentication",
        ));
    }
    let san = certificate
        .subject_alternative_name()
        .map_err(|_| configuration("Gateway workload certificate SAN is invalid"))?
        .ok_or_else(|| configuration("Gateway workload certificate has no SAN"))?;
    let uris = san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        })
        .collect::<Vec<_>>();
    if uris.len() != 1 {
        return Err(configuration(
            "Gateway workload certificate must contain exactly one URI SAN",
        ));
    }
    let uri = uris[0];
    if uri.contains(['?', '#', '%']) {
        return Err(configuration("Gateway workload URI SAN is not canonical"));
    }
    let parsed =
        url::Url::parse(uri).map_err(|_| configuration("Gateway workload URI SAN is invalid"))?;
    if parsed.scheme() != "spiffe"
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.as_str() != uri
        || parsed.host_str() != Some(expected.trust_domain.as_str())
    {
        return Err(configuration(
            "Gateway workload URI SAN trust domain is not expected",
        ));
    }
    let segments = parsed
        .path_segments()
        .ok_or_else(|| configuration("Gateway workload URI SAN has no path"))?
        .collect::<Vec<_>>();
    if segments.len() != 7
        || segments[0] != "workloads"
        || segments[1] != "edge-clusters"
        || segments[2] != expected.edge_cluster_id
        || segments[3] != "gateway-pools"
        || segments[5] != "gateway-replicas"
        || GatewayPoolId::new(segments[4]).is_err()
        || GatewayReplicaId::new(segments[6]).is_err()
    {
        return Err(configuration(
            "Gateway workload URI SAN is outside the configured EdgeCluster",
        ));
    }
    Ok(())
}

/// Returns the monotonic deadline at which an already-established Gateway TLS connection must be
/// closed. Rustls validates the leaf during the handshake, but it does not terminate an existing
/// HTTP/2 connection when the peer certificate later reaches `notAfter`.
///
/// The caller owns the connection lifetime and should treat every parse/expiry error as a
/// transport failure so the normal reconnect loop can obtain a newly-issued Gateway certificate.
pub(crate) fn gateway_server_certificate_deadline(
    peer_certificates: Option<&[CertificateDer<'_>]>,
) -> AgentDaemonResult<Instant> {
    let certificates = peer_certificates
        .ok_or_else(|| configuration("Gateway did not present a server certificate"))?;
    let leaf = certificates
        .first()
        .ok_or_else(|| configuration("Gateway presented an empty server certificate chain"))?;
    let (remainder, certificate) = X509Certificate::from_der(leaf.as_ref()).map_err(|error| {
        configuration(format!(
            "Gateway server certificate is invalid DER: {error}"
        ))
    })?;
    if !remainder.is_empty() {
        return Err(configuration(
            "Gateway server certificate contains trailing DER data",
        ));
    }
    let not_after_seconds = certificate.validity().not_after.timestamp();
    let now_instant = Instant::now();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| configuration("system clock precedes Unix epoch"))?;
    certificate_deadline_from_not_after(
        not_after_seconds,
        u64::try_from(now.as_millis())
            .map_err(|_| configuration("system clock is out of range"))?,
        now_instant,
    )
}

fn certificate_deadline_from_not_after(
    not_after_seconds: i64,
    now_unix_ms: u64,
    now_instant: Instant,
) -> AgentDaemonResult<Instant> {
    let not_after_millis = i128::from(not_after_seconds)
        .checked_mul(1_000)
        .ok_or_else(|| configuration("Gateway server certificate expiry timestamp overflowed"))?;
    let now_millis = i128::from(now_unix_ms);
    if not_after_millis <= now_millis {
        return Err(configuration("Gateway server certificate is expired"));
    }
    let remaining_millis = u64::try_from(not_after_millis - now_millis)
        .map_err(|_| configuration("Gateway server certificate expiry is out of range"))?;
    now_instant
        .checked_add(Duration::from_millis(remaining_millis))
        .ok_or_else(|| configuration("Gateway server certificate deadline overflowed"))
}

#[derive(Debug)]
struct GatewayServerCertificateVerifier {
    webpki: Arc<dyn rustls::client::danger::ServerCertVerifier>,
    expected: GatewayServerIdentity,
}

impl rustls::client::danger::ServerCertVerifier for GatewayServerCertificateVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let verified = self.webpki.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        validate_gateway_server_certificate(Some(std::slice::from_ref(end_entity)), &self.expected)
            .map_err(|_| {
                rustls::Error::InvalidCertificate(
                    rustls::CertificateError::ApplicationVerificationFailure,
                )
            })?;
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signed: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.webpki
            .verify_tls12_signature(message, certificate, signed)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signed: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.webpki
            .verify_tls13_signature(message, certificate, signed)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.webpki.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.webpki.requires_raw_public_keys()
    }

    fn root_hint_subjects(&self) -> Option<&[rustls::DistinguishedName]> {
        self.webpki.root_hint_subjects()
    }
}

const MAX_AGENT_CERTIFICATE_PEM_BYTES: usize = 1024 * 1024;

/// Converts and validates an approved wire bundle before it enters the durable identity store.
pub(crate) fn certificate_state_from_bundle(
    bundle: &AgentWorkloadCertificateBundle,
    identity: &SystemIdentityRecord,
    expected_edge_cluster_id: &str,
) -> AgentDaemonResult<AgentCertificateState> {
    bundle.validate().map_err(|error| {
        configuration(format!("Agent workload certificate is invalid: {error}"))
    })?;
    let approved = identity
        .approved
        .as_ref()
        .ok_or_else(|| configuration("Agent workload certificate arrived before Agent approval"))?;
    if bundle.edge_cluster_id.as_str() != expected_edge_cluster_id
        || bundle.agent_id.as_str() != approved.agent_id
        || bundle.enrollment_id.as_str() != approved.enrollment_id
    {
        return Err(configuration(
            "Agent workload certificate identity does not match persisted approval",
        ));
    }
    let signing_key = crate::signing_key_from_identity(identity)?;
    let public_key_spki =
        Ed25519PublicKeySpki::new(crate::identity::public_key_spki_der(&signing_key)?)
            .map_err(|error| configuration(error.to_string()))?;
    if bundle.public_key_fingerprint != public_key_spki.fingerprint() {
        return Err(configuration(
            "Agent workload certificate public key does not match the Agent key",
        ));
    }
    validate_leaf_certificate(bundle, public_key_spki.as_der())?;
    if bundle.issuer_chain_der.len() > MAX_AGENT_WORKLOAD_CERTIFICATE_CHAIN_LENGTH {
        return Err(configuration(
            "Agent workload certificate chain is too long",
        ));
    }
    let mut certificate_chain_pem = Vec::with_capacity(1 + bundle.issuer_chain_der.len());
    certificate_chain_pem.push(der_to_pem(
        "CERTIFICATE",
        bundle.leaf_certificate_der.as_bytes(),
    ));
    for certificate in &bundle.issuer_chain_der {
        certificate_chain_pem.push(der_to_pem("CERTIFICATE", certificate.as_bytes()));
    }
    let pem_bytes = certificate_chain_pem.iter().map(String::len).sum::<usize>();
    if pem_bytes > MAX_AGENT_CERTIFICATE_PEM_BYTES {
        return Err(configuration(
            "Agent workload certificate chain exceeds the size limit",
        ));
    }
    Ok(AgentCertificateState {
        certificate_chain_pem,
        certificate_generation: bundle.certificate_generation.get(),
        session_generation: bundle.session_generation.get(),
        mount_generation: bundle.mount_generation.get(),
        owner_generation: bundle.owner_generation.get(),
        public_key_fingerprint: Some(bundle.public_key_fingerprint),
        identity_uri: Some(bundle.identity_uri.clone()),
        not_before_unix_ms: Some(bundle.not_before_unix_ms),
        not_after_unix_ms: Some(bundle.not_after_unix_ms),
        renew_at_unix_ms: Some(bundle.renew_at_unix_ms),
    })
}

fn validate_leaf_certificate(
    bundle: &AgentWorkloadCertificateBundle,
    expected_public_key_spki: &[u8],
) -> AgentDaemonResult<()> {
    if bundle.leaf_certificate_der.as_bytes().len() > MAX_AGENT_WORKLOAD_CERTIFICATE_DER_BYTES {
        return Err(configuration(
            "Agent workload leaf certificate exceeds the size limit",
        ));
    }
    let (remainder, certificate) =
        X509Certificate::from_der(bundle.leaf_certificate_der.as_bytes())
            .map_err(|_| configuration("Agent workload leaf certificate is not valid DER"))?;
    if !remainder.is_empty() || certificate.public_key().raw != expected_public_key_spki {
        return Err(configuration(
            "Agent workload leaf certificate public key is not the Agent key",
        ));
    }
    let san = certificate
        .subject_alternative_name()
        .map_err(|_| configuration("Agent workload leaf certificate SAN is invalid"))?
        .ok_or_else(|| configuration("Agent workload leaf certificate has no SAN"))?;
    let mut uris = san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        });
    if uris.next() != Some(bundle.identity_uri.as_str()) || uris.next().is_some() {
        return Err(configuration(
            "Agent workload leaf certificate URI SAN is not exact",
        ));
    }
    Ok(())
}

pub(crate) fn rustls_server_auth_client_config(
    trust_bundle: &GatewayTrustBundle,
    expected: Option<&GatewayServerIdentity>,
    alpn_protocols: Vec<Vec<u8>>,
) -> AgentDaemonResult<rustls::ClientConfig> {
    let roots = gateway_root_store(trust_bundle)?;
    let provider: Arc<rustls::crypto::CryptoProvider> =
        rustls::crypto::aws_lc_rs::default_provider().into();
    let mut config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| {
            configuration(format!(
                "Gateway TLS protocol configuration failed: {error}"
            ))
        })?
        .with_root_certificates(roots.clone())
        .with_no_client_auth();
    bind_gateway_server_identity(&mut config, roots, expected, provider)?;
    // A connection reopened after the observed leaf deadline must perform a full handshake and
    // obtain the rotated Gateway certificate. A resumed TLS session intentionally skips chain
    // verification and carries the previous peer chain, which is unsuitable for this lifecycle.
    config.resumption = rustls::client::Resumption::disabled();
    config.alpn_protocols = alpn_protocols;
    Ok(config)
}

pub(crate) fn rustls_client_config(
    trust_bundle: &GatewayTrustBundle,
    identity: &SystemIdentityRecord,
    expected: Option<&GatewayServerIdentity>,
    alpn_protocols: Vec<Vec<u8>>,
) -> AgentDaemonResult<rustls::ClientConfig> {
    use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    let roots = gateway_root_store(trust_bundle)?;
    let certificate = identity
        .certificate
        .as_ref()
        .ok_or_else(|| configuration("approved Agent has no workload certificate"))?;
    let chain = certificate
        .certificate_chain_pem
        .iter()
        .flat_map(|pem| CertificateDer::pem_slice_iter(pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| configuration(format!("Agent certificate PEM is invalid: {error}")))?;
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        identity.private_key.expose_secret().to_vec(),
    ));
    let provider: Arc<rustls::crypto::CryptoProvider> =
        rustls::crypto::aws_lc_rs::default_provider().into();
    let mut config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| {
            configuration(format!("Agent mTLS protocol configuration failed: {error}"))
        })?
        .with_root_certificates(roots.clone())
        .with_client_auth_cert(chain, key)
        .map_err(|error| configuration(format!("Agent mTLS configuration failed: {error}")))?;
    bind_gateway_server_identity(&mut config, roots, expected, provider)?;
    config.resumption = rustls::client::Resumption::disabled();
    config.alpn_protocols = alpn_protocols;
    Ok(config)
}

fn gateway_root_store(
    trust_bundle: &GatewayTrustBundle,
) -> AgentDaemonResult<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in trust_bundle.certificates() {
        roots.add(certificate.clone()).map_err(|error| {
            configuration(format!(
                "Gateway trust bundle contains an invalid trust anchor: {error}"
            ))
        })?;
    }
    Ok(roots)
}

fn bind_gateway_server_identity(
    config: &mut rustls::ClientConfig,
    roots: rustls::RootCertStore,
    expected: Option<&GatewayServerIdentity>,
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> AgentDaemonResult<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let webpki =
        rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|error| {
                configuration(format!("Gateway trust verifier is invalid: {error}"))
            })?;
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(GatewayServerCertificateVerifier {
            webpki,
            expected: expected.clone(),
        }));
    Ok(())
}

fn der_to_pem(label: &str, der: &[u8]) -> String {
    let encoded = STANDARD.encode(der);
    let mut output = String::new();
    let _ = writeln!(output, "-----BEGIN {label}-----");
    for chunk in encoded.as_bytes().chunks(64) {
        // Base64 is ASCII, so this conversion cannot fail.
        let line = std::str::from_utf8(chunk).expect("base64 is ASCII");
        let _ = writeln!(output, "{line}");
    }
    let _ = writeln!(output, "-----END {label}-----");
    output
}

impl GatewayTrustBundle {
    pub(crate) fn load(path: &Path) -> AgentDaemonResult<Self> {
        validate_path(path)?;
        let resolved = fs::canonicalize(path).map_err(|error| {
            configuration(format!(
                "Gateway trust bundle could not be resolved: {error}"
            ))
        })?;
        let metadata = fs::symlink_metadata(&resolved).map_err(|error| {
            configuration(format!(
                "Gateway trust bundle could not be inspected: {error}"
            ))
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(configuration(
                "Gateway trust bundle must resolve to a regular file",
            ));
        }
        if metadata.len() == 0 || metadata.len() > MAX_TRUST_BUNDLE_BYTES {
            return Err(configuration(format!(
                "Gateway trust bundle must contain 1..={MAX_TRUST_BUNDLE_BYTES} bytes"
            )));
        }
        validate_permissions(&metadata)?;

        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        fs::File::open(&resolved)
            .and_then(|file| {
                file.take(MAX_TRUST_BUNDLE_BYTES + 1)
                    .read_to_end(&mut bytes)
            })
            .map_err(|error| {
                configuration(format!("Gateway trust bundle could not be read: {error}"))
            })?;
        if bytes.len() as u64 > MAX_TRUST_BUNDLE_BYTES {
            return Err(configuration("Gateway trust bundle exceeds the size limit"));
        }
        if bytes
            .windows(b"PRIVATE KEY".len())
            .any(|part| part == b"PRIVATE KEY")
        {
            return Err(configuration(
                "Gateway trust bundle must not contain private key material",
            ));
        }
        let confirmed = fs::canonicalize(path).map_err(|error| {
            configuration(format!(
                "Gateway trust bundle could not be confirmed: {error}"
            ))
        })?;
        if confirmed != resolved {
            return Err(configuration(
                "Gateway trust bundle changed while it was being loaded",
            ));
        }

        let certificates = CertificateDer::pem_slice_iter(&bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| configuration(format!("Gateway trust bundle is invalid: {error}")))?;
        if certificates.is_empty() {
            return Err(configuration(
                "Gateway trust bundle must contain at least one PEM certificate",
            ));
        }
        Ok(Self { certificates })
    }

    pub(crate) fn certificates(&self) -> &[CertificateDer<'static>] {
        &self.certificates
    }
}

fn validate_path(path: &Path) -> AgentDaemonResult<()> {
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::CurDir | Component::Prefix(_)
            )
        })
    {
        return Err(configuration(
            "trust_bundle_file must be an absolute normalized path",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_permissions(metadata: &fs::Metadata) -> AgentDaemonResult<()> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(configuration(
            "Gateway trust bundle must not be writable by group or other users",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_permissions(_metadata: &fs::Metadata) -> AgentDaemonResult<()> {
    Ok(())
}

fn configuration(message: impl Into<String>) -> AgentDaemonError {
    AgentDaemonError::Configuration(message.into())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use neoengram_protocol::{
        AgentBootstrapProof, AgentBootstrapStatusRequest, AgentBootstrapStatusResponse,
        AgentBootstrapStatusState, AgentEnrollmentId, AgentInstallationId, AgentSignatureAlgorithm,
        Ed25519PublicKeySpki, Ed25519Signature, Extensions, RequestId, TenantId, UnixMillis,
        PROTOCOL_VERSION_V1,
    };
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;
    use crate::client::EnrollmentClient as _;

    fn workload_certificate(uri: &str, server_auth: bool) -> CertificateDer<'static> {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, SanType};

        let mut parameters =
            CertificateParams::new(vec!["gateway.example.test".to_owned()]).unwrap();
        parameters.subject_alt_names.push(SanType::URI(
            uri.try_into().expect("test URI SAN must be IA5"),
        ));
        parameters.extended_key_usages = vec![if server_auth {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
        let key = KeyPair::generate().unwrap();
        CertificateDer::from(parameters.self_signed(&key).unwrap().der().to_vec())
    }

    fn gateway_identity() -> GatewayServerIdentity {
        GatewayServerIdentity {
            trust_domain: "mesh.example.test".to_owned(),
            edge_cluster_id: "edge-a".to_owned(),
        }
    }

    struct GatewayTlsFixture {
        trust_bundle: GatewayTrustBundle,
        server_config: Arc<rustls::ServerConfig>,
    }

    /// Creates the same CA/leaf shape used by the production Gateway listener: a server-auth leaf
    /// carries the endpoint SAN and, once activated, one workload URI SAN. Keeping this fixture
    /// here lets both handshake and enrollment tests exercise the real rustls verifier.
    fn gateway_tls_fixture(
        uri: Option<&str>,
        endpoint_host: &str,
        alpn: Vec<Vec<u8>>,
    ) -> GatewayTlsFixture {
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
            KeyUsagePurpose, SanType,
        };
        use rustls_pki_types::PrivatePkcs8KeyDer;

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca = ca_parameters.self_signed(&ca_key).unwrap();

        let server_key = KeyPair::generate().unwrap();
        let mut server_parameters = CertificateParams::new(vec![endpoint_host.to_owned()]).unwrap();
        if let Some(uri) = uri {
            server_parameters.subject_alt_names.push(SanType::URI(
                uri.try_into().expect("test URI SAN must be IA5"),
            ));
        }
        server_parameters.extended_key_usages = if uri.is_some() {
            vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ]
        } else {
            vec![ExtendedKeyUsagePurpose::ServerAuth]
        };
        let server_certificate = server_parameters
            .signed_by(&server_key, &ca, &ca_key)
            .unwrap();
        let provider: Arc<rustls::crypto::CryptoProvider> =
            rustls::crypto::aws_lc_rs::default_provider().into();
        let mut server_config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![server_certificate.der().clone(), ca.der().clone()],
                PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )
            .unwrap();
        server_config.alpn_protocols = alpn;
        GatewayTlsFixture {
            trust_bundle: GatewayTrustBundle {
                certificates: vec![ca.der().clone()],
            },
            server_config: Arc::new(server_config),
        }
    }

    async fn gateway_tls_handshake(uri: &str) -> Result<(), std::io::Error> {
        use tokio_rustls::{TlsAcceptor, TlsConnector};

        let fixture = gateway_tls_fixture(Some(uri), "gateway.example.test", vec![b"h2".to_vec()]);
        let client_config = rustls_server_auth_client_config(
            &fixture.trust_bundle,
            Some(&gateway_identity()),
            vec![b"h2".to_vec()],
        )
        .unwrap();

        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let server = TlsAcceptor::from(fixture.server_config).accept(server_io);
        let client = TlsConnector::from(Arc::new(client_config)).connect(
            rustls::pki_types::ServerName::try_from("gateway.example.test").unwrap(),
            client_io,
        );
        let (_server, client) = tokio::join!(server, client);
        client.map(|_| ())
    }

    fn status_request() -> AgentBootstrapStatusRequest {
        AgentBootstrapStatusRequest {
            protocol_version: PROTOCOL_VERSION_V1,
            tenant_id: TenantId::new("tenant-a").unwrap(),
            bootstrap_request_id: RequestId::new("bootstrap-a").unwrap(),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            signed_at_unix_ms: UnixMillis::new(1),
            proof: AgentBootstrapProof {
                algorithm: AgentSignatureAlgorithm::Ed25519,
                public_key_spki: Ed25519PublicKeySpki::from_public_key_bytes([7; 32]),
                signature: Ed25519Signature::from_bytes([0; 64]),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        }
    }

    async fn read_http_request<S: AsyncRead + Unpin>(stream: &mut S) -> Option<Vec<u8>> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = std::str::from_utf8(&request[..header_end]).ok()?;
            let content_length = headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })?;
            if request.len() >= header_end + 4 + content_length {
                return Some(request);
            }
        }
    }

    async fn enrollment_status_over_tls(
        uri: Option<&str>,
        deferred_identity: bool,
    ) -> (
        Result<Option<AgentBootstrapStatusResponse>, crate::client::EnrollmentClientError>,
        bool,
    ) {
        use tokio_rustls::TlsAcceptor;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let fixture = gateway_tls_fixture(uri, "127.0.0.1", vec![b"http/1.1".to_vec()]);
        let trust_bundle = fixture.trust_bundle;
        let server_config = fixture.server_config;
        let server = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return false;
            };
            let Ok(mut stream) = TlsAcceptor::from(server_config).accept(stream).await else {
                return false;
            };
            // Bootstrap/status is server-auth only. A successful request must not depend on
            // an Agent workload certificate being available before Central approval.
            if stream
                .get_ref()
                .1
                .peer_certificates()
                .is_some_and(|certificates| !certificates.is_empty())
            {
                return false;
            }
            let Some(request) = read_http_request(&mut stream).await else {
                return false;
            };
            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                return false;
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            if !headers.starts_with("POST /agent/enrollment/status/query HTTP/1.1\r\n") {
                return false;
            }
            let request_id = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("x-request-id")
                        .then(|| value.trim().to_owned())
                })
                .unwrap();
            let status_request =
                AgentBootstrapStatusRequest::decode_json(&request[header_end + 4..]).unwrap();
            let response_body = serde_json::to_vec(&AgentBootstrapStatusResponse {
                protocol_version: PROTOCOL_VERSION_V1,
                bootstrap_request_id: status_request.bootstrap_request_id,
                installation_id: status_request.installation_id,
                state: AgentBootstrapStatusState::Pending,
                enrollment_id: AgentEnrollmentId::new("enrollment-a").unwrap(),
                agent_id: None,
                resource_version: neoengram_protocol::ResourceVersion::new(1),
                updated_at_unix_ms: UnixMillis::new(1),
                certificate: None,
                extensions: Extensions::new(),
            })
            .unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-request-id: {request_id}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                response_body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&response_body).await.unwrap();
            stream.shutdown().await.unwrap();
            true
        });

        let client = if deferred_identity {
            crate::client::ReqwestEnrollmentClient::with_gateway_trust_bundle_deferred_identity(
                url::Url::parse(&format!("https://{address}/")).unwrap(),
                &trust_bundle,
                gateway_identity(),
            )
        } else {
            crate::client::ReqwestEnrollmentClient::with_gateway_trust_bundle_and_identity(
                url::Url::parse(&format!("https://{address}/")).unwrap(),
                &trust_bundle,
                gateway_identity(),
            )
        }
        .unwrap();
        let result = client.status(&status_request()).await;
        let observed = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("TLS enrollment server must finish")
            .expect("TLS enrollment server task must not panic");
        (result, observed)
    }

    const TEST_CA: &[u8] = br#"-----BEGIN CERTIFICATE-----
MIIBtjCCAVugAwIBAgITBmyf1XSXNmY/Owua2eiedgPySjAKBggqhkjOPQQDAjA5
MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6b24g
Um9vdCBDQSAzMB4XDTE1MDUyNjAwMDAwMFoXDTQwMDUyNjAwMDAwMFowOTELMAkG
A1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJvb3Qg
Q0EgMzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABCmXp8ZBf8ANm+gBG1bG8lKl
ui2yEujSLtf6ycXYqm0fc4E7O5hrOXwzpcVOho6AF2hiRVd9RFgdszflZwjrZt6j
QjBAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgGGMB0GA1UdDgQWBBSr
ttvXBp43rDCGB5Fwx5zEGbF4wDAKBggqhkjOPQQDAgNJADBGAiEA4IWSoxe3jfkr
BqWTrBqYaGFy+uGh0PsceGCmQ5nFuMQCIQCcAu/xlJyzlvnrxir4tiz+OpAUFteM
YyRIHN8wfdVoOw==
-----END CERTIFICATE-----
"#;

    #[test]
    fn gateway_server_identity_accepts_only_the_configured_cluster_scope() {
        let certificate = workload_certificate(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/gateway-pools/pool-a/gateway-replicas/replica-a",
            true,
        );
        assert!(validate_gateway_server_certificate(
            Some(std::slice::from_ref(&certificate)),
            &gateway_identity(),
        )
        .is_ok());

        let deadline =
            gateway_server_certificate_deadline(Some(std::slice::from_ref(&certificate)))
                .expect("a freshly generated Gateway leaf has a future deadline");
        assert!(deadline > Instant::now());

        let mut wrong_domain = gateway_identity();
        wrong_domain.trust_domain = "other.example.test".to_owned();
        assert!(validate_gateway_server_certificate(
            Some(std::slice::from_ref(&certificate)),
            &wrong_domain,
        )
        .is_err());

        let mut wrong_cluster = gateway_identity();
        wrong_cluster.edge_cluster_id = "edge-b".to_owned();
        assert!(validate_gateway_server_certificate(
            Some(std::slice::from_ref(&certificate)),
            &wrong_cluster,
        )
        .is_err());
    }

    #[test]
    fn gateway_server_certificate_deadline_fails_closed_at_not_after() {
        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock")
            .as_millis() as u64;
        let now = Instant::now();
        let not_after_seconds = (now_unix_ms / 1_000) as i64 + 5;
        let deadline = certificate_deadline_from_not_after(not_after_seconds, now_unix_ms, now)
            .expect("future certificate deadline");
        assert!(deadline > now);
        assert!(certificate_deadline_from_not_after(
            not_after_seconds,
            (not_after_seconds as u64) * 1_000,
            now,
        )
        .is_err());
    }

    #[test]
    fn gateway_server_identity_rejects_other_workload_kinds_and_client_only_eku() {
        for uri in [
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/agents/agent-a",
            "spiffe://mesh.example.test/workloads/central",
        ] {
            let certificate = workload_certificate(uri, true);
            assert!(validate_gateway_server_certificate(
                Some(std::slice::from_ref(&certificate)),
                &gateway_identity(),
            )
            .is_err());
        }

        let client_only = workload_certificate(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/gateway-pools/pool-a/gateway-replicas/replica-a",
            false,
        );
        assert!(validate_gateway_server_certificate(
            Some(std::slice::from_ref(&client_only)),
            &gateway_identity(),
        )
        .is_err());
    }

    #[tokio::test]
    async fn actual_tls_handshake_binds_gateway_identity_without_a_probe_connection() {
        assert!(gateway_tls_handshake(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/gateway-pools/pool-a/gateway-replicas/replica-a"
        )
        .await
        .is_ok());
        assert!(gateway_tls_handshake(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-b/gateway-pools/pool-b/gateway-replicas/replica-b"
        )
        .await
        .is_err());
        assert!(gateway_tls_handshake(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/agents/agent-a"
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn enrollment_accepts_activated_gateway_leaf_without_agent_client_certificate() {
        let (result, request_reached_server) = enrollment_status_over_tls(Some(
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/gateway-pools/pool-a/gateway-replicas/replica-a",
        ), false)
        .await;
        let status = result
            .expect("valid Gateway workload URI and endpoint SAN must complete enrollment TLS")
            .expect("test enrollment server returns a status response");
        assert_eq!(status.state, AgentBootstrapStatusState::Pending);
        assert!(
            request_reached_server,
            "the HTTPS enrollment request must reach Gateway"
        );
    }

    #[tokio::test]
    async fn enrollment_rejects_dns_only_activation_certificate_for_immediate_identity() {
        let (result, request_reached_server) = enrollment_status_over_tls(None, false).await;
        assert!(
            result.is_err(),
            "immediate workload identity validation must reject a DNS-only certificate"
        );
        assert!(!request_reached_server);
    }

    #[tokio::test]
    async fn enrollment_bootstrap_accepts_dns_only_certificate_with_deferred_identity() {
        let (result, request_reached_server) = enrollment_status_over_tls(None, true).await;
        let status = result
            .expect("bootstrap server-auth certificate must complete enrollment TLS")
            .expect("test enrollment server returns a status response");
        assert_eq!(status.state, AgentBootstrapStatusState::Pending);
        assert!(
            request_reached_server,
            "the HTTPS bootstrap request must reach an activated or unactivated Gateway"
        );
    }

    #[tokio::test]
    async fn enrollment_rejects_wrong_cluster_agent_and_central_uri_sans() {
        for uri in [
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-b/gateway-pools/pool-a/gateway-replicas/replica-a",
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-a/agents/agent-a",
            "spiffe://mesh.example.test/workloads/central",
        ] {
            let (result, request_reached_server) = enrollment_status_over_tls(Some(uri), false).await;
            assert!(result.is_err(), "unexpectedly accepted URI SAN: {uri}");
            assert!(
                !request_reached_server,
                "URI SAN {uri} must be rejected before an enrollment HTTP request"
            );
        }
    }

    #[test]
    fn loads_a_bounded_regular_pem_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("gateway-ca.pem");
        fs::write(&path, TEST_CA).unwrap();
        let bundle = GatewayTrustBundle::load(&path).unwrap();
        assert_eq!(bundle.certificates().len(), 1);
    }

    #[test]
    fn rejects_relative_empty_and_private_key_files() {
        assert!(GatewayTrustBundle::load(Path::new("gateway-ca.pem")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("gateway-ca.pem");
        fs::write(&path, []).unwrap();
        assert!(GatewayTrustBundle::load(&path).is_err());
        fs::write(
            &path,
            b"-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        assert!(GatewayTrustBundle::load(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_group_or_world_writable_files() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("gateway-ca.pem");
        fs::write(&path, TEST_CA).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(GatewayTrustBundle::load(&path).is_err());
    }
}
