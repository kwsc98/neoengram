use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Args;
use neoengram_domain::protocol::TRANSFER_ALPN;
use rustls::{client::ClientConfig, server::WebPkiClientVerifier, RootCertStore, ServerConfig};
use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer, X509Certificate},
};

const MAX_CERTIFICATE_CHAIN_BYTES: u64 = 1024 * 1024;
const MAX_PRIVATE_KEY_BYTES: u64 = 64 * 1024;
const MAX_CLIENT_CA_BYTES: u64 = 1024 * 1024;

/// Listener transport settings shared by the Agent, Central-control, and peer endpoints.
///
/// The Agent listener offers optional client authentication so an unissued Agent can reach only
/// bootstrap/status at the application layer. Central-control and peer listeners require a client
/// certificate whenever a workload CA is configured.
#[derive(Debug, Clone, Default, Args)]
pub(crate) struct GatewayTransportConfig {
    /// PEM certificate chain presented by all Gateway listeners.
    #[arg(long, env = "SYNAPSE_GATEWAY_TLS_CERTIFICATE_FILE")]
    pub(crate) tls_certificate_file: Option<PathBuf>,

    /// PEM private key matching the Gateway listener certificate.
    #[arg(
        long,
        env = "SYNAPSE_GATEWAY_TLS_PRIVATE_KEY_FILE",
        hide_env_values = true
    )]
    pub(crate) tls_private_key_file: Option<PathBuf>,

    /// PEM CA bundle used to authenticate Central, Agent, and peer workload certificates.
    #[arg(long, env = "SYNAPSE_GATEWAY_TLS_CLIENT_CA_FILE")]
    pub(crate) tls_client_ca_file: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GatewayTransportConfigError {
    #[error("Gateway TLS certificate and private key files must be configured together")]
    IncompleteTlsIdentity,
    #[error("plaintext Gateway listeners are permitted only when every listener is loopback")]
    PlaintextExposed,
    #[error("exposed Gateway listeners require a workload client CA for mTLS")]
    MissingClientCa,
    #[error("Gateway workload client CA requires a configured TLS identity")]
    ClientCaWithoutIdentity,
    #[error("Gateway TLS {kind} file is invalid: {message}")]
    InvalidFile { kind: &'static str, message: String },
    #[error("Gateway TLS certificate file contains no PEM certificates")]
    EmptyCertificateChain,
    #[error("Gateway workload client CA file contains no PEM certificates")]
    EmptyClientCa,
    #[error("Gateway TLS private key file contains no supported PEM private key")]
    MissingPrivateKey,
    #[error("Gateway TLS identity is invalid: {0}")]
    InvalidTlsIdentity(String),
}

impl GatewayTransportConfig {
    pub(crate) fn client_ca_file(&self) -> Option<&Path> {
        self.tls_client_ca_file.as_deref()
    }

    /// Validates the identity carried by the local Gateway listener leaf before any listener is
    /// bound. A trusted CA alone is insufficient: every Replica must present exactly its
    /// configured SPIFFE URI and be usable for both sides of the mTLS peer connection.
    pub(crate) fn validate_local_server_identity(
        &self,
        edge_cluster_id: &str,
        gateway_pool_id: &str,
        gateway_replica_id: &str,
        workload_trust_domain: Option<&str>,
    ) -> Result<(), GatewayTransportConfigError> {
        let Some(certificate_path) = &self.tls_certificate_file else {
            return Ok(());
        };
        let trust_domain = workload_trust_domain.ok_or_else(|| {
            GatewayTransportConfigError::InvalidTlsIdentity(
                "workload trust domain is required to validate the local Gateway certificate"
                    .to_owned(),
            )
        })?;
        let certificate_pem =
            read_bounded_file(certificate_path, "certificate", MAX_CERTIFICATE_CHAIN_BYTES)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| GatewayTransportConfigError::InvalidFile {
                kind: "certificate",
                message: error.to_string(),
            })?;
        let leaf = certificates
            .first()
            .ok_or(GatewayTransportConfigError::EmptyCertificateChain)?;
        let (remainder, certificate) =
            X509Certificate::from_der(leaf.as_ref()).map_err(|error| {
                GatewayTransportConfigError::InvalidTlsIdentity(format!(
                    "local Gateway certificate is invalid DER: {error}"
                ))
            })?;
        if !remainder.is_empty() {
            return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                "local Gateway certificate contains trailing DER bytes".to_owned(),
            ));
        }
        let eku = certificate
            .extended_key_usage()
            .map_err(|error| {
                GatewayTransportConfigError::InvalidTlsIdentity(format!(
                    "local Gateway certificate EKU is invalid: {error}"
                ))
            })?
            .ok_or_else(|| {
                GatewayTransportConfigError::InvalidTlsIdentity(
                    "local Gateway certificate has no extended key usage".to_owned(),
                )
            })?;
        if !eku.value.server_auth || !eku.value.client_auth {
            return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                "local Gateway certificate must permit both server and client authentication"
                    .to_owned(),
            ));
        }
        let san = certificate
            .subject_alternative_name()
            .map_err(|error| {
                GatewayTransportConfigError::InvalidTlsIdentity(format!(
                    "local Gateway certificate SAN is invalid: {error}"
                ))
            })?
            .ok_or_else(|| {
                GatewayTransportConfigError::InvalidTlsIdentity(
                    "local Gateway certificate has no subjectAltName".to_owned(),
                )
            })?;
        let uris = san
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                GeneralName::URI(uri) => Some(*uri),
                _ => None,
            })
            .collect::<Vec<_>>();
        let expected_uri = format!(
            "spiffe://{trust_domain}/workloads/edge-clusters/{edge_cluster_id}/gateway-pools/{gateway_pool_id}/gateway-replicas/{gateway_replica_id}"
        );
        if uris.as_slice() != [expected_uri.as_str()] {
            return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                "local Gateway certificate URI SAN does not match the configured Replica identity"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Validates the short-lived server-auth certificate used before Replica activation. The
    /// bootstrap leaf intentionally has no workload URI identity and does not need clientAuth;
    /// it only needs at least one DNS/IP SAN so Central can authenticate the registered endpoint.
    pub(crate) fn validate_local_bootstrap_server_identity(
        &self,
    ) -> Result<(), GatewayTransportConfigError> {
        let Some(certificate_path) = &self.tls_certificate_file else {
            return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                "a bootstrap Gateway requires a server certificate".to_owned(),
            ));
        };
        let certificate_pem =
            read_bounded_file(certificate_path, "certificate", MAX_CERTIFICATE_CHAIN_BYTES)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| GatewayTransportConfigError::InvalidFile {
                kind: "certificate",
                message: error.to_string(),
            })?;
        let leaf = certificates
            .first()
            .ok_or(GatewayTransportConfigError::EmptyCertificateChain)?;
        let (remainder, certificate) =
            X509Certificate::from_der(leaf.as_ref()).map_err(|error| {
                GatewayTransportConfigError::InvalidTlsIdentity(format!(
                    "bootstrap Gateway certificate is invalid DER: {error}"
                ))
            })?;
        if !remainder.is_empty() {
            return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                "bootstrap Gateway certificate contains trailing DER bytes".to_owned(),
            ));
        }
        let eku = certificate
            .extended_key_usage()
            .map_err(|error| {
                GatewayTransportConfigError::InvalidTlsIdentity(format!(
                    "bootstrap Gateway certificate EKU is invalid: {error}"
                ))
            })?
            .ok_or_else(|| {
                GatewayTransportConfigError::InvalidTlsIdentity(
                    "bootstrap Gateway certificate has no extended key usage".to_owned(),
                )
            })?;
        if !eku.value.server_auth {
            return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                "bootstrap Gateway certificate must permit server authentication".to_owned(),
            ));
        }
        let san = certificate
            .subject_alternative_name()
            .map_err(|error| {
                GatewayTransportConfigError::InvalidTlsIdentity(format!(
                    "bootstrap Gateway certificate SAN is invalid: {error}"
                ))
            })?
            .ok_or_else(|| {
                GatewayTransportConfigError::InvalidTlsIdentity(
                    "bootstrap Gateway certificate has no subjectAltName".to_owned(),
                )
            })?;
        let mut endpoint_san_count = 0_usize;
        for name in &san.value.general_names {
            match name {
                GeneralName::DNSName(value) if !value.is_empty() => endpoint_san_count += 1,
                GeneralName::IPAddress(value) if matches!(value.len(), 4 | 16) => {
                    endpoint_san_count += 1;
                }
                GeneralName::URI(_) => {
                    return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                        "bootstrap Gateway certificate must not contain a workload URI SAN"
                            .to_owned(),
                    ));
                }
                _ => {}
            }
        }
        if endpoint_san_count == 0 {
            return Err(GatewayTransportConfigError::InvalidTlsIdentity(
                "bootstrap Gateway certificate must contain a DNS or IP SAN".to_owned(),
            ));
        }
        Ok(())
    }

    /// Builds the client mTLS identity used for one-hop peer connections. Production peer
    /// forwarding is unavailable unless both a client certificate and a CA bundle are present.
    pub(crate) fn load_peer_client_config(
        &self,
    ) -> Result<Option<Arc<ClientConfig>>, GatewayTransportConfigError> {
        let (Some(certificate_path), Some(private_key_path), Some(ca_path)) = (
            &self.tls_certificate_file,
            &self.tls_private_key_file,
            &self.tls_client_ca_file,
        ) else {
            return Ok(None);
        };
        let certificate_pem =
            read_bounded_file(certificate_path, "certificate", MAX_CERTIFICATE_CHAIN_BYTES)?;
        let private_key_pem =
            read_bounded_file(private_key_path, "private key", MAX_PRIVATE_KEY_BYTES)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| GatewayTransportConfigError::InvalidFile {
                kind: "certificate",
                message: error.to_string(),
            })?;
        if certificates.is_empty() {
            return Err(GatewayTransportConfigError::EmptyCertificateChain);
        }
        let private_key = PrivateKeyDer::from_pem_slice(&private_key_pem)
            .map_err(|_| GatewayTransportConfigError::MissingPrivateKey)?;
        let roots = load_client_roots(ca_path)?;
        let provider: Arc<rustls::crypto::CryptoProvider> =
            rustls::crypto::aws_lc_rs::default_provider().into();
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| GatewayTransportConfigError::InvalidTlsIdentity(error.to_string()))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, private_key)
            .map_err(|error| GatewayTransportConfigError::InvalidTlsIdentity(error.to_string()))?;
        // Gateway peer connections carry identity-bound control frames.  A reconnect must perform
        // a complete certificate handshake so a rotated or revoked leaf cannot be hidden behind a
        // cached TLS session.
        config.resumption = rustls::client::Resumption::disabled();
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Some(Arc::new(config)))
    }

    pub(crate) fn validate_listener_exposure(
        &self,
        listeners: [SocketAddr; 3],
    ) -> Result<(), GatewayTransportConfigError> {
        let loopback_only = listeners.iter().all(|address| address.ip().is_loopback());
        match (&self.tls_certificate_file, &self.tls_private_key_file) {
            (Some(_), Some(_)) if self.tls_client_ca_file.is_some() || loopback_only => Ok(()),
            (Some(_), Some(_)) => Err(GatewayTransportConfigError::MissingClientCa),
            (None, None) if self.tls_client_ca_file.is_some() => {
                Err(GatewayTransportConfigError::ClientCaWithoutIdentity)
            }
            (None, None) if loopback_only => Ok(()),
            (None, None) => Err(GatewayTransportConfigError::PlaintextExposed),
            _ => Err(GatewayTransportConfigError::IncompleteTlsIdentity),
        }
    }

    /// Builds listener-specific TLS policies in Agent, Central-control, and peer order.
    pub(crate) fn load_server_configs(
        &self,
        listeners: [SocketAddr; 3],
    ) -> Result<[Option<Arc<ServerConfig>>; 3], GatewayTransportConfigError> {
        self.validate_listener_exposure(listeners)?;
        let (Some(certificate_path), Some(private_key_path)) =
            (&self.tls_certificate_file, &self.tls_private_key_file)
        else {
            return Ok([None, None, None]);
        };

        let certificate_pem =
            read_bounded_file(certificate_path, "certificate", MAX_CERTIFICATE_CHAIN_BYTES)?;
        let private_key_pem =
            read_bounded_file(private_key_path, "private key", MAX_PRIVATE_KEY_BYTES)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| GatewayTransportConfigError::InvalidFile {
                kind: "certificate",
                message: error.to_string(),
            })?;
        if certificates.is_empty() {
            return Err(GatewayTransportConfigError::EmptyCertificateChain);
        }
        let private_key = PrivateKeyDer::from_pem_slice(&private_key_pem)
            .map_err(|_| GatewayTransportConfigError::MissingPrivateKey)?;
        let roots = self
            .tls_client_ca_file
            .as_deref()
            .map(load_client_roots)
            .transpose()?;

        let agent = build_server_config(
            &certificates,
            &private_key,
            roots.as_ref(),
            ClientAuthentication::Optional,
        )?;
        let control = build_server_config(
            &certificates,
            &private_key,
            roots.as_ref(),
            ClientAuthentication::Required,
        )?;
        let peer = build_server_config(
            &certificates,
            &private_key,
            roots.as_ref(),
            ClientAuthentication::Required,
        )?;
        Ok([Some(agent), Some(control), Some(peer)])
    }

    /// Builds the server-only rustls policy used by the QUIC transfer listener.  QUIC has a
    /// distinct ALPN and always requires workload mTLS, even on loopback, because transfer
    /// tickets are bearer capabilities for object bytes.
    pub(crate) fn load_quic_server_config(
        &self,
    ) -> Result<Arc<ServerConfig>, GatewayTransportConfigError> {
        let (Some(certificate_path), Some(private_key_path), Some(ca_path)) = (
            &self.tls_certificate_file,
            &self.tls_private_key_file,
            &self.tls_client_ca_file,
        ) else {
            return Err(GatewayTransportConfigError::MissingClientCa);
        };
        let certificate_pem =
            read_bounded_file(certificate_path, "certificate", MAX_CERTIFICATE_CHAIN_BYTES)?;
        let private_key_pem =
            read_bounded_file(private_key_path, "private key", MAX_PRIVATE_KEY_BYTES)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| GatewayTransportConfigError::InvalidFile {
                kind: "certificate",
                message: error.to_string(),
            })?;
        if certificates.is_empty() {
            return Err(GatewayTransportConfigError::EmptyCertificateChain);
        }
        let private_key = PrivateKeyDer::from_pem_slice(&private_key_pem)
            .map_err(|_| GatewayTransportConfigError::MissingPrivateKey)?;
        let roots = load_client_roots(ca_path)?;
        let mut server = build_server_config(
            &certificates,
            &private_key,
            Some(&roots),
            ClientAuthentication::Required,
        )?;
        Arc::get_mut(&mut server)
            .expect("new QUIC TLS configuration must have one owner")
            .alpn_protocols = vec![TRANSFER_ALPN.as_bytes().to_vec()];
        Ok(server)
    }

    /// Builds the mTLS client policy for a Gateway-to-Gateway QUIC hop.  It intentionally uses a
    /// separate ALPN from the H2 peer forwarder and disables TLS resumption so a rotated workload
    /// credential is checked on every transfer connection.
    pub(crate) fn load_quic_client_config(
        &self,
    ) -> Result<Arc<ClientConfig>, GatewayTransportConfigError> {
        let (Some(certificate_path), Some(private_key_path), Some(ca_path)) = (
            &self.tls_certificate_file,
            &self.tls_private_key_file,
            &self.tls_client_ca_file,
        ) else {
            return Err(GatewayTransportConfigError::MissingClientCa);
        };
        let certificate_pem =
            read_bounded_file(certificate_path, "certificate", MAX_CERTIFICATE_CHAIN_BYTES)?;
        let private_key_pem =
            read_bounded_file(private_key_path, "private key", MAX_PRIVATE_KEY_BYTES)?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| GatewayTransportConfigError::InvalidFile {
                kind: "certificate",
                message: error.to_string(),
            })?;
        if certificates.is_empty() {
            return Err(GatewayTransportConfigError::EmptyCertificateChain);
        }
        let private_key = PrivateKeyDer::from_pem_slice(&private_key_pem)
            .map_err(|_| GatewayTransportConfigError::MissingPrivateKey)?;
        let roots = load_client_roots(ca_path)?;
        let provider: Arc<rustls::crypto::CryptoProvider> =
            rustls::crypto::aws_lc_rs::default_provider().into();
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| GatewayTransportConfigError::InvalidTlsIdentity(error.to_string()))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, private_key)
            .map_err(|error| GatewayTransportConfigError::InvalidTlsIdentity(error.to_string()))?;
        client.resumption = rustls::client::Resumption::disabled();
        client.alpn_protocols = vec![TRANSFER_ALPN.as_bytes().to_vec()];
        Ok(Arc::new(client))
    }

    /// Builds the server-only TLS policy used by the public console/S3 listener.  The public
    /// listener intentionally has no workload client-CA verifier; workload mTLS remains confined
    /// to the Agent, Central-control, and peer listeners above.
    pub(crate) fn load_public_server_config(
        certificate_path: &Path,
        private_key_path: &Path,
    ) -> Result<Arc<ServerConfig>, GatewayTransportConfigError> {
        let certificate_pem = read_bounded_file(
            certificate_path,
            "public certificate",
            MAX_CERTIFICATE_CHAIN_BYTES,
        )?;
        let private_key_pem = read_bounded_file(
            private_key_path,
            "public private key",
            MAX_PRIVATE_KEY_BYTES,
        )?;
        let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| GatewayTransportConfigError::InvalidFile {
                kind: "public certificate",
                message: error.to_string(),
            })?;
        if certificates.is_empty() {
            return Err(GatewayTransportConfigError::EmptyCertificateChain);
        }
        let private_key = PrivateKeyDer::from_pem_slice(&private_key_pem)
            .map_err(|_| GatewayTransportConfigError::MissingPrivateKey)?;
        let mut server = build_server_config(
            &certificates,
            &private_key,
            None,
            ClientAuthentication::Optional,
        )?;
        // Browser clients negotiate either HTTP/2 or HTTP/1.1 on the public endpoint.  Workload
        // listeners intentionally remain H2-only, so this ALPN policy is local to this method.
        Arc::get_mut(&mut server)
            .expect("new public TLS configuration must have one owner")
            .alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(server)
    }

    #[cfg(test)]
    pub(crate) fn load_server_config(
        &self,
        listeners: [SocketAddr; 3],
    ) -> Result<Option<Arc<ServerConfig>>, GatewayTransportConfigError> {
        self.load_server_configs(listeners)
            .map(|configs| configs[1].clone())
    }
}

#[derive(Clone, Copy)]
enum ClientAuthentication {
    Optional,
    Required,
}

fn load_client_roots(path: &Path) -> Result<RootCertStore, GatewayTransportConfigError> {
    let bytes = read_bounded_file(path, "workload client CA", MAX_CLIENT_CA_BYTES)?;
    let certificates = CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GatewayTransportConfigError::InvalidFile {
            kind: "workload client CA",
            message: error.to_string(),
        })?;
    if certificates.is_empty() {
        return Err(GatewayTransportConfigError::EmptyClientCa);
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate).map_err(|error| {
            GatewayTransportConfigError::InvalidTlsIdentity(format!(
                "workload client CA is invalid: {error}"
            ))
        })?;
    }
    Ok(roots)
}

fn build_server_config(
    certificates: &[CertificateDer<'static>],
    private_key: &PrivateKeyDer<'static>,
    roots: Option<&RootCertStore>,
    client_authentication: ClientAuthentication,
) -> Result<Arc<ServerConfig>, GatewayTransportConfigError> {
    let provider: Arc<rustls::crypto::CryptoProvider> =
        rustls::crypto::aws_lc_rs::default_provider().into();
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| GatewayTransportConfigError::InvalidTlsIdentity(error.to_string()))?;
    let mut server = if let Some(roots) = roots {
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots.clone()), provider);
        let verifier = match client_authentication {
            ClientAuthentication::Optional => verifier.allow_unauthenticated(),
            ClientAuthentication::Required => verifier,
        }
        .build()
        .map_err(|error| GatewayTransportConfigError::InvalidTlsIdentity(error.to_string()))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates.to_vec(), private_key.clone_key())
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates.to_vec(), private_key.clone_key())
    }
    .map_err(|error| GatewayTransportConfigError::InvalidTlsIdentity(error.to_string()))?;
    // Never resume a Gateway workload session.  This keeps certificate validity and credential
    // generation checks on the reconnect path instead of allowing cached session state to bypass
    // them.  `send_tls13_tickets = 0` also disables TLS 1.3 ticket issuance explicitly.
    server.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    server.send_tls13_tickets = 0;
    server.max_tls13_tickets = 0;
    server.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(server))
}

fn read_bounded_file(
    path: &Path,
    kind: &'static str,
    max_bytes: u64,
) -> Result<Vec<u8>, GatewayTransportConfigError> {
    let metadata =
        fs::metadata(path).map_err(|error| GatewayTransportConfigError::InvalidFile {
            kind,
            message: error.to_string(),
        })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(GatewayTransportConfigError::InvalidFile {
            kind,
            message: format!("must be a regular file containing 1..={max_bytes} bytes"),
        });
    }
    let bytes = fs::read(path).map_err(|error| GatewayTransportConfigError::InvalidFile {
        kind,
        message: error.to_string(),
    })?;
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(GatewayTransportConfigError::InvalidFile {
            kind,
            message: format!("must contain 1..={max_bytes} bytes"),
        });
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, SanType};

    const TEST_CERTIFICATE: &str = r#"-----BEGIN CERTIFICATE-----
MIIBWTCCAQugAwIBAgIUCS5+f6+Hv76VzMzaORcbF+6VErEwBQYDK2VwMBQxEjAQ
BgNVBAMMCWxvY2FsaG9zdDAeFw0yNjA4MDkwNTMxMTRaFw0zNjA4MDYwNTMxMTRa
MBQxEjAQBgNVBAMMCWxvY2FsaG9zdDAqMAUGAytlcAMhAA9ILZSNk98g8XvFv5Tm
Qk2xzs5BQmEqxpWbUhuAM1u0o28wbTAdBgNVHQ4EFgQUKU0zizsmKmEhTlTWEB5H
u8co1yMwHwYDVR0jBBgwFoAUKU0zizsmKmEhTlTWEB5Hu8co1yMwDwYDVR0TAQH/
BAUwAwEB/zAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwBQYDK2VwA0EAGZQa
gmMgv9bjqGBfnnwIJkLdTQoYq1bsyeSigZ1srzvbNIXIZBbmXcTllt82aSOBuV4K
5MxCub3Px0qOuLgBBA==
-----END CERTIFICATE-----
"#;
    const TEST_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEINQawrTMCmjrnfruh9FAsmFhzfyw4nNF+73pdTtdaJ46
-----END PRIVATE KEY-----
"#;

    fn loopback_listeners() -> [SocketAddr; 3] {
        [
            "127.0.0.1:8081".parse().unwrap(),
            "127.0.0.1:8082".parse().unwrap(),
            "127.0.0.1:8083".parse().unwrap(),
        ]
    }

    #[test]
    fn plaintext_is_loopback_only_and_identity_files_are_paired() {
        let config = GatewayTransportConfig::default();
        assert!(config
            .validate_listener_exposure(loopback_listeners())
            .is_ok());
        assert!(matches!(
            config.validate_listener_exposure([
                "0.0.0.0:8081".parse().unwrap(),
                "0.0.0.0:8082".parse().unwrap(),
                "0.0.0.0:8083".parse().unwrap(),
            ]),
            Err(GatewayTransportConfigError::PlaintextExposed)
        ));

        let incomplete = GatewayTransportConfig {
            tls_certificate_file: Some(PathBuf::from("/certificate.pem")),
            tls_private_key_file: None,
            tls_client_ca_file: None,
        };
        assert!(matches!(
            incomplete.validate_listener_exposure(loopback_listeners()),
            Err(GatewayTransportConfigError::IncompleteTlsIdentity)
        ));
    }

    #[test]
    fn pem_identity_builds_an_h2_only_server_config() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        fs::write(&certificate, TEST_CERTIFICATE).unwrap();
        fs::write(&private_key, TEST_PRIVATE_KEY).unwrap();
        let config = GatewayTransportConfig {
            tls_certificate_file: Some(certificate),
            tls_private_key_file: Some(private_key),
            tls_client_ca_file: None,
        };

        let server = config
            .load_server_config(loopback_listeners())
            .unwrap()
            .unwrap();
        assert_eq!(server.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn quic_identity_requires_client_ca_and_uses_transfer_alpn() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let client_ca = directory.path().join("client-ca.pem");
        fs::write(&certificate, TEST_CERTIFICATE).unwrap();
        fs::write(&private_key, TEST_PRIVATE_KEY).unwrap();
        let config_without_ca = GatewayTransportConfig {
            tls_certificate_file: Some(certificate.clone()),
            tls_private_key_file: Some(private_key.clone()),
            tls_client_ca_file: None,
        };
        assert!(matches!(
            config_without_ca.load_quic_server_config(),
            Err(GatewayTransportConfigError::MissingClientCa)
        ));

        fs::write(&client_ca, TEST_CERTIFICATE).unwrap();
        let config = GatewayTransportConfig {
            tls_certificate_file: Some(certificate),
            tls_private_key_file: Some(private_key),
            tls_client_ca_file: Some(client_ca),
        };
        let server = config.load_quic_server_config().unwrap();
        assert_eq!(
            server.alpn_protocols,
            vec![TRANSFER_ALPN.as_bytes().to_vec()]
        );
        assert!(!server.session_storage.can_cache());
        let client = config.load_quic_client_config().unwrap();
        assert_eq!(
            client.alpn_protocols,
            vec![TRANSFER_ALPN.as_bytes().to_vec()]
        );
        assert!(format!("{:?}", client.resumption).contains("Disabled"));
    }

    #[test]
    fn public_identity_supports_browser_http_versions_without_client_auth() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        fs::write(&certificate, TEST_CERTIFICATE).unwrap();
        fs::write(&private_key, TEST_PRIVATE_KEY).unwrap();

        let server =
            GatewayTransportConfig::load_public_server_config(&certificate, &private_key).unwrap();
        assert_eq!(
            server.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
        assert!(!server.session_storage.can_cache());
    }

    #[test]
    fn malformed_pem_fails_before_listener_startup() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        fs::write(&certificate, "not a certificate").unwrap();
        fs::write(&private_key, TEST_PRIVATE_KEY).unwrap();
        let config = GatewayTransportConfig {
            tls_certificate_file: Some(certificate),
            tls_private_key_file: Some(private_key),
            tls_client_ca_file: None,
        };

        assert!(matches!(
            config.load_server_config(loopback_listeners()),
            Err(GatewayTransportConfigError::InvalidFile { .. })
                | Err(GatewayTransportConfigError::EmptyCertificateChain)
        ));
    }

    #[test]
    fn exposed_tls_requires_a_client_ca_and_builds_mtls_policies() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let client_ca = directory.path().join("client-ca.pem");
        fs::write(&certificate, TEST_CERTIFICATE).unwrap();
        fs::write(&private_key, TEST_PRIVATE_KEY).unwrap();
        fs::write(&client_ca, TEST_CERTIFICATE).unwrap();
        let listeners = [
            "0.0.0.0:8081".parse().unwrap(),
            "0.0.0.0:8082".parse().unwrap(),
            "0.0.0.0:8083".parse().unwrap(),
        ];
        let mut config = GatewayTransportConfig {
            tls_certificate_file: Some(certificate),
            tls_private_key_file: Some(private_key),
            tls_client_ca_file: None,
        };
        assert!(matches!(
            config.load_server_configs(listeners),
            Err(GatewayTransportConfigError::MissingClientCa)
        ));
        config.tls_client_ca_file = Some(client_ca);
        let configs = config.load_server_configs(listeners).unwrap();
        assert!(configs.iter().all(Option::is_some));
        let peer_client = config.load_peer_client_config().unwrap().unwrap();
        assert_eq!(peer_client.alpn_protocols, vec![b"h2".to_vec()]);
        assert!(format!("{:?}", peer_client.resumption).contains("Disabled"));
        for server in configs.into_iter().flatten() {
            assert!(!server.session_storage.can_cache());
            assert_eq!(server.send_tls13_tickets, 0);
            assert_eq!(server.max_tls13_tickets, 0);
        }
    }

    #[test]
    fn local_gateway_leaf_must_match_replica_uri_and_support_both_tls_roles() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let identity_uri = "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a";
        let mut parameters = CertificateParams::new(Vec::<String>::new()).unwrap();
        parameters
            .subject_alt_names
            .push(SanType::URI(identity_uri.try_into().unwrap()));
        parameters.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().unwrap();
        let certificate_value = parameters.self_signed(&key).unwrap();
        fs::write(&certificate, certificate_value.pem()).unwrap();
        fs::write(&private_key, TEST_PRIVATE_KEY).unwrap();
        let config = GatewayTransportConfig {
            tls_certificate_file: Some(certificate.clone()),
            tls_private_key_file: Some(private_key),
            tls_client_ca_file: None,
        };

        config
            .validate_local_server_identity(
                "cluster-a",
                "pool-a",
                "replica-a",
                Some("mesh.example.test"),
            )
            .unwrap();
        assert!(config
            .validate_local_server_identity(
                "cluster-a",
                "pool-a",
                "replica-b",
                Some("mesh.example.test"),
            )
            .is_err());

        let mut server_only = CertificateParams::new(Vec::<String>::new()).unwrap();
        server_only
            .subject_alt_names
            .push(SanType::URI(identity_uri.try_into().unwrap()));
        server_only.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_only_key = KeyPair::generate().unwrap();
        fs::write(
            &certificate,
            server_only.self_signed(&server_only_key).unwrap().pem(),
        )
        .unwrap();
        assert!(config
            .validate_local_server_identity(
                "cluster-a",
                "pool-a",
                "replica-a",
                Some("mesh.example.test"),
            )
            .is_err());
    }

    #[test]
    fn bootstrap_leaf_requires_server_auth_and_endpoint_san_but_not_workload_identity() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let mut parameters =
            CertificateParams::new(vec!["gateway.example.test".to_owned()]).unwrap();
        parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate().unwrap();
        fs::write(&certificate, parameters.self_signed(&key).unwrap().pem()).unwrap();
        fs::write(&private_key, key.serialize_pem()).unwrap();
        let config = GatewayTransportConfig {
            tls_certificate_file: Some(certificate.clone()),
            tls_private_key_file: Some(private_key),
            tls_client_ca_file: None,
        };
        config.validate_local_bootstrap_server_identity().unwrap();

        let mut workload = CertificateParams::new(Vec::<String>::new()).unwrap();
        workload.subject_alt_names.push(SanType::URI(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a"
                .try_into()
                .unwrap(),
        ));
        workload.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let workload_key = KeyPair::generate().unwrap();
        fs::write(
            &certificate,
            workload.self_signed(&workload_key).unwrap().pem(),
        )
        .unwrap();
        assert!(matches!(
            config.validate_local_bootstrap_server_identity(),
            Err(GatewayTransportConfigError::InvalidTlsIdentity(_))
        ));
    }
}
