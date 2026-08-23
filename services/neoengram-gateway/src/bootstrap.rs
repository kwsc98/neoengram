use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use clap::Args;
use neoengram_domain::protocol::{
    decode_bounded_unique_json, AgentBootstrapProof, ContentDigest, Ed25519PublicKeySpki,
    Ed25519Signature, GatewayBootstrapCertificateDelivery, GatewayBootstrapChallenge,
    GatewayBootstrapChallengeRequest, GatewayBootstrapProofResponse, UnixMillis,
    CURRENT_WIRE_VERSION, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES,
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use rustls::{
    server::{danger::ClientCertVerifier, WebPkiClientVerifier},
    RootCertStore,
};
use rustls_pki_types::{pem::PemObject, PrivatePkcs8KeyDer};
use tokio::sync::Mutex;
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer, X509Certificate},
};

use crate::tunnel::GatewayIdentity;

const MAX_BOOTSTRAP_PRIVATE_KEY_BYTES: u64 = 64 * 1024;
const MAX_BOOTSTRAP_TOKEN_BYTES: u64 = 1024;
const MAX_BOOTSTRAP_CERTIFICATE_BYTES: u64 = 1024 * 1024;
const ACTIVATION_TOKEN_PREFIX: &str = "nggw_v1_";

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct GatewayBootstrapConfig {
    /// PEM PKCS#8 Ed25519 key used only for Replica activation proof-of-possession.
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_BOOTSTRAP_PRIVATE_KEY_FILE",
        hide_env_values = true
    )]
    pub(crate) private_key_file: Option<PathBuf>,

    /// File containing the one-time activation token returned by Central.
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_BOOTSTRAP_ACTIVATION_TOKEN_FILE",
        hide_env_values = true
    )]
    pub(crate) activation_token_file: Option<PathBuf>,

    /// Destination for the atomically installed leaf and issuer PEM chain.
    #[arg(long, env = "NEOENGRAM_GATEWAY_BOOTSTRAP_CERTIFICATE_CHAIN_FILE")]
    pub(crate) certificate_chain_file: Option<PathBuf>,
}

impl GatewayBootstrapConfig {
    pub(crate) fn validate(&self) -> Result<(), BootstrapError> {
        let bootstrap_configured = [
            self.private_key_file.is_some(),
            self.activation_token_file.is_some(),
            self.certificate_chain_file.is_some(),
        ];
        // The trust domain is a long-lived workload identity setting. The three bootstrap
        // material paths are a separate, short-lived activation capability and may be removed
        // after the provisioner promotes the delivered workload certificate.
        if bootstrap_configured.iter().any(|configured| *configured)
            && !bootstrap_configured.iter().all(|configured| *configured)
        {
            return Err(BootstrapError::Configuration(
                "bootstrap private key, activation token, and certificate chain path must be configured together"
                    .into(),
            ));
        }
        if let (Some(private_key), Some(token), Some(certificate)) = (
            &self.private_key_file,
            &self.activation_token_file,
            &self.certificate_chain_file,
        ) {
            if private_key == token || private_key == certificate || token == certificate {
                return Err(BootstrapError::Configuration(
                    "bootstrap private key, token, and certificate paths must be distinct".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn load(
        &self,
        identity: GatewayIdentity,
        workload_trust_domain: Option<&str>,
        active_certificate_file: Option<&Path>,
        client_ca_file: Option<&Path>,
    ) -> Result<Option<Arc<GatewayBootstrap>>, BootstrapError> {
        self.validate()?;
        let (Some(private_key_path), Some(token_path), Some(certificate_path)) = (
            self.private_key_file.as_deref(),
            self.activation_token_file.as_deref(),
            self.certificate_chain_file.clone(),
        ) else {
            return Ok(None);
        };
        let trust_domain = workload_trust_domain.ok_or_else(|| {
            BootstrapError::Configuration(
                "workload trust domain is required while Gateway bootstrap is enabled".into(),
            )
        })?;
        require_restricted_file(private_key_path, "bootstrap private key")?;
        require_restricted_file(token_path, "bootstrap activation token")?;
        let private_key_pem = read_bounded(
            private_key_path,
            "bootstrap private key",
            MAX_BOOTSTRAP_PRIVATE_KEY_BYTES,
        )?;
        let private_key = PrivatePkcs8KeyDer::from_pem_slice(&private_key_pem).map_err(|_| {
            BootstrapError::Configuration(
                "bootstrap private key must be a PEM PKCS#8 Ed25519 key".into(),
            )
        })?;
        let key_pair = Ed25519KeyPair::from_pkcs8_maybe_unchecked(private_key.secret_pkcs8_der())
            .map_err(|_| {
            BootstrapError::Configuration("bootstrap private key is not valid Ed25519".into())
        })?;
        let token_bytes = read_bounded(
            token_path,
            "bootstrap activation token",
            MAX_BOOTSTRAP_TOKEN_BYTES,
        )?;
        let token = std::str::from_utf8(&token_bytes)
            .map_err(|_| {
                BootstrapError::Configuration("bootstrap activation token is not UTF-8".into())
            })?
            .trim_end_matches(['\r', '\n']);
        validate_activation_token(token)?;
        let public_key_spki = Ed25519PublicKeySpki::from_public_key_bytes(
            key_pair
                .public_key()
                .as_ref()
                .try_into()
                .map_err(|_| BootstrapError::Key)?,
        );
        let expected_identity_uri = format!(
            "spiffe://{trust_domain}/workloads/edge-clusters/{}/gateway-pools/{}/gateway-replicas/{}",
            identity.edge_cluster_id, identity.gateway_pool_id, identity.gateway_replica_id
        );
        let certificate_installed = certificate_path.exists()
            && active_certificate_file != Some(certificate_path.as_path());
        let certificate_verifier = client_ca_file.map(load_certificate_verifier).transpose()?;
        Ok(Some(Arc::new(GatewayBootstrap {
            identity,
            key_pair: Arc::new(key_pair),
            public_key_spki,
            expected_identity_uri,
            activation_token_digest: ContentDigest::hash(token.as_bytes()),
            certificate_path,
            certificate_verifier,
            state: Mutex::new(BootstrapState {
                pending: None,
                certificate_installed,
                installed_request_id: None,
                installed_certificate_digest: None,
            }),
        })))
    }
}

pub(crate) struct GatewayBootstrap {
    identity: GatewayIdentity,
    key_pair: Arc<Ed25519KeyPair>,
    public_key_spki: Ed25519PublicKeySpki,
    expected_identity_uri: String,
    activation_token_digest: ContentDigest,
    certificate_path: PathBuf,
    certificate_verifier: Option<Arc<dyn ClientCertVerifier>>,
    state: Mutex<BootstrapState>,
}

struct BootstrapState {
    pending: Option<GatewayBootstrapChallenge>,
    certificate_installed: bool,
    installed_request_id: Option<neoengram_domain::protocol::RequestId>,
    installed_certificate_digest: Option<ContentDigest>,
}

impl GatewayBootstrap {
    pub(crate) async fn restart_required(&self) -> bool {
        self.state.lock().await.certificate_installed
    }

    pub(crate) async fn prove(
        &self,
        body: &[u8],
    ) -> Result<GatewayBootstrapProofResponse, BootstrapError> {
        let request: GatewayBootstrapChallengeRequest =
            decode_bounded_unique_json(body, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES)
                .map_err(|error| BootstrapError::Invalid(error.to_string()))?;
        request
            .validate()
            .map_err(|error| BootstrapError::Invalid(error.to_string()))?;
        let now = unix_millis()?;
        if now.get() < request.challenge.issued_at_unix_ms.get()
            || now.get() >= request.challenge.expires_at_unix_ms.get()
        {
            return Err(BootstrapError::Expired);
        }
        if request.challenge.edge_cluster_id != self.identity.edge_cluster_id
            || request.challenge.gateway_pool_id != self.identity.gateway_pool_id
            || request.challenge.gateway_replica_id != self.identity.gateway_replica_id
            || request.challenge.activation_token_digest != self.activation_token_digest
        {
            return Err(BootstrapError::CredentialRejected);
        }

        let mut state = self.state.lock().await;
        if state.certificate_installed || self.certificate_path.exists() {
            state.certificate_installed = true;
            return Err(BootstrapError::AlreadyActivated);
        }
        if let Some(pending) = &state.pending {
            if pending != &request.challenge && now.get() < pending.expires_at_unix_ms.get() {
                return Err(BootstrapError::ChallengeInProgress);
            }
        }
        let signing_bytes = request
            .challenge
            .signing_bytes()
            .map_err(|error| BootstrapError::Invalid(error.to_string()))?;
        let signature = Ed25519Signature::new(self.key_pair.sign(&signing_bytes).as_ref().to_vec())
            .map_err(|_| BootstrapError::Key)?;
        state.pending = Some(request.challenge);
        Ok(GatewayBootstrapProofResponse {
            wire_version: CURRENT_WIRE_VERSION,
            proof: AgentBootstrapProof::new(self.public_key_spki.clone(), signature),
        })
    }

    pub(crate) async fn install_certificate(&self, body: &[u8]) -> Result<(), BootstrapError> {
        let delivery: GatewayBootstrapCertificateDelivery =
            decode_bounded_unique_json(body, MAX_AGENT_ENROLLMENT_MESSAGE_BYTES)
                .map_err(|error| BootstrapError::Invalid(error.to_string()))?;
        delivery
            .validate()
            .map_err(|error| BootstrapError::Invalid(error.to_string()))?;
        if delivery.certificate_generation.get() != 1 {
            return Err(BootstrapError::Invalid(
                "initial workload certificate generation must be 1".into(),
            ));
        }

        let mut state = self.state.lock().await;
        let certificate_digest = ContentDigest::hash(delivery.leaf_certificate_der.as_bytes());
        if state.certificate_installed || self.certificate_path.exists() {
            self.validate_leaf_certificate(&delivery)?;
            let expected = certificate_bundle_pem(&delivery);
            let installed = read_installed_certificate(&self.certificate_path)?;
            state.certificate_installed = true;
            if installed == expected {
                state.installed_request_id = Some(delivery.request_id);
                state.installed_certificate_digest = Some(certificate_digest);
                return Ok(());
            }
            return Err(BootstrapError::AlreadyActivated);
        }
        let pending = state.pending.as_ref().ok_or(BootstrapError::NoChallenge)?;
        let now = unix_millis()?;
        if now.get() >= pending.expires_at_unix_ms.get() {
            state.pending = None;
            return Err(BootstrapError::Expired);
        }
        if delivery.request_id != pending.request_id {
            return Err(BootstrapError::CredentialRejected);
        }
        self.validate_leaf_certificate(&delivery)?;
        let pem = certificate_bundle_pem(&delivery);
        let certificate_path = self.certificate_path.clone();
        tokio::task::spawn_blocking(move || atomic_write_restricted(&certificate_path, &pem))
            .await
            .map_err(|error| BootstrapError::Persistence(error.to_string()))??;
        state.pending = None;
        state.certificate_installed = true;
        state.installed_request_id = Some(delivery.request_id);
        state.installed_certificate_digest = Some(certificate_digest);
        Ok(())
    }

    fn validate_leaf_certificate(
        &self,
        delivery: &GatewayBootstrapCertificateDelivery,
    ) -> Result<(), BootstrapError> {
        let (remainder, certificate) = X509Certificate::from_der(
            delivery.leaf_certificate_der.as_bytes(),
        )
        .map_err(|error| {
            BootstrapError::Invalid(format!("leaf certificate is invalid DER: {error}"))
        })?;
        if !remainder.is_empty() {
            return Err(BootstrapError::Invalid(
                "leaf certificate contains trailing DER data".into(),
            ));
        }
        if certificate.public_key().raw != self.public_key_spki.as_der() {
            return Err(BootstrapError::CredentialRejected);
        }
        let now = unix_millis()?;
        validate_certificate_validity(&certificate, now)?;
        let san = certificate
            .subject_alternative_name()
            .map_err(|error| {
                BootstrapError::Invalid(format!("leaf certificate URI SAN is invalid: {error}"))
            })?
            .ok_or_else(|| {
                BootstrapError::Invalid("leaf certificate has no subjectAltName".into())
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
        if uris.as_slice() != [self.expected_identity_uri.as_str()] {
            return Err(BootstrapError::CredentialRejected);
        }
        let eku = certificate
            .extended_key_usage()
            .map_err(|error| {
                BootstrapError::Invalid(format!("leaf certificate EKU is invalid: {error}"))
            })?
            .ok_or(BootstrapError::CredentialRejected)?;
        if !eku.value.client_auth || !eku.value.server_auth {
            return Err(BootstrapError::CredentialRejected);
        }
        // The protocol carries opaque DER for the issuer chain, but accepting arbitrary bytes
        // would let a successful HTTP response install a chain that can never be used for mTLS.
        // Parse and time-check every certificate before acknowledging delivery.  Cryptographic
        // chain verification remains delegated to the configured workload CA verifier below.
        for (index, chain_der) in delivery.issuer_chain_der.iter().enumerate() {
            let (remainder, chain_certificate) = X509Certificate::from_der(chain_der.as_bytes())
                .map_err(|error| {
                    BootstrapError::Invalid(format!(
                        "issuer chain certificate {index} is invalid DER: {error}"
                    ))
                })?;
            if !remainder.is_empty() {
                return Err(BootstrapError::Invalid(format!(
                    "issuer chain certificate {index} contains trailing DER data"
                )));
            }
            validate_certificate_validity(&chain_certificate, now)?;
        }
        if let Some(verifier) = &self.certificate_verifier {
            let leaf = rustls_pki_types::CertificateDer::from(
                delivery.leaf_certificate_der.as_bytes().to_vec(),
            );
            let intermediates = delivery
                .issuer_chain_der
                .iter()
                .map(|certificate| {
                    rustls_pki_types::CertificateDer::from(certificate.as_bytes().to_vec())
                })
                .collect::<Vec<_>>();
            verifier
                .verify_client_cert(&leaf, &intermediates, rustls_pki_types::UnixTime::now())
                .map_err(|_| BootstrapError::CredentialRejected)?;
        }
        Ok(())
    }
}

fn validate_certificate_validity(
    certificate: &X509Certificate<'_>,
    now: UnixMillis,
) -> Result<(), BootstrapError> {
    let not_before = certificate_timestamp_millis(certificate.validity().not_before.timestamp())?;
    let not_after = certificate_timestamp_millis(certificate.validity().not_after.timestamp())?;
    if not_after <= not_before || now.get() < not_before || now.get() >= not_after {
        return Err(BootstrapError::CredentialRejected);
    }
    Ok(())
}

fn certificate_timestamp_millis(seconds: i64) -> Result<u64, BootstrapError> {
    u64::try_from(seconds)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or_else(|| BootstrapError::Invalid("certificate validity timestamp is invalid".into()))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BootstrapError {
    #[error("Gateway bootstrap configuration is invalid: {0}")]
    Configuration(String),
    #[error("Gateway bootstrap request is invalid: {0}")]
    Invalid(String),
    #[error("Gateway bootstrap credential was rejected")]
    CredentialRejected,
    #[error("Gateway bootstrap challenge has expired")]
    Expired,
    #[error("another Gateway bootstrap challenge is active")]
    ChallengeInProgress,
    #[error("Gateway Replica is already activated")]
    AlreadyActivated,
    #[error("Gateway bootstrap certificate has no matching challenge")]
    NoChallenge,
    #[error("Gateway bootstrap Ed25519 key operation failed")]
    Key,
    #[error("Gateway bootstrap certificate persistence failed: {0}")]
    Persistence(String),
}

fn validate_activation_token(token: &str) -> Result<(), BootstrapError> {
    if !token.starts_with(ACTIVATION_TOKEN_PREFIX)
        || token.len() > ACTIVATION_TOKEN_PREFIX.len() + 128
        || token
            .bytes()
            .skip(ACTIVATION_TOKEN_PREFIX.len())
            .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'_' && byte != b'-')
    {
        return Err(BootstrapError::Configuration(
            "bootstrap activation token has an invalid shape".into(),
        ));
    }
    let encoded = &token[ACTIVATION_TOKEN_PREFIX.len()..];
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .map_err(|_| {
            BootstrapError::Configuration(
                "bootstrap activation token is not canonical base64url".into(),
            )
        })?;
    if decoded.len() != 32
        || base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&decoded) != encoded
    {
        return Err(BootstrapError::Configuration(
            "bootstrap activation token must contain exactly 32 bytes".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_workload_trust_domain(value: &str) -> Result<(), BootstrapError> {
    let valid = !value.is_empty()
        && value.len() <= 253
        && !value.bytes().any(|byte| byte.is_ascii_uppercase())
        && value.split('.').all(|label| {
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
    if !valid {
        return Err(BootstrapError::Configuration(
            "workload trust domain must be a canonical lowercase DNS name".into(),
        ));
    }
    Ok(())
}

fn load_certificate_verifier(path: &Path) -> Result<Arc<dyn ClientCertVerifier>, BootstrapError> {
    let bytes = read_bounded(path, "workload client CA", 1024 * 1024)?;
    let certificates = rustls_pki_types::CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            BootstrapError::Configuration(format!("workload client CA is invalid: {error}"))
        })?;
    if certificates.is_empty() {
        return Err(BootstrapError::Configuration(
            "workload client CA contains no certificates".into(),
        ));
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate).map_err(|error| {
            BootstrapError::Configuration(format!("workload client CA is invalid: {error}"))
        })?;
    }
    let provider: Arc<rustls::crypto::CryptoProvider> =
        rustls::crypto::aws_lc_rs::default_provider().into();
    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
        .build()
        .map_err(|error| {
            BootstrapError::Configuration(format!("workload client CA is invalid: {error}"))
        })
}

fn unix_millis() -> Result<UnixMillis, BootstrapError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| BootstrapError::Configuration(error.to_string()))?
        .as_millis()
        .try_into()
        .map_err(|_| BootstrapError::Configuration("system clock overflow".into()))?;
    Ok(UnixMillis::new(millis))
}

fn read_bounded(path: &Path, kind: &str, max_bytes: u64) -> Result<Vec<u8>, BootstrapError> {
    let metadata = fs::metadata(path)
        .map_err(|error| BootstrapError::Configuration(format!("{kind}: {error}")))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(BootstrapError::Configuration(format!(
            "{kind} must be a regular file containing 1..={max_bytes} bytes"
        )));
    }
    fs::read(path).map_err(|error| BootstrapError::Configuration(format!("{kind}: {error}")))
}

fn read_installed_certificate(path: &Path) -> Result<Vec<u8>, BootstrapError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| BootstrapError::Persistence(error.to_string()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_BOOTSTRAP_CERTIFICATE_BYTES
    {
        return Err(BootstrapError::Persistence(
            "installed certificate must be a bounded regular file".into(),
        ));
    }
    fs::read(path).map_err(|error| BootstrapError::Persistence(error.to_string()))
}

fn require_restricted_file(path: &Path, kind: &str) -> Result<(), BootstrapError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| BootstrapError::Configuration(format!("{kind}: {error}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(BootstrapError::Configuration(format!(
            "{kind} must be a regular file and not a symbolic link"
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode();
        if mode & 0o027 != 0 {
            return Err(BootstrapError::Configuration(format!(
                "{kind} must not be group-writable or accessible by other users"
            )));
        }
    }
    Ok(())
}

fn certificate_bundle_pem(delivery: &GatewayBootstrapCertificateDelivery) -> Vec<u8> {
    let mut pem = Vec::new();
    append_pem(&mut pem, delivery.leaf_certificate_der.as_bytes());
    for certificate in &delivery.issuer_chain_der {
        append_pem(&mut pem, certificate.as_bytes());
    }
    pem
}

fn append_pem(output: &mut Vec<u8>, der: &[u8]) {
    output.extend_from_slice(b"-----BEGIN CERTIFICATE-----\n");
    let encoded = STANDARD.encode(der);
    for chunk in encoded.as_bytes().chunks(64) {
        output.extend_from_slice(chunk);
        output.push(b'\n');
    }
    output.extend_from_slice(b"-----END CERTIFICATE-----\n");
}

fn atomic_write_restricted(path: &Path, bytes: &[u8]) -> Result<(), BootstrapError> {
    let parent = path.parent().ok_or_else(|| {
        BootstrapError::Persistence("certificate path has no parent directory".into())
    })?;
    if !parent.is_dir() {
        return Err(BootstrapError::Persistence(
            "certificate parent directory does not exist".into(),
        ));
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            BootstrapError::Persistence("certificate path has no valid file name".into())
        })?;
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random).map_err(|error| BootstrapError::Persistence(error.to_string()))?;
    let suffix = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let temporary = parent.join(format!(".{file_name}.tmp-{suffix}"));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|error| BootstrapError::Persistence(error.to_string()))?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
        fs::File::open(parent)?.sync_all()?;
        Ok::<_, std::io::Error>(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(BootstrapError::Persistence(error.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use neoengram_domain::protocol::{
        CertificateGeneration, EdgeClusterId, GatewayOpaqueBytes, GatewayPoolId, GatewayReplicaId,
    };

    use super::*;

    const TOKEN: &str = "nggw_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEINQawrTMCmjrnfruh9FAsmFhzfyw4nNF+73pdTtdaJ46
-----END PRIVATE KEY-----
"#;

    fn identity() -> GatewayIdentity {
        GatewayIdentity {
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            software_version: "test".to_owned(),
        }
    }

    fn write_secret(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, PermissionsExt::from_mode(0o600)).unwrap();
        }
    }

    fn fixture_with_ca(
        ca_pem: Option<&str>,
    ) -> (tempfile::TempDir, Arc<GatewayBootstrap>, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let key = directory.path().join("activation-key.pem");
        let token = directory.path().join("activation-token");
        let certificate = directory.path().join("workload-chain.pem");
        let ca = directory.path().join("workload-ca.pem");
        write_secret(&key, PRIVATE_KEY.as_bytes());
        write_secret(&token, TOKEN.as_bytes());
        if let Some(ca_pem) = ca_pem {
            write_secret(&ca, ca_pem.as_bytes());
        }
        let bootstrap = GatewayBootstrapConfig {
            private_key_file: Some(key),
            activation_token_file: Some(token),
            certificate_chain_file: Some(certificate.clone()),
        }
        .load(
            identity(),
            Some("mesh.example.test"),
            None,
            ca_pem.map(|_| ca.as_path()),
        )
        .unwrap()
        .unwrap();
        (directory, bootstrap, certificate)
    }

    fn fixture() -> (tempfile::TempDir, Arc<GatewayBootstrap>, PathBuf) {
        fixture_with_ca(None)
    }

    fn challenge() -> GatewayBootstrapChallenge {
        let now = unix_millis().unwrap().get();
        GatewayBootstrapChallenge {
            request_id: neoengram_domain::protocol::RequestId::new("request-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            activation_token_digest: ContentDigest::hash(TOKEN.as_bytes()),
            nonce: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7_u8; 32]),
            issued_at_unix_ms: UnixMillis::new(now.saturating_sub(1)),
            expires_at_unix_ms: UnixMillis::new(now + 60_000 - 1),
        }
    }

    fn leaf_certificate(uri: &str, private_key_pem: Option<&str>) -> Vec<u8> {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, SanType};

        let mut parameters = CertificateParams::new(Vec::<String>::new()).unwrap();
        parameters
            .subject_alt_names
            .push(SanType::URI(uri.try_into().unwrap()));
        parameters.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let key = private_key_pem
            .map(KeyPair::from_pem)
            .transpose()
            .unwrap()
            .unwrap_or_else(|| KeyPair::generate().unwrap());
        parameters.self_signed(&key).unwrap().der().to_vec()
    }

    fn leaf_certificate_with_dates(
        uri: &str,
        private_key_pem: &str,
        not_before_year: i32,
        not_after_year: i32,
    ) -> Vec<u8> {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, SanType};

        let mut parameters = CertificateParams::new(Vec::<String>::new()).unwrap();
        parameters.not_before = rcgen::date_time_ymd(not_before_year, 1, 1);
        parameters.not_after = rcgen::date_time_ymd(not_after_year, 1, 1);
        parameters
            .subject_alt_names
            .push(SanType::URI(uri.try_into().unwrap()));
        parameters.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let key = KeyPair::from_pem(private_key_pem).unwrap();
        parameters.self_signed(&key).unwrap().der().to_vec()
    }

    fn matching_leaf_certificate() -> Vec<u8> {
        leaf_certificate(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a",
            Some(PRIVATE_KEY),
        )
    }

    fn ca_signed_leaf() -> (Vec<u8>, Vec<u8>, String) {
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
            KeyUsagePurpose, SanType,
        };

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca = ca_parameters.self_signed(&ca_key).unwrap();
        let leaf_key = KeyPair::from_pem(PRIVATE_KEY).unwrap();
        let mut leaf_parameters = CertificateParams::new(Vec::<String>::new()).unwrap();
        leaf_parameters.subject_alt_names.push(SanType::URI(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a"
                .try_into()
                .unwrap(),
        ));
        leaf_parameters.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let leaf = leaf_parameters.signed_by(&leaf_key, &ca, &ca_key).unwrap();
        (leaf.der().to_vec(), ca.der().to_vec(), ca.pem())
    }

    #[test]
    fn bootstrap_file_configuration_is_all_or_nothing() {
        let incomplete = GatewayBootstrapConfig {
            private_key_file: Some(PathBuf::from("/key")),
            activation_token_file: None,
            certificate_chain_file: None,
        };
        assert!(matches!(
            incomplete.validate(),
            Err(BootstrapError::Configuration(_))
        ));
    }

    #[test]
    fn trust_domain_survives_after_short_lived_bootstrap_material_is_removed() {
        let post_activation = GatewayBootstrapConfig {
            private_key_file: None,
            activation_token_file: None,
            certificate_chain_file: None,
        };
        post_activation.validate().unwrap();
        assert!(post_activation
            .load(identity(), Some("mesh.example.test"), None, None)
            .unwrap()
            .is_none());

        let bootstrap_without_trust_domain = GatewayBootstrapConfig {
            private_key_file: Some(PathBuf::from("/key")),
            activation_token_file: Some(PathBuf::from("/token")),
            certificate_chain_file: Some(PathBuf::from("/certificate")),
        };
        assert!(bootstrap_without_trust_domain
            .load(identity(), None, None, None)
            .is_err());
    }

    #[tokio::test]
    async fn challenge_is_signed_and_certificate_is_installed_once() {
        let (directory, bootstrap, certificate_path) = fixture();
        let challenge = challenge();
        let request = GatewayBootstrapChallengeRequest {
            wire_version: CURRENT_WIRE_VERSION,
            challenge: challenge.clone(),
        };
        let proof = bootstrap
            .prove(&serde_json::to_vec(&request).unwrap())
            .await
            .unwrap();
        proof
            .proof
            .verify(&challenge.signing_bytes().unwrap())
            .unwrap();

        let delivery = GatewayBootstrapCertificateDelivery {
            wire_version: CURRENT_WIRE_VERSION,
            request_id: challenge.request_id,
            certificate_generation: CertificateGeneration::new(1),
            leaf_certificate_der: GatewayOpaqueBytes::new(matching_leaf_certificate()).unwrap(),
            issuer_chain_der: vec![GatewayOpaqueBytes::new(matching_leaf_certificate()).unwrap()],
        };
        let delivery_bytes = serde_json::to_vec(&delivery).unwrap();
        bootstrap
            .install_certificate(&delivery_bytes)
            .await
            .unwrap();
        let persisted = fs::read_to_string(&certificate_path).unwrap();
        assert_eq!(persisted.matches("BEGIN CERTIFICATE").count(), 2);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&certificate_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        bootstrap
            .install_certificate(&delivery_bytes)
            .await
            .unwrap();
        assert!(bootstrap.restart_required().await);
        let restarted = GatewayBootstrapConfig {
            private_key_file: Some(directory.path().join("activation-key.pem")),
            activation_token_file: Some(directory.path().join("activation-token")),
            certificate_chain_file: Some(certificate_path.clone()),
        }
        .load(
            identity(),
            Some("mesh.example.test"),
            Some(&certificate_path),
            None,
        )
        .unwrap()
        .unwrap();
        assert!(!restarted.restart_required().await);
        restarted
            .install_certificate(&delivery_bytes)
            .await
            .unwrap();
        let mut different_chain = delivery;
        different_chain.issuer_chain_der = vec![
            GatewayOpaqueBytes::new(matching_leaf_certificate()).unwrap(),
            GatewayOpaqueBytes::new(matching_leaf_certificate()).unwrap(),
        ];
        assert!(matches!(
            restarted
                .install_certificate(&serde_json::to_vec(&different_chain).unwrap())
                .await,
            Err(BootstrapError::AlreadyActivated)
        ));
        assert!(matches!(
            bootstrap
                .prove(&serde_json::to_vec(&request).unwrap())
                .await,
            Err(BootstrapError::AlreadyActivated)
        ));
    }

    #[tokio::test]
    async fn certificate_delivery_is_fenced_by_request_and_generation() {
        let (_directory, bootstrap, _certificate_path) = fixture();
        let challenge = challenge();
        let request = GatewayBootstrapChallengeRequest {
            wire_version: CURRENT_WIRE_VERSION,
            challenge: challenge.clone(),
        };
        bootstrap
            .prove(&serde_json::to_vec(&request).unwrap())
            .await
            .unwrap();
        let delivery = |request_id, generation, leaf| GatewayBootstrapCertificateDelivery {
            wire_version: CURRENT_WIRE_VERSION,
            request_id,
            certificate_generation: CertificateGeneration::new(generation),
            leaf_certificate_der: GatewayOpaqueBytes::new(leaf).unwrap(),
            issuer_chain_der: vec![GatewayOpaqueBytes::new(matching_leaf_certificate()).unwrap()],
        };
        let wrong_request = delivery(
            neoengram_domain::protocol::RequestId::new("request-b").unwrap(),
            1,
            matching_leaf_certificate(),
        );
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&wrong_request).unwrap())
                .await,
            Err(BootstrapError::CredentialRejected)
        ));
        let wrong_generation =
            delivery(challenge.request_id.clone(), 2, matching_leaf_certificate());
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&wrong_generation).unwrap())
                .await,
            Err(BootstrapError::Invalid(_))
        ));
        let wrong_san = delivery(
            challenge.request_id.clone(),
            1,
            leaf_certificate(
                "spiffe://mesh.example.test/workloads/edge-clusters/cluster-b/gateway-pools/pool-a/gateway-replicas/replica-a",
                Some(PRIVATE_KEY),
            ),
        );
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&wrong_san).unwrap())
                .await,
            Err(BootstrapError::CredentialRejected)
        ));
        let wrong_key = delivery(
            challenge.request_id.clone(),
            1,
            leaf_certificate(
                "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a",
                None,
            ),
        );
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&wrong_key).unwrap())
                .await,
            Err(BootstrapError::CredentialRejected)
        ));
        let expired = delivery(
            challenge.request_id.clone(),
            1,
            leaf_certificate_with_dates(
                "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a",
                PRIVATE_KEY,
                2000,
                2001,
            ),
        );
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&expired).unwrap())
                .await,
            Err(BootstrapError::CredentialRejected)
        ));
        let future = delivery(
            challenge.request_id.clone(),
            1,
            leaf_certificate_with_dates(
                "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a",
                PRIVATE_KEY,
                2099,
                2100,
            ),
        );
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&future).unwrap())
                .await,
            Err(BootstrapError::CredentialRejected)
        ));
        let mut invalid_chain = delivery(challenge.request_id, 1, matching_leaf_certificate());
        invalid_chain.issuer_chain_der = vec![GatewayOpaqueBytes::new([0x30, 0x02]).unwrap()];
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&invalid_chain).unwrap())
                .await,
            Err(BootstrapError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn certificate_delivery_requires_the_configured_ca_chain() {
        let (leaf, ca, ca_pem) = ca_signed_leaf();
        let (untrusted_leaf, untrusted_ca, _) = ca_signed_leaf();
        let (_directory, bootstrap, _certificate_path) = fixture_with_ca(Some(&ca_pem));
        let challenge = challenge();
        bootstrap
            .prove(
                &serde_json::to_vec(&GatewayBootstrapChallengeRequest {
                    wire_version: CURRENT_WIRE_VERSION,
                    challenge: challenge.clone(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let delivery = GatewayBootstrapCertificateDelivery {
            wire_version: CURRENT_WIRE_VERSION,
            request_id: challenge.request_id,
            certificate_generation: CertificateGeneration::new(1),
            leaf_certificate_der: GatewayOpaqueBytes::new(leaf).unwrap(),
            issuer_chain_der: vec![GatewayOpaqueBytes::new(ca).unwrap()],
        };
        let mut untrusted = delivery.clone();
        untrusted.leaf_certificate_der = GatewayOpaqueBytes::new(untrusted_leaf).unwrap();
        untrusted.issuer_chain_der = vec![GatewayOpaqueBytes::new(untrusted_ca).unwrap()];
        assert!(matches!(
            bootstrap
                .install_certificate(&serde_json::to_vec(&untrusted).unwrap())
                .await,
            Err(BootstrapError::CredentialRejected)
        ));
        bootstrap
            .install_certificate(&serde_json::to_vec(&delivery).unwrap())
            .await
            .unwrap();
    }
}
