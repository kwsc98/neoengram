use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use neoengram_central::{
    run_with_dependencies, CentralCommandKeyId, CentralCommandKeyState, CentralCommandKeyring,
    CentralCommandSignature, CentralCommandSignatureRequest, CentralCommandSigner,
    CentralCommandSignerError, CentralCommandTrustBundle, CentralCommandVerificationKey, Config,
    GatewayActivationDependencies, HttpGatewayBootstrapTransport, IssuedWorkloadCertificate,
    RuntimeDependencies, WorkloadCertificateIssuer, WorkloadCertificateIssuerError,
    WorkloadCertificateRequest, WorkloadCertificateUsage,
};
use neoengram_domain::protocol::{CertificateGeneration, Ed25519PublicKeySpki, Ed25519Signature};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    SanType, SubjectPublicKeyInfo,
};
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use time::OffsetDateTime;
use tracing_subscriber::EnvFilter;

const DEVELOPMENT_COMMAND_KEY_ID: &str = "central-command-local";
const DEVELOPMENT_COMMAND_KEY_SEED: [u8; 32] = [0x42; 32];
const DEVELOPMENT_WORKLOAD_TRUST_DOMAIN: &str = "development.neoengram.local";

struct DevelopmentCommandSigner {
    key_pair: Ed25519KeyPair,
    generation: CertificateGeneration,
}

#[async_trait]
impl CentralCommandSigner for DevelopmentCommandSigner {
    async fn sign(
        &self,
        request: CentralCommandSignatureRequest,
    ) -> Result<CentralCommandSignature, CentralCommandSignerError> {
        let signature = Ed25519Signature::new(
            self.key_pair
                .sign(request.signing_bytes())
                .as_ref()
                .to_vec(),
        )
        .map_err(|error| CentralCommandSignerError::Internal(error.to_string()))?;
        CentralCommandSignature::new(request.key_id().clone(), self.generation, signature)
            .map_err(|error| CentralCommandSignerError::Internal(error.to_string()))
    }
}

fn development_command_keyring(
) -> Result<Arc<CentralCommandKeyring>, Box<dyn std::error::Error + Send + Sync>> {
    let key_pair = Ed25519KeyPair::from_seed_unchecked(&DEVELOPMENT_COMMAND_KEY_SEED)
        .map_err(|_| "invalid development command signing seed")?;
    let generation = CertificateGeneration::new(1);
    let key_id = CentralCommandKeyId::new(DEVELOPMENT_COMMAND_KEY_ID)?;
    let verification_key = CentralCommandVerificationKey::new(
        key_id.clone(),
        generation,
        Ed25519PublicKeySpki::from_public_key_bytes(
            key_pair
                .public_key()
                .as_ref()
                .try_into()
                .map_err(|_| "invalid Ed25519 public key")?,
        ),
        CentralCommandKeyState::Active,
    )?;
    let trust_bundle = CentralCommandTrustBundle::new(vec![verification_key])?;
    Ok(Arc::new(CentralCommandKeyring::new(
        Arc::new(DevelopmentCommandSigner {
            key_pair,
            generation,
        }),
        trust_bundle,
        key_id,
        generation,
    )?))
}

struct DevelopmentWorkloadCertificateIssuer {
    issuer: Certificate,
    issuer_key: KeyPair,
}

impl DevelopmentWorkloadCertificateIssuer {
    fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let issuer_key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        params.not_before = OffsetDateTime::from_unix_timestamp(0)?;
        params.not_after = OffsetDateTime::from_unix_timestamp(4_000_000_000)?;
        let issuer = params.self_signed(&issuer_key)?;
        Ok(Self { issuer, issuer_key })
    }
}

#[async_trait]
impl WorkloadCertificateIssuer for DevelopmentWorkloadCertificateIssuer {
    async fn issue(
        &self,
        request: WorkloadCertificateRequest,
    ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
        let mut params = CertificateParams::new(Vec::<String>::new())
            .map_err(|error| WorkloadCertificateIssuerError::Internal(error.to_string()))?;
        params.not_before = OffsetDateTime::from_unix_timestamp(
            i64::try_from(request.not_before_unix_ms().get() / 1_000)
                .map_err(|error| WorkloadCertificateIssuerError::Internal(error.to_string()))?,
        )
        .map_err(|error| WorkloadCertificateIssuerError::Internal(error.to_string()))?;
        params.not_after = OffsetDateTime::from_unix_timestamp(
            i64::try_from(request.not_after_unix_ms().get().div_ceil(1_000))
                .map_err(|error| WorkloadCertificateIssuerError::Internal(error.to_string()))?,
        )
        .map_err(|error| WorkloadCertificateIssuerError::Internal(error.to_string()))?;
        params.subject_alt_names.push(SanType::URI(
            request
                .identity_uri()
                .as_str()
                .try_into()
                .map_err(|error: rcgen::Error| {
                    WorkloadCertificateIssuerError::Internal(error.to_string())
                })?,
        ));
        for name in request.server_names() {
            if let Ok(address) = name.parse() {
                params.subject_alt_names.push(SanType::IpAddress(address));
            } else {
                params
                    .subject_alt_names
                    .push(SanType::DnsName(name.clone().try_into().map_err(
                        |error: rcgen::Error| {
                            WorkloadCertificateIssuerError::Internal(error.to_string())
                        },
                    )?));
            }
        }
        params.extended_key_usages = request
            .required_extended_key_usages()
            .iter()
            .map(|usage| match usage {
                WorkloadCertificateUsage::ClientAuth => ExtendedKeyUsagePurpose::ClientAuth,
                WorkloadCertificateUsage::ServerAuth => ExtendedKeyUsagePurpose::ServerAuth,
            })
            .collect();
        let public_key = SubjectPublicKeyInfo::from_der(request.public_key_spki().as_der())
            .map_err(|error| WorkloadCertificateIssuerError::Internal(error.to_string()))?;
        let leaf = params
            .signed_by(&public_key, &self.issuer, &self.issuer_key)
            .map_err(|error| WorkloadCertificateIssuerError::Internal(error.to_string()))?;
        IssuedWorkloadCertificate::from_request(
            request,
            leaf.der().to_vec(),
            vec![self.issuer.der().to_vec()],
        )
        .map_err(WorkloadCertificateIssuerError::Invalid)
    }
}

fn development_gateway_activation(
) -> Result<GatewayActivationDependencies, Box<dyn std::error::Error + Send + Sync>> {
    let issuer = Arc::new(DevelopmentWorkloadCertificateIssuer::new()?);
    // The local bootstrap certificate is generated for the loopback-only development process and
    // is intentionally not part of the host trust store. Production supplies a configured CA
    // bundle through its Gateway bootstrap transport instead.
    let transport = Arc::new(HttpGatewayBootstrapTransport::new(
        reqwest::Client::builder().danger_accept_invalid_certs(true),
    )?);
    Ok(GatewayActivationDependencies::new(
        issuer,
        transport,
        DEVELOPMENT_WORKLOAD_TRUST_DOMAIN,
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::parse();
    config.validate()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()?;
    let dependencies = if config.development {
        RuntimeDependencies::default()
            .with_central_command_keyring(development_command_keyring()?)
            .with_gateway_activation(development_gateway_activation()?)
    } else {
        RuntimeDependencies::default()
    };
    run_with_dependencies(config, dependencies).await?;
    Ok(())
}
