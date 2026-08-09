//! Central-authoritative activation of a Gateway Replica.
//!
//! The bootstrap HTTP/H2 adapter is deliberately kept outside this module.  This service owns the
//! security-sensitive state transition: a short-lived activation token is looked up by digest,
//! the Replica proves possession of its Ed25519 key over a Central-issued challenge, an external
//! workload CA issues the certificate, and ResourceVersion compare-and-swap boundaries persist it
//! before delivery and activate the Replica only after delivery is acknowledged.

use std::{cmp, collections::BTreeSet, fmt, sync::Arc};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentBootstrapProof, CertificateGeneration, EdgeClusterId, GatewayOpaqueBytes, GatewayPoolId,
    GatewayReplicaId, RequestId, UnixMillis,
};
use neoengramd::{
    CentralError, CentralErrorCode, Clock, GatewayCredentialState, GatewayRegistryRepository,
    GatewayReplicaCertificateRecord, GatewayReplicaRecord, GatewayReplicaState,
};
use serde::Serialize;
use thiserror::Error;
use url::Url;

use super::{
    validate_issued_workload_certificate, IssuedWorkloadCertificate, WorkloadCertificateIssuance,
    WorkloadCertificateIssuer, WorkloadCertificateIssuerError, WorkloadCertificateRequest,
    WorkloadIdentityUri, WorkloadPkiError,
};

/// A challenge is valid only for this short window.  It is also bounded by the activation token
/// expiry, so a challenge can never extend a token's lifetime.
pub const GATEWAY_ACTIVATION_CHALLENGE_TTL_MS: u64 =
    neoengram_protocol::GATEWAY_BOOTSTRAP_CHALLENGE_TTL_MS;
const ACTIVATION_NONCE_BYTES: usize = 32;
const ACTIVATION_TOKEN_PREFIX: &str = "nggw_v1_";
const ACTIVATION_DOMAIN_V1: &str = "neoengram-gateway-replica-activation-v1";

/// Gateway-specific name for the existing validated Ed25519 proof-of-possession wire shape.
/// Reusing this type keeps SPKI parsing and signature validation identical to Agent bootstrap.
pub type GatewayReplicaActivationProof = AgentBootstrapProof;

/// Central-issued, Replica-specific proof-of-possession challenge.
///
/// The token itself is never included in this value.  Its digest is included in the signed
/// material so a proof cannot be moved to another activation token or Replica.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct GatewayReplicaActivationChallenge {
    request_id: RequestId,
    edge_cluster_id: EdgeClusterId,
    gateway_pool_id: GatewayPoolId,
    gateway_replica_id: GatewayReplicaId,
    activation_token_digest: ContentDigest,
    nonce: String,
    issued_at_unix_ms: UnixMillis,
    expires_at_unix_ms: UnixMillis,
}

impl fmt::Debug for GatewayReplicaActivationChallenge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayReplicaActivationChallenge")
            .field("request_id", &self.request_id)
            .field("edge_cluster_id", &self.edge_cluster_id)
            .field("gateway_pool_id", &self.gateway_pool_id)
            .field("gateway_replica_id", &self.gateway_replica_id)
            .field("activation_token_digest", &"[REDACTED]")
            .field("nonce", &"[REDACTED]")
            .field("issued_at_unix_ms", &self.issued_at_unix_ms)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish()
    }
}

impl GatewayReplicaActivationChallenge {
    /// Stable request identifier used both for the certificate issuance request and audit logs.
    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub fn edge_cluster_id(&self) -> &EdgeClusterId {
        &self.edge_cluster_id
    }

    #[must_use]
    pub fn gateway_pool_id(&self) -> &GatewayPoolId {
        &self.gateway_pool_id
    }

    #[must_use]
    pub fn gateway_replica_id(&self) -> &GatewayReplicaId {
        &self.gateway_replica_id
    }

    /// Returns the canonical base64url nonce that the Replica must sign.
    #[must_use]
    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    #[must_use]
    pub const fn issued_at_unix_ms(&self) -> UnixMillis {
        self.issued_at_unix_ms
    }

    #[must_use]
    pub const fn expires_at_unix_ms(&self) -> UnixMillis {
        self.expires_at_unix_ms
    }

    #[must_use]
    pub const fn activation_token_digest(&self) -> ContentDigest {
        self.activation_token_digest
    }

    /// Returns the domain-separated canonical bytes covered by the Replica's Ed25519 signature.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, GatewayReplicaActivationError> {
        self.validate()
            .map_err(|_| GatewayReplicaActivationError::InvalidChallenge)?;
        let input = ActivationChallengeSigningInput {
            version: 1,
            request_id: &self.request_id,
            edge_cluster_id: &self.edge_cluster_id,
            gateway_pool_id: &self.gateway_pool_id,
            gateway_replica_id: &self.gateway_replica_id,
            activation_token_digest: self.activation_token_digest,
            nonce: &self.nonce,
            issued_at_unix_ms: self.issued_at_unix_ms,
            expires_at_unix_ms: self.expires_at_unix_ms,
        };
        neoengram_protocol::domain_separated_jcs_bytes(ACTIVATION_DOMAIN_V1, &input)
            .map_err(|_| GatewayReplicaActivationError::InvalidChallenge)
    }

    fn validate(&self) -> Result<(), GatewayReplicaActivationError> {
        if self.issued_at_unix_ms.get() == 0
            || self.expires_at_unix_ms.get() <= self.issued_at_unix_ms.get()
            || self
                .expires_at_unix_ms
                .get()
                .saturating_sub(self.issued_at_unix_ms.get())
                > GATEWAY_ACTIVATION_CHALLENGE_TTL_MS
        {
            return Err(GatewayReplicaActivationError::InvalidChallenge);
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(self.nonce.as_bytes())
            .map_err(|_| GatewayReplicaActivationError::InvalidChallenge)?;
        if decoded.len() != ACTIVATION_NONCE_BYTES || URL_SAFE_NO_PAD.encode(&decoded) != self.nonce
        {
            return Err(GatewayReplicaActivationError::InvalidChallenge);
        }
        Ok(())
    }

    fn validate_at(&self, now: UnixMillis) -> Result<(), GatewayReplicaActivationError> {
        self.validate()?;
        if now.get() < self.issued_at_unix_ms.get() || now.get() >= self.expires_at_unix_ms.get() {
            return Err(GatewayReplicaActivationError::ChallengeExpired);
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ActivationChallengeSigningInput<'a> {
    version: u8,
    request_id: &'a RequestId,
    edge_cluster_id: &'a EdgeClusterId,
    gateway_pool_id: &'a GatewayPoolId,
    gateway_replica_id: &'a GatewayReplicaId,
    activation_token_digest: ContentDigest,
    nonce: &'a str,
    issued_at_unix_ms: UnixMillis,
    expires_at_unix_ms: UnixMillis,
}

/// Result of a successful activation. The certificate bytes are returned to the bootstrap
/// adapter for delivery and retained in the Registry so an interrupted prepare/deliver/commit
/// sequence can retry the exact issuance result without minting a second certificate.
#[derive(Clone, PartialEq, Eq)]
pub struct GatewayReplicaActivationResult {
    pub replica: GatewayReplicaRecord,
    pub certificate: IssuedWorkloadCertificate,
}

impl fmt::Debug for GatewayReplicaActivationResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayReplicaActivationResult")
            .field("replica", &self.replica)
            .field("certificate", &self.certificate)
            .finish()
    }
}

/// Errors are intentionally coarse for token and proof failures.  Callers must not be able to use
/// the bootstrap endpoint as a token or Replica identity oracle.
#[derive(Debug, Error)]
pub enum GatewayReplicaActivationError {
    #[error("Gateway Replica activation credential was rejected")]
    CredentialRejected,
    #[error("Gateway Replica activation challenge is invalid")]
    InvalidChallenge,
    #[error("Gateway Replica activation challenge has expired")]
    ChallengeExpired,
    #[error("Gateway Replica activation proof is invalid")]
    ProofInvalid,
    #[error("Gateway Replica activation was superseded by another request")]
    ConcurrentActivation,
    #[error("secure Gateway Replica activation nonce generation failed")]
    Randomness,
    #[error("Gateway Replica activation configuration is invalid: {0}")]
    Configuration(#[source] WorkloadPkiError),
    #[error("workload certificate issuer failed: {0}")]
    CertificateIssuer(#[source] WorkloadCertificateIssuerError),
    #[error("Gateway Registry operation failed: {0}")]
    Registry(#[source] CentralError),
    #[error("Gateway Replica activation resource version overflow")]
    ResourceVersionOverflow,
    #[error("Gateway Replica activation certificate material is invalid")]
    CertificateMaterialInvalid,
}

/// Domain service for one-time Gateway Replica activation.
pub struct GatewayReplicaActivationService {
    repository: Arc<dyn GatewayRegistryRepository>,
    issuance: WorkloadCertificateIssuance,
    clock: Arc<dyn Clock>,
    trust_domain: String,
}

impl GatewayReplicaActivationService {
    /// Constructs an activation service.  `trust_domain` is checked when each identity URI is
    /// built, keeping configuration errors fail-closed even when the service is assembled before
    /// the first bootstrap request.
    #[must_use]
    pub fn new(
        repository: Arc<dyn GatewayRegistryRepository>,
        issuer: Arc<dyn WorkloadCertificateIssuer>,
        clock: Arc<dyn Clock>,
        trust_domain: impl Into<String>,
    ) -> Self {
        Self {
            repository,
            issuance: WorkloadCertificateIssuance::new(issuer),
            clock,
            trust_domain: trust_domain.into(),
        }
    }

    /// Returns the Registry-owned Replica record for an activation token.
    ///
    /// Activation callers must use this value when choosing the bootstrap origin.  In
    /// particular, an endpoint supplied by an HTTP request or a stale in-memory record must not
    /// be allowed to redirect proof and certificate delivery to a different server.
    pub async fn replica_for_activation(
        &self,
        activation_token: &str,
    ) -> Result<GatewayReplicaRecord, GatewayReplicaActivationError> {
        let token_digest = activation_token_digest(activation_token)?;
        self.repository
            .get_replica_by_activation_token_digest(&token_digest)
            .await
            .map_err(GatewayReplicaActivationError::Registry)?
            .ok_or(GatewayReplicaActivationError::CredentialRejected)
    }

    /// Issues a short-lived challenge after validating the one-time activation token.
    pub async fn issue_challenge(
        &self,
        activation_token: &str,
    ) -> Result<GatewayReplicaActivationChallenge, GatewayReplicaActivationError> {
        let token_digest = activation_token_digest(activation_token)?;
        let record = self
            .repository
            .get_replica_by_activation_token_digest(&token_digest)
            .await
            .map_err(GatewayReplicaActivationError::Registry)?
            .ok_or(GatewayReplicaActivationError::CredentialRejected)?;
        let now = self.clock.now();
        self.validate_pending_credential(&record, now).await?;

        let mut nonce = [0_u8; ACTIVATION_NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(|_| GatewayReplicaActivationError::Randomness)?;
        let nonce_encoded = URL_SAFE_NO_PAD.encode(nonce);
        let request_id = RequestId::new(format!("gwact-{}", ContentDigest::hash(nonce).to_hex()))
            .map_err(|_| GatewayReplicaActivationError::Randomness)?;
        let expires_at = cmp::min(
            record.credential.activation_expires_at_unix_ms.get(),
            now.get()
                .checked_add(GATEWAY_ACTIVATION_CHALLENGE_TTL_MS)
                .ok_or(GatewayReplicaActivationError::ResourceVersionOverflow)?,
        );
        let challenge = GatewayReplicaActivationChallenge {
            request_id,
            edge_cluster_id: record.edge_cluster_id,
            gateway_pool_id: record.gateway_pool_id,
            gateway_replica_id: record.gateway_replica_id,
            activation_token_digest: token_digest,
            nonce: nonce_encoded,
            issued_at_unix_ms: now,
            expires_at_unix_ms: UnixMillis::new(expires_at),
        };
        challenge
            .validate()
            .map_err(|_| GatewayReplicaActivationError::InvalidChallenge)?;
        Ok(challenge)
    }

    /// Returns a previously prepared certificate, if delivery was interrupted after the prepare
    /// CAS.  The certificate is returned only for the exact Replica identified by the token; the
    /// token remains unconsumed until [`Self::commit`] succeeds.
    pub async fn resume_prepared(
        &self,
        activation_token: &str,
    ) -> Result<Option<GatewayReplicaActivationResult>, GatewayReplicaActivationError> {
        let token_digest = activation_token_digest(activation_token)?;
        let record = self
            .repository
            .get_replica_by_activation_token_digest(&token_digest)
            .await
            .map_err(GatewayReplicaActivationError::Registry)?
            .ok_or(GatewayReplicaActivationError::CredentialRejected)?;
        let Some(persisted) = record.credential.certificate.as_ref() else {
            self.validate_pending_credential(&record, self.clock.now())
                .await?;
            return Ok(None);
        };
        if record.state != GatewayReplicaState::Pending
            || record.credential.state != GatewayCredentialState::PendingCertificateDelivery
            || record.credential.activation_consumed_at_unix_ms.is_some()
        {
            return Err(GatewayReplicaActivationError::CredentialRejected);
        }
        let now = self.clock.now();
        if now.get() >= persisted.not_after_unix_ms.get() {
            return Err(GatewayReplicaActivationError::CredentialRejected);
        }
        let certificate = self
            .issued_certificate_from_record(&record, persisted)
            .map_err(|_| GatewayReplicaActivationError::CertificateMaterialInvalid)?;
        Ok(Some(GatewayReplicaActivationResult {
            replica: record,
            certificate,
        }))
    }

    /// Verifies proof-of-possession and durably prepares the certificate for delivery.  The
    /// Replica remains Pending and the activation token remains unconsumed until [`Self::commit`]
    /// is called after the bootstrap transport acknowledges delivery.
    pub async fn prepare(
        &self,
        challenge: &GatewayReplicaActivationChallenge,
        activation_token: &str,
        proof: &GatewayReplicaActivationProof,
    ) -> Result<GatewayReplicaActivationResult, GatewayReplicaActivationError> {
        let now = self.clock.now();
        challenge.validate_at(now)?;
        let token_digest = activation_token_digest(activation_token)?;
        if token_digest != challenge.activation_token_digest {
            return Err(GatewayReplicaActivationError::CredentialRejected);
        }
        let record = self
            .repository
            .get_replica_by_activation_token_digest(&token_digest)
            .await
            .map_err(GatewayReplicaActivationError::Registry)?
            .ok_or(GatewayReplicaActivationError::CredentialRejected)?;
        if record.gateway_replica_id != challenge.gateway_replica_id
            || record.gateway_pool_id != challenge.gateway_pool_id
            || record.edge_cluster_id != challenge.edge_cluster_id
        {
            return Err(GatewayReplicaActivationError::CredentialRejected);
        }
        self.validate_pending_credential(&record, now).await?;

        let signing_bytes = challenge.signing_bytes()?;
        proof
            .verify(&signing_bytes)
            .map_err(|_| GatewayReplicaActivationError::ProofInvalid)?;

        let identity = WorkloadIdentityUri::gateway_replica(
            &self.trust_domain,
            record.edge_cluster_id.clone(),
            record.gateway_pool_id.clone(),
            record.gateway_replica_id.clone(),
        )
        .map_err(GatewayReplicaActivationError::Configuration)?;
        let server_names = self.gateway_server_names(&record).await?;
        let certificate_request = WorkloadCertificateRequest::new_with_server_names(
            challenge.request_id.clone(),
            identity,
            CertificateGeneration::new(1),
            proof.public_key_spki.clone(),
            now,
            server_names,
        )
        .map_err(GatewayReplicaActivationError::Configuration)?;
        let certificate = self
            .issuance
            .issue(certificate_request)
            .await
            .map_err(GatewayReplicaActivationError::CertificateIssuer)?;
        validate_issued_workload_certificate(&certificate, now)
            .map_err(|_| GatewayReplicaActivationError::CertificateMaterialInvalid)?;

        let persisted_certificate = self
            .persisted_certificate(&certificate)
            .map_err(|_| GatewayReplicaActivationError::CertificateMaterialInvalid)?;
        let expected_resource_version = record.resource_version.get();
        let next_resource_version = expected_resource_version
            .checked_add(1)
            .ok_or(GatewayReplicaActivationError::ResourceVersionOverflow)?;
        let mut next = record;
        // All certificate metadata is written in the same CAS as the prepared payload.  A
        // concurrent prepare therefore cannot issue a second durable certificate for this token.
        next.credential.public_key_fingerprint = Some(proof.public_key_spki.fingerprint());
        next.credential.certificate_generation = Some(CertificateGeneration::new(1));
        next.credential.certificate_fingerprint = Some(certificate.leaf_certificate_fingerprint());
        next.credential.certificate_not_after_unix_ms =
            Some(certificate.request().not_after_unix_ms());
        next.credential.certificate = Some(persisted_certificate);
        next.credential.state = GatewayCredentialState::PendingCertificateDelivery;
        next.resource_version = neoengram_protocol::ResourceVersion::new(next_resource_version);
        next.updated_at_unix_ms = now;
        let replica = self
            .repository
            .replace_replica(expected_resource_version, next)
            .await
            .map_err(|error| {
                if error.code() == CentralErrorCode::ConcurrentUpdate {
                    GatewayReplicaActivationError::ConcurrentActivation
                } else {
                    GatewayReplicaActivationError::Registry(error)
                }
            })?;
        Ok(GatewayReplicaActivationResult {
            replica,
            certificate,
        })
    }

    /// Commits a prepared activation only after the Replica has acknowledged certificate
    /// delivery.  This is a second ResourceVersion CAS and consumes the token atomically with the
    /// Pending -> Active transition.  Exact duplicate commits are idempotent; a different
    /// certificate or request is fenced.
    pub async fn commit(
        &self,
        prepared: &GatewayReplicaActivationResult,
    ) -> Result<GatewayReplicaActivationResult, GatewayReplicaActivationError> {
        let replica_id = &prepared.replica.gateway_replica_id;
        let current = self
            .repository
            .get_replica(replica_id)
            .await
            .map_err(GatewayReplicaActivationError::Registry)?
            .ok_or(GatewayReplicaActivationError::CredentialRejected)?;
        let persisted = current
            .credential
            .certificate
            .as_ref()
            .ok_or(GatewayReplicaActivationError::CredentialRejected)?;
        let expected_persisted = self
            .persisted_certificate(&prepared.certificate)
            .map_err(|_| GatewayReplicaActivationError::CertificateMaterialInvalid)?;
        if persisted != &expected_persisted {
            return Err(GatewayReplicaActivationError::ConcurrentActivation);
        }
        let now = self.clock.now();
        // Certificate material is prepared before the network delivery.  Do not let a delayed
        // ACK turn an already-expired leaf into an Active credential, and do not report an
        // expired Active replay as a successful idempotent activation.
        if now.get() < persisted.not_before_unix_ms.get()
            || now.get() >= persisted.not_after_unix_ms.get()
        {
            return Err(GatewayReplicaActivationError::CertificateMaterialInvalid);
        }
        if current.state == GatewayReplicaState::Active
            && current.credential.state == GatewayCredentialState::Active
        {
            return Ok(GatewayReplicaActivationResult {
                replica: current,
                certificate: prepared.certificate.clone(),
            });
        }
        if current.state != GatewayReplicaState::Pending
            || current.credential.state != GatewayCredentialState::PendingCertificateDelivery
            || current.credential.activation_consumed_at_unix_ms.is_some()
        {
            return Err(GatewayReplicaActivationError::ConcurrentActivation);
        }
        let next_resource_version = current
            .resource_version
            .get()
            .checked_add(1)
            .ok_or(GatewayReplicaActivationError::ResourceVersionOverflow)?;
        let mut next = current.clone();
        next.state = GatewayReplicaState::Active;
        next.credential.state = GatewayCredentialState::Active;
        next.credential.activation_consumed_at_unix_ms = Some(now);
        next.resource_version = neoengram_protocol::ResourceVersion::new(next_resource_version);
        next.updated_at_unix_ms = now;
        let replica = match self
            .repository
            .replace_replica(current.resource_version.get(), next)
            .await
        {
            Ok(replica) => replica,
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                let latest = self
                    .repository
                    .get_replica(replica_id)
                    .await
                    .map_err(GatewayReplicaActivationError::Registry)?
                    .ok_or(GatewayReplicaActivationError::ConcurrentActivation)?;
                if latest.state != GatewayReplicaState::Active
                    || latest.credential.state != GatewayCredentialState::Active
                    || latest.credential.certificate.as_ref() != Some(&expected_persisted)
                {
                    return Err(GatewayReplicaActivationError::ConcurrentActivation);
                }
                latest
            }
            Err(error) => return Err(GatewayReplicaActivationError::Registry(error)),
        };
        Ok(GatewayReplicaActivationResult {
            replica,
            certificate: prepared.certificate.clone(),
        })
    }

    fn persisted_certificate(
        &self,
        certificate: &IssuedWorkloadCertificate,
    ) -> Result<GatewayReplicaCertificateRecord, GatewayReplicaActivationError> {
        let leaf_certificate_der = GatewayOpaqueBytes::new(certificate.leaf_certificate_der())
            .map_err(|_| GatewayReplicaActivationError::CertificateMaterialInvalid)?;
        let issuer_chain_der = certificate
            .issuer_chain_der()
            .iter()
            .cloned()
            .map(GatewayOpaqueBytes::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| GatewayReplicaActivationError::CertificateMaterialInvalid)?;
        Ok(GatewayReplicaCertificateRecord {
            request_id: certificate.request().request_id().clone(),
            public_key_spki: certificate.request().public_key_spki().clone(),
            certificate_generation: certificate.request().certificate_generation(),
            not_before_unix_ms: certificate.request().not_before_unix_ms(),
            not_after_unix_ms: certificate.request().not_after_unix_ms(),
            server_names: certificate.request().server_names().clone(),
            leaf_certificate_der,
            issuer_chain_der,
        })
    }

    async fn gateway_server_names(
        &self,
        record: &GatewayReplicaRecord,
    ) -> Result<BTreeSet<String>, GatewayReplicaActivationError> {
        let pool = self
            .repository
            .get_pool(&record.gateway_pool_id)
            .await
            .map_err(GatewayReplicaActivationError::Registry)?
            .ok_or(GatewayReplicaActivationError::CredentialRejected)?;
        if pool.edge_cluster_id != record.edge_cluster_id {
            return Err(GatewayReplicaActivationError::CredentialRejected);
        }
        let endpoints = [
            pool.agent_endpoint.as_str(),
            record.control_endpoint.as_str(),
            record.peer_endpoint.as_str(),
            record.bootstrap_endpoint.as_str(),
        ];
        let mut names = BTreeSet::new();
        for endpoint in endpoints
            .into_iter()
            .chain(pool.s3_endpoint.iter().map(String::as_str))
        {
            let url = Url::parse(endpoint)
                .map_err(|_| GatewayReplicaActivationError::CertificateMaterialInvalid)?;
            let host = url
                .host()
                .ok_or(GatewayReplicaActivationError::CertificateMaterialInvalid)?;
            names.insert(match host {
                url::Host::Domain(domain) => domain.to_owned(),
                url::Host::Ipv4(address) => address.to_string(),
                url::Host::Ipv6(address) => address.to_string(),
            });
        }
        if names.is_empty() {
            return Err(GatewayReplicaActivationError::CertificateMaterialInvalid);
        }
        Ok(names)
    }

    fn issued_certificate_from_record(
        &self,
        record: &GatewayReplicaRecord,
        persisted: &GatewayReplicaCertificateRecord,
    ) -> Result<IssuedWorkloadCertificate, WorkloadPkiError> {
        let identity = WorkloadIdentityUri::gateway_replica(
            &self.trust_domain,
            record.edge_cluster_id.clone(),
            record.gateway_pool_id.clone(),
            record.gateway_replica_id.clone(),
        )?;
        let lifetime_ms = persisted
            .not_after_unix_ms
            .get()
            .checked_sub(persisted.not_before_unix_ms.get())
            .ok_or(WorkloadPkiError::InvalidValidityWindow)?;
        let request = WorkloadCertificateRequest::with_lifetime_and_server_names(
            persisted.request_id.clone(),
            identity,
            persisted.certificate_generation,
            persisted.public_key_spki.clone(),
            persisted.not_before_unix_ms,
            lifetime_ms,
            persisted.server_names.clone(),
        )?;
        IssuedWorkloadCertificate::from_request(
            request,
            persisted.leaf_certificate_der.as_bytes().to_vec(),
            persisted
                .issuer_chain_der
                .iter()
                .map(|certificate| certificate.as_bytes().to_vec())
                .collect(),
        )
    }

    async fn validate_pending_credential(
        &self,
        record: &GatewayReplicaRecord,
        now: UnixMillis,
    ) -> Result<(), GatewayReplicaActivationError> {
        if record.state != GatewayReplicaState::Pending
            || record.credential.state != GatewayCredentialState::PendingActivation
            || record.credential.activation_consumed_at_unix_ms.is_some()
            || record.credential.certificate.is_some()
        {
            return Err(GatewayReplicaActivationError::CredentialRejected);
        }
        if now.get() < record.credential.activation_created_at_unix_ms.get()
            || now.get() >= record.credential.activation_expires_at_unix_ms.get()
        {
            // Persist Expired where possible.  A concurrent activation/revocation is already a
            // terminal failure, so its CAS result is intentionally not exposed to the caller.
            let mut expired = record.clone();
            expired.credential.state = GatewayCredentialState::Expired;
            if let Some(next_resource_version) = record.resource_version.get().checked_add(1) {
                expired.resource_version =
                    neoengram_protocol::ResourceVersion::new(next_resource_version);
                expired.updated_at_unix_ms = now;
                match self
                    .repository
                    .replace_replica(record.resource_version.get(), expired)
                    .await
                {
                    Ok(_) | Err(CentralError { .. }) => {}
                }
            }
            return Err(GatewayReplicaActivationError::CredentialRejected);
        }
        Ok(())
    }
}

fn activation_token_digest(
    activation_token: &str,
) -> Result<ContentDigest, GatewayReplicaActivationError> {
    if !activation_token.starts_with(ACTIVATION_TOKEN_PREFIX)
        || activation_token.len() > ACTIVATION_TOKEN_PREFIX.len() + 128
        || activation_token
            .bytes()
            .skip(ACTIVATION_TOKEN_PREFIX.len())
            .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'_' && byte != b'-')
    {
        return Err(GatewayReplicaActivationError::CredentialRejected);
    }
    let encoded = &activation_token[ACTIVATION_TOKEN_PREFIX.len()..];
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .map_err(|_| GatewayReplicaActivationError::CredentialRejected)?;
    if decoded.len() != 32 || URL_SAFE_NO_PAD.encode(&decoded) != encoded {
        return Err(GatewayReplicaActivationError::CredentialRejected);
    }
    Ok(ContentDigest::hash(activation_token.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use neoengram_protocol::{
        ContentDigest, Ed25519PublicKeySpki, Ed25519Signature, Extensions, GatewayPoolId,
        PrincipalId, PrincipalKind, PrincipalRef, ProtocolVersion, ResourceVersion,
    };
    use neoengramd::{
        GatewayPoolRecord, GatewayPoolState, GatewayReplicaCredential, InMemoryClock,
        InMemoryGatewayRegistry,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tokio::sync::Barrier;

    use super::*;

    const TOKEN: &str = "nggw_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[derive(Clone)]
    struct EchoIssuer {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl WorkloadCertificateIssuer for EchoIssuer {
        async fn issue(
            &self,
            request: WorkloadCertificateRequest,
        ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::service::test_issue_workload_certificate(request))
        }
    }

    #[derive(Clone)]
    struct InvalidDerIssuer;

    #[async_trait]
    impl WorkloadCertificateIssuer for InvalidDerIssuer {
        async fn issue(
            &self,
            request: WorkloadCertificateRequest,
        ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
            Ok(
                IssuedWorkloadCertificate::from_request(request, vec![0x01], vec![vec![0x02]])
                    .expect("non-empty opaque certificate bytes pass constructor size checks"),
            )
        }
    }

    #[derive(Clone)]
    struct BarrierIssuer {
        calls: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
    }

    #[async_trait]
    impl WorkloadCertificateIssuer for BarrierIssuer {
        async fn issue(
            &self,
            request: WorkloadCertificateRequest,
        ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.barrier.wait().await;
            Ok(crate::service::test_issue_workload_certificate(request))
        }
    }

    fn key() -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap()
    }

    fn pool() -> GatewayPoolRecord {
        let actor = PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("system").unwrap(),
            extensions: Extensions::new(),
        };
        GatewayPoolRecord {
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            display_name: "pool".to_owned(),
            agent_endpoint: "https://gateway.example.test".to_owned(),
            s3_endpoint: None,
            desired_replicas: 1,
            minimum_ready_replicas: 1,
            state: GatewayPoolState::Provisioning,
            config_generation: neoengram_protocol::Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
            created_by: actor.clone(),
            updated_by: actor,
        }
    }

    fn replica() -> GatewayReplicaRecord {
        GatewayReplicaRecord {
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            control_endpoint: "https://control.gateway.example.test".to_owned(),
            peer_endpoint: "https://peer.gateway.example.test".to_owned(),
            bootstrap_endpoint: "https://bootstrap.gateway.example.test".to_owned(),
            software_version: "test".to_owned(),
            supported_protocol_versions: [ProtocolVersion::V1].into_iter().collect(),
            capabilities: ["agent_edge".to_owned()].into_iter().collect(),
            last_heartbeat_at_unix_ms: None,
            state: GatewayReplicaState::Pending,
            credential: GatewayReplicaCredential {
                activation_token_digest: ContentDigest::hash(TOKEN.as_bytes()),
                activation_created_at_unix_ms: UnixMillis::new(1_000),
                activation_expires_at_unix_ms: UnixMillis::new(1_000 + 900_000),
                activation_consumed_at_unix_ms: None,
                public_key_fingerprint: None,
                certificate_generation: None,
                certificate_fingerprint: None,
                certificate_not_after_unix_ms: None,
                certificate: None,
                state: GatewayCredentialState::PendingActivation,
            },
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        }
    }

    async fn fixture() -> (
        GatewayReplicaActivationService,
        Arc<InMemoryGatewayRegistry>,
        Arc<InMemoryClock>,
        Arc<AtomicUsize>,
    ) {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        repository.insert_pool(pool()).await.unwrap();
        repository.insert_replica(replica()).await.unwrap();
        let clock = Arc::new(InMemoryClock::new(2_000));
        let calls = Arc::new(AtomicUsize::new(0));
        let service = GatewayReplicaActivationService::new(
            repository.clone(),
            Arc::new(EchoIssuer {
                calls: calls.clone(),
            }),
            clock.clone(),
            "mesh.example.test",
        );
        (service, repository, clock, calls)
    }

    fn proof(challenge: &GatewayReplicaActivationChallenge) -> GatewayReplicaActivationProof {
        let key = key();
        let public_key = Ed25519PublicKeySpki::from_public_key_bytes(
            key.public_key().as_ref().try_into().unwrap(),
        );
        let signature = Ed25519Signature::new(
            key.sign(&challenge.signing_bytes().unwrap())
                .as_ref()
                .to_vec(),
        )
        .unwrap();
        GatewayReplicaActivationProof::new(public_key, signature)
    }

    #[tokio::test]
    async fn activation_prepare_deliver_commit_activates_replica() {
        let (service, repository, _clock, calls) = fixture().await;
        let challenge = service.issue_challenge(TOKEN).await.unwrap();
        let prepared = service
            .prepare(&challenge, TOKEN, &proof(&challenge))
            .await
            .unwrap();
        assert_eq!(prepared.replica.state, GatewayReplicaState::Pending);
        assert_eq!(
            prepared.replica.credential.state,
            GatewayCredentialState::PendingCertificateDelivery
        );
        assert_eq!(prepared.replica.resource_version.get(), 2);
        assert!(prepared
            .replica
            .credential
            .activation_consumed_at_unix_ms
            .is_none());
        let result = service.commit(&prepared).await.unwrap();
        assert_eq!(result.replica.state, GatewayReplicaState::Active);
        assert_eq!(
            result.replica.credential.state,
            GatewayCredentialState::Active
        );
        assert_eq!(result.replica.resource_version.get(), 3);
        assert_eq!(
            result
                .replica
                .credential
                .certificate_generation
                .unwrap()
                .get(),
            1
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let stored = repository
            .get_replica(&GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, result.replica);
        let replayed = service.commit(&prepared).await.unwrap();
        assert_eq!(replayed.replica, result.replica);
    }

    #[tokio::test]
    async fn commit_rejects_a_prepared_certificate_after_its_validity_window() {
        let (service, _repository, clock, _calls) = fixture().await;
        let challenge = service.issue_challenge(TOKEN).await.unwrap();
        let prepared = service
            .prepare(&challenge, TOKEN, &proof(&challenge))
            .await
            .unwrap();
        let not_after = prepared
            .replica
            .credential
            .certificate_not_after_unix_ms
            .expect("prepare persists certificate expiry")
            .get();
        clock.set(not_after);

        assert!(matches!(
            service.commit(&prepared).await,
            Err(GatewayReplicaActivationError::CertificateMaterialInvalid)
        ));

        clock.set(
            prepared
                .replica
                .credential
                .certificate
                .as_ref()
                .expect("prepared certificate payload")
                .not_before_unix_ms
                .get()
                .saturating_sub(1),
        );
        assert!(matches!(
            service.commit(&prepared).await,
            Err(GatewayReplicaActivationError::CertificateMaterialInvalid)
        ));
    }

    #[tokio::test]
    async fn prepared_certificate_survives_delivery_failure_and_reuses_issuer_result() {
        let (service, repository, clock, calls) = fixture().await;
        let challenge = service.issue_challenge(TOKEN).await.unwrap();
        let first = service
            .prepare(&challenge, TOKEN, &proof(&challenge))
            .await
            .unwrap();
        let stored = repository
            .get_replica(&GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, GatewayReplicaState::Pending);
        assert!(stored.credential.activation_consumed_at_unix_ms.is_none());
        assert!(stored.credential.certificate.is_some());

        // A retry after the transport has failed reconstructs the exact CA response from the
        // Registry; it does not issue a second certificate or require a second proof exchange.
        let resumed = service
            .resume_prepared(TOKEN)
            .await
            .unwrap()
            .expect("pending delivery must be recoverable");
        assert_eq!(resumed.certificate, first.certificate);
        assert_eq!(resumed.replica, first.replica);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Token expiry gates new proofs, not recovery of a certificate prepared while the token
        // was valid. Delivery can continue until the issued leaf itself expires.
        clock.set(901_001);
        let resumed = service
            .resume_prepared(TOKEN)
            .await
            .unwrap()
            .expect("prepared delivery survives activation-token expiry");
        let committed = service.commit(&resumed).await.unwrap();
        assert_eq!(committed.replica.state, GatewayReplicaState::Active);
        assert_eq!(committed.replica.resource_version.get(), 3);
    }

    #[tokio::test]
    async fn invalid_issuer_certificate_material_is_rejected_without_registry_mutation() {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        repository.insert_pool(pool()).await.unwrap();
        let original = replica();
        repository.insert_replica(original.clone()).await.unwrap();
        let service = GatewayReplicaActivationService::new(
            repository.clone(),
            Arc::new(InvalidDerIssuer),
            Arc::new(InMemoryClock::new(2_000)),
            "mesh.example.test",
        );
        let challenge = service.issue_challenge(TOKEN).await.unwrap();

        assert!(matches!(
            service.prepare(&challenge, TOKEN, &proof(&challenge)).await,
            Err(GatewayReplicaActivationError::CertificateMaterialInvalid)
        ));

        let stored = repository
            .get_replica(&GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, original);
        assert_eq!(
            stored.credential.state,
            GatewayCredentialState::PendingActivation
        );
        assert_eq!(stored.resource_version, ResourceVersion::new(1));
        assert!(stored.credential.certificate.is_none());
    }

    #[tokio::test]
    async fn token_replay_and_consumed_challenge_fail_before_issuer() {
        let (service, _repository, _clock, calls) = fixture().await;
        let challenge = service.issue_challenge(TOKEN).await.unwrap();
        service
            .prepare(&challenge, TOKEN, &proof(&challenge))
            .await
            .unwrap();
        let prepared = service
            .resume_prepared(TOKEN)
            .await
            .unwrap()
            .expect("prepared certificate remains recoverable");
        service.commit(&prepared).await.unwrap();
        assert!(matches!(
            service.issue_challenge(TOKEN).await,
            Err(GatewayReplicaActivationError::CredentialRejected)
        ));
        assert!(matches!(
            service.prepare(&challenge, TOKEN, &proof(&challenge)).await,
            Err(GatewayReplicaActivationError::CredentialRejected)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalid_proof_and_expired_challenge_are_fail_closed() {
        let (service, _repository, clock, calls) = fixture().await;
        let challenge = service.issue_challenge(TOKEN).await.unwrap();
        let mut bad = proof(&challenge);
        bad.signature = Ed25519Signature::from_bytes([0; 64]);
        assert!(matches!(
            service.prepare(&challenge, TOKEN, &bad).await,
            Err(GatewayReplicaActivationError::ProofInvalid)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        clock.set(challenge.expires_at_unix_ms().get());
        assert!(matches!(
            service.prepare(&challenge, TOKEN, &proof(&challenge)).await,
            Err(GatewayReplicaActivationError::ChallengeExpired)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn expired_token_is_marked_expired_and_cannot_issue_challenge() {
        let (service, repository, clock, _calls) = fixture().await;
        clock.set(1_000 + 900_000);
        assert!(matches!(
            service.issue_challenge(TOKEN).await,
            Err(GatewayReplicaActivationError::CredentialRejected)
        ));
        let stored = repository
            .get_replica(&GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.credential.state, GatewayCredentialState::Expired);
        assert_eq!(stored.resource_version.get(), 2);
    }

    #[tokio::test]
    async fn challenge_is_bound_to_token_and_replica_identity() {
        let (service, _repository, _clock, calls) = fixture().await;
        let challenge = service.issue_challenge(TOKEN).await.unwrap();
        let wire_challenge = neoengram_protocol::GatewayBootstrapChallenge {
            request_id: challenge.request_id().clone(),
            edge_cluster_id: challenge.edge_cluster_id().clone(),
            gateway_pool_id: challenge.gateway_pool_id().clone(),
            gateway_replica_id: challenge.gateway_replica_id().clone(),
            activation_token_digest: challenge.activation_token_digest(),
            nonce: challenge.nonce().to_owned(),
            issued_at_unix_ms: challenge.issued_at_unix_ms(),
            expires_at_unix_ms: challenge.expires_at_unix_ms(),
        };
        assert_eq!(
            challenge.signing_bytes().unwrap(),
            wire_challenge.signing_bytes().unwrap()
        );
        let other_token = "nggw_v1_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
        assert!(matches!(
            service
                .prepare(&challenge, other_token, &proof(&challenge))
                .await,
            Err(GatewayReplicaActivationError::CredentialRejected)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_activation_is_fenced_by_replica_resource_version() {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        repository.insert_pool(pool()).await.unwrap();
        repository.insert_replica(replica()).await.unwrap();
        let clock = Arc::new(InMemoryClock::new(2_000));
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(GatewayReplicaActivationService::new(
            repository.clone(),
            Arc::new(BarrierIssuer {
                calls: calls.clone(),
                barrier: Arc::new(Barrier::new(2)),
            }),
            clock,
            "mesh.example.test",
        ));
        let challenge = service.issue_challenge(TOKEN).await.unwrap();
        let activation_proof = proof(&challenge);

        let first = {
            let service = service.clone();
            let challenge = challenge.clone();
            let activation_proof = activation_proof.clone();
            tokio::spawn(async move { service.prepare(&challenge, TOKEN, &activation_proof).await })
        };
        let second = {
            let service = service.clone();
            let challenge = challenge.clone();
            let activation_proof = activation_proof.clone();
            tokio::spawn(async move { service.prepare(&challenge, TOKEN, &activation_proof).await })
        };
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert_eq!(
            usize::from(matches!(
                &first,
                Err(GatewayReplicaActivationError::ConcurrentActivation)
            )) + usize::from(matches!(
                &second,
                Err(GatewayReplicaActivationError::ConcurrentActivation)
            )),
            1
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let prepared = first
            .as_ref()
            .ok()
            .or_else(|| second.as_ref().ok())
            .unwrap();
        service.commit(prepared).await.unwrap();
        let stored = repository
            .get_replica(&GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, GatewayReplicaState::Active);
        assert_eq!(stored.credential.state, GatewayCredentialState::Active);
    }
}
