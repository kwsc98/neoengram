use std::{collections::BTreeMap, fmt, sync::Arc};

use async_trait::async_trait;
use neoengram_domain::protocol::materialization::{
    MaterializationBatchTicket, SignedMaterializationBatchTicket,
};
use neoengram_domain::protocol::{
    CentralSignedPayload, CertificateGeneration, ContentDigest, Ed25519PublicKeySpki,
    Ed25519Signature, Extensions, GatewayOpaqueBytes, SignedTransferTicket, TransferTicket,
    UnixMillis,
};
use thiserror::Error;

/// Default validity of a Central command signature.
pub const DEFAULT_CENTRAL_COMMAND_TTL_MS: u64 = 30_000;
/// Hard limit preventing a delayed command from remaining replayable indefinitely.
pub const MAX_CENTRAL_COMMAND_TTL_MS: u64 = 5 * 60 * 1_000;
const MAX_KEY_ID_BYTES: usize = 128;

/// Stable, non-secret alias for one external Central command-signing key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CentralCommandKeyId(String);

impl CentralCommandKeyId {
    pub fn new(value: impl Into<String>) -> Result<Self, CentralCommandSecurityError> {
        let value = value.into();
        let mut bytes = value.bytes();
        let valid = !value.is_empty()
            && value.len() <= MAX_KEY_ID_BYTES
            && bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && bytes.all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            });
        if !valid {
            return Err(CentralCommandSecurityError::InvalidKeyId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CentralCommandKeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Lifecycle state published in an Agent's Central command trust bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CentralCommandKeyState {
    /// May sign and verify new commands.
    Active,
    /// May verify commands during rotation but cannot sign new commands.
    Retiring,
    /// Must fail closed even for a signature that is otherwise cryptographically valid.
    Revoked,
}

/// One generation-bound public verification key in the Agent trust bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentralCommandVerificationKey {
    key_id: CentralCommandKeyId,
    certificate_generation: CertificateGeneration,
    public_key_spki: Ed25519PublicKeySpki,
    state: CentralCommandKeyState,
}

impl CentralCommandVerificationKey {
    pub fn new(
        key_id: CentralCommandKeyId,
        certificate_generation: CertificateGeneration,
        public_key_spki: Ed25519PublicKeySpki,
        state: CentralCommandKeyState,
    ) -> Result<Self, CentralCommandSecurityError> {
        if certificate_generation.get() == 0 {
            return Err(CentralCommandSecurityError::InvalidKeyGeneration);
        }
        Ok(Self {
            key_id,
            certificate_generation,
            public_key_spki,
            state,
        })
    }

    #[must_use]
    pub fn key_id(&self) -> &CentralCommandKeyId {
        &self.key_id
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
    pub const fn state(&self) -> CentralCommandKeyState {
        self.state
    }
}

/// Immutable selection and verification view distributed to Agents.
#[derive(Debug, Clone)]
pub struct CentralCommandTrustBundle {
    keys: BTreeMap<(CentralCommandKeyId, u64), CentralCommandVerificationKey>,
}

impl CentralCommandTrustBundle {
    pub fn new(
        keys: Vec<CentralCommandVerificationKey>,
    ) -> Result<Self, CentralCommandSecurityError> {
        if keys.is_empty() {
            return Err(CentralCommandSecurityError::EmptyTrustBundle);
        }
        let mut indexed = BTreeMap::new();
        for key in keys {
            let index = (key.key_id.clone(), key.certificate_generation.get());
            if indexed.insert(index, key).is_some() {
                return Err(CentralCommandSecurityError::DuplicateTrustKey);
            }
        }
        Ok(Self { keys: indexed })
    }

    /// Selects only an exact `key_id` and generation pair and rejects revoked material.
    pub fn select(
        &self,
        key_id: &str,
        certificate_generation: CertificateGeneration,
    ) -> Result<&CentralCommandVerificationKey, CentralCommandSecurityError> {
        let parsed_key_id = CentralCommandKeyId::new(key_id)?;
        if let Some(key) = self
            .keys
            .get(&(parsed_key_id.clone(), certificate_generation.get()))
        {
            if key.state == CentralCommandKeyState::Revoked {
                return Err(CentralCommandSecurityError::RevokedKey);
            }
            return Ok(key);
        }
        if self
            .keys
            .keys()
            .any(|(candidate, _)| candidate == &parsed_key_id)
        {
            Err(CentralCommandSecurityError::KeyGenerationMismatch)
        } else {
            Err(CentralCommandSecurityError::UnknownKey)
        }
    }

    /// Selects the exact trust-bundle key before verifying integrity and the signed TTL window.
    pub fn verify_at(
        &self,
        payload: &CentralSignedPayload,
        now_unix_ms: UnixMillis,
    ) -> Result<&CentralCommandVerificationKey, CentralCommandSecurityError> {
        let key = self.select(&payload.key_id, payload.certificate_generation)?;
        payload
            .verify_at(&key.public_key_spki, now_unix_ms)
            .map_err(|error| {
                CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
            })?;
        Ok(key)
    }

    fn select_for_signing(
        &self,
        key_id: &CentralCommandKeyId,
        certificate_generation: CertificateGeneration,
    ) -> Result<&CentralCommandVerificationKey, CentralCommandSecurityError> {
        let key = self.select(key_id.as_str(), certificate_generation)?;
        if key.state != CentralCommandKeyState::Active {
            return Err(CentralCommandSecurityError::SigningKeyNotActive);
        }
        Ok(key)
    }
}

/// Exact canonical bytes and key selection passed to an external HSM signer.
#[derive(Clone, PartialEq, Eq)]
pub struct CentralCommandSignatureRequest {
    key_id: CentralCommandKeyId,
    certificate_generation: CertificateGeneration,
    signed_at_unix_ms: UnixMillis,
    expires_at_unix_ms: UnixMillis,
    payload_digest: ContentDigest,
    signing_bytes: Vec<u8>,
}

impl CentralCommandSignatureRequest {
    #[must_use]
    pub fn key_id(&self) -> &CentralCommandKeyId {
        &self.key_id
    }

    #[must_use]
    pub const fn certificate_generation(&self) -> CertificateGeneration {
        self.certificate_generation
    }

    #[must_use]
    pub const fn signed_at_unix_ms(&self) -> UnixMillis {
        self.signed_at_unix_ms
    }

    #[must_use]
    pub const fn expires_at_unix_ms(&self) -> UnixMillis {
        self.expires_at_unix_ms
    }

    #[must_use]
    pub fn ttl_ms(&self) -> u64 {
        self.expires_at_unix_ms.get() - self.signed_at_unix_ms.get()
    }

    #[must_use]
    pub const fn payload_digest(&self) -> ContentDigest {
        self.payload_digest
    }

    #[must_use]
    pub fn signing_bytes(&self) -> &[u8] {
        &self.signing_bytes
    }
}

impl fmt::Debug for CentralCommandSignatureRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CentralCommandSignatureRequest")
            .field("key_id", &self.key_id)
            .field("certificate_generation", &self.certificate_generation)
            .field("signed_at_unix_ms", &self.signed_at_unix_ms)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("payload_digest", &self.payload_digest)
            .field("signing_bytes_length", &self.signing_bytes.len())
            .finish()
    }
}

/// Generation-bound Ed25519 result returned by the external signing adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentralCommandSignature {
    key_id: CentralCommandKeyId,
    certificate_generation: CertificateGeneration,
    signature: Ed25519Signature,
}

impl CentralCommandSignature {
    pub fn new(
        key_id: CentralCommandKeyId,
        certificate_generation: CertificateGeneration,
        signature: Ed25519Signature,
    ) -> Result<Self, CentralCommandSecurityError> {
        if certificate_generation.get() == 0 {
            return Err(CentralCommandSecurityError::InvalidKeyGeneration);
        }
        Ok(Self {
            key_id,
            certificate_generation,
            signature,
        })
    }

    #[must_use]
    pub fn key_id(&self) -> &CentralCommandKeyId {
        &self.key_id
    }

    #[must_use]
    pub const fn certificate_generation(&self) -> CertificateGeneration {
        self.certificate_generation
    }

    #[must_use]
    pub fn signature(&self) -> &Ed25519Signature {
        &self.signature
    }
}

/// Port implemented by an external KMS/HSM-backed Ed25519 signer.
///
/// No local or file-backed private-key implementation is provided by `neoengram-central`.
#[async_trait]
pub trait CentralCommandSigner: Send + Sync {
    async fn sign(
        &self,
        request: CentralCommandSignatureRequest,
    ) -> Result<CentralCommandSignature, CentralCommandSignerError>;
}

/// Central facade that constructs the complete signed envelope and validates the HSM response.
#[derive(Clone)]
pub struct CentralCommandKeyring {
    signer: Arc<dyn CentralCommandSigner>,
    trust_bundle: CentralCommandTrustBundle,
    active_key_id: CentralCommandKeyId,
    active_certificate_generation: CertificateGeneration,
}

impl CentralCommandKeyring {
    pub fn new(
        signer: Arc<dyn CentralCommandSigner>,
        trust_bundle: CentralCommandTrustBundle,
        active_key_id: CentralCommandKeyId,
        active_certificate_generation: CertificateGeneration,
    ) -> Result<Self, CentralCommandSecurityError> {
        trust_bundle.select_for_signing(&active_key_id, active_certificate_generation)?;
        Ok(Self {
            signer,
            trust_bundle,
            active_key_id,
            active_certificate_generation,
        })
    }

    pub async fn sign(
        &self,
        payload: GatewayOpaqueBytes,
        signed_at_unix_ms: UnixMillis,
    ) -> Result<CentralSignedPayload, CentralCommandSecurityError> {
        self.sign_with_ttl_ms(payload, signed_at_unix_ms, DEFAULT_CENTRAL_COMMAND_TTL_MS)
            .await
    }

    pub async fn sign_with_ttl_ms(
        &self,
        payload: GatewayOpaqueBytes,
        signed_at_unix_ms: UnixMillis,
        ttl_ms: u64,
    ) -> Result<CentralSignedPayload, CentralCommandSecurityError> {
        if signed_at_unix_ms.get() == 0 {
            return Err(CentralCommandSecurityError::InvalidSigningTime);
        }
        if ttl_ms == 0 || ttl_ms > MAX_CENTRAL_COMMAND_TTL_MS {
            return Err(CentralCommandSecurityError::InvalidTtl);
        }
        let expires_at_unix_ms = signed_at_unix_ms
            .get()
            .checked_add(ttl_ms)
            .map(UnixMillis::new)
            .ok_or(CentralCommandSecurityError::InvalidSigningTime)?;
        let verification_key = self
            .trust_bundle
            .select_for_signing(&self.active_key_id, self.active_certificate_generation)?
            .clone();
        let payload_digest = ContentDigest::hash(payload.as_bytes());
        let mut signed = CentralSignedPayload {
            key_id: self.active_key_id.as_str().to_owned(),
            certificate_generation: self.active_certificate_generation,
            signed_at_unix_ms,
            expires_at_unix_ms,
            payload_digest,
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions: Extensions::new(),
        };
        let signing_bytes = signed.signing_bytes().map_err(|error| {
            CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
        })?;
        let result = self
            .signer
            .sign(CentralCommandSignatureRequest {
                key_id: self.active_key_id.clone(),
                certificate_generation: self.active_certificate_generation,
                signed_at_unix_ms,
                expires_at_unix_ms,
                payload_digest,
                signing_bytes,
            })
            .await?;
        if result.key_id != self.active_key_id
            || result.certificate_generation != self.active_certificate_generation
        {
            return Err(CentralCommandSecurityError::SignerResponseMismatch);
        }
        signed.signature = result.signature;
        signed
            .verify(verification_key.public_key_spki())
            .map_err(|_| CentralCommandSecurityError::SignerResponseMismatch)?;
        Ok(signed)
    }

    /// Signs one immutable transfer capability using the same key lifecycle and TTL checks as
    /// Central-to-Agent control commands. The ticket bytes are the signed payload, so a receiver
    /// can reject any endpoint, generation, ObjectSet, or byte-limit mutation before opening QUIC.
    pub async fn sign_transfer_ticket(
        &self,
        ticket: TransferTicket,
        signed_at_unix_ms: UnixMillis,
        ttl_ms: u64,
    ) -> Result<SignedTransferTicket, CentralCommandSecurityError> {
        let payload = SignedTransferTicket::payload_bytes(&ticket).map_err(|error| {
            CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
        })?;
        let signed = self
            .sign_with_ttl_ms(
                GatewayOpaqueBytes::new(payload).map_err(|error| {
                    CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
                })?,
                signed_at_unix_ms,
                ttl_ms,
            )
            .await?;
        SignedTransferTicket::new(ticket, signed)
            .map_err(|error| CentralCommandSecurityError::InvalidSignedPayload(error.to_string()))
    }

    pub fn verify_transfer_ticket(
        &self,
        ticket: &SignedTransferTicket,
        now_unix_ms: UnixMillis,
    ) -> Result<(), CentralCommandSecurityError> {
        ticket.validate().map_err(|error| {
            CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
        })?;
        self.trust_bundle
            .verify_at(&ticket.central_signature, now_unix_ms)
            .map(|_| ())
    }

    /// Signs one v2 source-grouped materialization batch.  The ticket payload is the canonical
    /// domain-separated ticket bytes, so every Gateway/Agent hop can verify the exact manifest,
    /// placement and route fences without trusting Central-side mutable state.
    pub async fn sign_materialization_batch_ticket(
        &self,
        ticket: MaterializationBatchTicket,
        signed_at_unix_ms: UnixMillis,
        ttl_ms: u64,
    ) -> Result<SignedMaterializationBatchTicket, CentralCommandSecurityError> {
        let payload = ticket.payload_bytes().map_err(|error| {
            CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
        })?;
        let signed = self
            .sign_with_ttl_ms(
                GatewayOpaqueBytes::new(payload).map_err(|error| {
                    CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
                })?,
                signed_at_unix_ms,
                ttl_ms,
            )
            .await?;
        SignedMaterializationBatchTicket::new(ticket, signed)
            .map_err(|error| CentralCommandSecurityError::InvalidSignedPayload(error.to_string()))
    }

    /// Verifies a v2 materialization ticket against the configured Central trust bundle and time
    /// fence.  The domain validator also checks namespace, generation and capability bindings.
    pub fn verify_materialization_batch_ticket(
        &self,
        ticket: &SignedMaterializationBatchTicket,
        now_unix_ms: UnixMillis,
    ) -> Result<(), CentralCommandSecurityError> {
        ticket.validate().map_err(|error| {
            CentralCommandSecurityError::InvalidSignedPayload(error.to_string())
        })?;
        self.trust_bundle
            .verify_at(&ticket.central_signature, now_unix_ms)
            .map(|_| ())
    }

    #[must_use]
    pub fn trust_bundle(&self) -> &CentralCommandTrustBundle {
        &self.trust_bundle
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CentralCommandSignerError {
    #[error("Central command signer is unavailable: {0}")]
    Unavailable(String),
    #[error("Central command signing request was rejected: {0}")]
    Rejected(String),
    #[error("Central command signer failed: {0}")]
    Internal(String),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CentralCommandSecurityError {
    #[error("Central command key ID is not a canonical local alias")]
    InvalidKeyId,
    #[error("Central command key generation must be positive")]
    InvalidKeyGeneration,
    #[error("Central command trust bundle must contain at least one key")]
    EmptyTrustBundle,
    #[error("Central command trust bundle contains a duplicate key generation")]
    DuplicateTrustKey,
    #[error("Central command trust bundle does not contain the selected key")]
    UnknownKey,
    #[error("Central command trust bundle key generation does not match")]
    KeyGenerationMismatch,
    #[error("Central command trust bundle key is revoked")]
    RevokedKey,
    #[error("Central command signing key is not active")]
    SigningKeyNotActive,
    #[error("Central command signing time is invalid")]
    InvalidSigningTime,
    #[error("Central command TTL must be positive and no greater than five minutes")]
    InvalidTtl,
    #[error("external signer response does not match the requested key generation")]
    SignerResponseMismatch,
    #[error("Central signed payload is invalid: {0}")]
    InvalidSignedPayload(String),
    #[error(transparent)]
    Signer(#[from] CentralCommandSignerError),
}

#[cfg(test)]
mod tests {
    use ring::signature::{Ed25519KeyPair, KeyPair as _};

    use super::*;

    struct TestSigner {
        key_pair: Ed25519KeyPair,
        response_generation: CertificateGeneration,
    }

    #[async_trait]
    impl CentralCommandSigner for TestSigner {
        async fn sign(
            &self,
            request: CentralCommandSignatureRequest,
        ) -> Result<CentralCommandSignature, CentralCommandSignerError> {
            CentralCommandSignature::new(
                request.key_id().clone(),
                self.response_generation,
                Ed25519Signature::new(
                    self.key_pair
                        .sign(request.signing_bytes())
                        .as_ref()
                        .to_vec(),
                )
                .unwrap(),
            )
            .map_err(|error| CentralCommandSignerError::Internal(error.to_string()))
        }
    }

    fn test_key(seed: u8) -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
    }

    fn verification_key(
        key_pair: &Ed25519KeyPair,
        generation: u64,
        state: CentralCommandKeyState,
    ) -> CentralCommandVerificationKey {
        CentralCommandVerificationKey::new(
            CentralCommandKeyId::new("central-command-a").unwrap(),
            CertificateGeneration::new(generation),
            Ed25519PublicKeySpki::from_public_key_bytes(
                key_pair.public_key().as_ref().try_into().unwrap(),
            ),
            state,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn keyring_constructs_digest_ttl_and_verifiable_signature() {
        let signing_key = test_key(7);
        let trusted_key = verification_key(&signing_key, 4, CentralCommandKeyState::Active);
        let trust_bundle = CentralCommandTrustBundle::new(vec![trusted_key]).unwrap();
        let keyring = CentralCommandKeyring::new(
            Arc::new(TestSigner {
                key_pair: signing_key,
                response_generation: CertificateGeneration::new(4),
            }),
            trust_bundle,
            CentralCommandKeyId::new("central-command-a").unwrap(),
            CertificateGeneration::new(4),
        )
        .unwrap();
        let bytes = b"authoritative-command".to_vec();
        let signed = keyring
            .sign(
                GatewayOpaqueBytes::new(bytes.clone()).unwrap(),
                UnixMillis::new(10_000),
            )
            .await
            .unwrap();

        assert_eq!(signed.key_id, "central-command-a");
        assert_eq!(signed.certificate_generation.get(), 4);
        assert_eq!(signed.payload_digest, ContentDigest::hash(bytes));
        assert_eq!(
            signed.expires_at_unix_ms.get(),
            10_000 + DEFAULT_CENTRAL_COMMAND_TTL_MS
        );
        keyring
            .trust_bundle()
            .verify_at(&signed, UnixMillis::new(10_001))
            .unwrap();
    }

    #[test]
    fn trust_bundle_rejects_unknown_generation_and_revoked_key() {
        let key = test_key(9);
        let active = verification_key(&key, 2, CentralCommandKeyState::Active);
        let bundle = CentralCommandTrustBundle::new(vec![active]).unwrap();
        assert!(matches!(
            bundle.select("central-command-a", CertificateGeneration::new(3)),
            Err(CentralCommandSecurityError::KeyGenerationMismatch)
        ));
        assert!(matches!(
            bundle.select("unknown", CertificateGeneration::new(2)),
            Err(CentralCommandSecurityError::UnknownKey)
        ));

        let revoked = verification_key(&key, 2, CentralCommandKeyState::Revoked);
        let bundle = CentralCommandTrustBundle::new(vec![revoked]).unwrap();
        assert!(matches!(
            bundle.select("central-command-a", CertificateGeneration::new(2)),
            Err(CentralCommandSecurityError::RevokedKey)
        ));
    }

    #[tokio::test]
    async fn keyring_rejects_hsm_response_for_another_generation() {
        let signing_key = test_key(11);
        let trusted_key = verification_key(&signing_key, 1, CentralCommandKeyState::Active);
        let keyring = CentralCommandKeyring::new(
            Arc::new(TestSigner {
                key_pair: signing_key,
                response_generation: CertificateGeneration::new(2),
            }),
            CentralCommandTrustBundle::new(vec![trusted_key]).unwrap(),
            CentralCommandKeyId::new("central-command-a").unwrap(),
            CertificateGeneration::new(1),
        )
        .unwrap();
        assert!(matches!(
            keyring
                .sign(
                    GatewayOpaqueBytes::new(b"command".to_vec()).unwrap(),
                    UnixMillis::new(10_000),
                )
                .await,
            Err(CentralCommandSecurityError::SignerResponseMismatch)
        ));
    }

    #[tokio::test]
    async fn keyring_rejects_unbounded_command_ttl() {
        let signing_key = test_key(12);
        let trusted_key = verification_key(&signing_key, 1, CentralCommandKeyState::Active);
        let keyring = CentralCommandKeyring::new(
            Arc::new(TestSigner {
                key_pair: signing_key,
                response_generation: CertificateGeneration::new(1),
            }),
            CentralCommandTrustBundle::new(vec![trusted_key]).unwrap(),
            CentralCommandKeyId::new("central-command-a").unwrap(),
            CertificateGeneration::new(1),
        )
        .unwrap();
        assert!(matches!(
            keyring
                .sign_with_ttl_ms(
                    GatewayOpaqueBytes::new(b"command".to_vec()).unwrap(),
                    UnixMillis::new(10_000),
                    MAX_CENTRAL_COMMAND_TTL_MS + 1,
                )
                .await,
            Err(CentralCommandSecurityError::InvalidTtl)
        ));
    }
}
