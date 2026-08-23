use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use zeroize::Zeroizing;

const ENVELOPE_MAGIC: &[u8; 4] = b"S3E2";
const NONCE_BYTES: usize = 12;
const DATA_KEY_BYTES: usize = 32;
const WRAPPED_DATA_KEY_BYTES: usize = DATA_KEY_BYTES + aead::MAX_TAG_LEN;
const MAX_KEY_ID_BYTES: usize = 512;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum S3SecretEnvelopeError {
    #[error("S3 envelope key ID is invalid")]
    InvalidKeyId,
    #[error("S3 envelope key ID is duplicated")]
    DuplicateKeyId,
    #[error("S3 envelope ciphertext is invalid")]
    InvalidCiphertext,
    #[error("S3 envelope key is unavailable")]
    KeyUnavailable,
    #[error("S3 envelope cryptographic operation failed")]
    CryptographicFailure,
}

/// KMS/HSM integration boundary for persisted S3 credential Secrets.
///
/// Implementations must create a unique data-encryption key per call, bind `context` as
/// authenticated encryption data, and persist the wrapping-key ID with the wrapped DEK so old
/// credentials remain decryptable during key rotation.
#[async_trait]
pub trait S3SecretEnvelope: Send + Sync {
    async fn encrypt_secret(
        &self,
        context: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, S3SecretEnvelopeError>;

    async fn decrypt_secret(
        &self,
        context: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, S3SecretEnvelopeError>;
}

/// File-key implementation for loopback development and deterministic integration fixtures.
/// Production composition roots inject a KMS/HSM-backed [`S3SecretEnvelope`] instead.
#[derive(Clone)]
pub struct LocalS3SecretEnvelope {
    active_key_id: Arc<str>,
    keys: Arc<BTreeMap<String, [u8; 32]>>,
}

impl std::fmt::Debug for LocalS3SecretEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalS3SecretEnvelope")
            .field("active_key_id", &self.active_key_id)
            .field("key_count", &self.keys.len())
            .finish()
    }
}

impl LocalS3SecretEnvelope {
    pub fn new(
        active_key_id: impl Into<String>,
        key: [u8; 32],
    ) -> Result<Self, S3SecretEnvelopeError> {
        let active_key_id = active_key_id.into();
        validate_key_id(&active_key_id)?;
        let mut keys = BTreeMap::new();
        keys.insert(active_key_id.clone(), key);
        Ok(Self {
            active_key_id: Arc::from(active_key_id),
            keys: Arc::new(keys),
        })
    }

    /// Adds an old unwrap-only key. Encryption always uses the active key passed to [`Self::new`].
    pub fn with_decryption_key(
        mut self,
        key_id: impl Into<String>,
        key: [u8; 32],
    ) -> Result<Self, S3SecretEnvelopeError> {
        let key_id = key_id.into();
        validate_key_id(&key_id)?;
        if Arc::make_mut(&mut self.keys).insert(key_id, key).is_some() {
            return Err(S3SecretEnvelopeError::DuplicateKeyId);
        }
        Ok(self)
    }

    #[must_use]
    pub fn cursor_signing_key(&self) -> [u8; 32] {
        let key = self
            .keys
            .get(self.active_key_id.as_ref())
            .expect("active local S3 envelope key is present");
        blake3::derive_key("neoengram s3 continuation token signing v1", key)
    }

    fn active_key(&self) -> (&str, &[u8; 32]) {
        (
            self.active_key_id.as_ref(),
            self.keys
                .get(self.active_key_id.as_ref())
                .expect("active local S3 envelope key is present"),
        )
    }
}

#[async_trait]
impl S3SecretEnvelope for LocalS3SecretEnvelope {
    async fn encrypt_secret(
        &self,
        context: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, S3SecretEnvelopeError> {
        let (key_id, wrapping_key) = self.active_key();
        let key_id_bytes = key_id.as_bytes();
        let key_id_length =
            u16::try_from(key_id_bytes.len()).map_err(|_| S3SecretEnvelopeError::InvalidKeyId)?;
        let mut data_key = Zeroizing::new([0_u8; DATA_KEY_BYTES]);
        getrandom::fill(data_key.as_mut())
            .map_err(|_| S3SecretEnvelopeError::CryptographicFailure)?;

        let mut wrap_nonce = [0_u8; NONCE_BYTES];
        let mut secret_nonce = [0_u8; NONCE_BYTES];
        getrandom::fill(&mut wrap_nonce)
            .and_then(|_| getrandom::fill(&mut secret_nonce))
            .map_err(|_| S3SecretEnvelopeError::CryptographicFailure)?;
        let wrapped_data_key = seal(
            wrapping_key,
            wrap_nonce,
            &dek_aad(key_id, context),
            data_key.as_ref(),
        )?;
        let encrypted_secret = seal(
            data_key.as_ref(),
            secret_nonce,
            &secret_aad(context),
            plaintext,
        )?;

        let mut output = Vec::with_capacity(
            ENVELOPE_MAGIC.len()
                + 2
                + key_id_bytes.len()
                + NONCE_BYTES
                + WRAPPED_DATA_KEY_BYTES
                + NONCE_BYTES
                + encrypted_secret.len(),
        );
        output.extend_from_slice(ENVELOPE_MAGIC);
        output.extend_from_slice(&key_id_length.to_be_bytes());
        output.extend_from_slice(key_id_bytes);
        output.extend_from_slice(&wrap_nonce);
        output.extend_from_slice(&wrapped_data_key);
        output.extend_from_slice(&secret_nonce);
        output.extend_from_slice(&encrypted_secret);
        Ok(output)
    }

    async fn decrypt_secret(
        &self,
        context: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, S3SecretEnvelopeError> {
        let minimum = ENVELOPE_MAGIC.len()
            + 2
            + 1
            + NONCE_BYTES
            + WRAPPED_DATA_KEY_BYTES
            + NONCE_BYTES
            + aead::MAX_TAG_LEN;
        if ciphertext.len() < minimum || !ciphertext.starts_with(ENVELOPE_MAGIC) {
            return Err(S3SecretEnvelopeError::InvalidCiphertext);
        }
        let key_id_length = usize::from(u16::from_be_bytes(
            ciphertext[4..6]
                .try_into()
                .map_err(|_| S3SecretEnvelopeError::InvalidCiphertext)?,
        ));
        if !(1..=MAX_KEY_ID_BYTES).contains(&key_id_length) {
            return Err(S3SecretEnvelopeError::InvalidCiphertext);
        }
        let key_id_end = 6_usize
            .checked_add(key_id_length)
            .ok_or(S3SecretEnvelopeError::InvalidCiphertext)?;
        let wrap_nonce_end = key_id_end
            .checked_add(NONCE_BYTES)
            .ok_or(S3SecretEnvelopeError::InvalidCiphertext)?;
        let wrapped_key_end = wrap_nonce_end
            .checked_add(WRAPPED_DATA_KEY_BYTES)
            .ok_or(S3SecretEnvelopeError::InvalidCiphertext)?;
        let secret_nonce_end = wrapped_key_end
            .checked_add(NONCE_BYTES)
            .ok_or(S3SecretEnvelopeError::InvalidCiphertext)?;
        if secret_nonce_end + aead::MAX_TAG_LEN > ciphertext.len() {
            return Err(S3SecretEnvelopeError::InvalidCiphertext);
        }
        let key_id = std::str::from_utf8(&ciphertext[6..key_id_end])
            .map_err(|_| S3SecretEnvelopeError::InvalidCiphertext)?;
        validate_key_id(key_id).map_err(|_| S3SecretEnvelopeError::InvalidCiphertext)?;
        let wrapping_key = self
            .keys
            .get(key_id)
            .ok_or(S3SecretEnvelopeError::KeyUnavailable)?;
        let wrap_nonce: [u8; NONCE_BYTES] = ciphertext[key_id_end..wrap_nonce_end]
            .try_into()
            .map_err(|_| S3SecretEnvelopeError::InvalidCiphertext)?;
        let secret_nonce: [u8; NONCE_BYTES] = ciphertext[wrapped_key_end..secret_nonce_end]
            .try_into()
            .map_err(|_| S3SecretEnvelopeError::InvalidCiphertext)?;
        let mut data_key = Zeroizing::new(open(
            wrapping_key,
            wrap_nonce,
            &dek_aad(key_id, context),
            &ciphertext[wrap_nonce_end..wrapped_key_end],
        )?);
        if data_key.len() != DATA_KEY_BYTES {
            return Err(S3SecretEnvelopeError::InvalidCiphertext);
        }
        open(
            data_key.as_mut_slice(),
            secret_nonce,
            &secret_aad(context),
            &ciphertext[secret_nonce_end..],
        )
    }
}

fn validate_key_id(key_id: &str) -> Result<(), S3SecretEnvelopeError> {
    if key_id.is_empty()
        || key_id.len() > MAX_KEY_ID_BYTES
        || key_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == 0x7f)
    {
        return Err(S3SecretEnvelopeError::InvalidKeyId);
    }
    Ok(())
}

fn seal(
    key: &[u8],
    nonce: [u8; NONCE_BYTES],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, S3SecretEnvelopeError> {
    let key = UnboundKey::new(&AES_256_GCM, key)
        .map(LessSafeKey::new)
        .map_err(|_| S3SecretEnvelopeError::CryptographicFailure)?;
    let mut ciphertext = plaintext.to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad),
        &mut ciphertext,
    )
    .map_err(|_| S3SecretEnvelopeError::CryptographicFailure)?;
    Ok(ciphertext)
}

fn open(
    key: &[u8],
    nonce: [u8; NONCE_BYTES],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, S3SecretEnvelopeError> {
    let key = UnboundKey::new(&AES_256_GCM, key)
        .map(LessSafeKey::new)
        .map_err(|_| S3SecretEnvelopeError::CryptographicFailure)?;
    let mut plaintext = ciphertext.to_vec();
    key.open_in_place(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad),
        &mut plaintext,
    )
    .map(|bytes| bytes.to_vec())
    .map_err(|_| S3SecretEnvelopeError::InvalidCiphertext)
}

fn dek_aad(key_id: &str, context: &[u8]) -> Vec<u8> {
    let mut aad = b"neoengram-s3-dek-v2\0".to_vec();
    aad.extend_from_slice(key_id.as_bytes());
    aad.push(0);
    aad.extend_from_slice(context);
    aad
}

fn secret_aad(context: &[u8]) -> Vec<u8> {
    let mut aad = b"neoengram-s3-secret-v2\0".to_vec();
    aad.extend_from_slice(context);
    aad
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn each_secret_uses_a_distinct_wrapped_data_key_and_context() {
        let envelope = LocalS3SecretEnvelope::new("local-a", [7; 32]).unwrap();
        let first = envelope
            .encrypt_secret(b"credential-a", b"shared-secret")
            .await
            .unwrap();
        let second = envelope
            .encrypt_secret(b"credential-a", b"shared-secret")
            .await
            .unwrap();

        assert_ne!(first, second);
        assert!(first.starts_with(ENVELOPE_MAGIC));
        assert_eq!(
            envelope
                .decrypt_secret(b"credential-a", &first)
                .await
                .unwrap(),
            b"shared-secret"
        );
        assert!(envelope
            .decrypt_secret(b"credential-b", &first)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn rotation_encrypts_with_active_key_and_retains_old_unwrap_key() {
        let old = LocalS3SecretEnvelope::new("local-old", [3; 32]).unwrap();
        let old_ciphertext = old
            .encrypt_secret(b"credential-a", b"old-secret")
            .await
            .unwrap();
        let rotated = LocalS3SecretEnvelope::new("local-new", [4; 32])
            .unwrap()
            .with_decryption_key("local-old", [3; 32])
            .unwrap();
        let new_ciphertext = rotated
            .encrypt_secret(b"credential-b", b"new-secret")
            .await
            .unwrap();

        assert_eq!(
            rotated
                .decrypt_secret(b"credential-a", &old_ciphertext)
                .await
                .unwrap(),
            b"old-secret"
        );
        assert_eq!(
            rotated
                .decrypt_secret(b"credential-b", &new_ciphertext)
                .await
                .unwrap(),
            b"new-secret"
        );
        assert!(new_ciphertext
            .windows("local-new".len())
            .any(|value| value == b"local-new"));
    }
}
