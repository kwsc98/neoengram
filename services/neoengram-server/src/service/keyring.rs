use std::{collections::BTreeMap, fmt, path::Path};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use neoengram_protocol::RequestId;
use serde::Deserialize;
use tokio::io::AsyncReadExt;

const KEYRING_VERSION: u32 = 1;
const KEY_BYTES: usize = 32;
const MAX_KEYRING_BYTES: u64 = 64 * 1024;
const MAX_KEYS: usize = 32;
const TOKEN_DOMAIN: &[u8] = b"neoengram-agent-enrollment-token-v1\0";

/// Versioned keyed-BLAKE3 material used to derive replayable enrollment tokens and cursor MACs.
#[derive(Clone)]
pub struct EnrollmentKeyring {
    active_key_id: String,
    keys: BTreeMap<String, [u8; KEY_BYTES]>,
}

impl fmt::Debug for EnrollmentKeyring {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnrollmentKeyring")
            .field("active_key_id", &self.active_key_id)
            .field("key_ids", &self.keys.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KeyringError {
    #[error("failed to read enrollment keyring: {0}")]
    Io(String),
    #[error("invalid enrollment keyring: {0}")]
    Invalid(String),
    #[error("enrollment key {0:?} is unavailable")]
    UnknownKey(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyringDocument {
    version: u32,
    active_key_id: String,
    keys: BTreeMap<String, String>,
}

impl EnrollmentKeyring {
    /// Loads a bounded, strict JSON keyring. Secret values must be 32-byte base64url-no-pad keys.
    pub async fn load(path: &Path) -> Result<Self, KeyringError> {
        let path_metadata = tokio::fs::symlink_metadata(path)
            .await
            .map_err(|error| KeyringError::Io(error.to_string()))?;
        if path_metadata.file_type().is_symlink() {
            return Err(KeyringError::Invalid(
                "keyring must not be a symbolic link".into(),
            ));
        }
        let file = open_keyring(path).await?;
        let metadata = file
            .metadata()
            .await
            .map_err(|error| KeyringError::Io(error.to_string()))?;
        if !metadata.is_file() || metadata.len() > MAX_KEYRING_BYTES {
            return Err(KeyringError::Invalid(
                "keyring must be a regular file no larger than 64 KiB".into(),
            ));
        }
        validate_keyring_permissions(&metadata)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_KEYRING_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| KeyringError::Io(error.to_string()))?;
        Self::from_json(&bytes)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, KeyringError> {
        let document: KeyringDocument = serde_json::from_slice(bytes)
            .map_err(|error| KeyringError::Invalid(error.to_string()))?;
        if document.version != KEYRING_VERSION {
            return Err(KeyringError::Invalid(format!(
                "unsupported keyring version {}",
                document.version
            )));
        }
        if document.keys.is_empty() || document.keys.len() > MAX_KEYS {
            return Err(KeyringError::Invalid(format!(
                "keyring must contain 1..={MAX_KEYS} keys"
            )));
        }
        let mut keys = BTreeMap::new();
        for (key_id, encoded) in document.keys {
            RequestId::new(&key_id)
                .map_err(|error| KeyringError::Invalid(format!("invalid key ID: {error}")))?;
            let decoded = URL_SAFE_NO_PAD.decode(encoded.as_bytes()).map_err(|_| {
                KeyringError::Invalid(format!("key {key_id:?} is not base64url-no-pad"))
            })?;
            let key: [u8; KEY_BYTES] = decoded.try_into().map_err(|_| {
                KeyringError::Invalid(format!("key {key_id:?} must decode to 32 bytes"))
            })?;
            keys.insert(key_id, key);
        }
        if !keys.contains_key(&document.active_key_id) {
            return Err(KeyringError::Invalid(
                "active_key_id does not name a configured key".into(),
            ));
        }
        Ok(Self {
            active_key_id: document.active_key_id,
            keys,
        })
    }

    #[must_use]
    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    /// Derives the exact opaque token from immutable persisted enrollment material.
    pub fn derive_token(&self, key_id: &str, material: &[u8]) -> Result<String, KeyringError> {
        let digest = self.keyed_hash(key_id, TOKEN_DOMAIN, material)?;
        Ok(format!("ngenr_v1_{}", URL_SAFE_NO_PAD.encode(digest)))
    }

    pub fn mac(
        &self,
        key_id: &str,
        domain: &[u8],
        material: &[u8],
    ) -> Result<[u8; 32], KeyringError> {
        self.keyed_hash(key_id, domain, material)
    }

    pub fn verify_mac(
        &self,
        key_id: &str,
        domain: &[u8],
        material: &[u8],
        expected: &[u8],
    ) -> Result<bool, KeyringError> {
        let actual = self.keyed_hash(key_id, domain, material)?;
        Ok(constant_time_eq(&actual, expected))
    }

    fn keyed_hash(
        &self,
        key_id: &str,
        domain: &[u8],
        material: &[u8],
    ) -> Result<[u8; 32], KeyringError> {
        let key = self
            .keys
            .get(key_id)
            .ok_or_else(|| KeyringError::UnknownKey(key_id.to_owned()))?;
        let mut hasher = blake3::Hasher::new_keyed(key);
        hasher.update(domain);
        hasher.update(&(material.len() as u64).to_le_bytes());
        hasher.update(material);
        Ok(*hasher.finalize().as_bytes())
    }
}

async fn open_keyring(path: &Path) -> Result<tokio::fs::File, KeyringError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options
        .open(path)
        .await
        .map_err(|error| KeyringError::Io(error.to_string()))
}

#[cfg(unix)]
fn validate_keyring_permissions(metadata: &std::fs::Metadata) -> Result<(), KeyringError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(KeyringError::Invalid(
            "keyring must be owned by the effective process user".into(),
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(KeyringError::Invalid(
            "keyring permissions must not grant group or other access".into(),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_keyring_permissions(_metadata: &std::fs::Metadata) -> Result<(), KeyringError> {
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "active_key_id": "enrollment-key-a",
            "keys": {
                "enrollment-key-a": URL_SAFE_NO_PAD.encode([7_u8; 32]),
                "enrollment-key-old": URL_SAFE_NO_PAD.encode([8_u8; 32])
            }
        }))
        .unwrap()
    }

    #[test]
    fn token_derivation_is_stable_and_domain_separated() {
        let keyring = EnrollmentKeyring::from_json(&document()).unwrap();
        let first = keyring
            .derive_token("enrollment-key-a", b"intent-a")
            .unwrap();
        assert_eq!(
            first,
            keyring
                .derive_token("enrollment-key-a", b"intent-a")
                .unwrap()
        );
        assert_ne!(
            first,
            keyring
                .derive_token("enrollment-key-old", b"intent-a")
                .unwrap()
        );
        assert!(first.starts_with("ngenr_v1_"));
    }

    #[test]
    fn keyring_never_accepts_missing_or_malformed_key_material() {
        assert!(
            EnrollmentKeyring::from_json(br#"{"version":1,"active_key_id":"a","keys":{}}"#)
                .is_err()
        );
        assert!(EnrollmentKeyring::from_json(
            br#"{"version":1,"active_key_id":"a","keys":{"a":"AA"}}"#
        )
        .is_err());
        assert!(
            !format!("{:?}", EnrollmentKeyring::from_json(&document()).unwrap())
                .contains(&URL_SAFE_NO_PAD.encode([7_u8; 32]))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn keyring_loader_rejects_weak_permissions_and_symlinks() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let keyring_path = directory.path().join("keyring.json");
        std::fs::write(&keyring_path, document()).unwrap();
        std::fs::set_permissions(&keyring_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            EnrollmentKeyring::load(&keyring_path).await,
            Err(KeyringError::Invalid(_))
        ));

        std::fs::set_permissions(&keyring_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        EnrollmentKeyring::load(&keyring_path).await.unwrap();

        let link_path = directory.path().join("keyring-link.json");
        symlink(&keyring_path, &link_path).unwrap();
        assert!(matches!(
            EnrollmentKeyring::load(&link_path).await,
            Err(KeyringError::Invalid(_))
        ));
    }
}
