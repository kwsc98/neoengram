use std::{path::Path, sync::Arc};

use neoengram_agent::{
    PrivateKeyMaterial, SqliteSystemIdentityStore, SystemIdentityRecord, SystemIdentitySeed,
};
use neoengram_protocol::Ed25519PublicKeySpki;
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use uuid::Uuid;

use crate::{AgentDaemonError, AgentDaemonResult};

/// Secret-free projection of the durable Agent identity for diagnostics and acceptance tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedIdentitySummary {
    pub revision: u64,
    pub bootstrap_request_id: String,
    pub installation_id: String,
    pub approved_agent_id: Option<String>,
    pub approved_enrollment_id: Option<String>,
    pub terminal_enrollment_state: Option<String>,
    pub terminal_enrollment_id: Option<String>,
}

pub type AgentSigningKey = Arc<Ed25519KeyPair>;

/// Loads and validates the durable identity without exposing its private key material.
pub fn load_persisted_identity(
    state_dir: impl AsRef<Path>,
) -> AgentDaemonResult<Option<PersistedIdentitySummary>> {
    let store = SqliteSystemIdentityStore::open(state_dir)?;
    store.integrity_check()?;
    Ok(store.load()?.map(|record| {
        let (approved_agent_id, approved_enrollment_id) = record.approved.map_or_else(
            || (None, None),
            |approved| (Some(approved.agent_id), Some(approved.enrollment_id)),
        );
        let (terminal_enrollment_state, terminal_enrollment_id) =
            record.terminal_enrollment.map_or_else(
                || (None, None),
                |terminal| {
                    (
                        Some(terminal.state.as_str().to_owned()),
                        Some(terminal.enrollment_id),
                    )
                },
            );
        PersistedIdentitySummary {
            revision: record.revision,
            bootstrap_request_id: record.bootstrap_request_id,
            installation_id: record.installation_id,
            approved_agent_id,
            approved_enrollment_id,
            terminal_enrollment_state,
            terminal_enrollment_id,
        }
    }))
}

/// Loads the immutable installation identity, creating an Ed25519 PKCS#8 key and stable IDs once.
pub fn load_or_create_identity(
    store: &SqliteSystemIdentityStore,
) -> AgentDaemonResult<SystemIdentityRecord> {
    if let Some(identity) = store.load()? {
        signing_key_from_identity(&identity)?;
        return Ok(identity);
    }

    let private_key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| AgentDaemonError::Identity("failed to generate Ed25519 PKCS#8 key".into()))?;
    let seed = SystemIdentitySeed::new(
        format!("bootstrap-{}", Uuid::new_v4().simple()),
        format!("installation-{}", Uuid::new_v4().simple()),
        PrivateKeyMaterial::new(private_key.as_ref().to_vec())?,
    )?;
    let identity = store.initialize(seed)?;
    signing_key_from_identity(&identity)?;
    Ok(identity)
}

/// Parses the persisted PKCS#8 document and rejects any non-Ed25519 or malformed key material.
pub fn signing_key_from_identity(
    identity: &SystemIdentityRecord,
) -> AgentDaemonResult<AgentSigningKey> {
    Ed25519KeyPair::from_pkcs8(identity.private_key.expose_secret())
        .map(Arc::new)
        .map_err(|_| {
            AgentDaemonError::Identity("persisted Agent key is not valid Ed25519 PKCS#8".into())
        })
}

pub(crate) fn public_key_spki_der(signing_key: &Ed25519KeyPair) -> AgentDaemonResult<Vec<u8>> {
    let public_key: [u8; 32] = signing_key
        .public_key()
        .as_ref()
        .try_into()
        .map_err(|_| AgentDaemonError::Identity("Ed25519 public key is not 32 bytes".into()))?;
    Ok(Ed25519PublicKeySpki::from_public_key_bytes(public_key)
        .as_der()
        .to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_identity_is_pkcs8_and_restart_stable() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteSystemIdentityStore::open(directory.path()).unwrap();
        let first = load_or_create_identity(&store).unwrap();
        let first_key = signing_key_from_identity(&first).unwrap();
        assert!(first.bootstrap_request_id.starts_with("bootstrap-"));
        assert!(first.installation_id.starts_with("installation-"));
        assert!(!public_key_spki_der(&first_key).unwrap().is_empty());
        drop(store);

        let reopened = SqliteSystemIdentityStore::open(directory.path()).unwrap();
        let second = load_or_create_identity(&reopened).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first_key.public_key().as_ref(),
            signing_key_from_identity(&second)
                .unwrap()
                .public_key()
                .as_ref()
        );
        drop(reopened);

        let summary = load_persisted_identity(directory.path()).unwrap().unwrap();
        assert_eq!(summary.revision, first.revision);
        assert_eq!(summary.bootstrap_request_id, first.bootstrap_request_id);
        assert_eq!(summary.installation_id, first.installation_id);
        assert_eq!(summary.approved_agent_id, None);
        assert_eq!(summary.approved_enrollment_id, None);
        assert_eq!(summary.terminal_enrollment_state, None);
        assert_eq!(summary.terminal_enrollment_id, None);
    }
}
