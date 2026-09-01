//! Verification of Central's signed Assignment and Decision commands.
//!
//! The private signing side lives behind the Central KMS/HSM port. Agents only load a bounded,
//! generation-bound public trust bundle and fail closed when a command cannot be authenticated.

use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Component, Path},
};

use neoengram_domain::protocol::{
    AgentChannelDownstreamFrame, AgentChannelDownstreamMessage, CentralSignedPayload,
    CertificateGeneration, Ed25519PublicKeySpki, S3ReadTicket, SignedMaterializationBatchTicket,
    SignedTransferTicket, UnixMillis,
};
use serde::Deserialize;

use crate::{AgentDaemonError, AgentDaemonResult};

const MAX_TRUST_BUNDLE_BYTES: u64 = 256 * 1024;
const MAX_TRUST_KEYS: usize = 32;
const MAX_COMMAND_TTL_MS: u64 = 5 * 60 * 1_000;
const MAX_KEY_ID_BYTES: usize = 128;
const TRUST_BUNDLE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrustKeyState {
    Active,
    Retiring,
    Revoked,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustBundleDocument {
    schema_version: u16,
    keys: Vec<TrustKeyDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustKeyDocument {
    key_id: String,
    certificate_generation: CertificateGeneration,
    public_key_spki: Ed25519PublicKeySpki,
    state: TrustKeyState,
}

#[derive(Debug, Clone)]
struct VerificationKey {
    public_key_spki: Ed25519PublicKeySpki,
    state: TrustKeyState,
}

/// Immutable public key set distributed to one Agent.
#[derive(Debug, Clone)]
pub struct CentralCommandTrustBundle {
    keys: BTreeMap<(String, u64), VerificationKey>,
}

impl CentralCommandTrustBundle {
    /// Loads a bounded, strict JSON trust bundle from a protected regular file.
    pub fn load(path: &Path) -> AgentDaemonResult<Self> {
        validate_path(path)?;
        // Kubernetes ConfigMap entries are symlinks. Resolve once, inspect and open that exact
        // target, then confirm that the projected path still resolves to it.
        let resolved = fs::canonicalize(path).map_err(|error| {
            configuration(format!(
                "Central command trust bundle could not be resolved: {error}"
            ))
        })?;
        let metadata = fs::symlink_metadata(&resolved).map_err(|error| {
            configuration(format!(
                "Central command trust bundle could not be inspected: {error}"
            ))
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(configuration(
                "Central command trust bundle must resolve to a regular file",
            ));
        }
        if metadata.len() == 0 || metadata.len() > MAX_TRUST_BUNDLE_BYTES {
            return Err(configuration(format!(
                "Central command trust bundle must contain 1..={MAX_TRUST_BUNDLE_BYTES} bytes"
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
                configuration(format!(
                    "Central command trust bundle could not be read: {error}"
                ))
            })?;
        if bytes.len() as u64 > MAX_TRUST_BUNDLE_BYTES {
            return Err(configuration(
                "Central command trust bundle exceeds the size limit",
            ));
        }
        let confirmed = fs::canonicalize(path).map_err(|error| {
            configuration(format!(
                "Central command trust bundle could not be confirmed: {error}"
            ))
        })?;
        if confirmed != resolved {
            return Err(configuration(
                "Central command trust bundle changed while it was being loaded",
            ));
        }
        Self::from_json(&bytes)
    }

    fn from_json(bytes: &[u8]) -> AgentDaemonResult<Self> {
        let document: TrustBundleDocument = serde_json::from_slice(bytes).map_err(|error| {
            configuration(format!("Central command trust bundle is invalid: {error}"))
        })?;
        Self::from_document(document)
    }

    fn from_document(document: TrustBundleDocument) -> AgentDaemonResult<Self> {
        if document.schema_version != TRUST_BUNDLE_SCHEMA_VERSION {
            return Err(configuration(
                "Central command trust bundle schema_version must be 1",
            ));
        }
        if document.keys.is_empty() || document.keys.len() > MAX_TRUST_KEYS {
            return Err(configuration(format!(
                "Central command trust bundle must contain 1..={MAX_TRUST_KEYS} keys"
            )));
        }
        let mut keys = BTreeMap::new();
        for key in document.keys {
            validate_key_id(&key.key_id)?;
            let generation = key.certificate_generation.get();
            if generation == 0 {
                return Err(configuration(
                    "Central command trust bundle key generation must be positive",
                ));
            }
            if keys
                .insert(
                    (key.key_id, generation),
                    VerificationKey {
                        public_key_spki: key.public_key_spki,
                        state: key.state,
                    },
                )
                .is_some()
            {
                return Err(configuration(
                    "Central command trust bundle contains a duplicate key generation",
                ));
            }
        }
        Ok(Self { keys })
    }

    /// Verifies a channel frame before dispatch. Open/Ack/Error frames remain unsigned.
    pub(crate) fn verify_downstream_if_command(
        &self,
        frame: &AgentChannelDownstreamFrame,
        now_unix_ms: UnixMillis,
    ) -> AgentDaemonResult<()> {
        if !matches!(
            &frame.message,
            AgentChannelDownstreamMessage::Assignment(_)
                | AgentChannelDownstreamMessage::Decision(_)
                | AgentChannelDownstreamMessage::LifecycleAssignment(_)
                | AgentChannelDownstreamMessage::ReplicationAssignment(_)
                | AgentChannelDownstreamMessage::MaterializationAssignment(_)
        ) {
            return Ok(());
        }
        let signature = frame.central_signature.as_ref().ok_or_else(|| {
            command_rejected("Central command channel frame is missing its command signature")
        })?;
        verify_signature(
            signature,
            frame.central_command_payload_bytes(),
            self,
            now_unix_ms,
        )
    }

    /// Verifies a Central-issued S3 read ticket with the same generation-bound trust roots used
    /// for Assignment and Decision delivery.  The signed envelope timestamps must equal the
    /// ticket timestamps so neither layer can extend the other's authorization window.
    pub(crate) fn verify_s3_ticket(
        &self,
        ticket: &S3ReadTicket,
        now_unix_ms: UnixMillis,
    ) -> AgentDaemonResult<()> {
        ticket.validate_at(now_unix_ms).map_err(command_rejected)?;
        let signature = ticket.central_signature().map_err(command_rejected)?;
        if signature.signed_at_unix_ms != ticket.issued_at_unix_ms
            || signature.expires_at_unix_ms != ticket.expires_at_unix_ms
        {
            return Err(command_rejected(
                "Central S3 ticket signature window does not match the ticket",
            ));
        }
        verify_signature(
            &signature,
            ticket.signing_bytes().map_err(|error| {
                neoengram_domain::protocol::ProtocolError::InvalidField {
                    field: "s3_read_ticket",
                    reason: error.to_owned(),
                }
            }),
            self,
            now_unix_ms,
        )
    }

    /// Verifies a Central-issued replication ticket before any source object is opened or any
    /// target staging file is created.
    pub(crate) fn verify_transfer_ticket(
        &self,
        ticket: &SignedTransferTicket,
        now_unix_ms: UnixMillis,
    ) -> AgentDaemonResult<()> {
        if ticket.ticket.deadline_unix_ms != ticket.central_signature.expires_at_unix_ms {
            return Err(command_rejected(
                "Central transfer signature expiry does not match the ticket deadline",
            ));
        }
        verify_signature(
            &ticket.central_signature,
            SignedTransferTicket::payload_bytes(&ticket.ticket),
            self,
            now_unix_ms,
        )
    }

    /// Verifies a v2 materialization batch ticket before a manifest is accepted or target
    /// staging is touched. The v2 ticket uses its own domain-separated canonical payload, so a
    /// valid legacy transfer signature cannot be replayed as a materialization capability.
    pub(crate) fn verify_materialization_ticket(
        &self,
        ticket: &SignedMaterializationBatchTicket,
        now_unix_ms: UnixMillis,
    ) -> AgentDaemonResult<()> {
        ticket
            .validate()
            .map_err(|error| command_rejected(error.to_string()))?;
        if ticket.ticket.deadline_unix_ms != ticket.central_signature.expires_at_unix_ms {
            return Err(command_rejected(
                "Central materialization signature expiry does not match the batch ticket deadline",
            ));
        }
        verify_signature(
            &ticket.central_signature,
            ticket.ticket.payload_bytes(),
            self,
            now_unix_ms,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_test_keys(
        keys: Vec<(
            String,
            CertificateGeneration,
            Ed25519PublicKeySpki,
            TrustKeyState,
        )>,
    ) -> AgentDaemonResult<Self> {
        Self::from_document(TrustBundleDocument {
            schema_version: TRUST_BUNDLE_SCHEMA_VERSION,
            keys: keys
                .into_iter()
                .map(
                    |(key_id, certificate_generation, public_key_spki, state)| TrustKeyDocument {
                        key_id,
                        certificate_generation,
                        public_key_spki,
                        state,
                    },
                )
                .collect(),
        })
    }
}

fn verify_signature(
    signature: &CentralSignedPayload,
    payload: neoengram_domain::protocol::ProtocolResult<Vec<u8>>,
    bundle: &CentralCommandTrustBundle,
    now_unix_ms: UnixMillis,
) -> AgentDaemonResult<()> {
    let key_id = signature.key_id.clone();
    validate_key_id(&key_id).map_err(|error| command_rejected(error.to_string()))?;
    let generation = signature.certificate_generation.get();
    let key = bundle
        .keys
        .get(&(key_id.clone(), generation))
        .ok_or_else(|| {
            if bundle
                .keys
                .keys()
                .any(|(candidate, _)| candidate == &key_id)
            {
                command_rejected("Central command trust bundle key generation does not match")
            } else {
                command_rejected("Central command trust bundle does not contain the command key")
            }
        })?;
    if key.state == TrustKeyState::Revoked {
        return Err(command_rejected(
            "Central command signing key has been revoked",
        ));
    }
    let ttl = signature
        .expires_at_unix_ms
        .get()
        .checked_sub(signature.signed_at_unix_ms.get())
        .ok_or_else(|| command_rejected("Central command signature has an invalid TTL"))?;
    if ttl == 0 || ttl > MAX_COMMAND_TTL_MS {
        return Err(command_rejected(
            "Central command signature TTL is outside the allowed window",
        ));
    }
    let payload = payload.map_err(|error| command_rejected(error.to_string()))?;
    if signature.payload.as_bytes() != payload {
        return Err(command_rejected(
            "Central command signature payload does not match the received command",
        ));
    }
    signature
        .verify_at(&key.public_key_spki, now_unix_ms)
        .map_err(|error| command_rejected(format!("Central command signature rejected: {error}")))
}

fn validate_key_id(value: &str) -> AgentDaemonResult<()> {
    let mut bytes = value.bytes();
    let valid = !value.is_empty()
        && value.len() <= MAX_KEY_ID_BYTES
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(configuration(
            "Central command key_id is not a canonical alias",
        ))
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
            "central_command_trust_bundle_file must be an absolute normalized path",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_permissions(metadata: &fs::Metadata) -> AgentDaemonResult<()> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(configuration(
            "Central command trust bundle must not be writable by group or other users",
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

fn command_rejected(message: impl Into<String>) -> AgentDaemonError {
    AgentDaemonError::Session(message.into())
}

#[cfg(test)]
mod tests {
    use neoengram_domain::core::ContentDigest;
    use neoengram_domain::protocol::{
        AgentChannelDownstreamMessage, AssignmentGeneration, AssignmentId, ControlError,
        DecisionGeneration, Ed25519Signature, ErrorCode, Extensions, GatewayOpaqueBytes,
        IndexRevision, JobDecision, JobId, JobState, MessageId, PublishDecision, SequenceNumber,
        SessionGeneration, WireIndexVersion, CURRENT_WIRE_VERSION,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair as _};

    use super::*;

    fn signing_key(seed: u8) -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
    }

    fn public_key(key: &Ed25519KeyPair) -> Ed25519PublicKeySpki {
        Ed25519PublicKeySpki::from_public_key_bytes(key.public_key().as_ref().try_into().unwrap())
    }

    fn trust_bundle(
        key: &Ed25519KeyPair,
        generation: u64,
        state: TrustKeyState,
    ) -> CentralCommandTrustBundle {
        CentralCommandTrustBundle::from_test_keys(vec![(
            "central-command-a".to_owned(),
            CertificateGeneration::new(generation),
            public_key(key),
            state,
        )])
        .unwrap()
    }

    fn decision() -> JobDecision {
        JobDecision {
            job_id: JobId::new("job-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            decision_generation: DecisionGeneration::new(1),
            decision: PublishDecision::Publish {
                published_index_version: WireIndexVersion {
                    revision: IndexRevision::new(1),
                    digest: ContentDigest::from_bytes([0x44; 32]),
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            },
            final_state: JobState::Succeeded,
            extensions: Extensions::new(),
        }
    }

    fn decision_frame() -> AgentChannelDownstreamFrame {
        AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(2),
            message_id: MessageId::new("central-decision-a").unwrap(),
            correlation_id: None,
            session_generation: SessionGeneration::new(3),
            sent_at_unix_ms: UnixMillis::new(1_000),
            central_signature: None,
            message: AgentChannelDownstreamMessage::Decision(decision()),
            extensions: Extensions::new(),
        }
    }

    fn signed_payload(
        payload: Vec<u8>,
        key: &Ed25519KeyPair,
        generation: u64,
        signed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    ) -> CentralSignedPayload {
        let payload = GatewayOpaqueBytes::new(payload).unwrap();
        let mut signed = CentralSignedPayload {
            key_id: "central-command-a".to_owned(),
            certificate_generation: CertificateGeneration::new(generation),
            signed_at_unix_ms: UnixMillis::new(signed_at_unix_ms),
            expires_at_unix_ms: UnixMillis::new(expires_at_unix_ms),
            payload_digest: ContentDigest::hash(payload.as_bytes()),
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions: Extensions::new(),
        };
        signed.signature =
            Ed25519Signature::new(key.sign(&signed.signing_bytes().unwrap()).as_ref().to_vec())
                .unwrap();
        signed
    }

    fn sign_frame(
        frame: &mut AgentChannelDownstreamFrame,
        key: &Ed25519KeyPair,
        generation: u64,
        signed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    ) {
        frame.central_signature = Some(signed_payload(
            frame.central_command_payload_bytes().unwrap(),
            key,
            generation,
            signed_at_unix_ms,
            expires_at_unix_ms,
        ));
    }

    fn error_frame() -> AgentChannelDownstreamFrame {
        AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(2),
            message_id: MessageId::new("error-a").unwrap(),
            correlation_id: None,
            session_generation: SessionGeneration::new(3),
            sent_at_unix_ms: UnixMillis::new(1_000),
            central_signature: None,
            message: AgentChannelDownstreamMessage::Error(ControlError {
                code: ErrorCode::new("UNAVAILABLE").unwrap(),
                message: "temporarily unavailable".to_owned(),
                retryable: true,
                retry_after_ms: None,
                extensions: Extensions::new(),
            }),
            extensions: Extensions::new(),
        }
    }

    fn trust_document(key: &Ed25519KeyPair) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "keys": [{
                "key_id": "central-command-a",
                "certificate_generation": "1",
                "public_key_spki": public_key(key),
                "state": "active"
            }]
        }))
        .unwrap()
    }

    #[test]
    fn strict_document_rejects_unknown_fields_and_duplicate_generations() {
        let key_pair = signing_key(7);
        let valid = String::from_utf8(trust_document(&key_pair)).unwrap();
        let duplicate_member = valid.replacen(
            "\"schema_version\":1",
            "\"schema_version\":1,\"schema_version\":1",
            1,
        );
        assert!(CentralCommandTrustBundle::from_json(duplicate_member.as_bytes()).is_err());

        let mut unknown =
            serde_json::from_slice::<serde_json::Value>(&trust_document(&key_pair)).unwrap();
        unknown["unexpected"] = serde_json::json!(true);
        assert!(
            CentralCommandTrustBundle::from_json(&serde_json::to_vec(&unknown).unwrap()).is_err()
        );

        let mut unknown_key_field =
            serde_json::from_slice::<serde_json::Value>(&trust_document(&key_pair)).unwrap();
        unknown_key_field["keys"][0]["unexpected"] = serde_json::json!(true);
        assert!(CentralCommandTrustBundle::from_json(
            &serde_json::to_vec(&unknown_key_field).unwrap()
        )
        .is_err());

        let key = Ed25519PublicKeySpki::from_public_key_bytes([7; 32]);
        let error = CentralCommandTrustBundle::from_test_keys(vec![
            (
                "central-a".to_owned(),
                CertificateGeneration::new(1),
                key.clone(),
                TrustKeyState::Active,
            ),
            (
                "central-a".to_owned(),
                CertificateGeneration::new(1),
                key,
                TrustKeyState::Retiring,
            ),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("duplicate"));

        let mut malformed_state =
            serde_json::from_slice::<serde_json::Value>(&trust_document(&key_pair)).unwrap();
        malformed_state["keys"][0]["state"] = serde_json::json!("unknown");
        assert!(CentralCommandTrustBundle::from_json(
            &serde_json::to_vec(&malformed_state).unwrap()
        )
        .is_err());
    }

    #[test]
    fn strict_document_rejects_malformed_spki_and_empty_or_oversized_key_sets() {
        let key_pair = signing_key(8);
        let mut malformed =
            serde_json::from_slice::<serde_json::Value>(&trust_document(&key_pair)).unwrap();
        malformed["keys"][0]["public_key_spki"] = serde_json::json!("not-an-ed25519-spki");
        assert!(
            CentralCommandTrustBundle::from_json(&serde_json::to_vec(&malformed).unwrap()).is_err()
        );

        assert!(
            CentralCommandTrustBundle::from_json(br#"{"schema_version":1,"keys":[]}"#).is_err()
        );

        let keys = (0..=MAX_TRUST_KEYS)
            .map(|index| {
                serde_json::json!({
                    "key_id": format!("central-{index}"),
                    "certificate_generation": "1",
                    "public_key_spki": public_key(&key_pair),
                    "state": "active"
                })
            })
            .collect::<Vec<_>>();
        assert!(CentralCommandTrustBundle::from_json(
            &serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "keys": keys
            }))
            .unwrap()
        )
        .is_err());
    }

    #[test]
    fn loader_accepts_a_kubernetes_style_symlink() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("projected-command-trust.json");
        let link = directory.path().join("central-command-trust.json");
        fs::write(&target, trust_document(&signing_key(9))).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();
        #[cfg(not(unix))]
        fs::copy(&target, &link).unwrap();

        CentralCommandTrustBundle::load(&link).unwrap();
    }

    #[test]
    fn loader_rejects_relative_empty_and_oversized_files() {
        assert!(CentralCommandTrustBundle::load(Path::new("relative.json")).is_err());
        let directory = tempfile::tempdir().unwrap();
        assert!(CentralCommandTrustBundle::load(directory.path()).is_err());
        let path = directory.path().join("command-trust.json");
        fs::write(&path, []).unwrap();
        assert!(CentralCommandTrustBundle::load(&path).is_err());
        fs::write(&path, vec![b' '; MAX_TRUST_BUNDLE_BYTES as usize + 1]).unwrap();
        assert!(CentralCommandTrustBundle::load(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn loader_rejects_group_or_world_writable_files() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("command-trust.json");
        fs::write(&path, trust_document(&signing_key(10))).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(CentralCommandTrustBundle::load(&path).is_err());
    }

    #[test]
    fn active_and_retiring_keys_verify_exact_command_payloads() {
        let key = signing_key(11);
        let mut frame = decision_frame();
        sign_frame(&mut frame, &key, 1, 1_000, 2_000);
        trust_bundle(&key, 1, TrustKeyState::Active)
            .verify_downstream_if_command(&frame, UnixMillis::new(1_001))
            .unwrap();
        trust_bundle(&key, 1, TrustKeyState::Retiring)
            .verify_downstream_if_command(&frame, UnixMillis::new(1_001))
            .unwrap();

        trust_bundle(&key, 1, TrustKeyState::Active)
            .verify_downstream_if_command(&error_frame(), UnixMillis::new(1_001))
            .unwrap();
    }

    #[test]
    fn verification_rejects_missing_tampered_and_wrong_signatures() {
        let key = signing_key(12);
        let bundle = trust_bundle(&key, 1, TrustKeyState::Active);
        assert!(bundle
            .verify_downstream_if_command(&decision_frame(), UnixMillis::new(1_001))
            .unwrap_err()
            .to_string()
            .contains("missing"));

        let mut signed = decision_frame();
        sign_frame(&mut signed, &key, 1, 1_000, 2_000);
        let mut tampered = signed.clone();
        tampered.sent_at_unix_ms = UnixMillis::new(1_001);
        assert!(bundle
            .verify_downstream_if_command(&tampered, UnixMillis::new(1_001))
            .unwrap_err()
            .to_string()
            .contains("does not match"));

        signed.central_signature.as_mut().unwrap().signature =
            Ed25519Signature::from_bytes([0; 64]);
        assert!(bundle
            .verify_downstream_if_command(&signed, UnixMillis::new(1_001))
            .is_err());
    }

    #[test]
    fn verification_rejects_unknown_mismatched_and_revoked_keys() {
        let key = signing_key(13);
        let bundle = trust_bundle(&key, 1, TrustKeyState::Active);
        let mut frame = decision_frame();
        sign_frame(&mut frame, &key, 1, 1_000, 2_000);

        frame.central_signature.as_mut().unwrap().key_id = "unknown-key".to_owned();
        assert!(bundle
            .verify_downstream_if_command(&frame, UnixMillis::new(1_001))
            .unwrap_err()
            .to_string()
            .contains("does not contain"));

        frame.central_signature.as_mut().unwrap().key_id = "central-command-a".to_owned();
        frame
            .central_signature
            .as_mut()
            .unwrap()
            .certificate_generation = CertificateGeneration::new(2);
        assert!(bundle
            .verify_downstream_if_command(&frame, UnixMillis::new(1_001))
            .unwrap_err()
            .to_string()
            .contains("generation"));

        let mut revoked = decision_frame();
        sign_frame(&mut revoked, &key, 1, 1_000, 2_000);
        assert!(trust_bundle(&key, 1, TrustKeyState::Revoked)
            .verify_downstream_if_command(&revoked, UnixMillis::new(1_001))
            .unwrap_err()
            .to_string()
            .contains("revoked"));
    }

    #[test]
    fn verification_rejects_expired_future_and_excessive_ttl_signatures() {
        let key = signing_key(14);
        let bundle = trust_bundle(&key, 1, TrustKeyState::Active);
        let mut frame = decision_frame();
        sign_frame(&mut frame, &key, 1, 1_000, 2_000);
        assert!(bundle
            .verify_downstream_if_command(&frame, UnixMillis::new(999))
            .is_err());
        assert!(bundle
            .verify_downstream_if_command(&frame, UnixMillis::new(2_000))
            .is_err());

        let mut excessive_ttl = decision_frame();
        sign_frame(
            &mut excessive_ttl,
            &key,
            1,
            1_000,
            1_000 + MAX_COMMAND_TTL_MS + 1,
        );
        assert!(bundle
            .verify_downstream_if_command(&excessive_ttl, UnixMillis::new(1_001))
            .unwrap_err()
            .to_string()
            .contains("TTL"));
    }
}
