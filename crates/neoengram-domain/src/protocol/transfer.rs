//! QUIC data-plane transfer contracts.
//!
//! Control messages remain strict Envelope/NDJSON.  Object bytes never use JSON: this module
//! defines the small, bounded, length-prefixed binary frame used on a QUIC transfer stream.  The
//! codec is transport-neutral and can be used by Quinn, a test transport, or a future archive
//! backend without coupling the domain crate to a network runtime.

use std::{fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::materialization::{BatchManifest, BatchManifestPage, SignedMaterializationBatchTicket};
use super::placement::{CommitObjectSet, ObjectSet};
use super::validation::parse_unique_json;
use crate::{
    AgentId, ArtifactId, CentralSignedPayload, CommitId, ContentDigest, DecimalU64, EdgeClusterId,
    GatewayPoolId, MountGeneration, ObjectId, PlacementId, ProtocolError, ProtocolResult,
    RouteGeneration, SessionGeneration, StorageVolumeId, TenantId, TransferId, UnixMillis,
};

/// ALPN negotiated by all v2 object materialization QUIC connections.
pub const TRANSFER_ALPN: &str = "neoengram-transfer-v2";
/// Legacy v1 ALPN retained only as a named migration marker.  v2 listeners must not advertise or
/// accept it.
#[deprecated(note = "v1 transfer is not accepted by the clean-slate materialization protocol")]
pub const TRANSFER_ALPN_V1: &str = "neoengram-transfer-v1";
/// Maximum encoded frame, including the four-byte length prefix and one-byte kind.
pub const MAX_TRANSFER_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Maximum ObjectChunk payload accepted by one frame.  Ranges larger than this are split by the
/// transfer scheduler rather than allowing an unbounded allocation in a Gateway or Agent.
pub const MAX_TRANSFER_CHUNK_BYTES: usize = 1024 * 1024;
const MAX_TRANSFER_STRING_BYTES: usize = 4096;
const MAX_TRANSFER_OBJECTS: usize = 65_535;

/// The physical endpoints bound into a short-lived TransferTicket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferEndpoint {
    pub placement_id: PlacementId,
    pub agent_id: AgentId,
    pub gateway_pool_id: GatewayPoolId,
    pub edge_cluster_id: EdgeClusterId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_volume_id: Option<StorageVolumeId>,
}

impl TransferEndpoint {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.placement_id.as_str().is_empty()
            || self.agent_id.as_str().is_empty()
            || self.gateway_pool_id.as_str().is_empty()
            || self.edge_cluster_id.as_str().is_empty()
        {
            return Err(ProtocolError::InvalidField {
                field: "transfer_endpoint",
                reason: "endpoint identities must not be empty".to_owned(),
            });
        }
        Ok(())
    }
}

/// Central-issued, short-lived capability for exactly one object-set transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferTicket {
    pub transfer_id: TransferId,
    pub tenant_id: TenantId,
    /// Artifact namespace for the artifact-scoped Volume CAS. This is signed together with the
    /// rest of the transfer capability so an assignment cannot redirect bytes into another
    /// artifact's object namespace.
    pub artifact_id: ArtifactId,
    pub commit_id: CommitId,
    pub object_set_digest: ContentDigest,
    pub source: TransferEndpoint,
    pub target: TransferEndpoint,
    /// Source route fences are kept separately from the target Agent fences. A relay must not
    /// reuse a target session or mount generation when it opens the source side of a transfer.
    pub source_session_generation: SessionGeneration,
    pub source_mount_generation: MountGeneration,
    pub source_route_generation: RouteGeneration,
    /// Target route fences used by the receiving Agent/Gateway.
    pub session_generation: SessionGeneration,
    pub mount_generation: MountGeneration,
    pub route_generation: RouteGeneration,
    pub deadline_unix_ms: UnixMillis,
    pub max_bytes: DecimalU64,
    /// The exact Object IDs permitted by this capability.  An empty list is valid for an empty
    /// Commit and is never interpreted as “all objects”.
    pub allowed_objects: Vec<ObjectId>,
}

/// A Central-authenticated transfer capability. The unsigned ticket remains a value object for
/// deterministic local workers; network-facing hops carry this envelope so the exact Tenant,
/// ObjectSet, endpoints, generations, byte limit, and expiry are signed together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignedTransferTicket {
    pub ticket: TransferTicket,
    pub central_signature: CentralSignedPayload,
}

impl SignedTransferTicket {
    pub fn payload_bytes(ticket: &TransferTicket) -> ProtocolResult<Vec<u8>> {
        serde_json::to_vec(ticket).map_err(|error| ProtocolError::Serialization(error.to_string()))
    }

    pub fn new(
        ticket: TransferTicket,
        central_signature: CentralSignedPayload,
    ) -> ProtocolResult<Self> {
        ticket.validate()?;
        central_signature.validate()?;
        if central_signature.payload.as_bytes() != Self::payload_bytes(&ticket)? {
            return Err(ProtocolError::InvalidField {
                field: "central_signature",
                reason: "signature payload does not match TransferTicket".to_owned(),
            });
        }
        Ok(Self {
            ticket,
            central_signature,
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.ticket.validate()?;
        self.central_signature.validate()?;
        if self.central_signature.payload.as_bytes() != Self::payload_bytes(&self.ticket)? {
            return Err(ProtocolError::InvalidField {
                field: "central_signature",
                reason: "signature payload does not match TransferTicket".to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn as_ticket(&self) -> &TransferTicket {
        &self.ticket
    }
}

impl TransferTicket {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.source.validate()?;
        self.target.validate()?;
        for (field, value) in [
            (
                "source_session_generation",
                self.source_session_generation.get(),
            ),
            (
                "source_mount_generation",
                self.source_mount_generation.get(),
            ),
            (
                "source_route_generation",
                self.source_route_generation.get(),
            ),
            ("session_generation", self.session_generation.get()),
            ("mount_generation", self.mount_generation.get()),
            ("route_generation", self.route_generation.get()),
        ] {
            if value == 0 {
                return Err(ProtocolError::InvalidField {
                    field,
                    reason: "must be greater than zero".to_owned(),
                });
            }
        }
        if self.deadline_unix_ms.get() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "deadline_unix_ms",
                reason: "must be a positive Unix timestamp".to_owned(),
            });
        }
        if self.allowed_objects.len() > MAX_TRANSFER_OBJECTS {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "allowed_objects",
                limit: MAX_TRANSFER_OBJECTS,
                actual: self.allowed_objects.len(),
            });
        }
        let mut sorted = self.allowed_objects.clone();
        sorted.sort_unstable();
        if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ProtocolError::InvalidField {
                field: "allowed_objects",
                reason: "object IDs must be unique".to_owned(),
            });
        }
        if sorted != self.allowed_objects {
            return Err(ProtocolError::InvalidField {
                field: "allowed_objects",
                reason: "object IDs must be in ascending canonical order".to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn allows(&self, object_id: ObjectId) -> bool {
        self.allowed_objects.binary_search(&object_id).is_ok()
    }
}

/// Frame kind tags are stable and intentionally not serde/JSON representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FrameKind {
    OpenTransfer = 1,
    ObjectRequest = 2,
    ObjectChunk = 3,
    ObjectProof = 4,
    ObjectAck = 5,
    CommitObjectSet = 6,
    TransferError = 7,
    CloseTransfer = 8,
    OpenTransferSigned = 9,
    /// A clean-slate v2 materialization capability. This is intentionally a distinct frame kind
    /// from the legacy whole-Commit opening frames so a v1 ticket cannot be replayed on the v2
    /// ALPN by a listener that only performs structural decoding.
    OpenMaterializationSigned = 10,
    MaterializationManifest = 11,
    MaterializationManifestPage = 12,
    /// Network-only capability probe. It carries no business ticket or object scope; the
    /// Gateway validates the negotiated v2 ALPN and mTLS peer before replying with Ack.
    Preflight = 13,
    PreflightAck = 14,
}

impl TryFrom<u8> for FrameKind {
    type Error = TransferFrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => Self::OpenTransfer,
            2 => Self::ObjectRequest,
            3 => Self::ObjectChunk,
            4 => Self::ObjectProof,
            5 => Self::ObjectAck,
            6 => Self::CommitObjectSet,
            7 => Self::TransferError,
            8 => Self::CloseTransfer,
            9 => Self::OpenTransferSigned,
            10 => Self::OpenMaterializationSigned,
            11 => Self::MaterializationManifest,
            12 => Self::MaterializationManifestPage,
            13 => Self::Preflight,
            14 => Self::PreflightAck,
            _ => return Err(TransferFrameError::UnknownKind(value)),
        })
    }
}

/// A binary object-transfer frame.  All object bytes are carried only by `ObjectChunk`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum TransferFrame {
    OpenTransfer(TransferTicket),
    OpenTransferSigned(SignedTransferTicket),
    OpenMaterializationSigned(SignedMaterializationBatchTicket),
    MaterializationManifest(BatchManifest),
    MaterializationManifestPage(BatchManifestPage),
    Preflight,
    PreflightAck,
    ObjectRequest(ObjectRequest),
    ObjectChunk(ObjectChunk),
    ObjectProof(ObjectProof),
    ObjectAck(ObjectAck),
    CommitObjectSet(CommitObjectSet),
    TransferError(TransferError),
    CloseTransfer(CloseTransfer),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRequest {
    pub object_id: ObjectId,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectChunk {
    pub object_id: ObjectId,
    pub offset: u64,
    pub bytes: Vec<u8>,
}

impl ObjectChunk {
    pub fn new(
        object_id: ObjectId,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, TransferFrameError> {
        if bytes.len() > MAX_TRANSFER_CHUNK_BYTES {
            return Err(TransferFrameError::LimitExceeded {
                field: "bytes",
                limit: MAX_TRANSFER_CHUNK_BYTES,
                actual: bytes.len(),
            });
        }
        Ok(Self {
            object_id,
            offset,
            bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectProof {
    pub object_id: ObjectId,
    pub digest: ContentDigest,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAck {
    pub object_id: ObjectId,
    pub offset: u64,
    pub length: u64,
    pub accepted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransferErrorCode {
    InvalidTicket = 1,
    Fenced = 2,
    SourceUnavailable = 3,
    DataUnavailable = 4,
    DigestMismatch = 5,
    DeadlineExceeded = 6,
    Cancelled = 7,
    Internal = 8,
}

impl TryFrom<u8> for TransferErrorCode {
    type Error = TransferFrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => Self::InvalidTicket,
            2 => Self::Fenced,
            3 => Self::SourceUnavailable,
            4 => Self::DataUnavailable,
            5 => Self::DigestMismatch,
            6 => Self::DeadlineExceeded,
            7 => Self::Cancelled,
            8 => Self::Internal,
            _ => return Err(TransferFrameError::UnknownErrorCode(value)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferError {
    pub code: TransferErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseTransfer {
    pub committed: bool,
}

/// Errors returned by the bounded binary codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferFrameError {
    Truncated,
    UnknownKind(u8),
    UnknownErrorCode(u8),
    InvalidField(&'static str),
    InvalidIdentifier(String),
    InvalidDigest,
    InvalidUtf8,
    LimitExceeded {
        field: &'static str,
        limit: usize,
        actual: usize,
    },
}

impl fmt::Display for TransferFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated transfer frame"),
            Self::UnknownKind(kind) => write!(formatter, "unknown transfer frame kind {kind}"),
            Self::UnknownErrorCode(code) => write!(formatter, "unknown transfer error code {code}"),
            Self::InvalidField(field) => write!(formatter, "invalid transfer field {field}"),
            Self::InvalidIdentifier(value) => {
                write!(formatter, "invalid transfer identifier {value}")
            }
            Self::InvalidDigest => formatter.write_str("invalid transfer digest"),
            Self::InvalidUtf8 => formatter.write_str("invalid UTF-8 in transfer frame"),
            Self::LimitExceeded {
                field,
                limit,
                actual,
            } => {
                write!(
                    formatter,
                    "transfer field {field} exceeds {limit}: {actual}"
                )
            }
        }
    }
}

impl std::error::Error for TransferFrameError {}

impl TransferFrame {
    /// Encodes exactly one length-prefixed frame.
    pub fn encode(&self) -> Result<Vec<u8>, TransferFrameError> {
        let mut payload = Vec::new();
        match self {
            Self::OpenTransfer(ticket) => {
                ticket
                    .validate()
                    .map_err(|_| TransferFrameError::InvalidField("ticket"))?;
                payload.push(FrameKind::OpenTransfer as u8);
                encode_ticket(&mut payload, ticket)?;
            }
            Self::OpenTransferSigned(ticket) => {
                ticket
                    .validate()
                    .map_err(|_| TransferFrameError::InvalidField("signed_ticket"))?;
                payload.push(FrameKind::OpenTransferSigned as u8);
                encode_signed_ticket(&mut payload, ticket)?;
            }
            Self::OpenMaterializationSigned(ticket) => {
                ticket
                    .validate()
                    .map_err(|_| TransferFrameError::InvalidField("materialization_ticket"))?;
                payload.push(FrameKind::OpenMaterializationSigned as u8);
                encode_signed_materialization_ticket(&mut payload, ticket)?;
            }
            Self::MaterializationManifest(manifest) => {
                manifest
                    .validate()
                    .map_err(|_| TransferFrameError::InvalidField("materialization_manifest"))?;
                payload.push(FrameKind::MaterializationManifest as u8);
                encode_materialization_json(&mut payload, manifest, "materialization_manifest")?;
            }
            Self::MaterializationManifestPage(page) => {
                page.validate().map_err(|_| {
                    TransferFrameError::InvalidField("materialization_manifest_page")
                })?;
                payload.push(FrameKind::MaterializationManifestPage as u8);
                encode_materialization_json(&mut payload, page, "materialization_manifest_page")?;
            }
            Self::Preflight => payload.push(FrameKind::Preflight as u8),
            Self::PreflightAck => payload.push(FrameKind::PreflightAck as u8),
            Self::ObjectRequest(request) => {
                payload.push(FrameKind::ObjectRequest as u8);
                put_object_id(&mut payload, request.object_id);
                put_u64(&mut payload, request.offset);
                put_u64(&mut payload, request.length);
            }
            Self::ObjectChunk(chunk) => {
                if chunk.bytes.len() > MAX_TRANSFER_CHUNK_BYTES {
                    return Err(TransferFrameError::LimitExceeded {
                        field: "bytes",
                        limit: MAX_TRANSFER_CHUNK_BYTES,
                        actual: chunk.bytes.len(),
                    });
                }
                payload.push(FrameKind::ObjectChunk as u8);
                put_object_id(&mut payload, chunk.object_id);
                put_u64(&mut payload, chunk.offset);
                put_bytes(&mut payload, &chunk.bytes)?;
            }
            Self::ObjectProof(proof) => {
                payload.push(FrameKind::ObjectProof as u8);
                put_object_id(&mut payload, proof.object_id);
                put_digest(&mut payload, proof.digest);
                put_u64(&mut payload, proof.size);
            }
            Self::ObjectAck(ack) => {
                payload.push(FrameKind::ObjectAck as u8);
                put_object_id(&mut payload, ack.object_id);
                put_u64(&mut payload, ack.offset);
                put_u64(&mut payload, ack.length);
                payload.push(u8::from(ack.accepted));
            }
            Self::CommitObjectSet(set) => {
                set.validate()
                    .map_err(|_| TransferFrameError::InvalidField("object_set"))?;
                payload.push(FrameKind::CommitObjectSet as u8);
                encode_commit_object_set(&mut payload, set)?;
            }
            Self::TransferError(error) => {
                if error.message.len() > MAX_TRANSFER_STRING_BYTES {
                    return Err(TransferFrameError::LimitExceeded {
                        field: "message",
                        limit: MAX_TRANSFER_STRING_BYTES,
                        actual: error.message.len(),
                    });
                }
                payload.push(FrameKind::TransferError as u8);
                payload.push(error.code as u8);
                put_string(&mut payload, &error.message)?;
            }
            Self::CloseTransfer(close) => {
                payload.push(FrameKind::CloseTransfer as u8);
                payload.push(u8::from(close.committed));
            }
        }
        let total = payload
            .len()
            .checked_add(4)
            .ok_or(TransferFrameError::LimitExceeded {
                field: "frame",
                limit: MAX_TRANSFER_FRAME_BYTES,
                actual: usize::MAX,
            })?;
        if total > MAX_TRANSFER_FRAME_BYTES {
            return Err(TransferFrameError::LimitExceeded {
                field: "frame",
                limit: MAX_TRANSFER_FRAME_BYTES,
                actual: total,
            });
        }
        let length =
            u32::try_from(payload.len()).map_err(|_| TransferFrameError::LimitExceeded {
                field: "frame",
                limit: u32::MAX as usize,
                actual: payload.len(),
            })?;
        let mut encoded = Vec::with_capacity(total);
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    /// Decodes exactly one frame and rejects trailing bytes.
    pub fn decode(encoded: &[u8]) -> Result<Self, TransferFrameError> {
        if encoded.len() < 5 {
            return Err(TransferFrameError::Truncated);
        }
        if encoded.len() > MAX_TRANSFER_FRAME_BYTES {
            return Err(TransferFrameError::LimitExceeded {
                field: "frame",
                limit: MAX_TRANSFER_FRAME_BYTES,
                actual: encoded.len(),
            });
        }
        let declared =
            u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize;
        if declared != encoded.len() - 4 {
            return Err(TransferFrameError::InvalidField("length_prefix"));
        }
        let mut reader = Reader::new(&encoded[4..]);
        let kind = FrameKind::try_from(reader.byte()?)?;
        let frame = match kind {
            FrameKind::OpenTransfer => Self::OpenTransfer(decode_ticket(&mut reader)?),
            FrameKind::OpenTransferSigned => {
                Self::OpenTransferSigned(decode_signed_ticket(&mut reader)?)
            }
            FrameKind::OpenMaterializationSigned => {
                Self::OpenMaterializationSigned(decode_signed_materialization_ticket(&mut reader)?)
            }
            FrameKind::MaterializationManifest => {
                let manifest: BatchManifest =
                    decode_materialization_json(&mut reader, "materialization_manifest")?;
                manifest
                    .validate()
                    .map_err(|_| TransferFrameError::InvalidField("materialization_manifest"))?;
                Self::MaterializationManifest(manifest)
            }
            FrameKind::MaterializationManifestPage => {
                let page: BatchManifestPage =
                    decode_materialization_json(&mut reader, "materialization_manifest_page")?;
                page.validate().map_err(|_| {
                    TransferFrameError::InvalidField("materialization_manifest_page")
                })?;
                Self::MaterializationManifestPage(page)
            }
            FrameKind::Preflight => Self::Preflight,
            FrameKind::PreflightAck => Self::PreflightAck,
            FrameKind::ObjectRequest => Self::ObjectRequest(ObjectRequest {
                object_id: reader.object_id()?,
                offset: reader.u64()?,
                // A zero-length request is the explicit completion handshake for an empty
                // object.  The frame does not carry the object size, so source-side sessions
                // enforce that zero is only valid for an empty ObjectRef.
                length: reader.u64()?,
            }),
            FrameKind::ObjectChunk => {
                let object_id = reader.object_id()?;
                let offset = reader.u64()?;
                let bytes = reader.bytes(MAX_TRANSFER_CHUNK_BYTES)?;
                Self::ObjectChunk(ObjectChunk {
                    object_id,
                    offset,
                    bytes,
                })
            }
            FrameKind::ObjectProof => Self::ObjectProof(ObjectProof {
                object_id: reader.object_id()?,
                digest: reader.digest()?,
                size: reader.u64()?,
            }),
            FrameKind::ObjectAck => Self::ObjectAck(ObjectAck {
                object_id: reader.object_id()?,
                offset: reader.u64()?,
                length: reader.u64()?,
                accepted: match reader.byte()? {
                    0 => false,
                    1 => true,
                    _ => return Err(TransferFrameError::InvalidField("accepted")),
                },
            }),
            FrameKind::CommitObjectSet => {
                Self::CommitObjectSet(decode_commit_object_set(&mut reader)?)
            }
            FrameKind::TransferError => Self::TransferError(TransferError {
                code: TransferErrorCode::try_from(reader.byte()?)?,
                message: reader.string()?,
            }),
            FrameKind::CloseTransfer => Self::CloseTransfer(CloseTransfer {
                committed: match reader.byte()? {
                    0 => false,
                    1 => true,
                    _ => return Err(TransferFrameError::InvalidField("committed")),
                },
            }),
        };
        if !reader.finished() {
            return Err(TransferFrameError::InvalidField("trailing_bytes"));
        }
        Ok(frame)
    }
}

fn encode_ticket(out: &mut Vec<u8>, ticket: &TransferTicket) -> Result<(), TransferFrameError> {
    put_string(out, ticket.transfer_id.as_str())?;
    put_string(out, ticket.tenant_id.as_str())?;
    put_string(out, ticket.artifact_id.as_str())?;
    put_digest(out, ticket.commit_id.digest());
    put_digest(out, ticket.object_set_digest);
    encode_endpoint(out, &ticket.source)?;
    encode_endpoint(out, &ticket.target)?;
    put_u64(out, ticket.source_session_generation.get());
    put_u64(out, ticket.source_mount_generation.get());
    put_u64(out, ticket.source_route_generation.get());
    put_u64(out, ticket.session_generation.get());
    put_u64(out, ticket.mount_generation.get());
    put_u64(out, ticket.route_generation.get());
    put_u64(out, ticket.deadline_unix_ms.get());
    put_u64(out, ticket.max_bytes.get());
    let count = u16::try_from(ticket.allowed_objects.len()).map_err(|_| {
        TransferFrameError::LimitExceeded {
            field: "allowed_objects",
            limit: MAX_TRANSFER_OBJECTS,
            actual: ticket.allowed_objects.len(),
        }
    })?;
    out.extend_from_slice(&count.to_be_bytes());
    for object_id in &ticket.allowed_objects {
        put_object_id(out, *object_id);
    }
    Ok(())
}

fn encode_signed_ticket(
    out: &mut Vec<u8>,
    ticket: &SignedTransferTicket,
) -> Result<(), TransferFrameError> {
    let encoded = serde_json::to_vec(ticket)
        .map_err(|_| TransferFrameError::InvalidField("signed_ticket"))?;
    if encoded.len() > MAX_TRANSFER_FRAME_BYTES {
        return Err(TransferFrameError::LimitExceeded {
            field: "signed_ticket",
            limit: MAX_TRANSFER_FRAME_BYTES,
            actual: encoded.len(),
        });
    }
    put_bytes(out, &encoded)
}

fn decode_signed_ticket(
    reader: &mut Reader<'_>,
) -> Result<SignedTransferTicket, TransferFrameError> {
    let bytes = reader.bytes(MAX_TRANSFER_FRAME_BYTES)?;
    let value =
        parse_unique_json(&bytes).map_err(|_| TransferFrameError::InvalidField("signed_ticket"))?;
    let ticket: SignedTransferTicket = serde_json::from_value(value)
        .map_err(|_| TransferFrameError::InvalidField("signed_ticket"))?;
    ticket
        .validate()
        .map_err(|_| TransferFrameError::InvalidField("signed_ticket"))?;
    Ok(ticket)
}

fn encode_signed_materialization_ticket(
    out: &mut Vec<u8>,
    ticket: &SignedMaterializationBatchTicket,
) -> Result<(), TransferFrameError> {
    let encoded = serde_json::to_vec(ticket)
        .map_err(|_| TransferFrameError::InvalidField("materialization_ticket"))?;
    if encoded.len() > MAX_TRANSFER_FRAME_BYTES {
        return Err(TransferFrameError::LimitExceeded {
            field: "materialization_ticket",
            limit: MAX_TRANSFER_FRAME_BYTES,
            actual: encoded.len(),
        });
    }
    put_bytes(out, &encoded)
}

fn decode_signed_materialization_ticket(
    reader: &mut Reader<'_>,
) -> Result<SignedMaterializationBatchTicket, TransferFrameError> {
    let bytes = reader.bytes(MAX_TRANSFER_FRAME_BYTES)?;
    // Ticket JSON is part of the signed capability.  Parse through the duplicate-key rejecting
    // decoder before deserializing so an intermediary cannot make two implementations disagree
    // about which duplicate member was covered by the signature.
    let value = parse_unique_json(&bytes)
        .map_err(|_| TransferFrameError::InvalidField("materialization_ticket"))?;
    let ticket: SignedMaterializationBatchTicket = serde_json::from_value(value)
        .map_err(|_| TransferFrameError::InvalidField("materialization_ticket"))?;
    ticket
        .validate()
        .map_err(|_| TransferFrameError::InvalidField("materialization_ticket"))?;
    Ok(ticket)
}

fn encode_materialization_json<T: Serialize>(
    out: &mut Vec<u8>,
    value: &T,
    field: &'static str,
) -> Result<(), TransferFrameError> {
    let encoded = serde_json::to_vec(value).map_err(|_| TransferFrameError::InvalidField(field))?;
    if encoded.len() > MAX_TRANSFER_FRAME_BYTES {
        return Err(TransferFrameError::LimitExceeded {
            field,
            limit: MAX_TRANSFER_FRAME_BYTES,
            actual: encoded.len(),
        });
    }
    put_blob(out, &encoded, MAX_TRANSFER_FRAME_BYTES, field)
}

fn decode_materialization_json<'a, T: for<'de> Deserialize<'de>>(
    reader: &mut Reader<'a>,
    field: &'static str,
) -> Result<T, TransferFrameError> {
    let bytes = reader.bytes(MAX_TRANSFER_FRAME_BYTES)?;
    // Manifest descriptors/pages are authenticated indirectly by the ticket's digest. Reject
    // duplicate JSON members before normal Serde decoding to keep that digest unambiguous.
    let value = parse_unique_json(&bytes).map_err(|_| TransferFrameError::InvalidField(field))?;
    serde_json::from_value(value).map_err(|_| TransferFrameError::InvalidField(field))
}

fn decode_ticket(reader: &mut Reader<'_>) -> Result<TransferTicket, TransferFrameError> {
    let ticket = TransferTicket {
        transfer_id: reader.id("transfer ID")?,
        tenant_id: reader.id("tenant ID")?,
        artifact_id: reader.id("artifact ID")?,
        commit_id: CommitId::from_digest(reader.digest()?),
        object_set_digest: reader.digest()?,
        source: decode_endpoint(reader)?,
        target: decode_endpoint(reader)?,
        source_session_generation: SessionGeneration::new(
            reader.u64_nonzero("source_session_generation")?,
        ),
        source_mount_generation: MountGeneration::new(
            reader.u64_nonzero("source_mount_generation")?,
        ),
        source_route_generation: RouteGeneration::new(
            reader.u64_nonzero("source_route_generation")?,
        ),
        session_generation: SessionGeneration::new(reader.u64_nonzero("session_generation")?),
        mount_generation: MountGeneration::new(reader.u64_nonzero("mount_generation")?),
        route_generation: RouteGeneration::new(reader.u64_nonzero("route_generation")?),
        deadline_unix_ms: UnixMillis::new(reader.u64_nonzero("deadline_unix_ms")?),
        max_bytes: DecimalU64::new(reader.u64()?),
        allowed_objects: {
            let count = reader.u16()? as usize;
            if count > MAX_TRANSFER_OBJECTS {
                return Err(TransferFrameError::LimitExceeded {
                    field: "allowed_objects",
                    limit: MAX_TRANSFER_OBJECTS,
                    actual: count,
                });
            }
            let mut objects = Vec::with_capacity(count);
            for _ in 0..count {
                objects.push(reader.object_id()?);
            }
            objects
        },
    };
    ticket
        .validate()
        .map_err(|_| TransferFrameError::InvalidField("ticket"))?;
    Ok(ticket)
}

fn encode_endpoint(
    out: &mut Vec<u8>,
    endpoint: &TransferEndpoint,
) -> Result<(), TransferFrameError> {
    put_string(out, endpoint.placement_id.as_str())?;
    put_string(out, endpoint.agent_id.as_str())?;
    put_string(out, endpoint.gateway_pool_id.as_str())?;
    put_string(out, endpoint.edge_cluster_id.as_str())?;
    match &endpoint.storage_volume_id {
        Some(volume) => {
            out.push(1);
            put_string(out, volume.as_str())?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn decode_endpoint(reader: &mut Reader<'_>) -> Result<TransferEndpoint, TransferFrameError> {
    let endpoint = TransferEndpoint {
        placement_id: reader.id("placement ID")?,
        agent_id: reader.id("agent ID")?,
        gateway_pool_id: reader.id("gateway pool ID")?,
        edge_cluster_id: reader.id("edge cluster ID")?,
        storage_volume_id: match reader.byte()? {
            0 => None,
            1 => Some(reader.id("storage volume ID")?),
            _ => return Err(TransferFrameError::InvalidField("storage_volume_id")),
        },
    };
    endpoint
        .validate()
        .map_err(|_| TransferFrameError::InvalidField("endpoint"))?;
    Ok(endpoint)
}

fn encode_commit_object_set(
    out: &mut Vec<u8>,
    set: &CommitObjectSet,
) -> Result<(), TransferFrameError> {
    put_string(out, set.tenant_id.as_str())?;
    put_digest(out, set.commit_id.digest());
    put_digest(out, set.object_set.object_set_digest);
    let count = u32::try_from(set.object_set.objects.len()).map_err(|_| {
        TransferFrameError::LimitExceeded {
            field: "objects",
            limit: MAX_TRANSFER_OBJECTS,
            actual: set.object_set.objects.len(),
        }
    })?;
    out.extend_from_slice(&count.to_be_bytes());
    for object in &set.object_set.objects {
        put_object_id(out, object.object_id);
        put_u64(out, object.size.get());
        put_u64(out, object.ordinal.get());
        out.push(match object.encoding {
            super::placement::ObjectEncoding::Raw => 0,
            super::placement::ObjectEncoding::Zstd => 1,
        });
    }
    Ok(())
}

fn decode_commit_object_set(
    reader: &mut Reader<'_>,
) -> Result<CommitObjectSet, TransferFrameError> {
    let tenant_id = reader.id("tenant ID")?;
    let commit_id = CommitId::from_digest(reader.digest()?);
    let object_set_digest = reader.digest()?;
    let count = reader.u32()? as usize;
    if count > MAX_TRANSFER_OBJECTS {
        return Err(TransferFrameError::LimitExceeded {
            field: "objects",
            limit: MAX_TRANSFER_OBJECTS,
            actual: count,
        });
    }
    let mut objects = Vec::with_capacity(count);
    for _ in 0..count {
        let object_id = reader.object_id()?;
        let size = reader.u64()?;
        let ordinal = reader.u64()?;
        let encoding = match reader.byte()? {
            0 => super::placement::ObjectEncoding::Raw,
            1 => super::placement::ObjectEncoding::Zstd,
            _ => return Err(TransferFrameError::InvalidField("encoding")),
        };
        objects.push(super::placement::CommitObject::new(
            object_id, size, encoding, ordinal,
        ));
    }
    let object_set = ObjectSet {
        object_set_digest,
        objects,
    };
    let set = CommitObjectSet {
        tenant_id,
        commit_id,
        object_set,
    };
    set.validate()
        .map_err(|_| TransferFrameError::InvalidField("object_set"))?;
    Ok(set)
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_object_id(out: &mut Vec<u8>, object_id: ObjectId) {
    out.extend_from_slice(object_id.as_bytes());
}

fn put_digest(out: &mut Vec<u8>, digest: ContentDigest) {
    out.extend_from_slice(digest.as_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), TransferFrameError> {
    if value.len() > MAX_TRANSFER_STRING_BYTES || value.len() > u16::MAX as usize {
        return Err(TransferFrameError::LimitExceeded {
            field: "string",
            limit: MAX_TRANSFER_STRING_BYTES,
            actual: value.len(),
        });
    }
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), TransferFrameError> {
    put_blob(out, bytes, MAX_TRANSFER_CHUNK_BYTES, "bytes")
}

fn put_blob(
    out: &mut Vec<u8>,
    bytes: &[u8],
    max: usize,
    field: &'static str,
) -> Result<(), TransferFrameError> {
    if bytes.len() > max || bytes.len() > u32::MAX as usize {
        return Err(TransferFrameError::LimitExceeded {
            field,
            limit: max,
            actual: bytes.len(),
        });
    }
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], TransferFrameError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(TransferFrameError::Truncated)?;
        if end > self.bytes.len() {
            return Err(TransferFrameError::Truncated);
        }
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn byte(&mut self) -> Result<u8, TransferFrameError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, TransferFrameError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("checked length"),
        ))
    }

    fn u32(&mut self) -> Result<u32, TransferFrameError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("checked length"),
        ))
    }

    fn u64(&mut self) -> Result<u64, TransferFrameError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("checked length"),
        ))
    }

    fn u64_nonzero(&mut self, field: &'static str) -> Result<u64, TransferFrameError> {
        let value = self.u64()?;
        if value == 0 {
            return Err(TransferFrameError::InvalidField(field));
        }
        Ok(value)
    }

    fn digest(&mut self) -> Result<ContentDigest, TransferFrameError> {
        Ok(ContentDigest::from_bytes(
            self.take(32)?.try_into().expect("checked length"),
        ))
    }

    fn object_id(&mut self) -> Result<ObjectId, TransferFrameError> {
        Ok(ObjectId::from_bytes(
            self.take(32)?.try_into().expect("checked length"),
        ))
    }

    fn string(&mut self) -> Result<String, TransferFrameError> {
        let length = self.u16()? as usize;
        if length > MAX_TRANSFER_STRING_BYTES {
            return Err(TransferFrameError::LimitExceeded {
                field: "string",
                limit: MAX_TRANSFER_STRING_BYTES,
                actual: length,
            });
        }
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| TransferFrameError::InvalidUtf8)
    }

    fn bytes(&mut self, max: usize) -> Result<Vec<u8>, TransferFrameError> {
        let length = self.u32()? as usize;
        if length > max {
            return Err(TransferFrameError::LimitExceeded {
                field: "bytes",
                limit: max,
                actual: length,
            });
        }
        Ok(self.take(length)?.to_vec())
    }

    fn id<T>(&mut self, kind: &'static str) -> Result<T, TransferFrameError>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        self.string()?
            .parse()
            .map_err(|error| TransferFrameError::InvalidIdentifier(format!("{kind}: {error}")))
    }

    fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::materialization::BatchManifestPage;
    use super::*;
    use crate::{BackendId, PlacementGeneration};
    use crate::{Generation, MaterializationBatchId, MaterializationId, ObjectNamespaceId};

    fn object(byte: u8) -> ObjectId {
        ObjectId::from_bytes([byte; 32])
    }

    fn ticket() -> TransferTicket {
        TransferTicket {
            transfer_id: TransferId::new("transfer-1").unwrap(),
            tenant_id: TenantId::new("tenant-1").unwrap(),
            artifact_id: ArtifactId::new("artifact-1").unwrap(),
            commit_id: CommitId::from_bytes([8; 32]),
            object_set_digest: ContentDigest::from_bytes([9; 32]),
            source: TransferEndpoint {
                placement_id: PlacementId::new("placement-src").unwrap(),
                agent_id: AgentId::new("agent-src").unwrap(),
                gateway_pool_id: GatewayPoolId::new("gateway-src").unwrap(),
                edge_cluster_id: EdgeClusterId::new("cluster-src").unwrap(),
                storage_volume_id: Some(StorageVolumeId::new("volume-src").unwrap()),
            },
            target: TransferEndpoint {
                placement_id: PlacementId::new("placement-dst").unwrap(),
                agent_id: AgentId::new("agent-dst").unwrap(),
                gateway_pool_id: GatewayPoolId::new("gateway-dst").unwrap(),
                edge_cluster_id: EdgeClusterId::new("cluster-dst").unwrap(),
                storage_volume_id: Some(StorageVolumeId::new("volume-dst").unwrap()),
            },
            source_session_generation: SessionGeneration::new(4),
            source_mount_generation: MountGeneration::new(5),
            source_route_generation: RouteGeneration::new(6),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            deadline_unix_ms: UnixMillis::new(100),
            max_bytes: DecimalU64::new(1000),
            allowed_objects: vec![object(1), object(2)],
        }
    }

    #[test]
    fn ticket_round_trips_in_binary_without_json() {
        let frame = TransferFrame::OpenTransfer(ticket());
        let bytes = frame.encode().unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            bytes.len() - 4
        );
        assert_eq!(TransferFrame::decode(&bytes).unwrap(), frame);
    }

    #[test]
    fn preflight_frames_round_trip_without_a_business_scope() {
        for frame in [TransferFrame::Preflight, TransferFrame::PreflightAck] {
            let bytes = frame.encode().unwrap();
            assert_eq!(
                bytes,
                vec![
                    0,
                    0,
                    0,
                    1,
                    if frame == TransferFrame::Preflight {
                        13
                    } else {
                        14
                    }
                ]
            );
            assert_eq!(TransferFrame::decode(&bytes).unwrap(), frame);
        }
    }

    #[test]
    fn object_chunk_rejects_oversized_payload_and_trailing_bytes() {
        let frame = TransferFrame::ObjectChunk(ObjectChunk {
            object_id: object(1),
            offset: 0,
            bytes: vec![1, 2, 3],
        });
        let mut bytes = frame.encode().unwrap();
        bytes.push(0);
        assert!(TransferFrame::decode(&bytes).is_err());
        assert!(ObjectChunk::new(object(1), 0, vec![0; MAX_TRANSFER_CHUNK_BYTES + 1]).is_err());
    }

    #[test]
    fn empty_object_request_round_trips_with_a_zero_length_range() {
        let frame = TransferFrame::ObjectRequest(ObjectRequest {
            object_id: object(1),
            offset: 0,
            length: 0,
        });
        let encoded = frame.encode().unwrap();
        assert_eq!(TransferFrame::decode(&encoded).unwrap(), frame);
    }

    #[test]
    fn unknown_frame_kind_is_rejected() {
        assert!(matches!(
            TransferFrame::decode(&[0, 0, 0, 1, 99]),
            Err(TransferFrameError::UnknownKind(99))
        ));
    }

    #[test]
    fn materialization_manifest_frame_rejects_duplicate_json_members() {
        let page = BatchManifestPage::new(
            MaterializationId::new("materialization-1").unwrap(),
            MaterializationBatchId::new("batch-1").unwrap(),
            Generation::new(1),
            Generation::new(1),
            ObjectNamespaceId::new("artifact-1").unwrap(),
            0,
            1,
            Vec::new(),
        )
        .unwrap();
        let encoded = serde_json::to_string(&page).unwrap();
        let duplicate = format!(
            "{},\"batch_id\":\"{}\"}}",
            encoded.trim_end_matches('}'),
            page.batch_id
        );
        let mut payload = vec![FrameKind::MaterializationManifestPage as u8];
        payload.extend_from_slice(&(duplicate.len() as u32).to_be_bytes());
        payload.extend_from_slice(duplicate.as_bytes());
        let mut frame = Vec::with_capacity(payload.len() + 4);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        assert!(matches!(
            TransferFrame::decode(&frame),
            Err(TransferFrameError::InvalidField(
                "materialization_manifest_page"
            ))
        ));
    }

    #[allow(dead_code)]
    fn _keep_imports_used() {
        let _ = BackendId::new("backend");
        let _ = PlacementGeneration::new(1);
    }
}
