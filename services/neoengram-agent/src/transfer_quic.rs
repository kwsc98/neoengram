//! Agent-side framing helpers for the immutable-object transfer protocol.
//!
//! The helpers in this module deliberately stop at the Agent boundary.  They accept split
//! asynchronous read/write halves, which are directly compatible with Quinn's `RecvStream` and
//! `SendStream`, but do not create a QUIC endpoint or own TLS configuration.  Gateways can relay
//! the exact frames without seeing a Volume handle or persisting payload bytes.
//!
//! [`AgentTransferSinkSession`] drives the target side of a transfer through the runtime
//! [`neoengram_runtime::TransferSink`] port.  [`AgentTransferSourceSession`] serves requests from
//! a source [`neoengram_runtime::TransferSource`].  The caller remains responsible for verifying
//! the Central signature with its command trust bundle before accepting a signed ticket; these
//! helpers additionally enforce the tenant, ObjectSet digest, object allow-list, range, and frame
//! fences needed by the data plane.

use std::io;

use neoengram_domain::protocol::{
    ObjectAck, ObjectChunk, ObjectProof, ObjectRequest, ObjectSet, SignedTransferTicket,
    TransferErrorCode, TransferFrame, TransferFrameError, TransferTicket, MAX_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_FRAME_BYTES,
};
use neoengram_domain::{CommitObject, TenantId};
use neoengram_runtime::{ObjectRange, ObjectTransferOutcome, TransferSink, TransferSource};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Errors raised by Agent-side transfer framing or the source/sink data-plane ports.
#[derive(Debug, thiserror::Error)]
pub enum AgentTransferError {
    #[error("transfer frame read failed: {0}")]
    Read(#[source] io::Error),
    #[error("transfer frame write failed: {0}")]
    Write(#[source] io::Error),
    #[error("invalid transfer frame: {0}")]
    Frame(#[from] TransferFrameError),
    #[error("invalid transfer ticket: {0}")]
    Ticket(String),
    #[error("transfer scope violation: {0}")]
    Scope(String),
    #[error("unexpected transfer frame: expected {expected}, got {actual}")]
    UnexpectedFrame {
        expected: &'static str,
        actual: &'static str,
    },
    #[error("transfer backend failed: {0}")]
    Backend(String),
    #[error("remote transfer failed ({code:?}): {message}")]
    Remote {
        code: TransferErrorCode,
        message: String,
    },
}

/// A bounded, length-prefixed transfer frame channel over split async stream halves.
///
/// Quinn's `RecvStream` and `SendStream` satisfy the bounds, as do Tokio test streams.  Keeping
/// the halves separate prevents this adapter from assuming that the underlying transport is a
/// single bidirectional byte stream.
#[derive(Debug)]
pub struct AgentTransferFrameChannel<R, W> {
    recv: R,
    send: W,
}

impl<R, W> AgentTransferFrameChannel<R, W> {
    #[must_use]
    pub fn new(recv: R, send: W) -> Self {
        Self { recv, send }
    }

    #[must_use]
    pub fn into_parts(self) -> (R, W) {
        (self.recv, self.send)
    }
}

impl<R, W> AgentTransferFrameChannel<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// Reads exactly one bounded frame and leaves the stream positioned at the next frame.
    pub async fn recv_frame(&mut self) -> Result<TransferFrame, AgentTransferError> {
        let mut prefix = [0_u8; 4];
        self.recv
            .read_exact(&mut prefix)
            .await
            .map_err(AgentTransferError::Read)?;
        let payload_len = u32::from_be_bytes(prefix) as usize;
        let total_len = payload_len
            .checked_add(4)
            .ok_or(TransferFrameError::LimitExceeded {
                field: "frame",
                limit: MAX_TRANSFER_FRAME_BYTES,
                actual: usize::MAX,
            })?;
        if payload_len == 0 || total_len > MAX_TRANSFER_FRAME_BYTES {
            return Err(TransferFrameError::LimitExceeded {
                field: "frame",
                limit: MAX_TRANSFER_FRAME_BYTES,
                actual: total_len,
            }
            .into());
        }
        let mut encoded = Vec::with_capacity(total_len);
        encoded.extend_from_slice(&prefix);
        encoded.resize(total_len, 0);
        self.recv
            .read_exact(&mut encoded[4..])
            .await
            .map_err(AgentTransferError::Read)?;
        Ok(TransferFrame::decode(&encoded)?)
    }

    /// Writes exactly one bounded frame.  Payload bytes never pass through Central.
    pub async fn send_frame(&mut self, frame: &TransferFrame) -> Result<(), AgentTransferError> {
        let encoded = frame.encode()?;
        self.send
            .write_all(&encoded)
            .await
            .map_err(AgentTransferError::Write)
    }
}

/// Source-side Agent session.  It serves bounded `ObjectRequest` frames from a selected runtime
/// source placement and never opens an arbitrary local path based on wire input.
#[derive(Debug)]
pub struct AgentTransferSourceSession<R, W> {
    channel: AgentTransferFrameChannel<R, W>,
}

impl<R, W> AgentTransferSourceSession<R, W> {
    #[must_use]
    pub fn new(recv: R, send: W) -> Self {
        Self {
            channel: AgentTransferFrameChannel::new(recv, send),
        }
    }

    #[must_use]
    pub fn channel(&self) -> &AgentTransferFrameChannel<R, W> {
        &self.channel
    }

    pub fn channel_mut(&mut self) -> &mut AgentTransferFrameChannel<R, W> {
        &mut self.channel
    }
}

impl<R, W> AgentTransferSourceSession<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// Receives the signed opening frame and validates its structural transfer scope.
    pub async fn accept_open(
        &mut self,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
    ) -> Result<SignedTransferTicket, AgentTransferError> {
        let frame = self.channel.recv_frame().await?;
        let signed = match frame {
            TransferFrame::OpenTransferSigned(ticket) => ticket,
            other => {
                return Err(unexpected("OpenTransferSigned", frame_name(&other)));
            }
        };
        signed
            .validate()
            .map_err(|error| AgentTransferError::Ticket(error.to_string()))?;
        validate_transfer_scope(&signed.ticket, object_set, tenant_id)?;
        Ok(signed)
    }

    /// Serves requests until the target sends a committed or non-committed close frame.
    ///
    /// The Central signature must be verified by the caller before invoking this method.  The
    /// source only reads ranges named by the ticket's ObjectSet and acknowledges each accepted
    /// chunk before accepting another request.
    pub async fn serve(
        &mut self,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
        source: &dyn TransferSource,
    ) -> Result<(), AgentTransferError> {
        let signed = self.accept_open(object_set, tenant_id).await?;
        let ticket = signed.ticket;
        let mut served_bytes = 0_u64;
        loop {
            let frame = self.channel.recv_frame().await?;
            match frame {
                TransferFrame::ObjectRequest(request) => {
                    let served = self
                        .serve_request(&ticket, object_set, tenant_id, source, request)
                        .await?;
                    served_bytes = served_bytes.checked_add(served).ok_or_else(|| {
                        AgentTransferError::Scope("served byte count overflows u64".into())
                    })?;
                    if served_bytes > ticket.max_bytes.get() {
                        return Err(AgentTransferError::Scope(
                            "source transfer exceeds TransferTicket byte limit".into(),
                        ));
                    }
                }
                TransferFrame::CloseTransfer(_) => return Ok(()),
                TransferFrame::TransferError(error) => {
                    return Err(AgentTransferError::Remote {
                        code: error.code,
                        message: error.message,
                    });
                }
                other => {
                    return Err(unexpected(
                        "ObjectRequest or CloseTransfer",
                        frame_name(&other),
                    ));
                }
            }
        }
    }

    async fn serve_request(
        &mut self,
        ticket: &TransferTicket,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
        source: &dyn TransferSource,
        request: ObjectRequest,
    ) -> Result<u64, AgentTransferError> {
        let object = object_set
            .iter()
            .find(|object| object.object_id == request.object_id)
            .ok_or_else(|| {
                AgentTransferError::Scope("requested object is not in ObjectSet".into())
            })?;
        if !ticket.allows(request.object_id) {
            return Err(AgentTransferError::Scope(
                "requested object is not authorized by TransferTicket".into(),
            ));
        }
        let expected = object.object_spec();
        let range = ObjectRange::new(request.offset, request.length)
            .map_err(|error| AgentTransferError::Scope(error.to_string()))?;
        if range.end() > expected.size {
            return Err(AgentTransferError::Scope(
                "requested range exceeds object size".into(),
            ));
        }
        let mut bytes = Vec::with_capacity(range.length as usize);
        let copied = source
            .open_object(tenant_id, &expected, range, &mut bytes)
            .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
        if copied != range.length || bytes.len() != range.length as usize {
            return Err(AgentTransferError::Backend(
                "source returned an unexpected range length".into(),
            ));
        }
        let chunk = ObjectChunk::new(request.object_id, request.offset, bytes)?;
        self.channel
            .send_frame(&TransferFrame::ObjectChunk(chunk))
            .await?;
        if range.end() == expected.size {
            self.channel
                .send_frame(&TransferFrame::ObjectProof(ObjectProof {
                    object_id: expected.id,
                    digest: expected.id.digest(),
                    size: expected.size,
                }))
                .await?;
        }
        let frame = self.channel.recv_frame().await?;
        let ack = match frame {
            TransferFrame::ObjectAck(ack) => ack,
            TransferFrame::TransferError(error) => {
                return Err(AgentTransferError::Remote {
                    code: error.code,
                    message: error.message,
                });
            }
            other => return Err(unexpected("ObjectAck", frame_name(&other))),
        };
        if ack.object_id != request.object_id
            || ack.offset != request.offset
            || ack.length != request.length
        {
            return Err(AgentTransferError::Scope(
                "ObjectAck does not match the requested range".into(),
            ));
        }
        if !ack.accepted {
            return Err(AgentTransferError::Remote {
                code: TransferErrorCode::DataUnavailable,
                message: "target rejected object chunk".into(),
            });
        }
        Ok(range.length)
    }
}

/// Target-side Agent session.  It requests ranges and writes them into a runtime transfer sink.
#[derive(Debug)]
pub struct AgentTransferSinkSession<R, W> {
    channel: AgentTransferFrameChannel<R, W>,
    chunk_bytes: usize,
    ticket: Option<SignedTransferTicket>,
    transferred_bytes: u64,
}

impl<R, W> AgentTransferSinkSession<R, W> {
    #[must_use]
    pub fn new(recv: R, send: W) -> Self {
        Self {
            channel: AgentTransferFrameChannel::new(recv, send),
            chunk_bytes: MAX_TRANSFER_CHUNK_BYTES,
            ticket: None,
            transferred_bytes: 0,
        }
    }

    pub fn with_chunk_bytes(mut self, chunk_bytes: usize) -> Result<Self, AgentTransferError> {
        if chunk_bytes == 0 || chunk_bytes > MAX_TRANSFER_CHUNK_BYTES {
            return Err(AgentTransferError::Scope(format!(
                "chunk size must be in 1..={MAX_TRANSFER_CHUNK_BYTES}"
            )));
        }
        self.chunk_bytes = chunk_bytes;
        Ok(self)
    }

    #[must_use]
    pub fn channel(&self) -> &AgentTransferFrameChannel<R, W> {
        &self.channel
    }

    pub fn channel_mut(&mut self) -> &mut AgentTransferFrameChannel<R, W> {
        &mut self.channel
    }
}

impl<R, W> AgentTransferSinkSession<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// Opens a target session by sending the exact signed capability through the Gateway.
    pub async fn open(
        &mut self,
        signed_ticket: SignedTransferTicket,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
    ) -> Result<(), AgentTransferError> {
        signed_ticket
            .validate()
            .map_err(|error| AgentTransferError::Ticket(error.to_string()))?;
        validate_transfer_scope(&signed_ticket.ticket, object_set, tenant_id)?;
        self.channel
            .send_frame(&TransferFrame::OpenTransferSigned(signed_ticket.clone()))
            .await?;
        self.ticket = Some(signed_ticket);
        self.transferred_bytes = 0;
        Ok(())
    }

    /// Copies a complete ObjectSet and closes the session after all objects are published locally.
    pub async fn copy_object_set(
        &mut self,
        signed_ticket: SignedTransferTicket,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
        transfer_id: &neoengram_domain::TransferId,
        sink: &dyn TransferSink,
    ) -> Result<Vec<ObjectTransferOutcome>, AgentTransferError> {
        validate_transfer_scope(&signed_ticket.ticket, object_set, tenant_id)?;
        let total_size = object_set.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.size.get())
                .ok_or_else(|| AgentTransferError::Scope("ObjectSet size overflows u64".into()))
        })?;
        if total_size > signed_ticket.ticket.max_bytes.get() {
            return Err(AgentTransferError::Scope(
                "ObjectSet exceeds TransferTicket byte limit".into(),
            ));
        }
        self.open(signed_ticket, object_set, tenant_id).await?;
        let mut outcomes = Vec::with_capacity(object_set.len());
        for object in object_set {
            outcomes.push(
                self.pull_object(tenant_id, transfer_id, object, sink)
                    .await?,
            );
        }
        self.close(true).await?;
        Ok(outcomes)
    }

    /// Closes the stream without publishing a PlacementSet.  A cancelled transfer leaves any
    /// target staging for the caller's GC/retry policy and never makes it visible as a placement.
    pub async fn close(&mut self, committed: bool) -> Result<(), AgentTransferError> {
        self.channel
            .send_frame(&TransferFrame::CloseTransfer(
                neoengram_domain::protocol::CloseTransfer { committed },
            ))
            .await
    }

    /// Pulls one object, resuming at the sink's durable staging offset.
    pub async fn pull_object(
        &mut self,
        tenant_id: &TenantId,
        transfer_id: &neoengram_domain::TransferId,
        object: &CommitObject,
        sink: &dyn TransferSink,
    ) -> Result<ObjectTransferOutcome, AgentTransferError> {
        let ticket = self
            .ticket
            .as_ref()
            .ok_or_else(|| AgentTransferError::Scope("transfer session is not open".into()))?
            .ticket
            .clone();
        if ticket.tenant_id != *tenant_id
            || ticket.transfer_id != *transfer_id
            || !ticket.allows(object.object_id)
        {
            return Err(AgentTransferError::Scope(
                "object is outside the open TransferTicket scope".into(),
            ));
        }
        let expected = object.object_spec();
        let mut offset = sink
            .resume_offset(transfer_id, tenant_id, &object.object_id)
            .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
        if offset > expected.size {
            return Err(AgentTransferError::Backend(
                "durable staging offset exceeds object size".into(),
            ));
        }
        let resumed_from = offset;
        let mut bytes_transferred = 0_u64;
        while offset < expected.size {
            let length = (expected.size - offset).min(self.chunk_bytes as u64);
            self.channel
                .send_frame(&TransferFrame::ObjectRequest(ObjectRequest {
                    object_id: object.object_id,
                    offset,
                    length,
                }))
                .await?;
            let request_end = offset + length;
            loop {
                let frame = self.channel.recv_frame().await?;
                match frame {
                    TransferFrame::ObjectChunk(chunk) => {
                        if chunk.object_id != object.object_id
                            || chunk.offset != offset
                            || chunk.bytes.is_empty()
                            || chunk.bytes.len() as u64 > request_end - offset
                        {
                            return Err(AgentTransferError::Scope(
                                "ObjectChunk does not match the requested range".into(),
                            ));
                        }
                        let chunk_len = chunk.bytes.len() as u64;
                        let staged = sink
                            .accept_object(transfer_id, tenant_id, &expected, offset, &chunk.bytes)
                            .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
                        let expected_offset = offset + chunk_len;
                        if staged.accepted_bytes != chunk_len
                            || staged.staged_size != expected_offset
                        {
                            return Err(AgentTransferError::Backend(
                                "sink acknowledged an unexpected staged offset".into(),
                            ));
                        }
                        self.channel
                            .send_frame(&TransferFrame::ObjectAck(ObjectAck {
                                object_id: object.object_id,
                                offset,
                                length: chunk_len,
                                accepted: true,
                            }))
                            .await?;
                        offset = expected_offset;
                        bytes_transferred += chunk_len;
                        if offset == request_end {
                            break;
                        }
                    }
                    TransferFrame::TransferError(error) => {
                        return Err(AgentTransferError::Remote {
                            code: error.code,
                            message: error.message,
                        });
                    }
                    other => return Err(unexpected("ObjectChunk", frame_name(&other))),
                }
            }
            if offset == expected.size {
                let frame = self.channel.recv_frame().await?;
                let proof = match frame {
                    TransferFrame::ObjectProof(proof) => proof,
                    other => return Err(unexpected("ObjectProof", frame_name(&other))),
                };
                if proof.object_id != object.object_id
                    || proof.size != expected.size
                    || proof.digest != expected.id.digest()
                {
                    return Err(AgentTransferError::Scope(
                        "ObjectProof does not match ObjectSet identity".into(),
                    ));
                }
            }
        }
        let transferred = self
            .transferred_bytes
            .checked_add(bytes_transferred)
            .ok_or_else(|| {
                AgentTransferError::Scope("transferred byte count overflows u64".into())
            })?;
        if transferred > ticket.max_bytes.get() {
            return Err(AgentTransferError::Scope(
                "sink transfer exceeds TransferTicket byte limit".into(),
            ));
        }
        let published = sink
            .commit_object(transfer_id, tenant_id, &expected)
            .map_err(|error| AgentTransferError::Backend(error.to_string()))?;
        self.transferred_bytes = transferred;
        Ok(ObjectTransferOutcome {
            object_id: object.object_id,
            bytes_transferred,
            resumed_from,
            published,
        })
    }
}

fn validate_transfer_scope(
    ticket: &TransferTicket,
    object_set: &[CommitObject],
    tenant_id: &TenantId,
) -> Result<(), AgentTransferError> {
    ticket
        .validate()
        .map_err(|error| AgentTransferError::Ticket(error.to_string()))?;
    if ticket.tenant_id != *tenant_id {
        return Err(AgentTransferError::Scope(
            "transfer ticket tenant does not match local tenant".into(),
        ));
    }
    let digest = ObjectSet::digest_for(object_set)
        .map_err(|error| AgentTransferError::Scope(error.to_string()))?;
    if ticket.object_set_digest != digest {
        return Err(AgentTransferError::Scope(
            "transfer ticket ObjectSet digest does not match assignment".into(),
        ));
    }
    let mut expected = object_set
        .iter()
        .map(|object| object.object_id)
        .collect::<Vec<_>>();
    expected.sort_unstable();
    let mut allowed = ticket.allowed_objects.clone();
    allowed.sort_unstable();
    if expected != allowed {
        return Err(AgentTransferError::Scope(
            "transfer ticket object allow-list does not match ObjectSet".into(),
        ));
    }
    Ok(())
}

fn unexpected(expected: &'static str, actual: &'static str) -> AgentTransferError {
    AgentTransferError::UnexpectedFrame { expected, actual }
}

fn frame_name(frame: &TransferFrame) -> &'static str {
    match frame {
        TransferFrame::OpenTransfer(_) => "OpenTransfer",
        TransferFrame::OpenTransferSigned(_) => "OpenTransferSigned",
        TransferFrame::OpenMaterializationSigned(_) => "OpenMaterializationSigned",
        TransferFrame::MaterializationManifest(_) => "MaterializationManifest",
        TransferFrame::MaterializationManifestPage(_) => "MaterializationManifestPage",
        TransferFrame::Preflight => "Preflight",
        TransferFrame::PreflightAck => "PreflightAck",
        TransferFrame::ObjectRequest(_) => "ObjectRequest",
        TransferFrame::ObjectChunk(_) => "ObjectChunk",
        TransferFrame::ObjectProof(_) => "ObjectProof",
        TransferFrame::ObjectAck(_) => "ObjectAck",
        TransferFrame::CommitObjectSet(_) => "CommitObjectSet",
        TransferFrame::TransferError(_) => "TransferError",
        TransferFrame::CloseTransfer(_) => "CloseTransfer",
    }
}

#[cfg(test)]
mod tests {
    use neoengram_domain::protocol::{
        ArtifactId, CentralSignedPayload, CommitObject, DecimalU64, EdgeClusterId, Extensions,
        GatewayOpaqueBytes, GatewayPoolId, MountGeneration, ObjectEncoding, PlacementId,
        RouteGeneration, SessionGeneration, StorageVolumeId, TransferEndpoint, TransferId,
        UnixMillis,
    };
    use neoengram_domain::{CommitId, ContentDigest, ObjectId};
    use neoengram_runtime::{ObjectBackend, ObjectSpec, VolumeCasBackend};
    use tokio::io::duplex;

    use super::*;

    fn signed_ticket(object_set: &[CommitObject], tenant: &TenantId) -> SignedTransferTicket {
        let object_set_digest = ObjectSet::digest_for(object_set).unwrap();
        let object_ids = object_set.iter().map(|object| object.object_id).collect();
        let ticket = TransferTicket {
            transfer_id: TransferId::new("transfer-frame-test").unwrap(),
            tenant_id: tenant.clone(),
            artifact_id: ArtifactId::new("artifact-test").unwrap(),
            commit_id: CommitId::from_bytes([1; 32]),
            object_set_digest,
            source: endpoint("source"),
            target: endpoint("target"),
            source_session_generation: SessionGeneration::new(1),
            source_mount_generation: MountGeneration::new(1),
            source_route_generation: RouteGeneration::new(1),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            max_bytes: DecimalU64::new(
                object_set
                    .iter()
                    .map(|object| object.size.get())
                    .sum::<u64>(),
            ),
            allowed_objects: object_ids,
        };
        let payload =
            GatewayOpaqueBytes::new(SignedTransferTicket::payload_bytes(&ticket).unwrap()).unwrap();
        SignedTransferTicket::new(
            ticket,
            CentralSignedPayload {
                key_id: "test".to_owned(),
                certificate_generation: neoengram_domain::protocol::CertificateGeneration::new(1),
                signed_at_unix_ms: UnixMillis::new(1),
                expires_at_unix_ms: UnixMillis::new(u64::MAX),
                payload_digest: ContentDigest::hash(payload.as_bytes()),
                payload,
                signature: neoengram_domain::protocol::Ed25519Signature::from_bytes([0; 64]),
                extensions: Extensions::new(),
            },
        )
        .unwrap()
    }

    fn endpoint(name: &str) -> TransferEndpoint {
        TransferEndpoint {
            placement_id: PlacementId::new(format!("placement-{name}")).unwrap(),
            agent_id: neoengram_domain::AgentId::new(format!("agent-{name}")).unwrap(),
            gateway_pool_id: GatewayPoolId::new(format!("pool-{name}")).unwrap(),
            edge_cluster_id: EdgeClusterId::new(format!("cluster-{name}")).unwrap(),
            storage_volume_id: Some(StorageVolumeId::new(format!("volume-{name}")).unwrap()),
        }
    }

    fn channel_pair() -> (
        AgentTransferFrameChannel<tokio::io::DuplexStream, tokio::io::DuplexStream>,
        AgentTransferFrameChannel<tokio::io::DuplexStream, tokio::io::DuplexStream>,
    ) {
        let (left_to_right_write, left_to_right_read) = duplex(64 * 1024);
        let (right_to_left_write, right_to_left_read) = duplex(64 * 1024);
        (
            AgentTransferFrameChannel::new(right_to_left_read, left_to_right_write),
            AgentTransferFrameChannel::new(left_to_right_read, right_to_left_write),
        )
    }

    #[tokio::test]
    async fn channel_round_trips_frames_and_rejects_trailing_frame_bytes() {
        let (mut sender, mut receiver) = channel_pair();
        let object_id = ObjectId::from_bytes([7; 32]);
        sender
            .send_frame(&TransferFrame::ObjectRequest(ObjectRequest {
                object_id,
                offset: 4,
                length: 3,
            }))
            .await
            .unwrap();
        assert_eq!(
            receiver.recv_frame().await.unwrap(),
            TransferFrame::ObjectRequest(ObjectRequest {
                object_id,
                offset: 4,
                length: 3,
            })
        );
        let (mut raw_write, raw_read) = duplex(64 * 1024);
        raw_write.write_all(&[0, 0, 0, 3, 8, 1, 0]).await.unwrap();
        drop(raw_write);
        let mut malformed = AgentTransferFrameChannel::new(raw_read, duplex(64).0);
        // The frame is rejected either by the bounded stream read (the extra byte is not a
        // complete ObjectRequest) or by the codec's truncation check, depending on how the
        // duplex stream is scheduled. Both outcomes are fail-closed.
        assert!(malformed.recv_frame().await.is_err());
    }

    #[tokio::test]
    async fn source_and_sink_sessions_copy_with_resume_and_ack() {
        let tenant = TenantId::new("tenant-frame").unwrap();
        let payload = b"frame-session-payload";
        let object = CommitObject::new(
            ObjectId::for_bytes(payload),
            payload.len() as u64,
            ObjectEncoding::Raw,
            0,
        );
        let object_set = vec![object];
        let signed = signed_ticket(&object_set, &tenant);
        let source_root = tempfile::tempdir().unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let source_backend = VolumeCasBackend::open_or_create(source_root.path()).unwrap();
        let target_backend = VolumeCasBackend::open_or_create(target_root.path()).unwrap();
        let seed_transfer = TransferId::new("seed-frame").unwrap();
        source_backend
            .stage_write(
                &seed_transfer,
                &tenant,
                &ObjectSpec::for_bytes(payload),
                0,
                payload,
            )
            .unwrap();
        source_backend
            .verify_and_publish(&seed_transfer, &tenant, &ObjectSpec::for_bytes(payload))
            .unwrap();
        let transfer_id = TransferId::new("transfer-frame-test").unwrap();
        target_backend
            .stage_write(
                &transfer_id,
                &tenant,
                &ObjectSpec::for_bytes(payload),
                0,
                &payload[..4],
            )
            .unwrap();

        let (source_channel, sink_channel) = channel_pair();
        let (source_recv, source_send) = source_channel.into_parts();
        let (sink_recv, sink_send) = sink_channel.into_parts();
        let source_object_set = object_set.clone();
        let source_tenant = tenant.clone();
        let source = tokio::spawn(async move {
            let mut session = AgentTransferSourceSession::new(source_recv, source_send);
            session
                .serve(&source_object_set, &source_tenant, &source_backend)
                .await
        });
        let mut sink = AgentTransferSinkSession::new(sink_recv, sink_send)
            .with_chunk_bytes(4)
            .unwrap();
        let outcomes = sink
            .copy_object_set(signed, &object_set, &tenant, &transfer_id, &target_backend)
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].resumed_from, 4);
        assert_eq!(
            outcomes[0].published,
            neoengram_runtime::ObjectPutOutcome::Created
        );
        assert_eq!(
            target_backend
                .inspect(&tenant, &object.object_id)
                .unwrap()
                .unwrap()
                .size,
            payload.len() as u64
        );
        source.await.unwrap().unwrap();
    }

    #[test]
    fn scope_rejects_allow_list_mismatch() {
        let tenant = TenantId::new("tenant-frame").unwrap();
        let object = CommitObject::new(ObjectId::from_bytes([3; 32]), 1, ObjectEncoding::Raw, 0);
        let mut ticket = signed_ticket(&[object], &tenant).ticket;
        ticket.allowed_objects.clear();
        assert!(validate_transfer_scope(&ticket, &[object], &tenant).is_err());
    }
}
