use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{AgentError, AgentErrorCode};
use neoengram_domain::core::LogicalPath;
use neoengram_domain::protocol::{
    S3ReadCancel, S3ReadData, S3ReadEnd, S3ReadError, S3ReadFrame, S3ReadHead, S3ReadOpen,
    UnixMillis, S3_READ_FRAME_MAX_BYTES,
};
use tokio::sync::mpsc;

use crate::{CentralCommandTrustBundle, ImmutableByteRange, S3SnapshotSource};

const S3_READ_OUTPUT_BUFFER: usize = 8;
const MAX_STREAM_ID_BYTES: usize = 128;
const BACKPRESSURE_POLL_INTERVAL: Duration = Duration::from_millis(2);

/// Executes Central-authorized immutable reads against a ticket-bound Snapshot source.
/// The bounded output queue is the backpressure boundary consumed by the Gateway H2 stream.
#[derive(Clone)]
pub struct S3ReadExecutor {
    source: Arc<dyn S3SnapshotSource>,
    trust_bundle: Arc<CentralCommandTrustBundle>,
    active: Arc<Mutex<BTreeMap<String, Arc<AtomicBool>>>>,
}

impl std::fmt::Debug for S3ReadExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ReadExecutor")
            .finish_non_exhaustive()
    }
}

impl S3ReadExecutor {
    #[must_use]
    pub fn new(
        source: Arc<dyn S3SnapshotSource>,
        trust_bundle: Arc<CentralCommandTrustBundle>,
    ) -> Self {
        Self {
            source,
            trust_bundle,
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Consumes the two request-side frame kinds. Response-side frames on this boundary are a
    /// protocol error rather than being reflected back into the read stream.
    pub fn handle_frame(
        &self,
        frame: S3ReadFrame,
    ) -> Result<Option<mpsc::Receiver<S3ReadFrame>>, S3ReadError> {
        match frame {
            S3ReadFrame::Open(request) => self.open(*request).map(Some),
            S3ReadFrame::Cancel(request) => {
                let _ = self.cancel(&request);
                Ok(None)
            }
            frame => Err(read_error(
                response_stream_id(&frame),
                "unexpected_frame",
                "Agent S3 reader accepts only Open and Cancel frames",
            )),
        }
    }

    /// Starts one read and returns a bounded frame receiver. Dropping the receiver or cancelling
    /// the stream wakes a backpressured producer, so it cannot keep reading ahead indefinitely.
    pub fn open(&self, request: S3ReadOpen) -> Result<mpsc::Receiver<S3ReadFrame>, S3ReadError> {
        self.open_at(request, now_unix_ms())
    }

    pub(crate) fn open_at(
        &self,
        request: S3ReadOpen,
        now_unix_ms: UnixMillis,
    ) -> Result<mpsc::Receiver<S3ReadFrame>, S3ReadError> {
        validate_open(&request)?;
        self.trust_bundle
            .verify_s3_ticket(&request.ticket, now_unix_ms)
            .map_err(|_| {
                read_error(
                    &request.stream_id,
                    "ticket_rejected",
                    "read ticket rejected",
                )
            })?;
        let path = LogicalPath::parse(request.ticket.logical_path.clone()).map_err(|_| {
            read_error(
                &request.stream_id,
                "invalid_path",
                "ticket path is not canonical",
            )
        })?;
        let reader = self
            .source
            .immutable_reader_for_ticket(&request.ticket)
            .map_err(|error| agent_read_error(&request.stream_id, &error))?;

        let cancelled = Arc::new(AtomicBool::new(false));
        {
            let mut active = self.active.lock().map_err(|_| {
                read_error(
                    &request.stream_id,
                    "unavailable",
                    "read registry is unavailable",
                )
            })?;
            if active.contains_key(&request.stream_id) {
                return Err(read_error(
                    &request.stream_id,
                    "duplicate_stream",
                    "read stream ID is already active",
                ));
            }
            active.insert(request.stream_id.clone(), cancelled.clone());
        }

        let (sender, receiver) = mpsc::channel(S3_READ_OUTPUT_BUFFER);
        let active = self.active.clone();
        std::thread::spawn(move || {
            let stream_id = request.stream_id.clone();
            let result = execute_read(&request, &path, reader.as_ref(), &cancelled, &sender);
            if let Err(error) = result {
                if !cancelled.load(Ordering::Acquire) {
                    let _ = send_frame(&cancelled, &sender, S3ReadFrame::Error(error), &stream_id);
                }
            }
            if let Ok(mut streams) = active.lock() {
                streams.remove(&stream_id);
            }
        });
        Ok(receiver)
    }

    #[cfg(test)]
    fn with_mounts(
        source: Arc<dyn S3SnapshotSource>,
        trust_bundle: Arc<CentralCommandTrustBundle>,
    ) -> Self {
        Self {
            source,
            trust_bundle,
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Applies `ReadCancel` to the exact active stream. A stale or duplicate cancel is harmless.
    #[must_use]
    pub fn cancel(&self, request: &S3ReadCancel) -> bool {
        let Ok(active) = self.active.lock() else {
            return false;
        };
        let Some(cancelled) = active.get(&request.stream_id) else {
            return false;
        };
        cancelled.store(true, Ordering::Release);
        true
    }
}

fn validate_open(request: &S3ReadOpen) -> Result<(), S3ReadError> {
    if request.stream_id.is_empty()
        || request.stream_id.len() > MAX_STREAM_ID_BYTES
        || request.start > request.end_exclusive
        || request.start < request.ticket.allowed_start
        || request.end_exclusive > request.ticket.allowed_end_exclusive
        || request.end_exclusive > request.ticket.size_bytes
    {
        return Err(read_error(
            &request.stream_id,
            "invalid_range",
            "requested range is outside the ticket authorization",
        ));
    }
    Ok(())
}

fn execute_read(
    request: &S3ReadOpen,
    path: &LogicalPath,
    reader: &dyn crate::ImmutableSnapshotReader,
    cancelled: &AtomicBool,
    sender: &mpsc::Sender<S3ReadFrame>,
) -> Result<(), S3ReadError> {
    let head = reader
        .head(path)
        .map_err(|error| agent_read_error(&request.stream_id, &error))?;
    if head.size_bytes != request.ticket.size_bytes {
        return Err(read_error(
            &request.stream_id,
            "snapshot_changed",
            "mounted object size does not match the immutable ticket",
        ));
    }
    send_frame(
        cancelled,
        sender,
        S3ReadFrame::Head(S3ReadHead {
            stream_id: request.stream_id.clone(),
            size_bytes: head.size_bytes,
            last_modified_unix_ms: request.ticket.issued_at_unix_ms,
            etag: request.ticket.manifest_id,
            content_type: 0,
        }),
        &request.stream_id,
    )?;
    let mut offset = request.start;
    while offset < request.end_exclusive {
        if cancelled.load(Ordering::Acquire) {
            return Ok(());
        }
        let end_exclusive = request
            .end_exclusive
            .min(offset.saturating_add(S3_READ_FRAME_MAX_BYTES as u64));
        let range = ImmutableByteRange::new(offset, end_exclusive)
            .map_err(|error| agent_read_error(&request.stream_id, &error))?;
        let bytes = reader
            .read_range(path, range)
            .map_err(|error| agent_read_error(&request.stream_id, &error))?;
        if bytes.len() as u64 != end_exclusive - offset {
            return Err(read_error(
                &request.stream_id,
                "short_read",
                "immutable Snapshot returned an incomplete range",
            ));
        }
        send_frame(
            cancelled,
            sender,
            S3ReadFrame::Data(S3ReadData {
                stream_id: request.stream_id.clone(),
                offset,
                bytes,
            }),
            &request.stream_id,
        )?;
        offset = end_exclusive;
    }
    if !cancelled.load(Ordering::Acquire) {
        send_frame(
            cancelled,
            sender,
            S3ReadFrame::End(S3ReadEnd {
                stream_id: request.stream_id.clone(),
            }),
            &request.stream_id,
        )?;
    }
    Ok(())
}

fn send_frame(
    cancelled: &AtomicBool,
    sender: &mpsc::Sender<S3ReadFrame>,
    frame: S3ReadFrame,
    stream_id: &str,
) -> Result<(), S3ReadError> {
    let mut frame = frame;
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Ok(());
        }
        match sender.try_send(frame) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(pending)) => {
                frame = pending;
                std::thread::park_timeout(BACKPRESSURE_POLL_INTERVAL);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(read_error(
                    stream_id,
                    "cancelled",
                    "read response consumer disconnected",
                ));
            }
        }
    }
}

fn response_stream_id(frame: &S3ReadFrame) -> &str {
    match frame {
        S3ReadFrame::Open(value) => &value.stream_id,
        S3ReadFrame::Head(value) => &value.stream_id,
        S3ReadFrame::Data(value) => &value.stream_id,
        S3ReadFrame::End(value) => &value.stream_id,
        S3ReadFrame::Cancel(value) => &value.stream_id,
        S3ReadFrame::Error(value) => &value.stream_id,
    }
}

fn agent_read_error(stream_id: &str, error: &AgentError) -> S3ReadError {
    let code = match error.code() {
        AgentErrorCode::GenerationMismatch | AgentErrorCode::ScopeMismatch => "fenced",
        AgentErrorCode::AssignmentNotFound | AgentErrorCode::AssignmentMismatch => "not_found",
        AgentErrorCode::MountUnavailable => "snapshot_unavailable",
        AgentErrorCode::ProtocolInvalid | AgentErrorCode::InvalidAssignment => "protocol_invalid",
        AgentErrorCode::ObjectTransferFailed => "object_unavailable",
        _ => "read_failed",
    };
    read_error(stream_id, code, "immutable Snapshot read failed")
}

fn read_error(stream_id: &str, code: &str, detail: &str) -> S3ReadError {
    S3ReadError {
        stream_id: stream_id.to_owned(),
        code: code.to_owned(),
        detail: detail.to_owned(),
    }
}

fn now_unix_ms() -> UnixMillis {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    UnixMillis::new(u64::try_from(millis).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use crate::AgentResult;
    use neoengram_domain::core::ContentDigest;
    use neoengram_domain::protocol::{
        AgentId, CentralSignedPayload, CertificateGeneration, Ed25519PublicKeySpki,
        Ed25519Signature, Extensions, GatewayConnectionId, GatewayOpaqueBytes, GatewayReplicaId,
        MountGeneration, OwnerGeneration, RouteGeneration, S3ReadTicket, SessionGeneration,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair as _};

    use crate::{command_trust::TrustKeyState, ImmutableObjectHead, ImmutableSnapshotReader};

    use super::*;

    #[derive(Debug)]
    struct MemoryReader {
        bytes: Arc<Vec<u8>>,
        reads: Arc<AtomicUsize>,
    }

    impl ImmutableSnapshotReader for MemoryReader {
        fn head(&self, _path: &LogicalPath) -> AgentResult<ImmutableObjectHead> {
            Ok(ImmutableObjectHead {
                size_bytes: self.bytes.len() as u64,
            })
        }

        fn read_range(
            &self,
            _path: &LogicalPath,
            range: ImmutableByteRange,
        ) -> AgentResult<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.bytes[range.start as usize..range.end_exclusive as usize].to_vec())
        }
    }

    struct MemoryMounts {
        reader: Arc<dyn ImmutableSnapshotReader>,
        resolutions: Arc<AtomicUsize>,
    }

    impl S3SnapshotSource for MemoryMounts {
        fn immutable_reader_for_ticket(
            &self,
            _ticket: &S3ReadTicket,
        ) -> AgentResult<Arc<dyn ImmutableSnapshotReader>> {
            self.resolutions.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&self.reader))
        }
    }

    struct Fixture {
        executor: S3ReadExecutor,
        signing_key: Ed25519KeyPair,
        reads: Arc<AtomicUsize>,
        resolutions: Arc<AtomicUsize>,
    }

    impl Fixture {
        fn new(bytes: Vec<u8>) -> Self {
            let signing_key = Ed25519KeyPair::from_seed_unchecked(&[0x5a; 32]).unwrap();
            let public_key = Ed25519PublicKeySpki::from_public_key_bytes(
                signing_key.public_key().as_ref().try_into().unwrap(),
            );
            let trust_bundle = CentralCommandTrustBundle::from_test_keys(vec![(
                "central-s3-test".to_owned(),
                CertificateGeneration::new(4),
                public_key,
                TrustKeyState::Active,
            )])
            .unwrap();
            let reads = Arc::new(AtomicUsize::new(0));
            let resolutions = Arc::new(AtomicUsize::new(0));
            let reader: Arc<dyn ImmutableSnapshotReader> = Arc::new(MemoryReader {
                bytes: Arc::new(bytes),
                reads: Arc::clone(&reads),
            });
            let mounts: Arc<dyn S3SnapshotSource> = Arc::new(MemoryMounts {
                reader,
                resolutions: Arc::clone(&resolutions),
            });
            Self {
                executor: S3ReadExecutor::with_mounts(mounts, Arc::new(trust_bundle)),
                signing_key,
                reads,
                resolutions,
            }
        }

        fn ticket(&self, size_bytes: usize, logical_path: &str) -> S3ReadTicket {
            let ticket = S3ReadTicket {
                ticket_id: "s3-ticket-test".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                project_id: "project-a".to_owned(),
                artifact_id: "artifact-a".to_owned(),
                snapshot_id: "snapshot-a".to_owned(),
                snapshot_lifecycle_generation: neoengram_domain::protocol::LifecycleGeneration::new(
                    1,
                ),
                commit_id: ContentDigest::from_bytes([0x11; 32]),
                index_digest: ContentDigest::from_bytes([0x22; 32]),
                bucket: "bucket-a".to_owned(),
                access_point_policy_generation: neoengram_domain::protocol::ResourceVersion::new(1),
                logical_path: logical_path.to_owned(),
                manifest_id: ContentDigest::from_bytes([0x33; 32]),
                size_bytes: size_bytes as u64,
                allowed_start: 0,
                allowed_end_exclusive: size_bytes as u64,
                gateway_pool_id: "gateway-pool-a".to_owned(),
                owner_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
                owner_peer_endpoint: "https://replica-a.peer.example".to_owned(),
                agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
                route_generation: RouteGeneration::new(3),
                agent_id: AgentId::new("agent-a").unwrap(),
                owner_generation: OwnerGeneration::new(7),
                mount_generation: MountGeneration::new(8),
                session_generation: SessionGeneration::new(9),
                issued_at_unix_ms: UnixMillis::new(1_000),
                expires_at_unix_ms: UnixMillis::new(20_000),
                signature: GatewayOpaqueBytes::new(Vec::new()).unwrap(),
            };
            sign_ticket(ticket, &self.signing_key)
        }
    }

    fn sign_ticket(ticket: S3ReadTicket, key: &Ed25519KeyPair) -> S3ReadTicket {
        let payload = GatewayOpaqueBytes::new(ticket.signing_bytes().unwrap()).unwrap();
        let mut signed = CentralSignedPayload {
            key_id: "central-s3-test".to_owned(),
            certificate_generation: CertificateGeneration::new(4),
            signed_at_unix_ms: ticket.issued_at_unix_ms,
            expires_at_unix_ms: ticket.expires_at_unix_ms,
            payload_digest: ContentDigest::hash(payload.as_bytes()),
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions: Extensions::new(),
        };
        signed.signature =
            Ed25519Signature::new(key.sign(&signed.signing_bytes().unwrap()).as_ref().to_vec())
                .unwrap();
        ticket.with_central_signature(&signed).unwrap()
    }

    #[test]
    fn range_outside_the_ticket_is_rejected_before_mount_resolution() {
        let fixture = Fixture::new(vec![0; 32]);
        let mut ticket = fixture.ticket(32, "nested/file.bin");
        ticket.allowed_start = 8;
        ticket.signature = GatewayOpaqueBytes::new(Vec::new()).unwrap();
        let ticket = sign_ticket(ticket, &fixture.signing_key);
        let error = fixture
            .executor
            .open_at(
                S3ReadOpen {
                    stream_id: "range-outside".to_owned(),
                    ticket,
                    start: 7,
                    end_exclusive: 16,
                },
                UnixMillis::new(2_000),
            )
            .unwrap_err();
        assert_eq!(error.code, "invalid_range");
        assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn path_traversal_is_rejected_before_mount_resolution() {
        let fixture = Fixture::new(vec![0; 8]);
        let error = fixture
            .executor
            .open_at(
                S3ReadOpen {
                    stream_id: "path-traversal".to_owned(),
                    ticket: fixture.ticket(8, "../secret"),
                    start: 0,
                    end_exclusive: 8,
                },
                UnixMillis::new(2_000),
            )
            .unwrap_err();
        assert_eq!(error.code, "invalid_path");
        assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn signed_owner_route_cannot_be_changed_before_agent_validation() {
        let fixture = Fixture::new(vec![0; 8]);
        let mut ticket = fixture.ticket(8, "nested/file.bin");
        ticket.owner_replica_id = GatewayReplicaId::new("replica-b").unwrap();
        let error = fixture
            .executor
            .open_at(
                S3ReadOpen {
                    stream_id: "route-tampered".to_owned(),
                    ticket,
                    start: 0,
                    end_exclusive: 8,
                },
                UnixMillis::new(2_000),
            )
            .unwrap_err();
        assert_eq!(error.code, "ticket_rejected");
        assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancel_unblocks_a_backpressured_producer_without_end_frame() {
        let frame_count = S3_READ_OUTPUT_BUFFER + 4;
        let size = frame_count * S3_READ_FRAME_MAX_BYTES;
        let fixture = Fixture::new(vec![0x5a; size]);
        let stream_id = "cancel-backpressured";
        let mut receiver = fixture
            .executor
            .open_at(
                S3ReadOpen {
                    stream_id: stream_id.to_owned(),
                    ticket: fixture.ticket(size, "nested/file.bin"),
                    start: 0,
                    end_exclusive: size as u64,
                },
                UnixMillis::new(2_000),
            )
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.reads.load(Ordering::SeqCst) < S3_READ_OUTPUT_BUFFER {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(fixture.reads.load(Ordering::SeqCst), S3_READ_OUTPUT_BUFFER);
        assert!(fixture
            .executor
            .handle_frame(S3ReadFrame::Cancel(S3ReadCancel {
                stream_id: stream_id.to_owned(),
            }))
            .unwrap()
            .is_none());

        let mut saw_end = false;
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(frame) = receiver.recv().await {
                saw_end |= matches!(frame, S3ReadFrame::End(_));
            }
        })
        .await
        .unwrap();
        assert!(!saw_end);
        assert!(fixture.reads.load(Ordering::SeqCst) < frame_count);
    }

    #[tokio::test]
    async fn payload_is_split_into_protocol_sized_frames() {
        let size = S3_READ_FRAME_MAX_BYTES * 2 + 7;
        let expected = (0..size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let fixture = Fixture::new(expected.clone());
        let mut receiver = fixture
            .executor
            .open_at(
                S3ReadOpen {
                    stream_id: "framed".to_owned(),
                    ticket: fixture.ticket(size, "nested/file.bin"),
                    start: 0,
                    end_exclusive: size as u64,
                },
                UnixMillis::new(2_000),
            )
            .unwrap();

        assert!(matches!(receiver.recv().await, Some(S3ReadFrame::Head(_))));
        let mut offsets = Vec::new();
        let mut assembled = Vec::new();
        loop {
            match receiver.recv().await.unwrap() {
                S3ReadFrame::Data(data) => {
                    assert!(data.bytes.len() <= S3_READ_FRAME_MAX_BYTES);
                    offsets.push(data.offset);
                    assembled.extend_from_slice(&data.bytes);
                }
                S3ReadFrame::End(_) => break,
                frame => panic!("unexpected S3 read frame: {frame:?}"),
            }
        }
        assert_eq!(
            offsets,
            vec![
                0,
                S3_READ_FRAME_MAX_BYTES as u64,
                (S3_READ_FRAME_MAX_BYTES * 2) as u64,
            ]
        );
        assert_eq!(assembled, expected);
        assert_eq!(fixture.reads.load(Ordering::SeqCst), 3);
    }
}
