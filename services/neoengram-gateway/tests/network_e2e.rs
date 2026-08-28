use std::{
    collections::VecDeque,
    convert::Infallible,
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    pin::Pin,
    process::{Child, Command, Stdio},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use http::{
    header::{ACCEPT, CONTENT_TYPE},
    Method, Request, Version,
};
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    client::conn::http2,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use neoengram_domain::protocol::{
    AgentAuthenticatedRequest, AgentBootId, AgentBootstrapProof, AgentChannelDownstreamFrame,
    AgentChannelDownstreamMessage, AgentChannelNdjsonDecoder, AgentChannelUpstreamFrame,
    AgentChannelUpstreamMessage, AgentChannelUpstreamPayload, AgentId, AgentInstallationId,
    AgentMountId, AgentMountIdentityDigest, AgentRequestProof, AgentSessionOpenPayload,
    AgentSessionOpenResponse, AssignmentGeneration, AssignmentId, ContentDigest,
    DecisionGeneration, Ed25519PublicKeySpki, Ed25519Signature, Extensions, GatewayAgentAction,
    GatewayConnectionId, GatewayControlError, GatewayControlFrame, GatewayControlMessage,
    GatewayControlNdjsonDecoder, GatewayErrorCode, GatewayOpaqueBytes, GatewayPeerForwardAccepted,
    GatewayPeerForwardRequest, GatewayPoolId, GatewayReplicaId, GatewayRouteLeaseGranted,
    IndexRevision, JobDecision, JobId, JobState, MessageId, MountGeneration, OwnerGeneration,
    PublishDecision, RequestId, ResourceVersion, RouteGeneration, SequenceNumber,
    SessionGeneration, SessionId, UnixMillis, WireIndexVersion, AGENT_SESSION_CHANNEL_OPEN_PATH,
    CURRENT_WIRE_VERSION, GATEWAY_CONTROL_CHANNEL_PATH,
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};

const EDGE_CLUSTER_ID: &str = "cluster-network-e2e";
const GATEWAY_POOL_ID: &str = "pool-network-e2e";
const AGENT_ID: &str = "agent-network-e2e";
const IO_TIMEOUT: Duration = Duration::from_secs(5);

// This harness speaks the real versioned Central protocol while leaving domain persistence to
// focused Registry tests. Every Gateway listener, H2 stream, and peer hop below is a real socket.

#[derive(Debug, Clone, Copy)]
struct GatewayPorts {
    agent: u16,
    control: u16,
    peer: u16,
}

impl GatewayPorts {
    fn allocate() -> Self {
        let listeners = (0..3)
            .map(|_| StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
            .collect::<Vec<_>>();
        let ports = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect::<Vec<_>>();
        Self {
            agent: ports[0],
            control: ports[1],
            peer: ports[2],
        }
    }

    fn address(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }
}

struct GatewayProcess {
    child: Option<Child>,
    ports: GatewayPorts,
}

impl GatewayProcess {
    async fn start(replica_id: &str) -> Self {
        // The three ephemeral sockets are released before the child binds them. A parallel test
        // or another local process can win that small race, so retry with a fresh allocation
        // instead of treating a bind collision as a Gateway protocol failure.
        for attempt in 0..8 {
            let ports = GatewayPorts::allocate();
            let child = Command::new(env!("CARGO_BIN_EXE_neoengram-gateway"))
                .args([
                    "--edge-cluster-id",
                    EDGE_CLUSTER_ID,
                    "--gateway-pool-id",
                    GATEWAY_POOL_ID,
                    "--gateway-replica-id",
                    replica_id,
                    "--agent-listen",
                    &GatewayPorts::address(ports.agent).to_string(),
                    "--control-listen",
                    &GatewayPorts::address(ports.control).to_string(),
                    "--peer-listen",
                    &GatewayPorts::address(ports.peer).to_string(),
                    "--log",
                    "neoengram_gateway=error",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("Gateway process must start");
            let mut process = Self {
                child: Some(child),
                ports,
            };
            if process.wait_until_listening().await {
                return process;
            }
            process.stop();
            assert!(
                attempt < 7,
                "Gateway control listener did not start after retrying ephemeral ports"
            );
        }
        unreachable!("the bounded Gateway process startup retry loop always returns or asserts")
    }

    async fn wait_until_listening(&mut self) -> bool {
        for _ in 0..150 {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                let _ = status;
                return false;
            }
            if TcpStream::connect(GatewayPorts::address(self.ports.control))
                .await
                .is_ok()
            {
                return true;
            }
            sleep(Duration::from_millis(20)).await;
        }
        false
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

struct ChannelBody {
    frames: mpsc::Receiver<Bytes>,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.get_mut().frames)
            .poll_recv(context)
            .map(|frame| frame.map(|bytes| Ok(Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.frames.is_closed() && self.frames.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

struct CentralLink {
    replica_id: GatewayReplicaId,
    connection_id: GatewayConnectionId,
    output: mpsc::Sender<Bytes>,
    input: mpsc::Receiver<GatewayControlFrame>,
    next_sequence: u64,
    driver: JoinHandle<()>,
    reader: JoinHandle<()>,
}

impl CentralLink {
    async fn connect(replica_id: &str, control_port: u16) -> Self {
        let stream = timeout(
            IO_TIMEOUT,
            TcpStream::connect(GatewayPorts::address(control_port)),
        )
        .await
        .expect("control TCP connect timed out")
        .expect("control TCP connect failed");
        let (mut sender, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .expect("control HTTP/2 handshake failed");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let (output, body) = mpsc::channel(64);
        let uri = format!("http://127.0.0.1:{control_port}{GATEWAY_CONTROL_CHANNEL_PATH}");
        let request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, "application/x-ndjson")
            .header(ACCEPT, "application/x-ndjson")
            .header("x-request-id", format!("central-{replica_id}"))
            .body(ChannelBody { frames: body })
            .unwrap();
        let mut response: http::Response<Incoming> =
            timeout(IO_TIMEOUT, sender.send_request(request))
                .await
                .expect("control HTTP response timed out")
                .expect("control HTTP request failed");
        assert!(response.status().is_success());
        assert_eq!(response.version(), Version::HTTP_2);

        let (input_sender, input) = mpsc::channel(128);
        let reader = tokio::spawn(async move {
            let mut decoder = GatewayControlNdjsonDecoder::new();
            while let Some(frame) = response.body_mut().frame().await {
                let Ok(frame) = frame else { return };
                let Ok(bytes) = frame.into_data() else {
                    continue;
                };
                let Ok(lines) = decoder.push(&bytes) else {
                    return;
                };
                for line in lines {
                    let Ok(frame) = GatewayControlFrame::decode_json(&line) else {
                        return;
                    };
                    if input_sender.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        });
        let mut link = Self {
            replica_id: GatewayReplicaId::new(replica_id).unwrap(),
            connection_id: GatewayConnectionId::new("pending-control-connection").unwrap(),
            output,
            input,
            next_sequence: 1,
            driver,
            reader,
        };
        let hello = link.receive().await;
        assert_eq!(hello.sequence, SequenceNumber::new(1));
        assert_eq!(hello.hop_count, 0);
        assert_eq!(hello.gateway_replica_id, link.replica_id);
        assert!(matches!(
            hello.message,
            GatewayControlMessage::ReplicaHello(_)
        ));
        link.connection_id = hello.connection_id;
        link
    }

    async fn send(&mut self, request_id: RequestId, message: GatewayControlMessage) {
        let now = now_unix_ms();
        let frame = GatewayControlFrame {
            wire_version: CURRENT_WIRE_VERSION,
            gateway_pool_id: GatewayPoolId::new(GATEWAY_POOL_ID).unwrap(),
            gateway_replica_id: self.replica_id.clone(),
            connection_id: self.connection_id.clone(),
            sequence: SequenceNumber::new(self.next_sequence),
            request_id,
            trace_id: None,
            sent_at_unix_ms: now,
            deadline_unix_ms: UnixMillis::new(now.get().saturating_add(5_000)),
            hop_count: 0,
            message,
            extensions: Extensions::new(),
        };
        self.next_sequence += 1;
        self.output
            .send(Bytes::from(frame.encode_ndjson().unwrap()))
            .await
            .expect("Gateway control request body closed");
    }

    async fn receive(&mut self) -> GatewayControlFrame {
        timeout(IO_TIMEOUT, self.input.recv())
            .await
            .expect("Gateway control frame timed out")
            .expect("Gateway control stream closed")
    }

    async fn receive_for_request(&mut self, request_id: &RequestId) -> GatewayControlFrame {
        loop {
            let frame = self.receive().await;
            if &frame.request_id == request_id {
                return frame;
            }
        }
    }
}

impl Drop for CentralLink {
    fn drop(&mut self) {
        self.driver.abort();
        self.reader.abort();
    }
}

struct AgentStream {
    body_sender: Option<mpsc::Sender<Bytes>>,
    response: Incoming,
    decoder: AgentChannelNdjsonDecoder,
    ready: VecDeque<Vec<u8>>,
    driver: JoinHandle<()>,
}

impl AgentStream {
    async fn open(agent_port: u16, request_id: &str, open: &AgentChannelUpstreamFrame) -> Self {
        let stream = timeout(
            IO_TIMEOUT,
            TcpStream::connect(GatewayPorts::address(agent_port)),
        )
        .await
        .expect("Agent TCP connect timed out")
        .expect("Agent TCP connect failed");
        let (mut sender, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .expect("Agent HTTP/2 handshake failed");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let (body_sender, body) = mpsc::channel(8);
        body_sender
            .send(Bytes::from(open.encode_ndjson().unwrap()))
            .await
            .unwrap();
        let uri = format!("http://127.0.0.1:{agent_port}{AGENT_SESSION_CHANNEL_OPEN_PATH}");
        let request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, "application/x-ndjson")
            .header(ACCEPT, "application/x-ndjson")
            .header("x-request-id", request_id)
            .body(ChannelBody { frames: body })
            .unwrap();
        let response = timeout(IO_TIMEOUT, sender.send_request(request))
            .await
            .expect("Agent HTTP response timed out")
            .expect("Agent HTTP request failed");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.version(), Version::HTTP_2);
        Self {
            body_sender: Some(body_sender),
            response: response.into_body(),
            decoder: AgentChannelNdjsonDecoder::new(),
            ready: VecDeque::new(),
            driver,
        }
    }

    async fn receive_line(&mut self) -> Option<Vec<u8>> {
        if let Some(line) = self.ready.pop_front() {
            return Some(line);
        }
        loop {
            let frame = self.response.frame().await?;
            let frame = frame.expect("Agent response body failed");
            let Ok(bytes) = frame.into_data() else {
                continue;
            };
            self.ready
                .extend(self.decoder.push(&bytes).expect("Agent NDJSON is invalid"));
            if let Some(line) = self.ready.pop_front() {
                return Some(line);
            }
        }
    }
}

impl Drop for AgentStream {
    fn drop(&mut self) {
        self.body_sender.take();
        self.driver.abort();
    }
}

struct ObservedAgentOpen {
    stream_id: GatewayConnectionId,
    frame: AgentChannelUpstreamFrame,
}

async fn observe_agent_open(link: &mut CentralLink, request_id: &RequestId) -> ObservedAgentOpen {
    let stream_id = loop {
        let frame = link.receive_for_request(request_id).await;
        if let GatewayControlMessage::AgentStreamOpen(open) = frame.message {
            assert_eq!(open.action, GatewayAgentAction::SessionChannelOpen);
            break open.stream_id;
        }
    };
    let mut decoder = AgentChannelNdjsonDecoder::new();
    loop {
        let frame = link.receive_for_request(request_id).await;
        let GatewayControlMessage::AgentStreamData(data) = frame.message else {
            continue;
        };
        assert_eq!(data.stream_id, stream_id);
        let lines = decoder.push(data.chunk.as_bytes()).unwrap();
        if let Some(line) = lines.first() {
            let frame = AgentChannelUpstreamFrame::decode_json(line).unwrap();
            assert_eq!(frame.request.agent_id, AgentId::new(AGENT_ID).unwrap());
            frame.verify_open().unwrap();
            return ObservedAgentOpen { stream_id, frame };
        }
    }
}

async fn grant_agent_route(
    link: &mut CentralLink,
    request_id: RequestId,
    observed: &ObservedAgentOpen,
    session_generation: SessionGeneration,
    route_generation: RouteGeneration,
    lease_expires_at_unix_ms: UnixMillis,
) {
    link.send(
        request_id.clone(),
        GatewayControlMessage::RouteGranted(GatewayRouteLeaseGranted {
            agent_id: observed.frame.request.agent_id.clone(),
            owner_replica_id: link.replica_id.clone(),
            agent_connection_id: observed.stream_id.clone(),
            session_generation,
            route_generation,
            lease_expires_at_unix_ms,
            replayed: false,
        }),
    )
    .await;
    let now = now_unix_ms();
    let opened = AgentChannelDownstreamFrame {
        wire_version: CURRENT_WIRE_VERSION,
        sequence: SequenceNumber::new(1),
        message_id: MessageId::new(format!("opened-{}", session_generation.get())).unwrap(),
        correlation_id: Some(observed.frame.request.payload.message_id.clone()),
        session_generation,
        sent_at_unix_ms: now,
        central_signature: None,
        message: AgentChannelDownstreamMessage::Opened(AgentSessionOpenResponse {
            wire_version: CURRENT_WIRE_VERSION,
            request_id: observed.frame.request.request_id.clone(),
            agent_id: observed.frame.request.agent_id.clone(),
            session_id: SessionId::new(format!("session-{}", session_generation.get())).unwrap(),
            session_generation,
            agent_mount_id: AgentMountId::new("mount-network-e2e").unwrap(),
            mount_generation: MountGeneration::new(session_generation.get()),
            owner_generation: OwnerGeneration::new(session_generation.get()),
            resource_version: ResourceVersion::new(session_generation.get() + 1),
            opened_at_unix_ms: now,
            replayed: false,
            extensions: Extensions::new(),
        }),
        extensions: Extensions::new(),
    };
    link.send(
        request_id,
        GatewayControlMessage::AgentStreamData(
            neoengram_domain::protocol::GatewayAgentStreamData {
                stream_id: observed.stream_id.clone(),
                chunk: GatewayOpaqueBytes::new(opened.encode_ndjson().unwrap()).unwrap(),
            },
        ),
    )
    .await;
}

async fn forward_decision(
    ingress: &mut CentralLink,
    target_replica_id: &str,
    target_peer_port: u16,
    observed: &ObservedAgentOpen,
    session_generation: SessionGeneration,
    route_generation: RouteGeneration,
    decision_number: u64,
) -> Vec<u8> {
    let decision = decision_frame(session_generation, decision_number);
    let request_id = RequestId::new(format!("peer-forward-{decision_number}")).unwrap();
    ingress
        .send(
            request_id.clone(),
            GatewayControlMessage::PeerForward(GatewayPeerForwardRequest {
                source_replica_id: ingress.replica_id.clone(),
                target_replica_id: GatewayReplicaId::new(target_replica_id).unwrap(),
                target_peer_endpoint: format!("http://127.0.0.1:{target_peer_port}"),
                agent_id: AgentId::new(AGENT_ID).unwrap(),
                agent_connection_id: observed.stream_id.clone(),
                session_generation,
                route_generation,
                frame: GatewayOpaqueBytes::new(decision.clone()).unwrap(),
            }),
        )
        .await;
    let acknowledgement = ingress.receive_for_request(&request_id).await;
    let GatewayControlMessage::PeerForwardAccepted(GatewayPeerForwardAccepted {
        source_replica_id,
        target_replica_id: acknowledged_target,
        agent_connection_id,
        session_generation: acknowledged_session,
        route_generation: acknowledged_route,
        ..
    }) = acknowledgement.message
    else {
        panic!("ingress Gateway did not return the peer-forward acknowledgement")
    };
    assert_eq!(source_replica_id, ingress.replica_id);
    assert_eq!(
        acknowledged_target,
        GatewayReplicaId::new(target_replica_id).unwrap()
    );
    assert_eq!(agent_connection_id, observed.stream_id);
    assert_eq!(acknowledged_session, session_generation);
    assert_eq!(acknowledged_route, route_generation);
    decision
}

fn agent_open_frame(request_id: &str, message_id: &str) -> AgentChannelUpstreamFrame {
    let document = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
    let key_pair = Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap();
    let mut frame = AgentChannelUpstreamFrame {
        request: AgentAuthenticatedRequest {
            wire_version: CURRENT_WIRE_VERSION,
            request_id: RequestId::new(request_id).unwrap(),
            agent_id: AgentId::new(AGENT_ID).unwrap(),
            installation_id: AgentInstallationId::new("installation-network-e2e").unwrap(),
            boot_id: AgentBootId::new(format!("boot-{request_id}")).unwrap(),
            session_id: None,
            session_generation: None,
            signed_at_unix_ms: now_unix_ms(),
            payload: AgentChannelUpstreamPayload {
                sequence: SequenceNumber::new(1),
                message_id: MessageId::new(message_id).unwrap(),
                correlation_id: None,
                message: AgentChannelUpstreamMessage::Open(AgentSessionOpenPayload {
                    mount_identity_digest: AgentMountIdentityDigest::new(ContentDigest::hash(
                        b"network-e2e-mount",
                    )),
                    expected_resource_version: ResourceVersion::new(1),
                    capabilities: None,
                    extensions: Extensions::new(),
                }),
                extensions: Extensions::new(),
            },
            proof: AgentRequestProof {
                unsigned_body_digest: ContentDigest::hash(b"pending"),
                possession: AgentBootstrapProof::new(
                    Ed25519PublicKeySpki::from_public_key_bytes(
                        key_pair.public_key().as_ref().try_into().unwrap(),
                    ),
                    Ed25519Signature::from_bytes([0; 64]),
                ),
            },
            extensions: Extensions::new(),
        },
    };
    frame.request.proof.unsigned_body_digest = frame.computed_unsigned_body_digest().unwrap();
    frame.request.proof.possession.signature = Ed25519Signature::new(
        key_pair
            .sign(&frame.signing_bytes().unwrap())
            .as_ref()
            .to_vec(),
    )
    .unwrap();
    frame.verify_open().unwrap();
    frame
}

fn decision_frame(session_generation: SessionGeneration, number: u64) -> Vec<u8> {
    AgentChannelDownstreamFrame {
        wire_version: CURRENT_WIRE_VERSION,
        sequence: SequenceNumber::new(2),
        message_id: MessageId::new(format!("decision-message-{number}")).unwrap(),
        correlation_id: None,
        session_generation,
        sent_at_unix_ms: now_unix_ms(),
        central_signature: None,
        message: AgentChannelDownstreamMessage::Decision(JobDecision {
            job_id: JobId::new(format!("job-{number}")).unwrap(),
            assignment_id: AssignmentId::new(format!("assignment-{number}")).unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            decision_generation: DecisionGeneration::new(1),
            decision: PublishDecision::Publish {
                published_index_version: WireIndexVersion {
                    revision: IndexRevision::new(number),
                    digest: ContentDigest::hash(number.to_be_bytes()),
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            },
            final_state: JobState::Succeeded,
            extensions: Extensions::new(),
        }),
        extensions: Extensions::new(),
    }
    .encode_ndjson()
    .unwrap()
}

fn now_unix_ms() -> UnixMillis {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    UnixMillis::new(u64::try_from(millis).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loopback_h2_peer_forwarding_recovers_after_the_owner_lease_expires() {
    let mut gateway_a = GatewayProcess::start("replica-a").await;
    let mut gateway_b = GatewayProcess::start("replica-b").await;
    let mut central_a = CentralLink::connect("replica-a", gateway_a.ports.control).await;
    let mut central_b = CentralLink::connect("replica-b", gateway_b.ports.control).await;

    let first_request_id = RequestId::new("agent-open-owner-b").unwrap();
    let first_open = agent_open_frame(first_request_id.as_str(), "agent-open-message-b");
    let mut agent_b = AgentStream::open(
        gateway_b.ports.agent,
        first_request_id.as_str(),
        &first_open,
    )
    .await;
    let first_observed = observe_agent_open(&mut central_b, &first_request_id).await;
    let first_session = SessionGeneration::new(1);
    let first_route = RouteGeneration::new(1);
    let first_expiry = UnixMillis::new(now_unix_ms().get().saturating_add(3_000));
    grant_agent_route(
        &mut central_b,
        first_request_id,
        &first_observed,
        first_session,
        first_route,
        first_expiry,
    )
    .await;
    let opened = timeout(IO_TIMEOUT, agent_b.receive_line())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        AgentChannelDownstreamFrame::decode_json(&opened)
            .unwrap()
            .message,
        AgentChannelDownstreamMessage::Opened(_)
    ));

    let first_decision = forward_decision(
        &mut central_a,
        "replica-b",
        gateway_b.ports.peer,
        &first_observed,
        first_session,
        first_route,
        1,
    )
    .await;
    let delivered = timeout(IO_TIMEOUT, agent_b.receive_line())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered, first_decision[..first_decision.len() - 1]);

    gateway_b.stop();
    drop(agent_b);
    drop(central_b);

    let dead_owner_request_id = RequestId::new("peer-forward-dead-owner").unwrap();
    let dead_owner_decision = decision_frame(first_session, 99);
    central_a
        .send(
            dead_owner_request_id.clone(),
            GatewayControlMessage::PeerForward(GatewayPeerForwardRequest {
                source_replica_id: central_a.replica_id.clone(),
                target_replica_id: GatewayReplicaId::new("replica-b").unwrap(),
                target_peer_endpoint: format!("http://127.0.0.1:{}", gateway_b.ports.peer),
                agent_id: AgentId::new(AGENT_ID).unwrap(),
                agent_connection_id: first_observed.stream_id.clone(),
                session_generation: first_session,
                route_generation: first_route,
                frame: GatewayOpaqueBytes::new(dead_owner_decision).unwrap(),
            }),
        )
        .await;
    let dead_owner_response = central_a.receive_for_request(&dead_owner_request_id).await;
    let GatewayControlMessage::Error(error) = dead_owner_response.message else {
        panic!("a dead owner must produce a fail-closed Gateway error");
    };
    assert_eq!(error.code, GatewayErrorCode::RouteUnavailable);
    assert!(error.retryable);

    let fenced_request_id = RequestId::new("agent-open-before-expiry-a").unwrap();
    let fenced_open = agent_open_frame(fenced_request_id.as_str(), "agent-open-message-fenced");
    let mut fenced_agent = AgentStream::open(
        gateway_a.ports.agent,
        fenced_request_id.as_str(),
        &fenced_open,
    )
    .await;
    let fenced_observed = observe_agent_open(&mut central_a, &fenced_request_id).await;
    assert_eq!(
        fenced_observed.frame.request.agent_id,
        first_observed.frame.request.agent_id
    );
    assert!(now_unix_ms().get() < first_expiry.get());
    central_a
        .send(
            fenced_request_id,
            GatewayControlMessage::Error(GatewayControlError {
                code: GatewayErrorCode::RouteFenced,
                detail: "the previous owner lease is still active".to_owned(),
                retryable: false,
            }),
        )
        .await;
    let fenced_error = timeout(IO_TIMEOUT, fenced_agent.receive_line())
        .await
        .expect("fenced Agent response must include a structured error")
        .expect("fenced Agent response must not close before the error frame");
    let fenced_error = AgentChannelDownstreamFrame::decode_json(&fenced_error)
        .expect("Gateway fencing error must be a valid Agent frame");
    let AgentChannelDownstreamMessage::Error(fenced_error) = fenced_error.message else {
        panic!("fenced Agent response must be a protocol error");
    };
    assert_eq!(fenced_error.code.as_str(), "GATEWAY_ROUTE_FENCED");
    assert!(!fenced_error.retryable);
    assert!(timeout(IO_TIMEOUT, fenced_agent.receive_line())
        .await
        .expect("fenced Agent stream must close after the error")
        .is_none());
    drop(fenced_agent);

    let remaining = first_expiry
        .get()
        .saturating_sub(now_unix_ms().get())
        .saturating_add(50);
    sleep(Duration::from_millis(remaining)).await;

    let recovery_request_id = RequestId::new("agent-open-after-expiry-a").unwrap();
    let recovery_open =
        agent_open_frame(recovery_request_id.as_str(), "agent-open-message-recovered");
    let mut recovered_agent = AgentStream::open(
        gateway_a.ports.agent,
        recovery_request_id.as_str(),
        &recovery_open,
    )
    .await;
    let recovered_observed = observe_agent_open(&mut central_a, &recovery_request_id).await;
    let recovered_session = SessionGeneration::new(2);
    let recovered_route = RouteGeneration::new(2);
    grant_agent_route(
        &mut central_a,
        recovery_request_id,
        &recovered_observed,
        recovered_session,
        recovered_route,
        UnixMillis::new(now_unix_ms().get().saturating_add(30_000)),
    )
    .await;
    let recovered_opened = timeout(IO_TIMEOUT, recovered_agent.receive_line())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        AgentChannelDownstreamFrame::decode_json(&recovered_opened)
            .unwrap()
            .session_generation,
        recovered_session
    );

    let mut replacement_b = GatewayProcess::start("replica-b").await;
    let mut replacement_central_b =
        CentralLink::connect("replica-b", replacement_b.ports.control).await;
    let recovered_decision = forward_decision(
        &mut replacement_central_b,
        "replica-a",
        gateway_a.ports.peer,
        &recovered_observed,
        recovered_session,
        recovered_route,
        2,
    )
    .await;
    let delivered = timeout(IO_TIMEOUT, recovered_agent.receive_line())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivered,
        recovered_decision[..recovered_decision.len() - 1]
    );

    replacement_b.stop();
    gateway_a.stop();
}
