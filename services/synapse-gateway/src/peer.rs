use std::{fmt::Write as _, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::{
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE},
    HeaderMap, Request, Version,
};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Body, Incoming},
    client::conn::http2,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use neoengram_domain::protocol::{
    GatewayControlError, GatewayControlFrame, GatewayControlMessage, GatewayErrorCode,
    GatewayPeerForwardAccepted, GatewayReplicaId, S3ReadChannelFrame, S3ReadFrame, S3ReadOpen,
    S3ReadTicket, GATEWAY_PEER_FORWARD_PATH, MAX_GATEWAY_CONTROL_FRAME_BYTES,
    S3_READ_CHANNEL_CONTENT_TYPE, S3_READ_FRAME_MAX_BYTES, S3_READ_PEER_PATH,
};
use rustls::ClientConfig;
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    time::{timeout, Instant},
};
use tokio_rustls::TlsConnector;
use url::{Position, Url};
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer, X509Certificate},
};

use crate::{
    public_listener::{BoxError, S3ControlError},
    s3_backend::S3ObjectRead,
    s3_read_channel::S3PeerReader,
    tunnel::{GatewayIdentity, PeerForwarder},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PEER_IN_FLIGHT: usize = 256;
const S3_PEER_OUTPUT_BUFFER: usize = 8;

trait PeerIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> PeerIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Concrete one-shot HTTP/2 peer transport. The endpoint is supplied by Central's persisted
/// Replica record for each request; this object never maintains a static peer directory.
pub(crate) struct H2PeerForwarder {
    identity: GatewayIdentity,
    tls: Option<Arc<ClientConfig>>,
    workload_trust_domain: Option<Arc<str>>,
    allow_loopback_http: bool,
    admission: Arc<Semaphore>,
}

impl H2PeerForwarder {
    pub(crate) fn new(
        identity: GatewayIdentity,
        tls: Option<Arc<ClientConfig>>,
        workload_trust_domain: Option<Arc<str>>,
        allow_loopback_http: bool,
    ) -> Self {
        Self {
            identity,
            tls,
            workload_trust_domain,
            allow_loopback_http,
            admission: Arc::new(Semaphore::new(MAX_PEER_IN_FLIGHT)),
        }
    }

    async fn forward_inner(
        &self,
        target_peer_endpoint: &str,
        frame: GatewayControlFrame,
        _permit: OwnedSemaphorePermit,
    ) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
        let endpoint = parse_peer_endpoint(target_peer_endpoint, self.allow_loopback_http)?;
        let host =
            endpoint_host(&endpoint).ok_or_else(|| endpoint_error("peer endpoint has no host"))?;
        let port = endpoint
            .port_or_known_default()
            .ok_or_else(|| endpoint_error("peer endpoint has no port"))?;
        let remaining = frame
            .deadline_unix_ms
            .get()
            .saturating_sub(now_unix_ms().get());
        if remaining == 0 {
            return Err(control_error(
                GatewayErrorCode::DeadlineExceeded,
                "peer forwarding deadline has elapsed",
                false,
            ));
        }
        let connect_deadline =
            Instant::now() + CONNECT_TIMEOUT.min(Duration::from_millis(remaining));
        let stream = timeout(
            deadline_duration(connect_deadline),
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| unavailable("peer TCP connection timed out"))?
        .map_err(|error| unavailable(format!("peer TCP connection failed: {error}")))?;
        let (stream, target_replica_id) = match endpoint.scheme() {
            "http" => (
                Box::new(stream) as Box<dyn PeerIo>,
                frame_target_replica(&frame)?,
            ),
            "https" => {
                let tls = self
                    .tls
                    .clone()
                    .ok_or_else(|| unavailable("peer mTLS client identity is unavailable"))?;
                let server_name = ServerName::try_from(host.clone()).map_err(|error| {
                    endpoint_error(format!("peer TLS server name is invalid: {error}"))
                })?;
                let tls_stream = timeout(
                    deadline_duration(connect_deadline),
                    TlsConnector::from(tls).connect(server_name, stream),
                )
                .await
                .map_err(|_| unavailable("peer TLS handshake timed out"))?
                .map_err(|error| unavailable(format!("peer TLS handshake failed: {error}")))?;
                let target = frame_target_replica(&frame)?;
                verify_peer_server_identity(
                    tls_stream.get_ref().1.peer_certificates(),
                    &self.identity,
                    &target,
                    self.workload_trust_domain.as_deref(),
                )?;
                (Box::new(tls_stream) as Box<dyn PeerIo>, target)
            }
            _ => return Err(endpoint_error("peer endpoint must use HTTP or HTTPS")),
        };
        let (mut sender, connection) = timeout(
            deadline_duration(connect_deadline),
            http2::handshake(TokioExecutor::new(), TokioIo::new(stream)),
        )
        .await
        .map_err(|_| unavailable("peer HTTP/2 handshake timed out"))?
        .map_err(|error| unavailable(format!("peer HTTP/2 handshake failed: {error}")))?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "Gateway peer HTTP/2 connection ended");
            }
        });
        let body = serde_json::to_vec(&frame)
            .map_err(|error| protocol_error(format!("peer frame encoding failed: {error}")))?;
        let uri = endpoint
            .join(GATEWAY_PEER_FORWARD_PATH)
            .map_err(|error| endpoint_error(format!("peer endpoint path is invalid: {error}")))?;
        let request = Request::builder()
            .method("POST")
            .version(Version::HTTP_2)
            .uri(uri.as_str())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .body(Full::new(Bytes::from(body)))
            .map_err(|error| protocol_error(format!("peer request metadata failed: {error}")))?;
        let mut response: http::Response<Incoming> = timeout(
            deadline_duration(connect_deadline),
            sender.send_request(request),
        )
        .await
        .map_err(|_| unavailable("peer forwarding request timed out"))?
        .map_err(|error| unavailable(format!("peer forwarding request failed: {error}")))?;
        if !response.status().is_success() || response.version() != Version::HTTP_2 {
            let status = response.status();
            return Err(match status {
                http::StatusCode::REQUEST_TIMEOUT | http::StatusCode::GATEWAY_TIMEOUT => {
                    control_error(
                        GatewayErrorCode::DeadlineExceeded,
                        format!("owner Replica returned {status}"),
                        false,
                    )
                }
                http::StatusCode::TOO_MANY_REQUESTS => control_error(
                    GatewayErrorCode::ResourceExhausted,
                    format!("owner Replica returned {status}"),
                    true,
                ),
                http::StatusCode::UNAUTHORIZED | http::StatusCode::FORBIDDEN => {
                    identity_rejected(format!("owner Replica rejected peer request with {status}"))
                }
                status if status.is_server_error() => {
                    unavailable(format!("owner Replica returned {status}"))
                }
                _ => protocol_error(format!("owner Replica rejected peer request with {status}")),
            });
        }
        validate_peer_response_headers(response.headers(), MAX_GATEWAY_CONTROL_FRAME_BYTES)?;
        let bytes = read_bounded_response_body(
            response.body_mut(),
            connect_deadline,
            MAX_GATEWAY_CONTROL_FRAME_BYTES,
        )
        .await?;
        let response_frame = GatewayControlFrame::decode_json(&bytes)
            .map_err(|error| protocol_error(format!("peer response frame is invalid: {error}")))?;
        response_frame
            .validate_at(now_unix_ms())
            .map_err(|error| protocol_error(format!("peer response validation failed: {error}")))?;
        if response_frame.hop_count != 1
            || response_frame.gateway_pool_id != self.identity.gateway_pool_id
            || response_frame.gateway_replica_id != target_replica_id
            || response_frame.request_id != frame.request_id
        {
            return Err(identity_rejected(
                "peer response identity, hop, or request ID does not match",
            ));
        }
        match response_frame.message {
            GatewayControlMessage::PeerForwardAccepted(accepted)
                if accepted.target_replica_id == target_replica_id
                    && accepted.source_replica_id == self.identity.gateway_replica_id =>
            {
                Ok(accepted)
            }
            GatewayControlMessage::Error(error) => Err(error),
            _ => Err(protocol_error("peer response is not an acknowledgement")),
        }
    }
}

fn validate_peer_response_headers(
    headers: &HeaderMap,
    max_bytes: usize,
) -> Result<(), GatewayControlError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| protocol_error("peer response has no valid Content-Type"))?;
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return Err(protocol_error(
            "peer response Content-Type is not application/json",
        ));
    }
    if let Some(content_length) = headers.get(CONTENT_LENGTH) {
        let content_length = content_length
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| protocol_error("peer response Content-Length is invalid"))?;
        if content_length > max_bytes as u64 {
            return Err(protocol_error(
                "peer response exceeds the Gateway frame limit",
            ));
        }
    }
    Ok(())
}

/// Reads a peer response without ever accumulating more than the protocol frame limit.  H2 can
/// deliver an untrusted response in arbitrarily many DATA frames, so checking the size only after
/// `collect` would allow a remote Replica to consume unbounded memory first.
async fn read_bounded_response_body<B>(
    body: &mut B,
    deadline: Instant,
    max_bytes: usize,
) -> Result<Bytes, GatewayControlError>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    let mut bytes = BytesMut::new();
    loop {
        let frame = timeout(deadline_duration(deadline), body.frame())
            .await
            .map_err(|_| unavailable("peer response body timed out"))?;
        let Some(frame) = frame else {
            break;
        };
        let frame =
            frame.map_err(|error| unavailable(format!("peer response body failed: {error}")))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.len() > max_bytes.saturating_sub(bytes.len()) {
            return Err(protocol_error(
                "peer response exceeds the Gateway frame limit",
            ));
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes.freeze())
}

#[async_trait]
impl PeerForwarder for H2PeerForwarder {
    async fn forward(
        &self,
        target_peer_endpoint: &str,
        frame: GatewayControlFrame,
    ) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            control_error(
                GatewayErrorCode::ResourceExhausted,
                "peer forwarding queue is full",
                true,
            )
        })?;
        self.forward_inner(target_peer_endpoint, frame, permit)
            .await
    }
}

#[async_trait]
impl S3PeerReader for H2PeerForwarder {
    async fn open_peer(&self, ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError> {
        if ticket.gateway_pool_id != self.identity.gateway_pool_id.as_str()
            || ticket.owner_replica_id == self.identity.gateway_replica_id
            || ticket.validate_at(now_unix_ms()).is_err()
        {
            return Err(S3ControlError::AccessDenied);
        }
        let endpoint = parse_peer_endpoint(&ticket.owner_peer_endpoint, self.allow_loopback_http)
            .map_err(|_| S3ControlError::Unavailable)?;
        let host = endpoint_host(&endpoint).ok_or(S3ControlError::Unavailable)?;
        let port = endpoint
            .port_or_known_default()
            .ok_or(S3ControlError::Unavailable)?;
        let remaining = ticket
            .expires_at_unix_ms
            .get()
            .saturating_sub(now_unix_ms().get());
        if remaining == 0 {
            return Err(S3ControlError::AccessDenied);
        }
        let connect_deadline =
            Instant::now() + CONNECT_TIMEOUT.min(Duration::from_millis(remaining));
        let stream = timeout(
            deadline_duration(connect_deadline),
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| S3ControlError::Unavailable)?
        .map_err(|_| S3ControlError::Unavailable)?;
        let stream: Box<dyn PeerIo> = match endpoint.scheme() {
            "http" => Box::new(stream),
            "https" => {
                let tls = self.tls.clone().ok_or(S3ControlError::Unavailable)?;
                let server_name =
                    ServerName::try_from(host.clone()).map_err(|_| S3ControlError::Unavailable)?;
                let tls_stream = timeout(
                    deadline_duration(connect_deadline),
                    TlsConnector::from(tls).connect(server_name, stream),
                )
                .await
                .map_err(|_| S3ControlError::Unavailable)?
                .map_err(|_| S3ControlError::Unavailable)?;
                verify_peer_server_identity(
                    tls_stream.get_ref().1.peer_certificates(),
                    &self.identity,
                    &ticket.owner_replica_id,
                    self.workload_trust_domain.as_deref(),
                )
                .map_err(|_| S3ControlError::AccessDenied)?;
                Box::new(tls_stream)
            }
            _ => return Err(S3ControlError::Unavailable),
        };
        let (mut sender, connection) = timeout(
            deadline_duration(connect_deadline),
            http2::handshake(TokioExecutor::new(), TokioIo::new(stream)),
        )
        .await
        .map_err(|_| S3ControlError::Unavailable)?
        .map_err(|_| S3ControlError::Unavailable)?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "Gateway S3 peer HTTP/2 connection ended");
            }
        });

        let stream_id = fresh_s3_peer_stream_id().map_err(|_| S3ControlError::Internal)?;
        let expected_length = ticket
            .allowed_end_exclusive
            .saturating_sub(ticket.allowed_start);
        let open = S3ReadChannelFrame::Read(S3ReadFrame::Open(Box::new(S3ReadOpen {
            stream_id,
            start: ticket.allowed_start,
            end_exclusive: ticket.allowed_end_exclusive,
            ticket: ticket.clone(),
        })))
        .encode()
        .map_err(|_| S3ControlError::Internal)?;
        let uri = endpoint
            .join(S3_READ_PEER_PATH)
            .map_err(|_| S3ControlError::Unavailable)?;
        let request = Request::builder()
            .method("POST")
            .version(Version::HTTP_2)
            .uri(uri.as_str())
            .header(CONTENT_TYPE, S3_READ_CHANNEL_CONTENT_TYPE)
            .header(ACCEPT, "application/octet-stream")
            .header(
                "x-synapse-source-replica",
                self.identity.gateway_replica_id.as_str(),
            )
            .body(Full::new(Bytes::from(open)))
            .map_err(|_| S3ControlError::Internal)?;
        let response: http::Response<Incoming> = timeout(
            deadline_duration(connect_deadline),
            sender.send_request(request),
        )
        .await
        .map_err(|_| S3ControlError::Unavailable)?
        .map_err(|_| S3ControlError::Unavailable)?;
        if response.status() != http::StatusCode::OK || response.version() != Version::HTTP_2 {
            return Err(S3ControlError::Unavailable);
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        let content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if content_type != Some("application/octet-stream")
            || content_length != Some(expected_length)
        {
            return Err(S3ControlError::Internal);
        }

        let (output, receiver) = mpsc::channel(S3_PEER_OUTPUT_BUFFER);
        let mut body = response.into_body();
        let reader = tokio::spawn(async move {
            let mut received = 0_u64;
            loop {
                let frame = match body.frame().await {
                    Some(Ok(frame)) => frame,
                    Some(Err(error)) => {
                        let _ = output
                            .send(Err(peer_stream_error(format!(
                                "S3 peer response body failed: {error}"
                            ))))
                            .await;
                        return;
                    }
                    None if received == expected_length => return,
                    None => {
                        let _ = output
                            .send(Err(peer_stream_error(
                                "S3 peer response ended before Content-Length",
                            )))
                            .await;
                        return;
                    }
                };
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                if data.is_empty() {
                    continue;
                }
                let length = u64::try_from(data.len()).unwrap_or(u64::MAX);
                if data.len() > S3_READ_FRAME_MAX_BYTES
                    || received.saturating_add(length) > expected_length
                {
                    let _ = output
                        .send(Err(peer_stream_error(
                            "S3 peer response exceeded the authorized range",
                        )))
                        .await;
                    return;
                }
                received += length;
                if output.send(Ok(data)).await.is_err() {
                    return;
                }
            }
        });
        S3ObjectRead::new(receiver, move || reader.abort())
    }
}

fn fresh_s3_peer_stream_id() -> Result<String, ()> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|_| ())?;
    let mut value = String::with_capacity(41);
    value.push_str("s3-peer-");
    for byte in random {
        write!(&mut value, "{byte:02x}").map_err(|_| ())?;
    }
    Ok(value)
}

fn peer_stream_error(detail: impl Into<String>) -> BoxError {
    Box::new(std::io::Error::other(detail.into()))
}

fn frame_target_replica(
    frame: &GatewayControlFrame,
) -> Result<GatewayReplicaId, GatewayControlError> {
    let GatewayControlMessage::PeerForward(request) = &frame.message else {
        return Err(protocol_error("peer frame is not a peer_forward request"));
    };
    Ok(request.target_replica_id.clone())
}

fn parse_peer_endpoint(value: &str, allow_loopback_http: bool) -> Result<Url, GatewayControlError> {
    let endpoint = Url::parse(value)
        .map_err(|error| endpoint_error(format!("peer endpoint is invalid: {error}")))?;
    if &endpoint[..Position::BeforePath] != value
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
    {
        return Err(endpoint_error("peer endpoint must be a canonical origin"));
    }
    if endpoint.scheme() == "http" {
        let loopback = is_loopback_host(&endpoint);
        if !allow_loopback_http || !loopback {
            return Err(identity_rejected(
                "plaintext peer forwarding is loopback-only",
            ));
        }
    } else if endpoint.scheme() != "https" {
        return Err(endpoint_error("peer endpoint must use HTTP or HTTPS"));
    }
    Ok(endpoint)
}

fn is_loopback_host(endpoint: &Url) -> bool {
    endpoint.host().is_some_and(|host| match host {
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(address) => address.is_loopback(),
        url::Host::Ipv6(address) => address.is_loopback(),
    })
}

fn endpoint_host(endpoint: &Url) -> Option<String> {
    endpoint.host().map(|host| match host {
        url::Host::Domain(domain) => domain.to_owned(),
        url::Host::Ipv4(address) => address.to_string(),
        url::Host::Ipv6(address) => address.to_string(),
    })
}

fn verify_peer_server_identity(
    certificates: Option<&[CertificateDer<'_>]>,
    identity: &GatewayIdentity,
    target: &GatewayReplicaId,
    trust_domain: Option<&str>,
) -> Result<(), GatewayControlError> {
    let leaf = certificates
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| identity_rejected("peer server did not present a certificate"))?;
    let (_, certificate) = X509Certificate::from_der(leaf.as_ref())
        .map_err(|_| identity_rejected("peer server certificate is invalid"))?;
    let uris = certificate
        .subject_alternative_name()
        .ok()
        .flatten()
        .map(|san| {
            san.value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    GeneralName::URI(uri) => Some(*uri),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if uris.len() != 1 {
        return Err(identity_rejected(
            "peer server certificate must contain one URI SAN",
        ));
    }
    let value = uris[0];
    if value.contains(['?', '#', '%']) {
        return Err(identity_rejected("peer server URI SAN is not canonical"));
    }
    let url = Url::parse(value).map_err(|_| identity_rejected("peer server URI SAN is invalid"))?;
    if url.scheme() != "spiffe"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str() != value
    {
        return Err(identity_rejected("peer server URI SAN is not SPIFFE"));
    }
    let expected_domain = trust_domain.ok_or_else(|| {
        identity_rejected("Gateway workload trust domain is required for peer TLS")
    })?;
    if url.host_str() != Some(expected_domain) {
        return Err(identity_rejected(
            "peer server URI SAN trust domain mismatch",
        ));
    }
    let expected = format!(
        "/workloads/edge-clusters/{}/gateway-pools/{}/gateway-replicas/{}",
        identity.edge_cluster_id, identity.gateway_pool_id, target
    );
    if url.path() != expected {
        return Err(identity_rejected(
            "peer server URI SAN does not identify the target Replica",
        ));
    }
    Ok(())
}

fn deadline_duration(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

fn now_unix_ms() -> neoengram_domain::protocol::UnixMillis {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    neoengram_domain::protocol::UnixMillis::new(u64::try_from(millis).unwrap_or(u64::MAX))
}

fn control_error(
    code: GatewayErrorCode,
    detail: impl Into<String>,
    retryable: bool,
) -> GatewayControlError {
    GatewayControlError {
        code,
        detail: detail.into(),
        retryable,
    }
}

fn unavailable(detail: impl Into<String>) -> GatewayControlError {
    control_error(GatewayErrorCode::RouteUnavailable, detail, true)
}

fn endpoint_error(detail: impl Into<String>) -> GatewayControlError {
    control_error(GatewayErrorCode::ProtocolInvalid, detail, false)
}

fn identity_rejected(detail: impl Into<String>) -> GatewayControlError {
    control_error(GatewayErrorCode::IdentityRejected, detail, false)
}

fn protocol_error(detail: impl Into<String>) -> GatewayControlError {
    control_error(GatewayErrorCode::ProtocolInvalid, detail, false)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        convert::Infallible,
        pin::Pin,
        task::{Context, Poll},
    };

    use super::*;
    use hyper::{body::Frame, service::service_fn};
    use neoengram_domain::protocol::{
        AgentId, ContentDigest, EdgeClusterId, GatewayConnectionId, GatewayOpaqueBytes,
        GatewayPoolId, MountGeneration, OwnerGeneration, RouteGeneration, SessionGeneration,
        UnixMillis,
    };
    use rcgen::{CertificateParams, KeyPair, SanType};
    use tokio::net::TcpListener;

    struct ChunkedBody {
        chunks: VecDeque<Bytes>,
        polls: usize,
    }

    impl ChunkedBody {
        fn new(chunks: impl IntoIterator<Item = Bytes>) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
                polls: 0,
            }
        }
    }

    impl Body for ChunkedBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.polls += 1;
            Poll::Ready(self.chunks.pop_front().map(|chunk| Ok(Frame::data(chunk))))
        }
    }

    fn identity() -> GatewayIdentity {
        GatewayIdentity {
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            software_version: "0.2.0".to_owned(),
        }
    }

    #[test]
    fn plaintext_peer_endpoints_are_strictly_loopback_development_only() {
        parse_peer_endpoint("http://127.0.0.1:8083", true).unwrap();
        parse_peer_endpoint("http://[::1]:8083", true).unwrap();
        assert!(parse_peer_endpoint("http://127.0.0.1:8083", false).is_err());
        assert!(parse_peer_endpoint("http://gateway.example:8083", true).is_err());
        assert!(parse_peer_endpoint("https://gateway.example/path", false).is_err());
    }

    #[test]
    fn peer_success_headers_require_json_and_a_bounded_declared_length() {
        let mut headers = HeaderMap::new();
        assert!(validate_peer_response_headers(&headers, 5).is_err());

        headers.insert(CONTENT_TYPE, http::HeaderValue::from_static("text/plain"));
        assert!(validate_peer_response_headers(&headers, 5).is_err());

        headers.insert(
            CONTENT_TYPE,
            http::HeaderValue::from_static("application/json; charset=utf-8"),
        );
        headers.insert(
            CONTENT_LENGTH,
            http::HeaderValue::from_static("not-a-number"),
        );
        assert!(validate_peer_response_headers(&headers, 5).is_err());

        headers.insert(CONTENT_LENGTH, http::HeaderValue::from_static("6"));
        assert!(validate_peer_response_headers(&headers, 5).is_err());

        headers.insert(CONTENT_LENGTH, http::HeaderValue::from_static("5"));
        validate_peer_response_headers(&headers, 5).unwrap();
    }

    #[test]
    fn peer_server_uri_san_must_identify_the_exact_target_replica() {
        let mut parameters = CertificateParams::new(vec!["gateway.example.test".to_owned()])
            .expect("certificate parameters");
        parameters.subject_alt_names.push(SanType::URI(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-b"
                .try_into()
                .expect("URI SAN"),
        ));
        let key = KeyPair::generate().expect("test key");
        let certificate = parameters.self_signed(&key).expect("test certificate");
        let leaf = CertificateDer::from(certificate.der().to_vec());

        verify_peer_server_identity(
            Some(std::slice::from_ref(&leaf)),
            &identity(),
            &GatewayReplicaId::new("replica-b").unwrap(),
            Some("mesh.example.test"),
        )
        .unwrap();
        assert!(verify_peer_server_identity(
            Some(std::slice::from_ref(&leaf)),
            &identity(),
            &GatewayReplicaId::new("replica-c").unwrap(),
            Some("mesh.example.test"),
        )
        .is_err());
        assert!(verify_peer_server_identity(
            Some(std::slice::from_ref(&leaf)),
            &identity(),
            &GatewayReplicaId::new("replica-b").unwrap(),
            Some("other.example.test"),
        )
        .is_err());
        assert!(verify_peer_server_identity(
            Some(std::slice::from_ref(&leaf)),
            &identity(),
            &GatewayReplicaId::new("replica-b").unwrap(),
            None,
        )
        .is_err());
    }

    #[tokio::test]
    async fn chunked_peer_response_is_rejected_at_the_limit_before_more_frames_are_read() {
        let mut body = ChunkedBody::new([
            Bytes::from_static(b"1234"),
            Bytes::from_static(b"56"),
            Bytes::from_static(b"this frame must not be polled"),
        ]);

        let error =
            read_bounded_response_body(&mut body, Instant::now() + Duration::from_secs(1), 5)
                .await
                .expect_err("the second DATA frame crosses the response limit");
        assert_eq!(error.code, GatewayErrorCode::ProtocolInvalid);
        assert_eq!(
            body.polls, 2,
            "the reader must stop at the first oversized chunk"
        );
        assert_eq!(body.chunks.len(), 1);
    }

    #[tokio::test]
    async fn s3_peer_reader_streams_raw_h2_bytes_from_the_exact_owner_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(socket),
                    service_fn(|request: Request<Incoming>| async move {
                        assert_eq!(request.uri().path(), S3_READ_PEER_PATH);
                        assert_eq!(request.version(), Version::HTTP_2);
                        let body = request.into_body().collect().await.unwrap().to_bytes();
                        let frame = S3ReadChannelFrame::decode(&body).unwrap();
                        let S3ReadChannelFrame::Read(S3ReadFrame::Open(open)) = frame else {
                            panic!("peer request did not contain ReadOpen");
                        };
                        assert_eq!(open.ticket.owner_replica_id.as_str(), "replica-b");
                        Ok::<_, Infallible>(
                            http::Response::builder()
                                .status(http::StatusCode::OK)
                                .header(CONTENT_TYPE, "application/octet-stream")
                                .header(CONTENT_LENGTH, "4")
                                .body(Full::new(Bytes::from_static(b"data")))
                                .unwrap(),
                        )
                    }),
                )
                .await
                .unwrap();
        });

        let now = now_unix_ms();
        let ticket = S3ReadTicket {
            ticket_id: "ticket-peer-network".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            project_id: "project-a".to_owned(),
            artifact_id: "artifact-a".to_owned(),
            snapshot_id: "snapshot-a".to_owned(),
            snapshot_lifecycle_generation: neoengram_domain::protocol::LifecycleGeneration::new(1),
            commit_id: ContentDigest::hash(b"commit"),
            index_digest: ContentDigest::hash(b"index"),
            bucket: "bucket-a".to_owned(),
            access_point_policy_generation: neoengram_domain::protocol::ResourceVersion::new(1),
            logical_path: "object.bin".to_owned(),
            manifest_id: ContentDigest::hash(b"manifest"),
            size_bytes: 4,
            allowed_start: 0,
            allowed_end_exclusive: 4,
            gateway_pool_id: "pool-a".to_owned(),
            owner_replica_id: GatewayReplicaId::new("replica-b").unwrap(),
            owner_peer_endpoint: format!("http://{address}"),
            agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
            route_generation: RouteGeneration::new(5),
            agent_id: AgentId::new("agent-a").unwrap(),
            owner_generation: OwnerGeneration::new(6),
            mount_generation: MountGeneration::new(7),
            session_generation: SessionGeneration::new(8),
            issued_at_unix_ms: UnixMillis::new(now.get().saturating_sub(1)),
            expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
            signature: GatewayOpaqueBytes::new(vec![1]).unwrap(),
        };
        let reader = H2PeerForwarder::new(identity(), None, None, true);
        let read = S3PeerReader::open_peer(&reader, ticket).await.unwrap();
        let (mut bytes, mut cancellation) = read.into_parts();
        assert_eq!(
            bytes.recv().await.unwrap().unwrap(),
            Bytes::from_static(b"data")
        );
        assert!(bytes.recv().await.is_none());
        cancellation.disarm();
        server.await.unwrap();
    }
}
