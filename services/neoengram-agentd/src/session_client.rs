use std::{
    convert::Infallible,
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, USER_AGENT},
    HeaderMap, Method, Request, StatusCode, Version,
};
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    client::conn::http2,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use neoengram_protocol::{
    decode_bounded_unique_json, AgentActionAcceptedResponse, AgentAuthenticatedRequest,
    AgentBootId, AgentBootstrapProof, AgentChannelUpstreamFrame, AgentChannelUpstreamPayload,
    AgentHeartbeatReportPayload, AgentHeartbeatReportResponse, AgentId, AgentIndexPageQueryPayload,
    AgentIndexPageQueryResponse, AgentInstallationId, AgentJobReportCreatePayload,
    AgentManifestPageQueryPayload, AgentManifestPageQueryResponse, AgentMessageListQueryPayload,
    AgentMessageListQueryResponse, AgentMetadataBatchStagePayload, AgentMetadataPageStagePayload,
    AgentMetadataStageResponse, AgentRequestProof, AgentSessionClosePayload,
    AgentSessionCloseResponse, AgentSessionOpenPayload, AgentSessionOpenResponse,
    Ed25519PublicKeySpki, Ed25519Signature, Extensions, ProtocolVersion, RequestId,
    SessionGeneration, SessionId, UnixMillis, AGENT_JOB_INDEX_PAGE_QUERY_PATH,
    AGENT_JOB_MANIFEST_PAGE_QUERY_PATH, AGENT_JOB_METADATA_BATCH_STAGE_PATH,
    AGENT_JOB_METADATA_PAGE_STAGE_PATH, AGENT_JOB_REPORT_CREATE_PATH,
    AGENT_SESSION_CHANNEL_OPEN_PATH, AGENT_SESSION_CLOSE_PATH, AGENT_SESSION_HEARTBEAT_REPORT_PATH,
    AGENT_SESSION_MESSAGE_LIST_QUERY_PATH, AGENT_SESSION_OPEN_PATH,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::{net::TcpStream, sync::mpsc, task::JoinHandle};
use url::Url;
use uuid::Uuid;

use crate::{
    identity::public_key_spki_der,
    session_channel::{channel_buffers, spawn_response_reader, AgentChannelConnection},
    AgentDaemonError, AgentDaemonResult, AgentSigningKey,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 9 * 1024 * 1024;
const JSON_CONTENT_TYPE: &str = "application/json";
const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";
const REQUEST_ID_HEADER: &str = "x-request-id";

struct AgentStreamingBody {
    frames: mpsc::Receiver<Bytes>,
}

impl Body for AgentStreamingBody {
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

struct AbortTaskOnDrop(Option<JoinHandle<()>>);

impl AbortTaskOnDrop {
    fn new(task: JoinHandle<()>) -> Self {
        Self(Some(task))
    }

    fn into_handle(mut self) -> JoinHandle<()> {
        self.0
            .take()
            .expect("H2 connection task must exist until channel construction")
    }
}

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionClientError {
    retryable: bool,
    message: String,
}

impl AgentSessionClientError {
    fn new(retryable: bool, message: impl Into<String>) -> Self {
        Self {
            retryable,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    pub(crate) fn protocol(error: impl fmt::Display) -> Self {
        Self::new(false, error.to_string())
    }

    pub(crate) fn transport(error: impl fmt::Display) -> Self {
        Self::new(true, error.to_string())
    }
}

impl fmt::Display for AgentSessionClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AgentSessionClientError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionFence {
    pub session_id: SessionId,
    pub session_generation: SessionGeneration,
}

/// Produces route-bound Ed25519 requests from one immutable Agent boot identity.
pub struct AgentRequestSigner {
    agent_id: AgentId,
    installation_id: AgentInstallationId,
    boot_id: AgentBootId,
    signing_key: AgentSigningKey,
    public_key_spki: Ed25519PublicKeySpki,
}

impl fmt::Debug for AgentRequestSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRequestSigner")
            .field("agent_id", &self.agent_id)
            .field("installation_id", &self.installation_id)
            .field("boot_id", &self.boot_id)
            .field("signing_key", &"[REDACTED]")
            .finish()
    }
}

impl AgentRequestSigner {
    pub fn new(
        agent_id: AgentId,
        installation_id: AgentInstallationId,
        boot_id: AgentBootId,
        signing_key: AgentSigningKey,
    ) -> AgentDaemonResult<Self> {
        let public_key_spki = Ed25519PublicKeySpki::new(public_key_spki_der(&signing_key)?)
            .map_err(|error| AgentDaemonError::EnrollmentProtocol(error.to_string()))?;
        Ok(Self {
            agent_id,
            installation_id,
            boot_id,
            signing_key,
            public_key_spki,
        })
    }

    pub fn sign<T: Serialize>(
        &self,
        canonical_path: &'static str,
        fence: Option<&AgentSessionFence>,
        signed_at_unix_ms: UnixMillis,
        payload: T,
    ) -> Result<AgentAuthenticatedRequest<T>, AgentSessionClientError> {
        let mut request = AgentAuthenticatedRequest {
            protocol_version: ProtocolVersion::V1,
            request_id: RequestId::new(format!("agent-request-{}", Uuid::new_v4().simple()))
                .map_err(protocol_error)?,
            agent_id: self.agent_id.clone(),
            installation_id: self.installation_id.clone(),
            boot_id: self.boot_id.clone(),
            session_id: fence.map(|value| value.session_id.clone()),
            session_generation: fence.map(|value| value.session_generation),
            signed_at_unix_ms,
            payload,
            proof: AgentRequestProof {
                unsigned_body_digest: neoengram_protocol::ContentDigest::from_bytes([0; 32]),
                possession: AgentBootstrapProof::new(
                    self.public_key_spki.clone(),
                    Ed25519Signature::from_bytes([0; 64]),
                ),
            },
            extensions: Extensions::new(),
        };
        request.proof.unsigned_body_digest = request
            .computed_unsigned_body_digest()
            .map_err(protocol_error)?;
        let signing_bytes = request
            .signing_bytes(canonical_path)
            .map_err(protocol_error)?;
        request.proof.possession.signature =
            Ed25519Signature::new(self.signing_key.sign(&signing_bytes).as_ref().to_vec())
                .map_err(protocol_error)?;
        if fence.is_some() {
            request.validate_session().map_err(protocol_error)?;
        } else {
            request.validate_open().map_err(protocol_error)?;
        }
        Ok(request)
    }

    pub fn sign_channel(
        &self,
        fence: Option<&AgentSessionFence>,
        signed_at_unix_ms: UnixMillis,
        payload: AgentChannelUpstreamPayload,
    ) -> Result<AgentChannelUpstreamFrame, AgentSessionClientError> {
        let request = self.sign(
            AGENT_SESSION_CHANNEL_OPEN_PATH,
            fence,
            signed_at_unix_ms,
            payload,
        )?;
        let frame = AgentChannelUpstreamFrame { request };
        frame.verify().map_err(AgentSessionClientError::protocol)?;
        Ok(frame)
    }
}

#[async_trait]
pub trait AgentSessionClient: Send + Sync {
    async fn connect_channel(
        &self,
        open: &AgentChannelUpstreamFrame,
    ) -> Result<AgentChannelConnection, AgentSessionClientError>;

    async fn open(
        &self,
        request: &AgentAuthenticatedRequest<AgentSessionOpenPayload>,
    ) -> Result<AgentSessionOpenResponse, AgentSessionClientError>;
    async fn heartbeat(
        &self,
        request: &AgentAuthenticatedRequest<AgentHeartbeatReportPayload>,
    ) -> Result<AgentHeartbeatReportResponse, AgentSessionClientError>;
    async fn poll(
        &self,
        request: &AgentAuthenticatedRequest<AgentMessageListQueryPayload>,
    ) -> Result<AgentMessageListQueryResponse, AgentSessionClientError>;
    async fn create_report(
        &self,
        request: &AgentAuthenticatedRequest<AgentJobReportCreatePayload>,
    ) -> Result<AgentActionAcceptedResponse, AgentSessionClientError>;
    async fn stage_metadata_batch(
        &self,
        request: &AgentAuthenticatedRequest<AgentMetadataBatchStagePayload>,
    ) -> Result<AgentMetadataStageResponse, AgentSessionClientError>;
    async fn stage_metadata_page(
        &self,
        request: &AgentAuthenticatedRequest<AgentMetadataPageStagePayload>,
    ) -> Result<AgentMetadataStageResponse, AgentSessionClientError>;
    async fn query_index_page(
        &self,
        request: &AgentAuthenticatedRequest<AgentIndexPageQueryPayload>,
    ) -> Result<AgentIndexPageQueryResponse, AgentSessionClientError>;
    async fn query_manifest_page(
        &self,
        request: &AgentAuthenticatedRequest<AgentManifestPageQueryPayload>,
    ) -> Result<AgentManifestPageQueryResponse, AgentSessionClientError>;
    async fn close(
        &self,
        request: &AgentAuthenticatedRequest<AgentSessionClosePayload>,
    ) -> Result<AgentSessionCloseResponse, AgentSessionClientError>;
}

#[derive(Debug, Clone)]
pub struct ReqwestAgentSessionClient {
    client: reqwest::Client,
    endpoint: Url,
}

impl ReqwestAgentSessionClient {
    pub fn new(endpoint: Url) -> AgentDaemonResult<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(REQUEST_TIMEOUT)
            .http2_prior_knowledge()
            .user_agent(concat!("neoengram-agent/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| AgentDaemonError::Enrollment(error.to_string()))?;
        Ok(Self { client, endpoint })
    }

    async fn post<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &'static str,
        request: &AgentAuthenticatedRequest<T>,
    ) -> Result<R, AgentSessionClientError> {
        tokio::time::timeout(REQUEST_TIMEOUT, self.post_inner(path, request))
            .await
            .map_err(|_| AgentSessionClientError::transport("Agent API request timed out"))?
    }

    async fn post_inner<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &'static str,
        request: &AgentAuthenticatedRequest<T>,
    ) -> Result<R, AgentSessionClientError> {
        let url = self
            .endpoint
            .join(path.trim_start_matches('/'))
            .map_err(|error| AgentSessionClientError::new(false, error.to_string()))?;
        let body = serde_json::to_vec(request)
            .map_err(|error| AgentSessionClientError::new(false, error.to_string()))?;
        let response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .header(
                ACCEPT,
                format!("{JSON_CONTENT_TYPE}, {PROBLEM_CONTENT_TYPE}"),
            )
            .header(REQUEST_ID_HEADER, request.request_id.as_str())
            .body(body)
            .send()
            .await
            .map_err(|error| {
                AgentSessionClientError::new(true, format!("Agent API is unavailable: {error}"))
            })?;
        validate_response_request_id(&response, &request.request_id)?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        if response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > MAX_RESPONSE_BYTES)
        {
            return Err(AgentSessionClientError::new(
                false,
                "Agent API response exceeds the transport limit",
            ));
        }
        let body = read_bounded(response).await?;
        if !status.is_success() {
            return Err(remote_error(status, &body));
        }
        if content_type
            .split(';')
            .next()
            .map(str::trim)
            .is_none_or(|value| value != JSON_CONTENT_TYPE)
        {
            return Err(AgentSessionClientError::new(
                false,
                "Agent API success response is not application/json",
            ));
        }
        decode_bounded_unique_json(&body, MAX_RESPONSE_BYTES).map_err(protocol_error)
    }

    async fn connect_streaming_channel(
        &self,
        open: &AgentChannelUpstreamFrame,
    ) -> Result<AgentChannelConnection, AgentSessionClientError> {
        if !matches!(
            open.request.payload.message,
            neoengram_protocol::AgentChannelUpstreamMessage::Open(_)
        ) {
            return Err(AgentSessionClientError::protocol(
                "Agent control channel must start with channel.open",
            ));
        }
        let request_id = open.request.request_id.clone();
        let first_frame = open
            .encode_ndjson()
            .map(Bytes::from)
            .map_err(AgentSessionClientError::protocol)?;
        let url = self
            .endpoint
            .join(AGENT_SESSION_CHANNEL_OPEN_PATH.trim_start_matches('/'))
            .map_err(AgentSessionClientError::protocol)?;
        if url.scheme() != "http" {
            return Err(AgentSessionClientError::protocol(
                "Agent control channel currently requires cleartext HTTP/2 prior knowledge",
            ));
        }
        let host = url.host_str().ok_or_else(|| {
            AgentSessionClientError::protocol("Agent control channel endpoint has no host")
        })?;
        let port = url.port_or_known_default().ok_or_else(|| {
            AgentSessionClientError::protocol("Agent control channel endpoint has no port")
        })?;
        let (outgoing, outgoing_rx) = channel_buffers();
        outgoing.send(first_frame).await.map_err(|_| {
            AgentSessionClientError::transport(
                "Agent control channel request stream closed before Open",
            )
        })?;

        let stream = tokio::time::timeout(REQUEST_TIMEOUT, TcpStream::connect((host, port)))
            .await
            .map_err(|_| {
                AgentSessionClientError::transport("Agent control channel connection timed out")
            })?
            .map_err(|error| {
                AgentSessionClientError::transport(format!(
                    "Agent control channel is unavailable: {error}"
                ))
            })?;
        let (mut sender, connection) = tokio::time::timeout(
            REQUEST_TIMEOUT,
            http2::handshake::<_, _, AgentStreamingBody>(
                TokioExecutor::new(),
                TokioIo::new(stream),
            ),
        )
        .await
        .map_err(|_| AgentSessionClientError::transport("Agent HTTP/2 handshake timed out"))?
        .map_err(|error| {
            AgentSessionClientError::transport(format!("Agent HTTP/2 handshake failed: {error}"))
        })?;
        let connection_task = AbortTaskOnDrop::new(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let request = Request::builder()
            .method(Method::POST)
            .uri(url.as_str())
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .header(
                ACCEPT,
                format!("{NDJSON_CONTENT_TYPE}, {PROBLEM_CONTENT_TYPE}"),
            )
            .header(REQUEST_ID_HEADER, request_id.as_str())
            .header(
                USER_AGENT,
                concat!("neoengram-agent/", env!("CARGO_PKG_VERSION")),
            )
            .body(AgentStreamingBody {
                frames: outgoing_rx,
            })
            .map_err(AgentSessionClientError::protocol)?;
        let response = tokio::time::timeout(REQUEST_TIMEOUT, sender.send_request(request))
            .await
            .map_err(|_| {
                AgentSessionClientError::transport("Agent control channel response timed out")
            })?
            .map_err(|error| {
                AgentSessionClientError::transport(format!(
                    "Agent control channel is unavailable: {error}"
                ))
            })?;
        let sender_keepalive = tokio::spawn(async move {
            let _sender = sender;
            std::future::pending::<()>().await;
        });
        validate_response_request_id_headers(response.headers(), &request_id)?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        if !status.is_success() {
            if response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|length| length > MAX_RESPONSE_BYTES)
            {
                return Err(AgentSessionClientError::protocol(
                    "Agent API response exceeds the transport limit",
                ));
            }
            let body = read_h2_bounded(response.into_body()).await?;
            return Err(remote_error(status, &body));
        }
        if response.version() != Version::HTTP_2 {
            return Err(AgentSessionClientError::protocol(
                "Agent control channel response is not HTTP/2",
            ));
        }
        if !has_content_type(&content_type, NDJSON_CONTENT_TYPE) {
            return Err(AgentSessionClientError::protocol(
                "Agent control channel response is not application/x-ndjson",
            ));
        }

        let mut response_body = response.into_body();
        let (chunks_tx, chunks_rx) = mpsc::channel(64);
        let response_reader = tokio::spawn(async move {
            while let Some(frame) = response_body.frame().await {
                let chunk = match frame {
                    Ok(frame) => match frame.into_data() {
                        Ok(bytes) => Ok(bytes),
                        Err(_) => continue,
                    },
                    Err(error) => Err(AgentSessionClientError::transport(format!(
                        "Agent control channel response failed: {error}"
                    ))),
                };
                let failed = chunk.is_err();
                if chunks_tx.send(chunk).await.is_err() || failed {
                    break;
                }
            }
        });
        let (incoming, reader) = spawn_response_reader(chunks_rx);
        Ok(AgentChannelConnection::new(
            outgoing,
            incoming,
            vec![
                sender_keepalive,
                connection_task.into_handle(),
                response_reader,
                reader,
            ],
        ))
    }
}

#[async_trait]
impl AgentSessionClient for ReqwestAgentSessionClient {
    async fn connect_channel(
        &self,
        open: &AgentChannelUpstreamFrame,
    ) -> Result<AgentChannelConnection, AgentSessionClientError> {
        self.connect_streaming_channel(open).await
    }

    async fn open(
        &self,
        request: &AgentAuthenticatedRequest<AgentSessionOpenPayload>,
    ) -> Result<AgentSessionOpenResponse, AgentSessionClientError> {
        self.post(AGENT_SESSION_OPEN_PATH, request).await
    }

    async fn heartbeat(
        &self,
        request: &AgentAuthenticatedRequest<AgentHeartbeatReportPayload>,
    ) -> Result<AgentHeartbeatReportResponse, AgentSessionClientError> {
        self.post(AGENT_SESSION_HEARTBEAT_REPORT_PATH, request)
            .await
    }

    async fn poll(
        &self,
        request: &AgentAuthenticatedRequest<AgentMessageListQueryPayload>,
    ) -> Result<AgentMessageListQueryResponse, AgentSessionClientError> {
        self.post(AGENT_SESSION_MESSAGE_LIST_QUERY_PATH, request)
            .await
    }

    async fn create_report(
        &self,
        request: &AgentAuthenticatedRequest<AgentJobReportCreatePayload>,
    ) -> Result<AgentActionAcceptedResponse, AgentSessionClientError> {
        self.post(AGENT_JOB_REPORT_CREATE_PATH, request).await
    }

    async fn stage_metadata_batch(
        &self,
        request: &AgentAuthenticatedRequest<AgentMetadataBatchStagePayload>,
    ) -> Result<AgentMetadataStageResponse, AgentSessionClientError> {
        self.post(AGENT_JOB_METADATA_BATCH_STAGE_PATH, request)
            .await
    }

    async fn stage_metadata_page(
        &self,
        request: &AgentAuthenticatedRequest<AgentMetadataPageStagePayload>,
    ) -> Result<AgentMetadataStageResponse, AgentSessionClientError> {
        self.post(AGENT_JOB_METADATA_PAGE_STAGE_PATH, request).await
    }

    async fn query_index_page(
        &self,
        request: &AgentAuthenticatedRequest<AgentIndexPageQueryPayload>,
    ) -> Result<AgentIndexPageQueryResponse, AgentSessionClientError> {
        self.post(AGENT_JOB_INDEX_PAGE_QUERY_PATH, request).await
    }

    async fn query_manifest_page(
        &self,
        request: &AgentAuthenticatedRequest<AgentManifestPageQueryPayload>,
    ) -> Result<AgentManifestPageQueryResponse, AgentSessionClientError> {
        self.post(AGENT_JOB_MANIFEST_PAGE_QUERY_PATH, request).await
    }

    async fn close(
        &self,
        request: &AgentAuthenticatedRequest<AgentSessionClosePayload>,
    ) -> Result<AgentSessionCloseResponse, AgentSessionClientError> {
        self.post(AGENT_SESSION_CLOSE_PATH, request).await
    }
}

async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>, AgentSessionClientError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        AgentSessionClientError::new(true, format!("Agent API response failed: {error}"))
    })? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(AgentSessionClientError::new(
                false,
                "Agent API response exceeds the transport limit",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn read_h2_bounded(mut body: Incoming) -> Result<Vec<u8>, AgentSessionClientError> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            AgentSessionClientError::transport(format!("Agent API response failed: {error}"))
        })?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if bytes.len().saturating_add(data.len()) > MAX_RESPONSE_BYTES {
            return Err(AgentSessionClientError::protocol(
                "Agent API response exceeds the transport limit",
            ));
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes)
}

fn validate_response_request_id(
    response: &reqwest::Response,
    expected: &RequestId,
) -> Result<(), AgentSessionClientError> {
    validate_response_request_id_headers(response.headers(), expected)
}

fn validate_response_request_id_headers(
    headers: &HeaderMap,
    expected: &RequestId,
) -> Result<(), AgentSessionClientError> {
    let observed = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| AgentSessionClientError::new(false, "Agent API omitted X-Request-ID"))?;
    let observed = RequestId::new(observed).map_err(|_| {
        AgentSessionClientError::new(false, "Agent API returned invalid X-Request-ID")
    })?;
    if &observed != expected {
        return Err(AgentSessionClientError::new(
            false,
            "Agent API returned a different X-Request-ID",
        ));
    }
    Ok(())
}

fn remote_error(status: StatusCode, body: &[u8]) -> AgentSessionClientError {
    let problem: Option<Value> = decode_bounded_unique_json(body, MAX_RESPONSE_BYTES).ok();
    let code = problem
        .as_ref()
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN");
    let retryable = problem
        .as_ref()
        .and_then(|value| value.get("retryable"))
        .and_then(Value::as_bool)
        .unwrap_or(status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error());
    AgentSessionClientError::new(
        retryable,
        format!("Agent API returned HTTP {} ({code})", status.as_u16()),
    )
}

fn protocol_error(error: impl std::fmt::Display) -> AgentSessionClientError {
    AgentSessionClientError::new(false, error.to_string())
}

fn has_content_type(observed: &str, expected: &str) -> bool {
    observed
        .split(';')
        .next()
        .map(str::trim)
        .is_some_and(|value| value.eq_ignore_ascii_case(expected))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hyper::{
        body::Incoming, server::conn::http2 as server_http2, service::service_fn, Response,
    };
    use neoengram_protocol::{
        AgentChannelDownstreamFrame, AgentChannelDownstreamMessage, AgentChannelUpstreamMessage,
        AgentMountIdentityDigest, ContentDigest, ControlError, ErrorCode, MessageId,
        ResourceVersion, SequenceNumber,
    };
    use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
    use tokio::net::TcpListener;

    use super::*;

    fn signed_open() -> AgentChannelUpstreamFrame {
        let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer = AgentRequestSigner::new(
            AgentId::new("agent-h2-test").unwrap(),
            AgentInstallationId::new("installation-h2-test").unwrap(),
            AgentBootId::new("boot-h2-test").unwrap(),
            Arc::new(Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap()),
        )
        .unwrap();
        signer
            .sign_channel(
                None,
                UnixMillis::new(10),
                AgentChannelUpstreamPayload {
                    sequence: SequenceNumber::new(1),
                    message_id: MessageId::new("channel-open-h2-test").unwrap(),
                    correlation_id: None,
                    message: AgentChannelUpstreamMessage::Open(AgentSessionOpenPayload {
                        mount_identity_digest: AgentMountIdentityDigest::new(
                            ContentDigest::from_bytes([7; 32]),
                        ),
                        expected_resource_version: ResourceVersion::new(1),
                        extensions: Extensions::new(),
                    }),
                    extensions: Extensions::new(),
                },
            )
            .unwrap()
    }

    fn downstream_error() -> AgentChannelDownstreamFrame {
        AgentChannelDownstreamFrame {
            protocol_version: ProtocolVersion::V1,
            sequence: SequenceNumber::new(1),
            message_id: MessageId::new("downstream-h2-test").unwrap(),
            correlation_id: None,
            session_generation: SessionGeneration::new(1),
            sent_at_unix_ms: UnixMillis::new(11),
            message: AgentChannelDownstreamMessage::Error(ControlError {
                code: ErrorCode::new("TEST_FRAME").unwrap(),
                message: "stream remains bidirectional".to_owned(),
                retryable: false,
                retry_after_ms: None,
                extensions: Extensions::new(),
            }),
            extensions: Extensions::new(),
        }
    }

    #[tokio::test]
    async fn h2_channel_returns_before_request_eof_and_remains_bidirectional() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (later_request_tx, mut later_request_rx) = mpsc::channel::<Vec<u8>>(1);
        let (request_finished_tx, mut request_finished_rx) = mpsc::channel::<()>(1);
        let response_frame = downstream_error();
        let encoded_response = Bytes::from(response_frame.encode_ndjson().unwrap());

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(move |request: Request<Incoming>| {
                let later_request_tx = later_request_tx.clone();
                let request_finished_tx = request_finished_tx.clone();
                let encoded_response = encoded_response.clone();
                async move {
                    assert_eq!(request.method(), Method::POST);
                    assert_eq!(request.uri().path(), AGENT_SESSION_CHANNEL_OPEN_PATH);
                    assert_eq!(request.version(), Version::HTTP_2);
                    let request_id = request.headers()[REQUEST_ID_HEADER].clone();
                    let mut request_body = request.into_body();
                    let first = request_body
                        .frame()
                        .await
                        .expect("client must send channel.open")
                        .unwrap()
                        .into_data()
                        .expect("channel.open must be a DATA frame");
                    let first = first
                        .strip_suffix(b"\n")
                        .expect("channel.open must be LF terminated");
                    AgentChannelUpstreamFrame::decode_json(first).unwrap();

                    let (response_tx, response_rx) = mpsc::channel(1);
                    tokio::spawn(async move {
                        if let Some(Ok(frame)) = request_body.frame().await {
                            let bytes = frame
                                .into_data()
                                .expect("later request must be a DATA frame");
                            later_request_tx.send(bytes.to_vec()).await.unwrap();
                            response_tx.send(encoded_response).await.unwrap();
                        }
                        loop {
                            match request_body.frame().await {
                                Some(Ok(_)) => {}
                                Some(Err(_)) | None => break,
                            }
                        }
                        let _ = request_finished_tx.send(()).await;
                    });

                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .version(Version::HTTP_2)
                            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
                            .header(REQUEST_ID_HEADER, request_id)
                            .body(AgentStreamingBody {
                                frames: response_rx,
                            })
                            .unwrap(),
                    )
                }
            });
            let _ = server_http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });

        let client =
            ReqwestAgentSessionClient::new(Url::parse(&format!("http://{address}/")).unwrap())
                .unwrap();
        let open = signed_open();
        let mut channel = tokio::time::timeout(
            Duration::from_secs(2),
            client.connect_streaming_channel(&open),
        )
        .await
        .expect("response headers must arrive before request EOF")
        .unwrap();

        channel.writer.send(&open).await.unwrap();
        let later_request = tokio::time::timeout(Duration::from_secs(2), later_request_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let later_request = later_request
            .strip_suffix(b"\n")
            .expect("later request must be LF terminated");
        assert_eq!(
            AgentChannelUpstreamFrame::decode_json(later_request).unwrap(),
            open
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), channel.receive())
                .await
                .unwrap()
                .unwrap(),
            Some(response_frame)
        );

        drop(channel);
        tokio::time::timeout(Duration::from_secs(2), request_finished_rx.recv())
            .await
            .expect("dropping the channel must close the H2 request stream")
            .expect("server must observe the request stream closing");
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server connection task must stop after channel drop")
            .unwrap();
    }
}
