use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::{Bytes, BytesMut};
use http::{
    header::{ALLOW, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER},
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Version,
};
use http_body_util::{BodyExt, Either, Full};
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto,
};
use serde::Serialize;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};

mod control_channel;
mod registry_handler;

pub use registry_handler::{
    AgentDataPlaneHandler, RegistryAgentApiHandler, RegistryAgentEnrollmentHandler,
};

pub const AGENT_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
pub const AGENT_MAX_METADATA_PAGE_BODY_BYTES: usize =
    neoengram_protocol::MAX_METADATA_PAGE_BYTES + AGENT_MAX_REQUEST_BODY_BYTES;
pub const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const AGENT_MAX_CONCURRENT_REQUESTS: usize = 64;
const AGENT_OVERLOAD_RESPONSE_TIMEOUT: Duration = Duration::from_millis(100);
const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const MAX_REQUEST_ID_BYTES: usize = 128;
const JSON_CONTENT_TYPE: &str = "application/json";
const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";

#[derive(Clone, Copy)]
struct AgentResourceLimits {
    max_connections: usize,
    max_channels: usize,
    max_requests: usize,
    request_timeout: Duration,
    overload_response_timeout: Duration,
}

impl AgentResourceLimits {
    const PRODUCTION: Self = Self {
        max_connections: AGENT_MAX_CONCURRENT_REQUESTS,
        max_channels: AGENT_MAX_CONCURRENT_REQUESTS,
        max_requests: AGENT_MAX_CONCURRENT_REQUESTS,
        request_timeout: AGENT_REQUEST_TIMEOUT,
        overload_response_timeout: AGENT_OVERLOAD_RESPONSE_TIMEOUT,
    };
}

/// Fixed operations exposed by the independent action-style Agent API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentAction {
    EnrollmentBootstrap,
    EnrollmentStatusQuery,
    SessionOpen,
    SessionHeartbeatReport,
    SessionMessageListQuery,
    JobReportCreate,
    JobMetadataBatchStage,
    JobMetadataPageStage,
    JobIndexPageQuery,
    JobManifestPageQuery,
    SessionClose,
}

impl AgentAction {
    fn from_path(path: &str) -> Option<Self> {
        match path {
            neoengram_protocol::AGENT_ENROLLMENT_BOOTSTRAP_PATH => Some(Self::EnrollmentBootstrap),
            neoengram_protocol::AGENT_ENROLLMENT_STATUS_QUERY_PATH => {
                Some(Self::EnrollmentStatusQuery)
            }
            neoengram_protocol::AGENT_SESSION_OPEN_PATH => Some(Self::SessionOpen),
            neoengram_protocol::AGENT_SESSION_HEARTBEAT_REPORT_PATH => {
                Some(Self::SessionHeartbeatReport)
            }
            neoengram_protocol::AGENT_SESSION_MESSAGE_LIST_QUERY_PATH => {
                Some(Self::SessionMessageListQuery)
            }
            neoengram_protocol::AGENT_JOB_REPORT_CREATE_PATH => Some(Self::JobReportCreate),
            neoengram_protocol::AGENT_JOB_METADATA_BATCH_STAGE_PATH => {
                Some(Self::JobMetadataBatchStage)
            }
            neoengram_protocol::AGENT_JOB_METADATA_PAGE_STAGE_PATH => {
                Some(Self::JobMetadataPageStage)
            }
            neoengram_protocol::AGENT_JOB_INDEX_PAGE_QUERY_PATH => Some(Self::JobIndexPageQuery),
            neoengram_protocol::AGENT_JOB_MANIFEST_PAGE_QUERY_PATH => {
                Some(Self::JobManifestPageQuery)
            }
            neoengram_protocol::AGENT_SESSION_CLOSE_PATH => Some(Self::SessionClose),
            _ => None,
        }
    }

    const fn max_body_bytes(self) -> usize {
        match self {
            Self::JobMetadataPageStage => AGENT_MAX_METADATA_PAGE_BODY_BYTES,
            _ => AGENT_MAX_REQUEST_BODY_BYTES,
        }
    }
}

/// Raw-body application boundary used by the Hyper adapter.
#[async_trait]
pub trait AgentApiHandler: Send + Sync + 'static {
    async fn handle(&self, action: AgentAction, body: &[u8]) -> Result<Vec<u8>, AgentHttpError>;

    /// Opens one H2 reverse control channel after consuming and authenticating its first frame.
    async fn open_control_channel(
        &self,
        _body: Incoming,
    ) -> Result<AgentControlChannel, AgentHttpError> {
        Err(AgentHttpError::unavailable())
    }
}

/// Streaming response frames produced by an authenticated Agent control channel.
pub struct AgentControlChannel {
    frames: mpsc::Receiver<Bytes>,
}

impl AgentControlChannel {
    /// Creates a channel response backed by a bounded frame receiver.
    #[must_use]
    pub fn new(frames: mpsc::Receiver<Bytes>) -> Self {
        Self { frames }
    }
}

impl fmt::Debug for AgentControlChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentControlChannel")
            .finish_non_exhaustive()
    }
}

struct AgentStreamingBody {
    frames: mpsc::Receiver<Bytes>,
    _stream_permit: Option<OwnedSemaphorePermit>,
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

type AgentResponseBody = Either<Full<Bytes>, AgentStreamingBody>;

enum AgentHandledResponse {
    Json(Vec<u8>),
    Channel(AgentControlChannel),
}

pub(crate) struct AgentNdjsonInput<B> {
    body: B,
    decoder: neoengram_protocol::AgentChannelNdjsonDecoder,
    ready: VecDeque<Vec<u8>>,
    finished: bool,
}

impl<B> AgentNdjsonInput<B>
where
    B: Body<Data = Bytes> + Unpin,
{
    pub(crate) fn new(body: B) -> Self {
        Self {
            body,
            decoder: neoengram_protocol::AgentChannelNdjsonDecoder::new(),
            ready: VecDeque::new(),
            finished: false,
        }
    }

    pub(crate) async fn next_line(&mut self) -> Result<Option<Vec<u8>>, AgentHttpError> {
        if let Some(line) = self.ready.pop_front() {
            return Ok(Some(line));
        }
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(frame) = self.body.frame().await else {
                self.finished = true;
                self.decoder
                    .finish()
                    .map_err(|_| AgentHttpError::protocol_invalid())?;
                return Ok(None);
            };
            let frame = frame.map_err(|_| AgentHttpError::protocol_invalid())?;
            let data = frame
                .into_data()
                .map_err(|_| AgentHttpError::protocol_invalid())?;
            self.ready.extend(
                self.decoder
                    .push(&data)
                    .map_err(|_| AgentHttpError::protocol_invalid())?,
            );
            if let Some(line) = self.ready.pop_front() {
                return Ok(Some(line));
            }
        }
    }
}

/// Compatibility export for callers migrating from the enrollment-only listener.
pub use AgentApiHandler as AgentEnrollmentHandler;

/// Sanitized Agent transport failure.
#[derive(Debug, Clone)]
pub struct AgentHttpError {
    status: StatusCode,
    code: &'static str,
    detail: &'static str,
    retryable: bool,
    retry_after_ms: Option<u64>,
}

impl AgentHttpError {
    #[must_use]
    pub const fn new(
        status: StatusCode,
        code: &'static str,
        detail: &'static str,
        retryable: bool,
    ) -> Self {
        Self {
            status,
            code,
            detail,
            retryable,
            retry_after_ms: None,
        }
    }

    #[must_use]
    pub const fn with_retry_after_ms(mut self, retry_after_ms: u64) -> Self {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }

    #[must_use]
    pub const fn bootstrap_denied() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "AGENT_BOOTSTRAP_DENIED",
            "Agent bootstrap credentials or scope were denied",
            false,
        )
    }

    #[must_use]
    pub const fn protocol_invalid() -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "PROTOCOL_INVALID",
            "Agent request does not satisfy the protocol",
            false,
        )
    }

    #[must_use]
    pub const fn session_fenced() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "AGENT_SESSION_FENCED",
            "Agent request belongs to a stale or closed session",
            false,
        )
    }

    #[must_use]
    pub const fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "SERVICE_UNAVAILABLE",
            "Agent enrollment is temporarily unavailable",
            true,
        )
        .with_retry_after_ms(1_000)
    }
}

/// Immutable resource controls for the Agent listener.
#[derive(Debug, Clone, Copy)]
pub struct AgentListenerConfig {
    pub bind: SocketAddr,
    pub graceful_shutdown_timeout: Duration,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum AgentServerError {
    #[error("failed to bind Agent listener: {0}")]
    Bind(String),
    #[error("Agent listener failed: {0}")]
    Runtime(String),
}

#[derive(Clone)]
pub struct AgentServerHandle {
    shutdown: watch::Sender<bool>,
    completion: watch::Receiver<Option<Result<(), AgentServerError>>>,
}

impl AgentServerHandle {
    pub async fn shutdown(&self) -> Result<(), AgentServerError> {
        let _ = self.shutdown.send(true);
        self.wait().await
    }

    pub async fn wait(&self) -> Result<(), AgentServerError> {
        let mut completion = self.completion.clone();
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            completion.changed().await.map_err(|_| {
                AgentServerError::Runtime(
                    "completion channel closed without a terminal result".into(),
                )
            })?;
        }
    }
}

pub struct RunningAgentServer {
    local_addr: SocketAddr,
    handle: AgentServerHandle,
}

impl RunningAgentServer {
    pub async fn start(
        config: AgentListenerConfig,
        handler: Arc<dyn AgentApiHandler>,
    ) -> Result<Self, AgentServerError> {
        let listener = TcpListener::bind(config.bind)
            .await
            .map_err(|error| AgentServerError::Bind(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| AgentServerError::Bind(error.to_string()))?;
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (completion_sender, completion) = watch::channel(None);
        tokio::spawn(run_listener(
            listener,
            handler,
            shutdown_receiver,
            completion_sender,
            config.graceful_shutdown_timeout,
            AgentResourceLimits::PRODUCTION,
        ));
        Ok(Self {
            local_addr,
            handle: AgentServerHandle {
                shutdown,
                completion,
            },
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    pub fn handle(&self) -> AgentServerHandle {
        self.handle.clone()
    }

    pub async fn shutdown(self) -> Result<(), AgentServerError> {
        self.handle.shutdown().await
    }
}

impl Drop for RunningAgentServer {
    fn drop(&mut self) {
        let _ = self.handle.shutdown.send(true);
    }
}

async fn run_listener(
    listener: TcpListener,
    handler: Arc<dyn AgentApiHandler>,
    mut shutdown: watch::Receiver<bool>,
    completion: watch::Sender<Option<Result<(), AgentServerError>>>,
    graceful_shutdown_timeout: Duration,
    limits: AgentResourceLimits,
) {
    let connection_semaphore = Arc::new(Semaphore::new(limits.max_connections));
    let channel_semaphore = Arc::new(Semaphore::new(limits.max_channels));
    let request_semaphore = Arc::new(Semaphore::new(limits.max_requests));
    let mut connections = JoinSet::new();
    let terminal = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(AgentServerError::Runtime(error.to_string())),
                };
                let Ok(permit) = connection_semaphore.clone().try_acquire_owned() else {
                    tracing::debug!(
                        limit = limits.max_connections,
                        "Agent HTTP connection rejected because the connection budget is exhausted"
                    );
                    let _ = tokio::time::timeout(
                        limits.overload_response_timeout,
                        reject_overloaded_connection(stream, limits.overload_response_timeout),
                    )
                    .await;
                    continue;
                };
                let handler = handler.clone();
                let connection_shutdown = shutdown.clone();
                let channel_semaphore = channel_semaphore.clone();
                let request_semaphore = request_semaphore.clone();
                connections.spawn(async move {
                    serve_connection(
                        stream,
                        handler,
                        permit,
                        connection_shutdown,
                        channel_semaphore,
                        request_semaphore,
                        limits.request_timeout,
                    )
                    .await;
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    tracing::warn!(%error, "Agent HTTP connection task failed");
                }
            }
        }
    };

    let drain = async {
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                tracing::warn!(%error, "Agent HTTP connection task failed during drain");
            }
        }
    };
    if tokio::time::timeout(graceful_shutdown_timeout, drain)
        .await
        .is_err()
    {
        connections.abort_all();
    }
    let _ = completion.send(Some(terminal));
}

async fn serve_connection(
    stream: tokio::net::TcpStream,
    handler: Arc<dyn AgentApiHandler>,
    _permit: OwnedSemaphorePermit,
    mut shutdown: watch::Receiver<bool>,
    channel_semaphore: Arc<Semaphore>,
    request_semaphore: Arc<Semaphore>,
    request_timeout: Duration,
) {
    let serve = async {
        let service = service_fn(move |request| {
            dispatch(
                request,
                handler.clone(),
                channel_semaphore.clone(),
                request_semaphore.clone(),
                request_timeout,
            )
        });
        let mut builder = auto::Builder::new(TokioExecutor::new());
        builder
            .http1()
            .keep_alive(false)
            .timer(TokioTimer::new())
            .header_read_timeout(request_timeout);
        builder
            .http2()
            .timer(TokioTimer::new())
            .max_concurrent_streams(AGENT_MAX_CONCURRENT_REQUESTS as u32);
        let connection = builder.serve_connection(TokioIo::new(stream), service);
        tokio::pin!(connection);
        tokio::select! {
            result = &mut connection => {
                if let Err(error) = result {
                    tracing::debug!(%error, "Agent HTTP connection ended with a protocol error");
                }
            }
            _ = shutdown.changed() => {
                connection.as_mut().graceful_shutdown();
                let _ = connection.await;
            }
        }
    };
    serve.await;
}

async fn dispatch(
    request: Request<Incoming>,
    handler: Arc<dyn AgentApiHandler>,
    channel_semaphore: Arc<Semaphore>,
    request_semaphore: Arc<Semaphore>,
    request_timeout: Duration,
) -> Result<Response<AgentResponseBody>, Infallible> {
    let path = request.uri().path().to_owned();
    let request_id = match request_id(request.headers()) {
        Ok(request_id) => request_id,
        Err(error) => {
            let request_id = generated_request_id();
            return Ok(problem_response(error, &path, &request_id));
        }
    };
    let is_channel = path == neoengram_protocol::AGENT_SESSION_CHANNEL_OPEN_PATH;
    let stream_permit = match if is_channel {
        channel_semaphore.try_acquire_owned()
    } else {
        request_semaphore.try_acquire_owned()
    } {
        Ok(permit) => permit,
        Err(_) => {
            return Ok(problem_response(
                AgentHttpError::unavailable().with_retry_after_ms(100),
                &path,
                &request_id,
            ));
        }
    };
    let response =
        match tokio::time::timeout(request_timeout, handle_request(request, handler)).await {
            Ok(Ok(AgentHandledResponse::Json(body))) => {
                json_response(StatusCode::OK, body, &request_id)
            }
            Ok(Ok(AgentHandledResponse::Channel(channel))) => {
                return Ok(channel_response(channel, &request_id, stream_permit));
            }
            Ok(Err(error)) => problem_response(error, &path, &request_id),
            Err(_) => problem_response(
                AgentHttpError::new(
                    StatusCode::GATEWAY_TIMEOUT,
                    "REQUEST_TIMEOUT",
                    "Agent request exceeded the server deadline",
                    true,
                ),
                &path,
                &request_id,
            ),
        };
    Ok(response)
}

async fn reject_overloaded_connection(
    stream: tokio::net::TcpStream,
    header_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = service_fn(move |request: Request<Incoming>| async move {
        let path = request.uri().path().to_owned();
        let (error, request_id) = match request_id(request.headers()) {
            Ok(request_id) => (
                AgentHttpError::unavailable().with_retry_after_ms(100),
                request_id,
            ),
            Err(error) => (error, generated_request_id()),
        };
        Ok::<_, Infallible>(problem_response(error, &path, &request_id))
    });
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .keep_alive(false)
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);
    builder.http2().timer(TokioTimer::new());
    builder
        .serve_connection(TokioIo::new(stream), service)
        .await
}

async fn handle_request(
    request: Request<Incoming>,
    handler: Arc<dyn AgentApiHandler>,
) -> Result<AgentHandledResponse, AgentHttpError> {
    if request.method() != Method::POST {
        return Err(AgentHttpError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
            "Only POST is supported by Agent API routes",
            false,
        ));
    }
    let path = request.uri().path().to_owned();
    if request.uri().query().is_some() {
        return Err(AgentHttpError::new(
            StatusCode::NOT_FOUND,
            "ROUTE_NOT_FOUND",
            "Agent route was not found",
            false,
        ));
    }
    if path == neoengram_protocol::AGENT_SESSION_CHANNEL_OPEN_PATH {
        if request.version() != Version::HTTP_2 {
            return Err(AgentHttpError::new(
                StatusCode::HTTP_VERSION_NOT_SUPPORTED,
                "HTTP_VERSION_REQUIRED",
                "Agent control channels require HTTP/2",
                false,
            ));
        }
        if !has_content_type(request.headers(), NDJSON_CONTENT_TYPE) {
            return Err(AgentHttpError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UNSUPPORTED_MEDIA_TYPE",
                "Content-Type must be application/x-ndjson",
                false,
            ));
        }
        return handler
            .open_control_channel(request.into_body())
            .await
            .map(AgentHandledResponse::Channel);
    }
    if !is_json_content_type(request.headers()) {
        return Err(AgentHttpError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            "Content-Type must be application/json",
            false,
        ));
    }
    let action = AgentAction::from_path(&path).ok_or_else(|| {
        AgentHttpError::new(
            StatusCode::NOT_FOUND,
            "ROUTE_NOT_FOUND",
            "Agent route was not found",
            false,
        )
    })?;
    let max_body_bytes = action.max_body_bytes();
    if request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > max_body_bytes)
    {
        return Err(payload_too_large());
    }
    let body = read_bounded(request.into_body(), max_body_bytes).await?;
    handler
        .handle(action, &body)
        .await
        .map(AgentHandledResponse::Json)
}

async fn read_bounded(
    mut body: Incoming,
    max_body_bytes: usize,
) -> Result<Vec<u8>, AgentHttpError> {
    let mut bytes = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| AgentHttpError::protocol_invalid())?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if bytes.len().saturating_add(data.len()) > max_body_bytes {
            return Err(payload_too_large());
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes.to_vec())
}

fn payload_too_large() -> AgentHttpError {
    AgentHttpError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "PROTOCOL_LIMIT_EXCEEDED",
        "Agent request body exceeds the operation limit",
        false,
    )
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    has_content_type(headers, JSON_CONTENT_TYPE)
}

fn has_content_type(headers: &HeaderMap, expected: &str) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next().and_then(|value| value.to_str().ok()) else {
        return false;
    };
    values.next().is_none()
        && value
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
}

fn request_id(headers: &HeaderMap) -> Result<String, AgentHttpError> {
    let mut values = headers.get_all(&REQUEST_ID_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(generated_request_id());
    };
    if values.next().is_some() {
        return Err(AgentHttpError::protocol_invalid());
    }
    let value = value
        .to_str()
        .map_err(|_| AgentHttpError::protocol_invalid())?;
    if !valid_request_id(value) {
        return Err(AgentHttpError::protocol_invalid());
    }
    Ok(value.to_owned())
}

fn generated_request_id() -> String {
    let mut random = [0_u8; 18];
    if getrandom::fill(&mut random).is_ok() {
        format!("req-{}", URL_SAFE_NO_PAD.encode(random))
    } else {
        format!("req-{}", blake3::hash(b"request-id-fallback").to_hex())
    }
}

fn valid_request_id(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_REQUEST_ID_BYTES || !value.is_ascii() {
        return false;
    }
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

#[derive(Serialize)]
struct ProblemDocument<'a> {
    #[serde(rename = "type")]
    type_uri: String,
    title: &'static str,
    status: u16,
    detail: &'static str,
    instance: &'a str,
    code: &'static str,
    request_id: &'a str,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<String>,
}

fn problem_response(
    error: AgentHttpError,
    path: &str,
    request_id: &str,
) -> Response<AgentResponseBody> {
    let type_name = error.code.to_ascii_lowercase().replace('_', "-");
    let document = ProblemDocument {
        type_uri: format!("urn:neoengram:problem:{type_name}"),
        title: problem_title(error.status),
        status: error.status.as_u16(),
        detail: error.detail,
        instance: path,
        code: error.code,
        request_id,
        retryable: error.retryable,
        retry_after_ms: error.retry_after_ms.map(|value| value.to_string()),
    };
    let bytes = serde_json::to_vec(&document).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = response(error.status, PROBLEM_CONTENT_TYPE, bytes, request_id);
    if let Some(milliseconds) = error.retry_after_ms {
        let seconds = milliseconds.saturating_add(999) / 1_000;
        if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
    }
    response
}

fn json_response(
    status: StatusCode,
    body: Vec<u8>,
    request_id: &str,
) -> Response<AgentResponseBody> {
    response(status, JSON_CONTENT_TYPE, body, request_id)
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
    request_id: &str,
) -> Response<AgentResponseBody> {
    let mut response = Response::new(Either::Left(Full::new(Bytes::from(body))));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static("POST"));
    }
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response.headers_mut().insert(&REQUEST_ID_HEADER, value);
    }
    response
}

fn channel_response(
    channel: AgentControlChannel,
    request_id: &str,
    stream_permit: OwnedSemaphorePermit,
) -> Response<AgentResponseBody> {
    let body = AgentStreamingBody {
        frames: channel.frames,
        _stream_permit: Some(stream_permit),
    };
    let mut response = Response::new(Either::Right(body));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(NDJSON_CONTENT_TYPE));
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response.headers_mut().insert(&REQUEST_ID_HEADER, value);
    }
    response
}

fn problem_title(status: StatusCode) -> &'static str {
    match status {
        StatusCode::FORBIDDEN => "Agent bootstrap denied",
        StatusCode::NOT_FOUND => "Route not found",
        StatusCode::METHOD_NOT_ALLOWED => "Method not allowed",
        StatusCode::PAYLOAD_TOO_LARGE => "Payload too large",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => "Unsupported media type",
        StatusCode::HTTP_VERSION_NOT_SUPPORTED => "HTTP version not supported",
        StatusCode::UNPROCESSABLE_ENTITY => "Protocol invalid",
        StatusCode::CONFLICT => "Enrollment conflict",
        StatusCode::GATEWAY_TIMEOUT => "Request timeout",
        StatusCode::SERVICE_UNAVAILABLE => "Service unavailable",
        _ => "Internal server error",
    }
}

impl fmt::Debug for RunningAgentServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunningAgentServer")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use hyper::client::conn::http2;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::Mutex,
    };

    use super::*;

    #[test]
    fn request_id_policy_matches_public_resource_ids() {
        assert!(valid_request_id("req:agent.bootstrap-1"));
        assert!(!valid_request_id("-invalid"));
        assert!(!valid_request_id("contains space"));
        assert!(!valid_request_id(&"a".repeat(129)));
    }

    #[test]
    fn manifest_metadata_action_is_routable_without_reintroducing_chunk_actions() {
        assert_eq!(
            AgentAction::from_path(neoengram_protocol::AGENT_JOB_MANIFEST_PAGE_QUERY_PATH),
            Some(AgentAction::JobManifestPageQuery)
        );
        assert_eq!(AgentAction::from_path("/agent/job/object/upload"), None);
    }

    #[derive(Default)]
    struct TestHandler {
        calls: AtomicUsize,
        delay: Duration,
        channel_first_line: Mutex<Option<Vec<u8>>>,
        channel_sender: Mutex<Option<mpsc::Sender<Bytes>>>,
    }

    #[async_trait]
    impl AgentApiHandler for TestHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            _body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(b"{}".to_vec())
        }

        async fn open_control_channel(
            &self,
            body: Incoming,
        ) -> Result<AgentControlChannel, AgentHttpError> {
            let mut input = AgentNdjsonInput::new(body);
            let first = input
                .next_line()
                .await?
                .ok_or_else(AgentHttpError::protocol_invalid)?;
            *self.channel_first_line.lock().await = Some(first);
            let (frames, receiver) = mpsc::channel(1);
            frames
                .send(Bytes::from_static(b"{\"type\":\"channel.opened\"}\n"))
                .await
                .map_err(|_| AgentHttpError::unavailable())?;
            *self.channel_sender.lock().await = Some(frames);
            Ok(AgentControlChannel::new(receiver))
        }
    }

    async fn start_test_listener(
        limits: AgentResourceLimits,
        handler: Arc<TestHandler>,
    ) -> (
        SocketAddr,
        watch::Sender<bool>,
        watch::Receiver<Option<Result<(), AgentServerError>>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        let (completion_sender, completion) = watch::channel(None);
        tokio::spawn(run_listener(
            listener,
            handler,
            receiver,
            completion_sender,
            Duration::from_secs(1),
            limits,
        ));
        (address, shutdown, completion)
    }

    async fn post_raw(address: SocketAddr) -> Vec<u8> {
        post_path_raw(address, "/agent/enrollment/bootstrap").await
    }

    async fn post_path_raw(address: SocketAddr, path: &str) -> Vec<u8> {
        post_path_raw_with_request_id_headers(address, path, "x-request-id: agent:test-request\r\n")
            .await
    }

    async fn post_raw_with_request_id_headers(
        address: SocketAddr,
        request_id_headers: &str,
    ) -> Vec<u8> {
        post_path_raw_with_request_id_headers(
            address,
            "/agent/enrollment/bootstrap",
            request_id_headers,
        )
        .await
    }

    async fn post_path_raw_with_request_id_headers(
        address: SocketAddr,
        path: &str,
        request_id_headers: &str,
    ) -> Vec<u8> {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let request = format!(
            "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\n{request_id_headers}content-length: 2\r\nconnection: close\r\n\r\n{{}}"
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap_or_default();
        response
    }

    async fn stop_test_listener(
        shutdown: watch::Sender<bool>,
        mut completion: watch::Receiver<Option<Result<(), AgentServerError>>>,
    ) {
        let _ = shutdown.send(true);
        while completion.borrow().is_none() {
            completion.changed().await.unwrap();
        }
        completion.borrow().clone().unwrap().unwrap();
    }

    #[tokio::test]
    async fn legacy_central_object_payload_actions_are_not_routable() {
        let handler = Arc::new(TestHandler::default());
        let (address, shutdown, completion) =
            start_test_listener(AgentResourceLimits::PRODUCTION, handler.clone()).await;

        for path in [
            "/agent/job/object/missing/query",
            "/agent/job/object/upload",
        ] {
            let response = String::from_utf8(post_path_raw(address, path).await).unwrap();
            assert!(response.starts_with("HTTP/1.1 404 Not Found\r\n"));
            assert!(response.contains("\"code\":\"ROUTE_NOT_FOUND\""));
        }
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);

        stop_test_listener(shutdown, completion).await;
    }

    #[tokio::test]
    async fn slow_headers_hold_one_connection_budget_and_overflow_gets_problem_details() {
        let limits = AgentResourceLimits {
            max_connections: 1,
            max_channels: 1,
            max_requests: 1,
            request_timeout: Duration::from_millis(300),
            overload_response_timeout: Duration::from_millis(100),
        };
        let handler = Arc::new(TestHandler::default());
        let (address, shutdown, completion) = start_test_listener(limits, handler.clone()).await;

        let mut slow = TcpStream::connect(address).await.unwrap();
        slow.write_all(b"POST /agent/enrollment/bootstrap HTTP/1.1\r\nhost: localhost\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let invalid = post_raw_with_request_id_headers(
            address,
            "x-request-id: first\r\nx-request-id: second\r\n",
        )
        .await;
        let invalid = String::from_utf8(invalid).unwrap();
        assert!(invalid.starts_with("HTTP/1.1 422 Unprocessable Entity\r\n"));
        assert!(invalid.contains("\"code\":\"PROTOCOL_INVALID\""));
        assert!(!invalid.contains("\r\nx-request-id: first\r\n"));

        let overloaded = post_raw(address).await;
        let overloaded = String::from_utf8(overloaded).unwrap();
        assert!(overloaded.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
        assert!(overloaded.contains("\r\nx-request-id: agent:test-request\r\n"));
        assert!(overloaded.contains("\"code\":\"SERVICE_UNAVAILABLE\""));
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);

        tokio::time::sleep(Duration::from_millis(320)).await;
        let success = String::from_utf8(post_raw(address).await).unwrap();
        assert!(success.starts_with("HTTP/1.1 200 OK\r\n"));
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);

        stop_test_listener(shutdown, completion).await;
    }

    #[tokio::test]
    async fn invalid_request_id_fails_before_reading_body_or_calling_the_handler() {
        let handler = Arc::new(TestHandler::default());
        let (address, shutdown, completion) =
            start_test_listener(AgentResourceLimits::PRODUCTION, handler.clone()).await;

        let response =
            post_raw_with_request_id_headers(address, "x-request-id: -invalid\r\n").await;
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 422 Unprocessable Entity\r\n"));
        assert!(response.contains("\"code\":\"PROTOCOL_INVALID\""));
        assert!(!response.contains("\r\nx-request-id: -invalid\r\n"));
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);

        stop_test_listener(shutdown, completion).await;
    }

    #[tokio::test]
    async fn request_deadline_returns_problem_details() {
        let limits = AgentResourceLimits {
            max_connections: 1,
            max_channels: 1,
            max_requests: 1,
            request_timeout: Duration::from_millis(40),
            overload_response_timeout: Duration::from_millis(100),
        };
        let handler = Arc::new(TestHandler {
            calls: AtomicUsize::new(0),
            delay: Duration::from_millis(200),
            channel_first_line: Mutex::new(None),
            channel_sender: Mutex::new(None),
        });
        let (address, shutdown, completion) = start_test_listener(limits, handler.clone()).await;

        let response = String::from_utf8(post_raw(address).await).unwrap();
        assert!(response.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"));
        assert!(response.contains("application/problem+json"));
        assert!(response.contains("\"code\":\"REQUEST_TIMEOUT\""));
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);

        stop_test_listener(shutdown, completion).await;
    }

    #[tokio::test]
    async fn h2_prior_knowledge_channel_ignores_data_boundaries_and_streams_ndjson() {
        let handler = Arc::new(TestHandler::default());
        let (address, shutdown, completion) =
            start_test_listener(AgentResourceLimits::PRODUCTION, handler.clone()).await;
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut client, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let (request_frames, request_body) = mpsc::channel(2);
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://{address}{}",
                neoengram_protocol::AGENT_SESSION_CHANNEL_OPEN_PATH
            ))
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(AgentStreamingBody {
                frames: request_body,
                _stream_permit: None,
            })
            .unwrap();
        let response = tokio::spawn(async move { client.send_request(request).await });

        request_frames
            .send(Bytes::from_static(b"{\"type\":\"channel."))
            .await
            .unwrap();
        request_frames
            .send(Bytes::from_static(b"open\"}\n"))
            .await
            .unwrap();

        let mut response = response.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.version(), Version::HTTP_2);
        assert_eq!(response.headers()[CONTENT_TYPE], NDJSON_CONTENT_TYPE);
        let data = response
            .body_mut()
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(data, Bytes::from_static(b"{\"type\":\"channel.opened\"}\n"));
        assert_eq!(
            handler.channel_first_line.lock().await.as_deref(),
            Some(b"{\"type\":\"channel.open\"}".as_slice())
        );

        drop(request_frames);
        drop(response);
        stop_test_listener(shutdown, completion).await;
    }

    #[tokio::test]
    async fn h2_channels_and_unary_requests_have_independent_global_budgets() {
        let limits = AgentResourceLimits {
            max_connections: 1,
            max_channels: 1,
            max_requests: 1,
            request_timeout: Duration::from_secs(1),
            overload_response_timeout: Duration::from_millis(100),
        };
        let handler = Arc::new(TestHandler::default());
        let (address, shutdown, completion) = start_test_listener(limits, handler.clone()).await;
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut client, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });

        let (channel_frames, channel_body) = mpsc::channel(1);
        let channel_request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://{address}{}",
                neoengram_protocol::AGENT_SESSION_CHANNEL_OPEN_PATH
            ))
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(AgentStreamingBody {
                frames: channel_body,
                _stream_permit: None,
            })
            .unwrap();
        channel_frames
            .send(Bytes::from_static(b"{\"type\":\"channel.open\"}\n"))
            .await
            .unwrap();
        let channel_response = client.send_request(channel_request).await.unwrap();
        assert_eq!(channel_response.status(), StatusCode::OK);

        let (unary_frames, unary_body) = mpsc::channel(1);
        unary_frames.send(Bytes::from_static(b"{}")).await.unwrap();
        drop(unary_frames);
        let unary_request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://{address}{}",
                neoengram_protocol::AGENT_SESSION_OPEN_PATH
            ))
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .body(AgentStreamingBody {
                frames: unary_body,
                _stream_permit: None,
            })
            .unwrap();
        let unary_response = client.send_request(unary_request).await.unwrap();
        assert_eq!(unary_response.status(), StatusCode::OK);

        let (second_channel_frames, second_channel_body) = mpsc::channel(1);
        second_channel_frames
            .send(Bytes::from_static(b"{\"type\":\"channel.open\"}\n"))
            .await
            .unwrap();
        drop(second_channel_frames);
        let second_channel_request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "http://{address}{}",
                neoengram_protocol::AGENT_SESSION_CHANNEL_OPEN_PATH
            ))
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(AgentStreamingBody {
                frames: second_channel_body,
                _stream_permit: None,
            })
            .unwrap();
        let overloaded_channel = client.send_request(second_channel_request).await.unwrap();
        assert_eq!(overloaded_channel.status(), StatusCode::SERVICE_UNAVAILABLE);

        drop(channel_frames);
        drop(channel_response);
        handler.channel_sender.lock().await.take();
        stop_test_listener(shutdown, completion).await;
    }
}
