use std::{convert::Infallible, fmt, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::{Bytes, BytesMut};
use http::{
    header::{ALLOW, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER},
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode,
};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Serialize;
use tokio::{
    net::TcpListener,
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};

mod registry_handler;

pub use registry_handler::{
    AgentDataPlaneHandler, RegistryAgentApiHandler, RegistryAgentEnrollmentHandler,
};

pub const AGENT_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
pub const AGENT_MAX_METADATA_PAGE_BODY_BYTES: usize =
    neoengram_protocol::MAX_METADATA_PAGE_BYTES + AGENT_MAX_REQUEST_BODY_BYTES;
pub const AGENT_MAX_OBJECT_UPLOAD_BODY_BYTES: usize =
    (neoengram_protocol::MAX_INLINE_OBJECT_BYTES * 4).div_ceil(3) + AGENT_MAX_REQUEST_BODY_BYTES;
pub const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const AGENT_MAX_CONCURRENT_REQUESTS: usize = 64;
const AGENT_OVERLOAD_RESPONSE_TIMEOUT: Duration = Duration::from_millis(100);
const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const MAX_REQUEST_ID_BYTES: usize = 128;
const JSON_CONTENT_TYPE: &str = "application/json";
const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";

#[derive(Clone, Copy)]
struct AgentResourceLimits {
    max_connections: usize,
    request_timeout: Duration,
    overload_response_timeout: Duration,
}

impl AgentResourceLimits {
    const PRODUCTION: Self = Self {
        max_connections: AGENT_MAX_CONCURRENT_REQUESTS,
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
    JobObjectMissingQuery,
    JobObjectUpload,
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
            neoengram_protocol::AGENT_JOB_OBJECT_MISSING_QUERY_PATH => {
                Some(Self::JobObjectMissingQuery)
            }
            neoengram_protocol::AGENT_JOB_OBJECT_UPLOAD_PATH => Some(Self::JobObjectUpload),
            neoengram_protocol::AGENT_SESSION_CLOSE_PATH => Some(Self::SessionClose),
            _ => None,
        }
    }

    const fn max_body_bytes(self) -> usize {
        match self {
            Self::JobMetadataPageStage => AGENT_MAX_METADATA_PAGE_BODY_BYTES,
            Self::JobObjectUpload => AGENT_MAX_OBJECT_UPLOAD_BODY_BYTES,
            _ => AGENT_MAX_REQUEST_BODY_BYTES,
        }
    }
}

/// Raw-body application boundary used by the Hyper adapter.
#[async_trait]
pub trait AgentApiHandler: Send + Sync + 'static {
    async fn handle(&self, action: AgentAction, body: &[u8]) -> Result<Vec<u8>, AgentHttpError>;
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
    let semaphore = Arc::new(Semaphore::new(limits.max_connections));
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
                let Ok(permit) = semaphore.clone().try_acquire_owned() else {
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
                connections.spawn(async move {
                    serve_connection(
                        stream,
                        handler,
                        permit,
                        connection_shutdown,
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
    request_timeout: Duration,
) {
    let serve = async {
        let service =
            service_fn(move |request| dispatch(request, handler.clone(), request_timeout));
        let mut builder = http1::Builder::new();
        builder
            .keep_alive(false)
            .timer(TokioTimer::new())
            .header_read_timeout(request_timeout);
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
    request_timeout: Duration,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = request.uri().path().to_owned();
    let request_id = match request_id(request.headers()) {
        Ok(request_id) => request_id,
        Err(error) => {
            let request_id = generated_request_id();
            return Ok(problem_response(error, &path, &request_id));
        }
    };
    let response =
        match tokio::time::timeout(request_timeout, handle_request(request, handler)).await {
            Ok(Ok(body)) => json_response(StatusCode::OK, body, &request_id),
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
) -> Result<(), hyper::Error> {
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
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(false)
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);
    builder
        .serve_connection(TokioIo::new(stream), service)
        .await
}

async fn handle_request(
    request: Request<Incoming>,
    handler: Arc<dyn AgentApiHandler>,
) -> Result<Vec<u8>, AgentHttpError> {
    if request.method() != Method::POST {
        return Err(AgentHttpError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
            "Only POST is supported by Agent API routes",
            false,
        ));
    }
    if !is_json_content_type(request.headers()) {
        return Err(AgentHttpError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            "Content-Type must be application/json",
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
    handler.handle(action, &body).await
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
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next().and_then(|value| value.to_str().ok()) else {
        return false;
    };
    values.next().is_none()
        && value
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(JSON_CONTENT_TYPE))
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

fn problem_response(error: AgentHttpError, path: &str, request_id: &str) -> Response<Full<Bytes>> {
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

fn json_response(status: StatusCode, body: Vec<u8>, request_id: &str) -> Response<Full<Bytes>> {
    response(status, JSON_CONTENT_TYPE, body, request_id)
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
    request_id: &str,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
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

fn problem_title(status: StatusCode) -> &'static str {
    match status {
        StatusCode::FORBIDDEN => "Agent bootstrap denied",
        StatusCode::NOT_FOUND => "Route not found",
        StatusCode::METHOD_NOT_ALLOWED => "Method not allowed",
        StatusCode::PAYLOAD_TOO_LARGE => "Payload too large",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => "Unsupported media type",
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

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::*;

    #[test]
    fn request_id_policy_matches_public_resource_ids() {
        assert!(valid_request_id("req:agent.bootstrap-1"));
        assert!(!valid_request_id("-invalid"));
        assert!(!valid_request_id("contains space"));
        assert!(!valid_request_id(&"a".repeat(129)));
    }

    #[derive(Default)]
    struct TestHandler {
        calls: AtomicUsize,
        delay: Duration,
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
        post_raw_with_request_id_headers(address, "x-request-id: agent:test-request\r\n").await
    }

    async fn post_raw_with_request_id_headers(
        address: SocketAddr,
        request_id_headers: &str,
    ) -> Vec<u8> {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let request = format!(
            "POST /agent/enrollment/bootstrap HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\n{request_id_headers}content-length: 2\r\nconnection: close\r\n\r\n{{}}"
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
    async fn slow_headers_hold_one_connection_budget_and_overflow_gets_problem_details() {
        let limits = AgentResourceLimits {
            max_connections: 1,
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
            request_timeout: Duration::from_millis(40),
            overload_response_timeout: Duration::from_millis(100),
        };
        let handler = Arc::new(TestHandler {
            calls: AtomicUsize::new(0),
            delay: Duration::from_millis(200),
        });
        let (address, shutdown, completion) = start_test_listener(limits, handler.clone()).await;

        let response = String::from_utf8(post_raw(address).await).unwrap();
        assert!(response.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"));
        assert!(response.contains("application/problem+json"));
        assert!(response.contains("\"code\":\"REQUEST_TIMEOUT\""));
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);

        stop_test_listener(shutdown, completion).await;
    }
}
