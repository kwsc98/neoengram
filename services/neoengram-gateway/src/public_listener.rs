//! Public HTTP entry point for the console and the read-only S3 hostname.
//!
//! This module deliberately has no direct access to the workload tunnel. S3 requests are parsed
//! here and delegated to an authorization-aware Central adapter; authorized object bytes are
//! streamed through the bounded Agent read channel using Central-issued tickets. Keeping the
//! route and host policy here prevents a public request from reaching the mTLS control handlers.

use std::{
    convert::Infallible,
    error::Error,
    fmt::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{
    header::{
        HeaderName, HeaderValue, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_ALLOW_ORIGIN, ALLOW, CONTENT_LENGTH, CONTENT_TYPE, HOST,
    },
    HeaderMap, Method, Request, Response, StatusCode, Uri,
};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt as _, Full};
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto::Builder as ConnectionBuilder,
};
use percent_encoding::percent_decode_str;
use tokio::{
    fs,
    io::AsyncWriteExt as _,
    net::{TcpListener, TcpStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};
use url::Url;

use crate::central_http::CentralHttpClient;

const PUBLIC_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_HOST_BYTES: usize = 255;

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;
type PublicBody = UnsyncBoxBody<Bytes, BoxError>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PublicListenerLimits {
    max_connections: usize,
    max_in_flight_requests: usize,
    max_request_bytes: usize,
    request_deadline: Duration,
}

pub(crate) struct PublicListenerConfig {
    console_host: String,
    s3_host: String,
    web_root: PathBuf,
    central_upstream: Option<Url>,
    max_streams: usize,
    allow_loopback_hosts: bool,
}

impl PublicListenerConfig {
    pub(crate) fn new(
        console_host: String,
        s3_host: String,
        web_root: PathBuf,
        central_upstream: Option<Url>,
        max_streams: usize,
        allow_loopback_hosts: bool,
    ) -> Self {
        Self {
            console_host,
            s3_host,
            web_root,
            central_upstream,
            max_streams,
            allow_loopback_hosts,
        }
    }
}

impl PublicListenerLimits {
    pub(crate) fn new(
        max_connections: usize,
        max_in_flight_requests: usize,
        max_request_bytes: usize,
        request_deadline: Duration,
    ) -> Self {
        debug_assert!(max_connections > 0);
        debug_assert!(max_in_flight_requests > 0);
        debug_assert!(max_request_bytes > 0);
        debug_assert!(!request_deadline.is_zero());
        Self {
            max_connections,
            max_in_flight_requests,
            max_request_bytes,
            request_deadline,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PublicListenerState {
    console_host: Arc<str>,
    s3_host: Arc<str>,
    web_root: Arc<PathBuf>,
    central_upstream: Option<Url>,
    central_client: Option<CentralHttpClient>,
    max_streams: u32,
    s3_admission: Arc<Semaphore>,
    max_connections: usize,
    request_admission: Arc<Semaphore>,
    max_request_bytes: usize,
    request_deadline: Duration,
    draining: Arc<AtomicBool>,
    s3_backend: Option<Arc<dyn S3ControlBackend>>,
    /// Loopback binds are useful during local development where the browser sends `localhost`
    /// or `127.0.0.1` rather than the configured production hostname.
    allow_loopback_hosts: bool,
}

impl PublicListenerState {
    pub(crate) fn new(
        config: PublicListenerConfig,
        limits: PublicListenerLimits,
        draining: Arc<AtomicBool>,
    ) -> Self {
        Self {
            console_host: Arc::<str>::from(
                normalize_configured_host(&config.console_host)
                    .expect("GatewayConfig validated console_host before listener startup"),
            ),
            s3_host: Arc::<str>::from(
                normalize_configured_host(&config.s3_host)
                    .expect("GatewayConfig validated s3_host before listener startup"),
            ),
            web_root: Arc::new(config.web_root),
            central_upstream: config.central_upstream,
            central_client: None,
            max_streams: u32::try_from(config.max_streams).unwrap_or(u32::MAX),
            s3_admission: Arc::new(Semaphore::new(config.max_streams)),
            max_connections: limits.max_connections,
            request_admission: Arc::new(Semaphore::new(limits.max_in_flight_requests)),
            max_request_bytes: limits.max_request_bytes,
            request_deadline: limits.request_deadline,
            draining,
            allow_loopback_hosts: config.allow_loopback_hosts,
            s3_backend: None,
        }
    }

    /// Installs a read-only metadata adapter for the public S3 listener.  Production wiring is
    /// intentionally explicit: without an adapter the Gateway never treats a public request as
    /// authorized and returns a bounded, protocol-shaped unavailable response.
    pub(crate) fn with_s3_backend(mut self, backend: Arc<dyn S3ControlBackend>) -> Self {
        self.s3_backend = Some(backend);
        self
    }

    /// Installs the shared mTLS Central client used by the browser control-plane proxy.
    pub(crate) fn with_central_client(mut self, client: CentralHttpClient) -> Self {
        self.central_client = Some(client);
        self
    }
}

pub(crate) fn spawn_listener(
    listeners: &mut JoinSet<std::io::Result<()>>,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    state: PublicListenerState,
    shutdown: watch::Receiver<bool>,
) {
    let connection_admission = Arc::new(Semaphore::new(state.max_connections));
    listeners.spawn(serve_listener(
        listener,
        tls_acceptor,
        Arc::new(state),
        connection_admission,
        shutdown,
    ));
}

async fn serve_listener(
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    state: Arc<PublicListenerState>,
    connection_admission: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    info!(
        listener = "public",
        address = %listener.local_addr()?,
        console_host = %state.console_host,
        s3_host = %state.s3_host,
        "Gateway public listener started"
    );
    loop {
        let (mut socket, peer_address) = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            accepted = listener.accept() => accepted?,
        };
        let permit = match connection_admission.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    listener = "public",
                    %peer_address,
                    "Gateway public connection admission limit reached"
                );
                let _ = socket.shutdown().await;
                continue;
            }
        };
        let state = state.clone();
        let shutdown = shutdown.clone();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            serve_connection(socket, peer_address, tls_acceptor, state, permit, shutdown).await;
        });
    }
}

async fn serve_connection(
    socket: TcpStream,
    peer_address: SocketAddr,
    tls_acceptor: Option<TlsAcceptor>,
    state: Arc<PublicListenerState>,
    _permit: OwnedSemaphorePermit,
    shutdown: watch::Receiver<bool>,
) {
    let result = if let Some(acceptor) = tls_acceptor {
        match tokio::time::timeout(PUBLIC_TLS_HANDSHAKE_TIMEOUT, acceptor.accept(socket)).await {
            Ok(Ok(stream)) => serve_http_connection(stream, state, shutdown).await,
            Ok(Err(error)) => {
                warn!(listener = "public", %peer_address, %error, "public TLS handshake failed");
                return;
            }
            Err(_) => {
                warn!(listener = "public", %peer_address, "public TLS handshake timed out");
                return;
            }
        }
    } else {
        serve_http_connection(socket, state, shutdown).await
    };
    if let Err(error) = result {
        error!(listener = "public", %peer_address, %error, "public connection failed");
    }
}

async fn serve_http_connection<I>(
    io: I,
    state: Arc<PublicListenerState>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), BoxError>
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let max_streams = state.max_streams;
    let service_state = state.clone();
    let service = service_fn(move |request: Request<Incoming>| {
        handle_request(request, service_state.clone())
    });
    let mut builder = ConnectionBuilder::new(TokioExecutor::new());
    builder.http1().keep_alive(true).timer(TokioTimer::new());
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(max_streams);
    let connection = builder.serve_connection(TokioIo::new(io), service);
    tokio::pin!(connection);
    tokio::select! {
        result = &mut connection => result,
        changed = shutdown.changed() => {
            if changed.is_ok() && *shutdown.borrow() {
                connection.as_mut().graceful_shutdown();
            }
            connection.await
        }
    }
}

async fn handle_request<B>(
    request: Request<B>,
    state: Arc<PublicListenerState>,
) -> Result<Response<PublicBody>, Infallible>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Error + Send + Sync + 'static,
{
    let path = request.uri().path();
    if request.method() == Method::GET && path == "/health/live" {
        return Ok(json_response(
            StatusCode::OK,
            serde_json::json!({
                "service": "neoengram-gateway",
                "listener": "public",
                "status": "live"
            }),
        ));
    }
    if request.method() == Method::GET && path == "/health/ready" {
        return Ok(if state.draining.load(Ordering::Acquire) {
            problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "gateway_draining",
                "Gateway is draining",
            )
        } else {
            json_response(
                StatusCode::OK,
                serde_json::json!({
                    "service": "neoengram-gateway",
                    "listener": "public",
                    "status": "ready",
                    "centralConfigured": state.central_upstream.is_some()
                }),
            )
        });
    }
    let host = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            request
                .uri()
                .authority()
                .map(|authority| authority.as_str())
        })
        .and_then(normalize_request_host);
    let is_s3 = host
        .as_deref()
        .is_some_and(|host| is_s3_request_host(host, &state.s3_host));
    let request_permit = match state.request_admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) if is_s3 => {
            return Ok(s3_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "SlowDown",
                "The Gateway request admission limit has been reached.",
                "GET, HEAD, OPTIONS",
                request.method() == Method::HEAD,
            ))
        }
        Err(_) => {
            return Ok(problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "request_admission_exhausted",
                "Gateway request admission limit reached",
            ))
        }
    };
    let response = if state.draining.load(Ordering::Acquire) {
        problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway_draining",
            "Gateway is draining",
        )
    } else if is_s3 {
        handle_s3_request(
            request,
            host.as_deref().expect("S3 host was matched above"),
            &state,
        )
        .await
    } else if host.as_deref() == Some(state.console_host.as_ref())
        || (state.allow_loopback_hosts && host.as_deref().is_some_and(is_loopback_host))
    {
        if is_api_path(path) {
            proxy_central(request, &state).await
        } else {
            serve_static(request, &state).await
        }
    } else {
        problem_response(
            StatusCode::MISDIRECTED_REQUEST,
            "unknown_public_host",
            "Unknown public host",
        )
    };
    Ok(retain_request_permit(response, request_permit))
}

fn is_api_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
}

/// Authentication material and canonical HTTP metadata passed to the trusted S3 adapter.  The
/// adapter must resolve the bucket/access key and verify SigV4 before returning any metadata.
#[derive(Debug, Clone)]
pub(crate) struct S3AuthorizationContext {
    pub method: Method,
    pub uri: Uri,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3ListObjectsV2Request {
    pub prefix: String,
    pub delimiter: Option<String>,
    pub continuation_token: Option<String>,
    pub start_after: Option<String>,
    pub max_keys: u16,
    pub encoding_type_url: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum S3ControlOperation {
    HeadBucket,
    GetBucketLocation,
    ListObjectsV2(S3ListObjectsV2Request),
    HeadObject {
        key: String,
        range: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
    },
    GetObject {
        key: String,
        range: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct S3ControlRequest {
    pub bucket: String,
    pub operation: S3ControlOperation,
    pub authorization: S3AuthorizationContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3BucketMetadata {
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3ObjectMetadata {
    pub key: String,
    pub size_bytes: u64,
    pub etag: String,
    pub last_modified_unix_ms: u64,
    pub content_type: String,
}

pub(crate) struct S3StreamCancellation {
    callback: Option<Box<dyn FnOnce() + Send + 'static>>,
    request_permit: Option<OwnedSemaphorePermit>,
}

impl S3StreamCancellation {
    pub(crate) fn new(callback: impl FnOnce() + Send + 'static) -> Self {
        Self {
            callback: Some(Box::new(callback)),
            request_permit: None,
        }
    }

    fn retain_request_permit(&mut self, permit: OwnedSemaphorePermit) {
        debug_assert!(self.request_permit.is_none());
        self.request_permit = Some(permit);
    }

    pub(crate) fn disarm(&mut self) {
        self.callback = None;
    }

    fn is_armed(&self) -> bool {
        self.callback.is_some()
    }
}

impl std::fmt::Debug for S3StreamCancellation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3StreamCancellation")
            .field("armed", &self.is_armed())
            .field("holds_request_permit", &self.request_permit.is_some())
            .finish()
    }
}

impl Drop for S3StreamCancellation {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback();
        }
    }
}

#[derive(Debug)]
pub(crate) struct S3ObjectStream {
    pub(crate) receiver: tokio::sync::mpsc::Receiver<Result<Bytes, BoxError>>,
    pub(crate) cancellation: S3StreamCancellation,
    pub(crate) expected_length: u64,
}

#[derive(Debug)]
pub(crate) enum S3ObjectBody {
    Empty,
    #[cfg(test)]
    Bytes(Bytes),
    Stream(S3ObjectStream),
}

#[derive(Debug)]
pub(crate) struct S3ObjectResponse {
    pub metadata: S3ObjectMetadata,
    pub body: S3ObjectBody,
    pub content_length: u64,
    pub content_range: Option<String>,
    pub status: StatusCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3ListObjectsV2Result {
    pub objects: Vec<S3ObjectMetadata>,
    pub common_prefixes: Vec<String>,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug)]
pub(crate) enum S3ControlResponse {
    Bucket(S3BucketMetadata),
    ListObjectsV2(S3ListObjectsV2Result),
    Object(S3ObjectResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum S3ControlError {
    AccessDenied,
    NoSuchBucket,
    NoSuchKey,
    InvalidArgument(String),
    Unavailable,
    Internal,
}

/// Trusted adapter boundary for Central-owned S3 metadata.  Implementations are responsible for
/// credential resolution, SigV4 verification, tenant authorization, and translating the public
/// bucket into Central's tenant/access-point identifiers.  Returning metadata is the authorization
/// decision; the public listener never caches that decision or invents a bucket binding.
#[async_trait]
pub(crate) trait S3ControlBackend: Send + Sync {
    async fn execute(&self, request: S3ControlRequest)
        -> Result<S3ControlResponse, S3ControlError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedS3Request {
    bucket: String,
    operation: ParsedS3Operation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedS3Operation {
    Control(S3ControlOperation),
    GetObject { key: String },
    HeadObject { key: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct S3ParseError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

async fn handle_s3_request<B>(
    request: Request<B>,
    request_host: &str,
    state: &PublicListenerState,
) -> Response<PublicBody> {
    let method = request.method();
    let allow = "GET, HEAD, OPTIONS";
    if method == Method::OPTIONS {
        let mut response = empty_response(StatusCode::NO_CONTENT);
        insert_s3_cors_headers(response.headers_mut());
        response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET, HEAD, OPTIONS"));
        return response;
    }
    if !matches!(*method, Method::GET | Method::HEAD) {
        return s3_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "The requested method is not allowed for this read-only endpoint.",
            allow,
            method == Method::HEAD,
        );
    }
    if !has_s3_authorization(&request) {
        return s3_error_response(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "A SigV4 Authorization header or presigned URL is required.",
            allow,
            *method == Method::HEAD,
        );
    }
    let parsed = match parse_s3_request(&request, request_host, &state.s3_host) {
        Ok(parsed) => parsed,
        Err(error) => {
            return s3_error_response(
                error.status,
                error.code,
                error.message,
                allow,
                *method == Method::HEAD,
            )
        }
    };
    let response_content_disposition = match response_content_disposition(request.uri()) {
        Ok(value) => value,
        Err(error) => {
            return s3_error_response(
                error.status,
                error.code,
                error.message,
                allow,
                *method == Method::HEAD,
            )
        }
    };
    let operation = match parsed.operation {
        ParsedS3Operation::Control(operation) => operation,
        ParsedS3Operation::GetObject { key } => {
            let range = request
                .headers()
                .get("range")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            S3ControlOperation::GetObject {
                key,
                range,
                if_match: request
                    .headers()
                    .get("if-match")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
                if_none_match: request
                    .headers()
                    .get("if-none-match")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            }
        }
        ParsedS3Operation::HeadObject { key } => S3ControlOperation::HeadObject {
            key,
            range: request
                .headers()
                .get("range")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            if_match: request
                .headers()
                .get("if-match")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            if_none_match: request
                .headers()
                .get("if-none-match")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        },
    };
    let Some(backend) = state.s3_backend.as_ref() else {
        return s3_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "The S3 metadata service is not configured on this Gateway.",
            allow,
            *method == Method::HEAD,
        );
    };
    let permit = match state.s3_admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return s3_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "SlowDown",
                "The S3 stream admission limit has been reached.",
                allow,
                *method == Method::HEAD,
            )
        }
    };
    let control_request = S3ControlRequest {
        bucket: parsed.bucket.clone(),
        operation: operation.clone(),
        authorization: S3AuthorizationContext {
            method: request.method().clone(),
            uri: request.uri().clone(),
            headers: request.headers().clone(),
        },
    };
    let mut response = match backend.execute(control_request).await {
        Ok(response) => response,
        Err(error) => return s3_control_error_response(error, allow, *method == Method::HEAD),
    };
    if let S3ControlResponse::Object(S3ObjectResponse {
        body: S3ObjectBody::Stream(stream),
        ..
    }) = &mut response
    {
        stream.cancellation.retain_request_permit(permit);
    }
    match (operation, response) {
        (S3ControlOperation::HeadBucket, S3ControlResponse::Bucket(bucket)) => {
            s3_head_bucket_response(&bucket)
        }
        (S3ControlOperation::GetBucketLocation, S3ControlResponse::Bucket(bucket)) => {
            s3_location_response(&bucket)
        }
        (
            S3ControlOperation::ListObjectsV2(list_request),
            S3ControlResponse::ListObjectsV2(result),
        ) => match s3_list_objects_v2_response(&parsed.bucket, &list_request, &result) {
            Ok(response) => response,
            Err(error) => s3_control_error_response(error, allow, false),
        },
        (S3ControlOperation::HeadObject { .. }, S3ControlResponse::Object(object)) => {
            s3_object_response(object, true, allow, None)
        }
        (S3ControlOperation::GetObject { .. }, S3ControlResponse::Object(object)) => {
            s3_object_response(object, false, allow, response_content_disposition.as_ref())
        }
        _ => s3_error_response(
            StatusCode::BAD_GATEWAY,
            "InternalError",
            "The S3 metadata service returned an incompatible response.",
            allow,
            *method == Method::HEAD,
        ),
    }
}

fn parse_s3_request<B>(
    request: &Request<B>,
    request_host: &str,
    configured_host: &str,
) -> Result<ParsedS3Request, S3ParseError> {
    let (bucket, key) =
        parse_s3_bucket_and_key(request.uri().path(), request_host, configured_host)?;
    let query = parse_s3_query(request.uri())?;
    if query.has("location") && query.has("list-type") {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "A bucket request cannot combine location and list-type.",
        });
    }
    let is_bucket = key.is_none();
    let operation = match (request.method().clone(), is_bucket) {
        (Method::HEAD, true) => ParsedS3Operation::Control(S3ControlOperation::HeadBucket),
        (Method::HEAD, false) => ParsedS3Operation::HeadObject {
            key: key.expect("object path was checked above"),
        },
        (Method::GET, false) => ParsedS3Operation::GetObject {
            key: key.expect("object path was checked above"),
        },
        (Method::GET, true) if query.has("location") => {
            if query
                .single("location")?
                .is_some_and(|value| !value.is_empty())
            {
                return Err(S3ParseError {
                    status: StatusCode::BAD_REQUEST,
                    code: "InvalidArgument",
                    message: "The location query parameter must not have a value.",
                });
            }
            ParsedS3Operation::Control(S3ControlOperation::GetBucketLocation)
        }
        (Method::GET, true) if query.has("list-type") => {
            if query.single("list-type")?.as_deref() != Some("2") {
                return Err(S3ParseError {
                    status: StatusCode::NOT_IMPLEMENTED,
                    code: "NotImplemented",
                    message: "Only ListObjectsV2 is supported.",
                });
            }
            ParsedS3Operation::Control(S3ControlOperation::ListObjectsV2(
                parse_list_objects_v2_query(&query)?,
            ))
        }
        (Method::GET, true) => {
            return Err(S3ParseError {
                status: StatusCode::NOT_IMPLEMENTED,
                code: "NotImplemented",
                message: "Only ListObjectsV2 is supported for bucket GET requests.",
            })
        }
        _ => {
            return Err(S3ParseError {
                status: StatusCode::METHOD_NOT_ALLOWED,
                code: "MethodNotAllowed",
                message: "The requested method is not allowed for this S3 resource.",
            })
        }
    };
    Ok(ParsedS3Request { bucket, operation })
}

fn has_s3_authorization<B>(request: &Request<B>) -> bool {
    if request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.trim().is_empty())
    {
        return true;
    }
    parse_s3_query(request.uri())
        .ok()
        .and_then(|query| query.single("X-Amz-Signature").ok().flatten())
        .is_some_and(|signature| {
            signature.len() == 64 && signature.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn parse_s3_bucket_and_key(
    path: &str,
    request_host: &str,
    configured_host: &str,
) -> Result<(String, Option<String>), S3ParseError> {
    let path = percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidURI",
            message: "The request URI is not valid UTF-8.",
        })?
        .into_owned();
    if !path.starts_with('/')
        || path.as_bytes().get(1) == Some(&b'/')
        || path.as_bytes().contains(&0)
    {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidURI",
            message: "The request URI is invalid.",
        });
    }
    let (bucket, key) = if request_host == configured_host {
        let resource = path.trim_start_matches('/');
        if resource.is_empty() {
            return Err(S3ParseError {
                status: StatusCode::BAD_REQUEST,
                code: "InvalidBucketName",
                message: "A bucket name is required in path-style S3 requests.",
            });
        }
        match resource.split_once('/') {
            Some((bucket, key)) => (bucket, (!key.is_empty()).then_some(key)),
            None => (resource, None),
        }
    } else {
        let bucket = request_host
            .strip_suffix(configured_host)
            .and_then(|prefix| prefix.strip_suffix('.'))
            .filter(|prefix| !prefix.is_empty())
            .ok_or(S3ParseError {
                status: StatusCode::MISDIRECTED_REQUEST,
                code: "InvalidHost",
                message: "The request host is not an S3 endpoint.",
            })?;
        let key = path.trim_start_matches('/');
        (bucket, (!key.is_empty()).then_some(key))
    };
    validate_s3_bucket_name_for_gateway(bucket)?;
    if let Some(key) = key {
        validate_s3_key_for_gateway(key)?;
        Ok((bucket.to_owned(), Some(key.to_owned())))
    } else {
        Ok((bucket.to_owned(), None))
    }
}

#[derive(Debug, Clone, Default)]
struct S3Query {
    pairs: Vec<(String, String)>,
}

impl S3Query {
    fn has(&self, name: &str) -> bool {
        self.pairs
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case(name))
    }

    fn single(&self, name: &str) -> Result<Option<String>, S3ParseError> {
        let values = self
            .pairs
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        match values.as_slice() {
            [] => Ok(None),
            [value] => Ok(Some((*value).clone())),
            _ => Err(S3ParseError {
                status: StatusCode::BAD_REQUEST,
                code: "InvalidArgument",
                message: "A query parameter was specified more than once.",
            }),
        }
    }
}

fn parse_s3_query(uri: &Uri) -> Result<S3Query, S3ParseError> {
    let query = uri.query().unwrap_or_default();
    if query.len() > 16 * 1024 {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidURI",
            message: "The request query string is too large.",
        });
    }
    let mut pairs = Vec::new();
    for part in query.split('&').filter(|part| !part.is_empty()) {
        let (name, value) = part.split_once('=').unwrap_or((part, ""));
        let name = percent_decode_str(name)
            .decode_utf8()
            .map_err(|_| S3ParseError {
                status: StatusCode::BAD_REQUEST,
                code: "InvalidURI",
                message: "The request query string is invalid UTF-8.",
            })?
            .into_owned();
        let value = percent_decode_str(value)
            .decode_utf8()
            .map_err(|_| S3ParseError {
                status: StatusCode::BAD_REQUEST,
                code: "InvalidURI",
                message: "The request query string is invalid UTF-8.",
            })?
            .into_owned();
        if name.is_empty() {
            return Err(S3ParseError {
                status: StatusCode::BAD_REQUEST,
                code: "InvalidArgument",
                message: "Query parameter names must not be empty.",
            });
        }
        pairs.push((name, value));
    }
    Ok(S3Query { pairs })
}

fn response_content_disposition(uri: &Uri) -> Result<Option<HeaderValue>, S3ParseError> {
    let value = parse_s3_query(uri)?.single("response-content-disposition")?;
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty()
        || value.len() > 1024
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == 0x7f)
    {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message:
                "response-content-disposition is empty, too large, or contains a control character.",
        });
    }
    HeaderValue::from_str(&value)
        .map(Some)
        .map_err(|_| S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "response-content-disposition is not a valid HTTP header value.",
        })
}

fn parse_list_objects_v2_query(query: &S3Query) -> Result<S3ListObjectsV2Request, S3ParseError> {
    let prefix = query.single("prefix")?.unwrap_or_default();
    validate_s3_prefix_for_gateway(&prefix)?;
    let delimiter = query.single("delimiter")?;
    let delimiter = delimiter.filter(|value| !value.is_empty());
    if delimiter.as_deref().is_some_and(|value| value != "/") {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "Only '/' is supported as a delimiter.",
        });
    }
    let max_keys = query.single("max-keys")?.map_or(Ok(1000), |value| {
        value.parse::<u16>().map_err(|_| S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "max-keys must be an integer between 0 and 1000.",
        })
    })?;
    if max_keys > 1000 {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "max-keys must be an integer between 0 and 1000.",
        });
    }
    let continuation_token = query.single("continuation-token")?;
    if continuation_token
        .as_deref()
        .is_some_and(|value| value.is_empty() || value.len() > 2048)
    {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "continuation-token is empty or too large.",
        });
    }
    let start_after = query.single("start-after")?;
    if start_after
        .as_deref()
        .is_some_and(|value| value.is_empty() || value.len() > 4096)
    {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "start-after is empty or too large.",
        });
    }
    if let Some(value) = start_after.as_deref() {
        validate_s3_cursor_key_for_gateway(value)?;
    }
    let encoding_type_url = query
        .single("encoding-type")?
        .map(|value| match value.as_str() {
            "url" => Ok(true),
            _ => Err(S3ParseError {
                status: StatusCode::BAD_REQUEST,
                code: "InvalidArgument",
                message: "Only encoding-type=url is supported.",
            }),
        })
        .transpose()?
        .unwrap_or(false);
    if let Some(fetch_owner) = query.single("fetch-owner")? {
        match fetch_owner.as_str() {
            "false" => {}
            "true" => {
                return Err(S3ParseError {
                    status: StatusCode::BAD_REQUEST,
                    code: "InvalidArgument",
                    message: "fetch-owner=true is not supported by this endpoint.",
                });
            }
            _ => {
                return Err(S3ParseError {
                    status: StatusCode::BAD_REQUEST,
                    code: "InvalidArgument",
                    message: "fetch-owner must be true or false.",
                });
            }
        }
    }
    Ok(S3ListObjectsV2Request {
        prefix,
        delimiter,
        continuation_token,
        start_after,
        max_keys,
        encoding_type_url,
    })
}

fn validate_s3_bucket_name_for_gateway(value: &str) -> Result<(), S3ParseError> {
    if !(3..=63).contains(&value.len())
        || value != value.to_ascii_lowercase()
        || value.starts_with('.')
        || value.ends_with('.')
        || value.starts_with('-')
        || value.ends_with('-')
        || value.contains("..")
        || value.contains(".-")
        || value.contains("-.")
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'.'
        })
        || value.parse::<std::net::Ipv4Addr>().is_ok()
        || matches!(value, "api" | "health" | "console" | "s3")
    {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidBucketName",
            message: "The specified bucket is not valid.",
        });
    }
    Ok(())
}

fn validate_s3_prefix_for_gateway(value: &str) -> Result<(), S3ParseError> {
    if value.len() > 1024 || value.as_bytes().contains(&0) {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "prefix is too large or contains an invalid character.",
        });
    }
    if value.is_empty() {
        return Ok(());
    }
    let canonical = value.strip_suffix('/').unwrap_or(value);
    if canonical.is_empty()
        || canonical.contains('\\')
        || canonical.split('/').any(|segment| {
            segment.is_empty() || segment == "." || segment == ".." || has_xml_control(segment)
        })
    {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "prefix must use canonical object path components.",
        });
    }
    Ok(())
}

fn validate_s3_key_for_gateway(value: &str) -> Result<(), S3ParseError> {
    if value.is_empty()
        || value.len() > 4096
        || value.contains('\\')
        || value.split('/').any(|segment| {
            segment.is_empty() || segment == "." || segment == ".." || has_xml_control(segment)
        })
    {
        return Err(S3ParseError {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: "The object key is not a canonical path.",
        });
    }
    Ok(())
}

/// `StartAfter` can point at a synthetic CommonPrefix when a delimiter is used. Such cursor
/// values end in `/` and are valid canonical prefixes even though they are not object keys.
fn validate_s3_cursor_key_for_gateway(value: &str) -> Result<(), S3ParseError> {
    if value.ends_with('/') {
        validate_s3_prefix_for_gateway(value)
    } else {
        validate_s3_key_for_gateway(value)
    }
}

fn has_xml_control(value: &str) -> bool {
    value.chars().any(|character| {
        matches!(character, '\u{0000}'..='\u{0008}' | '\u{000B}'..='\u{000C}' | '\u{000E}'..='\u{001F}')
    })
}

fn s3_control_error_response(
    error: S3ControlError,
    allow: &str,
    head_only: bool,
) -> Response<PublicBody> {
    let (status, code, message) = match error {
        S3ControlError::AccessDenied => (
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "The S3 request is not authorized.",
        ),
        S3ControlError::NoSuchBucket => (
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "The specified bucket does not exist.",
        ),
        S3ControlError::NoSuchKey => (
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist.",
        ),
        S3ControlError::InvalidArgument(message) => {
            let message = bounded_s3_error_message(&message);
            return s3_error_response(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                &message,
                allow,
                head_only,
            );
        }
        S3ControlError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "The S3 metadata service is temporarily unavailable.",
        ),
        S3ControlError::Internal => (
            StatusCode::BAD_GATEWAY,
            "InternalError",
            "The S3 metadata service returned an internal error.",
        ),
    };
    s3_error_response(status, code, message, allow, head_only)
}

fn bounded_s3_error_message(message: &str) -> String {
    const MAX_ERROR_CHARACTERS: usize = 1024;
    message.chars().take(MAX_ERROR_CHARACTERS).collect()
}

fn s3_head_bucket_response(bucket: &S3BucketMetadata) -> Response<PublicBody> {
    if bucket.region.is_empty() || bucket.region.len() > 128 || has_xml_control(&bucket.region) {
        return s3_error_response(
            StatusCode::BAD_GATEWAY,
            "InternalError",
            "The S3 metadata service returned an invalid region.",
            "GET, HEAD, OPTIONS",
            true,
        );
    }
    let mut response = empty_response(StatusCode::OK);
    response.headers_mut().insert(
        HeaderName::from_static("x-amz-bucket-region"),
        HeaderValue::from_str(&bucket.region).expect("validated S3 region header"),
    );
    insert_s3_cors_headers(response.headers_mut());
    response
}

fn s3_location_response(bucket: &S3BucketMetadata) -> Response<PublicBody> {
    if bucket.region.is_empty() || bucket.region.len() > 128 || has_xml_control(&bucket.region) {
        return s3_error_response(
            StatusCode::BAD_GATEWAY,
            "InternalError",
            "The S3 metadata service returned an invalid region.",
            "GET, HEAD, OPTIONS",
            false,
        );
    }
    let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    let _ = write!(
        body,
        "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
        xml_escape(&bucket.region)
    );
    xml_response(StatusCode::OK, body, false)
}

fn s3_list_objects_v2_response(
    bucket: &str,
    request: &S3ListObjectsV2Request,
    result: &S3ListObjectsV2Result,
) -> Result<Response<PublicBody>, S3ControlError> {
    let object_count = result.objects.len();
    let prefix_count = result.common_prefixes.len();
    let key_count = object_count
        .checked_add(prefix_count)
        .ok_or(S3ControlError::Internal)?;
    if key_count > usize::from(request.max_keys)
        || (request.max_keys == 0 && key_count != 0)
        || result
            .next_continuation_token
            .as_deref()
            .is_some_and(|token| token.is_empty() || token.len() > 2048)
    {
        return Err(S3ControlError::Internal);
    }
    for object in &result.objects {
        validate_s3_key_for_gateway(&object.key).map_err(|_| S3ControlError::Internal)?;
        if object.etag.is_empty() || object.etag.len() > 512 || has_xml_control(&object.etag) {
            return Err(S3ControlError::Internal);
        }
    }
    for prefix in &result.common_prefixes {
        validate_s3_prefix_for_gateway(prefix).map_err(|_| S3ControlError::Internal)?;
    }
    let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    body.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml_element(&mut body, "Name", bucket, false);
    xml_element(
        &mut body,
        "Prefix",
        &request.prefix,
        request.encoding_type_url,
    );
    xml_element(&mut body, "KeyCount", &key_count.to_string(), false);
    xml_element(&mut body, "MaxKeys", &request.max_keys.to_string(), false);
    if let Some(delimiter) = request.delimiter.as_deref() {
        xml_element(&mut body, "Delimiter", delimiter, request.encoding_type_url);
    }
    if request.encoding_type_url {
        xml_element(&mut body, "EncodingType", "url", false);
    }
    xml_element(
        &mut body,
        "IsTruncated",
        if result.next_continuation_token.is_some() {
            "true"
        } else {
            "false"
        },
        false,
    );
    if let Some(token) = request.continuation_token.as_deref() {
        xml_element(
            &mut body,
            "ContinuationToken",
            token,
            request.encoding_type_url,
        );
    }
    if let Some(token) = result.next_continuation_token.as_deref() {
        xml_element(
            &mut body,
            "NextContinuationToken",
            token,
            request.encoding_type_url,
        );
    }
    if let Some(start_after) = request.start_after.as_deref() {
        xml_element(
            &mut body,
            "StartAfter",
            start_after,
            request.encoding_type_url,
        );
    }
    for object in &result.objects {
        body.push_str("<Contents>");
        xml_element(&mut body, "Key", &object.key, request.encoding_type_url);
        let last_modified =
            unix_millis_to_rfc3339(object.last_modified_unix_ms).ok_or(S3ControlError::Internal)?;
        xml_element(&mut body, "LastModified", &last_modified, false);
        let etag = if object.etag.starts_with('"') && object.etag.ends_with('"') {
            object.etag.clone()
        } else {
            format!("\"{}\"", object.etag)
        };
        xml_element(&mut body, "ETag", &etag, false);
        xml_element(&mut body, "Size", &object.size_bytes.to_string(), false);
        xml_element(&mut body, "StorageClass", "STANDARD", false);
        body.push_str("</Contents>");
    }
    for prefix in &result.common_prefixes {
        body.push_str("<CommonPrefixes>");
        xml_element(&mut body, "Prefix", prefix, request.encoding_type_url);
        body.push_str("</CommonPrefixes>");
    }
    body.push_str("</ListBucketResult>");
    Ok(xml_response(StatusCode::OK, body, false))
}

fn s3_object_response(
    mut object: S3ObjectResponse,
    head_only: bool,
    allow: &str,
    response_content_disposition: Option<&HeaderValue>,
) -> Response<PublicBody> {
    let status = object.status;
    if !matches!(
        status,
        StatusCode::OK
            | StatusCode::PARTIAL_CONTENT
            | StatusCode::NOT_MODIFIED
            | StatusCode::PRECONDITION_FAILED
            | StatusCode::RANGE_NOT_SATISFIABLE
    ) {
        return s3_error_response(
            StatusCode::BAD_GATEWAY,
            "InternalError",
            "The S3 object service returned an invalid response.",
            allow,
            head_only,
        );
    }
    let mut response = match (head_only, object.body) {
        (_, S3ObjectBody::Empty) => empty_response(status),
        (true, _) => empty_response(status),
        #[cfg(test)]
        (false, S3ObjectBody::Bytes(body)) => {
            if u64::try_from(body.len()).ok() != Some(object.content_length) {
                return s3_error_response(
                    StatusCode::BAD_GATEWAY,
                    "InternalError",
                    "The S3 object service returned an invalid body length.",
                    allow,
                    false,
                );
            }
            response_with_body(status, body)
        }
        (false, S3ObjectBody::Stream(stream)) => response_with_stream(status, stream),
    };
    if !object.metadata.content_type.is_empty()
        && object.metadata.content_type.len() <= 256
        && !has_xml_control(&object.metadata.content_type)
    {
        if let Ok(value) = HeaderValue::from_str(&object.metadata.content_type) {
            response.headers_mut().insert(CONTENT_TYPE, value);
        }
    }
    if status != StatusCode::NOT_MODIFIED {
        if let Ok(value) = HeaderValue::from_str(&object.content_length.to_string()) {
            response.headers_mut().insert(CONTENT_LENGTH, value);
        }
    }
    if !object.metadata.etag.is_empty() && !has_xml_control(&object.metadata.etag) {
        let etag = if object.metadata.etag.starts_with('"') {
            object.metadata.etag.clone()
        } else {
            format!("\"{}\"", object.metadata.etag)
        };
        if let Ok(value) = HeaderValue::from_str(&etag) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("etag"), value);
        }
    }
    if let Some(last_modified) = unix_millis_to_http_date(object.metadata.last_modified_unix_ms) {
        if let Ok(value) = HeaderValue::from_str(&last_modified) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("last-modified"), value);
        }
    }
    if let Some(content_range) = object.content_range.take() {
        if let Ok(value) = HeaderValue::from_str(&content_range) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("content-range"), value);
        }
    }
    if let Some(content_disposition) = response_content_disposition {
        response.headers_mut().insert(
            HeaderName::from_static("content-disposition"),
            content_disposition.clone(),
        );
    }
    response.headers_mut().insert(
        ALLOW,
        HeaderValue::from_str(allow).expect("static S3 Allow header"),
    );
    insert_s3_cors_headers(response.headers_mut());
    response
}

fn xml_response(status: StatusCode, body: String, head_only: bool) -> Response<PublicBody> {
    let bytes = Bytes::from(body);
    let mut response = if head_only {
        empty_response(status)
    } else {
        response_with_body(status, bytes.clone())
    };
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    if let Ok(length) = HeaderValue::from_str(&bytes.len().to_string()) {
        response.headers_mut().insert(CONTENT_LENGTH, length);
    }
    insert_s3_cors_headers(response.headers_mut());
    response
}

fn xml_element(output: &mut String, name: &str, value: &str, url_encode: bool) {
    let value = if url_encode {
        s3_url_encode(value)
    } else {
        value.to_owned()
    };
    let _ = write!(output, "<{name}>{}</{name}>", xml_escape(&value));
}

fn xml_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '\'' => escaped.push_str("&apos;"),
            '"' => escaped.push_str("&quot;"),
            character => escaped.push(character),
        }
    }
    escaped
}

fn s3_url_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn unix_millis_to_rfc3339(value: u64) -> Option<String> {
    let seconds = i64::try_from(value / 1000).ok()?;
    let days = seconds.div_euclid(86_400);
    let remainder = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = remainder / 3_600;
    let minute = remainder % 3_600 / 60;
    let second = remainder % 60;
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:03}Z",
        value % 1000
    ))
}

fn unix_millis_to_http_date(value: u64) -> Option<String> {
    let seconds = i64::try_from(value / 1000).ok()?;
    let days = seconds.div_euclid(86_400);
    let remainder = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    if !(0..=9999).contains(&year) {
        return None;
    }
    let weekday = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        [usize::try_from((days + 4).rem_euclid(7)).ok()?];
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .get(usize::from(month.checked_sub(1)?))?;
    let hour = remainder / 3_600;
    let minute = remainder % 3_600 / 60;
    let second = remainder % 60;
    Some(format!(
        "{weekday}, {day:02} {month} {year:04} {hour:02}:{minute:02}:{second:02} GMT"
    ))
}

fn civil_from_days(days: i64) -> (i64, u8, u8) {
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month as u8, day as u8)
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct S3ByteRange {
    pub start: u64,
    pub end: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum S3RangeError {
    Invalid,
    Unsatisfiable,
}

/// Parses one RFC 9110 byte range.  S3 reads are intentionally single-range in this slice;
/// callers can map `Invalid` to `InvalidRange` and `Unsatisfiable` to HTTP 416.
#[cfg(test)]
pub(crate) fn parse_s3_range(
    value: &str,
    object_size: u64,
) -> Result<Option<S3ByteRange>, S3RangeError> {
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(S3RangeError::Invalid);
    };
    if spec.is_empty() || spec.contains(',') {
        return Err(S3RangeError::Invalid);
    }
    let Some((start, end)) = spec.split_once('-') else {
        return Err(S3RangeError::Invalid);
    };
    if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| S3RangeError::Invalid)?;
        if suffix == 0 || object_size == 0 {
            return Err(S3RangeError::Unsatisfiable);
        }
        let length = suffix.min(object_size);
        return Ok(Some(S3ByteRange {
            start: object_size - length,
            end: object_size - 1,
        }));
    }
    let start = start.parse::<u64>().map_err(|_| S3RangeError::Invalid)?;
    if start >= object_size {
        return Err(S3RangeError::Unsatisfiable);
    }
    let end = if end.is_empty() {
        object_size - 1
    } else {
        end.parse::<u64>().map_err(|_| S3RangeError::Invalid)?
    };
    if end < start {
        return Err(S3RangeError::Unsatisfiable);
    }
    Ok(Some(S3ByteRange {
        start,
        end: end.min(object_size - 1),
    }))
}

#[cfg(test)]
pub(crate) fn s3_if_none_match_matches(value: &str, current_etag: &str) -> bool {
    let current = current_etag.trim().trim_matches('"');
    value.split(',').map(str::trim).any(|candidate| {
        candidate == "*"
            || candidate
                .strip_prefix("W/")
                .unwrap_or(candidate)
                .trim_matches('"')
                == current
    })
}

async fn serve_static<B>(request: Request<B>, state: &PublicListenerState) -> Response<PublicBody> {
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        let mut response = problem_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "static_method_not_allowed",
            "Only GET and HEAD are supported for the console",
        );
        response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET, HEAD"));
        return response;
    }
    let requested_path = request.uri().path();
    let lookup = match static_path(requested_path) {
        Ok(path) => path,
        Err(_) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "invalid_static_path",
                "The requested path is invalid",
            )
        }
    };
    let root = match fs::canonicalize(&*state.web_root).await {
        Ok(root) => root,
        Err(_) => {
            return problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "web_root_unavailable",
                "The console assets are unavailable",
            )
        }
    };
    let file = match safe_file(&root, &lookup).await {
        Ok(Some(path)) => path,
        Ok(None) if should_spa_fallback(requested_path) => {
            match safe_file(&root, Path::new("index.html")).await {
                Ok(Some(path)) => path,
                Ok(None) => {
                    return problem_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "web_index_unavailable",
                        "The console entrypoint is unavailable",
                    )
                }
                Err(_) => {
                    return problem_response(
                        StatusCode::FORBIDDEN,
                        "invalid_static_path",
                        "The requested path is not allowed",
                    )
                }
            }
        }
        Ok(None) => {
            return problem_response(
                StatusCode::NOT_FOUND,
                "static_not_found",
                "The requested console asset was not found",
            )
        }
        Err(_) => {
            return problem_response(
                StatusCode::FORBIDDEN,
                "invalid_static_path",
                "The requested path is not allowed",
            )
        }
    };
    let metadata = match fs::metadata(&file).await {
        Ok(metadata) if metadata.is_file() => metadata,
        _ => {
            return problem_response(
                StatusCode::NOT_FOUND,
                "static_not_found",
                "The requested console asset was not found",
            )
        }
    };
    let content_type = content_type_for(&file);
    if request.method() == Method::HEAD {
        let mut response = empty_response(StatusCode::OK);
        set_static_headers(response.headers_mut(), content_type, metadata.len());
        return response;
    }
    let contents = match fs::read(&file).await {
        Ok(contents) => contents,
        Err(_) => {
            return problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "static_read_failed",
                "The requested console asset could not be read",
            )
        }
    };
    let mut response = response_with_body(StatusCode::OK, Bytes::from(contents));
    set_static_headers(response.headers_mut(), content_type, metadata.len());
    response
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicRequestBodyError {
    TooLarge,
    Read,
}

async fn collect_bounded_request_body<B>(
    mut body: B,
    max_bytes: usize,
) -> Result<Bytes, PublicRequestBodyError>
where
    B: Body<Data = Bytes> + Unpin,
{
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| PublicRequestBodyError::Read)?;
        let Ok(bytes) = frame.into_data() else {
            continue;
        };
        if collected.len().saturating_add(bytes.len()) > max_bytes {
            return Err(PublicRequestBodyError::TooLarge);
        }
        collected.extend_from_slice(&bytes);
    }
    Ok(Bytes::from(collected))
}

fn request_body_exceeds_declared_limit<B: Body>(request: &Request<B>, max_bytes: usize) -> bool {
    request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > max_bytes as u64)
        || request
            .body()
            .size_hint()
            .upper()
            .is_some_and(|length| length > max_bytes as u64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CentralProxyError {
    RequestBody(PublicRequestBodyError),
    Upstream,
}

async fn proxy_central<B>(request: Request<B>, state: &PublicListenerState) -> Response<PublicBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Error + Send + Sync + 'static,
{
    if request_body_exceeds_declared_limit(&request, state.max_request_bytes) {
        return problem_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "The request body exceeds the configured Gateway limit",
        );
    }
    let Some(base) = state.central_upstream.as_ref() else {
        return problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "central_unavailable",
            "The Central API upstream is not configured",
        );
    };
    let Some(client) = state.central_client.as_ref() else {
        return problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "central_unavailable",
            "The Central API client is not configured",
        );
    };
    let uri = match upstream_uri(base, request.uri()) {
        Ok(uri) => uri,
        Err(_) => {
            return problem_response(
                StatusCode::BAD_GATEWAY,
                "central_upstream_invalid",
                "The Central API upstream address is invalid",
            )
        }
    };
    let (mut parts, body) = request.into_parts();
    parts.uri = uri.clone();
    strip_hop_by_hop_headers(&mut parts.headers);
    if let Some(authority) = uri.authority() {
        if let Ok(host) = HeaderValue::from_str(authority.as_str()) {
            parts.headers.insert(HOST, host);
        }
    }
    parts.headers.remove(CONTENT_LENGTH);
    let response = match tokio::time::timeout(state.request_deadline, async {
        let body = collect_bounded_request_body(body, state.max_request_bytes)
            .await
            .map_err(CentralProxyError::RequestBody)?;
        parts.headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&body.len().to_string())
                .expect("bounded public API body length is a valid header"),
        );
        client
            .request(Request::from_parts(parts, Full::new(body)))
            .await
            .map_err(|_| CentralProxyError::Upstream)
    })
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(CentralProxyError::RequestBody(PublicRequestBodyError::TooLarge))) => {
            return problem_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "The request body exceeds the configured Gateway limit",
            )
        }
        Ok(Err(CentralProxyError::RequestBody(PublicRequestBodyError::Read))) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "request_body_invalid",
                "The request body could not be read",
            )
        }
        Ok(Err(CentralProxyError::Upstream)) => {
            return problem_response(
                StatusCode::BAD_GATEWAY,
                "central_unavailable",
                "The Central API upstream is unavailable",
            )
        }
        Err(_) => {
            return problem_response(
                StatusCode::GATEWAY_TIMEOUT,
                "central_deadline_exceeded",
                "The Central API request deadline was exceeded",
            )
        }
    };
    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop_headers(&mut parts.headers);
    Response::from_parts(
        parts,
        body.map_err(|error| Box::new(error) as BoxError)
            .boxed_unsync(),
    )
}

fn upstream_uri(base: &Url, original: &Uri) -> Result<Uri, ()> {
    let mut target = base.clone();
    target.set_path(original.path());
    target.set_query(original.query());
    target.as_str().parse().map_err(|_| ())
}

fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    for name in [
        HeaderName::from_static("connection"),
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-authenticate"),
        HeaderName::from_static("proxy-authorization"),
        HeaderName::from_static("te"),
        HeaderName::from_static("trailer"),
        HeaderName::from_static("transfer-encoding"),
        HeaderName::from_static("upgrade"),
    ] {
        headers.remove(name);
    }
}

async fn safe_file(root: &Path, relative: &Path) -> Result<Option<PathBuf>, ()> {
    let candidate = root.join(relative);
    let canonical = match fs::canonicalize(&candidate).await {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(()),
    };
    if !canonical.starts_with(root) {
        return Err(());
    }
    Ok(Some(canonical))
}

fn static_path(path: &str) -> Result<PathBuf, ()> {
    let decoded = percent_decode_str(path).decode_utf8().map_err(|_| ())?;
    if !decoded.starts_with('/') {
        return Err(());
    }
    let mut relative = PathBuf::new();
    for component in decoded.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "."
            || component == ".."
            || component.contains('\\')
            || component.as_bytes().contains(&0)
        {
            return Err(());
        }
        relative.push(component);
    }
    if relative.as_os_str().is_empty() {
        relative.push("index.html");
    }
    Ok(relative)
}

fn should_spa_fallback(path: &str) -> bool {
    path.rsplit('/')
        .next()
        .is_none_or(|segment| !segment.contains('.'))
}

fn content_type_for(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") | Some("mjs") => "text/javascript; charset=utf-8",
        Some("json") | Some("map") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("ico") => "image/x-icon",
        Some("webp") => "image/webp",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}

fn set_static_headers(headers: &mut HeaderMap, content_type: &'static str, length: u64) {
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Ok(value) = HeaderValue::from_str(&length.to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
}

fn s3_error_response(
    status: StatusCode,
    code: &'static str,
    message: &str,
    allow: &str,
    head_only: bool,
) -> Response<PublicBody> {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><RequestId>public-s3-gateway</RequestId></Error>",
        xml_escape(code),
        xml_escape(message),
    );
    let body_bytes = Bytes::from(body);
    let mut response = if head_only {
        empty_response(status)
    } else {
        response_with_body(status, body_bytes.clone())
    };
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response.headers_mut().insert(
        ALLOW,
        HeaderValue::from_str(allow).expect("static S3 Allow header"),
    );
    if let Ok(length) = HeaderValue::from_str(&body_bytes.len().to_string()) {
        response.headers_mut().insert(CONTENT_LENGTH, length);
    }
    insert_s3_cors_headers(response.headers_mut());
    response
}

fn insert_s3_cors_headers(headers: &mut HeaderMap) {
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, HEAD, OPTIONS"),
    );
    headers.insert(
        ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type, range, x-amz-content-sha256, x-amz-date, x-amz-security-token"),
    );
}

pub(crate) fn normalize_configured_host(host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.');
    if host.is_empty() || host.len() > MAX_HOST_BYTES || host.chars().any(char::is_whitespace) {
        return None;
    }
    match url::Host::parse(host).ok()? {
        url::Host::Domain(domain) => Some(domain.to_ascii_lowercase()),
        url::Host::Ipv4(address) => Some(address.to_string()),
        url::Host::Ipv6(address) => Some(address.to_string()),
    }
}

fn normalize_request_host(value: &str) -> Option<String> {
    if value.len() > MAX_HOST_BYTES || value.chars().any(char::is_whitespace) {
        return None;
    }
    let value = value.trim().trim_end_matches('.');
    if value.starts_with('[') {
        let end = value.find(']')?;
        return Some(value[1..end].to_ascii_lowercase());
    }
    Some(
        value
            .rsplit_once(':')
            .filter(|(host, port)| {
                !host.contains(':') && port.chars().all(|character| character.is_ascii_digit())
            })
            .map_or_else(
                || value.to_ascii_lowercase(),
                |(host, _)| host.to_ascii_lowercase(),
            ),
    )
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn is_s3_request_host(host: &str, configured: &str) -> bool {
    host == configured
        || host
            .strip_suffix(configured)
            .is_some_and(|bucket| bucket.ends_with('.') && bucket.len() > 1)
}

struct RequestAdmissionBody {
    inner: PublicBody,
    permit: Option<OwnedSemaphorePermit>,
}

impl Body for RequestAdmissionBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = std::pin::Pin::new(&mut self.inner).poll_frame(context);
        if matches!(
            &result,
            std::task::Poll::Ready(None) | std::task::Poll::Ready(Some(Err(_)))
        ) {
            self.permit.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn retain_request_permit(
    response: Response<PublicBody>,
    permit: OwnedSemaphorePermit,
) -> Response<PublicBody> {
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        RequestAdmissionBody {
            inner: body,
            permit: Some(permit),
        }
        .boxed_unsync(),
    )
}

fn empty_response(status: StatusCode) -> Response<PublicBody> {
    response_with_body(status, Bytes::new())
}

fn response_with_body(status: StatusCode, body: Bytes) -> Response<PublicBody> {
    Response::builder()
        .status(status)
        .body(full_body(body))
        .expect("public response metadata must be valid")
}

struct S3StreamingBody {
    receiver: tokio::sync::mpsc::Receiver<Result<Bytes, BoxError>>,
    cancellation: S3StreamCancellation,
    expected_length: u64,
    sent_length: u64,
    finished: bool,
}

impl Body for S3StreamingBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.finished {
            return std::task::Poll::Ready(None);
        }
        match self.receiver.poll_recv(context) {
            std::task::Poll::Ready(Some(Ok(bytes))) => {
                let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                if bytes.len() > neoengram_domain::protocol::S3_READ_FRAME_MAX_BYTES
                    || self.sent_length.saturating_add(length) > self.expected_length
                {
                    self.finished = true;
                    return std::task::Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "S3 object stream exceeded its authorized length",
                    )))));
                }
                self.sent_length += length;
                std::task::Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
            std::task::Poll::Ready(Some(Err(error))) => {
                self.finished = true;
                std::task::Poll::Ready(Some(Err(error)))
            }
            std::task::Poll::Ready(None) => {
                self.finished = true;
                if self.sent_length != self.expected_length {
                    return std::task::Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "S3 object stream ended before its authorized length",
                    )))));
                }
                self.cancellation.disarm();
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished && !self.cancellation.is_armed()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.expected_length.saturating_sub(self.sent_length))
    }
}

fn response_with_stream(status: StatusCode, stream: S3ObjectStream) -> Response<PublicBody> {
    let S3ObjectStream {
        receiver,
        cancellation,
        expected_length,
    } = stream;
    Response::builder()
        .status(status)
        .body(
            S3StreamingBody {
                receiver,
                cancellation,
                expected_length,
                sent_length: 0,
                finished: false,
            }
            .boxed_unsync(),
        )
        .expect("public response metadata must be valid")
}

fn json_response(status: StatusCode, value: serde_json::Value) -> Response<PublicBody> {
    let body = serde_json::to_vec(&value).expect("public JSON response must serialize");
    let mut response = response_with_body(status, Bytes::from(body));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn problem_response(
    status: StatusCode,
    code: &'static str,
    detail: &'static str,
) -> Response<PublicBody> {
    let mut response = json_response(
        status,
        serde_json::json!({
            "type": format!("https://neoengram.dev/problems/{code}"),
            "title": detail,
            "status": status.as_u16(),
            "code": code,
            "detail": detail
        }),
    );
    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("no-store"),
    );
    response
}

fn full_body(body: Bytes) -> PublicBody {
    Full::new(body)
        .map_err(|never: Infallible| match never {})
        .boxed_unsync()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;

    fn test_limits(
        max_connections: usize,
        max_in_flight_requests: usize,
        max_request_bytes: usize,
        request_deadline: Duration,
    ) -> PublicListenerLimits {
        PublicListenerLimits::new(
            max_connections,
            max_in_flight_requests,
            max_request_bytes,
            request_deadline,
        )
    }

    fn test_state(
        limits: PublicListenerLimits,
        central_upstream: Option<Url>,
    ) -> PublicListenerState {
        PublicListenerState::new(
            PublicListenerConfig::new(
                "console.example.com".into(),
                "s3.example.com".into(),
                PathBuf::from("."),
                central_upstream,
                8,
                false,
            ),
            limits,
            Arc::new(AtomicBool::new(false)),
        )
    }

    struct UnknownLengthBody {
        chunks: std::vec::IntoIter<Bytes>,
    }

    impl UnknownLengthBody {
        fn new(chunks: Vec<Bytes>) -> Self {
            Self {
                chunks: chunks.into_iter(),
            }
        }
    }

    impl Body for UnknownLengthBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            std::task::Poll::Ready(self.chunks.next().map(Frame::data).map(Ok))
        }
    }

    #[tokio::test]
    async fn public_connection_admission_closes_excess_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(test_state(
            test_limits(1, 1, 1024, Duration::from_secs(1)),
            None,
        ));
        let connection_admission = Arc::new(Semaphore::new(1));
        let held = connection_admission.clone().try_acquire_owned().unwrap();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let server = tokio::spawn(serve_listener(
            listener,
            None,
            state,
            connection_admission,
            shutdown_receiver,
        ));

        let mut connection = TcpStream::connect(address).await.unwrap();
        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), connection.read(&mut byte))
            .await
            .expect("excess public connection was not closed")
            .unwrap();
        assert_eq!(read, 0);

        drop(held);
        shutdown_sender.send(true).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn health_bypasses_request_admission_and_overload_preserves_error_formats() {
        let state = Arc::new(test_state(
            test_limits(8, 1, 1024, Duration::from_secs(1)),
            None,
        ));
        let held = state.request_admission.clone().try_acquire_owned().unwrap();

        let health = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/health/live")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let console = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/")
                .header(HOST, "console.example.com")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(console.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(console.headers()[CONTENT_TYPE], "application/json");

        let s3 = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/photos?list-type=2")
                .header(HOST, "s3.example.com")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(s3.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(s3.headers()[CONTENT_TYPE], "application/xml");

        drop(held);
        let response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/")
                .header(HOST, "unknown.example.com")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(state.request_admission.available_permits(), 0);
        drop(response);
        assert_eq!(state.request_admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn api_body_limit_rejects_declared_and_unknown_length_bodies() {
        let state = Arc::new(
            test_state(
                test_limits(8, 2, 4, Duration::from_secs(1)),
                Some(Url::parse("http://127.0.0.1:9").unwrap()),
            )
            .with_central_client(CentralHttpClient::new(None).unwrap()),
        );
        let declared = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri("/api/example")
                .header(HOST, "console.example.com")
                .header(CONTENT_LENGTH, "5")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state.clone(),
        )
        .await
        .unwrap();
        assert_eq!(declared.status(), StatusCode::PAYLOAD_TOO_LARGE);
        drop(declared);

        let streamed = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri("/api/example")
                .header(HOST, "console.example.com")
                .body(UnknownLengthBody::new(vec![
                    Bytes::from_static(b"123"),
                    Bytes::from_static(b"45"),
                ]))
                .unwrap(),
            state,
        )
        .await
        .unwrap();
        assert_eq!(streamed.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn central_proxy_uses_the_configured_request_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _socket = socket;
            std::future::pending::<()>().await;
        });
        let state = Arc::new(
            test_state(
                test_limits(8, 1, 1024, Duration::from_millis(30)),
                Some(Url::parse(&format!("http://{address}")).unwrap()),
            )
            .with_central_client(CentralHttpClient::new(None).unwrap()),
        );

        let response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/system/capabilities")
                .header(HOST, "console.example.com")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        upstream.abort();
        let _ = upstream.await;
    }

    #[test]
    fn static_paths_reject_traversal_after_percent_decoding() {
        assert!(static_path("/%2e%2e/secret").is_err());
        assert!(static_path("/assets/%5csecret.js").is_err());
        assert_eq!(static_path("/").unwrap(), PathBuf::from("index.html"));
    }

    #[test]
    fn request_host_normalization_removes_port_and_trailing_dot() {
        assert_eq!(
            normalize_request_host("Console.Example:8443"),
            Some("console.example".into())
        );
        assert_eq!(normalize_request_host("[::1]:8443"), Some("::1".into()));
        assert_eq!(normalize_request_host("bad host"), None);
    }

    #[test]
    fn s3_writes_have_read_only_allow_list() {
        let response = s3_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "write denied",
            "GET, HEAD, OPTIONS",
            false,
        );
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[ALLOW], "GET, HEAD, OPTIONS");
    }

    #[test]
    fn s3_host_accepts_path_style_and_one_bucket_prefix() {
        assert!(is_s3_request_host("s3.example.com", "s3.example.com"));
        assert!(is_s3_request_host(
            "dataset.s3.example.com",
            "s3.example.com"
        ));
        assert!(!is_s3_request_host("evil-s3.example.com", "s3.example.com"));
        assert!(!is_s3_request_host(".s3.example.com", "s3.example.com"));
    }

    #[test]
    fn parses_loopback_ip_path_style_with_a_port_in_the_authority() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("http://127.0.0.1:8080/photos/images%2Fscene.jpg")
            .body(())
            .unwrap();
        let parsed = parse_s3_request(&request, "127.0.0.1", "127.0.0.1").unwrap();
        assert_eq!(parsed.bucket, "photos");
        assert_eq!(
            parsed.operation,
            ParsedS3Operation::GetObject {
                key: "images/scene.jpg".to_owned()
            }
        );
    }

    #[test]
    fn parses_path_style_list_objects_v2_parameters() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com/photos?list-type=2&prefix=2026%2F&delimiter=%2F&max-keys=25&start-after=2026%2F001.jpg&encoding-type=url&fetch-owner=false")
            .body(())
            .unwrap();
        let parsed = parse_s3_request(&request, "s3.example.com", "s3.example.com").unwrap();
        assert_eq!(parsed.bucket, "photos");
        assert_eq!(
            parsed.operation,
            ParsedS3Operation::Control(S3ControlOperation::ListObjectsV2(S3ListObjectsV2Request {
                prefix: "2026/".into(),
                delimiter: Some("/".into()),
                continuation_token: None,
                start_after: Some("2026/001.jpg".into()),
                max_keys: 25,
                encoding_type_url: true,
            }))
        );
    }

    #[tokio::test]
    async fn rejects_fetch_owner_when_owner_identity_is_unavailable() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com/photos?list-type=2&fetch-owner=true")
            .header("authorization", "test-signature")
            .body(())
            .unwrap();

        let error = parse_s3_request(&request, "s3.example.com", "s3.example.com").unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.code, "InvalidArgument");
        assert_eq!(
            error.message,
            "fetch-owner=true is not supported by this endpoint."
        );

        let state = test_state(test_limits(8, 8, 1024, Duration::from_secs(1)), None);
        let response = handle_s3_request(request, "s3.example.com", &state).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .contains("<Code>InvalidArgument</Code>"));
    }

    #[test]
    fn parses_virtual_host_bucket_location_and_head_bucket() {
        let location = Request::builder()
            .method(Method::GET)
            .uri("https://photos.s3.example.com/?location")
            .body(())
            .unwrap();
        assert_eq!(
            parse_s3_request(&location, "photos.s3.example.com", "s3.example.com")
                .unwrap()
                .operation,
            ParsedS3Operation::Control(S3ControlOperation::GetBucketLocation)
        );
        let head = Request::builder()
            .method(Method::HEAD)
            .uri("https://photos.s3.example.com/")
            .body(())
            .unwrap();
        assert_eq!(
            parse_s3_request(&head, "photos.s3.example.com", "s3.example.com")
                .unwrap()
                .operation,
            ParsedS3Operation::Control(S3ControlOperation::HeadBucket)
        );
    }

    #[test]
    fn rejects_ambiguous_or_unsupported_list_parameters() {
        let duplicate = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com/photos?list-type=2&list-type=2")
            .body(())
            .unwrap();
        assert_eq!(
            parse_s3_request(&duplicate, "s3.example.com", "s3.example.com")
                .unwrap_err()
                .code,
            "InvalidArgument"
        );
        let delimiter = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com/photos?list-type=2&delimiter=,")
            .body(())
            .unwrap();
        assert_eq!(
            parse_s3_request(&delimiter, "s3.example.com", "s3.example.com")
                .unwrap_err()
                .code,
            "InvalidArgument"
        );

        let duplicate_leading_slash = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com//photos/file.txt")
            .body(())
            .unwrap();
        assert_eq!(
            parse_s3_request(&duplicate_leading_slash, "s3.example.com", "s3.example.com")
                .unwrap_err()
                .code,
            "InvalidURI"
        );

        let backslash = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com/photos/folder%5Cfile.txt")
            .body(())
            .unwrap();
        assert_eq!(
            parse_s3_request(&backslash, "s3.example.com", "s3.example.com")
                .unwrap_err()
                .code,
            "InvalidArgument"
        );
    }

    #[test]
    fn accepts_common_prefix_start_after_cursor() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com/photos?list-type=2&delimiter=%2F&start-after=folder%2F")
            .body(())
            .unwrap();
        let parsed = parse_s3_request(&request, "s3.example.com", "s3.example.com").unwrap();
        assert!(matches!(
            parsed.operation,
            ParsedS3Operation::Control(S3ControlOperation::ListObjectsV2(
                S3ListObjectsV2Request {
                    start_after: Some(ref value),
                    ..
                }
            )) if value == "folder/"
        ));
    }

    #[tokio::test]
    async fn renders_bounded_list_objects_v2_xml_with_encoding_and_escaping() {
        let request = S3ListObjectsV2Request {
            prefix: "a&/".into(),
            delimiter: Some("/".into()),
            continuation_token: Some("token".into()),
            start_after: None,
            max_keys: 2,
            encoding_type_url: true,
        };
        let result = S3ListObjectsV2Result {
            objects: vec![S3ObjectMetadata {
                key: "a&/one.txt".into(),
                size_bytes: 3,
                etag: "etag\"value".into(),
                last_modified_unix_ms: 1_735_689_600_000,
                content_type: "text/plain".into(),
            }],
            common_prefixes: vec!["a&/nested/".into()],
            next_continuation_token: Some("next".into()),
        };
        let response = s3_list_objects_v2_response("photos", &request, &result).unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/xml");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("<EncodingType>url</EncodingType>"));
        assert!(body.contains("<Prefix>a%26%2F</Prefix>"));
        assert!(body.contains("<Key>a%26%2Fone.txt</Key>"));
        assert!(!body.contains("<Owner>"));
    }

    #[test]
    fn formats_object_last_modified_as_imf_fixdate() {
        assert_eq!(
            unix_millis_to_http_date(1_735_689_600_000).as_deref(),
            Some("Wed, 01 Jan 2025 00:00:00 GMT")
        );
    }

    struct TestS3Backend;

    #[async_trait]
    impl S3ControlBackend for TestS3Backend {
        async fn execute(
            &self,
            request: S3ControlRequest,
        ) -> Result<S3ControlResponse, S3ControlError> {
            assert_eq!(request.bucket, "photos");
            assert_eq!(
                request.authorization.headers["authorization"],
                "test-signature"
            );
            match request.operation {
                S3ControlOperation::HeadBucket | S3ControlOperation::GetBucketLocation => {
                    Ok(S3ControlResponse::Bucket(S3BucketMetadata {
                        region: "ap-northeast-1".into(),
                    }))
                }
                S3ControlOperation::ListObjectsV2(request) => {
                    assert_eq!(request.max_keys, 10);
                    Ok(S3ControlResponse::ListObjectsV2(S3ListObjectsV2Result {
                        objects: vec![S3ObjectMetadata {
                            key: "a.txt".into(),
                            size_bytes: 4,
                            etag: "etag".into(),
                            last_modified_unix_ms: 0,
                            content_type: "text/plain".into(),
                        }],
                        common_prefixes: Vec::new(),
                        next_continuation_token: None,
                    }))
                }
                S3ControlOperation::HeadObject { .. } | S3ControlOperation::GetObject { .. } => {
                    Err(S3ControlError::Internal)
                }
            }
        }
    }

    #[tokio::test]
    async fn handler_adapts_control_metadata_to_list_and_bucket_responses() {
        let state = PublicListenerState::new(
            PublicListenerConfig::new(
                "console.example.com".into(),
                "s3.example.com".into(),
                PathBuf::from("."),
                None,
                8,
                true,
            ),
            PublicListenerLimits::new(8, 8, 1024, Duration::from_secs(1)),
            Arc::new(AtomicBool::new(false)),
        )
        .with_s3_backend(Arc::new(TestS3Backend));
        let list_request = Request::builder()
            .method(Method::GET)
            .uri("https://s3.example.com/photos?list-type=2&max-keys=10")
            .header(HOST, "s3.example.com")
            .header("authorization", "test-signature")
            .body(())
            .unwrap();
        let list_response = handle_s3_request(list_request, "s3.example.com", &state).await;
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = list_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        assert!(std::str::from_utf8(&list_body)
            .unwrap()
            .contains("<Key>a.txt</Key>"));

        let head_request = Request::builder()
            .method(Method::HEAD)
            .uri("https://s3.example.com/photos")
            .header(HOST, "s3.example.com")
            .header("authorization", "test-signature")
            .body(())
            .unwrap();
        let head_response = handle_s3_request(head_request, "s3.example.com", &state).await;
        assert_eq!(head_response.status(), StatusCode::OK);
        assert_eq!(
            head_response.headers()["x-amz-bucket-region"],
            "ap-northeast-1"
        );
    }

    #[tokio::test]
    async fn object_stream_enforces_length_and_cancels_on_early_drop() {
        let (_sender, receiver) = tokio::sync::mpsc::channel(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = cancelled.clone();
        let response = response_with_stream(
            StatusCode::OK,
            S3ObjectStream {
                receiver,
                cancellation: S3StreamCancellation::new(move || {
                    cancel_flag.store(true, Ordering::Release);
                }),
                expected_length: 4,
            },
        );
        drop(response);
        assert!(cancelled.load(Ordering::Acquire));

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender.send(Ok(Bytes::from_static(b"data"))).await.unwrap();
        drop(sender);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = cancelled.clone();
        let response = response_with_stream(
            StatusCode::OK,
            S3ObjectStream {
                receiver,
                cancellation: S3StreamCancellation::new(move || {
                    cancel_flag.store(true, Ordering::Release);
                }),
                expected_length: 4,
            },
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"data")
        );
        assert!(!cancelled.load(Ordering::Acquire));

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender.send(Ok(Bytes::from_static(b"no"))).await.unwrap();
        drop(sender);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = cancelled.clone();
        let response = response_with_stream(
            StatusCode::OK,
            S3ObjectStream {
                receiver,
                cancellation: S3StreamCancellation::new(move || {
                    cancel_flag.store(true, Ordering::Release);
                }),
                expected_length: 3,
            },
        );
        assert!(response.into_body().collect().await.is_err());
        assert!(cancelled.load(Ordering::Acquire));

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(sender);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = cancelled.clone();
        let response = response_with_stream(
            StatusCode::OK,
            S3ObjectStream {
                receiver,
                cancellation: S3StreamCancellation::new(move || {
                    cancel_flag.store(true, Ordering::Release);
                }),
                expected_length: 0,
            },
        );
        assert!(response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty());
        assert!(!cancelled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn stream_lifetime_holds_the_global_s3_admission_permit() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = admission.clone().try_acquire_owned().unwrap();
        let mut cancellation = S3StreamCancellation::new(|| {});
        cancellation.retain_request_permit(permit);

        assert!(admission.clone().try_acquire_owned().is_err());
        drop(cancellation);
        assert!(admission.try_acquire_owned().is_ok());
    }

    #[test]
    fn not_modified_object_response_omits_a_misleading_content_length() {
        let response = s3_object_response(
            S3ObjectResponse {
                metadata: S3ObjectMetadata {
                    key: "a.txt".to_owned(),
                    size_bytes: 100,
                    etag: "etag".to_owned(),
                    last_modified_unix_ms: 0,
                    content_type: "text/plain".to_owned(),
                },
                body: S3ObjectBody::Empty,
                content_length: 0,
                content_range: None,
                status: StatusCode::NOT_MODIFIED,
            },
            false,
            "GET, HEAD, OPTIONS",
            None,
        );
        assert!(!response.headers().contains_key(CONTENT_LENGTH));
    }

    #[tokio::test]
    async fn object_response_uses_authorized_payload_length() {
        let response = s3_object_response(
            S3ObjectResponse {
                metadata: S3ObjectMetadata {
                    key: "large.bin".to_owned(),
                    size_bytes: 100,
                    etag: "etag".to_owned(),
                    last_modified_unix_ms: 0,
                    content_type: "application/octet-stream".to_owned(),
                },
                body: S3ObjectBody::Bytes(Bytes::from_static(b"data")),
                content_length: 4,
                content_range: Some("bytes 0-3/100".to_owned()),
                status: StatusCode::PARTIAL_CONTENT,
            },
            false,
            "GET, HEAD, OPTIONS",
            Some(&HeaderValue::from_static("attachment")),
        );
        assert_eq!(response.headers()[CONTENT_LENGTH], "4");
        assert_eq!(response.headers()["content-disposition"], "attachment");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"data")
        );
    }

    #[test]
    fn range_and_conditional_helpers_follow_s3_single_range_rules() {
        assert_eq!(
            parse_s3_range("bytes=10-", 20).unwrap(),
            Some(S3ByteRange { start: 10, end: 19 })
        );
        assert_eq!(
            parse_s3_range("bytes=-5", 20).unwrap(),
            Some(S3ByteRange { start: 15, end: 19 })
        );
        assert_eq!(
            parse_s3_range("bytes=20-", 20),
            Err(S3RangeError::Unsatisfiable)
        );
        assert!(s3_if_none_match_matches("\"abc\", \"xyz\"", "abc"));
        assert!(!s3_if_none_match_matches("\"xyz\"", "abc"));
    }

    #[test]
    fn response_content_disposition_rejects_ambiguous_or_invalid_values() {
        let valid: Uri =
            "/bucket/key?response-content-disposition=attachment%3B%20filename%3Da.txt"
                .parse()
                .unwrap();
        assert_eq!(
            response_content_disposition(&valid).unwrap().unwrap(),
            "attachment; filename=a.txt"
        );

        let duplicate: Uri =
            "/bucket/key?response-content-disposition=attachment&response-content-disposition=inline"
                .parse()
                .unwrap();
        assert!(response_content_disposition(&duplicate).is_err());

        let injected: Uri = "/bucket/key?response-content-disposition=attachment%0D%0AX-Evil%3Ayes"
            .parse()
            .unwrap();
        assert!(response_content_disposition(&injected).is_err());

        let tab: Uri = "/bucket/key?response-content-disposition=attachment%09filename%3Da.txt"
            .parse()
            .unwrap();
        assert!(response_content_disposition(&tab).is_err());
    }
}
