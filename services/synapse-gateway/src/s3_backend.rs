//! Central authorization adapter for the public S3 listener.
//!
//! The public listener owns HTTP/S3 parsing, while Central owns credential lookup, SigV4
//! verification, access-point policy and snapshot fences.  This module is deliberately a small
//! HTTP client around that private boundary; it never authorizes a request locally.

use std::{sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use http::{
    header::{HeaderValue, ACCEPT, CONTENT_TYPE, HOST},
    Method, Request, StatusCode, Uri,
};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Body as _, Incoming};
use neoengram_domain::protocol::{
    decode_bounded_unique_json, S3AuthorizeOperation, S3AuthorizeRequest, S3AuthorizeResponse,
    S3AuthorizedObject, S3ReadTicket, S3SigV4Request, MAX_METADATA_PAGE_BYTES, S3_AUTHORIZE_PATH,
};
use serde_json::Value;
use url::Url;

use crate::central_http::CentralHttpClient;
use crate::public_listener::{
    BoxError, S3AuthorizationContext, S3BucketMetadata, S3ControlBackend, S3ControlError,
    S3ControlOperation, S3ControlRequest, S3ListObjectsV2Result, S3ObjectBody, S3ObjectMetadata,
    S3ObjectResponse, S3ObjectStream, S3StreamCancellation,
};

const AUTHORIZE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SIGV4_PATH_BYTES: usize = 4096;
const MAX_SIGV4_QUERY_BYTES: usize = 16 * 1024;
const MAX_SIGV4_HEADERS: usize = 128;
const MAX_SIGV4_HEADER_BYTES: usize = 64 * 1024;
const MAX_AUTHORIZE_REQUEST_BYTES: usize = 128 * 1024;
const MAX_AUTHORIZE_ERROR_BYTES: usize = 64 * 1024;
const MAX_OBJECT_READER_BUFFERED_FRAMES: usize = 8;

/// Central's metadata page limit is also the largest response the Gateway will materialize.
/// Object bytes are never carried by this endpoint.
const MAX_AUTHORIZE_RESPONSE_BYTES: usize = MAX_METADATA_PAGE_BYTES;

#[derive(Clone)]
pub(crate) struct CentralS3Backend {
    gateway_pool_id: Arc<str>,
    authorize_uri: Uri,
    client: CentralHttpClient,
    timeout: Duration,
    object_reader: Arc<dyn S3ObjectReader>,
}

impl std::fmt::Debug for CentralS3Backend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CentralS3Backend")
            .field("gateway_pool_id", &self.gateway_pool_id)
            .field("authorize_uri", &self.authorize_uri)
            .field("timeout", &self.timeout)
            .field("object_reader", &"dyn S3ObjectReader")
            .finish_non_exhaustive()
    }
}

impl CentralS3Backend {
    #[cfg(test)]
    pub(crate) fn new(
        gateway_pool_id: impl Into<String>,
        central_upstream: &Url,
    ) -> Result<Self, String> {
        // This constructor is retained for unit tests and local loopback development.  Runtime
        // startup uses `new_with_client`, which always receives the workload mTLS client.
        Self::new_with_client(
            gateway_pool_id,
            central_upstream,
            CentralHttpClient::new(None)?,
        )
    }

    pub(crate) fn new_with_client(
        gateway_pool_id: impl Into<String>,
        central_upstream: &Url,
        client: CentralHttpClient,
    ) -> Result<Self, String> {
        let mut endpoint = central_upstream.clone();
        endpoint.set_path(S3_AUTHORIZE_PATH);
        endpoint.set_query(None);
        let authorize_uri = endpoint
            .as_str()
            .parse::<Uri>()
            .map_err(|error| format!("Central S3 authorization endpoint is invalid: {error}"))?;
        Ok(Self {
            gateway_pool_id: Arc::<str>::from(gateway_pool_id.into()),
            authorize_uri,
            client,
            timeout: AUTHORIZE_TIMEOUT,
            object_reader: Arc::new(UnavailableS3ObjectReader),
        })
    }

    /// Installs the ticket executor used for authorized object GETs. The default reader returns
    /// unavailable, so deployments cannot accidentally turn a ticket into an empty success body.
    pub(crate) fn with_object_reader(mut self, object_reader: Arc<dyn S3ObjectReader>) -> Self {
        self.object_reader = object_reader;
        self
    }

    async fn authorize(
        &self,
        request: S3ControlRequest,
    ) -> Result<S3AuthorizeResponse, S3ControlError> {
        let protocol_request = self.to_protocol_request(&request)?;
        let encoded =
            serde_json::to_vec(&protocol_request).map_err(|_| S3ControlError::Internal)?;
        if encoded.len() > MAX_AUTHORIZE_REQUEST_BYTES {
            return Err(S3ControlError::InvalidArgument(
                "The S3 authorization request is too large.".to_owned(),
            ));
        }
        let outbound = Request::builder()
            .method(Method::POST)
            .uri(self.authorize_uri.clone())
            .header(HOST, central_host(&self.authorize_uri)?)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .body(Full::new(Bytes::from(encoded)))
            .map_err(|_| S3ControlError::Internal)?;
        let response = tokio::time::timeout(self.timeout, self.client.request(outbound))
            .await
            .map_err(|_| S3ControlError::Unavailable)?
            .map_err(|_| S3ControlError::Unavailable)?;
        let status = response.status();
        let max_body = if status.is_success() {
            MAX_AUTHORIZE_RESPONSE_BYTES
        } else {
            MAX_AUTHORIZE_ERROR_BYTES
        };
        let body = read_bounded_body(response.into_body(), max_body).await?;
        if !status.is_success() {
            return Err(map_central_status(status, &body));
        }
        decode_bounded_unique_json::<S3AuthorizeResponse>(&body, MAX_AUTHORIZE_RESPONSE_BYTES)
            .map_err(|_| S3ControlError::Internal)
    }

    fn to_protocol_request(
        &self,
        request: &S3ControlRequest,
    ) -> Result<S3AuthorizeRequest, S3ControlError> {
        Ok(S3AuthorizeRequest {
            gateway_pool_id: self.gateway_pool_id.to_string(),
            bucket: request.bucket.clone(),
            operation: protocol_operation(&request.operation),
            sigv4: sigv4_request(&request.authorization)?,
        })
    }
}

#[async_trait::async_trait]
impl S3ControlBackend for CentralS3Backend {
    async fn execute(
        &self,
        request: S3ControlRequest,
    ) -> Result<crate::public_listener::S3ControlResponse, S3ControlError> {
        let operation = request.operation.clone();
        let response = self.authorize(request).await?;
        map_authorize_response(&operation, response, self.object_reader.as_ref()).await
    }
}

/// A bounded Agent-side object stream plus a cancellation action. The cancellation callback must
/// be non-blocking; implementations can enqueue a cancel frame or spawn the async transport work.
pub(crate) struct S3ObjectRead {
    receiver: tokio::sync::mpsc::Receiver<Result<Bytes, BoxError>>,
    cancellation: S3StreamCancellation,
}

impl S3ObjectRead {
    pub(crate) fn new(
        receiver: tokio::sync::mpsc::Receiver<Result<Bytes, BoxError>>,
        cancellation: impl FnOnce() + Send + 'static,
    ) -> Result<Self, S3ControlError> {
        if receiver.max_capacity() > MAX_OBJECT_READER_BUFFERED_FRAMES {
            cancellation();
            return Err(S3ControlError::Internal);
        }
        Ok(Self {
            receiver,
            cancellation: S3StreamCancellation::new(cancellation),
        })
    }

    fn into_stream(self, expected_length: u64) -> S3ObjectStream {
        S3ObjectStream {
            receiver: self.receiver,
            cancellation: self.cancellation,
            expected_length,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        tokio::sync::mpsc::Receiver<Result<Bytes, BoxError>>,
        S3StreamCancellation,
    ) {
        (self.receiver, self.cancellation)
    }
}

#[async_trait::async_trait]
pub(crate) trait S3ObjectReader: Send + Sync {
    async fn open(&self, ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError>;
}

struct UnavailableS3ObjectReader;

#[async_trait::async_trait]
impl S3ObjectReader for UnavailableS3ObjectReader {
    async fn open(&self, _ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError> {
        Err(S3ControlError::Unavailable)
    }
}

fn protocol_operation(operation: &S3ControlOperation) -> S3AuthorizeOperation {
    match operation {
        S3ControlOperation::HeadBucket => S3AuthorizeOperation::HeadBucket,
        S3ControlOperation::GetBucketLocation => S3AuthorizeOperation::GetBucketLocation,
        S3ControlOperation::ListObjectsV2(request) => S3AuthorizeOperation::ListObjectsV2 {
            prefix: request.prefix.clone(),
            delimiter: request.delimiter.clone(),
            continuation_token: request.continuation_token.clone(),
            start_after: request.start_after.clone(),
            max_keys: request.max_keys,
        },
        S3ControlOperation::HeadObject {
            key,
            range,
            if_match,
            if_none_match,
        } => S3AuthorizeOperation::HeadObject {
            key: key.clone(),
            range: range.clone(),
            if_match: if_match.clone(),
            if_none_match: if_none_match.clone(),
        },
        S3ControlOperation::GetObject {
            key,
            range,
            if_match,
            if_none_match,
        } => S3AuthorizeOperation::GetObject {
            key: key.clone(),
            range: range.clone(),
            if_match: if_match.clone(),
            if_none_match: if_none_match.clone(),
        },
    }
}

fn sigv4_request(authorization: &S3AuthorizationContext) -> Result<S3SigV4Request, S3ControlError> {
    let path = authorization.uri.path().to_owned();
    let query = authorization.uri.query().unwrap_or_default();
    if path.len() > MAX_SIGV4_PATH_BYTES || query.len() > MAX_SIGV4_QUERY_BYTES {
        return Err(S3ControlError::InvalidArgument(
            "The S3 request URI is too large.".to_owned(),
        ));
    }
    let authority_host = if authorization.headers.contains_key(HOST) {
        None
    } else {
        Some(
            authorization
                .uri
                .authority()
                .ok_or(S3ControlError::AccessDenied)?
                .as_str(),
        )
    };
    if authorization.headers.len() + usize::from(authority_host.is_some()) > MAX_SIGV4_HEADERS {
        return Err(S3ControlError::InvalidArgument(
            "The S3 request has too many headers.".to_owned(),
        ));
    }
    let mut headers =
        Vec::with_capacity(authorization.headers.len() + usize::from(authority_host.is_some()));
    let mut header_bytes = 0usize;
    for (name, value) in &authorization.headers {
        let value = value.to_str().map_err(|_| S3ControlError::AccessDenied)?;
        header_bytes = header_bytes
            .checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or(S3ControlError::InvalidArgument(
                "The S3 request headers are too large.".to_owned(),
            ))?;
        if header_bytes > MAX_SIGV4_HEADER_BYTES {
            return Err(S3ControlError::InvalidArgument(
                "The S3 request headers are too large.".to_owned(),
            ));
        }
        headers.push((name.as_str().to_owned(), value.to_owned()));
    }
    if let Some(value) = authority_host {
        header_bytes = header_bytes
            .checked_add(HOST.as_str().len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or(S3ControlError::InvalidArgument(
                "The S3 request headers are too large.".to_owned(),
            ))?;
        if header_bytes > MAX_SIGV4_HEADER_BYTES {
            return Err(S3ControlError::InvalidArgument(
                "The S3 request headers are too large.".to_owned(),
            ));
        }
        headers.push((HOST.as_str().to_owned(), value.to_owned()));
    }
    Ok(S3SigV4Request {
        method: authorization.method.as_str().to_owned(),
        path,
        query: query.to_owned(),
        headers,
    })
}

fn central_host(uri: &Uri) -> Result<HeaderValue, S3ControlError> {
    uri.authority()
        .and_then(|authority| HeaderValue::from_str(authority.as_str()).ok())
        .ok_or(S3ControlError::Internal)
}

async fn map_authorize_response(
    operation: &S3ControlOperation,
    response: S3AuthorizeResponse,
    object_reader: &dyn S3ObjectReader,
) -> Result<crate::public_listener::S3ControlResponse, S3ControlError> {
    match (operation, response) {
        (S3ControlOperation::HeadBucket, S3AuthorizeResponse::Bucket { region })
        | (S3ControlOperation::GetBucketLocation, S3AuthorizeResponse::Bucket { region }) => Ok(
            crate::public_listener::S3ControlResponse::Bucket(S3BucketMetadata { region }),
        ),
        (
            S3ControlOperation::ListObjectsV2(request),
            S3AuthorizeResponse::ListObjectsV2 {
                objects,
                common_prefixes,
                next_continuation_token,
            },
        ) => {
            let returned = objects.len().saturating_add(common_prefixes.len());
            if returned > usize::from(request.max_keys) || returned > 1000 {
                return Err(S3ControlError::Internal);
            }
            Ok(crate::public_listener::S3ControlResponse::ListObjectsV2(
                S3ListObjectsV2Result {
                    objects: objects.into_iter().map(map_object).collect(),
                    common_prefixes,
                    next_continuation_token,
                },
            ))
        }
        (
            S3ControlOperation::HeadObject { range, .. },
            S3AuthorizeResponse::Object {
                object,
                status,
                content_length,
                content_range,
                ticket,
            },
        ) => {
            let status = StatusCode::from_u16(status).map_err(|_| S3ControlError::Internal)?;
            if ticket.is_some()
                || !matches!(
                    status,
                    StatusCode::OK
                        | StatusCode::PARTIAL_CONTENT
                        | StatusCode::NOT_MODIFIED
                        | StatusCode::PRECONDITION_FAILED
                        | StatusCode::RANGE_NOT_SATISFIABLE
                )
                || match status {
                    StatusCode::OK => {
                        range.is_some()
                            || content_length != object.size_bytes
                            || content_range.is_some()
                    }
                    StatusCode::PARTIAL_CONTENT => {
                        range.is_none()
                            || content_length > object.size_bytes
                            || content_range.is_none()
                    }
                    StatusCode::NOT_MODIFIED | StatusCode::PRECONDITION_FAILED => {
                        content_length != 0 || content_range.is_some()
                    }
                    StatusCode::RANGE_NOT_SATISFIABLE => {
                        content_length != 0 || content_range.is_none()
                    }
                    _ => true,
                }
            {
                return Err(S3ControlError::Internal);
            }
            Ok(crate::public_listener::S3ControlResponse::Object(
                S3ObjectResponse {
                    metadata: map_object(object),
                    body: S3ObjectBody::Empty,
                    content_length,
                    content_range,
                    status,
                },
            ))
        }
        (
            S3ControlOperation::GetObject { .. },
            S3AuthorizeResponse::Object {
                object,
                status,
                content_length,
                content_range,
                ticket,
            },
        ) => {
            let status = StatusCode::from_u16(status).map_err(|_| S3ControlError::Internal)?;
            if !matches!(
                status,
                StatusCode::OK
                    | StatusCode::PARTIAL_CONTENT
                    | StatusCode::NOT_MODIFIED
                    | StatusCode::PRECONDITION_FAILED
                    | StatusCode::RANGE_NOT_SATISFIABLE
            ) {
                return Err(S3ControlError::Internal);
            }
            if matches!(status, StatusCode::OK | StatusCode::PARTIAL_CONTENT) {
                let ticket = ticket.ok_or(S3ControlError::Internal)?;
                if !ticket_matches_object(&ticket, &object, content_length)
                    || (status == StatusCode::OK && content_range.is_some())
                    || (status == StatusCode::PARTIAL_CONTENT && content_range.is_none())
                {
                    return Err(S3ControlError::Internal);
                }
                let stream = object_reader
                    .open(*ticket)
                    .await?
                    .into_stream(content_length);
                return Ok(crate::public_listener::S3ControlResponse::Object(
                    S3ObjectResponse {
                        metadata: map_object(object),
                        body: S3ObjectBody::Stream(stream),
                        content_length,
                        content_range,
                        status,
                    },
                ));
            }
            if ticket.is_some() || content_length != 0 {
                return Err(S3ControlError::Internal);
            }
            Ok(crate::public_listener::S3ControlResponse::Object(
                S3ObjectResponse {
                    metadata: map_object(object),
                    body: S3ObjectBody::Empty,
                    content_length,
                    content_range,
                    status,
                },
            ))
        }
        _ => Err(S3ControlError::Internal),
    }
}

fn ticket_matches_object(
    ticket: &S3ReadTicket,
    object: &S3AuthorizedObject,
    content_length: u64,
) -> bool {
    ticket.logical_path == object.key
        && ticket.manifest_id == object.etag
        && ticket.size_bytes == object.size_bytes
        && ticket.allowed_start <= ticket.allowed_end_exclusive
        && ticket
            .allowed_end_exclusive
            .saturating_sub(ticket.allowed_start)
            == content_length
}

fn map_object(object: S3AuthorizedObject) -> S3ObjectMetadata {
    S3ObjectMetadata {
        key: object.key,
        size_bytes: object.size_bytes,
        etag: object.etag.to_string(),
        last_modified_unix_ms: object.last_modified_unix_ms.get(),
        content_type: object.content_type,
    }
}

async fn read_bounded_body(mut body: Incoming, max_bytes: usize) -> Result<Bytes, S3ControlError> {
    if body
        .size_hint()
        .upper()
        .is_some_and(|size| size > u64::try_from(max_bytes).unwrap_or(u64::MAX))
    {
        return Err(S3ControlError::Internal);
    }
    let mut bytes = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| S3ControlError::Unavailable)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if bytes.len().saturating_add(data.len()) > max_bytes {
            return Err(S3ControlError::Internal);
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes.freeze())
}

fn map_central_status(status: StatusCode, body: &[u8]) -> S3ControlError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => S3ControlError::AccessDenied,
        StatusCode::NOT_FOUND => {
            if problem_detail(body) == "S3 object not found" {
                S3ControlError::NoSuchKey
            } else {
                S3ControlError::NoSuchBucket
            }
        }
        StatusCode::BAD_REQUEST | StatusCode::CONFLICT | StatusCode::UNPROCESSABLE_ENTITY => {
            S3ControlError::InvalidArgument(problem_detail(body))
        }
        StatusCode::TOO_MANY_REQUESTS
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => S3ControlError::Unavailable,
        status if status.is_server_error() => S3ControlError::Internal,
        _ => S3ControlError::Internal,
    }
}

fn problem_detail(body: &[u8]) -> String {
    const MAX_DETAIL_CHARS: usize = 1024;
    decode_bounded_unique_json::<Value>(body, MAX_AUTHORIZE_ERROR_BYTES)
        .ok()
        .and_then(|value| {
            value
                .get("detail")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .map(|detail| detail.chars().take(MAX_DETAIL_CHARS).collect())
        .unwrap_or_else(|| "Central rejected the S3 authorization request.".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };

    use http::{header::HeaderName, HeaderMap, HeaderValue};
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use neoengram_domain::protocol::{
        AgentId, ContentDigest, GatewayConnectionId, GatewayOpaqueBytes, GatewayReplicaId,
        MountGeneration, OwnerGeneration, RouteGeneration, SessionGeneration, UnixMillis,
    };
    use tokio::{net::TcpListener, sync::oneshot};

    fn context() -> S3AuthorizationContext {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("s3.example.com"));
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );
        headers.append(
            HeaderName::from_static("x-test"),
            HeaderValue::from_static("first"),
        );
        headers.append(
            HeaderName::from_static("x-test"),
            HeaderValue::from_static("second"),
        );
        S3AuthorizationContext {
            method: Method::GET,
            uri: "/photos/a%20b?x=1&x=2".parse().unwrap(),
            headers,
        }
    }

    fn authorized_object() -> S3AuthorizedObject {
        S3AuthorizedObject {
            key: "a.txt".to_owned(),
            size_bytes: 4,
            etag: ContentDigest::hash(b"etag"),
            last_modified_unix_ms: UnixMillis::new(42),
            content_type: "text/plain".to_owned(),
        }
    }

    fn read_ticket(etag: ContentDigest) -> S3ReadTicket {
        S3ReadTicket {
            ticket_id: "ticket".to_owned(),
            tenant_id: "tenant".to_owned(),
            project_id: "project".to_owned(),
            artifact_id: "artifact".to_owned(),
            snapshot_id: "snapshot".to_owned(),
            snapshot_lifecycle_generation: neoengram_domain::protocol::LifecycleGeneration::new(1),
            commit_id: ContentDigest::hash(b"commit"),
            index_digest: ContentDigest::hash(b"index"),
            bucket: "photos".to_owned(),
            access_point_policy_generation: neoengram_domain::protocol::ResourceVersion::new(1),
            logical_path: "a.txt".to_owned(),
            manifest_id: etag,
            size_bytes: 4,
            allowed_start: 0,
            allowed_end_exclusive: 4,
            gateway_pool_id: "pool".to_owned(),
            owner_replica_id: GatewayReplicaId::new("replica").unwrap(),
            owner_peer_endpoint: "https://replica.peer.example".to_owned(),
            agent_connection_id: GatewayConnectionId::new("agent-connection").unwrap(),
            route_generation: RouteGeneration::new(1),
            agent_id: AgentId::new("agent").unwrap(),
            owner_generation: OwnerGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            session_generation: SessionGeneration::new(1),
            issued_at_unix_ms: UnixMillis::new(1),
            expires_at_unix_ms: UnixMillis::new(2),
            signature: GatewayOpaqueBytes::new(vec![1]).unwrap(),
        }
    }

    struct FixedReader {
        read: Mutex<Option<S3ObjectRead>>,
        opened: AtomicBool,
    }

    #[async_trait::async_trait]
    impl S3ObjectReader for FixedReader {
        async fn open(&self, _ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError> {
            self.opened.store(true, Ordering::Release);
            self.read
                .lock()
                .unwrap()
                .take()
                .ok_or(S3ControlError::Internal)
        }
    }

    #[test]
    fn converts_sigv4_request_without_rebuilding_uri_or_headers() {
        let converted = sigv4_request(&context()).unwrap();
        assert_eq!(converted.method, "GET");
        assert_eq!(converted.path, "/photos/a%20b");
        assert_eq!(converted.query, "x=1&x=2");
        assert_eq!(
            converted
                .headers
                .iter()
                .filter(|(name, _)| name == "x-test")
                .map(|(_, value)| value.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert!(converted
            .headers
            .iter()
            .any(|(name, value)| name == "x-amz-content-sha256" && value == "UNSIGNED-PAYLOAD"));
    }

    #[test]
    fn converts_http2_authority_to_the_sigv4_host_header() {
        let mut authorization = context();
        authorization.headers.remove(HOST);
        authorization.uri = "https://photos.s3.example.com/photos/a%20b?x=1&x=2"
            .parse()
            .unwrap();

        let converted = sigv4_request(&authorization).unwrap();

        assert!(converted
            .headers
            .iter()
            .any(|(name, value)| name == "host" && value == "photos.s3.example.com"));
    }

    #[test]
    fn rejects_sigv4_without_a_host_or_http2_authority() {
        let mut authorization = context();
        authorization.headers.remove(HOST);

        assert_eq!(
            sigv4_request(&authorization).unwrap_err(),
            S3ControlError::AccessDenied
        );
    }

    #[test]
    fn maps_central_statuses_without_leaking_error_details() {
        assert_eq!(
            map_central_status(StatusCode::FORBIDDEN, b"{}"),
            S3ControlError::AccessDenied
        );
        assert_eq!(
            map_central_status(StatusCode::NOT_FOUND, b"{}"),
            S3ControlError::NoSuchBucket
        );
        assert_eq!(
            map_central_status(
                StatusCode::NOT_FOUND,
                br#"{"detail":"S3 object not found"}"#,
            ),
            S3ControlError::NoSuchKey
        );
        assert_eq!(
            map_central_status(StatusCode::SERVICE_UNAVAILABLE, b"{}"),
            S3ControlError::Unavailable
        );
        let detail = map_central_status(
            StatusCode::UNPROCESSABLE_ENTITY,
            br#"{"detail":"invalid prefix"}"#,
        );
        assert_eq!(
            detail,
            S3ControlError::InvalidArgument("invalid prefix".to_owned())
        );
        let conflict = map_central_status(
            StatusCode::CONFLICT,
            br#"{"detail":"cursor is invalid or belongs to another catalog query"}"#,
        );
        assert_eq!(
            conflict,
            S3ControlError::InvalidArgument(
                "cursor is invalid or belongs to another catalog query".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn maps_head_object_metadata_and_rejects_get_without_stream_bridge() {
        let object = authorized_object();
        let response = map_authorize_response(
            &S3ControlOperation::HeadObject {
                key: "a.txt".to_owned(),
                range: None,
                if_match: None,
                if_none_match: None,
            },
            S3AuthorizeResponse::Object {
                object: object.clone(),
                status: 200,
                content_length: 4,
                content_range: None,
                ticket: None,
            },
            &UnavailableS3ObjectReader,
        )
        .await
        .unwrap();
        assert!(matches!(
            response,
            crate::public_listener::S3ControlResponse::Object(_)
        ));
        let etag = object.etag;
        let error = map_authorize_response(
            &S3ControlOperation::GetObject {
                key: "a.txt".to_owned(),
                range: None,
                if_match: None,
                if_none_match: None,
            },
            S3AuthorizeResponse::Object {
                object,
                status: 200,
                content_length: 4,
                content_range: None,
                ticket: Some(Box::new(read_ticket(etag))),
            },
            &UnavailableS3ObjectReader,
        )
        .await
        .unwrap_err();
        assert_eq!(error, S3ControlError::Unavailable);
    }

    #[tokio::test]
    async fn maps_head_object_range_without_opening_a_reader() {
        let object = authorized_object();
        let response = map_authorize_response(
            &S3ControlOperation::HeadObject {
                key: "a.txt".to_owned(),
                range: Some("bytes=1-2".to_owned()),
                if_match: None,
                if_none_match: None,
            },
            S3AuthorizeResponse::Object {
                object,
                status: 206,
                content_length: 2,
                content_range: Some("bytes 1-2/4".to_owned()),
                ticket: None,
            },
            &UnavailableS3ObjectReader,
        )
        .await
        .unwrap();
        let crate::public_listener::S3ControlResponse::Object(object) = response else {
            panic!("expected object response");
        };
        assert_eq!(object.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(object.content_length, 2);
        assert_eq!(object.content_range.as_deref(), Some("bytes 1-2/4"));
        assert!(matches!(object.body, S3ObjectBody::Empty));
    }

    #[tokio::test]
    async fn authorized_get_uses_injected_bounded_reader() {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender.try_send(Ok(Bytes::from_static(b"data"))).unwrap();
        drop(sender);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_flag = cancelled.clone();
        let reader = FixedReader {
            read: Mutex::new(Some(
                S3ObjectRead::new(receiver, move || {
                    cancel_flag.store(true, Ordering::Release);
                })
                .unwrap(),
            )),
            opened: AtomicBool::new(false),
        };
        let object = authorized_object();
        let response = map_authorize_response(
            &S3ControlOperation::GetObject {
                key: "a.txt".to_owned(),
                range: None,
                if_match: None,
                if_none_match: None,
            },
            S3AuthorizeResponse::Object {
                ticket: Some(Box::new(read_ticket(object.etag))),
                object,
                status: 200,
                content_length: 4,
                content_range: None,
            },
            &reader,
        )
        .await
        .unwrap();
        assert!(reader.opened.load(Ordering::Acquire));
        let crate::public_listener::S3ControlResponse::Object(object) = response else {
            panic!("expected object response");
        };
        assert_eq!(object.content_length, 4);
        assert!(matches!(&object.body, S3ObjectBody::Stream(_)));
        drop(object);
        assert!(cancelled.load(Ordering::Acquire));

        let (_sender, receiver) = tokio::sync::mpsc::channel(9);
        assert!(matches!(
            S3ObjectRead::new(receiver, || {}),
            Err(S3ControlError::Internal)
        ));
    }

    #[tokio::test]
    async fn central_round_trip_preserves_authorization_document() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (captured_sender, captured_receiver) = oneshot::channel();
        let captured_sender = Arc::new(Mutex::new(Some(captured_sender)));
        let server_sender = captured_sender.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(move |request: Request<Incoming>| {
                let server_sender = server_sender.clone();
                async move {
                    let (parts, body) = request.into_parts();
                    let body = body.collect().await.unwrap().to_bytes();
                    server_sender
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send((parts, body))
                        .unwrap();
                    let response = serde_json::to_vec(&S3AuthorizeResponse::Bucket {
                        region: "ap-northeast-1".to_owned(),
                    })
                    .unwrap();
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/json")
                            .body(Full::new(Bytes::from(response)))
                            .unwrap(),
                    )
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
        let backend = CentralS3Backend::new(
            "pool-a",
            &format!("http://{address}").parse::<Url>().unwrap(),
        )
        .unwrap();
        let request = S3ControlRequest {
            bucket: "photos".to_owned(),
            operation: S3ControlOperation::HeadBucket,
            authorization: context(),
        };
        let response = backend.execute(request).await.unwrap();
        assert!(matches!(
            response,
            crate::public_listener::S3ControlResponse::Bucket(S3BucketMetadata { ref region })
                if region == "ap-northeast-1"
        ));
        let (parts, body) = captured_receiver.await.unwrap();
        assert_eq!(parts.method, Method::POST);
        assert_eq!(parts.uri.path(), S3_AUTHORIZE_PATH);
        let document: S3AuthorizeRequest =
            decode_bounded_unique_json(&body, MAX_AUTHORIZE_REQUEST_BYTES).unwrap();
        assert_eq!(document.gateway_pool_id, "pool-a");
        assert_eq!(document.sigv4.path, "/photos/a%20b");
        assert_eq!(document.sigv4.query, "x=1&x=2");
        assert_eq!(
            document
                .sigv4
                .headers
                .iter()
                .filter(|(name, _)| name == "x-test")
                .count(),
            2
        );
        drop(backend);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_central_body_is_rejected_before_decode() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(|_request: Request<Incoming>| async move {
                let body = Bytes::from(vec![b'x'; MAX_AUTHORIZE_RESPONSE_BYTES + 1]);
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(body))
                        .unwrap(),
                )
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
        let backend = CentralS3Backend::new(
            "pool-a",
            &format!("http://{address}").parse::<Url>().unwrap(),
        )
        .unwrap();
        let request = S3ControlRequest {
            bucket: "photos".to_owned(),
            operation: S3ControlOperation::HeadBucket,
            authorization: context(),
        };
        assert!(matches!(
            backend.execute(request).await,
            Err(S3ControlError::Internal)
        ));
        drop(backend);
        task.await.unwrap();
    }
}
