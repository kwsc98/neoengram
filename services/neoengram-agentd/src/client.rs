use std::{fmt, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use neoengram_protocol::{
    decode_bounded_unique_json, AgentBootstrapAccepted, AgentBootstrapRequest,
    AgentBootstrapStatusRequest, AgentBootstrapStatusResponse, RequestId,
    AGENT_ENROLLMENT_BOOTSTRAP_PATH, AGENT_ENROLLMENT_STATUS_QUERY_PATH,
};
use reqwest::{
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE},
    StatusCode,
};
use serde::Serialize;
use serde_json::Value;
use url::Url;
use uuid::Uuid;
use zeroize::Zeroize;

use crate::{AgentDaemonError, AgentDaemonResult};

const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_AGENT_BODY_BYTES: usize = 64 * 1024;
const JSON_CONTENT_TYPE: &str = "application/json";
const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";
const REQUEST_ID_HEADER: &str = "x-request-id";

struct ZeroizingBodyOwner {
    bytes: Vec<u8>,
    #[cfg(test)]
    drop_observer: Option<DropObserver>,
}

#[cfg(test)]
type DropObserver = Box<dyn FnOnce(&[u8]) + Send>;

impl ZeroizingBodyOwner {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            #[cfg(test)]
            drop_observer: None,
        }
    }

    #[cfg(test)]
    fn with_drop_observer(bytes: Vec<u8>, drop_observer: DropObserver) -> Self {
        Self {
            bytes,
            drop_observer: Some(drop_observer),
        }
    }
}

impl AsRef<[u8]> for ZeroizingBodyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for ZeroizingBodyOwner {
    fn drop(&mut self) {
        self.bytes.as_mut_slice().zeroize();
        #[cfg(test)]
        if let Some(observer) = self.drop_observer.take() {
            observer(&self.bytes);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollmentClientError {
    retryable: bool,
    message: String,
}

impl EnrollmentClientError {
    #[must_use]
    pub fn new(retryable: bool, message: impl Into<String>) -> Self {
        Self {
            retryable,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

impl fmt::Display for EnrollmentClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for EnrollmentClientError {}

#[async_trait]
pub trait EnrollmentClient: Send + Sync {
    async fn bootstrap(
        &self,
        request: &AgentBootstrapRequest,
    ) -> Result<AgentBootstrapAccepted, EnrollmentClientError>;

    /// Returns `None` only when no candidate exists for a correctly formed preflight lookup.
    async fn status(
        &self,
        request: &AgentBootstrapStatusRequest,
    ) -> Result<Option<AgentBootstrapStatusResponse>, EnrollmentClientError>;
}

#[derive(Debug, Clone)]
pub struct ReqwestEnrollmentClient {
    client: reqwest::Client,
    bootstrap_url: Url,
    status_url: Url,
}

impl ReqwestEnrollmentClient {
    pub fn new(central_endpoint: Url) -> AgentDaemonResult<Self> {
        let bootstrap_url = central_endpoint
            .join(AGENT_ENROLLMENT_BOOTSTRAP_PATH.trim_start_matches('/'))
            .map_err(|error| AgentDaemonError::Configuration(error.to_string()))?;
        let status_url = central_endpoint
            .join(AGENT_ENROLLMENT_STATUS_QUERY_PATH.trim_start_matches('/'))
            .map_err(|error| AgentDaemonError::Configuration(error.to_string()))?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(AGENT_REQUEST_TIMEOUT)
            .timeout(AGENT_REQUEST_TIMEOUT)
            .user_agent(concat!("neoengram-agent/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| AgentDaemonError::Enrollment(error.to_string()))?;
        Ok(Self {
            client,
            bootstrap_url,
            status_url,
        })
    }

    async fn post<T: Serialize>(
        &self,
        url: &Url,
        request: &T,
    ) -> Result<HttpResponse, EnrollmentClientError> {
        let request_id = RequestId::new(format!("agent-request-{}", Uuid::new_v4().simple()))
            .map_err(|error| EnrollmentClientError::new(false, error.to_string()))?;
        let encoded = ZeroizingBodyOwner::new(
            serde_json::to_vec(request)
                .map_err(|error| EnrollmentClientError::new(false, error.to_string()))?,
        );
        if encoded.as_ref().len() > MAX_AGENT_BODY_BYTES {
            return Err(EnrollmentClientError::new(
                false,
                "Agent enrollment request exceeds the 64 KiB transport limit",
            ));
        }
        let encoded = Bytes::from_owner(encoded);
        let response = self
            .client
            .post(url.clone())
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .header(
                ACCEPT,
                format!("{JSON_CONTENT_TYPE}, {PROBLEM_CONTENT_TYPE}"),
            )
            .header(REQUEST_ID_HEADER, request_id.as_str())
            .body(encoded)
            .send()
            .await
            .map_err(|error| {
                EnrollmentClientError::new(true, format!("Agent listener is unavailable: {error}"))
            })?;
        let status = response.status();
        validate_response_request_id(&response, &request_id)?;
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        if let Some(length) = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
        {
            if length > MAX_AGENT_BODY_BYTES {
                return Err(EnrollmentClientError::new(
                    false,
                    "Agent listener response exceeds the 64 KiB transport limit",
                ));
            }
        }
        let body = read_bounded(response).await?;
        Ok(HttpResponse {
            status,
            content_type,
            body,
        })
    }
}

#[async_trait]
impl EnrollmentClient for ReqwestEnrollmentClient {
    async fn bootstrap(
        &self,
        request: &AgentBootstrapRequest,
    ) -> Result<AgentBootstrapAccepted, EnrollmentClientError> {
        let response = self.post(&self.bootstrap_url, request).await?;
        if !response.status.is_success() {
            return Err(remote_error(response));
        }
        require_json_content_type(&response.content_type)?;
        let accepted: AgentBootstrapAccepted =
            decode_bounded_unique_json(&response.body, MAX_AGENT_BODY_BYTES)
                .map_err(|error| EnrollmentClientError::new(false, error.to_string()))?;
        accepted
            .validate()
            .map_err(|error| EnrollmentClientError::new(false, error.to_string()))?;
        if accepted.bootstrap_request_id != request.bootstrap_request_id {
            return Err(EnrollmentClientError::new(
                false,
                "bootstrap response request identity does not match",
            ));
        }
        Ok(accepted)
    }

    async fn status(
        &self,
        request: &AgentBootstrapStatusRequest,
    ) -> Result<Option<AgentBootstrapStatusResponse>, EnrollmentClientError> {
        let response = self.post(&self.status_url, request).await?;
        if !response.status.is_success() {
            let code = problem_code(&response.body);
            if response.status == StatusCode::NOT_FOUND
                || (matches!(
                    response.status,
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                ) && code.as_deref() == Some("AGENT_BOOTSTRAP_DENIED"))
            {
                return Ok(None);
            }
            return Err(remote_error(response));
        }
        require_json_content_type(&response.content_type)?;
        let status = AgentBootstrapStatusResponse::decode_json(&response.body)
            .map_err(|error| EnrollmentClientError::new(false, error.to_string()))?;
        if status.bootstrap_request_id != request.bootstrap_request_id
            || status.installation_id != request.installation_id
        {
            return Err(EnrollmentClientError::new(
                false,
                "bootstrap status response identity does not match",
            ));
        }
        Ok(Some(status))
    }
}

struct HttpResponse {
    status: StatusCode,
    content_type: String,
    body: Vec<u8>,
}

async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>, EnrollmentClientError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        EnrollmentClientError::new(true, format!("Agent listener response failed: {error}"))
    })? {
        if body.len().saturating_add(chunk.len()) > MAX_AGENT_BODY_BYTES {
            return Err(EnrollmentClientError::new(
                false,
                "Agent listener response exceeds the 64 KiB transport limit",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn validate_response_request_id(
    response: &reqwest::Response,
    expected: &RequestId,
) -> Result<(), EnrollmentClientError> {
    let observed = response
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| EnrollmentClientError::new(false, "Agent listener omitted X-Request-ID"))?;
    let observed = RequestId::new(observed).map_err(|_| {
        EnrollmentClientError::new(false, "Agent listener returned invalid X-Request-ID")
    })?;
    if &observed != expected {
        return Err(EnrollmentClientError::new(
            false,
            "Agent listener returned a different X-Request-ID",
        ));
    }
    Ok(())
}

fn require_json_content_type(content_type: &str) -> Result<(), EnrollmentClientError> {
    let media_type = content_type
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default();
    if media_type == JSON_CONTENT_TYPE {
        Ok(())
    } else {
        Err(EnrollmentClientError::new(
            false,
            "Agent listener success response is not application/json",
        ))
    }
}

fn remote_error(response: HttpResponse) -> EnrollmentClientError {
    let code = problem_code(&response.body).unwrap_or_else(|| "UNKNOWN".into());
    let retryable = problem_retryable(&response.body).unwrap_or(
        response.status == StatusCode::TOO_MANY_REQUESTS || response.status.is_server_error(),
    );
    EnrollmentClientError::new(
        retryable,
        format!(
            "Agent listener returned HTTP {} ({code})",
            response.status.as_u16()
        ),
    )
}

fn problem_code(body: &[u8]) -> Option<String> {
    problem_value(body)
        .and_then(|value| value.get("code").and_then(Value::as_str).map(str::to_owned))
}

fn problem_retryable(body: &[u8]) -> Option<bool> {
    problem_value(body).and_then(|value| value.get("retryable").and_then(Value::as_bool))
}

fn problem_value(body: &[u8]) -> Option<Value> {
    decode_bounded_unique_json(body, MAX_AGENT_BODY_BYTES).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    #[test]
    fn request_body_zeroizes_only_after_the_last_bytes_owner_drops() {
        let observed = Arc::new(Mutex::new(None));
        let observed_on_drop = Arc::clone(&observed);
        let owner = ZeroizingBodyOwner::with_drop_observer(
            br#"{"bootstrap_token":"secret-bootstrap-token"}"#.to_vec(),
            Box::new(move |bytes| {
                *observed_on_drop.lock().unwrap() = Some(bytes.to_vec());
            }),
        );
        let body = Bytes::from_owner(owner);
        let last_owner = body.clone();
        assert!(body
            .windows(b"secret-bootstrap-token".len())
            .any(|window| window == b"secret-bootstrap-token"));

        drop(body);
        assert!(observed.lock().unwrap().is_none());
        drop(last_owner);
        let zeroized = observed.lock().unwrap().take().unwrap();
        assert!(!zeroized.is_empty());
        assert!(zeroized.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn problem_parsing_never_exposes_detail_as_the_error_message() {
        let response = HttpResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            content_type: PROBLEM_CONTENT_TYPE.into(),
            body: br#"{"code":"AGENT_BUSY","detail":"secret rejection reason","retryable":true}"#
                .to_vec(),
        };
        let error = remote_error(response);
        assert!(error.retryable());
        assert!(error.to_string().contains("AGENT_BUSY"));
        assert!(!error.to_string().contains("secret rejection reason"));
    }

    #[test]
    fn duplicate_problem_members_are_not_trusted() {
        let body = br#"{"code":"ONE","code":"TWO","retryable":true}"#;
        assert!(problem_code(body).is_none());
        assert!(problem_retryable(body).is_none());
    }

    #[tokio::test]
    async fn reqwest_status_uses_the_agent_route_and_round_trips_request_id() {
        use neoengram_protocol::{
            AgentBootstrapProof, AgentBootstrapStatusRequest, AgentInstallationId,
            AgentSignatureAlgorithm, Ed25519PublicKeySpki, Ed25519Signature, Extensions, TenantId,
            UnixMillis, PROTOCOL_VERSION_V1,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .unwrap();
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            assert!(headers.starts_with("POST /agent/enrollment/status/query HTTP/1.1\r\n"));
            let request_id = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case(REQUEST_ID_HEADER)
                        .then(|| value.trim().to_owned())
                })
                .unwrap();
            AgentBootstrapStatusRequest::decode_json(&request[header_end + 4..]).unwrap();
            let body = br#"{"type":"urn:neoengram:problem:agent-bootstrap-denied","title":"Denied","status":403,"code":"AGENT_BOOTSTRAP_DENIED","request_id":"ignored","retryable":false}"#;
            let response = format!(
                "HTTP/1.1 403 Forbidden\r\ncontent-type: {PROBLEM_CONTENT_TYPE}\r\nx-request-id: {request_id}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let client =
            ReqwestEnrollmentClient::new(Url::parse(&format!("http://{address}/")).unwrap())
                .unwrap();
        let request = AgentBootstrapStatusRequest {
            protocol_version: PROTOCOL_VERSION_V1,
            tenant_id: TenantId::new("tenant-a").unwrap(),
            bootstrap_request_id: RequestId::new("bootstrap-a").unwrap(),
            installation_id: AgentInstallationId::new("installation-a").unwrap(),
            signed_at_unix_ms: UnixMillis::new(1),
            proof: AgentBootstrapProof {
                algorithm: AgentSignatureAlgorithm::Ed25519,
                public_key_spki: Ed25519PublicKeySpki::from_public_key_bytes([7; 32]),
                signature: Ed25519Signature::from_bytes([0; 64]),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
        assert!(client.status(&request).await.unwrap().is_none());
        server.await.unwrap();
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "HTTP request ended before its declared body");
            request.extend_from_slice(&chunk[..read]);
            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if request.len() >= header_end + 4 + content_length {
                return request;
            }
        }
    }
}
