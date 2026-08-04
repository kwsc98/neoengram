use std::{fmt, time::Duration};

use async_trait::async_trait;
use neoengram_protocol::{
    decode_bounded_unique_json, AgentActionAcceptedResponse, AgentAuthenticatedRequest,
    AgentBootId, AgentBootstrapProof, AgentHeartbeatReportPayload, AgentHeartbeatReportResponse,
    AgentId, AgentIndexPageQueryPayload, AgentIndexPageQueryResponse, AgentInstallationId,
    AgentJobReportCreatePayload, AgentMessageListQueryPayload, AgentMessageListQueryResponse,
    AgentMetadataBatchStagePayload, AgentMetadataPageStagePayload, AgentMetadataStageResponse,
    AgentObjectMissingQueryPayload, AgentObjectMissingQueryResponse, AgentObjectUploadPayload,
    AgentObjectUploadResponse, AgentRequestProof, AgentSessionClosePayload,
    AgentSessionCloseResponse, AgentSessionOpenPayload, AgentSessionOpenResponse,
    Ed25519PublicKeySpki, Ed25519Signature, Extensions, ProtocolVersion, RequestId,
    SessionGeneration, SessionId, UnixMillis, AGENT_JOB_INDEX_PAGE_QUERY_PATH,
    AGENT_JOB_METADATA_BATCH_STAGE_PATH, AGENT_JOB_METADATA_PAGE_STAGE_PATH,
    AGENT_JOB_OBJECT_MISSING_QUERY_PATH, AGENT_JOB_OBJECT_UPLOAD_PATH,
    AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_CLOSE_PATH, AGENT_SESSION_HEARTBEAT_REPORT_PATH,
    AGENT_SESSION_MESSAGE_LIST_QUERY_PATH, AGENT_SESSION_OPEN_PATH,
};
use reqwest::{
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE},
    StatusCode,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::{identity::public_key_spki_der, AgentDaemonError, AgentDaemonResult, AgentSigningKey};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 9 * 1024 * 1024;
const JSON_CONTENT_TYPE: &str = "application/json";
const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";
const REQUEST_ID_HEADER: &str = "x-request-id";

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
}

#[async_trait]
pub trait AgentSessionClient: Send + Sync {
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
    async fn query_missing_objects(
        &self,
        request: &AgentAuthenticatedRequest<AgentObjectMissingQueryPayload>,
    ) -> Result<AgentObjectMissingQueryResponse, AgentSessionClientError>;
    async fn upload_object(
        &self,
        request: &AgentAuthenticatedRequest<AgentObjectUploadPayload>,
    ) -> Result<AgentObjectUploadResponse, AgentSessionClientError>;
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
            .timeout(REQUEST_TIMEOUT)
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
}

#[async_trait]
impl AgentSessionClient for ReqwestAgentSessionClient {
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

    async fn query_missing_objects(
        &self,
        request: &AgentAuthenticatedRequest<AgentObjectMissingQueryPayload>,
    ) -> Result<AgentObjectMissingQueryResponse, AgentSessionClientError> {
        self.post(AGENT_JOB_OBJECT_MISSING_QUERY_PATH, request)
            .await
    }

    async fn upload_object(
        &self,
        request: &AgentAuthenticatedRequest<AgentObjectUploadPayload>,
    ) -> Result<AgentObjectUploadResponse, AgentSessionClientError> {
        self.post(AGENT_JOB_OBJECT_UPLOAD_PATH, request).await
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

fn validate_response_request_id(
    response: &reqwest::Response,
    expected: &RequestId,
) -> Result<(), AgentSessionClientError> {
    let observed = response
        .headers()
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
