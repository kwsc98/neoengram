use std::{collections::VecDeque, fmt, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use tokio::sync::mpsc;

mod control_channel;
mod registry_handler;

pub use registry_handler::{AgentDataPlaneHandler, RegistryAgentApiHandler};

pub const AGENT_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
pub const AGENT_MAX_METADATA_PAGE_BODY_BYTES: usize =
    neoengram_domain::protocol::MAX_METADATA_PAGE_BYTES + AGENT_MAX_REQUEST_BODY_BYTES;
pub const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Fixed operations exposed by the independent action-style Agent API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentAction {
    EnrollmentBootstrap,
    EnrollmentStatusQuery,
    SessionOpen,
    SessionHeartbeatReport,
    JobReportCreate,
    JobMetadataBatchStage,
    JobMetadataPageStage,
    JobIndexPageQuery,
    JobManifestPageQuery,
    SessionClose,
}

impl AgentAction {
    pub fn from_path(path: &str) -> Option<Self> {
        match neoengram_domain::protocol::agent_action_from_path(path)? {
            neoengram_domain::protocol::AgentActionKind::EnrollmentBootstrap => {
                Some(Self::EnrollmentBootstrap)
            }
            neoengram_domain::protocol::AgentActionKind::EnrollmentStatusQuery => {
                Some(Self::EnrollmentStatusQuery)
            }
            neoengram_domain::protocol::AgentActionKind::SessionOpen => Some(Self::SessionOpen),
            neoengram_domain::protocol::AgentActionKind::SessionHeartbeatReport => {
                Some(Self::SessionHeartbeatReport)
            }
            neoengram_domain::protocol::AgentActionKind::JobReportCreate => {
                Some(Self::JobReportCreate)
            }
            neoengram_domain::protocol::AgentActionKind::JobMetadataBatchStage => {
                Some(Self::JobMetadataBatchStage)
            }
            neoengram_domain::protocol::AgentActionKind::JobMetadataPageStage => {
                Some(Self::JobMetadataPageStage)
            }
            neoengram_domain::protocol::AgentActionKind::JobIndexPageQuery => {
                Some(Self::JobIndexPageQuery)
            }
            neoengram_domain::protocol::AgentActionKind::JobManifestPageQuery => {
                Some(Self::JobManifestPageQuery)
            }
            neoengram_domain::protocol::AgentActionKind::SessionClose => Some(Self::SessionClose),
            neoengram_domain::protocol::AgentActionKind::SessionChannelOpen => None,
        }
    }

    const fn registry_kind(self) -> neoengram_domain::protocol::AgentActionKind {
        match self {
            Self::EnrollmentBootstrap => {
                neoengram_domain::protocol::AgentActionKind::EnrollmentBootstrap
            }
            Self::EnrollmentStatusQuery => {
                neoengram_domain::protocol::AgentActionKind::EnrollmentStatusQuery
            }
            Self::SessionOpen => neoengram_domain::protocol::AgentActionKind::SessionOpen,
            Self::SessionHeartbeatReport => {
                neoengram_domain::protocol::AgentActionKind::SessionHeartbeatReport
            }
            Self::JobReportCreate => neoengram_domain::protocol::AgentActionKind::JobReportCreate,
            Self::JobMetadataBatchStage => {
                neoengram_domain::protocol::AgentActionKind::JobMetadataBatchStage
            }
            Self::JobMetadataPageStage => {
                neoengram_domain::protocol::AgentActionKind::JobMetadataPageStage
            }
            Self::JobIndexPageQuery => {
                neoengram_domain::protocol::AgentActionKind::JobIndexPageQuery
            }
            Self::JobManifestPageQuery => {
                neoengram_domain::protocol::AgentActionKind::JobManifestPageQuery
            }
            Self::SessionClose => neoengram_domain::protocol::AgentActionKind::SessionClose,
        }
    }

    #[must_use]
    pub const fn path(self) -> &'static str {
        self.registry_kind().path()
    }

    pub const fn max_body_bytes(self) -> usize {
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
        _input: AgentControlInput,
    ) -> Result<AgentControlChannel, AgentHttpError> {
        Err(AgentHttpError::unavailable())
    }

    /// Opens an Agent channel while atomically acquiring its Central-authoritative Gateway route.
    async fn open_routed_control_channel(
        &self,
        _input: AgentControlInput,
        _route: GatewayAgentRouteContext,
    ) -> Result<RoutedAgentControlChannel, AgentHttpError> {
        Err(AgentHttpError::unavailable())
    }
}

/// Hop-authenticated route context supplied by one verified Gateway control session.
#[derive(Debug, Clone)]
pub struct GatewayAgentRouteContext {
    pub route_request_id: neoengram_domain::protocol::RequestId,
    pub gateway_pool_id: neoengram_domain::protocol::GatewayPoolId,
    pub gateway_replica_id: neoengram_domain::protocol::GatewayReplicaId,
    pub connection_id: neoengram_domain::protocol::GatewayConnectionId,
    pub observed_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    pub lease_expires_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    pub heartbeat_timeout_ms: u64,
}

/// Agent channel and the route committed in the same repository transaction as session open.
pub struct RoutedAgentControlChannel {
    pub channel: AgentControlChannel,
    pub route: crate::AgentRouteLease,
    pub replayed: bool,
    /// Previous authoritative owner replaced by the atomic session/route acquire, if any.
    pub fenced: Option<crate::AgentRouteLease>,
}

impl fmt::Debug for RoutedAgentControlChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutedAgentControlChannel")
            .field("route", &self.route)
            .field("replayed", &self.replayed)
            .field("fenced", &self.fenced)
            .finish_non_exhaustive()
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

    /// Receives the next already-bounded NDJSON frame for a transport adapter.
    pub async fn next_frame(&mut self) -> Option<Bytes> {
        self.frames.recv().await
    }
}

impl fmt::Debug for AgentControlChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentControlChannel")
            .finish_non_exhaustive()
    }
}

/// Transport-neutral, bounded input for one Agent full-duplex control channel.
///
/// Hyper and Gateway tunnel adapters feed raw chunks through [`AgentControlInputWriter`]. The
/// decoder deliberately ignores transport frame boundaries and yields LF-delimited protocol
/// frames, matching the public Agent contract.
pub struct AgentControlInput {
    chunks: mpsc::Receiver<Result<Bytes, AgentHttpError>>,
    decoder: neoengram_domain::protocol::AgentChannelNdjsonDecoder,
    ready: VecDeque<Vec<u8>>,
    finished: bool,
}

/// Producer half for [`AgentControlInput`]. Dropping every writer closes the input stream.
#[derive(Clone)]
pub struct AgentControlInputWriter {
    chunks: mpsc::Sender<Result<Bytes, AgentHttpError>>,
}

pub(crate) enum AgentControlInputTrySendError {
    Full,
    Closed,
}

impl AgentControlInput {
    /// Creates a bounded transport bridge. `capacity` must be positive.
    #[must_use]
    pub fn channel(capacity: usize) -> (AgentControlInputWriter, Self) {
        assert!(
            capacity > 0,
            "Agent control input capacity must be positive"
        );
        let (chunks, receiver) = mpsc::channel(capacity);
        (
            AgentControlInputWriter { chunks },
            Self {
                chunks: receiver,
                decoder: neoengram_domain::protocol::AgentChannelNdjsonDecoder::new(),
                ready: VecDeque::new(),
                finished: false,
            },
        )
    }

    pub(crate) async fn next_line(&mut self) -> Result<Option<Vec<u8>>, AgentHttpError> {
        if let Some(line) = self.ready.pop_front() {
            return Ok(Some(line));
        }
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(chunk) = self.chunks.recv().await else {
                self.finished = true;
                self.decoder
                    .finish()
                    .map_err(|_| AgentHttpError::protocol_invalid())?;
                return Ok(None);
            };
            let data = chunk?;
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

impl AgentControlInputWriter {
    /// Applies bounded backpressure while forwarding one transport chunk.
    pub async fn send(&self, chunk: Bytes) -> Result<(), AgentHttpError> {
        self.chunks
            .send(Ok(chunk))
            .await
            .map_err(|_| AgentHttpError::unavailable())
    }

    /// Attempts one bounded transport write without waiting for a slow Agent consumer.  Gateway
    /// control readers use this boundary so a full per-Agent queue closes only that stream instead
    /// of head-of-line blocking heartbeats and unrelated Agent routes on the same Replica.
    pub(crate) fn try_send(&self, chunk: Bytes) -> Result<(), AgentControlInputTrySendError> {
        match self.chunks.try_send(Ok(chunk)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(AgentControlInputTrySendError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(AgentControlInputTrySendError::Closed),
        }
    }

    /// Terminates the consumer with a sanitized transport failure.
    pub async fn fail(&self, error: AgentHttpError) {
        let _ = self.chunks.send(Err(error)).await;
    }
}

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

    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn detail(&self) -> &'static str {
        self.detail
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    #[must_use]
    pub const fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }
}

#[cfg(test)]
mod action_registry_tests {
    use super::*;
    use neoengram_domain::protocol::{
        AgentActionKind, AgentActionTransport, AGENT_ACTION_REGISTRY,
    };

    #[test]
    fn unary_dispatch_matches_the_shared_agent_registry() {
        for descriptor in AGENT_ACTION_REGISTRY {
            let action = AgentAction::from_path(descriptor.path);
            if descriptor.transport == AgentActionTransport::Http2Ndjson {
                assert_eq!(descriptor.kind, AgentActionKind::SessionChannelOpen);
                assert_eq!(action, None);
            } else {
                let action = action.expect("unary Agent action must have a Central handler");
                assert_eq!(action.path(), descriptor.path);
            }
        }
    }
}
