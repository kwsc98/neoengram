use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use hyper::body::Incoming;
use neoengram_protocol::{
    decode_bounded_unique_json, AgentActionAcceptedResponse, AgentAuthenticatedRequest,
    AgentBootstrapRequest, AgentBootstrapStatusRequest, AgentChannelAck,
    AgentChannelDownstreamFrame, AgentChannelDownstreamMessage, AgentChannelUpstreamFrame,
    AgentChannelUpstreamMessage, AgentHeartbeatReportPayload, AgentHeartbeatReportResponse,
    AgentId, AgentIndexPageQueryPayload, AgentIndexPageQueryResponse, AgentInstallationId,
    AgentJobReportCreatePayload, AgentManifestPageQueryPayload, AgentManifestPageQueryResponse,
    AgentMessageListQueryPayload, AgentMessageListQueryResponse, AgentMetadataBatchStagePayload,
    AgentMetadataPageStagePayload, AgentMetadataStageResponse, AgentSessionClosePayload,
    AgentSessionCloseResponse, AgentSessionOpenPayload, AgentSessionOpenResponse, ControlError,
    ControlMessage, DecimalU64, ErrorCode, Extensions, MessageId, ProtocolVersion, SequenceNumber,
    SessionGeneration, SessionId, UnixMillis, AGENT_JOB_INDEX_PAGE_QUERY_PATH,
    AGENT_JOB_MANIFEST_PAGE_QUERY_PATH, AGENT_JOB_METADATA_BATCH_STAGE_PATH,
    AGENT_JOB_METADATA_PAGE_STAGE_PATH, AGENT_JOB_REPORT_CREATE_PATH,
    AGENT_SESSION_CHANNEL_OPEN_PATH, AGENT_SESSION_CLOSE_PATH, AGENT_SESSION_HEARTBEAT_REPORT_PATH,
    AGENT_SESSION_MESSAGE_LIST_QUERY_PATH, AGENT_SESSION_OPEN_PATH, MAX_AGENT_POLL_MESSAGES,
};
use neoengramd::{
    AgentRegistryService, BootstrapStorageEnrollmentRequest, CentralError, CentralErrorCode,
    CloseAgentSessionRequest, ControlPlane, MetadataBatchSubmission, OpenAgentSessionRequest,
    ReceiveReportRequest, StageMetadataBatchRequest,
};

use super::{
    control_channel::LiveAgentChannels, AgentAction, AgentApiHandler, AgentControlChannel,
    AgentHttpError, AgentNdjsonInput, AGENT_MAX_REQUEST_BODY_BYTES,
};

const AGENT_ACTION_MAX_CLOCK_SKEW_MS: u64 = 60_000;
const AGENT_CHANNEL_DELIVERY_INTERVAL: Duration = Duration::from_millis(500);
const AGENT_CHANNEL_REDELIVERY_INTERVAL: Duration = Duration::from_secs(5);
const AGENT_CHANNEL_RESPONSE_BUFFER: usize = MAX_AGENT_POLL_MESSAGES * 2;

/// Extension point for the filesystem-backed Index and object data plane.
///
/// The registry handler authenticates and fences every request before invoking this port.
#[async_trait]
pub trait AgentDataPlaneHandler: Send + Sync + 'static {
    async fn query_index_page(
        &self,
        request: AgentAuthenticatedRequest<AgentIndexPageQueryPayload>,
    ) -> Result<AgentIndexPageQueryResponse, AgentHttpError>;

    async fn query_manifest_page(
        &self,
        request: AgentAuthenticatedRequest<AgentManifestPageQueryPayload>,
    ) -> Result<AgentManifestPageQueryResponse, AgentHttpError>;
}

/// Hyper adapter that shares the process-wide registry and control-plane services.
#[derive(Clone)]
pub struct RegistryAgentApiHandler {
    registry: Arc<AgentRegistryService>,
    control: Option<Arc<ControlPlane>>,
    data_plane: Option<Arc<dyn AgentDataPlaneHandler>>,
    accepting: Arc<AtomicBool>,
    live_channels: LiveAgentChannels,
}

#[derive(Clone)]
struct EstablishedAgentChannel {
    agent_id: AgentId,
    installation_id: AgentInstallationId,
    boot_id: neoengram_protocol::AgentBootId,
    session_id: SessionId,
    session_generation: SessionGeneration,
}

enum AppliedChannelFrame {
    Continue(AgentChannelAck),
    Close(AgentChannelAck),
}

/// Compatibility name retained while the composition root migrates to `RegistryAgentApiHandler`.
pub type RegistryAgentEnrollmentHandler = RegistryAgentApiHandler;

impl RegistryAgentApiHandler {
    #[must_use]
    pub fn new(registry: Arc<AgentRegistryService>) -> Self {
        Self {
            registry,
            control: None,
            data_plane: None,
            accepting: Arc::new(AtomicBool::new(true)),
            live_channels: LiveAgentChannels::default(),
        }
    }

    #[must_use]
    pub fn with_readiness(registry: Arc<AgentRegistryService>, accepting: Arc<AtomicBool>) -> Self {
        Self {
            registry,
            control: None,
            data_plane: None,
            accepting,
            live_channels: LiveAgentChannels::default(),
        }
    }

    #[must_use]
    pub fn with_transport(
        registry: Arc<AgentRegistryService>,
        control: Arc<ControlPlane>,
        data_plane: Arc<dyn AgentDataPlaneHandler>,
        accepting: Arc<AtomicBool>,
    ) -> Self {
        Self {
            registry,
            control: Some(control),
            data_plane: Some(data_plane),
            accepting,
            live_channels: LiveAgentChannels::default(),
        }
    }

    fn require_control(&self) -> Result<&ControlPlane, AgentHttpError> {
        self.control
            .as_deref()
            .ok_or_else(AgentHttpError::unavailable)
    }

    fn require_data_plane(&self) -> Result<&dyn AgentDataPlaneHandler, AgentHttpError> {
        self.data_plane
            .as_deref()
            .ok_or_else(AgentHttpError::unavailable)
    }

    async fn authenticate<T: serde::Serialize>(
        &self,
        request: &AgentAuthenticatedRequest<T>,
        path: &'static str,
    ) -> Result<neoengramd::AgentRegistryRecord, AgentHttpError> {
        self.registry
            .authenticate_agent_action(
                request,
                path,
                path != AGENT_SESSION_OPEN_PATH,
                AGENT_ACTION_MAX_CLOCK_SKEW_MS,
            )
            .await
            .map_err(map_registry_error)
    }

    async fn bootstrap(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request = AgentBootstrapRequest::decode_json(body)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let result = self
            .registry
            .bootstrap_storage_enrollment(BootstrapStorageEnrollmentRequest { request })
            .await
            .map_err(map_registry_error)?;
        encode(&result.accepted)
    }

    async fn bootstrap_status(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request = AgentBootstrapStatusRequest::decode_json(body)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let response = self
            .registry
            .bootstrap_status_with_clock_skew(request, AGENT_ACTION_MAX_CLOCK_SKEW_MS)
            .await
            .map_err(map_registry_error)?;
        encode(&response)
    }

    async fn session_open(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentSessionOpenPayload> = decode(body)?;
        let authenticated = self.authenticate(&request, AGENT_SESSION_OPEN_PATH).await?;
        let result = self
            .registry
            .open_session(OpenAgentSessionRequest {
                agent_id: request.agent_id.clone(),
                installation_id: request.installation_id.clone(),
                boot_id: request.boot_id.clone(),
                mount_identity_digest: request.payload.mount_identity_digest,
                expected_resource_version: request.payload.expected_resource_version,
            })
            .await
            .map_err(map_registry_error)?;
        let opened_at = result
            .record
            .instance
            .as_ref()
            .and_then(|instance| instance.session_opened_at_unix_ms)
            .ok_or_else(AgentHttpError::unavailable)?;
        debug_assert_eq!(
            authenticated.enrollment.reserved_agent_id,
            result.record.enrollment.reserved_agent_id
        );
        encode(&AgentSessionOpenResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            agent_id: request.agent_id,
            session_id: result.session_id,
            session_generation: result.session_generation,
            agent_mount_id: result.record.mount.agent_mount_id.clone(),
            mount_generation: result.record.mount.mount_generation,
            owner_generation: result.record.owner.owner_generation,
            resource_version: result.record.resource_version,
            opened_at_unix_ms: opened_at,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn heartbeat_report(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentHeartbeatReportPayload> = decode(body)?;
        request
            .payload
            .heartbeat
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_SESSION_HEARTBEAT_REPORT_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        let report = &request.payload.mount_report;
        if request.payload.heartbeat.agent_id != request.agent_id
            || report.agent_id != request.agent_id
            || report.installation_id != request.installation_id
            || report.boot_id != request.boot_id
            || report.session_generation != generation
            || report.sequence != request.payload.heartbeat.sequence
        {
            return Err(AgentHttpError::session_fenced());
        }
        let result = self
            .registry
            .report_mount(request.payload.mount_report)
            .await
            .map_err(map_registry_error)?;
        let received_at = result
            .record
            .instance
            .as_ref()
            .and_then(|instance| instance.last_heartbeat_at_unix_ms)
            .ok_or_else(AgentHttpError::unavailable)?;
        encode(&AgentHeartbeatReportResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            resource_version: result.record.resource_version,
            received_at_unix_ms: received_at,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn message_list_query(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentMessageListQueryPayload> = decode(body)?;
        request
            .payload
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_SESSION_MESSAGE_LIST_QUERY_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        let messages = self
            .require_control()?
            .poll_agent_messages(
                &request.agent_id,
                generation,
                usize::from(request.payload.max_messages),
            )
            .await
            .map_err(map_registry_error)?;
        encode(&AgentMessageListQueryResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            messages,
            retry_after_ms: DecimalU64::new(1_000),
            extensions: Extensions::new(),
        })
    }

    async fn job_report_create(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentJobReportCreatePayload> = decode(body)?;
        self.authenticate(&request, AGENT_JOB_REPORT_CREATE_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        request
            .payload
            .report
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        if request.payload.report.session_generation != generation {
            return Err(AgentHttpError::session_fenced());
        }
        let report = match request.payload.report.message {
            ControlMessage::Accepted(value) => neoengramd::AgentReport::Accepted(value),
            ControlMessage::Progress(value) => neoengramd::AgentReport::Progress(value),
            ControlMessage::Prepared(value) => neoengramd::AgentReport::Prepared(value),
            ControlMessage::Failed(value) => neoengramd::AgentReport::Failed(value),
            ControlMessage::Finalized(value) => neoengramd::AgentReport::Finalized(value),
            _ => return Err(AgentHttpError::protocol_invalid()),
        };
        let result = self
            .require_control()?
            .receive_session_report(
                ReceiveReportRequest {
                    tenant_id: request.payload.tenant_id,
                    agent_id: request.agent_id,
                    report,
                },
                generation,
            )
            .await
            .map_err(map_registry_error)?;
        encode(&AgentActionAcceptedResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            resource_version: result.job.resource_version,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn metadata_batch_stage(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentMetadataBatchStagePayload> = decode(body)?;
        self.authenticate(&request, AGENT_JOB_METADATA_BATCH_STAGE_PATH)
            .await?;
        request
            .payload
            .descriptor
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let result = self
            .require_control()?
            .stage_metadata_batch(StageMetadataBatchRequest {
                tenant_id: request.payload.tenant_id,
                job_id: request.payload.job_id,
                agent_id: request.agent_id,
                submission: MetadataBatchSubmission::Descriptor(request.payload.descriptor),
            })
            .await
            .map_err(map_registry_error)?;
        encode(&AgentMetadataStageResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            batch_id: result.batch_id,
            complete: result.complete,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn metadata_page_stage(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentMetadataPageStagePayload> = decode(body)?;
        self.authenticate(&request, AGENT_JOB_METADATA_PAGE_STAGE_PATH)
            .await?;
        request
            .payload
            .page
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let result = self
            .require_control()?
            .stage_metadata_batch(StageMetadataBatchRequest {
                tenant_id: request.payload.tenant_id,
                job_id: request.payload.job_id,
                agent_id: request.agent_id,
                submission: MetadataBatchSubmission::Page(request.payload.page),
            })
            .await
            .map_err(map_registry_error)?;
        encode(&AgentMetadataStageResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            batch_id: result.batch_id,
            complete: result.complete,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn index_page_query(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentIndexPageQueryPayload> = decode(body)?;
        request
            .payload
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_JOB_INDEX_PAGE_QUERY_PATH)
            .await?;
        encode(&self.require_data_plane()?.query_index_page(request).await?)
    }

    async fn manifest_page_query(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentManifestPageQueryPayload> = decode(body)?;
        request
            .payload
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_JOB_MANIFEST_PAGE_QUERY_PATH)
            .await?;
        encode(
            &self
                .require_data_plane()?
                .query_manifest_page(request)
                .await?,
        )
    }

    async fn session_close(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentSessionClosePayload> = decode(body)?;
        self.authenticate(&request, AGENT_SESSION_CLOSE_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        let record = self
            .registry
            .close_session(CloseAgentSessionRequest {
                agent_id: request.agent_id,
                boot_id: request.boot_id,
                session_generation: generation,
                expected_resource_version: request.payload.expected_resource_version,
            })
            .await
            .map_err(map_registry_error)?;
        encode(&AgentSessionCloseResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            resource_version: record.resource_version,
            closed_at_unix_ms: record
                .storage_enrollment
                .updated_at_unix_ms
                .unwrap_or_else(|| UnixMillis::new(0)),
            extensions: Extensions::new(),
        })
    }

    async fn control_channel_open(
        &self,
        body: Incoming,
    ) -> Result<AgentControlChannel, AgentHttpError> {
        let mut input = AgentNdjsonInput::new(body);
        let line = input
            .next_line()
            .await?
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        let frame = AgentChannelUpstreamFrame::decode_json(&line)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        frame
            .validate_sequence_after(None)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let AgentChannelUpstreamMessage::Open(open) = &frame.request.payload.message else {
            return Err(AgentHttpError::protocol_invalid());
        };
        self.registry
            .authenticate_agent_action(
                &frame.request,
                AGENT_SESSION_CHANNEL_OPEN_PATH,
                false,
                AGENT_ACTION_MAX_CLOCK_SKEW_MS,
            )
            .await
            .map_err(map_registry_error)?;
        let registration_fence = self
            .live_channels
            .acquire_fence(frame.request.agent_id.clone())
            .await;
        let opened = self
            .registry
            .open_session(OpenAgentSessionRequest {
                agent_id: frame.request.agent_id.clone(),
                installation_id: frame.request.installation_id.clone(),
                boot_id: frame.request.boot_id.clone(),
                mount_identity_digest: open.mount_identity_digest,
                expected_resource_version: open.expected_resource_version,
            })
            .await
            .map_err(map_registry_error)?;
        let opened_at_unix_ms = opened
            .record
            .instance
            .as_ref()
            .and_then(|instance| instance.session_opened_at_unix_ms)
            .ok_or_else(AgentHttpError::unavailable)?;
        let context = EstablishedAgentChannel {
            agent_id: frame.request.agent_id.clone(),
            installation_id: frame.request.installation_id.clone(),
            boot_id: frame.request.boot_id.clone(),
            session_id: opened.session_id.clone(),
            session_generation: opened.session_generation,
        };
        let response = AgentSessionOpenResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: frame.request.request_id.clone(),
            agent_id: context.agent_id.clone(),
            session_id: context.session_id.clone(),
            session_generation: context.session_generation,
            agent_mount_id: opened.record.mount.agent_mount_id.clone(),
            mount_generation: opened.record.mount.mount_generation,
            owner_generation: opened.record.owner.owner_generation,
            resource_version: opened.record.resource_version,
            opened_at_unix_ms,
            replayed: opened.replayed,
            extensions: Extensions::new(),
        };
        let (output, receiver) = tokio::sync::mpsc::channel(AGENT_CHANNEL_RESPONSE_BUFFER);
        let mut downstream_sequence = 0_u64;
        let open_message_id = frame.request.payload.message_id.clone();
        let opened_message_id = channel_message_id(
            &context,
            "opened",
            Some(&open_message_id),
            downstream_sequence.saturating_add(1),
        )?;
        self.send_channel_message(
            &output,
            &context,
            &mut downstream_sequence,
            opened_message_id,
            Some(open_message_id),
            opened_at_unix_ms,
            AgentChannelDownstreamMessage::Opened(response),
        )
        .await?;
        let registration = self
            .live_channels
            .register_fenced(
                &registration_fence,
                context.session_id.clone(),
                context.session_generation,
            )
            .await;
        drop(registration_fence);
        let handler = self.clone();
        tokio::spawn(async move {
            let registration_id = registration.registration_id;
            let mut replaced = registration.replaced;
            handler
                .run_control_channel(
                    &mut input,
                    &output,
                    &context,
                    registration_id,
                    &mut replaced,
                    &mut downstream_sequence,
                )
                .await;
            handler
                .live_channels
                .unregister(&context.agent_id, registration_id)
                .await;
        });
        Ok(AgentControlChannel::new(receiver))
    }

    async fn run_control_channel(
        &self,
        input: &mut AgentNdjsonInput<Incoming>,
        output: &tokio::sync::mpsc::Sender<Bytes>,
        context: &EstablishedAgentChannel,
        registration_id: u64,
        replaced: &mut tokio::sync::watch::Receiver<bool>,
        downstream_sequence: &mut u64,
    ) {
        let mut previous_upstream = SequenceNumber::new(1);
        let mut last_delivered = BTreeMap::new();
        let mut delivery = tokio::time::interval(AGENT_CHANNEL_DELIVERY_INTERVAL);
        delivery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if !self.accepting.load(Ordering::Acquire) || output.is_closed() {
                break;
            }
            tokio::select! {
                changed = replaced.changed() => {
                    if changed.is_err() || *replaced.borrow() {
                        break;
                    }
                }
                line = input.next_line() => {
                    let line = match line {
                        Ok(Some(line)) => line,
                        Ok(None) => break,
                        Err(error) => {
                            self.send_channel_error(
                                output,
                                context,
                                downstream_sequence,
                                None,
                                error,
                            ).await;
                            break;
                        }
                    };
                    let frame = match AgentChannelUpstreamFrame::decode_json(&line) {
                        Ok(frame) => frame,
                        Err(_) => {
                            self.send_channel_error(
                                output,
                                context,
                                downstream_sequence,
                                None,
                                AgentHttpError::protocol_invalid(),
                            ).await;
                            break;
                        }
                    };
                    let correlation = frame.request.payload.message_id.clone();
                    let Some(registration_fence) = self
                        .live_channels
                        .enter_current(&context.agent_id, registration_id)
                        .await
                    else {
                        self.send_channel_error(
                            output,
                            context,
                            downstream_sequence,
                            Some(correlation),
                            AgentHttpError::session_fenced(),
                        ).await;
                        break;
                    };
                    if let Err(error) = self
                        .authenticate_channel_frame(context, &frame)
                        .await
                    {
                        drop(registration_fence);
                        self.send_channel_error(
                            output,
                            context,
                            downstream_sequence,
                            Some(correlation),
                            error,
                        ).await;
                        break;
                    }
                    if frame.validate_sequence_after(Some(previous_upstream)).is_err()
                        || frame
                            .validate_session_generation(context.session_generation)
                            .is_err()
                    {
                        drop(registration_fence);
                        self.send_channel_error(
                            output,
                            context,
                            downstream_sequence,
                            Some(correlation),
                            AgentHttpError::session_fenced(),
                        ).await;
                        break;
                    }
                    let applied = self.apply_channel_frame(context, &frame).await;
                    drop(registration_fence);
                    match applied {
                        Ok(AppliedChannelFrame::Continue(ack)) => {
                            previous_upstream = frame.request.payload.sequence;
                            if self.send_channel_ack(
                                output,
                                context,
                                downstream_sequence,
                                correlation,
                                ack,
                            ).await.is_err() {
                                break;
                            }
                        }
                        Ok(AppliedChannelFrame::Close(ack)) => {
                            let _ = self.send_channel_ack(
                                output,
                                context,
                                downstream_sequence,
                                correlation,
                                ack,
                            ).await;
                            break;
                        }
                        Err(error) => {
                            self.send_channel_error(
                                output,
                                context,
                                downstream_sequence,
                                Some(correlation),
                                error,
                            ).await;
                            break;
                        }
                    }
                }
                _ = delivery.tick() => {
                    if let Err(error) = self
                        .deliver_channel_messages(
                            output,
                            context,
                            registration_id,
                            downstream_sequence,
                            &mut last_delivered,
                        )
                        .await
                    {
                        tracing::warn!(
                            agent_id = %context.agent_id,
                            session_generation = context.session_generation.get(),
                            code = error.code,
                            "Agent reverse-channel delivery pass failed"
                        );
                        if output.is_closed() || error.status != StatusCode::SERVICE_UNAVAILABLE {
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn apply_channel_frame(
        &self,
        context: &EstablishedAgentChannel,
        frame: &AgentChannelUpstreamFrame,
    ) -> Result<AppliedChannelFrame, AgentHttpError> {
        let sequence = frame.request.payload.sequence;
        let applied = match &frame.request.payload.message {
            AgentChannelUpstreamMessage::Heartbeat(payload) => {
                if payload.heartbeat.sequence != payload.mount_report.sequence {
                    return Err(AgentHttpError::session_fenced());
                }
                let result = self
                    .registry
                    .report_mount(payload.mount_report.clone())
                    .await
                    .map_err(map_registry_error)?;
                AppliedChannelFrame::Continue(AgentChannelAck {
                    acknowledged_sequence: sequence,
                    resource_version: result.record.resource_version,
                    replayed: result.replayed,
                    extensions: Extensions::new(),
                })
            }
            AgentChannelUpstreamMessage::Report(payload) => {
                let report = match &payload.report.message {
                    ControlMessage::Accepted(value) => {
                        neoengramd::AgentReport::Accepted(value.clone())
                    }
                    ControlMessage::Progress(value) => {
                        neoengramd::AgentReport::Progress(value.clone())
                    }
                    ControlMessage::Prepared(value) => {
                        neoengramd::AgentReport::Prepared(value.clone())
                    }
                    ControlMessage::Failed(value) => neoengramd::AgentReport::Failed(value.clone()),
                    ControlMessage::Finalized(value) => {
                        neoengramd::AgentReport::Finalized(value.clone())
                    }
                    _ => return Err(AgentHttpError::protocol_invalid()),
                };
                let result = self
                    .require_control()?
                    .receive_session_report(
                        ReceiveReportRequest {
                            tenant_id: payload.tenant_id.clone(),
                            agent_id: context.agent_id.clone(),
                            report,
                        },
                        context.session_generation,
                    )
                    .await
                    .map_err(map_registry_error)?;
                AppliedChannelFrame::Continue(AgentChannelAck {
                    acknowledged_sequence: sequence,
                    resource_version: result.job.resource_version,
                    replayed: result.replayed,
                    extensions: Extensions::new(),
                })
            }
            AgentChannelUpstreamMessage::Close(payload) => {
                let record = self
                    .registry
                    .close_session(CloseAgentSessionRequest {
                        agent_id: context.agent_id.clone(),
                        boot_id: context.boot_id.clone(),
                        session_generation: context.session_generation,
                        expected_resource_version: payload.expected_resource_version,
                    })
                    .await
                    .map_err(map_registry_error)?;
                AppliedChannelFrame::Close(AgentChannelAck {
                    acknowledged_sequence: sequence,
                    resource_version: record.resource_version,
                    replayed: false,
                    extensions: Extensions::new(),
                })
            }
            AgentChannelUpstreamMessage::Open(_) => {
                return Err(AgentHttpError::protocol_invalid());
            }
        };
        Ok(applied)
    }

    async fn authenticate_channel_frame(
        &self,
        context: &EstablishedAgentChannel,
        frame: &AgentChannelUpstreamFrame,
    ) -> Result<(), AgentHttpError> {
        self.registry
            .authenticate_agent_action(
                &frame.request,
                AGENT_SESSION_CHANNEL_OPEN_PATH,
                true,
                AGENT_ACTION_MAX_CLOCK_SKEW_MS,
            )
            .await
            .map_err(map_registry_error)?;
        if frame.request.agent_id != context.agent_id
            || frame.request.installation_id != context.installation_id
            || frame.request.boot_id != context.boot_id
            || frame.request.session_id.as_ref() != Some(&context.session_id)
            || frame.request.session_generation != Some(context.session_generation)
        {
            return Err(AgentHttpError::session_fenced());
        }
        Ok(())
    }

    async fn deliver_channel_messages(
        &self,
        output: &tokio::sync::mpsc::Sender<Bytes>,
        context: &EstablishedAgentChannel,
        registration_id: u64,
        downstream_sequence: &mut u64,
        last_delivered: &mut BTreeMap<MessageId, Instant>,
    ) -> Result<(), AgentHttpError> {
        let messages = self
            .require_control()?
            .poll_agent_messages(
                &context.agent_id,
                context.session_generation,
                MAX_AGENT_POLL_MESSAGES,
            )
            .await
            .map_err(map_registry_error)?;
        let pending = messages
            .iter()
            .map(|envelope| envelope.message_id.clone())
            .collect::<BTreeSet<_>>();
        last_delivered.retain(|message_id, _| pending.contains(message_id));
        let _delivery_fence = self
            .live_channels
            .enter_current(&context.agent_id, registration_id)
            .await
            .ok_or_else(AgentHttpError::session_fenced)?;
        for envelope in messages {
            let message_id = envelope.message_id;
            let now = Instant::now();
            let (message, delivery_policy) = match envelope.message {
                ControlMessage::Assignment(value) => (
                    AgentChannelDownstreamMessage::Assignment(value),
                    ChannelDeliveryPolicy::OncePerConnection,
                ),
                ControlMessage::Decision(value) => (
                    AgentChannelDownstreamMessage::Decision(value),
                    ChannelDeliveryPolicy::RedeliverAfter(AGENT_CHANNEL_REDELIVERY_INTERVAL),
                ),
                _ => continue,
            };
            if !channel_delivery_due(last_delivered, &message_id, now, delivery_policy) {
                continue;
            }
            Self::try_send_channel_message(
                output,
                context,
                downstream_sequence,
                message_id.clone(),
                None,
                self.registry.now(),
                message,
            )?;
            last_delivered.insert(message_id, Instant::now());
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn try_send_channel_message(
        output: &tokio::sync::mpsc::Sender<Bytes>,
        context: &EstablishedAgentChannel,
        downstream_sequence: &mut u64,
        message_id: MessageId,
        correlation_id: Option<MessageId>,
        sent_at_unix_ms: UnixMillis,
        message: AgentChannelDownstreamMessage,
    ) -> Result<(), AgentHttpError> {
        let (next, bytes) = encode_channel_message(
            context,
            *downstream_sequence,
            message_id,
            correlation_id,
            sent_at_unix_ms,
            message,
        )?;
        output
            .try_send(bytes)
            .map_err(|_| AgentHttpError::unavailable())?;
        *downstream_sequence = next;
        Ok(())
    }

    async fn send_channel_ack(
        &self,
        output: &tokio::sync::mpsc::Sender<Bytes>,
        context: &EstablishedAgentChannel,
        downstream_sequence: &mut u64,
        correlation_id: MessageId,
        ack: AgentChannelAck,
    ) -> Result<(), AgentHttpError> {
        let message_id = channel_message_id(
            context,
            "ack",
            Some(&correlation_id),
            downstream_sequence.saturating_add(1),
        )?;
        self.send_channel_message(
            output,
            context,
            downstream_sequence,
            message_id,
            Some(correlation_id),
            self.registry.now(),
            AgentChannelDownstreamMessage::Ack(ack),
        )
        .await
    }

    async fn send_channel_error(
        &self,
        output: &tokio::sync::mpsc::Sender<Bytes>,
        context: &EstablishedAgentChannel,
        downstream_sequence: &mut u64,
        correlation_id: Option<MessageId>,
        error: AgentHttpError,
    ) {
        let Ok(message_id) = channel_message_id(
            context,
            "error",
            correlation_id.as_ref(),
            downstream_sequence.saturating_add(1),
        ) else {
            return;
        };
        let control_error = ControlError {
            code: ErrorCode::new(error.code).unwrap_or_else(|_| {
                ErrorCode::new("INTERNAL").expect("static fallback error code is valid")
            }),
            message: error.detail.to_owned(),
            retryable: error.retryable,
            retry_after_ms: error.retry_after_ms.map(DecimalU64::new),
            extensions: Extensions::new(),
        };
        let _ = self
            .send_channel_message(
                output,
                context,
                downstream_sequence,
                message_id,
                correlation_id,
                self.registry.now(),
                AgentChannelDownstreamMessage::Error(control_error),
            )
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_channel_message(
        &self,
        output: &tokio::sync::mpsc::Sender<Bytes>,
        context: &EstablishedAgentChannel,
        downstream_sequence: &mut u64,
        message_id: MessageId,
        correlation_id: Option<MessageId>,
        sent_at_unix_ms: UnixMillis,
        message: AgentChannelDownstreamMessage,
    ) -> Result<(), AgentHttpError> {
        let (next, bytes) = encode_channel_message(
            context,
            *downstream_sequence,
            message_id,
            correlation_id,
            sent_at_unix_ms,
            message,
        )?;
        output
            .send(bytes)
            .await
            .map_err(|_| AgentHttpError::unavailable())?;
        *downstream_sequence = next;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_channel_message(
    context: &EstablishedAgentChannel,
    downstream_sequence: u64,
    message_id: MessageId,
    correlation_id: Option<MessageId>,
    sent_at_unix_ms: UnixMillis,
    message: AgentChannelDownstreamMessage,
) -> Result<(u64, Bytes), AgentHttpError> {
    let next = downstream_sequence
        .checked_add(1)
        .ok_or_else(AgentHttpError::protocol_invalid)?;
    let frame = AgentChannelDownstreamFrame {
        protocol_version: ProtocolVersion::V1,
        sequence: SequenceNumber::new(next),
        message_id,
        correlation_id,
        session_generation: context.session_generation,
        sent_at_unix_ms,
        message,
        extensions: Extensions::new(),
    };
    let bytes = frame
        .encode_ndjson()
        .map(Bytes::from)
        .map_err(|_| AgentHttpError::protocol_invalid())?;
    Ok((next, bytes))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelDeliveryPolicy {
    OncePerConnection,
    RedeliverAfter(Duration),
}

fn channel_delivery_due(
    last_delivered: &BTreeMap<MessageId, Instant>,
    message_id: &MessageId,
    now: Instant,
    policy: ChannelDeliveryPolicy,
) -> bool {
    match (last_delivered.get(message_id), policy) {
        (None, _) => true,
        (Some(_), ChannelDeliveryPolicy::OncePerConnection) => false,
        (Some(last_sent), ChannelDeliveryPolicy::RedeliverAfter(interval)) => {
            now.duration_since(*last_sent) >= interval
        }
    }
}

fn channel_message_id(
    context: &EstablishedAgentChannel,
    kind: &str,
    correlation_id: Option<&MessageId>,
    sequence: u64,
) -> Result<MessageId, AgentHttpError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(context.agent_id.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(context.session_id.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(&context.session_generation.get().to_be_bytes());
    hasher.update(&[0]);
    hasher.update(kind.as_bytes());
    hasher.update(&[0]);
    if let Some(correlation_id) = correlation_id {
        hasher.update(correlation_id.as_str().as_bytes());
    } else {
        hasher.update(&sequence.to_be_bytes());
    }
    MessageId::new(format!("server-{}", hasher.finalize().to_hex()))
        .map_err(|_| AgentHttpError::unavailable())
}

#[async_trait]
impl AgentApiHandler for RegistryAgentApiHandler {
    async fn handle(&self, action: AgentAction, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(AgentHttpError::unavailable());
        }
        match action {
            AgentAction::EnrollmentBootstrap => self.bootstrap(body).await,
            AgentAction::EnrollmentStatusQuery => self.bootstrap_status(body).await,
            AgentAction::SessionOpen => self.session_open(body).await,
            AgentAction::SessionHeartbeatReport => self.heartbeat_report(body).await,
            AgentAction::SessionMessageListQuery => self.message_list_query(body).await,
            AgentAction::JobReportCreate => self.job_report_create(body).await,
            AgentAction::JobMetadataBatchStage => self.metadata_batch_stage(body).await,
            AgentAction::JobMetadataPageStage => self.metadata_page_stage(body).await,
            AgentAction::JobIndexPageQuery => self.index_page_query(body).await,
            AgentAction::JobManifestPageQuery => self.manifest_page_query(body).await,
            AgentAction::SessionClose => self.session_close(body).await,
        }
    }

    async fn open_control_channel(
        &self,
        body: Incoming,
    ) -> Result<AgentControlChannel, AgentHttpError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(AgentHttpError::unavailable());
        }
        self.control_channel_open(body).await
    }
}

fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, AgentHttpError> {
    decode_bounded_unique_json(body, body.len().max(AGENT_MAX_REQUEST_BODY_BYTES))
        .map_err(|_| AgentHttpError::protocol_invalid())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, AgentHttpError> {
    serde_json::to_vec(value).map_err(|_| AgentHttpError::unavailable())
}

fn map_registry_error(error: CentralError) -> AgentHttpError {
    match error.code() {
        CentralErrorCode::StorageFailure | CentralErrorCode::Internal => {
            AgentHttpError::unavailable()
        }
        CentralErrorCode::ProtocolInvalid => AgentHttpError::protocol_invalid(),
        CentralErrorCode::GenerationMismatch => AgentHttpError::session_fenced(),
        CentralErrorCode::ConcurrentUpdate | CentralErrorCode::AgentSessionActive => {
            AgentHttpError::new(
                StatusCode::CONFLICT,
                "AGENT_ACTION_CONFLICT",
                "Agent action conflicts with authoritative state",
                true,
            )
        }
        CentralErrorCode::JobNotFound | CentralErrorCode::EnrollmentNotFound => {
            AgentHttpError::new(
                StatusCode::NOT_FOUND,
                "AGENT_RESOURCE_NOT_FOUND",
                "Agent-scoped resource was not found",
                false,
            )
        }
        CentralErrorCode::InvalidState
        | CentralErrorCode::AssignmentMismatch
        | CentralErrorCode::BatchTampered
        | CentralErrorCode::BatchUndeclared => AgentHttpError::new(
            StatusCode::CONFLICT,
            "AGENT_ACTION_REJECTED",
            "Agent action is incompatible with authoritative state",
            false,
        ),
        _ => AgentHttpError::bootstrap_denied(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_channel_context() -> EstablishedAgentChannel {
        EstablishedAgentChannel {
            agent_id: AgentId::new("agent-test").unwrap(),
            installation_id: AgentInstallationId::new("installation-test").unwrap(),
            boot_id: neoengram_protocol::AgentBootId::new("boot-test").unwrap(),
            session_id: SessionId::new("session-test").unwrap(),
            session_generation: SessionGeneration::new(1),
        }
    }

    fn channel_error(message: String) -> AgentChannelDownstreamMessage {
        AgentChannelDownstreamMessage::Error(ControlError {
            code: ErrorCode::new("TEST_ERROR").unwrap(),
            message,
            retryable: false,
            retry_after_ms: None,
            extensions: Extensions::new(),
        })
    }

    #[test]
    fn failed_downstream_enqueue_does_not_advance_sequence_or_retry_protocol_errors() {
        let context = test_channel_context();
        let (full_output, _full_receiver) = tokio::sync::mpsc::channel(1);
        full_output
            .try_send(Bytes::from_static(b"already-full"))
            .unwrap();
        let mut sequence = 7;
        let backpressure = RegistryAgentApiHandler::try_send_channel_message(
            &full_output,
            &context,
            &mut sequence,
            MessageId::new("message-backpressure").unwrap(),
            None,
            UnixMillis::new(1),
            channel_error("temporary backpressure".to_owned()),
        )
        .unwrap_err();
        assert_eq!(backpressure.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(sequence, 7);

        let (empty_output, _empty_receiver) = tokio::sync::mpsc::channel(1);
        let protocol_error = RegistryAgentApiHandler::try_send_channel_message(
            &empty_output,
            &context,
            &mut sequence,
            MessageId::new("message-oversized").unwrap(),
            None,
            UnixMillis::new(1),
            channel_error("x".repeat(4097)),
        )
        .unwrap_err();
        assert_eq!(protocol_error.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(!protocol_error.retryable);
        assert_eq!(sequence, 7);
    }

    #[test]
    fn assignments_are_once_per_connection_while_decisions_are_retried() {
        let assignment_id = MessageId::new("assignment-one").unwrap();
        let decision_id = MessageId::new("decision-one").unwrap();
        let resolved_id = MessageId::new("assignment-resolved").unwrap();
        let started = Instant::now();
        let mut last_delivered = BTreeMap::new();

        assert!(channel_delivery_due(
            &last_delivered,
            &assignment_id,
            started,
            ChannelDeliveryPolicy::OncePerConnection,
        ));
        last_delivered.insert(assignment_id.clone(), started);
        last_delivered.insert(decision_id.clone(), started);
        last_delivered.insert(resolved_id.clone(), started);
        assert!(!channel_delivery_due(
            &last_delivered,
            &assignment_id,
            started + AGENT_CHANNEL_REDELIVERY_INTERVAL * 2,
            ChannelDeliveryPolicy::OncePerConnection,
        ));
        assert!(!channel_delivery_due(
            &last_delivered,
            &decision_id,
            started + AGENT_CHANNEL_REDELIVERY_INTERVAL - Duration::from_millis(1),
            ChannelDeliveryPolicy::RedeliverAfter(AGENT_CHANNEL_REDELIVERY_INTERVAL),
        ));
        assert!(channel_delivery_due(
            &last_delivered,
            &decision_id,
            started + AGENT_CHANNEL_REDELIVERY_INTERVAL,
            ChannelDeliveryPolicy::RedeliverAfter(AGENT_CHANNEL_REDELIVERY_INTERVAL),
        ));

        let pending = BTreeSet::from([assignment_id.clone(), decision_id.clone()]);
        last_delivered.retain(|pending_id, _| pending.contains(pending_id));
        assert_eq!(last_delivered.len(), 2);
        assert!(last_delivered.contains_key(&assignment_id));
        assert!(last_delivered.contains_key(&decision_id));
        assert!(!last_delivered.contains_key(&resolved_id));

        let reconnected = BTreeMap::new();
        assert!(channel_delivery_due(
            &reconnected,
            &assignment_id,
            started + AGENT_CHANNEL_REDELIVERY_INTERVAL * 2,
            ChannelDeliveryPolicy::OncePerConnection,
        ));
    }

    #[test]
    fn stale_session_errors_are_sanitized_and_fenced() {
        let mapped = map_registry_error(CentralError::new(
            CentralErrorCode::GenerationMismatch,
            "sensitive generation detail",
        ));
        assert_eq!(mapped.status, StatusCode::CONFLICT);
        assert_eq!(mapped.code, "AGENT_SESSION_FENCED");
        assert!(!mapped.detail.contains("generation"));
    }
}
