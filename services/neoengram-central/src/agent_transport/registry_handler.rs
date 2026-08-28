use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::{
    AcquireAgentSessionRouteRequest, AgentRegistryService, BootstrapStorageEnrollmentRequest,
    CentralError, CentralErrorCode, CloseAgentSessionRequest, ControlPlane,
    GatewayRegistryRepository, MetadataBatchSubmission, OpenAgentSessionRequest,
    ReceiveReportRequest, StageMetadataBatchRequest,
};
use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use neoengram_domain::protocol::{
    decode_bounded_unique_json, validate_control_envelope, AgentActionAcceptedResponse,
    AgentAuthenticatedRequest, AgentBootstrapRequest, AgentBootstrapStatusRequest, AgentChannelAck,
    AgentChannelDownstreamFrame, AgentChannelDownstreamMessage, AgentChannelUpstreamFrame,
    AgentChannelUpstreamMessage, AgentHeartbeatReportPayload, AgentHeartbeatReportResponse,
    AgentId, AgentIndexPageQueryPayload, AgentIndexPageQueryResponse, AgentInstallationId,
    AgentJobReportCreatePayload, AgentManifestPageQueryPayload, AgentManifestPageQueryResponse,
    AgentMetadataBatchStagePayload, AgentMetadataPageStagePayload, AgentMetadataStageResponse,
    AgentSessionClosePayload, AgentSessionCloseResponse, AgentSessionOpenPayload,
    AgentSessionOpenResponse, ControlError, ControlMessage, DecimalU64, ErrorCode, Extensions,
    GatewayOpaqueBytes, MessageId, SequenceNumber, SessionGeneration, SessionId, UnixMillis,
    AGENT_JOB_INDEX_PAGE_QUERY_PATH, AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
    AGENT_JOB_METADATA_BATCH_STAGE_PATH, AGENT_JOB_METADATA_PAGE_STAGE_PATH,
    AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_CHANNEL_OPEN_PATH, AGENT_SESSION_CLOSE_PATH,
    AGENT_SESSION_HEARTBEAT_REPORT_PATH, AGENT_SESSION_OPEN_PATH, CURRENT_WIRE_VERSION,
    MAX_AGENT_CHANNEL_MESSAGES,
};

use super::{
    control_channel::LiveAgentChannels, AgentAction, AgentApiHandler, AgentControlChannel,
    AgentControlInput, AgentHttpError, GatewayAgentRouteContext, RoutedAgentControlChannel,
    AGENT_MAX_REQUEST_BODY_BYTES,
};
use crate::service::{
    AgentWorkloadCertificateError, AgentWorkloadCertificateService, CentralCommandKeyring,
};

const AGENT_ACTION_MAX_CLOCK_SKEW_MS: u64 = 60_000;
const AGENT_CHANNEL_DELIVERY_INTERVAL: Duration = Duration::from_millis(500);
const AGENT_CHANNEL_REDELIVERY_INTERVAL: Duration = Duration::from_secs(5);
const AGENT_CHANNEL_RESPONSE_BUFFER: usize = MAX_AGENT_CHANNEL_MESSAGES * 2;

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
    gateway_registry: Option<Arc<dyn GatewayRegistryRepository>>,
    accepting: Arc<AtomicBool>,
    live_channels: LiveAgentChannels,
    workload_certificate: Option<Arc<AgentWorkloadCertificateService>>,
    central_command_keyring: Option<Arc<CentralCommandKeyring>>,
    require_central_command_signatures: bool,
}

#[derive(Clone)]
struct EstablishedAgentChannel {
    agent_id: AgentId,
    installation_id: AgentInstallationId,
    boot_id: neoengram_domain::protocol::AgentBootId,
    session_id: SessionId,
    session_generation: SessionGeneration,
}

enum AppliedChannelFrame {
    Continue(AgentChannelAck),
    Close(AgentChannelAck),
}

impl RegistryAgentApiHandler {
    #[must_use]
    pub fn new(registry: Arc<AgentRegistryService>) -> Self {
        Self {
            registry,
            control: None,
            data_plane: None,
            gateway_registry: None,
            accepting: Arc::new(AtomicBool::new(true)),
            live_channels: LiveAgentChannels::default(),
            workload_certificate: None,
            central_command_keyring: None,
            require_central_command_signatures: false,
        }
    }

    #[must_use]
    pub fn with_readiness(registry: Arc<AgentRegistryService>, accepting: Arc<AtomicBool>) -> Self {
        Self {
            registry,
            control: None,
            data_plane: None,
            gateway_registry: None,
            accepting,
            live_channels: LiveAgentChannels::default(),
            workload_certificate: None,
            central_command_keyring: None,
            require_central_command_signatures: false,
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
            gateway_registry: None,
            accepting,
            live_channels: LiveAgentChannels::default(),
            workload_certificate: None,
            central_command_keyring: None,
            require_central_command_signatures: false,
        }
    }

    #[must_use]
    pub fn with_gateway_registry(
        mut self,
        gateway_registry: Arc<dyn GatewayRegistryRepository>,
    ) -> Self {
        self.gateway_registry = Some(gateway_registry);
        self
    }

    /// Installs the Central workload-PKI boundary used to prepare approved Agent credentials.
    #[must_use]
    pub fn with_workload_certificate_service(
        mut self,
        service: Arc<AgentWorkloadCertificateService>,
    ) -> Self {
        self.workload_certificate = Some(service);
        self
    }

    /// Installs the Central command-signing boundary used for Assignment and Decision delivery.
    #[must_use]
    pub fn with_central_command_keyring(mut self, keyring: Arc<CentralCommandKeyring>) -> Self {
        self.central_command_keyring = Some(keyring);
        self
    }

    /// Requires a configured Central command signer whenever a command is delivered.
    #[must_use]
    pub fn require_central_command_signatures(mut self) -> Self {
        self.require_central_command_signatures = true;
        self
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

    fn command_keyring(&self) -> Result<Option<&CentralCommandKeyring>, AgentHttpError> {
        match self.central_command_keyring.as_deref() {
            Some(keyring) => Ok(Some(keyring)),
            None if self.require_central_command_signatures => Err(AgentHttpError::unavailable()),
            None => Ok(None),
        }
    }

    async fn authenticate<T: serde::Serialize>(
        &self,
        request: &AgentAuthenticatedRequest<T>,
        path: &'static str,
    ) -> Result<crate::AgentRegistryRecord, AgentHttpError> {
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
            .bootstrap_status_with_clock_skew(request.clone(), AGENT_ACTION_MAX_CLOCK_SKEW_MS)
            .await
            .map_err(map_registry_error)?;
        let response = match &self.workload_certificate {
            Some(service) => service
                .attach_to_status(&request, response)
                .await
                .map_err(map_workload_certificate_error)?,
            None => response,
        };
        encode(&response)
    }

    async fn session_open(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentSessionOpenPayload> = decode(body)?;
        request
            .payload
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let authenticated = self.authenticate(&request, AGENT_SESSION_OPEN_PATH).await?;
        let result = self
            .registry
            .open_session(OpenAgentSessionRequest {
                agent_id: request.agent_id.clone(),
                installation_id: request.installation_id.clone(),
                boot_id: request.boot_id.clone(),
                mount_identity_digest: request.payload.mount_identity_digest,
                expected_resource_version: request.payload.expected_resource_version,
                capabilities: request
                    .payload
                    .capabilities
                    .clone()
                    .map(|capabilities| capabilities.into_iter().collect()),
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
            wire_version: CURRENT_WIRE_VERSION,
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
            wire_version: CURRENT_WIRE_VERSION,
            request_id: request.request_id,
            resource_version: result.record.resource_version,
            received_at_unix_ms: received_at,
            replayed: result.replayed,
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
        validate_control_envelope(&request.payload.report)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        if request.payload.report.header.session_generation != Some(generation) {
            return Err(AgentHttpError::session_fenced());
        }
        if let ControlMessage::ReplicationReport(report) = &request.payload.report.body {
            let result = self
                .require_control()?
                .receive_replication_report(
                    &request.payload.tenant_id,
                    &request.agent_id,
                    generation,
                    report.as_ref().clone(),
                )
                .await
                .map_err(map_registry_error)?;
            return encode(&AgentActionAcceptedResponse {
                wire_version: CURRENT_WIRE_VERSION,
                request_id: request.request_id,
                resource_version: result.resource_version,
                replayed: result.replayed,
                extensions: Extensions::new(),
            });
        }
        let report = match request.payload.report.body {
            ControlMessage::Accepted(value) => crate::AgentReport::Accepted(value),
            ControlMessage::Progress(value) => crate::AgentReport::Progress(value),
            ControlMessage::Prepared(value) => crate::AgentReport::Prepared(value),
            ControlMessage::Failed(value) => crate::AgentReport::Failed(value),
            ControlMessage::Finalized(value) => crate::AgentReport::Finalized(value),
            ControlMessage::LifecycleReport(value) => {
                let (resource_version, replayed) = self
                    .require_control()?
                    .receive_lifecycle_report(
                        &request.payload.tenant_id,
                        &request.agent_id,
                        generation,
                        *value,
                    )
                    .await
                    .map_err(map_registry_error)?;
                return encode(&AgentActionAcceptedResponse {
                    wire_version: CURRENT_WIRE_VERSION,
                    request_id: request.request_id,
                    resource_version,
                    replayed,
                    extensions: Extensions::new(),
                });
            }
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
            wire_version: CURRENT_WIRE_VERSION,
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
            wire_version: CURRENT_WIRE_VERSION,
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
            wire_version: CURRENT_WIRE_VERSION,
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
            wire_version: CURRENT_WIRE_VERSION,
            request_id: request.request_id,
            resource_version: record.resource_version,
            closed_at_unix_ms: record
                .storage_enrollment
                .updated_at_unix_ms
                .unwrap_or_else(|| UnixMillis::new(0)),
            extensions: Extensions::new(),
        })
    }

    async fn control_channel_open_internal(
        &self,
        mut input: AgentControlInput,
        route: Option<GatewayAgentRouteContext>,
    ) -> Result<
        (
            AgentControlChannel,
            Option<(crate::AgentRouteLease, bool, Option<crate::AgentRouteLease>)>,
        ),
        AgentHttpError,
    > {
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
        let session_request = OpenAgentSessionRequest {
            agent_id: frame.request.agent_id.clone(),
            installation_id: frame.request.installation_id.clone(),
            boot_id: frame.request.boot_id.clone(),
            mount_identity_digest: open.mount_identity_digest,
            expected_resource_version: open.expected_resource_version,
            capabilities: open
                .capabilities
                .clone()
                .map(|capabilities| capabilities.into_iter().collect()),
        };
        let (opened, routed) = match route {
            Some(route) => {
                let repository = self
                    .gateway_registry
                    .as_ref()
                    .ok_or_else(AgentHttpError::unavailable)?;
                let outcome = repository
                    .acquire_agent_session_route(AcquireAgentSessionRouteRequest {
                        route_request_id: route.route_request_id,
                        session: session_request,
                        gateway_pool_id: route.gateway_pool_id,
                        gateway_replica_id: route.gateway_replica_id,
                        connection_id: route.connection_id,
                        observed_at_unix_ms: route.observed_at_unix_ms,
                        lease_expires_at_unix_ms: route.lease_expires_at_unix_ms,
                        heartbeat_timeout_ms: route.heartbeat_timeout_ms,
                    })
                    .await
                    .map_err(map_registry_error)?;
                (
                    outcome.session,
                    Some((
                        outcome.route.lease,
                        outcome.route.replayed,
                        outcome.route.fenced,
                    )),
                )
            }
            None => (
                self.registry
                    .open_session(session_request)
                    .await
                    .map_err(map_registry_error)?,
                None,
            ),
        };
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
            wire_version: CURRENT_WIRE_VERSION,
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
        Ok((AgentControlChannel::new(receiver), routed))
    }

    async fn control_channel_open(
        &self,
        input: AgentControlInput,
    ) -> Result<AgentControlChannel, AgentHttpError> {
        self.control_channel_open_internal(input, None)
            .await
            .map(|(channel, _)| channel)
    }

    async fn run_control_channel(
        &self,
        input: &mut AgentControlInput,
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
                if let ControlMessage::LifecycleReport(report) = &payload.report.body {
                    let (resource_version, replayed) = self
                        .require_control()?
                        .receive_lifecycle_report(
                            &payload.tenant_id,
                            &context.agent_id,
                            context.session_generation,
                            report.as_ref().clone(),
                        )
                        .await
                        .map_err(map_registry_error)?;
                    return Ok(AppliedChannelFrame::Continue(AgentChannelAck {
                        acknowledged_sequence: sequence,
                        resource_version,
                        replayed,
                        extensions: Extensions::new(),
                    }));
                }
                if let ControlMessage::ReplicationReport(report) = &payload.report.body {
                    let result = self
                        .require_control()?
                        .receive_replication_report(
                            &payload.tenant_id,
                            &context.agent_id,
                            context.session_generation,
                            report.as_ref().clone(),
                        )
                        .await
                        .map_err(map_registry_error)?;
                    return Ok(AppliedChannelFrame::Continue(AgentChannelAck {
                        acknowledged_sequence: sequence,
                        resource_version: result.resource_version,
                        replayed: result.replayed,
                        extensions: Extensions::new(),
                    }));
                }
                let report = match &payload.report.body {
                    ControlMessage::Accepted(value) => crate::AgentReport::Accepted(value.clone()),
                    ControlMessage::Progress(value) => crate::AgentReport::Progress(value.clone()),
                    ControlMessage::Prepared(value) => crate::AgentReport::Prepared(value.clone()),
                    ControlMessage::Failed(value) => crate::AgentReport::Failed(value.clone()),
                    ControlMessage::Finalized(value) => {
                        crate::AgentReport::Finalized(value.clone())
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
            .deliverable_agent_messages(
                &context.agent_id,
                context.session_generation,
                MAX_AGENT_CHANNEL_MESSAGES,
            )
            .await
            .map_err(map_registry_error)?;
        let pending = messages
            .iter()
            .filter_map(|envelope| MessageId::new(envelope.header.request_id.as_str()).ok())
            .collect::<BTreeSet<_>>();
        last_delivered.retain(|message_id, _| pending.contains(message_id));
        let _delivery_fence = self
            .live_channels
            .enter_current(&context.agent_id, registration_id)
            .await
            .ok_or_else(AgentHttpError::session_fenced)?;
        for envelope in messages {
            let message_id = MessageId::new(envelope.header.request_id.as_str())
                .map_err(|_| AgentHttpError::protocol_invalid())?;
            let now = Instant::now();
            let (message, delivery_policy) = match envelope.body {
                ControlMessage::Assignment(value) => (
                    AgentChannelDownstreamMessage::Assignment(value),
                    ChannelDeliveryPolicy::OncePerConnection,
                ),
                ControlMessage::LifecycleAssignment(value) => (
                    AgentChannelDownstreamMessage::LifecycleAssignment(value),
                    ChannelDeliveryPolicy::OncePerConnection,
                ),
                ControlMessage::ReplicationAssignment(value) => (
                    AgentChannelDownstreamMessage::ReplicationAssignment(value),
                    ChannelDeliveryPolicy::RedeliverAfter(AGENT_CHANNEL_REDELIVERY_INTERVAL),
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
            self.try_send_channel_message(
                output,
                context,
                downstream_sequence,
                message_id.clone(),
                None,
                self.registry.now(),
                message,
            )
            .await?;
            last_delivered.insert(message_id, Instant::now());
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_send_channel_message(
        &self,
        output: &tokio::sync::mpsc::Sender<Bytes>,
        context: &EstablishedAgentChannel,
        downstream_sequence: &mut u64,
        message_id: MessageId,
        correlation_id: Option<MessageId>,
        sent_at_unix_ms: UnixMillis,
        message: AgentChannelDownstreamMessage,
    ) -> Result<(), AgentHttpError> {
        let (next, bytes) = self
            .encode_channel_message(
                context,
                *downstream_sequence,
                message_id,
                correlation_id,
                sent_at_unix_ms,
                message,
            )
            .await?;
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
        let (next, bytes) = self
            .encode_channel_message(
                context,
                *downstream_sequence,
                message_id,
                correlation_id,
                sent_at_unix_ms,
                message,
            )
            .await?;
        output
            .send(bytes)
            .await
            .map_err(|_| AgentHttpError::unavailable())?;
        *downstream_sequence = next;
        Ok(())
    }

    async fn sign_channel_frame(
        &self,
        frame: &mut AgentChannelDownstreamFrame,
    ) -> Result<(), AgentHttpError> {
        frame.central_signature = None;
        if !matches!(
            &frame.message,
            AgentChannelDownstreamMessage::Assignment(_)
                | AgentChannelDownstreamMessage::Decision(_)
                | AgentChannelDownstreamMessage::LifecycleAssignment(_)
                | AgentChannelDownstreamMessage::ReplicationAssignment(_)
        ) {
            return Ok(());
        }

        let Some(keyring) = self.command_keyring()? else {
            return Ok(());
        };
        let payload = frame
            .central_command_payload_bytes()
            .and_then(GatewayOpaqueBytes::new)
            .map_err(|_| AgentHttpError::unavailable())?;
        frame.central_signature = Some(
            keyring
                .sign(payload, self.registry.now())
                .await
                .map_err(|_| AgentHttpError::unavailable())?,
        );
        frame.validate().map_err(|_| AgentHttpError::unavailable())
    }

    #[allow(clippy::too_many_arguments)]
    async fn encode_channel_message(
        &self,
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
        let mut frame = AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(next),
            message_id,
            correlation_id,
            session_generation: context.session_generation,
            sent_at_unix_ms,
            central_signature: None,
            message,
            extensions: Extensions::new(),
        };
        self.sign_channel_frame(&mut frame).await?;
        let bytes = frame
            .encode_ndjson()
            .map(Bytes::from)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        Ok((next, bytes))
    }
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
        input: AgentControlInput,
    ) -> Result<AgentControlChannel, AgentHttpError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(AgentHttpError::unavailable());
        }
        self.control_channel_open(input).await
    }

    async fn open_routed_control_channel(
        &self,
        input: AgentControlInput,
        route: GatewayAgentRouteContext,
    ) -> Result<RoutedAgentControlChannel, AgentHttpError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(AgentHttpError::unavailable());
        }
        let (channel, routed) = self
            .control_channel_open_internal(input, Some(route))
            .await?;
        let (route, replayed, fenced) = routed.ok_or_else(AgentHttpError::unavailable)?;
        Ok(RoutedAgentControlChannel {
            channel,
            route,
            replayed,
            fenced,
        })
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
        // A route can disappear while a Gateway replica is restarting or while its lease is
        // being refreshed. Keep this distinct from bootstrap denial so an approved Agent can
        // retain its boot identity and retry session.open after the route recovers.
        CentralErrorCode::GatewayRouteUnavailable => AgentHttpError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "GATEWAY_ROUTE_UNAVAILABLE",
            "Gateway route is temporarily unavailable",
            true,
        )
        .with_retry_after_ms(1_000),
        CentralErrorCode::ProtocolInvalid => AgentHttpError::protocol_invalid(),
        CentralErrorCode::GenerationMismatch => AgentHttpError::session_fenced(),
        CentralErrorCode::ConcurrentUpdate => AgentHttpError::new(
            StatusCode::CONFLICT,
            "AGENT_ACTION_CONFLICT",
            "Agent action conflicts with authoritative state",
            true,
        ),
        // A different boot currently owns the session. This is an authoritative takeover, not a
        // transient CAS race; retrying it forever would leave a duplicate Agent looking healthy
        // while it can never become the owner of the session.
        CentralErrorCode::AgentSessionActive => AgentHttpError::new(
            StatusCode::CONFLICT,
            "AGENT_ACTION_CONFLICT",
            "Agent action conflicts with authoritative state",
            false,
        ),
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

fn map_workload_certificate_error(error: AgentWorkloadCertificateError) -> AgentHttpError {
    match error {
        AgentWorkloadCertificateError::ConcurrentUpdate => AgentHttpError::new(
            StatusCode::CONFLICT,
            "AGENT_CERTIFICATE_CONFLICT",
            "Agent workload certificate preparation is contended",
            true,
        ),
        AgentWorkloadCertificateError::Registry(error) => map_registry_error(error),
        AgentWorkloadCertificateError::Pki(_)
        | AgentWorkloadCertificateError::Issuer(_)
        | AgentWorkloadCertificateError::InvalidCertificateMaterial
        | AgentWorkloadCertificateError::MissingCredentialEvidence
        | AgentWorkloadCertificateError::Protocol(_) => AgentHttpError::unavailable(),
    }
}

#[cfg(test)]
mod tests {
    use neoengram_domain::protocol::{
        AgentMountId, ArtifactId, AssignmentGeneration, AssignmentId, AssignmentOperation,
        CertificateGeneration, ContentDigest, DecisionGeneration, Ed25519PublicKeySpki,
        Ed25519Signature, JobAssignment, JobDecision, JobState, MountGeneration, OwnerGeneration,
        PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef, ProjectId, PublishDecision,
        RequestId, ResourceVersion, StorageVolumeId, TenantId, WorkspaceMaterializeAssignment,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair as _};

    use super::*;
    use crate::service::{
        CentralCommandKeyId, CentralCommandKeyState, CentralCommandSignature,
        CentralCommandSignatureRequest, CentralCommandSigner, CentralCommandSignerError,
        CentralCommandTrustBundle, CentralCommandVerificationKey,
    };

    struct TestCommandSigner {
        key_pair: Ed25519KeyPair,
        unavailable: bool,
    }

    #[async_trait]
    impl CentralCommandSigner for TestCommandSigner {
        async fn sign(
            &self,
            request: CentralCommandSignatureRequest,
        ) -> Result<CentralCommandSignature, CentralCommandSignerError> {
            if self.unavailable {
                return Err(CentralCommandSignerError::Unavailable(
                    "test signer unavailable".to_owned(),
                ));
            }
            let signature = Ed25519Signature::new(
                self.key_pair
                    .sign(request.signing_bytes())
                    .as_ref()
                    .to_vec(),
            )
            .map_err(|error| CentralCommandSignerError::Internal(error.to_string()))?;
            CentralCommandSignature::new(
                request.key_id().clone(),
                request.certificate_generation(),
                signature,
            )
            .map_err(|error| CentralCommandSignerError::Internal(error.to_string()))
        }
    }

    fn test_command_keyring(unavailable: bool) -> Arc<CentralCommandKeyring> {
        let key_pair = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let public_key = Ed25519PublicKeySpki::from_public_key_bytes(
            key_pair.public_key().as_ref().try_into().unwrap(),
        );
        let key_id = CentralCommandKeyId::new("central-command-test").unwrap();
        let generation = CertificateGeneration::new(1);
        let verification_key = CentralCommandVerificationKey::new(
            key_id.clone(),
            generation,
            public_key,
            CentralCommandKeyState::Active,
        )
        .unwrap();
        Arc::new(
            CentralCommandKeyring::new(
                Arc::new(TestCommandSigner {
                    key_pair,
                    unavailable,
                }),
                CentralCommandTrustBundle::new(vec![verification_key]).unwrap(),
                key_id,
                generation,
            )
            .unwrap(),
        )
    }

    fn test_handler() -> RegistryAgentApiHandler {
        RegistryAgentApiHandler::new(Arc::new(AgentRegistryService::new(
            Arc::new(crate::InMemoryAgentRegistry::new()),
            Arc::new(crate::InMemoryClock::new(10_000)),
            30_000,
        )))
    }

    fn test_assignment() -> JobAssignment {
        let project_id = ProjectId::new("project-command-test").unwrap();
        let artifact_id = ArtifactId::new("artifact-command-test").unwrap();
        let playground_id = PlaygroundId::new("playground-command-test").unwrap();
        let relative_root = WorkspaceMaterializeAssignment::canonical_relative_root(
            &project_id,
            &artifact_id,
            &playground_id,
        )
        .unwrap();
        let mut assignment = WorkspaceMaterializeAssignment {
            job_id: neoengram_domain::protocol::JobId::new("job-command-test").unwrap(),
            assignment_id: AssignmentId::new("assignment-command-test").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-test").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::Service,
                id: PrincipalId::new("principal-command-test").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-command-test").unwrap(),
            project_id,
            artifact_id,
            playground_id,
            storage_volume_id: StorageVolumeId::new("volume-command-test").unwrap(),
            agent_mount_id: AgentMountId::new("mount-command-test").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root,
            base_commit_id: None,
            base_index_version: None,
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(20_000),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        JobAssignment {
            assignment: AssignmentOperation::WorkspaceMaterialize {
                input: assignment,
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        }
    }

    fn test_decision() -> JobDecision {
        JobDecision {
            job_id: neoengram_domain::protocol::JobId::new("job-command-test").unwrap(),
            assignment_id: AssignmentId::new("assignment-command-test").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            decision_generation: DecisionGeneration::new(1),
            decision: PublishDecision::Reject {
                error: ControlError {
                    code: ErrorCode::new("TEST_REJECTED").unwrap(),
                    message: "test rejection".to_owned(),
                    retryable: false,
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            },
            final_state: JobState::Rejected,
            extensions: Extensions::new(),
        }
    }

    fn test_channel_context() -> EstablishedAgentChannel {
        EstablishedAgentChannel {
            agent_id: AgentId::new("agent-test").unwrap(),
            installation_id: AgentInstallationId::new("installation-test").unwrap(),
            boot_id: neoengram_domain::protocol::AgentBootId::new("boot-test").unwrap(),
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

    #[tokio::test]
    async fn h2_assignments_and_decisions_are_signed_while_errors_remain_unsigned() {
        let keyring = test_command_keyring(false);
        let handler = test_handler().with_central_command_keyring(keyring.clone());
        let context = test_channel_context();
        let (output, mut receiver) = tokio::sync::mpsc::channel(3);
        let mut sequence = 0;
        for (message_id, message) in [
            (
                "h2-assignment-command-test",
                AgentChannelDownstreamMessage::Assignment(Box::new(test_assignment())),
            ),
            (
                "h2-decision-command-test",
                AgentChannelDownstreamMessage::Decision(test_decision()),
            ),
        ] {
            handler
                .try_send_channel_message(
                    &output,
                    &context,
                    &mut sequence,
                    MessageId::new(message_id).unwrap(),
                    None,
                    UnixMillis::new(9_000),
                    message,
                )
                .await
                .unwrap();
        }
        handler
            .try_send_channel_message(
                &output,
                &context,
                &mut sequence,
                MessageId::new("h2-error-test").unwrap(),
                None,
                UnixMillis::new(9_000),
                channel_error("test error".to_owned()),
            )
            .await
            .unwrap();
        assert_eq!(sequence, 3);

        for _ in 0..2 {
            let bytes = receiver.recv().await.unwrap();
            let frame =
                AgentChannelDownstreamFrame::decode_json(&bytes[..bytes.len() - 1]).unwrap();
            let signature = frame.central_signature.as_ref().unwrap();
            assert_eq!(
                signature.payload.as_bytes(),
                frame.central_command_payload_bytes().unwrap()
            );
            keyring
                .trust_bundle()
                .verify_at(signature, UnixMillis::new(10_001))
                .unwrap();
        }
        let bytes = receiver.recv().await.unwrap();
        let frame = AgentChannelDownstreamFrame::decode_json(&bytes[..bytes.len() - 1]).unwrap();
        assert!(matches!(
            frame.message,
            AgentChannelDownstreamMessage::Error(_)
        ));
        assert!(frame.central_signature.is_none());
    }

    #[tokio::test]
    async fn opened_ack_and_error_frames_are_never_signed() {
        let handler = test_handler().with_central_command_keyring(test_command_keyring(false));
        let context = test_channel_context();
        let (output, mut receiver) = tokio::sync::mpsc::channel(3);
        let mut sequence = 0;
        handler
            .try_send_channel_message(
                &output,
                &context,
                &mut sequence,
                MessageId::new("opened-test").unwrap(),
                Some(MessageId::new("open-request-test").unwrap()),
                UnixMillis::new(10_000),
                AgentChannelDownstreamMessage::Opened(AgentSessionOpenResponse {
                    wire_version: CURRENT_WIRE_VERSION,
                    request_id: RequestId::new("open-request-test").unwrap(),
                    agent_id: context.agent_id.clone(),
                    session_id: context.session_id.clone(),
                    session_generation: context.session_generation,
                    agent_mount_id: AgentMountId::new("mount-test").unwrap(),
                    mount_generation: MountGeneration::new(1),
                    owner_generation: OwnerGeneration::new(1),
                    resource_version: ResourceVersion::new(1),
                    opened_at_unix_ms: UnixMillis::new(10_000),
                    replayed: false,
                    extensions: Extensions::new(),
                }),
            )
            .await
            .unwrap();
        handler
            .try_send_channel_message(
                &output,
                &context,
                &mut sequence,
                MessageId::new("ack-test").unwrap(),
                Some(MessageId::new("heartbeat-test").unwrap()),
                UnixMillis::new(10_000),
                AgentChannelDownstreamMessage::Ack(AgentChannelAck {
                    acknowledged_sequence: SequenceNumber::new(1),
                    resource_version: ResourceVersion::new(1),
                    replayed: false,
                    extensions: Extensions::new(),
                }),
            )
            .await
            .unwrap();
        handler
            .try_send_channel_message(
                &output,
                &context,
                &mut sequence,
                MessageId::new("error-test").unwrap(),
                None,
                UnixMillis::new(10_000),
                channel_error("test error".to_owned()),
            )
            .await
            .unwrap();

        for _ in 0..3 {
            let bytes = receiver.recv().await.unwrap();
            let frame =
                AgentChannelDownstreamFrame::decode_json(&bytes[..bytes.len() - 1]).unwrap();
            assert!(frame.central_signature.is_none());
        }
    }

    #[tokio::test]
    async fn h2_signer_failures_do_not_enqueue_or_advance_the_sequence() {
        for handler in [
            test_handler().with_central_command_keyring(test_command_keyring(true)),
            test_handler().require_central_command_signatures(),
        ] {
            let context = test_channel_context();
            let (output, mut receiver) = tokio::sync::mpsc::channel(1);
            let mut sequence = 7;
            let error = handler
                .try_send_channel_message(
                    &output,
                    &context,
                    &mut sequence,
                    MessageId::new("h2-unavailable-command-test").unwrap(),
                    None,
                    UnixMillis::new(9_000),
                    AgentChannelDownstreamMessage::Decision(test_decision()),
                )
                .await
                .unwrap_err();
            assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(sequence, 7);
            assert!(matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
        }
    }

    #[tokio::test]
    async fn failed_downstream_enqueue_does_not_advance_sequence_or_retry_protocol_errors() {
        let handler = test_handler();
        let context = test_channel_context();
        let (full_output, _full_receiver) = tokio::sync::mpsc::channel(1);
        full_output
            .try_send(Bytes::from_static(b"already-full"))
            .unwrap();
        let mut sequence = 7;
        let backpressure = handler
            .try_send_channel_message(
                &full_output,
                &context,
                &mut sequence,
                MessageId::new("message-backpressure").unwrap(),
                None,
                UnixMillis::new(1),
                channel_error("temporary backpressure".to_owned()),
            )
            .await
            .unwrap_err();
        assert_eq!(backpressure.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(sequence, 7);

        let (empty_output, _empty_receiver) = tokio::sync::mpsc::channel(1);
        let protocol_error = handler
            .try_send_channel_message(
                &empty_output,
                &context,
                &mut sequence,
                MessageId::new("message-oversized").unwrap(),
                None,
                UnixMillis::new(1),
                channel_error("x".repeat(4097)),
            )
            .await
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

    #[test]
    fn unavailable_gateway_routes_are_retryable_for_approved_agents() {
        let mapped = map_registry_error(CentralError::new(
            CentralErrorCode::GatewayRouteUnavailable,
            "owner replica is restarting",
        ));
        assert_eq!(mapped.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.code, "GATEWAY_ROUTE_UNAVAILABLE");
        assert!(mapped.retryable);
        assert_eq!(mapped.retry_after_ms, Some(1_000));
    }

    #[test]
    fn active_session_conflict_is_not_retryable_for_a_duplicate_agent() {
        let mapped = map_registry_error(CentralError::new(
            CentralErrorCode::AgentSessionActive,
            "another boot owns the session",
        ));
        assert_eq!(mapped.status, StatusCode::CONFLICT);
        assert_eq!(mapped.code, "AGENT_ACTION_CONFLICT");
        assert!(!mapped.retryable);
    }
}
