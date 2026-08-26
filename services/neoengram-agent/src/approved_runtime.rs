use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    Agent, AgentAssignmentState, AgentError, AgentErrorCode, Clock, DurableReportSink,
    LifecycleJournal, OutboundReportQueue, SingleVolumeAgentConfig,
    SingleVolumeAssignmentValidator, SingleVolumeMountProbe, SqliteLedger, SqliteLedgerConfig,
    SqliteLifecycleJournal, SqliteLifecycleJournalConfig, SqliteOutboundReportQueue,
    SqliteOutboundReportQueueConfig,
};
use async_trait::async_trait;
use neoengram_domain::protocol::{
    new_control_envelope, AgentBootId, AgentChannelDownstreamFrame, AgentChannelDownstreamMessage,
    AgentChannelUpstreamMessage, AgentChannelUpstreamPayload, AgentHeartbeat,
    AgentHeartbeatReportPayload, AgentId, AgentInstallationId, AgentJobReportCreatePayload,
    AgentSessionClosePayload, AgentSessionOpenPayload, Extensions, JobAssignment, JobDecision,
    JobState, MessageId, MountAccessMode, RequestId, ResourceVersion, RunningJobObservation,
    S3ReadChannelHello, SequenceNumber, SessionGeneration, TenantId, TraceId, UnixMillis,
};
use tokio::time::{self, MissedTickBehavior};
use uuid::Uuid;

use crate::{
    health::HealthReporter,
    resource_lifecycle::{FilesystemResourceLifecycleExecutor, ResourceLifecycleExecutor},
    s3_channel::spawn_s3_read_channel,
    session_channel::{
        spawn_work_dispatcher, AgentChannelConnection, AgentWork, AgentWorkDispatcher,
        FencedAgentWork,
    },
    snapshot_mount::SnapshotDeliveryRecoveryOutcome,
    AgentConfig, AgentDaemonError, AgentDaemonResult, AgentMessageProcessor, AgentRequestSigner,
    AgentSessionBinding, AgentSessionClient, AgentSessionClientError, AgentSessionFence,
    AgentSigningKey, CentralCommandTrustBundle, CoreAgentMessageProcessor, ExecutionBridge,
    FilesystemExecution, MountProbe, MountedVolumeReplicationExecutor, RuntimeHealthPhase,
    S3ReadExecutor, S3SnapshotSource, SessionExecutionBridge, SharedResourceVersion,
    SharedSessionFence, SnapshotCasReaderFactory, SnapshotDeliveryMountManager,
    WorkspaceMaterializer,
};

const REPORT_INTERVAL: Duration = Duration::from_millis(100);
// Keep a report burst below the Gateway's bounded Agent input queue. Heartbeats and
// acknowledgements must still fit while Central is processing durable reports.
const MAX_REPORTS_PER_FLUSH: usize = 1;
const CHANNEL_ACTION_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);

#[derive(Default)]
struct DeferredProcessor(RwLock<Option<Arc<dyn AgentMessageProcessor>>>);

impl std::fmt::Debug for DeferredProcessor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeferredProcessor")
            .finish_non_exhaustive()
    }
}

impl DeferredProcessor {
    fn install(&self, processor: Arc<dyn AgentMessageProcessor>) -> AgentDaemonResult<()> {
        let mut current = self
            .0
            .write()
            .map_err(|_| AgentDaemonError::Session("processor lock is poisoned".to_owned()))?;
        if current.replace(processor).is_some() {
            return Err(AgentDaemonError::Session(
                "Agent message processor was installed more than once".to_owned(),
            ));
        }
        Ok(())
    }

    fn current(&self) -> AgentDaemonResult<Arc<dyn AgentMessageProcessor>> {
        self.0
            .read()
            .map_err(|_| AgentDaemonError::Session("processor lock is poisoned".to_owned()))?
            .clone()
            .ok_or_else(|| AgentDaemonError::Session("Agent processor is not ready".to_owned()))
    }
}

#[async_trait]
impl AgentMessageProcessor for DeferredProcessor {
    async fn handle_assignment(&self, assignment: JobAssignment) -> AgentDaemonResult<()> {
        self.current()?.handle_assignment(assignment).await
    }

    async fn handle_replication(
        &self,
        assignment: neoengram_domain::protocol::ReplicationAssignment,
    ) -> AgentDaemonResult<()> {
        self.current()?.handle_replication(assignment).await
    }

    async fn handle_lifecycle_assignment(
        &self,
        assignment: neoengram_domain::protocol::AgentResourceLifecycleAssignment,
    ) -> AgentDaemonResult<()> {
        self.current()?
            .handle_lifecycle_assignment(assignment)
            .await
    }

    async fn recover_assignment(&self, record: crate::LedgerRecord) -> AgentDaemonResult<()> {
        self.current()?.recover_assignment(record).await
    }

    async fn handle_decision(
        &self,
        tenant_id: &TenantId,
        decision: JobDecision,
    ) -> AgentDaemonResult<()> {
        self.current()?.handle_decision(tenant_id, decision).await
    }
}

#[derive(Debug, Default)]
struct RuntimeClock;

impl Clock for RuntimeClock {
    fn now_unix_ms(&self) -> Result<u64, AgentError> {
        now_unix_ms().map_err(|error| AgentError::new(AgentErrorCode::Internal, error.to_string()))
    }
}

pub(crate) fn build_replication_network(
    config: &AgentConfig,
) -> AgentDaemonResult<Option<Arc<crate::QuicTransferNetwork>>> {
    if !config.replication.enabled {
        return Ok(None);
    }
    let listen = config.replication_listen_socket_addr()?.ok_or_else(|| {
        AgentDaemonError::Configuration(
            "replication listener endpoint is not configured".to_owned(),
        )
    })?;
    let gateway_endpoint = config.replication_gateway_socket_addr()?.ok_or_else(|| {
        AgentDaemonError::Configuration("replication Gateway endpoint is not configured".to_owned())
    })?;
    let certificate_file = config
        .replication
        .tls_certificate_file
        .clone()
        .ok_or_else(|| {
            AgentDaemonError::Configuration(
                "replication TLS certificate is not configured".to_owned(),
            )
        })?;
    let private_key_file = config
        .replication
        .tls_private_key_file
        .clone()
        .ok_or_else(|| {
            AgentDaemonError::Configuration(
                "replication TLS private key is not configured".to_owned(),
            )
        })?;
    let client_ca_file = config.replication.tls_ca_file.clone().ok_or_else(|| {
        AgentDaemonError::Configuration("replication TLS CA is not configured".to_owned())
    })?;
    let server_name = config
        .replication
        .gateway_endpoint
        .as_ref()
        .and_then(|endpoint| endpoint.host_str())
        .ok_or_else(|| {
            AgentDaemonError::Configuration(
                "replication Gateway endpoint host is not configured".to_owned(),
            )
        })?
        .to_owned();
    let network = crate::QuicTransferNetwork::bind(&crate::QuicTransferNetworkConfig {
        listen: Some(listen),
        gateway_endpoint: Some(gateway_endpoint),
        certificate_file,
        private_key_file,
        client_ca_file,
        server_name,
    })
    .map_err(|error| {
        AgentDaemonError::Configuration(format!(
            "replication QUIC network could not start: {error}"
        ))
    })?;
    Ok(Some(Arc::new(network)))
}

/// Runs one already-approved Agent identity until shutdown.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_approved_session<C, P, F>(
    config: AgentConfig,
    client: Arc<C>,
    probe: P,
    agent_id: AgentId,
    installation_id: AgentInstallationId,
    signing_key: AgentSigningKey,
    initial_resource_version: ResourceVersion,
    command_trust_bundle: Option<Arc<CentralCommandTrustBundle>>,
    shutdown: F,
) -> AgentDaemonResult<()>
where
    C: AgentSessionClient + 'static,
    P: MountProbe,
    F: Future<Output = ()> + Send,
{
    let mut reporter = HealthReporter::acquire(config.storage.state_dir.clone())?;
    reporter.set_phase(RuntimeHealthPhase::ApprovedWaitingCertificate)?;
    let initial_observation = probe.probe();
    super::runtime::validate_bootstrap_probe(&initial_observation)?;
    let mount_identity_digest = initial_observation.mount_identity_digest.ok_or_else(|| {
        AgentDaemonError::MountProbe("mount identity is unavailable for session open".to_owned())
    })?;
    let boot_id = AgentBootId::new(format!("boot-{}", Uuid::new_v4().simple()))
        .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
    let signer = Arc::new(AgentRequestSigner::new(
        agent_id.clone(),
        installation_id.clone(),
        boot_id.clone(),
        signing_key,
    )?);
    let fence = SharedSessionFence::default();
    let resource_version = SharedResourceVersion::new(initial_resource_version);
    let reports = Arc::new(SqliteOutboundReportQueue::open(
        SqliteOutboundReportQueueConfig::new(
            config.storage.state_dir.clone(),
            agent_id.clone(),
            config.tenant_id.clone(),
        ),
    )?);
    reports.integrity_check()?;
    let deferred = Arc::new(DeferredProcessor::default());
    // Bind and preflight the replication data plane before opening the Central session. A
    // replication-enabled Agent may stay online when the Gateway is temporarily unavailable, but
    // Central must not persist the dynamic capability until the endpoint handshake succeeds.
    let replication_network = build_replication_network(&config)?;
    let replication_ready = match replication_network.as_ref() {
        Some(network) => match network.preflight_gateway().await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "replication Gateway QUIC preflight failed; capability will not be advertised");
                false
            }
        },
        None => false,
    };
    let open_payload = AgentSessionOpenPayload {
        mount_identity_digest,
        expected_resource_version: initial_resource_version,
        capabilities: Some(super::runtime::agent_capabilities(
            &config,
            replication_ready,
        )),
        extensions: Extensions::new(),
    };
    let reconnect_max_delay = Duration::from_secs(config.session.reconnect_max_delay_seconds);
    let mut shutdown = Box::pin(shutdown);
    let Some((mut channel, binding)) = connect_with_backoff(
        Arc::clone(&client),
        Arc::clone(&signer),
        &open_payload,
        &fence,
        &resource_version,
        None,
        reconnect_max_delay,
        &mut shutdown,
    )
    .await?
    else {
        return Ok(());
    };

    let volume = SingleVolumeAgentConfig {
        agent_id: agent_id.clone(),
        installation_id: installation_id.clone(),
        tenant_id: config.tenant_id.clone(),
        edge_cluster_id: config.edge_cluster_id.clone(),
        storage_volume_id: config.storage_volume_id.clone(),
        agent_mount_id: binding.agent_mount_id.clone(),
        mount_generation: binding.mount_generation,
        owner_generation: binding.owner_generation,
        expected_volume_marker: config.storage.expected_volume_marker.clone(),
        desired_access_mode: MountAccessMode::ReadWrite,
        data_root: config.storage.mount_path.clone(),
        state_root: config.storage.state_dir.clone(),
    };
    volume.validate()?;
    let ledger = Arc::new(SqliteLedger::open(SqliteLedgerConfig::new(
        config.storage.state_dir.clone(),
        agent_id,
        config.tenant_id.clone(),
    ))?);
    ledger.integrity_check()?;
    let lifecycle_journal = Arc::new(SqliteLifecycleJournal::open(
        SqliteLifecycleJournalConfig::new(
            config.storage.state_dir.clone(),
            volume.agent_id.clone(),
            volume.tenant_id.clone(),
        ),
    )?);
    lifecycle_journal.integrity_check()?;
    let clock: Arc<dyn Clock> = Arc::new(RuntimeClock);
    let report_sink = Arc::new(DurableReportSink::new(reports.clone(), Arc::clone(&clock)));
    let bridge: Arc<dyn ExecutionBridge> = Arc::new(SessionExecutionBridge::new(
        config.tenant_id.clone(),
        Arc::clone(&client),
        Arc::clone(&signer),
        fence.clone(),
        reports.clone(),
        tokio::runtime::Handle::current(),
    ));
    let execution = FilesystemExecution::new(&config.storage.mount_path, Arc::clone(&bridge));
    execution.initialize()?;
    let execution = Arc::new(execution);

    let (_replication_shutdown, replication_shutdown_receiver) = tokio::sync::watch::channel(false);
    if let (Some(network), Some(trust_bundle)) =
        (replication_network.clone(), command_trust_bundle.clone())
    {
        let source_network = Arc::clone(&network);
        let source_execution = Arc::clone(&execution);
        let source_tenant = config.tenant_id.clone();
        let source_agent = volume.agent_id.clone();
        tokio::spawn(async move {
            if let Err(error) = source_network
                .serve_source(
                    trust_bundle,
                    source_tenant,
                    source_agent,
                    source_execution,
                    replication_shutdown_receiver,
                )
                .await
            {
                tracing::error!(%error, "Agent QUIC source listener stopped");
            }
        });
    }
    let validator = Arc::new(SingleVolumeAssignmentValidator::new(
        volume.clone(),
        initial_observation.health,
    ));
    let agent = Arc::new(Agent::new(
        ledger,
        validator,
        execution.clone(),
        execution.clone(),
        report_sink,
        Arc::clone(&clock),
    ));
    let snapshot_delivery_mounts = Arc::new(SnapshotDeliveryMountManager::for_binding(
        config.storage.mount_path.clone(),
        &config.storage.state_dir,
        volume.agent_id.clone(),
        volume.tenant_id.clone(),
        volume.storage_volume_id.clone(),
        volume.agent_mount_id.clone(),
        volume.mount_generation,
        volume.owner_generation,
        binding.fence.session_generation,
        Arc::clone(&bridge),
    )?);
    let lifecycle_executor = Arc::new(FilesystemResourceLifecycleExecutor::new(
        volume.clone(),
        Arc::clone(&snapshot_delivery_mounts),
    )?);
    for record in lifecycle_journal.list_unfinished()? {
        if !matches!(
            record.action,
            neoengram_domain::protocol::ResourceLifecycleAction::Restore
        ) {
            lifecycle_executor.prepare_fence(&record.command)?;
        }
    }
    let recovered_deliveries = tokio::task::spawn_blocking({
        let snapshot_delivery_mounts = Arc::clone(&snapshot_delivery_mounts);
        move || snapshot_delivery_mounts.recover()
    })
    .await
    .map_err(|error| {
        AgentDaemonError::Session(format!("SnapshotDelivery recovery task failed: {error}"))
    })?
    .map_err(AgentDaemonError::from)?;
    for outcome in recovered_deliveries {
        match outcome {
            SnapshotDeliveryRecoveryOutcome::Mounted { assignment, stats } => {
                tracing::debug!(
                    delivery_id = %assignment.delivery_id,
                    files = stats.files,
                    bytes = stats.bytes,
                    "recovered a persisted read-only SnapshotDelivery FUSE mount"
                );
            }
            SnapshotDeliveryRecoveryOutcome::Failed {
                assignment: Some(assignment),
                code,
                message,
            } => {
                tracing::warn!(
                    delivery_id = %assignment.delivery_id,
                    error_code = ?code,
                    error = %message,
                    "retained a SnapshotDelivery descriptor that could not be remounted"
                );
            }
            SnapshotDeliveryRecoveryOutcome::Failed {
                assignment: None,
                code,
                message,
            } => {
                tracing::error!(error_code = ?code, error = %message, "isolated an unreadable SnapshotDelivery descriptor");
            }
        }
    }
    deferred.install(Arc::new(
        CoreAgentMessageProcessor::with_materializers(
            Arc::clone(&agent),
            Arc::new(WorkspaceMaterializer::for_binding(
                config.storage.mount_path.clone(),
                volume.agent_id.clone(),
                volume.tenant_id.clone(),
                volume.storage_volume_id.clone(),
                volume.agent_mount_id.clone(),
                volume.mount_generation,
                volume.owner_generation,
                Arc::clone(&bridge),
            )),
            reports.clone(),
            Arc::clone(&clock),
        )
        .with_snapshot_delivery_materializer(Arc::new(
            crate::SnapshotDeliveryMaterializer::for_binding(
                config.storage.mount_path.clone(),
                volume.agent_id.clone(),
                volume.tenant_id.clone(),
                volume.storage_volume_id.clone(),
                volume.agent_mount_id.clone(),
                volume.mount_generation,
                volume.owner_generation,
                Arc::clone(&bridge),
            ),
        ))
        .with_snapshot_delivery_mounts(Arc::clone(&snapshot_delivery_mounts))
        .with_lifecycle_journal(lifecycle_journal)
        .with_lifecycle_binding(volume.clone(), binding.fence.session_generation)
        .with_lifecycle_executor(lifecycle_executor)
        .with_replication_executor(Arc::new(
            MountedVolumeReplicationExecutor::new_with_network_and_session_fence(
                Arc::clone(&execution),
                volume.tenant_id.clone(),
                volume.agent_id.clone(),
                volume.storage_volume_id.clone(),
                command_trust_bundle.clone(),
                Arc::clone(&clock),
                replication_network,
                Some(fence.clone()),
                Some(volume.mount_generation),
            ),
        )),
    ))?;
    let mut dispatcher = spawn_work_dispatcher(config.tenant_id.clone(), deferred, fence.clone());

    // The object channel is independent of the JSON/NDJSON control stream.  It is enabled only
    // when the Agent has Central ticket-verification keys; otherwise reads remain fail-closed.
    let s3_source: Arc<dyn S3SnapshotSource> = Arc::new(SnapshotCasReaderFactory::for_binding(
        config.storage.mount_path.clone(),
        volume.agent_id.clone(),
        volume.tenant_id.clone(),
        volume.storage_volume_id.clone(),
        volume.mount_generation,
        volume.owner_generation,
        binding.fence.session_generation,
        Arc::clone(&bridge),
    ));
    let mut s3_read_channel = command_trust_bundle.as_ref().map(|trust_bundle| {
        spawn_s3_read_channel(
            Arc::clone(&client),
            S3ReadChannelHello {
                agent_id: volume.agent_id.clone(),
                session_generation: binding.fence.session_generation,
            },
            S3ReadExecutor::new(Arc::clone(&s3_source), Arc::clone(trust_bundle)),
        )
    });

    for record in agent.active_assignments()? {
        dispatcher
            .sender
            .send(FencedAgentWork {
                generation: binding.fence.session_generation,
                work: AgentWork::Recovery(record),
            })
            .await
            .map_err(|_| {
                AgentDaemonError::Session(
                    "Agent Job dispatcher stopped while scheduling recovery".to_owned(),
                )
            })?;
    }
    reporter.set_phase(RuntimeHealthPhase::SessionReady)?;

    let mut heartbeat_sequence = 0_u64;
    loop {
        match run_channel(
            &mut channel,
            &mut shutdown,
            &probe,
            &agent,
            &volume,
            &boot_id,
            Arc::clone(&signer),
            reports.as_ref(),
            &fence,
            &resource_version,
            &mut dispatcher,
            &mut heartbeat_sequence,
            config.session.heartbeat_interval_seconds,
            command_trust_bundle.as_deref(),
            &mut reporter,
        )
        .await?
        {
            ChannelExit::Shutdown => return Ok(()),
            ChannelExit::Reconnect => {}
        }
        tokio::select! {
            _ = shutdown.as_mut() => return Ok(()),
            _ = time::sleep(INITIAL_RECONNECT_DELAY) => {}
        }
        let Some(connected) = connect_with_backoff(
            Arc::clone(&client),
            Arc::clone(&signer),
            &open_payload,
            &fence,
            &resource_version,
            Some(&binding),
            reconnect_max_delay,
            &mut shutdown,
        )
        .await?
        else {
            return Ok(());
        };
        channel = connected.0;
        // Fence and recreate the binary channel with every control reconnect.  Even when Central
        // keeps the same session generation, this prevents an H2 stream that raced disconnect
        // from surviving into the newly authenticated control session.
        drop(s3_read_channel.take());
        s3_read_channel = command_trust_bundle.as_ref().map(|trust_bundle| {
            spawn_s3_read_channel(
                Arc::clone(&client),
                S3ReadChannelHello {
                    agent_id: volume.agent_id.clone(),
                    session_generation: connected.1.fence.session_generation,
                },
                S3ReadExecutor::new(Arc::clone(&s3_source), Arc::clone(trust_bundle)),
            )
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelExit {
    Shutdown,
    Reconnect,
}

#[derive(Debug)]
enum PendingAck {
    Heartbeat {
        sequence: SequenceNumber,
        sent_at: time::Instant,
    },
    Report {
        sequence: SequenceNumber,
        durable_message_id: MessageId,
        sent_at: time::Instant,
    },
    Close {
        sequence: SequenceNumber,
        sent_at: time::Instant,
    },
}

impl PendingAck {
    const fn sequence(&self) -> SequenceNumber {
        match self {
            Self::Heartbeat { sequence, .. }
            | Self::Report { sequence, .. }
            | Self::Close { sequence, .. } => *sequence,
        }
    }

    fn sent_at(&self) -> time::Instant {
        match self {
            Self::Heartbeat { sent_at, .. }
            | Self::Report { sent_at, .. }
            | Self::Close { sent_at, .. } => *sent_at,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_with_backoff<C, F>(
    client: Arc<C>,
    signer: Arc<AgentRequestSigner>,
    open_payload: &AgentSessionOpenPayload,
    fence: &SharedSessionFence,
    resource_version: &SharedResourceVersion,
    expected_binding: Option<&AgentSessionBinding>,
    max_delay: Duration,
    shutdown: &mut std::pin::Pin<Box<F>>,
) -> AgentDaemonResult<Option<(AgentChannelConnection, AgentSessionBinding)>>
where
    C: AgentSessionClient,
    F: Future<Output = ()> + Send,
{
    let mut delay = INITIAL_RECONNECT_DELAY;
    loop {
        let attempt = tokio::select! {
            _ = shutdown.as_mut() => return Ok(None),
            result = connect_once(
                client.as_ref(),
                signer.as_ref(),
                open_payload.clone(),
                fence,
                resource_version,
                expected_binding,
            ) => result,
        };
        match attempt {
            Ok(value) => return Ok(Some(value)),
            Err(error) if error.retryable() => {
                tokio::select! {
                    _ = shutdown.as_mut() => return Ok(None),
                    _ = time::sleep(delay) => {}
                }
                delay = delay.saturating_mul(2).min(max_delay);
            }
            Err(error) => return Err(AgentDaemonError::Session(error.to_string())),
        }
    }
}

async fn connect_once<C: AgentSessionClient>(
    client: &C,
    signer: &AgentRequestSigner,
    open_payload: AgentSessionOpenPayload,
    fence: &SharedSessionFence,
    resource_version: &SharedResourceVersion,
    expected_binding: Option<&AgentSessionBinding>,
) -> Result<(AgentChannelConnection, AgentSessionBinding), AgentSessionClientError> {
    let message_id = fresh_message_id("channel-open")?;
    let open = signer.sign_channel(
        None,
        UnixMillis::new(now_unix_ms().map_err(AgentSessionClientError::protocol)?),
        AgentChannelUpstreamPayload {
            sequence: SequenceNumber::new(1),
            message_id: message_id.clone(),
            correlation_id: None,
            message: AgentChannelUpstreamMessage::Open(open_payload),
            extensions: Extensions::new(),
        },
    )?;
    let request_id = open.request.request_id.clone();
    let agent_id = open.request.agent_id.clone();
    let mut channel = client.connect_channel(&open).await?;
    let opened = time::timeout(CHANNEL_ACTION_TIMEOUT, channel.receive())
        .await
        .map_err(|_| AgentSessionClientError::transport("Agent channel Open timed out"))??
        .ok_or_else(|| {
            AgentSessionClientError::transport("Agent channel ended before Opened response")
        })?;
    opened
        .validate_sequence_after(None)
        .map_err(AgentSessionClientError::protocol)?;
    if opened.correlation_id.as_ref() != Some(&message_id) {
        return Err(AgentSessionClientError::protocol(
            "Agent channel Opened correlation does not match Open",
        ));
    }
    let generation = opened.session_generation;
    let AgentChannelDownstreamMessage::Opened(response) = opened.message else {
        return Err(AgentSessionClientError::protocol(
            "Agent channel first response is not channel.opened",
        ));
    };
    if response.request_id != request_id || response.agent_id != agent_id {
        return Err(AgentSessionClientError::protocol(
            "Agent channel Opened identity differs from Open",
        ));
    }
    if response.session_generation.get() == 0
        || response.mount_generation.get() == 0
        || response.owner_generation.get() == 0
        || response.resource_version.get() == 0
    {
        return Err(AgentSessionClientError::protocol(
            "Agent channel Opened contains a zero generation or resource version",
        ));
    }
    if generation != response.session_generation {
        return Err(AgentSessionClientError::protocol(
            "Agent channel Opened frame and payload use different generations",
        ));
    }
    let binding = AgentSessionBinding {
        fence: AgentSessionFence {
            session_id: response.session_id,
            session_generation: response.session_generation,
        },
        agent_mount_id: response.agent_mount_id,
        mount_generation: response.mount_generation,
        owner_generation: response.owner_generation,
    };
    if expected_binding.is_some_and(|expected| expected != &binding) {
        return Err(AgentSessionClientError::protocol(
            "reconnected Agent channel returned a different session or owner fence",
        ));
    }
    fence
        .replace(Some(binding.fence.clone()))
        .map_err(AgentSessionClientError::protocol)?;
    resource_version
        .replace(response.resource_version)
        .map_err(AgentSessionClientError::protocol)?;
    Ok((channel, binding))
}

#[allow(clippy::too_many_arguments)]
async fn run_channel<P, F>(
    channel: &mut AgentChannelConnection,
    shutdown: &mut std::pin::Pin<Box<F>>,
    probe: &P,
    agent: &Agent,
    volume: &SingleVolumeAgentConfig,
    boot_id: &AgentBootId,
    signer: Arc<AgentRequestSigner>,
    reports: &SqliteOutboundReportQueue,
    fence: &SharedSessionFence,
    resource_version: &SharedResourceVersion,
    dispatcher: &mut AgentWorkDispatcher,
    heartbeat_sequence: &mut u64,
    heartbeat_interval_seconds: u64,
    command_trust_bundle: Option<&CentralCommandTrustBundle>,
    reporter: &mut HealthReporter,
) -> AgentDaemonResult<ChannelExit>
where
    P: MountProbe,
    F: Future<Output = ()> + Send,
{
    let established = fence.get()?;
    let mut upstream_sequence = 1_u64;
    let mut downstream_sequence = SequenceNumber::new(1);
    let mut pending = BTreeMap::<MessageId, PendingAck>::new();
    let mut seen_commands = BTreeSet::<MessageId>::new();
    let mut heartbeat = time::interval(Duration::from_secs(heartbeat_interval_seconds));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut report_tick = time::interval(REPORT_INTERVAL);
    report_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ack_watchdog = time::interval(Duration::from_secs(1));
    ack_watchdog.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.as_mut() => {
                flush_outbound_reports(
                    channel,
                    signer.as_ref(),
                    &established,
                    &mut upstream_sequence,
                    &mut downstream_sequence,
                    reports,
                    resource_version,
                    &mut pending,
                    &mut seen_commands,
                    dispatcher,
                    volume.tenant_id.clone(),
                    command_trust_bundle,
                ).await?;
                send_close(channel, signer.as_ref(), &established, resource_version,
                    &mut upstream_sequence, &mut pending).await?;
                let closed = wait_for_close_ack(channel, &mut downstream_sequence,
                    established.session_generation, reports, resource_version, &mut pending,
                    &mut seen_commands, dispatcher, command_trust_bundle).await;
                if matches!(closed, Ok(ChannelExit::Shutdown)) {
                    fence.replace(None)?;
                }
                return closed;
            }
            error = dispatcher.errors.recv() => {
                return Err(error.unwrap_or_else(|| AgentDaemonError::Session(
                    "Agent Job dispatcher stopped unexpectedly".to_owned()
                )));
            }
            received = channel.receive() => {
                let frame = match received {
                    Ok(Some(frame)) => frame,
                    Ok(None) => return Ok(ChannelExit::Reconnect),
                    Err(error) if error.retryable() => return Ok(ChannelExit::Reconnect),
                    Err(error) => return Err(AgentDaemonError::Session(error.to_string())),
                };
                if let AgentChannelDownstreamMessage::Error(error) = &frame.message {
                    let stale = matches!(
                        error.code.as_str(),
                        "STALE_SESSION_GENERATION" | "SESSION_FENCE_MISMATCH"
                    );
                    if error.retryable && !stale {
                        frame.validate_sequence_after(Some(downstream_sequence))
                            .and_then(|()| frame.validate_session_generation(
                                established.session_generation
                            ))
                            .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
                        return Ok(ChannelExit::Reconnect);
                    }
                }
                handle_downstream(frame, &mut downstream_sequence,
                    established.session_generation, reports, resource_version, &mut pending,
                    &mut seen_commands, dispatcher, command_trust_bundle).await?;
            }
            _ = heartbeat.tick() => {
                if !pending.values().any(|value| matches!(value, PendingAck::Heartbeat { .. })) {
                    *heartbeat_sequence = heartbeat_sequence.checked_add(1).ok_or_else(|| {
                        AgentDaemonError::Session("heartbeat sequence overflow".to_owned())
                    })?;
                    let payload = heartbeat_payload(resource_version, agent, volume, boot_id,
                        established.session_generation, *heartbeat_sequence, probe.probe())?;
                    let (message_id, sequence) = send_signed(channel, signer.as_ref(), &established,
                        &mut upstream_sequence, None,
                        AgentChannelUpstreamMessage::Heartbeat(payload), None).await?;
                    pending.insert(
                        message_id,
                        PendingAck::Heartbeat {
                            sequence,
                            sent_at: time::Instant::now(),
                        },
                    );
                }
                reporter.refresh()?;
            }
            _ = report_tick.tick() => {
                send_queued_reports(channel, signer.as_ref(), &established, reports,
                    &mut upstream_sequence, &mut pending, volume.tenant_id.clone()).await?;
            }
            _ = ack_watchdog.tick() => {
                let now = time::Instant::now();
                if pending.values().any(|ack| {
                    now.duration_since(ack.sent_at()) >= CHANNEL_ACTION_TIMEOUT
                }) {
                    return Ok(ChannelExit::Reconnect);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_outbound_reports(
    channel: &mut AgentChannelConnection,
    signer: &AgentRequestSigner,
    fence: &AgentSessionFence,
    upstream_sequence: &mut u64,
    downstream_sequence: &mut SequenceNumber,
    reports: &dyn OutboundReportQueue,
    resource_version: &SharedResourceVersion,
    pending: &mut BTreeMap<MessageId, PendingAck>,
    seen_commands: &mut BTreeSet<MessageId>,
    dispatcher: &AgentWorkDispatcher,
    tenant_id: TenantId,
    command_trust_bundle: Option<&CentralCommandTrustBundle>,
) -> AgentDaemonResult<()> {
    loop {
        send_queued_reports(
            channel,
            signer,
            fence,
            reports,
            upstream_sequence,
            pending,
            tenant_id.clone(),
        )
        .await?;
        if pending.is_empty() && reports.list(1)?.is_empty() {
            return Ok(());
        }
        drain_pending_acks(
            channel,
            downstream_sequence,
            fence.session_generation,
            reports,
            resource_version,
            pending,
            seen_commands,
            dispatcher,
            command_trust_bundle,
        )
        .await?;
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_pending_acks(
    channel: &mut AgentChannelConnection,
    previous_sequence: &mut SequenceNumber,
    generation: SessionGeneration,
    reports: &dyn OutboundReportQueue,
    resource_version: &SharedResourceVersion,
    pending: &mut BTreeMap<MessageId, PendingAck>,
    seen_commands: &mut BTreeSet<MessageId>,
    dispatcher: &AgentWorkDispatcher,
    command_trust_bundle: Option<&CentralCommandTrustBundle>,
) -> AgentDaemonResult<()> {
    time::timeout(CHANNEL_ACTION_TIMEOUT, async {
        while !pending.is_empty() {
            let frame = channel
                .receive()
                .await
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?
                .ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Agent channel ended while draining pending acknowledgements".to_owned(),
                    )
                })?;
            handle_downstream(
                frame,
                previous_sequence,
                generation,
                reports,
                resource_version,
                pending,
                seen_commands,
                dispatcher,
                command_trust_bundle,
            )
            .await?;
        }
        Ok(())
    })
    .await
    .map_err(|_| {
        AgentDaemonError::Session(
            "Agent channel pending acknowledgement drain timed out".to_owned(),
        )
    })?
}

async fn send_signed(
    channel: &AgentChannelConnection,
    signer: &AgentRequestSigner,
    fence: &AgentSessionFence,
    upstream_sequence: &mut u64,
    message_id: Option<MessageId>,
    message: AgentChannelUpstreamMessage,
    correlation_id: Option<MessageId>,
) -> AgentDaemonResult<(MessageId, SequenceNumber)> {
    *upstream_sequence = upstream_sequence
        .checked_add(1)
        .ok_or_else(|| AgentDaemonError::Session("channel sequence overflow".to_owned()))?;
    let sequence = SequenceNumber::new(*upstream_sequence);
    let message_id = match message_id {
        Some(value) => value,
        None => fresh_message_id("channel-message")
            .map_err(|error| AgentDaemonError::Session(error.to_string()))?,
    };
    let frame = signer
        .sign_channel(
            Some(fence),
            UnixMillis::new(now_unix_ms()?),
            AgentChannelUpstreamPayload {
                sequence,
                message_id: message_id.clone(),
                correlation_id,
                message,
                extensions: Extensions::new(),
            },
        )
        .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
    channel
        .writer
        .send(&frame)
        .await
        .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
    Ok((message_id, sequence))
}

#[allow(clippy::too_many_arguments)]
async fn send_queued_reports(
    channel: &AgentChannelConnection,
    signer: &AgentRequestSigner,
    fence: &AgentSessionFence,
    reports: &dyn OutboundReportQueue,
    upstream_sequence: &mut u64,
    pending: &mut BTreeMap<MessageId, PendingAck>,
    tenant_id: TenantId,
) -> AgentDaemonResult<()> {
    for queued in reports.list(MAX_REPORTS_PER_FLUSH)? {
        if pending.contains_key(&queued.message_id) {
            continue;
        }
        let request_id = RequestId::new(queued.message_id.to_string())
            .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
        let trace_id = TraceId::new(format!("report-{}", queued.message_id))
            .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
        let envelope = new_control_envelope(
            request_id,
            trace_id,
            Some(tenant_id.clone()),
            Some(fence.session_generation),
            queued.enqueued_at_unix_ms,
            queued.report.clone().into_control_message(),
        );
        neoengram_domain::protocol::validate_control_envelope(&envelope)
            .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
        let durable_message_id = queued.message_id.clone();
        let (message_id, sequence) = send_signed(
            channel,
            signer,
            fence,
            upstream_sequence,
            Some(durable_message_id.clone()),
            AgentChannelUpstreamMessage::Report(AgentJobReportCreatePayload {
                tenant_id: tenant_id.clone(),
                report: envelope,
                extensions: Extensions::new(),
            }),
            None,
        )
        .await?;
        pending.insert(
            message_id,
            PendingAck::Report {
                sequence,
                durable_message_id,
                sent_at: time::Instant::now(),
            },
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_downstream(
    frame: AgentChannelDownstreamFrame,
    previous_sequence: &mut SequenceNumber,
    generation: SessionGeneration,
    reports: &dyn OutboundReportQueue,
    resource_version: &SharedResourceVersion,
    pending: &mut BTreeMap<MessageId, PendingAck>,
    seen_commands: &mut BTreeSet<MessageId>,
    dispatcher: &AgentWorkDispatcher,
    command_trust_bundle: Option<&CentralCommandTrustBundle>,
) -> AgentDaemonResult<()> {
    frame
        .validate_sequence_after(Some(*previous_sequence))
        .and_then(|()| frame.validate_session_generation(generation))
        .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
    if let Some(bundle) = command_trust_bundle {
        bundle.verify_downstream_if_command(&frame, UnixMillis::new(now_unix_ms()?))?;
    }
    *previous_sequence = frame.sequence;
    match frame.message {
        AgentChannelDownstreamMessage::Opened(_) => Err(AgentDaemonError::Session(
            "Agent channel received a second Opened response".to_owned(),
        )),
        AgentChannelDownstreamMessage::Assignment(assignment) => {
            if !seen_commands.insert(frame.message_id) {
                return Ok(());
            }
            dispatcher
                .sender
                .send(FencedAgentWork {
                    generation,
                    work: AgentWork::Assignment(*assignment),
                })
                .await
                .map_err(|_| AgentDaemonError::Session("Agent Job dispatcher is closed".to_owned()))
        }
        AgentChannelDownstreamMessage::ReplicationAssignment(assignment) => {
            if !seen_commands.insert(frame.message_id) {
                return Ok(());
            }
            dispatcher
                .sender
                .send(FencedAgentWork {
                    generation,
                    work: AgentWork::Replication(*assignment),
                })
                .await
                .map_err(|_| {
                    AgentDaemonError::Session("Agent replication dispatcher is closed".to_owned())
                })
        }
        AgentChannelDownstreamMessage::LifecycleAssignment(assignment) => {
            if !seen_commands.insert(frame.message_id) {
                return Ok(());
            }
            if assignment.session_generation != generation {
                return Err(AgentDaemonError::Session(
                    "Agent lifecycle assignment carries a stale session generation".to_owned(),
                ));
            }
            dispatcher
                .sender
                .send(FencedAgentWork {
                    generation,
                    work: AgentWork::Lifecycle(*assignment),
                })
                .await
                .map_err(|_| {
                    AgentDaemonError::Session("Agent lifecycle dispatcher is closed".to_owned())
                })
        }
        AgentChannelDownstreamMessage::Decision(decision) => {
            if !seen_commands.insert(frame.message_id) {
                return Ok(());
            }
            dispatcher
                .sender
                .send(FencedAgentWork {
                    generation,
                    work: AgentWork::Decision(decision),
                })
                .await
                .map_err(|_| AgentDaemonError::Session("Agent Job dispatcher is closed".to_owned()))
        }
        AgentChannelDownstreamMessage::Ack(ack) => {
            let correlation = frame.correlation_id.ok_or_else(|| {
                AgentDaemonError::Session("Agent channel Ack omitted correlation".to_owned())
            })?;
            acknowledge(
                correlation,
                ack.acknowledged_sequence,
                ack.resource_version,
                reports,
                resource_version,
                pending,
            )
            .map(|_| ())
        }
        AgentChannelDownstreamMessage::Error(error) => {
            // A non-retryable report rejection is a terminal outcome for that durable
            // observation (for example, a report for a replication that Central cancelled).
            // Drop only the correlated report so one obsolete item cannot prevent the Agent
            // from reconnecting and accepting newer assignments.
            if error.code.as_str() == "AGENT_ACTION_REJECTED" && !error.retryable {
                let Some(correlation) = frame.correlation_id.as_ref() else {
                    return Err(AgentDaemonError::Session(
                        "Agent report rejection omitted correlation".to_owned(),
                    ));
                };
                let is_report = matches!(pending.get(correlation), Some(PendingAck::Report { .. }));
                if is_report {
                    let Some(PendingAck::Report {
                        durable_message_id, ..
                    }) = pending.remove(correlation)
                    else {
                        return Err(AgentDaemonError::Session(
                            "Agent report rejection correlation disappeared".to_owned(),
                        ));
                    };
                    if !reports.acknowledge(&durable_message_id)? {
                        return Err(AgentDaemonError::Session(
                            "rejected outbound report disappeared from the local queue".to_owned(),
                        ));
                    }
                    return Ok(());
                }
            }
            let stale = matches!(
                error.code.as_str(),
                "STALE_SESSION_GENERATION" | "SESSION_FENCE_MISMATCH"
            );
            Err(AgentDaemonError::Session(format!(
                "Agent channel protocol error {} (retryable={}, stale_fence={}): {}",
                error.code.as_str(),
                error.retryable,
                stale,
                error.message
            )))
        }
    }
}

fn acknowledge(
    correlation: MessageId,
    acknowledged_sequence: SequenceNumber,
    acknowledged_resource_version: ResourceVersion,
    reports: &dyn OutboundReportQueue,
    resource_version: &SharedResourceVersion,
    pending: &mut BTreeMap<MessageId, PendingAck>,
) -> AgentDaemonResult<PendingAck> {
    let expected = pending.get(&correlation).ok_or_else(|| {
        AgentDaemonError::Session("Agent channel Ack has an unknown correlation".to_owned())
    })?;
    if expected.sequence() != acknowledged_sequence {
        return Err(AgentDaemonError::Session(
            "Agent channel Ack sequence does not match its request".to_owned(),
        ));
    }
    let acknowledged = pending.remove(&correlation).ok_or_else(|| {
        AgentDaemonError::Session("Agent channel Ack correlation disappeared".to_owned())
    })?;
    match &acknowledged {
        PendingAck::Heartbeat { .. } => resource_version.replace(acknowledged_resource_version)?,
        PendingAck::Report {
            durable_message_id, ..
        } => {
            if !reports.acknowledge(durable_message_id)? {
                return Err(AgentDaemonError::Session(
                    "acknowledged outbound report disappeared from the local queue".to_owned(),
                ));
            }
        }
        PendingAck::Close { .. } => {}
    }
    Ok(acknowledged)
}

async fn send_close(
    channel: &AgentChannelConnection,
    signer: &AgentRequestSigner,
    fence: &AgentSessionFence,
    resource_version: &SharedResourceVersion,
    upstream_sequence: &mut u64,
    pending: &mut BTreeMap<MessageId, PendingAck>,
) -> AgentDaemonResult<()> {
    let (message_id, sequence) = send_signed(
        channel,
        signer,
        fence,
        upstream_sequence,
        None,
        AgentChannelUpstreamMessage::Close(AgentSessionClosePayload {
            expected_resource_version: resource_version.get()?,
            extensions: Extensions::new(),
        }),
        None,
    )
    .await?;
    pending.insert(
        message_id,
        PendingAck::Close {
            sequence,
            sent_at: time::Instant::now(),
        },
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_close_ack(
    channel: &mut AgentChannelConnection,
    previous_sequence: &mut SequenceNumber,
    generation: SessionGeneration,
    reports: &dyn OutboundReportQueue,
    resource_version: &SharedResourceVersion,
    pending: &mut BTreeMap<MessageId, PendingAck>,
    seen_commands: &mut BTreeSet<MessageId>,
    dispatcher: &AgentWorkDispatcher,
    command_trust_bundle: Option<&CentralCommandTrustBundle>,
) -> AgentDaemonResult<ChannelExit> {
    time::timeout(CHANNEL_ACTION_TIMEOUT, async {
        loop {
            let frame = channel
                .receive()
                .await
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?
                .ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Agent channel ended before Close acknowledgement".to_owned(),
                    )
                })?;
            let is_close_ack = matches!(frame.message, AgentChannelDownstreamMessage::Ack(_))
                && frame.correlation_id.as_ref().is_some_and(|correlation| {
                    matches!(pending.get(correlation), Some(PendingAck::Close { .. }))
                });
            handle_downstream(
                frame,
                previous_sequence,
                generation,
                reports,
                resource_version,
                pending,
                seen_commands,
                dispatcher,
                command_trust_bundle,
            )
            .await?;
            if is_close_ack {
                return Ok(ChannelExit::Shutdown);
            }
        }
    })
    .await
    .map_err(|_| {
        AgentDaemonError::Session("Agent channel Close acknowledgement timed out".to_owned())
    })?
}

fn fresh_message_id(prefix: &str) -> Result<MessageId, AgentSessionClientError> {
    MessageId::new(format!("{prefix}-{}", Uuid::new_v4().simple()))
        .map_err(AgentSessionClientError::protocol)
}

#[allow(clippy::too_many_arguments)]
fn heartbeat_payload(
    resource_version: &SharedResourceVersion,
    agent: &Agent,
    volume: &SingleVolumeAgentConfig,
    boot_id: &AgentBootId,
    session_generation: SessionGeneration,
    sequence: u64,
    observation: crate::FilesystemMountObservation,
) -> AgentDaemonResult<AgentHeartbeatReportPayload> {
    super::runtime::validate_bootstrap_probe(&observation)?;
    let observed_at = UnixMillis::new(now_unix_ms()?);
    let sequence = SequenceNumber::new(sequence);
    let mount_report = volume
        .mount_status(SingleVolumeMountProbe {
            boot_id: boot_id.clone(),
            session_generation,
            sequence,
            observed_volume_marker: observation.observed_volume_marker.ok_or_else(|| {
                AgentDaemonError::MountProbe("Volume marker disappeared".to_owned())
            })?,
            mount_identity_digest: observation.mount_identity_digest.ok_or_else(|| {
                AgentDaemonError::MountProbe("mount identity disappeared".to_owned())
            })?,
            access_mode: observation.access_mode.ok_or_else(|| {
                AgentDaemonError::MountProbe("mount access mode disappeared".to_owned())
            })?,
            health: observation.health,
            available_bytes: observation.available_bytes.ok_or_else(|| {
                AgentDaemonError::MountProbe("mount available space disappeared".to_owned())
            })?,
            observed_at_unix_ms: observed_at,
        })?
        .report;
    let running_jobs = agent
        .active_assignments()?
        .into_iter()
        .map(|record| RunningJobObservation {
            job_id: record.assignment.job_id,
            assignment_id: record.assignment.assignment_id,
            assignment_generation: record.assignment.assignment_generation,
            state: local_job_state(record.state),
            extensions: Extensions::new(),
        })
        .collect();
    Ok(AgentHeartbeatReportPayload {
        heartbeat: AgentHeartbeat {
            agent_id: volume.agent_id.clone(),
            observed_at_unix_ms: observed_at,
            acknowledged_resource_version: resource_version.get()?,
            sequence,
            running_jobs,
            mount_observations: Vec::new(),
            extensions: Extensions::new(),
        },
        mount_report,
        extensions: Extensions::new(),
    })
}

const fn local_job_state(state: AgentAssignmentState) -> JobState {
    match state {
        AgentAssignmentState::Claimed => JobState::Assigned,
        AgentAssignmentState::Accepted => JobState::Accepted,
        AgentAssignmentState::Running | AgentAssignmentState::Recovering => JobState::Running,
        AgentAssignmentState::Prepared | AgentAssignmentState::AwaitingDecision => {
            JobState::Prepared
        }
        AgentAssignmentState::Finalizing => JobState::Publishing,
        AgentAssignmentState::Completed => JobState::Succeeded,
        AgentAssignmentState::FailedReported => JobState::Failed,
    }
}

fn now_unix_ms() -> AgentDaemonResult<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AgentDaemonError::Session(error.to_string()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| AgentDaemonError::Session("system timestamp exceeds u64".to_owned()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{
            atomic::{AtomicU64, Ordering},
            Mutex,
        },
    };

    use crate::{AgentReport, AgentResult, QueuedAgentReport};
    use async_trait::async_trait;
    use neoengram_domain::core::ContentDigest;
    use neoengram_domain::protocol::{
        AgentChannelAck, AgentChannelUpstreamFrame, AgentInstallationId, AgentMountId, ArtifactId,
        AssignmentGeneration, AssignmentId, AssignmentOperation, CentralSignedPayload,
        CertificateGeneration, DecimalU64, DeliveryGeneration, Ed25519PublicKeySpki,
        Ed25519Signature, GatewayOpaqueBytes, HardlinkPolicy, JobAssignment, JobId,
        MountGeneration, OwnerGeneration, PlacementGeneration, PrincipalId, PrincipalKind,
        PrincipalRef, ProjectId, SessionId, SnapshotDeliveryAction, SnapshotDeliveryAssignment,
        SnapshotDeliveryMode, SnapshotId, StorageVolumeId, CURRENT_WIRE_VERSION,
    };
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair as _},
    };

    use super::*;

    #[derive(Debug, Default)]
    struct AckQueue(Mutex<BTreeSet<MessageId>>);

    impl OutboundReportQueue for AckQueue {
        fn enqueue(
            &self,
            _report: AgentReport,
            _enqueued_at_unix_ms: UnixMillis,
        ) -> AgentResult<QueuedAgentReport> {
            unreachable!("Ack test does not enqueue reports")
        }

        fn list(&self, _limit: usize) -> AgentResult<Vec<QueuedAgentReport>> {
            unreachable!("Ack test does not list reports")
        }

        fn acknowledge(&self, message_id: &MessageId) -> AgentResult<bool> {
            Ok(self.0.lock().unwrap().remove(message_id))
        }
    }

    #[derive(Debug)]
    struct DurableQueue(Mutex<Option<QueuedAgentReport>>);

    impl OutboundReportQueue for DurableQueue {
        fn enqueue(
            &self,
            _report: AgentReport,
            _enqueued_at_unix_ms: UnixMillis,
        ) -> AgentResult<QueuedAgentReport> {
            unreachable!("shutdown flush test starts with a durable report")
        }

        fn list(&self, _limit: usize) -> AgentResult<Vec<QueuedAgentReport>> {
            Ok(self.0.lock().unwrap().iter().cloned().collect())
        }

        fn acknowledge(&self, message_id: &MessageId) -> AgentResult<bool> {
            let mut queued = self.0.lock().unwrap();
            if queued
                .as_ref()
                .is_some_and(|report| &report.message_id == message_id)
            {
                queued.take();
                return Ok(true);
            }
            Ok(false)
        }
    }

    #[derive(Debug)]
    struct NoopProcessor;

    #[async_trait]
    impl AgentMessageProcessor for NoopProcessor {
        async fn handle_assignment(&self, _assignment: JobAssignment) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn handle_replication(
            &self,
            _assignment: neoengram_domain::protocol::ReplicationAssignment,
        ) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn handle_lifecycle_assignment(
            &self,
            _assignment: neoengram_domain::protocol::AgentResourceLifecycleAssignment,
        ) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn recover_assignment(&self, _record: crate::LedgerRecord) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn handle_decision(
            &self,
            _tenant_id: &TenantId,
            _decision: JobDecision,
        ) -> AgentDaemonResult<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct CountingProcessor(Arc<AtomicU64>);

    #[async_trait]
    impl AgentMessageProcessor for CountingProcessor {
        async fn handle_assignment(&self, _assignment: JobAssignment) -> AgentDaemonResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn handle_replication(
            &self,
            _assignment: neoengram_domain::protocol::ReplicationAssignment,
        ) -> AgentDaemonResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn handle_lifecycle_assignment(
            &self,
            _assignment: neoengram_domain::protocol::AgentResourceLifecycleAssignment,
        ) -> AgentDaemonResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn recover_assignment(&self, _record: crate::LedgerRecord) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn handle_decision(
            &self,
            _tenant_id: &TenantId,
            _decision: JobDecision,
        ) -> AgentDaemonResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn signed_assignment_frame(
        key: &Ed25519KeyPair,
        now_unix_ms: u64,
    ) -> AgentChannelDownstreamFrame {
        let mut frame = AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(2),
            message_id: MessageId::new("signed-assignment-frame").unwrap(),
            correlation_id: None,
            session_generation: SessionGeneration::new(3),
            sent_at_unix_ms: UnixMillis::new(now_unix_ms),
            central_signature: None,
            message: AgentChannelDownstreamMessage::Assignment(Box::new(JobAssignment {
                assignment: AssignmentOperation::SnapshotDelivery {
                    input: snapshot_assignment(),
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            })),
            extensions: Extensions::new(),
        };
        let payload =
            GatewayOpaqueBytes::new(frame.central_command_payload_bytes().unwrap()).unwrap();
        let mut signed = CentralSignedPayload {
            key_id: "central-command-a".to_owned(),
            certificate_generation: CertificateGeneration::new(1),
            signed_at_unix_ms: UnixMillis::new(now_unix_ms),
            expires_at_unix_ms: UnixMillis::new(now_unix_ms + 30_000),
            payload_digest: ContentDigest::hash(payload.as_bytes()),
            payload,
            signature: Ed25519Signature::from_bytes([0; 64]),
            extensions: Extensions::new(),
        };
        signed.signature =
            Ed25519Signature::new(key.sign(&signed.signing_bytes().unwrap()).as_ref().to_vec())
                .unwrap();
        frame.central_signature = Some(signed);
        frame
    }

    #[tokio::test]
    async fn central_signature_is_checked_before_assignment_dispatch() {
        let key_document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(key_document.as_ref()).unwrap();
        let public_key = Ed25519PublicKeySpki::from_public_key_bytes(
            key.public_key().as_ref().try_into().unwrap(),
        );
        let bundle = crate::command_trust::CentralCommandTrustBundle::from_test_keys(vec![(
            "central-command-a".to_owned(),
            CertificateGeneration::new(1),
            public_key,
            crate::command_trust::TrustKeyState::Active,
        )])
        .unwrap();
        let dispatched = Arc::new(AtomicU64::new(0));
        let shared_fence = SharedSessionFence::default();
        shared_fence
            .replace(Some(AgentSessionFence {
                session_id: SessionId::new("session-a").unwrap(),
                session_generation: SessionGeneration::new(3),
            }))
            .unwrap();
        let processor = Arc::new(CountingProcessor(Arc::clone(&dispatched)));
        let dispatcher =
            spawn_work_dispatcher(TenantId::new("tenant-a").unwrap(), processor, shared_fence);
        let queue = AckQueue::default();
        let resource_version = SharedResourceVersion::new(ResourceVersion::new(1));
        let mut previous = SequenceNumber::new(1);
        let mut pending = BTreeMap::new();
        let mut seen = BTreeSet::new();
        let now = now_unix_ms().unwrap();
        let unsigned = {
            let mut frame = signed_assignment_frame(&key, now);
            frame.central_signature = None;
            frame
        };
        assert!(handle_downstream(
            unsigned,
            &mut previous,
            SessionGeneration::new(3),
            &queue,
            &resource_version,
            &mut pending,
            &mut seen,
            &dispatcher,
            Some(&bundle),
        )
        .await
        .is_err());
        assert_eq!(previous, SequenceNumber::new(1));
        assert!(seen.is_empty());
        assert_eq!(dispatched.load(Ordering::SeqCst), 0);

        handle_downstream(
            signed_assignment_frame(&key, now),
            &mut previous,
            SessionGeneration::new(3),
            &queue,
            &resource_version,
            &mut pending,
            &mut seen,
            &dispatcher,
            Some(&bundle),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while dispatched.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn durable_report_is_deleted_only_by_exact_ack() {
        let durable_id = MessageId::new("report-a").unwrap();
        let wrong_id = MessageId::new("report-b").unwrap();
        let queue = AckQueue::default();
        queue.0.lock().unwrap().insert(durable_id.clone());
        let mut pending = BTreeMap::from([(
            durable_id.clone(),
            PendingAck::Report {
                sequence: SequenceNumber::new(7),
                durable_message_id: durable_id.clone(),
                sent_at: time::Instant::now(),
            },
        )]);
        let resource_version = SharedResourceVersion::new(ResourceVersion::new(3));

        assert!(acknowledge(
            wrong_id,
            SequenceNumber::new(7),
            ResourceVersion::new(4),
            &queue,
            &resource_version,
            &mut pending,
        )
        .is_err());
        assert!(acknowledge(
            durable_id.clone(),
            SequenceNumber::new(8),
            ResourceVersion::new(4),
            &queue,
            &resource_version,
            &mut pending,
        )
        .is_err());
        assert!(queue.0.lock().unwrap().contains(&durable_id));
        assert!(pending.contains_key(&durable_id));

        acknowledge(
            durable_id.clone(),
            SequenceNumber::new(7),
            ResourceVersion::new(4),
            &queue,
            &resource_version,
            &mut pending,
        )
        .unwrap();
        assert!(!queue.0.lock().unwrap().contains(&durable_id));
        assert!(pending.is_empty());
    }

    #[test]
    fn durable_report_survives_disconnect_before_ack_and_is_acked_after_reconnect() {
        let durable_id = MessageId::new("report-reconnect").unwrap();
        let queue = AckQueue::default();
        queue.0.lock().unwrap().insert(durable_id.clone());
        let first_connection = BTreeMap::from([(
            durable_id.clone(),
            PendingAck::Report {
                sequence: SequenceNumber::new(4),
                durable_message_id: durable_id.clone(),
                sent_at: time::Instant::now(),
            },
        )]);

        drop(first_connection);
        assert!(queue.0.lock().unwrap().contains(&durable_id));

        let mut reconnected = BTreeMap::from([(
            durable_id.clone(),
            PendingAck::Report {
                sequence: SequenceNumber::new(2),
                durable_message_id: durable_id.clone(),
                sent_at: time::Instant::now(),
            },
        )]);
        acknowledge(
            durable_id.clone(),
            SequenceNumber::new(2),
            ResourceVersion::new(5),
            &queue,
            &SharedResourceVersion::new(ResourceVersion::new(3)),
            &mut reconnected,
        )
        .unwrap();
        assert!(!queue.0.lock().unwrap().contains(&durable_id));
    }

    #[tokio::test]
    async fn shutdown_flushes_an_unsent_durable_report_before_close() {
        let generation = SessionGeneration::new(3);
        let durable_id = MessageId::new("report-before-close").unwrap();
        let queue = DurableQueue(Mutex::new(Some(QueuedAgentReport {
            sequence: 1,
            message_id: durable_id.clone(),
            enqueued_at_unix_ms: UnixMillis::new(10),
            report: AgentReport::Accepted(neoengram_domain::protocol::JobAccepted {
                job_id: JobId::new("job-before-close").unwrap(),
                assignment_id: AssignmentId::new("assignment-before-close").unwrap(),
                assignment_generation: AssignmentGeneration::new(1),
                accepted_at_unix_ms: UnixMillis::new(10),
                request_digest: "11".repeat(32).parse().unwrap(),
                extensions: Extensions::new(),
            }),
        })));
        let (outgoing, mut outgoing_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(4);
        let (incoming_tx, incoming) = tokio::sync::mpsc::channel(4);
        let reader = tokio::spawn(async {});
        let mut channel = AgentChannelConnection::new(outgoing, incoming, vec![reader]);
        incoming_tx
            .send(Ok(AgentChannelDownstreamFrame {
                wire_version: CURRENT_WIRE_VERSION,
                sequence: SequenceNumber::new(2),
                message_id: MessageId::new("report-ack").unwrap(),
                correlation_id: Some(durable_id.clone()),
                session_generation: generation,
                sent_at_unix_ms: UnixMillis::new(20),
                central_signature: None,
                message: AgentChannelDownstreamMessage::Ack(AgentChannelAck {
                    acknowledged_sequence: SequenceNumber::new(2),
                    resource_version: ResourceVersion::new(4),
                    replayed: false,
                    extensions: Extensions::new(),
                }),
                extensions: Extensions::new(),
            }))
            .await
            .unwrap();

        let key_document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer = AgentRequestSigner::new(
            AgentId::new("agent-a").unwrap(),
            AgentInstallationId::new("installation-a").unwrap(),
            AgentBootId::new("boot-a").unwrap(),
            Arc::new(Ed25519KeyPair::from_pkcs8(key_document.as_ref()).unwrap()),
        )
        .unwrap();
        let fence = AgentSessionFence {
            session_id: SessionId::new("session-a").unwrap(),
            session_generation: generation,
        };
        let shared_fence = SharedSessionFence::default();
        shared_fence.replace(Some(fence.clone())).unwrap();
        let dispatcher = spawn_work_dispatcher(
            TenantId::new("tenant-a").unwrap(),
            Arc::new(NoopProcessor),
            shared_fence,
        );
        let resource_version = SharedResourceVersion::new(ResourceVersion::new(3));
        let mut upstream_sequence = 1;
        let mut downstream_sequence = SequenceNumber::new(1);
        let mut pending = BTreeMap::new();
        let mut seen_commands = BTreeSet::new();

        flush_outbound_reports(
            &mut channel,
            &signer,
            &fence,
            &mut upstream_sequence,
            &mut downstream_sequence,
            &queue,
            &resource_version,
            &mut pending,
            &mut seen_commands,
            &dispatcher,
            TenantId::new("tenant-a").unwrap(),
            None,
        )
        .await
        .unwrap();

        let encoded = outgoing_rx.recv().await.unwrap();
        let report =
            AgentChannelUpstreamFrame::decode_json(encoded.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(report.request.payload.sequence, SequenceNumber::new(2));
        assert_eq!(report.request.payload.message_id, durable_id);
        assert!(matches!(
            report.request.payload.message,
            AgentChannelUpstreamMessage::Report(_)
        ));
        assert!(queue.list(1).unwrap().is_empty());
        assert_eq!(resource_version.get().unwrap(), ResourceVersion::new(3));
    }

    #[tokio::test]
    async fn shutdown_drains_heartbeat_ack_before_signing_close() {
        let generation = SessionGeneration::new(3);
        let heartbeat_id = MessageId::new("heartbeat-before-close").unwrap();
        let (outgoing, mut outgoing_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(4);
        let (incoming_tx, incoming) = tokio::sync::mpsc::channel(4);
        let reader = tokio::spawn(async {});
        let mut channel = AgentChannelConnection::new(outgoing, incoming, vec![reader]);
        incoming_tx
            .send(Ok(AgentChannelDownstreamFrame {
                wire_version: CURRENT_WIRE_VERSION,
                sequence: SequenceNumber::new(2),
                message_id: MessageId::new("heartbeat-ack").unwrap(),
                correlation_id: Some(heartbeat_id.clone()),
                session_generation: generation,
                sent_at_unix_ms: UnixMillis::new(20),
                central_signature: None,
                message: AgentChannelDownstreamMessage::Ack(AgentChannelAck {
                    acknowledged_sequence: SequenceNumber::new(2),
                    resource_version: ResourceVersion::new(4),
                    replayed: false,
                    extensions: Extensions::new(),
                }),
                extensions: Extensions::new(),
            }))
            .await
            .unwrap();

        let queue = AckQueue::default();
        let resource_version = SharedResourceVersion::new(ResourceVersion::new(3));
        let mut pending = BTreeMap::from([(
            heartbeat_id,
            PendingAck::Heartbeat {
                sequence: SequenceNumber::new(2),
                sent_at: time::Instant::now(),
            },
        )]);
        let mut downstream_sequence = SequenceNumber::new(1);
        let mut seen_commands = BTreeSet::new();
        let shared_fence = SharedSessionFence::default();
        shared_fence
            .replace(Some(AgentSessionFence {
                session_id: SessionId::new("session-a").unwrap(),
                session_generation: generation,
            }))
            .unwrap();
        let dispatcher = spawn_work_dispatcher(
            TenantId::new("tenant-a").unwrap(),
            Arc::new(NoopProcessor),
            shared_fence,
        );

        drain_pending_acks(
            &mut channel,
            &mut downstream_sequence,
            generation,
            &queue,
            &resource_version,
            &mut pending,
            &mut seen_commands,
            &dispatcher,
            None,
        )
        .await
        .unwrap();
        assert_eq!(resource_version.get().unwrap(), ResourceVersion::new(4));

        let key_document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer = AgentRequestSigner::new(
            AgentId::new("agent-a").unwrap(),
            AgentInstallationId::new("installation-a").unwrap(),
            AgentBootId::new("boot-a").unwrap(),
            Arc::new(Ed25519KeyPair::from_pkcs8(key_document.as_ref()).unwrap()),
        )
        .unwrap();
        let fence = AgentSessionFence {
            session_id: SessionId::new("session-a").unwrap(),
            session_generation: generation,
        };
        let mut upstream_sequence = 2;
        send_close(
            &channel,
            &signer,
            &fence,
            &resource_version,
            &mut upstream_sequence,
            &mut pending,
        )
        .await
        .unwrap();

        let encoded = outgoing_rx.recv().await.unwrap();
        let line = encoded.strip_suffix(b"\n").unwrap();
        let close = AgentChannelUpstreamFrame::decode_json(line).unwrap();
        let AgentChannelUpstreamMessage::Close(payload) = close.request.payload.message else {
            panic!("shutdown did not send a Close frame");
        };
        assert_eq!(payload.expected_resource_version, ResourceVersion::new(4));
    }

    fn snapshot_assignment() -> SnapshotDeliveryAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-a").unwrap();
        let delivery_id =
            neoengram_domain::protocol::SnapshotDeliveryId::new("delivery-a").unwrap();
        let target_relative_root =
            neoengram_domain::protocol::SnapshotDeliveryOperation::canonical_target_relative_root(
                &project_id,
                &artifact_id,
                &snapshot_id,
                &delivery_id,
            )
            .unwrap();
        let mut assignment = SnapshotDeliveryAssignment {
            job_id: JobId::new("job-snapshot-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-snapshot-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::System,
                id: PrincipalId::new("snapshot-mounter").unwrap(),
                extensions: Extensions::new(),
            },
            action: SnapshotDeliveryAction::Materialize,
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id,
            artifact_id,
            snapshot_id,
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            snapshot_size_bytes: DecimalU64::new(0),
            copy_reserve_bytes: DecimalU64::new(0),
            hardlink_policy: HardlinkPolicy::Disabled,
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            commit_id: ContentDigest::from_bytes([0x11; 32]),
            placement_generation: PlacementGeneration::new(1),
            mode: SnapshotDeliveryMode::Fuse,
            target_relative_root,
            source_index_digest: ContentDigest::from_bytes([0x22; 32]),
            request_digest: ContentDigest::from_bytes([0; 32]),
            delivery_id,
            delivery_generation: DeliveryGeneration::new(1),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.operation().request_digest().unwrap();
        assignment
    }
}
