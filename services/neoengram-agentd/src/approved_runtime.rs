use std::{
    future::Future,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use neoengram_agent::{
    Agent, AgentAssignmentState, AgentError, AgentErrorCode, Clock, DurableReportSink,
    SingleVolumeAgentConfig, SingleVolumeAssignmentValidator, SingleVolumeMountProbe, SqliteLedger,
    SqliteLedgerConfig, SqliteOutboundReportQueue, SqliteOutboundReportQueueConfig,
};
use neoengram_protocol::{
    AgentBootId, AgentHeartbeat, AgentHeartbeatReportPayload, AgentId, AgentInstallationId,
    AgentSessionOpenPayload, Extensions, JobAssignment, JobDecision, JobState, MountAccessMode,
    ResourceVersion, RunningJobObservation, SequenceNumber, SessionGeneration, TenantId,
    UnixMillis,
};
use tokio::time::{self, MissedTickBehavior};
use uuid::Uuid;

use crate::{
    health::HealthReporter, AgentConfig, AgentDaemonError, AgentDaemonResult,
    AgentMessageProcessor, AgentRequestSigner, AgentSessionClient, AgentSessionTransport,
    AgentSigningKey, CoreAgentMessageProcessor, ExecutionBridge, FilesystemExecution, MountProbe,
    RuntimeHealthPhase, SessionExecutionBridge, SharedResourceVersion, SharedSessionFence,
};

const POLL_INTERVAL: Duration = Duration::from_secs(1);

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

/// Runs one already-approved Agent identity until shutdown.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_approved_session<C, P, F>(
    config: AgentConfig,
    client: C,
    probe: P,
    agent_id: AgentId,
    installation_id: AgentInstallationId,
    signing_key: AgentSigningKey,
    initial_resource_version: ResourceVersion,
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
    let client = Arc::new(client);
    let fence = SharedSessionFence::default();
    let resource_version = SharedResourceVersion::new(initial_resource_version);
    let reports = Arc::new(SqliteOutboundReportQueue::open(
        SqliteOutboundReportQueueConfig::new(
            config.storage.state_dir.join("outbound"),
            agent_id.clone(),
            config.tenant_id.clone(),
        ),
    )?);
    reports.integrity_check()?;
    let deferred = Arc::new(DeferredProcessor::default());
    let mut transport = AgentSessionTransport::new_shared(
        config.tenant_id.clone(),
        Arc::clone(&signer),
        Arc::clone(&client),
        reports.clone(),
        deferred.clone(),
        fence.clone(),
        resource_version,
    );
    let binding = transport
        .open(
            UnixMillis::new(now_unix_ms()?),
            AgentSessionOpenPayload {
                mount_identity_digest,
                expected_resource_version: initial_resource_version,
                extensions: Extensions::new(),
            },
        )
        .await?;

    let volume = SingleVolumeAgentConfig {
        agent_id: agent_id.clone(),
        installation_id: installation_id.clone(),
        tenant_id: config.tenant_id.clone(),
        edge_cluster_id: config.edge_cluster_id.clone(),
        storage_volume_id: config.storage_volume_id.clone(),
        agent_mount_id: binding.agent_mount_id,
        mount_generation: binding.mount_generation,
        owner_generation: binding.owner_generation,
        expected_volume_marker: config.storage.expected_volume_marker.clone(),
        desired_access_mode: MountAccessMode::ReadWrite,
        data_root: config.storage.mount_path.clone(),
        state_root: config.storage.state_dir.clone(),
    };
    volume.validate()?;
    let ledger = Arc::new(SqliteLedger::open(SqliteLedgerConfig::new(
        config.storage.state_dir.join("ledger"),
        agent_id,
        config.tenant_id.clone(),
    ))?);
    ledger.integrity_check()?;
    let clock: Arc<dyn Clock> = Arc::new(RuntimeClock);
    let report_sink = Arc::new(DurableReportSink::new(reports.clone(), Arc::clone(&clock)));
    let bridge: Arc<dyn ExecutionBridge> = Arc::new(SessionExecutionBridge::new(
        config.tenant_id.clone(),
        Arc::clone(&client),
        Arc::clone(&signer),
        fence,
        reports.clone(),
        tokio::runtime::Handle::current(),
    ));
    let execution = Arc::new(FilesystemExecution::new(
        &config.storage.mount_path,
        config.storage.state_dir.join("objects"),
        bridge,
    ));
    let validator = Arc::new(SingleVolumeAssignmentValidator::new(
        volume.clone(),
        initial_observation.health,
    ));
    let agent = Arc::new(Agent::new(
        ledger,
        validator,
        execution.clone(),
        execution,
        report_sink,
        Arc::clone(&clock),
    ));
    deferred.install(Arc::new(CoreAgentMessageProcessor::new(Arc::clone(&agent))))?;

    let recovery_agent = Arc::clone(&agent);
    tokio::task::spawn_blocking(move || recovery_agent.recover_active())
        .await
        .map_err(join_error)??;
    flush_all(&mut transport).await?;
    reporter.set_phase(RuntimeHealthPhase::SessionReady)?;

    let mut shutdown = Box::pin(shutdown);
    let mut heartbeat = time::interval(Duration::from_secs(
        config.session.heartbeat_interval_seconds,
    ));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut poll = time::interval(POLL_INTERVAL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sequence = 0_u64;
    loop {
        tokio::select! {
            _ = shutdown.as_mut() => break,
            _ = heartbeat.tick() => {
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    AgentDaemonError::Session("heartbeat sequence overflow".to_owned())
                })?;
                let observation = probe.probe();
                let payload = heartbeat_payload(
                    &transport,
                    &agent,
                    &volume,
                    &boot_id,
                    binding.fence.session_generation,
                    sequence,
                    observation,
                )?;
                transport.heartbeat(UnixMillis::new(now_unix_ms()?), payload).await?;
                flush_all(&mut transport).await?;
                reporter.refresh()?;
            }
            _ = poll.tick() => {
                let poll_result = transport.poll_once(UnixMillis::new(now_unix_ms()?)).await;
                let flush_result = flush_all(&mut transport).await;
                flush_result?;
                poll_result?;
            }
        }
    }

    let flush_result = flush_all(&mut transport).await;
    let close_result = transport.close(UnixMillis::new(now_unix_ms()?)).await;
    flush_result?;
    close_result
}

async fn flush_all<C: AgentSessionClient>(
    transport: &mut AgentSessionTransport<C>,
) -> AgentDaemonResult<()> {
    loop {
        let acknowledged = transport
            .flush_reports(UnixMillis::new(now_unix_ms()?))
            .await?;
        if acknowledged < 32 {
            return Ok(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn heartbeat_payload<C: AgentSessionClient>(
    transport: &AgentSessionTransport<C>,
    agent: &Agent,
    volume: &SingleVolumeAgentConfig,
    boot_id: &AgentBootId,
    session_generation: SessionGeneration,
    sequence: u64,
    observation: neoengram_agent::FilesystemMountObservation,
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
            acknowledged_resource_version: transport.resource_version()?,
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

fn join_error(error: tokio::task::JoinError) -> AgentDaemonError {
    AgentDaemonError::Session(format!("Agent recovery task failed: {error}"))
}
