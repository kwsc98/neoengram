use std::{
    fs,
    future::Future,
    io::Read,
    path::Path,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use neoengram_agent::{
    ApprovedAgentIdentity, FilesystemMountObservation, FilesystemMountProbeConfig,
    MountProbeCondition, SqliteSystemIdentityStore, SystemIdentityRecord,
    TerminalEnrollmentOutcome, TerminalEnrollmentState,
};
use neoengram_protocol::{
    AgentBootstrapAccepted, AgentBootstrapProof, AgentBootstrapRequest,
    AgentBootstrapStatusRequest, AgentBootstrapStatusResponse, AgentBootstrapStatusState,
    AgentEnrollmentState, AgentId, AgentInstallationId, AgentSignatureAlgorithm,
    Ed25519PublicKeySpki, Ed25519Signature, Extensions, MountAccessMode, RequestId, ResourceHealth,
    ResourceVersion, UnixMillis, PROTOCOL_VERSION_V1,
};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::time::{self, MissedTickBehavior};
use zeroize::{Zeroize, Zeroizing};

use crate::status_clock::StatusTimestampClock;
use crate::{
    health::HealthReporter, identity::public_key_spki_der, AgentConfig, AgentDaemonError,
    AgentDaemonResult, AgentSessionClient, AgentSigningKey, EnrollmentBackoff, EnrollmentClient,
    EnrollmentClientError, ReqwestAgentSessionClient, ReqwestEnrollmentClient, RuntimeHealthPhase,
};

const TOKEN_FILE_MAX_BYTES: u64 = 2_050;
const HEALTH_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

pub trait MountProbe: Send + Sync {
    fn probe(&self) -> FilesystemMountObservation;
}

#[derive(Debug, Clone)]
pub struct FilesystemProbe {
    config: FilesystemMountProbeConfig,
}

impl FilesystemProbe {
    #[must_use]
    pub fn new(config: &AgentConfig) -> Self {
        Self {
            config: FilesystemMountProbeConfig {
                mount_root: config.storage.mount_path.clone(),
                expected_volume_marker: config.storage.expected_volume_marker.clone(),
                desired_access_mode: MountAccessMode::ReadWrite,
                hard_minimum_free_bytes: config.storage.hard_minimum_free_bytes,
                ready_minimum_free_bytes: config.storage.ready_minimum_free_bytes,
            },
        }
    }
}

impl MountProbe for FilesystemProbe {
    fn probe(&self) -> FilesystemMountObservation {
        self.config.probe()
    }
}

pub async fn run(config: AgentConfig) -> AgentDaemonResult<()> {
    config.validate()?;
    let client = ReqwestEnrollmentClient::new(config.central_endpoint.clone())?;
    let session_client = ReqwestAgentSessionClient::new(config.central_endpoint.clone())?;
    let probe = FilesystemProbe::new(&config);

    #[cfg(unix)]
    let shutdown = {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate()).map_err(AgentDaemonError::Signal)?;
        async move {
            tokio::select! {
                result = tokio::signal::ctrl_c() => { let _ = result; }
                _ = terminate.recv() => {}
            }
        }
    };
    #[cfg(not(unix))]
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    run_with_transports(config, client, session_client, probe, shutdown).await
}

/// Runs enrollment and the approved Agent session with injectable transports and shutdown.
///
/// This is the production lifecycle with process signals replaced by an injected future, allowing
/// socket-level acceptance tests and embedders to stop the daemon without sending an OS signal.
pub async fn run_with_transports<C, S, P, F>(
    config: AgentConfig,
    enrollment_client: C,
    session_client: S,
    probe: P,
    shutdown: F,
) -> AgentDaemonResult<()>
where
    C: EnrollmentClient + Clone,
    S: AgentSessionClient + 'static,
    P: MountProbe + Clone,
    F: Future<Output = ()> + Send + 'static,
{
    config.validate()?;
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_sender.send(true);
    });
    run_with(
        config.clone(),
        enrollment_client.clone(),
        probe.clone(),
        wait_for_shutdown(shutdown_receiver.clone()),
    )
    .await?;
    if *shutdown_receiver.borrow() {
        return Ok(());
    }

    let store = SqliteSystemIdentityStore::open(&config.storage.state_dir)?;
    let identity = store.load()?.ok_or_else(|| {
        AgentDaemonError::Identity("approved Agent identity disappeared".to_owned())
    })?;
    let Some(approved) = identity.approved.as_ref() else {
        return Ok(());
    };
    let signing_key = crate::signing_key_from_identity(&identity)?;
    let Some(resource_version) = query_approved_resource_version(
        &config,
        &enrollment_client,
        &identity,
        &signing_key,
        shutdown_receiver.clone(),
    )
    .await?
    else {
        return Ok(());
    };
    if *shutdown_receiver.borrow() {
        return Ok(());
    }
    crate::approved_runtime::run_approved_session(
        config,
        session_client,
        probe,
        AgentId::new(approved.agent_id.clone()).map_err(protocol_error)?,
        AgentInstallationId::new(identity.installation_id.clone()).map_err(protocol_error)?,
        signing_key,
        resource_version,
        wait_for_shutdown(shutdown_receiver),
    )
    .await
}

async fn query_approved_resource_version<C: EnrollmentClient>(
    config: &AgentConfig,
    client: &C,
    identity: &SystemIdentityRecord,
    signing_key: &AgentSigningKey,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> AgentDaemonResult<Option<ResourceVersion>> {
    let public_key_spki =
        Ed25519PublicKeySpki::new(public_key_spki_der(signing_key)?).map_err(protocol_error)?;
    let mut status_clock = StatusTimestampClock::open(&config.storage.state_dir)?;
    let mut signer = StatusRequestSigner {
        config,
        signing_key,
        public_key_spki: &public_key_spki,
        status_clock: &mut status_clock,
    };
    let mut backoff =
        EnrollmentBackoff::with_max_delay_seconds(config.session.reconnect_max_delay_seconds);
    loop {
        let request = signer.build(identity)?;
        match client.status(&request).await {
            Ok(Some(status)) => {
                status.validate().map_err(protocol_error)?;
                if status.state != AgentBootstrapStatusState::Approved
                    || status.agent_id.as_ref().map(|value| value.to_string())
                        != identity
                            .approved
                            .as_ref()
                            .map(|value| value.agent_id.clone())
                {
                    return Err(AgentDaemonError::EnrollmentProtocol(
                        "approved status query returned another identity or state".to_owned(),
                    ));
                }
                return Ok(Some(status.resource_version));
            }
            Ok(None) => {
                return Err(AgentDaemonError::EnrollmentProtocol(
                    "approved enrollment disappeared before session open".to_owned(),
                ));
            }
            Err(error) if error.retryable() => {
                let delay = next_delay(&mut backoff);
                tokio::select! {
                    _ = wait_for_shutdown(shutdown.clone()) => {
                        return Ok(None);
                    }
                    _ = time::sleep(delay) => {}
                }
            }
            Err(error) => return Err(enrollment_error(error)),
        }
        if *shutdown.borrow_and_update() {
            return Ok(None);
        }
    }
}

async fn wait_for_shutdown(mut receiver: tokio::sync::watch::Receiver<bool>) {
    if *receiver.borrow() {
        return;
    }
    while receiver.changed().await.is_ok() {
        if *receiver.borrow() {
            return;
        }
    }
}

/// Runs enrollment with injectable transport, probe, and shutdown for socket-free focused tests.
pub async fn run_with<C, P, F>(
    config: AgentConfig,
    client: C,
    probe: P,
    shutdown: F,
) -> AgentDaemonResult<()>
where
    C: EnrollmentClient,
    P: MountProbe,
    F: Future<Output = ()> + Send,
{
    config.validate()?;
    let mut reporter = HealthReporter::acquire(config.storage.state_dir.clone())?;
    let mut shutdown = Box::pin(shutdown);
    let store = SqliteSystemIdentityStore::open(&config.storage.state_dir)?;
    store.integrity_check()?;
    let mut identity = match store.load()? {
        Some(identity) => identity,
        None => crate::load_or_create_identity(&store)?,
    };
    if let Some(terminal) = &identity.terminal_enrollment {
        reporter.set_phase(match terminal.state {
            TerminalEnrollmentState::Rejected => RuntimeHealthPhase::Rejected,
            TerminalEnrollmentState::Expired => RuntimeHealthPhase::Expired,
        })?;
        return hold_terminal_phase(&mut reporter, &mut shutdown).await;
    }
    let signing_key = crate::signing_key_from_identity(&identity)?;
    let public_key_spki =
        Ed25519PublicKeySpki::new(public_key_spki_der(&signing_key)?).map_err(protocol_error)?;

    let observation = probe.probe();
    validate_bootstrap_probe(&observation)?;
    let bootstrap_probe = observation
        .to_bootstrap_probe(UnixMillis::new(now_unix_ms()?))
        .map_err(|error| AgentDaemonError::MountProbe(error.to_string()))?;

    let initial_phase = if identity.approved.is_some() {
        RuntimeHealthPhase::ApprovedWaitingCertificate
    } else {
        RuntimeHealthPhase::Bootstrapping
    };
    reporter.set_phase(initial_phase)?;

    if identity.approved.is_some() {
        return Ok(());
    }

    let mut status_clock = StatusTimestampClock::open(&config.storage.state_dir)?;
    let mut status_signer = StatusRequestSigner {
        config: &config,
        signing_key: &signing_key,
        public_key_spki: &public_key_spki,
        status_clock: &mut status_clock,
    };
    let preflight = lookup_status_until_known(
        &client,
        &identity,
        &mut status_signer,
        &mut reporter,
        &mut shutdown,
    )
    .await?;
    match preflight {
        PreflightDisposition::Shutdown => return Ok(()),
        PreflightDisposition::Candidate(status) => {
            match apply_status(&store, &mut identity, status, &mut reporter)? {
                StatusDisposition::Pending => {}
                StatusDisposition::Approved => return Ok(()),
                StatusDisposition::Terminal => {
                    return hold_terminal_phase(&mut reporter, &mut shutdown).await;
                }
            }
        }
        PreflightDisposition::Missing => {
            let token = read_bootstrap_token(&config.registration.bootstrap_token_file)?;
            let mut guarded_request = SensitiveBootstrapRequest::new(build_bootstrap_request(
                &config,
                &identity,
                &signing_key,
                &public_key_spki,
                token,
                bootstrap_probe,
            )?);
            let accepted = match bootstrap_until_accepted(
                &client,
                &mut guarded_request,
                config.session.reconnect_max_delay_seconds,
                &mut reporter,
                &mut shutdown,
            )
            .await?
            {
                BootstrapDisposition::Accepted(accepted) => accepted,
                BootstrapDisposition::Shutdown => return Ok(()),
            };
            drop(guarded_request);
            match apply_bootstrap_accepted(&store, &mut identity, accepted, &mut reporter)? {
                StatusDisposition::Pending => {}
                StatusDisposition::Approved => return Ok(()),
                StatusDisposition::Terminal => {
                    return hold_terminal_phase(&mut reporter, &mut shutdown).await;
                }
            }
        }
    }

    poll_status(
        &client,
        &store,
        &mut identity,
        &mut status_signer,
        &mut reporter,
        &mut shutdown,
    )
    .await
}

async fn lookup_status_until_known<C, F>(
    client: &C,
    identity: &SystemIdentityRecord,
    status_signer: &mut StatusRequestSigner<'_>,
    reporter: &mut HealthReporter,
    shutdown: &mut Pin<Box<F>>,
) -> AgentDaemonResult<PreflightDisposition>
where
    C: EnrollmentClient,
    F: Future<Output = ()> + Send,
{
    let mut backoff = EnrollmentBackoff::with_max_delay_seconds(status_signer.max_delay_seconds());
    loop {
        reporter.refresh()?;
        let request = status_signer.build(identity)?;
        match client.status(&request).await {
            Ok(Some(status)) => return Ok(PreflightDisposition::Candidate(status)),
            Ok(None) => return Ok(PreflightDisposition::Missing),
            Err(error) if error.retryable() => {
                let delay = next_delay(&mut backoff);
                if wait_with_health(delay, reporter, shutdown).await? {
                    return Ok(PreflightDisposition::Shutdown);
                }
            }
            Err(error) => return Err(enrollment_error(error)),
        }
    }
}

async fn bootstrap_until_accepted<C, F>(
    client: &C,
    request: &mut SensitiveBootstrapRequest,
    max_delay_seconds: u64,
    reporter: &mut HealthReporter,
    shutdown: &mut Pin<Box<F>>,
) -> AgentDaemonResult<BootstrapDisposition>
where
    C: EnrollmentClient,
    F: Future<Output = ()> + Send,
{
    let mut backoff = EnrollmentBackoff::with_max_delay_seconds(max_delay_seconds);
    loop {
        reporter.refresh()?;
        match client.bootstrap(request.as_request()).await {
            Ok(accepted) => return Ok(BootstrapDisposition::Accepted(accepted)),
            Err(error) if error.retryable() => {
                let delay = next_delay(&mut backoff);
                if wait_with_health(delay, reporter, shutdown).await? {
                    return Ok(BootstrapDisposition::Shutdown);
                }
            }
            Err(error) => return Err(enrollment_error(error)),
        }
    }
}

async fn poll_status<C, F>(
    client: &C,
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
    status_signer: &mut StatusRequestSigner<'_>,
    reporter: &mut HealthReporter,
    shutdown: &mut Pin<Box<F>>,
) -> AgentDaemonResult<()>
where
    C: EnrollmentClient,
    F: Future<Output = ()> + Send,
{
    reporter.set_phase(RuntimeHealthPhase::PendingApproval)?;
    let mut backoff = EnrollmentBackoff::with_max_delay_seconds(status_signer.max_delay_seconds());
    loop {
        let delay = next_delay(&mut backoff);
        if wait_with_health(delay, reporter, shutdown).await? {
            return Ok(());
        }
        let request = status_signer.build(identity)?;
        match client.status(&request).await {
            Ok(Some(status)) => match apply_status(store, identity, status, reporter)? {
                StatusDisposition::Pending => continue,
                StatusDisposition::Approved => return Ok(()),
                StatusDisposition::Terminal => {
                    return hold_terminal_phase(reporter, shutdown).await;
                }
            },
            Ok(None) => {
                return Err(AgentDaemonError::EnrollmentProtocol(
                    "accepted bootstrap candidate disappeared".into(),
                ));
            }
            Err(error) if error.retryable() => continue,
            Err(error) => return Err(enrollment_error(error)),
        }
    }
}

fn apply_bootstrap_accepted(
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
    accepted: AgentBootstrapAccepted,
    reporter: &mut HealthReporter,
) -> AgentDaemonResult<StatusDisposition> {
    accepted.validate().map_err(protocol_error)?;
    let expected_request_id =
        RequestId::new(identity.bootstrap_request_id.clone()).map_err(protocol_error)?;
    if accepted.bootstrap_request_id != expected_request_id {
        return Err(AgentDaemonError::EnrollmentProtocol(
            "bootstrap response identity does not match persisted request".into(),
        ));
    }
    match accepted.state {
        AgentEnrollmentState::PendingApproval => {
            reporter.set_phase(RuntimeHealthPhase::PendingApproval)?;
            Ok(StatusDisposition::Pending)
        }
        AgentEnrollmentState::Approved => {
            bind_approved(
                store,
                identity,
                accepted.agent_id.to_string(),
                accepted.enrollment_id.to_string(),
            )?;
            reporter.set_phase(RuntimeHealthPhase::ApprovedWaitingCertificate)?;
            Ok(StatusDisposition::Approved)
        }
        AgentEnrollmentState::Rejected | AgentEnrollmentState::Revoked => {
            bind_terminal(
                store,
                identity,
                TerminalEnrollmentState::Rejected,
                accepted.enrollment_id.to_string(),
            )?;
            reporter.set_phase(RuntimeHealthPhase::Rejected)?;
            Ok(StatusDisposition::Terminal)
        }
        AgentEnrollmentState::Expired => {
            bind_terminal(
                store,
                identity,
                TerminalEnrollmentState::Expired,
                accepted.enrollment_id.to_string(),
            )?;
            reporter.set_phase(RuntimeHealthPhase::Expired)?;
            Ok(StatusDisposition::Terminal)
        }
        AgentEnrollmentState::TokenIssued => Err(AgentDaemonError::EnrollmentProtocol(
            "bootstrap response cannot remain token_issued".into(),
        )),
    }
}

fn apply_status(
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
    status: AgentBootstrapStatusResponse,
    reporter: &mut HealthReporter,
) -> AgentDaemonResult<StatusDisposition> {
    status.validate().map_err(protocol_error)?;
    let expected_request_id =
        RequestId::new(identity.bootstrap_request_id.clone()).map_err(protocol_error)?;
    let expected_installation_id =
        AgentInstallationId::new(identity.installation_id.clone()).map_err(protocol_error)?;
    if status.bootstrap_request_id != expected_request_id
        || status.installation_id != expected_installation_id
    {
        return Err(AgentDaemonError::EnrollmentProtocol(
            "bootstrap status identity does not match persisted identity".into(),
        ));
    }
    match status.state {
        AgentBootstrapStatusState::Pending => {
            reporter.set_phase(RuntimeHealthPhase::PendingApproval)?;
            Ok(StatusDisposition::Pending)
        }
        AgentBootstrapStatusState::Approved => {
            let agent_id = status.agent_id.ok_or_else(|| {
                AgentDaemonError::EnrollmentProtocol(
                    "approved bootstrap status omitted Agent identity".into(),
                )
            })?;
            bind_approved(
                store,
                identity,
                agent_id.to_string(),
                status.enrollment_id.to_string(),
            )?;
            reporter.set_phase(RuntimeHealthPhase::ApprovedWaitingCertificate)?;
            Ok(StatusDisposition::Approved)
        }
        AgentBootstrapStatusState::Rejected => {
            bind_terminal(
                store,
                identity,
                TerminalEnrollmentState::Rejected,
                status.enrollment_id.to_string(),
            )?;
            reporter.set_phase(RuntimeHealthPhase::Rejected)?;
            Ok(StatusDisposition::Terminal)
        }
        AgentBootstrapStatusState::Expired => {
            bind_terminal(
                store,
                identity,
                TerminalEnrollmentState::Expired,
                status.enrollment_id.to_string(),
            )?;
            reporter.set_phase(RuntimeHealthPhase::Expired)?;
            Ok(StatusDisposition::Terminal)
        }
    }
}

fn bind_approved(
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
    agent_id: String,
    enrollment_id: String,
) -> AgentDaemonResult<()> {
    let approved = ApprovedAgentIdentity::new(agent_id, enrollment_id)?;
    *identity = store.bind_approved(identity.revision, approved)?;
    Ok(())
}

fn bind_terminal(
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
    state: TerminalEnrollmentState,
    enrollment_id: String,
) -> AgentDaemonResult<()> {
    let terminal = TerminalEnrollmentOutcome::new(state, enrollment_id)?;
    *identity = store.bind_terminal_enrollment(identity.revision, terminal)?;
    Ok(())
}

fn build_bootstrap_request(
    config: &AgentConfig,
    identity: &SystemIdentityRecord,
    signing_key: &AgentSigningKey,
    public_key_spki: &Ed25519PublicKeySpki,
    token: Zeroizing<String>,
    probe: neoengram_protocol::AgentBootstrapProbe,
) -> AgentDaemonResult<AgentBootstrapRequest> {
    let mut request = AgentBootstrapRequest {
        bootstrap_request_id: RequestId::new(identity.bootstrap_request_id.clone())
            .map_err(protocol_error)?,
        bootstrap_token: token.to_string(),
        installation_id: AgentInstallationId::new(identity.installation_id.clone())
            .map_err(protocol_error)?,
        tenant_id: config.tenant_id.clone(),
        edge_cluster_id: config.edge_cluster_id.clone(),
        storage_volume_id: config.storage_volume_id.clone(),
        volume_descriptor_digest: config.volume_descriptor_digest,
        agent_version: env!("CARGO_PKG_VERSION").to_owned(),
        supported_protocol_versions: vec![PROTOCOL_VERSION_V1],
        capabilities: Vec::new(),
        public_key_fingerprint: public_key_spki.fingerprint(),
        proof: placeholder_proof(public_key_spki.clone()),
        probe,
        extensions: Extensions::new(),
    };
    sign_bootstrap_request(&mut request, signing_key)?;
    Ok(request)
}

struct StatusRequestSigner<'a> {
    config: &'a AgentConfig,
    signing_key: &'a AgentSigningKey,
    public_key_spki: &'a Ed25519PublicKeySpki,
    status_clock: &'a mut StatusTimestampClock,
}

impl StatusRequestSigner<'_> {
    fn max_delay_seconds(&self) -> u64 {
        self.config.session.reconnect_max_delay_seconds
    }

    fn build(
        &mut self,
        identity: &SystemIdentityRecord,
    ) -> AgentDaemonResult<AgentBootstrapStatusRequest> {
        let mut request = AgentBootstrapStatusRequest {
            protocol_version: PROTOCOL_VERSION_V1,
            tenant_id: self.config.tenant_id.clone(),
            bootstrap_request_id: RequestId::new(identity.bootstrap_request_id.clone())
                .map_err(protocol_error)?,
            installation_id: AgentInstallationId::new(identity.installation_id.clone())
                .map_err(protocol_error)?,
            signed_at_unix_ms: UnixMillis::new(self.status_clock.next(now_unix_ms()?)?),
            proof: placeholder_proof(self.public_key_spki.clone()),
            extensions: Extensions::new(),
        };
        let signing_bytes = Zeroizing::new(request.signing_bytes().map_err(protocol_error)?);
        request.proof.signature =
            Ed25519Signature::new(self.signing_key.sign(&signing_bytes).as_ref().to_vec())
                .map_err(protocol_error)?;
        request
            .proof
            .verify(&signing_bytes)
            .map_err(protocol_error)?;
        Ok(request)
    }
}

fn sign_bootstrap_request(
    request: &mut AgentBootstrapRequest,
    signing_key: &AgentSigningKey,
) -> AgentDaemonResult<()> {
    let signing_bytes = Zeroizing::new(request.signing_bytes().map_err(protocol_error)?);
    request.proof.signature =
        Ed25519Signature::new(signing_key.sign(&signing_bytes).as_ref().to_vec())
            .map_err(protocol_error)?;
    request
        .proof
        .verify(&signing_bytes)
        .map_err(protocol_error)?;
    Ok(())
}

fn placeholder_proof(public_key_spki: Ed25519PublicKeySpki) -> AgentBootstrapProof {
    AgentBootstrapProof {
        algorithm: AgentSignatureAlgorithm::Ed25519,
        public_key_spki,
        signature: Ed25519Signature::from_bytes([0; 64]),
        extensions: Extensions::new(),
    }
}

pub(crate) fn validate_bootstrap_probe(
    observation: &FilesystemMountObservation,
) -> AgentDaemonResult<()> {
    let usable_condition = matches!(
        observation.condition,
        MountProbeCondition::Ready | MountProbeCondition::LowFreeSpace
    );
    let usable_health = matches!(
        observation.health,
        ResourceHealth::Ready | ResourceHealth::Degraded
    );
    if !usable_condition
        || !usable_health
        || !observation.marker_matches
        || !observation.mount_boundary_detected
        || observation.access_mode != Some(MountAccessMode::ReadWrite)
        || !observation.rename_supported
        || !observation.fsync_supported
        || observation.mount_identity_digest.is_none()
    {
        return Err(AgentDaemonError::MountProbe(format!(
            "mount readiness gate failed with condition {:?}",
            observation.condition
        )));
    }
    Ok(())
}

fn read_bootstrap_token(path: &Path) -> AgentDaemonResult<Zeroizing<String>> {
    // Projected Kubernetes Secret entries are symlinks; validate the resolved target.
    let metadata = fs::metadata(path).map_err(|error| {
        AgentDaemonError::Configuration(format!("bootstrap token file is unavailable: {error}"))
    })?;
    if !metadata.is_file() || metadata.len() > TOKEN_FILE_MAX_BYTES {
        return Err(AgentDaemonError::Configuration(
            "bootstrap token file must be a bounded regular file".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    fs::File::open(path)?
        .take(TOKEN_FILE_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > TOKEN_FILE_MAX_BYTES {
        return Err(AgentDaemonError::Configuration(
            "bootstrap token file exceeds the size limit".into(),
        ));
    }
    let mut token = String::from_utf8(bytes).map_err(|_| {
        AgentDaemonError::Configuration("bootstrap token file is not valid UTF-8".into())
    })?;
    if token.ends_with("\r\n") {
        token.truncate(token.len() - 2);
    } else if token.ends_with('\n') {
        token.pop();
    }
    if token.trim() != token {
        token.zeroize();
        return Err(AgentDaemonError::Configuration(
            "bootstrap token contains surrounding whitespace".into(),
        ));
    }
    Ok(Zeroizing::new(token))
}

fn next_delay(backoff: &mut EnrollmentBackoff) -> Duration {
    let mut random = [0_u8; 1];
    let jitter = if SystemRandom::new().fill(&mut random).is_ok() {
        i32::from(random[0] % 41) - 20
    } else {
        0
    };
    backoff.next_delay_with_jitter(jitter)
}

async fn hold_terminal_phase<F>(
    reporter: &mut HealthReporter,
    shutdown: &mut Pin<Box<F>>,
) -> AgentDaemonResult<()>
where
    F: Future<Output = ()> + Send,
{
    loop {
        if wait_with_health(HEALTH_REFRESH_INTERVAL, reporter, shutdown).await? {
            return Ok(());
        }
    }
}

async fn wait_with_health<F>(
    duration: Duration,
    reporter: &mut HealthReporter,
    shutdown: &mut Pin<Box<F>>,
) -> AgentDaemonResult<bool>
where
    F: Future<Output = ()> + Send,
{
    let deadline = time::sleep(duration);
    tokio::pin!(deadline);
    let mut refresh = time::interval(HEALTH_REFRESH_INTERVAL);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
    refresh.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.as_mut() => return Ok(true),
            _ = &mut deadline => return Ok(false),
            _ = refresh.tick() => reporter.refresh()?,
        }
    }
}

fn now_unix_ms() -> AgentDaemonResult<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        AgentDaemonError::EnrollmentProtocol("system clock precedes Unix epoch".into())
    })?;
    u64::try_from(duration.as_millis())
        .map_err(|_| AgentDaemonError::EnrollmentProtocol("system clock is out of range".into()))
}

fn protocol_error(error: neoengram_protocol::ProtocolError) -> AgentDaemonError {
    AgentDaemonError::EnrollmentProtocol(error.to_string())
}

fn enrollment_error(error: EnrollmentClientError) -> AgentDaemonError {
    AgentDaemonError::Enrollment(error.to_string())
}

enum StatusDisposition {
    Pending,
    Approved,
    Terminal,
}

enum PreflightDisposition {
    Missing,
    Candidate(AgentBootstrapStatusResponse),
    Shutdown,
}

enum BootstrapDisposition {
    Accepted(AgentBootstrapAccepted),
    Shutdown,
}

struct SensitiveBootstrapRequest {
    request: AgentBootstrapRequest,
}

impl SensitiveBootstrapRequest {
    fn new(request: AgentBootstrapRequest) -> Self {
        Self { request }
    }

    fn as_request(&self) -> &AgentBootstrapRequest {
        &self.request
    }
}

impl Drop for SensitiveBootstrapRequest {
    fn drop(&mut self) {
        self.request.bootstrap_token.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use async_trait::async_trait;
    use neoengram_protocol::{
        AgentEnrollmentId, AgentId, AgentMountId, AgentMountIdentityDigest, ContentDigest,
        MountGeneration, ResourceVersion, StorageVolumeId, VolumeMarkerId,
    };

    use super::*;
    use crate::{
        LoggingConfig, LoggingFormat, PvcReference, RegistrationConfig, SessionConfig,
        StorageAccessMode, StorageBackendType, StorageConfig,
    };

    #[derive(Clone)]
    struct FixedProbe(FilesystemMountObservation);

    impl MountProbe for FixedProbe {
        fn probe(&self) -> FilesystemMountObservation {
            self.0.clone()
        }
    }

    #[derive(Clone)]
    struct StartupHealthProbe {
        state_dir: std::path::PathBuf,
        observation: FilesystemMountObservation,
        observed_starting: Arc<AtomicBool>,
    }

    impl MountProbe for StartupHealthProbe {
        fn probe(&self) -> FilesystemMountObservation {
            assert!(crate::check_health(&self.state_dir, crate::HealthMode::Startup).is_err());
            assert!(crate::check_health(&self.state_dir, crate::HealthMode::Live).is_err());
            self.observed_starting.store(true, Ordering::SeqCst);
            self.observation.clone()
        }
    }

    #[derive(Clone)]
    struct ApprovingClient {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl EnrollmentClient for ApprovingClient {
        async fn bootstrap(
            &self,
            request: &AgentBootstrapRequest,
        ) -> Result<AgentBootstrapAccepted, EnrollmentClientError> {
            request.verify().unwrap();
            self.calls.lock().unwrap().push("bootstrap");
            Ok(AgentBootstrapAccepted {
                bootstrap_request_id: request.bootstrap_request_id.clone(),
                enrollment_id: AgentEnrollmentId::new("enrollment-approved").unwrap(),
                agent_id: AgentId::new("agent-approved").unwrap(),
                agent_mount_id: AgentMountId::new("mount-approved").unwrap(),
                state: AgentEnrollmentState::Approved,
                mount_generation: MountGeneration::new(1),
                resource_version: ResourceVersion::new(2),
                review_expires_at_unix_ms: UnixMillis::new(now_unix_ms().unwrap() + 60_000),
                replayed: false,
                extensions: Extensions::new(),
            })
        }

        async fn status(
            &self,
            request: &AgentBootstrapStatusRequest,
        ) -> Result<Option<AgentBootstrapStatusResponse>, EnrollmentClientError> {
            request.verify().unwrap();
            self.calls.lock().unwrap().push("status");
            Ok(None)
        }
    }

    #[derive(Clone, Copy)]
    enum TerminalResponseSource {
        BootstrapRejected,
        StatusExpired,
    }

    #[derive(Clone)]
    struct TerminalClient {
        source: TerminalResponseSource,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl EnrollmentClient for TerminalClient {
        async fn bootstrap(
            &self,
            request: &AgentBootstrapRequest,
        ) -> Result<AgentBootstrapAccepted, EnrollmentClientError> {
            request.verify().unwrap();
            self.calls.lock().unwrap().push("bootstrap");
            assert!(matches!(
                self.source,
                TerminalResponseSource::BootstrapRejected
            ));
            Ok(AgentBootstrapAccepted {
                bootstrap_request_id: request.bootstrap_request_id.clone(),
                enrollment_id: AgentEnrollmentId::new("enrollment-rejected").unwrap(),
                agent_id: AgentId::new("agent-rejected").unwrap(),
                agent_mount_id: AgentMountId::new("mount-rejected").unwrap(),
                state: AgentEnrollmentState::Rejected,
                mount_generation: MountGeneration::new(1),
                resource_version: ResourceVersion::new(2),
                review_expires_at_unix_ms: UnixMillis::new(now_unix_ms().unwrap() + 60_000),
                replayed: false,
                extensions: Extensions::new(),
            })
        }

        async fn status(
            &self,
            request: &AgentBootstrapStatusRequest,
        ) -> Result<Option<AgentBootstrapStatusResponse>, EnrollmentClientError> {
            request.verify().unwrap();
            self.calls.lock().unwrap().push("status");
            match self.source {
                TerminalResponseSource::BootstrapRejected => Ok(None),
                TerminalResponseSource::StatusExpired => Ok(Some(AgentBootstrapStatusResponse {
                    protocol_version: PROTOCOL_VERSION_V1,
                    bootstrap_request_id: request.bootstrap_request_id.clone(),
                    installation_id: request.installation_id.clone(),
                    state: AgentBootstrapStatusState::Expired,
                    enrollment_id: AgentEnrollmentId::new("enrollment-expired").unwrap(),
                    agent_id: None,
                    resource_version: ResourceVersion::new(2),
                    updated_at_unix_ms: UnixMillis::new(now_unix_ms().unwrap()),
                    extensions: Extensions::new(),
                })),
            }
        }
    }

    #[derive(Clone)]
    struct ForbiddenNetworkClient(Arc<AtomicUsize>);

    #[async_trait]
    impl EnrollmentClient for ForbiddenNetworkClient {
        async fn bootstrap(
            &self,
            _request: &AgentBootstrapRequest,
        ) -> Result<AgentBootstrapAccepted, EnrollmentClientError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("terminal restart must not bootstrap")
        }

        async fn status(
            &self,
            _request: &AgentBootstrapStatusRequest,
        ) -> Result<Option<AgentBootstrapStatusResponse>, EnrollmentClientError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("terminal restart must not poll status")
        }
    }

    #[derive(Clone)]
    struct ForbiddenProbe(Arc<AtomicUsize>);

    impl MountProbe for ForbiddenProbe {
        fn probe(&self) -> FilesystemMountObservation {
            self.0.fetch_add(1, Ordering::SeqCst);
            ready_probe()
        }
    }

    #[tokio::test]
    async fn bootstrap_approval_is_bound_by_cas_and_remains_not_ready() {
        let directory = tempfile::tempdir().unwrap();
        let token_path = directory.path().join("bootstrap-token");
        fs::write(&token_path, "x".repeat(40)).unwrap();
        let config = test_config(directory.path(), token_path);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = ApprovingClient {
            calls: Arc::clone(&calls),
        };
        run_with(config.clone(), client, FixedProbe(ready_probe()), async {})
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), ["status", "bootstrap"]);

        let store = SqliteSystemIdentityStore::open(&config.storage.state_dir).unwrap();
        let identity = store.load().unwrap().unwrap();
        assert_eq!(identity.approved.unwrap().agent_id, "agent-approved");
        drop(store);
        assert!(crate::check_health(&config.storage.state_dir, crate::HealthMode::Ready).is_err());
    }

    #[tokio::test]
    async fn mount_failure_prevents_all_network_requests() {
        let directory = tempfile::tempdir().unwrap();
        let token_path = directory.path().join("bootstrap-token");
        fs::write(&token_path, "x".repeat(40)).unwrap();
        let config = test_config(directory.path(), token_path);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = ApprovingClient {
            calls: Arc::clone(&calls),
        };
        let mut unavailable = ready_probe();
        unavailable.rename_supported = false;
        unavailable.condition = MountProbeCondition::ReadWriteProbeFailed;
        assert!(
            run_with(config.clone(), client, FixedProbe(unavailable), async {})
                .await
                .is_err()
        );
        assert!(calls.lock().unwrap().is_empty());
        assert!(
            crate::check_health(&config.storage.state_dir, crate::HealthMode::Startup).is_err()
        );
        assert!(crate::check_health(&config.storage.state_dir, crate::HealthMode::Live).is_err());
    }

    #[tokio::test]
    async fn health_stays_false_until_mount_initialization_completes() {
        let directory = tempfile::tempdir().unwrap();
        let token_path = directory.path().join("bootstrap-token");
        fs::write(&token_path, "x".repeat(40)).unwrap();
        let config = test_config(directory.path(), token_path);
        let observed_starting = Arc::new(AtomicBool::new(false));
        let probe = StartupHealthProbe {
            state_dir: config.storage.state_dir.clone(),
            observation: ready_probe(),
            observed_starting: Arc::clone(&observed_starting),
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = ApprovingClient {
            calls: Arc::clone(&calls),
        };

        run_with(config, client, probe, async {}).await.unwrap();
        assert!(observed_starting.load(Ordering::SeqCst));
        assert_eq!(*calls.lock().unwrap(), ["status", "bootstrap"]);
    }

    #[tokio::test]
    async fn terminal_outcomes_survive_restart_without_probe_or_network() {
        assert_terminal_restart(
            TerminalResponseSource::BootstrapRejected,
            TerminalEnrollmentState::Rejected,
            "enrollment-rejected",
            &["status", "bootstrap"],
        )
        .await;
        assert_terminal_restart(
            TerminalResponseSource::StatusExpired,
            TerminalEnrollmentState::Expired,
            "enrollment-expired",
            &["status"],
        )
        .await;
    }

    #[test]
    fn status_signer_uses_the_durable_monotonic_watermark() {
        let directory = tempfile::tempdir().unwrap();
        let config = test_config(
            directory.path(),
            directory.path().join("unused-bootstrap-token"),
        );
        let store = SqliteSystemIdentityStore::open(&config.storage.state_dir).unwrap();
        let identity = crate::load_or_create_identity(&store).unwrap();
        let signing_key = crate::signing_key_from_identity(&identity).unwrap();
        let public_key_spki =
            Ed25519PublicKeySpki::new(public_key_spki_der(&signing_key).unwrap()).unwrap();
        let future_watermark = u64::MAX - 10;
        {
            let mut clock = StatusTimestampClock::open(&config.storage.state_dir).unwrap();
            assert_eq!(clock.next(future_watermark).unwrap(), future_watermark);
            let mut signer = StatusRequestSigner {
                config: &config,
                signing_key: &signing_key,
                public_key_spki: &public_key_spki,
                status_clock: &mut clock,
            };
            let first = signer.build(&identity).unwrap();
            let second = signer.build(&identity).unwrap();
            assert_eq!(first.signed_at_unix_ms.get(), future_watermark + 1);
            assert_eq!(second.signed_at_unix_ms.get(), future_watermark + 2);
            first.verify().unwrap();
            second.verify().unwrap();
        }

        let mut restarted_clock = StatusTimestampClock::open(&config.storage.state_dir).unwrap();
        let mut restarted_signer = StatusRequestSigner {
            config: &config,
            signing_key: &signing_key,
            public_key_spki: &public_key_spki,
            status_clock: &mut restarted_clock,
        };
        let after_restart = restarted_signer.build(&identity).unwrap();
        assert_eq!(after_restart.signed_at_unix_ms.get(), future_watermark + 3);
        after_restart.verify().unwrap();
    }

    async fn assert_terminal_restart(
        source: TerminalResponseSource,
        expected_state: TerminalEnrollmentState,
        expected_enrollment_id: &str,
        expected_first_calls: &[&str],
    ) {
        let directory = tempfile::tempdir().unwrap();
        let token_path = directory.path().join("bootstrap-token");
        fs::write(&token_path, "x".repeat(40)).unwrap();
        let config = test_config(directory.path(), token_path.clone());
        let first_calls = Arc::new(Mutex::new(Vec::new()));
        run_with(
            config.clone(),
            TerminalClient {
                source,
                calls: Arc::clone(&first_calls),
            },
            FixedProbe(ready_probe()),
            async {},
        )
        .await
        .unwrap();
        assert_eq!(first_calls.lock().unwrap().as_slice(), expected_first_calls);

        let store = SqliteSystemIdentityStore::open(&config.storage.state_dir).unwrap();
        let persisted = store.load().unwrap().unwrap();
        assert_eq!(persisted.revision, 2);
        assert_eq!(
            persisted.terminal_enrollment,
            Some(TerminalEnrollmentOutcome::new(expected_state, expected_enrollment_id).unwrap())
        );
        drop(store);

        fs::remove_file(token_path).unwrap();
        let network_calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = Arc::new(AtomicUsize::new(0));
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let state_dir = config.storage.state_dir.clone();
        let restart = tokio::spawn(run_with(
            config,
            ForbiddenNetworkClient(Arc::clone(&network_calls)),
            ForbiddenProbe(Arc::clone(&probe_calls)),
            async move {
                let _ = shutdown_receiver.await;
            },
        ));

        wait_for_terminal_phase(&state_dir, expected_state.as_str()).await;
        crate::check_health(&state_dir, crate::HealthMode::Startup).unwrap();
        crate::check_health(&state_dir, crate::HealthMode::Live).unwrap();
        assert!(crate::check_health(&state_dir, crate::HealthMode::Ready).is_err());
        assert_eq!(network_calls.load(Ordering::SeqCst), 0);
        assert_eq!(probe_calls.load(Ordering::SeqCst), 0);
        shutdown_sender.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), restart)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let reopened = SqliteSystemIdentityStore::open(&state_dir).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(), persisted);
    }

    async fn wait_for_terminal_phase(state_dir: &Path, expected: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let matches = fs::read(state_dir.join("runtime-health.json"))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                    .and_then(|document| document["phase"].as_str().map(str::to_owned))
                    .is_some_and(|phase| phase == expected);
                if matches && crate::check_health(state_dir, crate::HealthMode::Live).is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("terminal phase {expected} was not restored"));
    }

    fn test_config(root: &Path, token_path: std::path::PathBuf) -> AgentConfig {
        let state_dir = root.join("state");
        AgentConfig {
            schema_version: 1,
            protocol_version: 1,
            central_endpoint: url::Url::parse("http://127.0.0.1:8080/").unwrap(),
            tenant_id: neoengram_protocol::TenantId::new("tenant-a").unwrap(),
            edge_cluster_id: neoengram_protocol::EdgeClusterId::new("edge-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"descriptor-a"),
            region: "region-a".into(),
            storage: StorageConfig {
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteOnce,
                mount_path: root.join("volume"),
                state_dir,
                marker_file: root
                    .join("volume")
                    .join(neoengram_agent::VOLUME_MARKER_FILE_NAME),
                expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
                pvc_reference: PvcReference {
                    namespace: "namespace-a".into(),
                    claim_name: "claim-a".into(),
                },
                hard_minimum_free_bytes: 0,
                ready_minimum_free_bytes: 0,
            },
            registration: RegistrationConfig {
                approval_required: true,
                token_id: neoengram_protocol::AgentEnrollmentTokenId::new("token-a").unwrap(),
                bootstrap_token_file: token_path,
            },
            session: SessionConfig {
                heartbeat_interval_seconds: 10,
                reconnect_max_delay_seconds: 30,
            },
            logging: LoggingConfig {
                format: LoggingFormat::Json,
                level: "info".into(),
            },
        }
    }

    fn ready_probe() -> FilesystemMountObservation {
        FilesystemMountObservation {
            observed_volume_marker: Some(VolumeMarkerId::new("volume-a").unwrap()),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: Some(MountAccessMode::ReadWrite),
            rename_supported: true,
            fsync_supported: true,
            health: ResourceHealth::Ready,
            available_bytes: Some(1024),
            mount_identity_digest: Some(AgentMountIdentityDigest::new(ContentDigest::hash(
                b"mount-a",
            ))),
            condition: MountProbeCondition::Ready,
        }
    }
}
