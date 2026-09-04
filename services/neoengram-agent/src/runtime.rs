use std::{
    fs,
    future::Future,
    io::Read,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    ApprovedAgentIdentity, DevelopmentDirectoryMountProbeConfig, FilesystemMountObservation,
    FilesystemMountProbeConfig, MountProbeCondition, SqliteSystemIdentityStore,
    SystemIdentityRecord, TerminalEnrollmentOutcome, TerminalEnrollmentState,
    VOLUME_MARKER_FILE_NAME,
};
use neoengram_domain::protocol::{
    AgentBootstrapAccepted, AgentBootstrapProof, AgentBootstrapRequest,
    AgentBootstrapStatusRequest, AgentBootstrapStatusResponse, AgentBootstrapStatusState,
    AgentEnrollmentState, AgentId, AgentInstallationId, AgentSignatureAlgorithm,
    Ed25519PublicKeySpki, Ed25519Signature, Extensions, MountAccessMode, RequestId, ResourceHealth,
    ResourceVersion, UnixMillis, CURRENT_WIRE_VERSION,
};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::time::{self, MissedTickBehavior};
use zeroize::{Zeroize, Zeroizing};

use crate::status_clock::StatusTimestampClock;
use crate::tls::GatewayServerIdentity;
use crate::{
    health::HealthReporter, identity::public_key_spki_der, AgentConfig, AgentDaemonError,
    AgentDaemonResult, AgentSessionClient, AgentSigningKey, EnrollmentBackoff, EnrollmentClient,
    EnrollmentClientError, ReqwestAgentSessionClient, ReqwestEnrollmentClient, RuntimeHealthPhase,
};

const TOKEN_FILE_MAX_BYTES: u64 = 2_050;
const HEALTH_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CertificateRenewalWindow {
    generation: u64,
    renew_at_unix_ms: u64,
    not_after_unix_ms: u64,
}

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

#[derive(Debug, Clone)]
pub struct DevelopmentDirectoryProbe {
    config: DevelopmentDirectoryMountProbeConfig,
}

impl DevelopmentDirectoryProbe {
    #[must_use]
    pub fn new(config: &AgentConfig, identity_root: PathBuf) -> Self {
        Self {
            config: DevelopmentDirectoryMountProbeConfig {
                directory_root: config.storage.mount_path.clone(),
                identity_root,
                expected_volume_marker: config.storage.expected_volume_marker.clone(),
                volume_descriptor_digest: config.volume_descriptor_digest,
                hard_minimum_free_bytes: config.storage.hard_minimum_free_bytes,
                ready_minimum_free_bytes: config.storage.ready_minimum_free_bytes,
            },
        }
    }
}

impl MountProbe for DevelopmentDirectoryProbe {
    fn probe(&self) -> FilesystemMountObservation {
        self.config.probe()
    }
}

pub async fn run(config: AgentConfig) -> AgentDaemonResult<()> {
    config.validate()?;
    let probe = FilesystemProbe::new(&config);
    run_process(config, probe).await
}

/// Runs the Agent with an ordinary-directory mount probe for loopback development only.
pub async fn run_with_development_directory_probe(
    mut config: AgentConfig,
) -> AgentDaemonResult<()> {
    config.validate()?;
    crate::config::validate_development_directory_probe_endpoint(&config.gateway_endpoint)?;
    let identity_root = resolve_development_storage_root(&mut config)?;
    let probe = DevelopmentDirectoryProbe::new(&config, identity_root);
    run_process(config, probe).await
}

fn resolve_development_storage_root(config: &mut AgentConfig) -> AgentDaemonResult<PathBuf> {
    let configured = config.storage.mount_path.clone();
    let configured_parent = configured.parent().ok_or_else(|| {
        AgentDaemonError::MountProbe(
            "development storage root must have an ordinary parent directory".to_owned(),
        )
    })?;
    let configured_name = configured.file_name().ok_or_else(|| {
        AgentDaemonError::MountProbe(
            "development storage root must name an ordinary directory".to_owned(),
        )
    })?;
    let identity_parent = fs::canonicalize(configured_parent).map_err(|error| {
        AgentDaemonError::MountProbe(format!(
            "development storage root parent could not be resolved: {error}"
        ))
    })?;
    let identity_root = identity_parent.join(configured_name);
    let resolved = fs::canonicalize(&configured).map_err(|error| {
        AgentDaemonError::MountProbe(format!(
            "development storage root could not be resolved: {error}"
        ))
    })?;
    let metadata = fs::symlink_metadata(&resolved).map_err(|error| {
        AgentDaemonError::MountProbe(format!(
            "resolved development storage root could not be inspected: {error}"
        ))
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AgentDaemonError::MountProbe(
            "development storage root must resolve to an ordinary directory".to_owned(),
        ));
    }
    let confirmed = fs::canonicalize(&configured).map_err(|error| {
        AgentDaemonError::MountProbe(format!(
            "development storage root could not be confirmed: {error}"
        ))
    })?;
    if confirmed != resolved {
        return Err(AgentDaemonError::MountProbe(
            "development storage root changed while it was being resolved".to_owned(),
        ));
    }
    config.storage.mount_path = resolved;
    config.storage.marker_file = config.storage.mount_path.join(VOLUME_MARKER_FILE_NAME);
    config.validate()?;
    Ok(identity_root)
}

async fn run_process<P>(config: AgentConfig, probe: P) -> AgentDaemonResult<()>
where
    P: MountProbe + Clone,
{
    let trust_bundle = crate::tls::GatewayTrustBundle::load(&config.trust_bundle_file)?;
    let gateway_identity = config
        .gateway_workload_trust_domain
        .as_ref()
        .map(|trust_domain| GatewayServerIdentity {
            trust_domain: trust_domain.clone(),
            edge_cluster_id: config.edge_cluster_id.to_string(),
        });
    let command_trust_bundle = config
        .central_command_trust_bundle_file
        .as_deref()
        .map(crate::CentralCommandTrustBundle::load)
        .transpose()?;
    let client = match gateway_identity.clone() {
        Some(identity) => ReqwestEnrollmentClient::with_gateway_trust_bundle_deferred_identity(
            config.gateway_endpoint.clone(),
            &trust_bundle,
            identity,
        )?,
        None => ReqwestEnrollmentClient::with_gateway_trust_bundle(
            config.gateway_endpoint.clone(),
            &trust_bundle,
        )?,
    };
    let session_client = match gateway_identity {
        Some(identity) => ReqwestAgentSessionClient::with_gateway_trust_bundle_deferred_identity(
            config.gateway_endpoint.clone(),
            &trust_bundle,
            identity,
        )?,
        None => ReqwestAgentSessionClient::with_gateway_trust_bundle(
            config.gateway_endpoint.clone(),
            &trust_bundle,
        )?,
    };

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

    run_with_transports_with_command_trust(
        config,
        client,
        session_client,
        probe,
        shutdown,
        command_trust_bundle,
    )
    .await
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
    let command_trust_bundle = config
        .central_command_trust_bundle_file
        .as_deref()
        .map(crate::CentralCommandTrustBundle::load)
        .transpose()?;
    run_with_transports_with_command_trust(
        config,
        enrollment_client,
        session_client,
        probe,
        shutdown,
        command_trust_bundle,
    )
    .await
}

async fn run_with_transports_with_command_trust<C, S, P, F>(
    config: AgentConfig,
    enrollment_client: C,
    session_client: S,
    probe: P,
    shutdown: F,
    command_trust_bundle: Option<crate::CentralCommandTrustBundle>,
) -> AgentDaemonResult<()>
where
    C: EnrollmentClient + Clone,
    S: AgentSessionClient + 'static,
    P: MountProbe + Clone,
    F: Future<Output = ()> + Send + 'static,
{
    config.validate()?;
    validate_replication_prerequisites(&config, command_trust_bundle.as_ref())?;
    let (replication_network, replication_ready) = if config.replication.enabled {
        // Preserve the startup invariant that a failed mount probe performs no network I/O.
        let observation = probe.probe();
        validate_bootstrap_probe(&observation)?;
        // Bootstrap capability evidence must include a real Gateway QUIC/TLS handshake, not only
        // local config parsing or a socket bind. A temporarily unavailable Gateway leaves the
        // Agent usable for non-replication work and simply withholds the dynamic capability.
        let network = crate::approved_runtime::build_replication_network(&config)?;
        let ready = match network
            .as_ref()
            .expect("enabled replication always builds a network")
            .preflight_gateway()
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "replication Gateway QUIC preflight failed; capability will not be advertised");
                false
            }
        };
        // Keep the preflight endpoint alive for the approved session. Building a second endpoint
        // on the same fixed listener would race Quinn's asynchronous driver shutdown and fail
        // with EADDRINUSE even though no other process owns the port.
        (network, ready)
    } else {
        (None, false)
    };
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_sender.send(true);
    });
    run_with_capabilities(
        config.clone(),
        enrollment_client.clone(),
        probe.clone(),
        wait_for_shutdown(shutdown_receiver.clone()),
        replication_ready,
    )
    .await?;
    if *shutdown_receiver.borrow() {
        return Ok(());
    }

    let store = SqliteSystemIdentityStore::open(&config.storage.state_dir)?;
    let mut identity = store.load()?.ok_or_else(|| {
        AgentDaemonError::Identity("approved Agent identity disappeared".to_owned())
    })?;
    let Some(approved) = identity.approved.clone() else {
        return Ok(());
    };
    let signing_key = crate::signing_key_from_identity(&identity)?;
    if identity.certificate.is_some() {
        enrollment_client
            .install_workload_identity(&identity)
            .map_err(enrollment_error)?;
    }
    let Some(mut resource_version) = query_approved_resource_version(
        &config,
        &enrollment_client,
        &store,
        &mut identity,
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
    enrollment_client
        .install_workload_identity(&identity)
        .map_err(enrollment_error)?;
    let session_client = Arc::new(session_client);
    session_client
        .install_workload_identity(&identity)
        .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
    let agent_id = AgentId::new(approved.agent_id).map_err(protocol_error)?;
    let installation_id =
        AgentInstallationId::new(identity.installation_id.clone()).map_err(protocol_error)?;
    let command_trust_bundle = command_trust_bundle.map(Arc::new);

    loop {
        let Some(renewal_window) = certificate_renewal_window(&identity)? else {
            return crate::approved_runtime::run_approved_session(
                config,
                session_client,
                probe,
                agent_id,
                installation_id,
                signing_key,
                resource_version,
                command_trust_bundle,
                replication_network,
                wait_for_shutdown(shutdown_receiver),
            )
            .await;
        };
        let (reload_sender, reload_receiver) = tokio::sync::watch::channel(false);
        let session = crate::approved_runtime::run_approved_session(
            config.clone(),
            Arc::clone(&session_client),
            probe.clone(),
            agent_id.clone(),
            installation_id.clone(),
            Arc::clone(&signing_key),
            resource_version,
            command_trust_bundle.clone(),
            replication_network.clone(),
            wait_for_shutdown_or_reload(shutdown_receiver.clone(), reload_receiver),
        );
        let renewal = renew_workload_certificate(
            &config,
            &enrollment_client,
            session_client.as_ref(),
            &store,
            &mut identity,
            &signing_key,
            renewal_window,
            shutdown_receiver.clone(),
        );
        tokio::pin!(session);
        tokio::pin!(renewal);
        tokio::select! {
            result = &mut session => return result,
            result = &mut renewal => {
                let _ = reload_sender.send(true);
                match result {
                    Ok(Some(next_resource_version)) => {
                        session.await?;
                        if *shutdown_receiver.borrow() {
                            return Ok(());
                        }
                        resource_version = next_resource_version;
                    }
                    Ok(None) => {
                        session.await?;
                        return Ok(());
                    }
                    Err(error) => {
                        if let Err(close_error) = session.await {
                            tracing::warn!(
                                error = %close_error,
                                "failed to close the Agent session after certificate renewal failed"
                            );
                        }
                        return Err(error);
                    }
                }
            }
        }
    }
}

async fn query_approved_resource_version<C: EnrollmentClient>(
    config: &AgentConfig,
    client: &C,
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
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
                validate_approved_status(identity, &status)?;
                if let Some(certificate) = status.certificate.clone() {
                    install_certificate_bundle(store, identity, config, certificate)?;
                }
                if config.gateway_endpoint.scheme() == "https" && identity.certificate.is_none() {
                    return Err(AgentDaemonError::EnrollmentProtocol(
                        "approved HTTPS Agent status omitted the workload certificate".to_owned(),
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

fn certificate_renewal_window(
    identity: &SystemIdentityRecord,
) -> AgentDaemonResult<Option<CertificateRenewalWindow>> {
    let Some(certificate) = identity.certificate.as_ref() else {
        return Ok(None);
    };
    let (Some(renew_at), Some(not_after)) =
        (certificate.renew_at_unix_ms, certificate.not_after_unix_ms)
    else {
        return Ok(None);
    };
    if certificate.certificate_generation == 0 || renew_at.get() >= not_after.get() {
        return Err(AgentDaemonError::Identity(
            "persisted Agent certificate has an invalid renewal window".to_owned(),
        ));
    }
    Ok(Some(CertificateRenewalWindow {
        generation: certificate.certificate_generation,
        renew_at_unix_ms: renew_at.get(),
        not_after_unix_ms: not_after.get(),
    }))
}

#[allow(clippy::too_many_arguments)]
async fn renew_workload_certificate<C, S>(
    config: &AgentConfig,
    enrollment_client: &C,
    session_client: &S,
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
    signing_key: &AgentSigningKey,
    window: CertificateRenewalWindow,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> AgentDaemonResult<Option<ResourceVersion>>
where
    C: EnrollmentClient,
    S: AgentSessionClient,
{
    if wait_until_or_shutdown(window.renew_at_unix_ms, shutdown.clone()).await? {
        return Ok(None);
    }
    let next_generation = window.generation.checked_add(1).ok_or_else(|| {
        AgentDaemonError::Identity("Agent workload certificate generation is exhausted".to_owned())
    })?;
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
        if *shutdown.borrow_and_update() {
            return Ok(None);
        }
        if now_unix_ms()? >= window.not_after_unix_ms {
            return Err(AgentDaemonError::Enrollment(
                "Agent workload certificate expired before renewal completed".to_owned(),
            ));
        }
        let request = signer.build(identity)?;
        match enrollment_client.status(&request).await {
            Ok(Some(status)) => {
                validate_approved_status(identity, &status)?;
                let certificate = status.certificate.clone().ok_or_else(|| {
                    AgentDaemonError::EnrollmentProtocol(
                        "approved Agent renewal status omitted the workload certificate".to_owned(),
                    )
                })?;
                let generation = certificate.certificate_generation.get();
                if generation < window.generation {
                    return Err(AgentDaemonError::EnrollmentProtocol(
                        "Agent workload certificate renewal moved generation backwards".to_owned(),
                    ));
                }
                if generation > window.generation {
                    install_certificate_bundle(store, identity, config, certificate)?;
                    let installed_generation = identity
                        .certificate
                        .as_ref()
                        .map_or(0, |value| value.certificate_generation);
                    if installed_generation != next_generation {
                        return Err(AgentDaemonError::Identity(
                            "Agent workload certificate renewal did not install the next generation"
                                .to_owned(),
                        ));
                    }
                    enrollment_client
                        .install_workload_identity(identity)
                        .map_err(enrollment_error)?;
                    session_client
                        .install_workload_identity(identity)
                        .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
                    return Ok(Some(status.resource_version));
                }
            }
            Ok(None) => {
                return Err(AgentDaemonError::EnrollmentProtocol(
                    "approved enrollment disappeared during certificate renewal".to_owned(),
                ));
            }
            Err(error) if error.retryable() => {}
            Err(error) => return Err(enrollment_error(error)),
        }
        let delay = next_delay(&mut backoff);
        let retry_at =
            now_unix_ms()?.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
        if wait_until_or_shutdown(retry_at.min(window.not_after_unix_ms), shutdown.clone()).await? {
            return Ok(None);
        }
    }
}

fn validate_approved_status(
    identity: &SystemIdentityRecord,
    status: &AgentBootstrapStatusResponse,
) -> AgentDaemonResult<()> {
    status.validate().map_err(protocol_error)?;
    let approved = identity.approved.as_ref().ok_or_else(|| {
        AgentDaemonError::Identity("approved Agent identity disappeared".to_owned())
    })?;
    let expected_request_id =
        RequestId::new(identity.bootstrap_request_id.clone()).map_err(protocol_error)?;
    let expected_installation_id =
        AgentInstallationId::new(identity.installation_id.clone()).map_err(protocol_error)?;
    if status.bootstrap_request_id != expected_request_id
        || status.installation_id != expected_installation_id
        || status.state != AgentBootstrapStatusState::Approved
        || status.agent_id.as_ref().map(AgentId::as_str) != Some(approved.agent_id.as_str())
        || status.enrollment_id.as_str() != approved.enrollment_id
    {
        return Err(AgentDaemonError::EnrollmentProtocol(
            "approved status query returned another identity or state".to_owned(),
        ));
    }
    Ok(())
}

async fn wait_until_or_shutdown(
    target_unix_ms: u64,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> AgentDaemonResult<bool> {
    let delay = Duration::from_millis(target_unix_ms.saturating_sub(now_unix_ms()?));
    Ok(tokio::select! {
        _ = wait_for_shutdown(shutdown) => true,
        _ = time::sleep(delay) => false,
    })
}

async fn wait_for_shutdown_or_reload(
    shutdown: tokio::sync::watch::Receiver<bool>,
    reload: tokio::sync::watch::Receiver<bool>,
) {
    tokio::select! {
        _ = wait_for_shutdown(shutdown) => {}
        _ = wait_for_shutdown(reload) => {}
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
    run_with_capabilities(config, client, probe, shutdown, false).await
}

async fn run_with_capabilities<C, P, F>(
    config: AgentConfig,
    client: C,
    probe: P,
    shutdown: F,
    replication_ready: bool,
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

    if identity.approved.is_some() && identity.certificate.is_some() {
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
            match apply_status(&config, &store, &mut identity, status, &mut reporter)? {
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
                replication_ready,
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
        &config,
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
    config: &AgentConfig,
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
            Ok(Some(status)) => match apply_status(config, store, identity, status, reporter)? {
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
    config: &AgentConfig,
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
            if let Some(certificate) = status.certificate {
                install_certificate_bundle(store, identity, config, certificate)?;
            }
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

fn install_certificate_bundle(
    store: &SqliteSystemIdentityStore,
    identity: &mut SystemIdentityRecord,
    config: &AgentConfig,
    bundle: neoengram_domain::protocol::AgentWorkloadCertificateBundle,
) -> AgentDaemonResult<()> {
    let certificate = crate::tls::certificate_state_from_bundle(
        &bundle,
        identity,
        config.edge_cluster_id.as_str(),
    )?;
    if identity.certificate.as_ref() == Some(&certificate) {
        return Ok(());
    }
    let expected_generation = identity
        .certificate
        .as_ref()
        .map_or(0, |value| value.certificate_generation);
    match store.install_certificate(identity.revision, expected_generation, certificate) {
        Ok(next) => {
            *identity = next;
            Ok(())
        }
        Err(error) if error.code() == crate::AgentErrorCode::LedgerConflict => {
            *identity = store.load()?.ok_or_else(|| {
                AgentDaemonError::Identity(
                    "Agent identity disappeared during certificate install".to_owned(),
                )
            })?;
            Ok(())
        }
        Err(error) => Err(error.into()),
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
    probe: neoengram_domain::protocol::AgentBootstrapProbe,
    replication_ready: bool,
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
        wire_version: CURRENT_WIRE_VERSION,
        capabilities: agent_capabilities(config, replication_ready),
        public_key_fingerprint: public_key_spki.fingerprint(),
        proof: placeholder_proof(public_key_spki.clone()),
        probe,
        extensions: Extensions::new(),
    };
    sign_bootstrap_request(&mut request, signing_key)?;
    Ok(request)
}

pub(crate) fn agent_capabilities(config: &AgentConfig, replication_ready: bool) -> Vec<String> {
    let mut capabilities = vec![
        "h2_control_channel_v1".to_owned(),
        neoengram_domain::protocol::OPERATION_TASK_CAPABILITY_V1.to_owned(),
        "managed_add_v1".to_owned(),
        "single_volume_v1".to_owned(),
        "volume_local_cas_v1".to_owned(),
        "workspace_materialize_v1".to_owned(),
    ];
    capabilities.push("snapshot_delivery_copy_v2".to_owned());
    // The v2 executor is installed by the approved runtime and uses the same dedicated QUIC
    // network as the replication worker. Advertise the capability only after the preflight has
    // succeeded; Central must never route a materialization batch to an Agent whose Gateway
    // transfer path is unavailable.
    if config.replication.enabled && replication_ready {
        capabilities.push("commit_materialization_v2".to_owned());
    }
    // The v2 FUSE backend and the device/inode proof required by Hardlink Delivery are only
    // implemented on these Unix targets. Do not advertise a mode that would fail after Central
    // has already accepted and persisted a Delivery.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    capabilities.extend([
        "snapshot_delivery_fuse_v2".to_owned(),
        "snapshot_delivery_hardlink_v2".to_owned(),
    ]);
    capabilities
}

fn validate_replication_prerequisites(
    config: &AgentConfig,
    command_trust_bundle: Option<&crate::CentralCommandTrustBundle>,
) -> AgentDaemonResult<()> {
    if !config.replication.enabled {
        return Ok(());
    }
    if command_trust_bundle.is_none() {
        return Err(AgentDaemonError::Configuration(
            "central_command_trust_bundle_file is required when replication is enabled".to_owned(),
        ));
    }
    if config.replication_listen_socket_addr()?.is_none()
        || config.replication_gateway_socket_addr()?.is_none()
    {
        return Err(AgentDaemonError::Configuration(
            "replication listener and Gateway endpoints are required when replication is enabled"
                .to_owned(),
        ));
    }
    Ok(())
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
            wire_version: CURRENT_WIRE_VERSION,
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

fn protocol_error(error: neoengram_domain::protocol::ProtocolError) -> AgentDaemonError {
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

#[allow(clippy::large_enum_variant)]
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
    use neoengram_domain::protocol::{
        AgentEnrollmentId, AgentId, AgentMountId, AgentMountIdentityDigest,
        AgentWorkloadCertificateBundle, AgentWorkloadCertificateDer, CertificateGeneration,
        ContentDigest, MountGeneration, OwnerGeneration, ResourceVersion, SessionGeneration,
        StorageVolumeId, VolumeMarkerId,
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
                    wire_version: CURRENT_WIRE_VERSION,
                    bootstrap_request_id: request.bootstrap_request_id.clone(),
                    installation_id: request.installation_id.clone(),
                    state: AgentBootstrapStatusState::Expired,
                    enrollment_id: AgentEnrollmentId::new("enrollment-expired").unwrap(),
                    agent_id: None,
                    resource_version: ResourceVersion::new(2),
                    updated_at_unix_ms: UnixMillis::new(now_unix_ms().unwrap()),
                    certificate: None,
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

    #[derive(Clone)]
    struct RenewingClient {
        response: AgentBootstrapStatusResponse,
        installed_generations: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl EnrollmentClient for RenewingClient {
        fn install_workload_identity(
            &self,
            identity: &SystemIdentityRecord,
        ) -> Result<(), EnrollmentClientError> {
            self.installed_generations.lock().unwrap().push(
                identity
                    .certificate
                    .as_ref()
                    .map_or(0, |certificate| certificate.certificate_generation),
            );
            Ok(())
        }

        async fn bootstrap(
            &self,
            _request: &AgentBootstrapRequest,
        ) -> Result<AgentBootstrapAccepted, EnrollmentClientError> {
            panic!("approved certificate renewal must not bootstrap")
        }

        async fn status(
            &self,
            request: &AgentBootstrapStatusRequest,
        ) -> Result<Option<AgentBootstrapStatusResponse>, EnrollmentClientError> {
            request.verify().unwrap();
            Ok(Some(self.response.clone()))
        }
    }

    #[cfg(unix)]
    #[test]
    fn development_runtime_uses_the_resolved_storage_root() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let actual = directory.path().join("actual-volume");
        let entry = directory.path().join("desktop-entry");
        let other = directory.path().join("other-volume");
        fs::create_dir(&actual).unwrap();
        fs::create_dir(&other).unwrap();
        symlink(&actual, &entry).unwrap();
        let mut config = test_config(directory.path(), directory.path().join("bootstrap-token"));
        config.storage.mount_path = entry.clone();
        config.storage.marker_file = entry.join(VOLUME_MARKER_FILE_NAME);
        fs::write(
            actual.join(VOLUME_MARKER_FILE_NAME),
            format!("{}\n", config.storage.expected_volume_marker),
        )
        .unwrap();

        let identity_root = resolve_development_storage_root(&mut config).unwrap();
        let resolved = actual.canonicalize().unwrap();
        let expected_identity_root = entry
            .parent()
            .unwrap()
            .canonicalize()
            .unwrap()
            .join(entry.file_name().unwrap());
        let probe = DevelopmentDirectoryProbe::new(&config, identity_root.clone());
        let before = probe.probe();
        fs::remove_file(&entry).unwrap();
        symlink(&other, &entry).unwrap();
        let after = probe.probe();

        assert_eq!(config.storage.mount_path, resolved);
        assert_eq!(identity_root, expected_identity_root);
        assert_eq!(
            config.storage.marker_file,
            resolved.join(VOLUME_MARKER_FILE_NAME)
        );
        assert_eq!(before.condition, MountProbeCondition::Ready);
        assert_eq!(after.condition, MountProbeCondition::Ready);
        assert_eq!(before.mount_identity_digest, after.mount_identity_digest);
    }

    #[cfg(unix)]
    #[test]
    fn development_runtime_identity_matches_canonical_path() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let actual_parent = directory.path().join("actual-parent");
        let linked_parent = directory.path().join("linked-parent");
        let actual = actual_parent.join("volume");
        let entry = linked_parent.join("volume");
        fs::create_dir(&actual_parent).unwrap();
        fs::create_dir(&actual).unwrap();
        symlink(&actual_parent, &linked_parent).unwrap();
        let mut config = test_config(directory.path(), directory.path().join("bootstrap-token"));
        config.storage.mount_path = entry.clone();
        config.storage.marker_file = entry.join(VOLUME_MARKER_FILE_NAME);

        let identity_root = resolve_development_storage_root(&mut config).unwrap();
        let canonical_root = entry.canonicalize().unwrap();

        assert_eq!(identity_root, canonical_root);
        assert_eq!(config.storage.mount_path, canonical_root);
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

    #[tokio::test]
    async fn certificate_renewal_installs_next_generation_and_reloads_clients() {
        let directory = tempfile::tempdir().unwrap();
        let config = test_config(
            directory.path(),
            directory.path().join("unused-bootstrap-token"),
        );
        let store = SqliteSystemIdentityStore::open(&config.storage.state_dir).unwrap();
        let mut identity = crate::load_or_create_identity(&store).unwrap();
        identity = store
            .bind_approved(
                identity.revision,
                ApprovedAgentIdentity::new("agent-a", "enrollment-a").unwrap(),
            )
            .unwrap();
        let now = now_unix_ms().unwrap();
        let first = workload_certificate_bundle(&identity, &config, 1, now, now + 60_000);
        install_certificate_bundle(&store, &mut identity, &config, first).unwrap();
        let second =
            workload_certificate_bundle(&identity, &config, 2, now + 30_000, now + 120_000);
        let response = AgentBootstrapStatusResponse {
            wire_version: CURRENT_WIRE_VERSION,
            bootstrap_request_id: RequestId::new(identity.bootstrap_request_id.clone()).unwrap(),
            installation_id: AgentInstallationId::new(identity.installation_id.clone()).unwrap(),
            state: AgentBootstrapStatusState::Approved,
            enrollment_id: AgentEnrollmentId::new("enrollment-a").unwrap(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            resource_version: ResourceVersion::new(9),
            updated_at_unix_ms: UnixMillis::new(now),
            certificate: Some(second),
            extensions: Extensions::new(),
        };
        let installed_generations = Arc::new(Mutex::new(Vec::new()));
        let enrollment_client = RenewingClient {
            response,
            installed_generations: Arc::clone(&installed_generations),
        };
        let session_client =
            ReqwestAgentSessionClient::new(config.gateway_endpoint.clone()).unwrap();
        let signing_key = crate::signing_key_from_identity(&identity).unwrap();
        let window = certificate_renewal_window(&identity).unwrap().unwrap();
        let (_shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);

        let renewed = tokio::time::timeout(
            Duration::from_secs(2),
            renew_workload_certificate(
                &config,
                &enrollment_client,
                &session_client,
                &store,
                &mut identity,
                &signing_key,
                window,
                shutdown_receiver,
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(renewed, Some(ResourceVersion::new(9)));
        assert_eq!(
            identity
                .certificate
                .as_ref()
                .unwrap()
                .certificate_generation,
            2
        );
        assert_eq!(*installed_generations.lock().unwrap(), [2]);
        assert_eq!(
            store
                .load()
                .unwrap()
                .unwrap()
                .certificate
                .unwrap()
                .certificate_generation,
            2
        );
    }

    fn workload_certificate_bundle(
        identity: &SystemIdentityRecord,
        config: &AgentConfig,
        generation: u64,
        renew_at_unix_ms: u64,
        not_after_unix_ms: u64,
    ) -> AgentWorkloadCertificateBundle {
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
            KeyUsagePurpose, SanType, SubjectPublicKeyInfo,
        };

        let approved = identity.approved.as_ref().unwrap();
        let identity_uri = [
            "spiffe://mesh.example.test/workloads/edge-clusters/",
            config.edge_cluster_id.as_str(),
            "/agents/",
            approved.agent_id.as_str(),
        ]
        .concat();
        let issuer_key = KeyPair::generate().unwrap();
        let mut issuer_parameters = CertificateParams::default();
        issuer_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        issuer_parameters.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let issuer = issuer_parameters.self_signed(&issuer_key).unwrap();
        let signing_key = crate::signing_key_from_identity(identity).unwrap();
        let public_key_spki =
            Ed25519PublicKeySpki::new(public_key_spki_der(&signing_key).unwrap()).unwrap();
        let public_key = SubjectPublicKeyInfo::from_der(public_key_spki.as_der()).unwrap();
        let mut leaf_parameters = CertificateParams::new(Vec::<String>::new()).unwrap();
        leaf_parameters
            .subject_alt_names
            .push(SanType::URI(identity_uri.clone().try_into().unwrap()));
        leaf_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let leaf = leaf_parameters
            .signed_by(&public_key, &issuer, &issuer_key)
            .unwrap();
        AgentWorkloadCertificateBundle {
            certificate_bundle_version: AgentWorkloadCertificateBundle::VERSION,
            edge_cluster_id: config.edge_cluster_id.clone(),
            agent_id: AgentId::new(approved.agent_id.clone()).unwrap(),
            enrollment_id: AgentEnrollmentId::new(approved.enrollment_id.clone()).unwrap(),
            identity_uri,
            public_key_fingerprint: public_key_spki.fingerprint(),
            certificate_generation: CertificateGeneration::new(generation),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            not_before_unix_ms: UnixMillis::new(renew_at_unix_ms.saturating_sub(1_000).max(1)),
            not_after_unix_ms: UnixMillis::new(not_after_unix_ms),
            renew_at_unix_ms: UnixMillis::new(renew_at_unix_ms),
            leaf_certificate_der: AgentWorkloadCertificateDer::new(leaf.der().to_vec()).unwrap(),
            issuer_chain_der: vec![AgentWorkloadCertificateDer::new(issuer.der().to_vec()).unwrap()],
            extensions: Extensions::new(),
        }
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
            wire_version: 1,
            gateway_endpoint: url::Url::parse("http://127.0.0.1:8080/").unwrap(),
            trust_bundle_file: root.join("gateway-ca.pem"),
            gateway_workload_trust_domain: None,
            central_command_trust_bundle_file: None,
            tenant_id: neoengram_domain::protocol::TenantId::new("tenant-a").unwrap(),
            edge_cluster_id: neoengram_domain::protocol::EdgeClusterId::new("edge-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"descriptor-a"),
            region: "region-a".into(),
            storage: StorageConfig {
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteOnce,
                mount_path: root.join("volume"),
                state_dir,
                marker_file: root.join("volume").join(crate::VOLUME_MARKER_FILE_NAME),
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
                token_id: neoengram_domain::protocol::AgentEnrollmentTokenId::new("token-a")
                    .unwrap(),
                bootstrap_token_file: token_path,
            },
            session: SessionConfig {
                heartbeat_interval_seconds: 10,
                reconnect_max_delay_seconds: 30,
            },
            replication: crate::ReplicationConfig::default(),
            logging: LoggingConfig {
                format: LoggingFormat::Json,
                level: "info".into(),
            },
        }
    }

    #[test]
    fn materialization_capability_requires_enabled_preflight() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = test_config(directory.path(), directory.path().join("bootstrap-token"));
        config.replication.enabled = true;

        let capabilities = agent_capabilities(&config, false);

        assert!(!capabilities
            .iter()
            .any(|capability| capability == "commit_materialization_v2"));

        let capabilities = agent_capabilities(&config, true);
        assert!(capabilities
            .iter()
            .any(|capability| capability == "commit_materialization_v2"));

        config.replication.enabled = false;
        let capabilities = agent_capabilities(&config, true);
        assert!(!capabilities
            .iter()
            .any(|capability| capability == "commit_materialization_v2"));
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
