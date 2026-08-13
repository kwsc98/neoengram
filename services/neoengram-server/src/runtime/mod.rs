use std::{
    fmt, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use clap::Parser;
use fusen_rs::{
    Error, ErrorCategory, RunningServer, Server, ServerConfig, ServerError, ServerRequestConfig,
    ServerRequestIdConfig,
};
use neoengram_protocol::{PrincipalKind, UnixMillis};
use neoengramd::{
    open_sqlite_authority, AgentRegistryService, Clock, ControlPlane, GatewayRegistryRepository,
    SqliteAuthority, SqliteAuthorityConfig, TenantRecord,
};
use tokio::io::AsyncReadExt;
use tokio::{
    sync::{watch, Mutex},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use url::Url;

use crate::{
    agent_transport::{
        AgentListenerConfig, AgentServerError, AgentServerHandle, RegistryAgentEnrollmentHandler,
        RunningAgentServer,
    },
    controller::{
        ArtifactApiServer, ArtifactController, GatewayRegistryApiServer, GatewayRegistryController,
        JobApiServer, JobController, PlaygroundApiServer, PlaygroundController, SnapshotApiServer,
        SnapshotController, StorageEnrollmentApiServer, StorageEnrollmentController,
        StorageVolumeApiServer, StorageVolumeController, SystemApiServer, SystemController,
        TenantApiServer, TenantController,
    },
    error::{application_error, map_central_error, NeoEngramProblemEncoder},
    gateway_activation_transport::{GatewayBootstrapTransport, GatewayReplicaActivationClient},
    gateway_connector::{GatewayConnectorConfig, RunningGatewayConnector},
    gateway_transport::CentralGatewayControl,
    identity::{
        AuthenticatedIdentity, AuthenticationInterceptor, Authenticator, OidcAuthenticator,
        OidcConfig, Permission, StaticRbacPolicy, StaticTokenAuthenticator,
    },
    service::{
        AgentDataPlaneService, AgentWorkloadCertificateService, CatalogService,
        CentralCommandKeyring, EnrollmentKeyring, EnrollmentService,
        GatewayCredentialLifecycleService, GatewayRegistryService, GatewayReplicaActivationService,
        HealthService, JobCoordinator, JobService, ReadinessProbe, SystemService,
        WorkloadCertificateIssuer, WorkspaceCommitService,
    },
};

const MAX_RBAC_DOCUMENT_BYTES: u64 = 1024 * 1024;
const ENROLLMENT_EXPIRY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const GATEWAY_CREDENTIAL_EXPIRY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const JOB_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
const GATEWAY_REPLICA_DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);

/// Command-line and environment configuration for the HTTP server.
#[derive(Clone, Parser)]
#[command(name = "neoengram-server", version, about)]
pub struct Config {
    /// Plain HTTP listen address. TLS termination is external to this process.
    #[arg(long, env = "NEOENGRAM_SERVER_BIND", default_value = "127.0.0.1:8080")]
    pub bind: SocketAddr,

    /// Enables Agent enrollment/control handling through registered Gateway replicas.
    #[arg(
        long,
        env = "NEOENGRAM_SERVER_AGENT_ENROLLMENT_ENABLED",
        default_value_t = false
    )]
    pub agent_enrollment_enabled: bool,

    /// Development-only loopback bootstrap listener for Agent integration tests.
    #[arg(long, env = "NEOENGRAM_SERVER_AGENT_BIND")]
    pub agent_bind: Option<SocketAddr>,

    /// Versioned enrollment token/cursor keyring JSON file.
    #[arg(long, env = "NEOENGRAM_SERVER_AGENT_ENROLLMENT_KEYRING_FILE")]
    pub agent_enrollment_keyring_file: Option<PathBuf>,

    /// Central trust bundle used to verify Gateway server certificates.
    #[arg(long, env = "NEOENGRAM_SERVER_GATEWAY_TLS_CA_FILE")]
    pub gateway_tls_ca_file: Option<PathBuf>,

    /// Central workload certificate chain presented to Gateway control listeners.
    #[arg(long, env = "NEOENGRAM_SERVER_GATEWAY_TLS_CLIENT_CERTIFICATE_FILE")]
    pub gateway_tls_client_certificate_file: Option<PathBuf>,

    /// Private key matching the Central workload certificate chain.
    #[arg(
        long,
        env = "NEOENGRAM_SERVER_GATEWAY_TLS_CLIENT_PRIVATE_KEY_FILE",
        hide_env_values = true
    )]
    pub gateway_tls_client_private_key_file: Option<PathBuf>,

    /// SPIFFE trust domain expected in Gateway Replica URI SANs.
    #[arg(long, env = "NEOENGRAM_SERVER_GATEWAY_WORKLOAD_TRUST_DOMAIN")]
    pub gateway_workload_trust_domain: Option<String>,

    /// Directory containing the single-process SQLite authority.
    #[arg(long, env = "NEOENGRAM_SERVER_AUTHORITY_DIR")]
    pub authority_dir: PathBuf,

    /// Immutable deny-by-default RBAC JSON document.
    #[arg(long, env = "NEOENGRAM_SERVER_RBAC_FILE")]
    pub rbac_file: Option<PathBuf>,

    /// OIDC issuer used in production mode.
    #[arg(long, env = "NEOENGRAM_SERVER_OIDC_ISSUER")]
    pub oidc_issuer: Option<Url>,

    /// Required OIDC audience used in production mode.
    #[arg(long, env = "NEOENGRAM_SERVER_OIDC_AUDIENCE")]
    pub oidc_audience: Option<String>,

    /// Optional fixed JWKS URI for providers without normal discovery metadata.
    #[arg(long, env = "NEOENGRAM_SERVER_OIDC_JWKS_URI")]
    pub oidc_jwks_uri: Option<Url>,

    /// Enables the loopback-only fixed-token development profile.
    #[arg(long, env = "NEOENGRAM_SERVER_DEVELOPMENT", default_value_t = false)]
    pub development: bool,

    /// Fixed development Bearer token. Never valid outside explicit development mode.
    #[arg(
        long,
        env = "NEOENGRAM_SERVER_DEVELOPMENT_TOKEN",
        hide_env_values = true
    )]
    pub development_token: Option<String>,

    /// Principal ID injected by the development authenticator.
    #[arg(
        long,
        env = "NEOENGRAM_SERVER_DEVELOPMENT_PRINCIPAL",
        default_value = "development-user"
    )]
    pub development_principal: String,

    /// Tenant grants used when development mode has no RBAC file.
    #[arg(
        long,
        env = "NEOENGRAM_SERVER_DEVELOPMENT_TENANTS",
        value_delimiter = ',',
        default_value = "tenant-local"
    )]
    pub development_tenants: Vec<String>,

    #[arg(
        long,
        env = "NEOENGRAM_SERVER_REQUEST_TIMEOUT_SECS",
        default_value_t = 30
    )]
    pub request_timeout_secs: u64,

    #[arg(
        long,
        env = "NEOENGRAM_SERVER_MAX_REQUEST_BODY_BYTES",
        default_value_t = 2 * 1024 * 1024
    )]
    pub max_request_body_bytes: usize,

    #[arg(
        long,
        env = "NEOENGRAM_SERVER_MAX_RESPONSE_BODY_BYTES",
        default_value_t = 2 * 1024 * 1024
    )]
    pub max_response_body_bytes: usize,

    #[arg(
        long,
        env = "NEOENGRAM_SERVER_MAX_CONCURRENT_REQUESTS",
        default_value_t = 1024
    )]
    pub max_concurrent_requests: usize,

    #[arg(
        long,
        env = "NEOENGRAM_SERVER_GRACEFUL_SHUTDOWN_SECS",
        default_value_t = 30
    )]
    pub graceful_shutdown_secs: u64,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("bind", &self.bind)
            .field("agent_enrollment_enabled", &self.agent_enrollment_enabled)
            .field("agent_bind", &self.agent_bind)
            .field(
                "agent_enrollment_keyring_file",
                &self.agent_enrollment_keyring_file,
            )
            .field("gateway_tls_ca_file", &self.gateway_tls_ca_file)
            .field(
                "gateway_tls_client_certificate_file",
                &self.gateway_tls_client_certificate_file,
            )
            .field(
                "gateway_tls_client_private_key_file",
                &self.gateway_tls_client_private_key_file,
            )
            .field(
                "gateway_workload_trust_domain",
                &self.gateway_workload_trust_domain,
            )
            .field("authority_dir", &self.authority_dir)
            .field("rbac_file", &self.rbac_file)
            .field("oidc_issuer", &self.oidc_issuer)
            .field("oidc_audience", &self.oidc_audience)
            .field("oidc_jwks_uri", &self.oidc_jwks_uri)
            .field("development", &self.development)
            .field(
                "development_token",
                &self.development_token.as_ref().map(|_| "<redacted>"),
            )
            .field("development_principal", &self.development_principal)
            .field("development_tenants", &self.development_tenants)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field("max_response_body_bytes", &self.max_response_body_bytes)
            .field("max_concurrent_requests", &self.max_concurrent_requests)
            .field("graceful_shutdown_secs", &self.graceful_shutdown_secs)
            .finish()
    }
}

impl Config {
    /// Validates all local settings before logging, storage, or network initialization.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        validate_config(self)
    }
}

/// Runtime construction and lifecycle failure.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("invalid server configuration: {0}")]
    Configuration(String),
    #[error("failed to read RBAC policy: {0}")]
    RbacIo(String),
    #[error("invalid RBAC policy: {0}")]
    Rbac(String),
    #[error("OIDC initialization failed: {0}")]
    Oidc(String),
    #[error("authority initialization failed: {0}")]
    Authority(String),
    #[error("enrollment keyring initialization failed: {0}")]
    EnrollmentKeyring(String),
    #[error("Fusen server configuration failed: {0}")]
    FusenConfig(String),
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error(transparent)]
    AgentServer(#[from] AgentServerError),
    #[error("shutdown signal failed: {0}")]
    Signal(#[source] io::Error),
}

/// Fully initialized application composition root.
pub struct AppState {
    authority: Arc<SqliteAuthority>,
    authenticator: Arc<dyn Authenticator>,
    jobs: Arc<JobService>,
    catalog: Arc<CatalogService>,
    gateways: Arc<GatewayRegistryService>,
    gateway_registry: Arc<dyn GatewayRegistryRepository>,
    gateway_credential_lifecycle: Arc<GatewayCredentialLifecycleService>,
    enrollments: Option<Arc<EnrollmentService>>,
    enrollment_registry: Option<Arc<AgentRegistryService>>,
    coordinator: Option<Arc<JobCoordinator>>,
    agent_handler: Option<Arc<RegistryAgentEnrollmentHandler>>,
    gateway_control: Option<Arc<CentralGatewayControl>>,
    gateway_connector_config: Option<GatewayConnectorConfig>,
    gateway_connector: Mutex<Option<RunningGatewayConnector>>,
    agent_running: Mutex<Option<RunningAgentServer>>,
    expiry_reconciler: Mutex<Option<EnrollmentExpiryReconciler>>,
    gateway_credential_reconciler: Mutex<Option<GatewayCredentialExpiryReconciler>>,
    job_reconciler: Mutex<Option<JobReconciler>>,
    accepting: Arc<AtomicBool>,
}

/// Optional runtime dependencies required to complete Gateway Replica activation.
///
/// The online Intermediate CA is deliberately a port: production supplies a KMS/HSM-backed
/// implementation, while tests and local orchestration can inject a deterministic issuer and
/// transport. `AppState::initialize` keeps the route available but returns a retryable
/// unavailable problem until these dependencies are configured.
pub struct GatewayActivationDependencies {
    pub issuer: Arc<dyn WorkloadCertificateIssuer>,
    pub transport: Arc<dyn GatewayBootstrapTransport>,
    pub trust_domain: String,
    /// Optional Central KMS/HSM-backed command signer. Production enrollment runtimes require it
    /// before delivering Assignment or Decision messages; development mode may omit it.
    pub command_keyring: Option<Arc<CentralCommandKeyring>>,
}

impl GatewayActivationDependencies {
    #[must_use]
    pub fn new(
        issuer: Arc<dyn WorkloadCertificateIssuer>,
        transport: Arc<dyn GatewayBootstrapTransport>,
        trust_domain: impl Into<String>,
    ) -> Self {
        Self {
            issuer,
            transport,
            trust_domain: trust_domain.into(),
            command_keyring: None,
        }
    }

    #[must_use]
    pub fn with_command_keyring(mut self, keyring: Arc<CentralCommandKeyring>) -> Self {
        self.command_keyring = Some(keyring);
        self
    }
}

impl AppState {
    /// Opens and checks the authority, then initializes authentication/RBAC and the control plane.
    pub async fn initialize(config: &Config) -> Result<Self, RuntimeError> {
        Self::initialize_with_gateway_activation(config, None).await
    }

    /// Initializes the control plane with the optional Gateway activation dependencies. Central
    /// still owns the Registry and constructs the activation service against that exact store;
    /// callers cannot supply an alternate endpoint or repository through this hook.
    pub async fn initialize_with_gateway_activation(
        config: &Config,
        activation: Option<GatewayActivationDependencies>,
    ) -> Result<Self, RuntimeError> {
        validate_config(config)?;
        let gateway_connector_config = build_gateway_connector_config(config)?;
        let authority = Arc::new(
            open_sqlite_authority(SqliteAuthorityConfig::new(config.authority_dir.clone()))
                .await
                .map_err(|error| RuntimeError::Authority(error.to_string()))?,
        );
        if let Err(error) = authority.integrity_check().await {
            authority.close().await;
            return Err(RuntimeError::Authority(error.to_string()));
        }
        let (authenticator, policy) = match authentication_and_policy(config).await {
            Ok(authentication) => authentication,
            Err(error) => {
                authority.close().await;
                return Err(error);
            }
        };
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let authority_store = authority.authority_store();
        let control = Arc::new(ControlPlane::new(
            policy.clone(),
            authority_store.clone(),
            clock.clone(),
        ));
        let catalog_repository = authority_store.control_catalog().ok_or_else(|| {
            RuntimeError::Authority("SQLite authority has no control catalog".to_owned())
        })?;
        let gateway_repository = authority_store.gateway_registry().ok_or_else(|| {
            RuntimeError::Authority("SQLite authority has no Gateway registry".to_owned())
        })?;
        let gateway_credential_lifecycle = Arc::new(GatewayCredentialLifecycleService::new(
            gateway_repository.clone(),
            clock.clone(),
        ));
        let agent_certificate_dependency = activation.as_ref().map(|dependencies| {
            (
                dependencies.issuer.clone(),
                dependencies.trust_domain.clone(),
            )
        });
        let command_keyring_dependency = activation
            .as_ref()
            .and_then(|dependencies| dependencies.command_keyring.clone());
        let mut gateway_service =
            GatewayRegistryService::new(gateway_repository.clone(), policy.clone(), clock.clone());
        if let Some(activation) = activation {
            let activation_service = Arc::new(GatewayReplicaActivationService::new(
                gateway_repository.clone(),
                activation.issuer,
                clock.clone(),
                activation.trust_domain,
            ));
            let activation_client = Arc::new(GatewayReplicaActivationClient::new(
                activation_service,
                activation.transport,
            ));
            gateway_service = gateway_service.with_activation_client(activation_client);
        }
        let gateways = Arc::new(gateway_service);
        if config.development {
            seed_development_tenants(
                catalog_repository.as_ref(),
                &config.development_tenants,
                clock.now(),
            )
            .await?;
        }
        let accepting = Arc::new(AtomicBool::new(false));
        let (enrollments, enrollment_registry, agent_handler, coordinator) = if config
            .agent_enrollment_enabled
        {
            let keyring_path =
                config
                    .agent_enrollment_keyring_file
                    .as_deref()
                    .ok_or_else(|| {
                        RuntimeError::Configuration("Agent enrollment keyring is missing".into())
                    })?;
            let keyring = match EnrollmentKeyring::load(keyring_path).await {
                Ok(keyring) => Arc::new(keyring),
                Err(error) => {
                    authority.close().await;
                    return Err(RuntimeError::EnrollmentKeyring(error.to_string()));
                }
            };
            let registry = match AgentRegistryService::from_authority(
                &authority.authority_store(),
                clock.clone(),
                30_000,
            ) {
                Ok(registry) => Arc::new(registry),
                Err(error) => {
                    authority.close().await;
                    return Err(RuntimeError::Authority(error.to_string()));
                }
            };
            let enrollments = Arc::new(EnrollmentService::new(
                registry.clone(),
                catalog_repository.clone(),
                policy.clone(),
                keyring,
                clock.clone(),
            ));
            let data_plane = Arc::new(AgentDataPlaneService::new(authority.clone()));
            let mut agent_handler = RegistryAgentEnrollmentHandler::with_transport(
                registry.clone(),
                control.clone(),
                data_plane,
                accepting.clone(),
            )
            .with_gateway_registry(gateway_repository.clone());
            if let Some(keyring) = command_keyring_dependency {
                agent_handler = agent_handler.with_central_command_keyring(keyring);
            } else if !config.development {
                agent_handler = agent_handler.require_central_command_signatures();
            }
            if let Some((issuer, trust_domain)) = agent_certificate_dependency {
                let certificate_service =
                    AgentWorkloadCertificateService::new(registry.clone(), issuer, trust_domain)
                        .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
                agent_handler =
                    agent_handler.with_workload_certificate_service(Arc::new(certificate_service));
            }
            let agent_handler = Arc::new(agent_handler);
            let coordinator = Arc::new(
                JobCoordinator::from_authority(
                    control.clone(),
                    &authority_store,
                    clock.clone(),
                    30_000,
                )
                .map_err(|error| RuntimeError::Authority(error.to_string()))?,
            );
            (
                Some(enrollments),
                Some(registry),
                Some(agent_handler),
                Some(coordinator),
            )
        } else {
            (None, None, None, None)
        };
        let catalog = CatalogService::new(
            catalog_repository,
            authority_store.publisher(),
            policy.clone(),
            clock.clone(),
        );
        let catalog = match authority_store.precommits() {
            Some(precommits) => catalog.with_precommits(precommits),
            None => catalog,
        };
        let catalog = match coordinator.as_ref() {
            Some(coordinator) => catalog.with_coordinator(coordinator.clone()),
            None => catalog,
        };
        let catalog = match enrollment_registry.as_ref() {
            Some(registry) => catalog.with_agent_registry(registry.clone()),
            None => catalog,
        };
        let workspace_commits =
            WorkspaceCommitService::from_authority(&authority_store, policy.clone(), clock.clone())
                .map(Arc::new)
                .map_err(|error| RuntimeError::Authority(error.to_string()))?;
        let catalog = catalog.with_workspace_commits(workspace_commits);
        let catalog = Arc::new(catalog);
        let jobs = JobService::from_authority(control, &authority_store)
            .map_err(|error| RuntimeError::Authority(error.to_string()))?;
        let jobs = match coordinator.as_ref() {
            Some(coordinator) => jobs.with_coordinator(coordinator.clone()),
            None => jobs,
        };
        let gateway_control = agent_handler.as_ref().map(|handler| {
            Arc::new(CentralGatewayControl::new(
                gateway_repository.clone(),
                handler.clone(),
                clock.clone(),
            ))
        });
        Ok(Self {
            authority,
            authenticator,
            jobs: Arc::new(jobs),
            catalog,
            gateways,
            gateway_registry: gateway_repository,
            gateway_credential_lifecycle,
            enrollments,
            enrollment_registry,
            coordinator,
            agent_handler,
            gateway_control,
            gateway_connector_config,
            gateway_connector: Mutex::new(None),
            agent_running: Mutex::new(None),
            expiry_reconciler: Mutex::new(None),
            gateway_credential_reconciler: Mutex::new(None),
            job_reconciler: Mutex::new(None),
            accepting,
        })
    }

    /// Starts the public listener, the outbound Gateway connector, and any development listener.
    pub async fn start_server(&self, config: &Config) -> Result<RunningServer, RuntimeError> {
        validate_config(config)?;
        if let Some(registry) = &self.enrollment_registry {
            registry
                .reconcile_expired_enrollments()
                .await
                .map_err(|error| RuntimeError::Authority(error.to_string()))?;
        }
        self.gateway_credential_lifecycle
            .reconcile_expired_credentials()
            .await
            .map_err(|error| RuntimeError::Authority(error.to_string()))?;
        // Complete all fallible, non-I/O public-server construction before exposing the Agent
        // socket. A build failure must not leave an Agent listener draining against a closing DB.
        let server = self.build_server(config)?;
        let agent_running = match (&self.agent_handler, config.agent_bind) {
            (Some(handler), Some(bind)) if config.development => Some(
                RunningAgentServer::start(
                    AgentListenerConfig {
                        bind,
                        graceful_shutdown_timeout: Duration::from_secs(
                            config.graceful_shutdown_secs,
                        ),
                    },
                    handler.clone(),
                )
                .await?,
            ),
            _ => None,
        };
        let running = match server.start().await {
            Ok(running) => running,
            Err(error) => {
                if let Some(agent) = agent_running {
                    let _ = agent.shutdown().await;
                }
                return Err(RuntimeError::Server(error));
            }
        };
        *self.agent_running.lock().await = agent_running;
        *self.gateway_connector.lock().await = self.gateway_control.as_ref().map(|control| {
            RunningGatewayConnector::start(
                self.gateway_registry.clone(),
                control.clone(),
                GATEWAY_REPLICA_DISCOVERY_INTERVAL,
                self.gateway_connector_config
                    .clone()
                    .expect("Gateway connector config exists when control is enabled"),
            )
        });
        *self.expiry_reconciler.lock().await = self
            .enrollment_registry
            .as_ref()
            .map(|registry| EnrollmentExpiryReconciler::start(registry.clone()));
        *self.gateway_credential_reconciler.lock().await = Some(
            GatewayCredentialExpiryReconciler::start(self.gateway_credential_lifecycle.clone()),
        );
        *self.job_reconciler.lock().await = self
            .coordinator
            .as_ref()
            .map(|coordinator| JobReconciler::start(coordinator.clone()));
        self.accepting.store(true, Ordering::Release);
        Ok(running)
    }

    fn build_server(&self, config: &Config) -> Result<Server, RuntimeError> {
        let readiness: Arc<dyn ReadinessProbe> = Arc::new(AuthorityReadiness {
            authority: self.authority.clone(),
            accepting: self.accepting.clone(),
        });
        let mut builder = Server::builder(config.bind.to_string())
            .config(server_config(config)?)
            .head_interceptor(AuthenticationInterceptor::new(self.authenticator.clone()))
            .problem_encoder(NeoEngramProblemEncoder)
            .interface(SystemApiServer::new(SystemController::new(
                Arc::new(SystemService::new(
                    self.enrollment_registry.is_some() && self.coordinator.is_some(),
                )),
                Arc::new(HealthService::new(readiness)),
            )))
            .interface(JobApiServer::new(JobController::new(self.jobs.clone())));
        builder = builder
            .interface(GatewayRegistryApiServer::new(
                GatewayRegistryController::new(self.gateways.clone()),
            ))
            .interface(TenantApiServer::new(TenantController::new(
                self.catalog.clone(),
            )))
            .interface(StorageVolumeApiServer::new(StorageVolumeController::new(
                self.catalog.clone(),
            )))
            .interface(ArtifactApiServer::new(ArtifactController::new(
                self.catalog.clone(),
            )))
            .interface(PlaygroundApiServer::new(PlaygroundController::new(
                self.catalog.clone(),
            )))
            .interface(SnapshotApiServer::new(SnapshotController::new(
                self.catalog.clone(),
            )));
        if let Some(enrollments) = &self.enrollments {
            builder = builder.interface(StorageEnrollmentApiServer::new(
                StorageEnrollmentController::new(enrollments.clone()),
            ));
        }
        builder.build().map_err(RuntimeError::Server)
    }

    /// Returns the development-only Agent bootstrap address after [`Self::start_server`].
    pub async fn agent_local_addr(&self) -> Option<SocketAddr> {
        self.agent_running
            .lock()
            .await
            .as_ref()
            .map(RunningAgentServer::local_addr)
    }

    async fn agent_handle(&self) -> Option<AgentServerHandle> {
        self.agent_running
            .lock()
            .await
            .as_ref()
            .map(RunningAgentServer::handle)
    }

    async fn stop_agent_listener(&self) -> Result<(), AgentServerError> {
        let running = self.agent_running.lock().await.take();
        match running {
            Some(running) => running.shutdown().await,
            None => Ok(()),
        }
    }

    async fn stop_gateway_connector(&self) {
        let connector = self.gateway_connector.lock().await.take();
        if let Some(connector) = connector {
            connector.shutdown().await;
        }
    }

    async fn stop_expiry_reconciler(&self) {
        if let Some(reconciler) = self.expiry_reconciler.lock().await.take() {
            reconciler.shutdown().await;
        }
    }

    async fn stop_gateway_credential_reconciler(&self) {
        if let Some(reconciler) = self.gateway_credential_reconciler.lock().await.take() {
            reconciler.shutdown().await;
        }
    }

    async fn stop_job_reconciler(&self) {
        if let Some(reconciler) = self.job_reconciler.lock().await.take() {
            reconciler.shutdown().await;
        }
    }

    /// Marks the application unavailable and closes its authority connections.
    pub async fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        let _ = self.stop_agent_listener().await;
        self.stop_gateway_connector().await;
        self.stop_expiry_reconciler().await;
        self.stop_gateway_credential_reconciler().await;
        self.stop_job_reconciler().await;
        self.authority.close().await;
    }
}

async fn seed_development_tenants(
    catalog: &dyn neoengramd::ControlCatalogRepository,
    configured: &[String],
    now: UnixMillis,
) -> Result<(), RuntimeError> {
    for tenant in configured.iter().filter(|tenant| tenant.as_str() != "*") {
        let tenant_id = neoengram_protocol::TenantId::new(tenant.clone())
            .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
        if catalog
            .get_tenant(&tenant_id)
            .await
            .map_err(|error| RuntimeError::Authority(error.to_string()))?
            .is_some()
        {
            continue;
        }
        catalog
            .insert_tenant(TenantRecord {
                tenant_id,
                display_name: tenant.clone(),
                description: None,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .map_err(|error| RuntimeError::Authority(error.to_string()))?;
    }
    Ok(())
}

struct EnrollmentExpiryReconciler {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl EnrollmentExpiryReconciler {
    fn start(registry: Arc<AgentRegistryService>) -> Self {
        let (shutdown, mut receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(ENROLLMENT_EXPIRY_RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! {
                    changed = receiver.changed() => {
                        if changed.is_err() || *receiver.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        match registry.reconcile_expired_enrollments().await {
                            Ok(result) => {
                                if result.expired_token_intents != 0
                                    || result.expired_review_enrollments != 0
                                {
                                    tracing::info!(
                                        expired_token_intents = result.expired_token_intents,
                                        expired_review_enrollments = result.expired_review_enrollments,
                                        "reconciled expired Agent enrollments"
                                    );
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    code = error.code().as_str(),
                                    "Agent enrollment expiry reconciliation failed"
                                );
                            }
                        }
                    }
                }
            }
        });
        Self { shutdown, task }
    }

    async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        if let Err(error) = self.task.await {
            tracing::warn!(%error, "Agent enrollment expiry reconciler task failed");
        }
    }
}

struct GatewayCredentialExpiryReconciler {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl GatewayCredentialExpiryReconciler {
    fn start(lifecycle: Arc<GatewayCredentialLifecycleService>) -> Self {
        let (shutdown, mut receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(GATEWAY_CREDENTIAL_EXPIRY_RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! {
                    changed = receiver.changed() => {
                        if changed.is_err() || *receiver.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        match lifecycle.reconcile_expired_credentials().await {
                            Ok(run) if run.revoked != 0 || run.contended != 0 => {
                                tracing::info!(
                                    examined = run.examined,
                                    revoked = run.revoked,
                                    contended = run.contended,
                                    "Gateway credential expiry reconciliation completed"
                                );
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(
                                    code = error.code().as_str(),
                                    "Gateway credential expiry reconciliation failed"
                                );
                            }
                        }
                    }
                }
            }
        });
        Self { shutdown, task }
    }

    async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        if let Err(error) = self.task.await {
            tracing::warn!(%error, "Gateway credential expiry reconciler task failed");
        }
    }
}

struct JobReconciler {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl JobReconciler {
    fn start(coordinator: Arc<JobCoordinator>) -> Self {
        let (shutdown, mut receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(JOB_RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = receiver.changed() => {
                        if changed.is_err() || *receiver.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        match coordinator.reconcile_once().await {
                            Ok(run) if run.assigned != 0 || run.expired != 0 || run.finalized != 0 => {
                                tracing::info!(
                                    examined = run.examined,
                                    assigned = run.assigned,
                                    expired = run.expired,
                                    finalized = run.finalized,
                                    "Job coordinator recovery pass completed"
                                );
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(%error, "Job coordinator recovery pass failed");
                            }
                        }
                    }
                }
            }
        });
        Self { shutdown, task }
    }

    async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        if let Err(error) = self.task.await {
            tracing::warn!(%error, "Job coordinator task failed");
        }
    }
}

/// Starts the server, handles SIGINT/SIGTERM, drains requests, and closes SQLite.
pub async fn run(config: Config) -> Result<(), RuntimeError> {
    let state = AppState::initialize(&config).await?;
    let running = match state.start_server(&config).await {
        Ok(running) => running,
        Err(error) => {
            state.close().await;
            return Err(error);
        }
    };
    let address = running.local_addr();
    let handle = running.handle();
    let agent_handle = state.agent_handle().await;
    let agent_address = state.agent_local_addr().await;
    tracing::info!(public_address = %address, ?agent_address, "NeoEngram HTTP server is ready");
    let waiter = handle.clone();
    let agent_waiter = agent_handle.clone();
    let result = tokio::select! {
        result = waiter.wait() => result.map_err(RuntimeError::Server),
        result = wait_for_agent(agent_waiter) => result.map_err(RuntimeError::AgentServer),
        signal = shutdown_signal() => {
            state.accepting.store(false, Ordering::Release);
            signal.map_err(RuntimeError::Signal)
        }
    };
    state.accepting.store(false, Ordering::Release);
    let public_shutdown = handle.shutdown();
    let agent_shutdown = shutdown_agent_handle(agent_handle);
    let (public_result, agent_result) = tokio::join!(public_shutdown, agent_shutdown);
    state.close().await;
    result?;
    public_result.map_err(RuntimeError::Server)?;
    agent_result.map_err(RuntimeError::AgentServer)
}

async fn wait_for_agent(handle: Option<AgentServerHandle>) -> Result<(), AgentServerError> {
    match handle {
        Some(handle) => handle.wait().await,
        None => std::future::pending().await,
    }
}

async fn shutdown_agent_handle(handle: Option<AgentServerHandle>) -> Result<(), AgentServerError> {
    match handle {
        Some(handle) => handle.shutdown().await,
        None => Ok(()),
    }
}

fn validate_config(config: &Config) -> Result<(), RuntimeError> {
    if config.authority_dir.as_os_str().is_empty() {
        return Err(RuntimeError::Configuration(
            "authority directory must be explicit".to_owned(),
        ));
    }
    match (
        config.agent_enrollment_enabled,
        config.agent_enrollment_keyring_file.as_ref(),
    ) {
        (true, Some(path)) => {
            if path.as_os_str().is_empty() {
                return Err(RuntimeError::Configuration(
                    "Agent enrollment keyring path must be explicit".to_owned(),
                ));
            }
        }
        (true, None) => {
            return Err(RuntimeError::Configuration(
                "Agent enrollment requires --agent-enrollment-keyring-file".to_owned(),
            ));
        }
        (false, None) => {}
        (false, Some(_)) => {
            return Err(RuntimeError::Configuration(
                "Agent enrollment settings require --agent-enrollment-enabled".to_owned(),
            ));
        }
    }
    let gateway_tls_count = [
        config.gateway_tls_ca_file.is_some(),
        config.gateway_tls_client_certificate_file.is_some(),
        config.gateway_tls_client_private_key_file.is_some(),
        config.gateway_workload_trust_domain.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    if gateway_tls_count > 0 && gateway_tls_count < 4 {
        return Err(RuntimeError::Configuration(
            "Gateway mTLS requires CA, client certificate, client private key, and workload trust domain"
                .to_owned(),
        ));
    }
    if gateway_tls_count > 0 && !config.agent_enrollment_enabled {
        return Err(RuntimeError::Configuration(
            "Gateway mTLS settings require --agent-enrollment-enabled".to_owned(),
        ));
    }
    if config.agent_enrollment_enabled && !config.development && gateway_tls_count == 0 {
        return Err(RuntimeError::Configuration(
            "production Agent control requires Gateway mTLS configuration".to_owned(),
        ));
    }
    for (name, path) in [
        ("Gateway CA bundle", config.gateway_tls_ca_file.as_ref()),
        (
            "Gateway client certificate",
            config.gateway_tls_client_certificate_file.as_ref(),
        ),
        (
            "Gateway client private key",
            config.gateway_tls_client_private_key_file.as_ref(),
        ),
    ] {
        if path.is_some_and(|path| path.as_os_str().is_empty()) {
            return Err(RuntimeError::Configuration(format!(
                "{name} path must be explicit"
            )));
        }
    }
    if let Some(agent_bind) = config.agent_bind {
        if !config.agent_enrollment_enabled {
            return Err(RuntimeError::Configuration(
                "Agent bootstrap listener requires --agent-enrollment-enabled".to_owned(),
            ));
        }
        if !config.development {
            return Err(RuntimeError::Configuration(
                "production Agent control must use registered Gateway replicas; --agent-bind is development-only"
                    .to_owned(),
            ));
        }
        if !agent_bind.ip().is_loopback() {
            return Err(RuntimeError::Configuration(
                "development Agent bootstrap listener is restricted to a loopback bind address"
                    .to_owned(),
            ));
        }
        if agent_bind == config.bind && agent_bind.port() != 0 {
            return Err(RuntimeError::Configuration(
                "public and Agent listeners must use different socket addresses".to_owned(),
            ));
        }
    }
    if config.request_timeout_secs == 0
        || config.graceful_shutdown_secs == 0
        || config.max_request_body_bytes == 0
        || config.max_response_body_bytes == 0
        || config.max_concurrent_requests == 0
    {
        return Err(RuntimeError::Configuration(
            "timeouts and resource limits must be greater than zero".to_owned(),
        ));
    }
    if config
        .rbac_file
        .as_ref()
        .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err(RuntimeError::Configuration(
            "RBAC policy path must be explicit".to_owned(),
        ));
    }
    if config.development {
        if !config.bind.ip().is_loopback() {
            return Err(RuntimeError::Configuration(
                "development authentication is restricted to a loopback bind address".to_owned(),
            ));
        }
        if config.development_token.is_none() {
            return Err(RuntimeError::Configuration(
                "development mode requires a fixed Bearer token".to_owned(),
            ));
        }
        if config.oidc_issuer.is_some()
            || config.oidc_audience.is_some()
            || config.oidc_jwks_uri.is_some()
        {
            return Err(RuntimeError::Configuration(
                "development fixed-token mode cannot also configure OIDC".to_owned(),
            ));
        }
        let principal = config.development_principal.clone();
        let identity = AuthenticatedIdentity::new(
            principal.clone(),
            PrincipalKind::User,
            Arc::<str>::from("development"),
            Arc::<str>::from(principal.clone()),
        )
        .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
        StaticTokenAuthenticator::new(
            config.development_token.clone().unwrap_or_default(),
            identity,
        )
        .map_err(RuntimeError::Configuration)?;
        if config.rbac_file.is_none() {
            StaticRbacPolicy::one_principal(
                principal,
                config.development_tenants.clone(),
                [
                    Permission::CreateAddJob,
                    Permission::QueryJob,
                    Permission::FinalizeAdd,
                    Permission::TenantRead,
                    Permission::TenantCreate,
                    Permission::StorageRead,
                    Permission::StorageCreate,
                    Permission::StorageEnrollmentCreate,
                    Permission::StorageEnrollmentRead,
                    Permission::StorageEnrollmentReview,
                    Permission::ArtifactRead,
                    Permission::ArtifactCreate,
                    Permission::PlaygroundRead,
                    Permission::PlaygroundCreate,
                    Permission::SnapshotRead,
                    Permission::SnapshotCreate,
                    Permission::GatewayRead,
                    Permission::GatewayManage,
                ],
            )
            .map_err(RuntimeError::Configuration)?;
        }
    } else {
        if config.development_token.is_some() {
            return Err(RuntimeError::Configuration(
                "a fixed Bearer token is only valid in explicit development mode".to_owned(),
            ));
        }
        if config.oidc_issuer.is_none() || config.oidc_audience.is_none() {
            return Err(RuntimeError::Configuration(
                "production mode requires an OIDC issuer and audience".to_owned(),
            ));
        }
        if config.rbac_file.is_none() {
            return Err(RuntimeError::Configuration(
                "production mode requires an RBAC policy file".to_owned(),
            ));
        }
        let mut oidc = OidcConfig::new(
            config.oidc_issuer.clone().expect("checked above"),
            config.oidc_audience.clone().expect("checked above"),
        );
        oidc.jwks_uri_override = config.oidc_jwks_uri.clone();
        oidc.validate()
            .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
    }
    Ok(())
}

fn build_gateway_connector_config(
    config: &Config,
) -> Result<Option<GatewayConnectorConfig>, RuntimeError> {
    if !config.agent_enrollment_enabled {
        return Ok(None);
    }
    let Some(ca_file) = config.gateway_tls_ca_file.as_deref() else {
        // `validate_config` restricts this branch to explicit development mode. The resulting
        // policy still rejects every non-loopback or public plaintext endpoint.
        return Ok(Some(GatewayConnectorConfig::loopback_development()));
    };
    let connector = GatewayConnectorConfig::load_mtls(
        ca_file,
        config
            .gateway_tls_client_certificate_file
            .as_deref()
            .expect("validated Gateway client certificate path"),
        config
            .gateway_tls_client_private_key_file
            .as_deref()
            .expect("validated Gateway client private key path"),
        config
            .gateway_workload_trust_domain
            .as_deref()
            .expect("validated Gateway workload trust domain"),
        config.development,
    )
    .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
    Ok(Some(connector))
}

async fn authentication_and_policy(
    config: &Config,
) -> Result<(Arc<dyn Authenticator>, Arc<StaticRbacPolicy>), RuntimeError> {
    if config.development {
        let principal_id = config.development_principal.clone();
        let identity = AuthenticatedIdentity::new(
            principal_id.clone(),
            PrincipalKind::User,
            Arc::<str>::from("development"),
            Arc::<str>::from(principal_id.clone()),
        )
        .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
        let token = config
            .development_token
            .clone()
            .ok_or_else(|| RuntimeError::Configuration("development token is missing".into()))?;
        let authenticator: Arc<dyn Authenticator> = Arc::new(
            StaticTokenAuthenticator::new(token, identity).map_err(RuntimeError::Configuration)?,
        );
        let policy = if let Some(path) = &config.rbac_file {
            load_policy(path).await?
        } else {
            Arc::new(
                StaticRbacPolicy::one_principal(
                    principal_id,
                    config.development_tenants.clone(),
                    [
                        Permission::CreateAddJob,
                        Permission::QueryJob,
                        Permission::FinalizeAdd,
                        Permission::TenantRead,
                        Permission::TenantCreate,
                        Permission::StorageRead,
                        Permission::StorageCreate,
                        Permission::StorageEnrollmentCreate,
                        Permission::StorageEnrollmentRead,
                        Permission::StorageEnrollmentReview,
                        Permission::ArtifactRead,
                        Permission::ArtifactCreate,
                        Permission::PlaygroundRead,
                        Permission::PlaygroundCreate,
                        Permission::SnapshotRead,
                        Permission::SnapshotCreate,
                        Permission::GatewayRead,
                        Permission::GatewayManage,
                    ],
                )
                .map_err(RuntimeError::Rbac)?,
            )
        };
        Ok((authenticator, policy))
    } else {
        let issuer = config
            .oidc_issuer
            .clone()
            .ok_or_else(|| RuntimeError::Configuration("OIDC issuer is missing".into()))?;
        let audience = config
            .oidc_audience
            .clone()
            .ok_or_else(|| RuntimeError::Configuration("OIDC audience is missing".into()))?;
        let mut oidc = OidcConfig::new(issuer, audience);
        oidc.jwks_uri_override = config.oidc_jwks_uri.clone();
        let authenticator: Arc<dyn Authenticator> = Arc::new(
            OidcAuthenticator::discover(oidc)
                .await
                .map_err(|error| RuntimeError::Oidc(error.to_string()))?,
        );
        let policy = load_policy(
            config
                .rbac_file
                .as_deref()
                .ok_or_else(|| RuntimeError::Configuration("RBAC policy is missing".into()))?,
        )
        .await?;
        Ok((authenticator, policy))
    }
}

async fn load_policy(path: &Path) -> Result<Arc<StaticRbacPolicy>, RuntimeError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|error| RuntimeError::RbacIo(error.to_string()))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|error| RuntimeError::RbacIo(error.to_string()))?;
    if !metadata.is_file() || metadata.len() > MAX_RBAC_DOCUMENT_BYTES {
        return Err(RuntimeError::RbacIo(
            "RBAC policy must be a regular file no larger than 1 MiB".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_RBAC_DOCUMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| RuntimeError::RbacIo(error.to_string()))?;
    if bytes.len() as u64 > MAX_RBAC_DOCUMENT_BYTES {
        return Err(RuntimeError::RbacIo(
            "RBAC policy exceeds the 1 MiB size limit".to_owned(),
        ));
    }
    StaticRbacPolicy::from_json(&bytes)
        .map(Arc::new)
        .map_err(RuntimeError::Rbac)
}

fn server_config(config: &Config) -> Result<ServerConfig, RuntimeError> {
    let inflight_multiplier = config.max_concurrent_requests.min(32);
    let request = ServerRequestConfig::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .max_concurrent_requests(config.max_concurrent_requests)
        .max_request_body_bytes(config.max_request_body_bytes)
        .max_response_body_bytes(config.max_response_body_bytes)
        .max_inflight_request_body_bytes(
            config
                .max_request_body_bytes
                .saturating_mul(inflight_multiplier),
        )
        .max_inflight_response_body_bytes(
            config
                .max_response_body_bytes
                .saturating_mul(inflight_multiplier),
        )
        .build()
        .map_err(|error| RuntimeError::FusenConfig(error.to_string()))?;
    let request_id = ServerRequestIdConfig::builder()
        .max_bytes(128)
        .allow_colon(true)
        .require_alphanumeric_prefix(true)
        .always_emit(true)
        .build()
        .map_err(|error| RuntimeError::FusenConfig(error.to_string()))?;
    ServerConfig::builder()
        .request(request)
        .request_id(request_id)
        .graceful_shutdown_timeout(Duration::from_secs(config.graceful_shutdown_secs))
        .build()
        .map_err(|error| RuntimeError::FusenConfig(error.to_string()))
}

struct AuthorityReadiness {
    authority: Arc<SqliteAuthority>,
    accepting: Arc<AtomicBool>,
}

#[async_trait]
impl ReadinessProbe for AuthorityReadiness {
    async fn check(&self) -> Result<(), Error> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "server_not_ready",
                "SERVER_NOT_READY",
                "the server is not ready to accept requests",
                true,
            ));
        }
        self.authority
            .readiness_check()
            .await
            .map_err(map_central_error)
    }
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UnixMillis {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        UnixMillis::new(millis)
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use tempfile::TempDir;

    use crate::{
        dto::{
            ActivateGatewayReplicaRequest, CreateGatewayPoolRequest, CreateGatewayReplicaRequest,
        },
        gateway_activation_transport::GatewayBootstrapTransportError,
        service::{
            GatewayReplicaActivationChallenge, GatewayReplicaActivationProof,
            IssuedWorkloadCertificate, WorkloadCertificateIssuerError, WorkloadCertificateRequest,
        },
    };

    use super::*;

    #[tokio::test]
    async fn gateway_activation_dependencies_are_wired_to_the_registry_service() {
        let authority = TempDir::new().unwrap();
        let mut config = development_agent_config();
        config.authority_dir = authority.path().to_path_buf();
        config.agent_enrollment_enabled = false;
        config.agent_enrollment_keyring_file = None;
        let transport = Arc::new(RecordingUnavailableBootstrapTransport::default());
        let state = AppState::initialize_with_gateway_activation(
            &config,
            Some(GatewayActivationDependencies::new(
                Arc::new(UnusedWorkloadIssuer),
                transport.clone(),
                "mesh.example.test",
            )),
        )
        .await
        .unwrap();
        let identity = AuthenticatedIdentity::new(
            config.development_principal.clone(),
            PrincipalKind::User,
            Arc::<str>::from("development"),
            Arc::<str>::from(config.development_principal.clone()),
        )
        .unwrap();
        state
            .gateways
            .create_pool(
                &identity,
                CreateGatewayPoolRequest {
                    gateway_pool_id: "pool-runtime".into(),
                    edge_cluster_id: "edge-runtime".into(),
                    display_name: "Runtime Gateway".into(),
                    agent_endpoint: "https://gateway.runtime.example".into(),
                    s3_endpoint: None,
                    desired_replicas: 1,
                    minimum_ready_replicas: 1,
                },
            )
            .await
            .unwrap();
        let created = state
            .gateways
            .create_replica(
                &identity,
                CreateGatewayReplicaRequest {
                    gateway_replica_id: "replica-runtime".into(),
                    gateway_pool_id: "pool-runtime".into(),
                    control_endpoint: "https://control.runtime.example".into(),
                    peer_endpoint: "https://peer.runtime.example".into(),
                    bootstrap_endpoint: "https://bootstrap.runtime.example".into(),
                    software_version: "test".into(),
                    supported_protocol_versions: vec![1],
                    capabilities: neoengram_protocol::gateway_capabilities_v1()
                        .into_iter()
                        .collect(),
                },
            )
            .await
            .unwrap();
        let result = state
            .gateways
            .activate_replica(
                &identity,
                ActivateGatewayReplicaRequest {
                    gateway_replica_id: "replica-runtime".into(),
                    expected_resource_version: created.gateway_replica.resource_version,
                    activation_token: created.activation_token.unwrap(),
                },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            transport.endpoint.lock().unwrap().as_deref(),
            Some("https://bootstrap.runtime.example")
        );
        state.close().await;
    }

    struct UnusedWorkloadIssuer;

    #[async_trait]
    impl WorkloadCertificateIssuer for UnusedWorkloadIssuer {
        async fn issue(
            &self,
            _request: WorkloadCertificateRequest,
        ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
            Err(WorkloadCertificateIssuerError::Internal(
                "issuer must not be reached after a transport failure".into(),
            ))
        }
    }

    #[derive(Default)]
    struct RecordingUnavailableBootstrapTransport {
        endpoint: StdMutex<Option<String>>,
    }

    #[async_trait]
    impl GatewayBootstrapTransport for RecordingUnavailableBootstrapTransport {
        async fn prove(
            &self,
            bootstrap_endpoint: &str,
            _challenge: &GatewayReplicaActivationChallenge,
        ) -> Result<GatewayReplicaActivationProof, GatewayBootstrapTransportError> {
            *self.endpoint.lock().unwrap() = Some(bootstrap_endpoint.to_owned());
            Err(GatewayBootstrapTransportError::Request(
                "injected transport failure".into(),
            ))
        }

        async fn deliver_certificate(
            &self,
            _bootstrap_endpoint: &str,
            _certificate: &IssuedWorkloadCertificate,
        ) -> Result<(), GatewayBootstrapTransportError> {
            panic!("certificate delivery must not run after proof transport failure")
        }
    }

    #[test]
    fn production_agent_control_requires_gateway_only_configuration() {
        let config = production_agent_config();
        config.validate().expect("Gateway-only production config");

        let mut direct = config;
        direct.agent_bind = Some("127.0.0.1:8081".parse().expect("socket address"));
        let error = direct
            .validate()
            .expect_err("production direct Agent listener must fail closed");
        assert!(error
            .to_string()
            .contains("--agent-bind is development-only"));
    }

    #[test]
    fn production_agent_control_requires_complete_gateway_mtls() {
        let mut config = production_agent_config();
        config.gateway_tls_client_private_key_file = None;
        let error = config
            .validate()
            .expect_err("production Gateway control must not run without a client key");
        assert!(error
            .to_string()
            .contains("requires CA, client certificate"));

        let mut development = development_agent_config();
        development.gateway_tls_ca_file = Some(PathBuf::from("/tmp/gateway-ca.pem"));
        let error = development
            .validate()
            .expect_err("partial development mTLS must fail closed");
        assert!(error
            .to_string()
            .contains("requires CA, client certificate"));
    }

    #[test]
    fn development_agent_bootstrap_listener_is_loopback_only() {
        let mut config = development_agent_config();
        config.agent_bind = Some("0.0.0.0:8081".parse().expect("socket address"));
        let error = config
            .validate()
            .expect_err("non-loopback development Agent listener must fail closed");
        assert!(error.to_string().contains("restricted to a loopback"));

        config.agent_bind = Some("127.0.0.1:8081".parse().expect("socket address"));
        config
            .validate()
            .expect("loopback development bootstrap listener");
    }

    fn production_agent_config() -> Config {
        Config {
            bind: "127.0.0.1:8080".parse().expect("socket address"),
            agent_enrollment_enabled: true,
            agent_bind: None,
            agent_enrollment_keyring_file: Some(PathBuf::from("/tmp/enrollment-keyring.json")),
            gateway_tls_ca_file: Some(PathBuf::from("/tmp/gateway-ca.pem")),
            gateway_tls_client_certificate_file: Some(PathBuf::from("/tmp/central-cert.pem")),
            gateway_tls_client_private_key_file: Some(PathBuf::from("/tmp/central-key.pem")),
            gateway_workload_trust_domain: Some("mesh.example.test".to_owned()),
            authority_dir: PathBuf::from("/tmp/neoengram-authority"),
            rbac_file: Some(PathBuf::from("/tmp/neoengram-rbac.json")),
            oidc_issuer: Some(Url::parse("https://issuer.example").expect("OIDC issuer")),
            oidc_audience: Some("neoengram".to_owned()),
            oidc_jwks_uri: None,
            development: false,
            development_token: None,
            development_principal: "unused".to_owned(),
            development_tenants: Vec::new(),
            request_timeout_secs: 30,
            max_request_body_bytes: 1024,
            max_response_body_bytes: 1024,
            max_concurrent_requests: 16,
            graceful_shutdown_secs: 30,
        }
    }

    fn development_agent_config() -> Config {
        Config {
            bind: "127.0.0.1:8080".parse().expect("socket address"),
            agent_enrollment_enabled: true,
            agent_bind: None,
            agent_enrollment_keyring_file: Some(PathBuf::from("/tmp/enrollment-keyring.json")),
            gateway_tls_ca_file: None,
            gateway_tls_client_certificate_file: None,
            gateway_tls_client_private_key_file: None,
            gateway_workload_trust_domain: None,
            authority_dir: PathBuf::from("/tmp/neoengram-authority"),
            rbac_file: None,
            oidc_issuer: None,
            oidc_audience: None,
            oidc_jwks_uri: None,
            development: true,
            development_token: Some("test-token".to_owned()),
            development_principal: "development-user".to_owned(),
            development_tenants: vec!["*".to_owned()],
            request_timeout_secs: 30,
            max_request_body_bytes: 1024,
            max_response_body_bytes: 1024,
            max_concurrent_requests: 16,
            graceful_shutdown_secs: 30,
        }
    }
}
