use std::{
    fmt,
    io::{self, Read as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    open_sqlite_authority, AgentRegistryService, Clock, ControlPlane, GatewayRegistryRepository,
    SqliteAuthority, SqliteAuthorityConfig, TenantRecord,
};
use async_trait::async_trait;
use clap::Parser;
use fusen_rs::{
    Error, ErrorCategory, RunningServer, Server, ServerConfig, ServerError, ServerRequestConfig,
    ServerRequestIdConfig,
};
use neoengram_domain::protocol::{PrincipalKind, UnixMillis};
use tokio::io::AsyncReadExt;
use tokio::{
    sync::{watch, Mutex},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use url::Url;

use crate::{
    agent_transport::RegistryAgentApiHandler,
    controller::{
        ArtifactApiServer, ArtifactController, GatewayRegistryApiServer, GatewayRegistryController,
        JobApiServer, JobController, PlaygroundApiServer, PlaygroundController, ProjectApiServer,
        ProjectController, ResourceLifecycleApiServer, ResourceLifecycleController, S3ApiServer,
        S3AuthorizationApiServer, S3AuthorizationController, S3Controller, SnapshotApiServer,
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
        HealthService, JobCoordinator, JobService, ReadinessProbe, ResourceLifecycleCoordinator,
        S3SecretEnvelope, SystemService, WorkloadCertificateIssuer, WorkspaceCommitService,
    },
};

const MAX_RBAC_DOCUMENT_BYTES: u64 = 1024 * 1024;
const ENROLLMENT_EXPIRY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const GATEWAY_CREDENTIAL_EXPIRY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const JOB_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
const RESOURCE_LIFECYCLE_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
const GATEWAY_REPLICA_DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_DEVELOPMENT_PERMISSIONS: [Permission; 26] = [
    Permission::CreateAddJob,
    Permission::QueryJob,
    Permission::FinalizeAdd,
    Permission::TenantRead,
    Permission::TenantCreate,
    Permission::TenantAdmin,
    Permission::StorageRead,
    Permission::StorageCreate,
    Permission::StorageEnrollmentCreate,
    Permission::StorageEnrollmentRead,
    Permission::StorageEnrollmentReview,
    Permission::ArtifactRead,
    Permission::ArtifactCreate,
    Permission::ProjectRead,
    Permission::ProjectCreate,
    Permission::PlaygroundRead,
    Permission::PlaygroundCreate,
    Permission::SnapshotRead,
    Permission::SnapshotCreate,
    Permission::S3AccessRead,
    Permission::S3AccessManage,
    Permission::ResourceLifecycleRead,
    Permission::ResourceLifecycleManage,
    Permission::RetentionManage,
    Permission::GatewayRead,
    Permission::GatewayManage,
];

/// Command-line and environment configuration for the HTTP server.
#[derive(Clone, Parser)]
#[command(name = "neoengram-central", version, about)]
pub struct Config {
    /// Plain HTTP listen address. TLS termination is external to this process.
    #[arg(long, env = "NEOENGRAM_CENTRAL_BIND", default_value = "127.0.0.1:8080")]
    pub bind: SocketAddr,

    /// Enables Agent enrollment/control handling through registered Gateway replicas.
    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_AGENT_ENROLLMENT_ENABLED",
        default_value_t = false
    )]
    pub agent_enrollment_enabled: bool,

    /// Versioned enrollment token/cursor keyring JSON file.
    #[arg(long, env = "NEOENGRAM_CENTRAL_AGENT_ENROLLMENT_KEYRING_FILE")]
    pub agent_enrollment_keyring_file: Option<PathBuf>,

    /// Central trust bundle used to verify Gateway server certificates.
    #[arg(long, env = "NEOENGRAM_CENTRAL_GATEWAY_TLS_CA_FILE")]
    pub gateway_tls_ca_file: Option<PathBuf>,

    /// Central workload certificate chain presented to Gateway control listeners.
    #[arg(long, env = "NEOENGRAM_CENTRAL_GATEWAY_TLS_CLIENT_CERTIFICATE_FILE")]
    pub gateway_tls_client_certificate_file: Option<PathBuf>,

    /// Private key matching the Central workload certificate chain.
    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_GATEWAY_TLS_CLIENT_PRIVATE_KEY_FILE",
        hide_env_values = true
    )]
    pub gateway_tls_client_private_key_file: Option<PathBuf>,

    /// SPIFFE trust domain expected in Gateway Replica URI SANs.
    #[arg(long, env = "NEOENGRAM_CENTRAL_GATEWAY_WORKLOAD_TRUST_DOMAIN")]
    pub gateway_workload_trust_domain: Option<String>,

    /// Directory containing the single-process SQLite authority.
    #[arg(long, env = "NEOENGRAM_CENTRAL_AUTHORITY_DIR")]
    pub authority_dir: PathBuf,

    /// Immutable deny-by-default RBAC JSON document.
    #[arg(long, env = "NEOENGRAM_CENTRAL_RBAC_FILE")]
    pub rbac_file: Option<PathBuf>,

    /// Development-only raw 32-byte KEK used by the local S3 envelope adapter.
    #[arg(long, env = "SYNAPSE_S3_ENVELOPE_KEY_FILE", hide_env_values = true)]
    pub s3_envelope_key_file: Option<PathBuf>,

    /// OIDC issuer used in production mode.
    #[arg(long, env = "NEOENGRAM_CENTRAL_OIDC_ISSUER")]
    pub oidc_issuer: Option<Url>,

    /// Required OIDC audience used in production mode.
    #[arg(long, env = "NEOENGRAM_CENTRAL_OIDC_AUDIENCE")]
    pub oidc_audience: Option<String>,

    /// Optional fixed JWKS URI for providers without normal discovery metadata.
    #[arg(long, env = "NEOENGRAM_CENTRAL_OIDC_JWKS_URI")]
    pub oidc_jwks_uri: Option<Url>,

    /// Enables the loopback-only fixed-token development profile.
    #[arg(long, env = "NEOENGRAM_CENTRAL_DEVELOPMENT", default_value_t = false)]
    pub development: bool,

    /// Fixed development Bearer token. Never valid outside explicit development mode.
    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_DEVELOPMENT_TOKEN",
        hide_env_values = true
    )]
    pub development_token: Option<String>,

    /// Principal ID injected by the development authenticator.
    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_DEVELOPMENT_PRINCIPAL",
        default_value = "development-user"
    )]
    pub development_principal: String,

    /// Tenant grants used when development mode has no RBAC file.
    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_DEVELOPMENT_TENANTS",
        value_delimiter = ',',
        default_value = "tenant-local"
    )]
    pub development_tenants: Vec<String>,

    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_REQUEST_TIMEOUT_SECS",
        default_value_t = 30
    )]
    pub request_timeout_secs: u64,

    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_MAX_REQUEST_BODY_BYTES",
        default_value_t = 2 * 1024 * 1024
    )]
    pub max_request_body_bytes: usize,

    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_MAX_RESPONSE_BODY_BYTES",
        default_value_t = 2 * 1024 * 1024
    )]
    pub max_response_body_bytes: usize,

    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_MAX_CONCURRENT_REQUESTS",
        default_value_t = 1024
    )]
    pub max_concurrent_requests: usize,

    #[arg(
        long,
        env = "NEOENGRAM_CENTRAL_GRACEFUL_SHUTDOWN_SECS",
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
            .field(
                "s3_envelope_key_file",
                &self.s3_envelope_key_file.as_ref().map(|_| "<redacted>"),
            )
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
    resource_lifecycle_coordinator: Option<Arc<ResourceLifecycleCoordinator>>,
    gateway_control: Option<Arc<CentralGatewayControl>>,
    gateway_connector_config: Option<GatewayConnectorConfig>,
    gateway_connector: Mutex<Option<RunningGatewayConnector>>,
    expiry_reconciler: Mutex<Option<EnrollmentExpiryReconciler>>,
    gateway_credential_reconciler: Mutex<Option<GatewayCredentialExpiryReconciler>>,
    job_reconciler: Mutex<Option<JobReconciler>>,
    resource_lifecycle_reconciler: Mutex<Option<ResourceLifecycleReconciler>>,
    s3_ticket_signing_enabled: bool,
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
        }
    }
}

/// Externally supplied production ports used by the Central runtime.
///
/// The command keyring is independent of Gateway Replica activation: deployments commonly use a
/// pre-provisioned GatewayPool while still requiring the KMS/HSM signer for Agent commands and S3
/// read tickets.
#[derive(Default)]
pub struct RuntimeDependencies {
    gateway_activation: Option<GatewayActivationDependencies>,
    central_command_keyring: Option<Arc<CentralCommandKeyring>>,
    s3_secret_envelope: Option<(Arc<dyn S3SecretEnvelope>, [u8; 32])>,
}

impl RuntimeDependencies {
    #[must_use]
    pub fn with_gateway_activation(mut self, activation: GatewayActivationDependencies) -> Self {
        self.gateway_activation = Some(activation);
        self
    }

    #[must_use]
    pub fn with_central_command_keyring(mut self, keyring: Arc<CentralCommandKeyring>) -> Self {
        self.central_command_keyring = Some(keyring);
        self
    }

    /// Installs the production KMS/HSM envelope provider and independent cursor HMAC key.
    #[must_use]
    pub fn with_s3_secret_envelope(
        mut self,
        envelope: Arc<dyn S3SecretEnvelope>,
        cursor_signing_key: [u8; 32],
    ) -> Self {
        self.s3_secret_envelope = Some((envelope, cursor_signing_key));
        self
    }
}

impl AppState {
    /// Opens and checks the authority, then initializes authentication/RBAC and the control plane.
    pub async fn initialize(config: &Config) -> Result<Self, RuntimeError> {
        Self::initialize_with_dependencies(config, RuntimeDependencies::default()).await
    }

    /// Initializes the control plane with the optional Gateway activation dependencies. Central
    /// still owns the Registry and constructs the activation service against that exact store;
    /// callers cannot supply an alternate endpoint or repository through this hook.
    pub async fn initialize_with_gateway_activation(
        config: &Config,
        activation: Option<GatewayActivationDependencies>,
    ) -> Result<Self, RuntimeError> {
        let dependencies = match activation {
            Some(activation) => RuntimeDependencies::default().with_gateway_activation(activation),
            None => RuntimeDependencies::default(),
        };
        Self::initialize_with_dependencies(config, dependencies).await
    }

    /// Initializes the control plane with independently supplied production ports.
    pub async fn initialize_with_dependencies(
        config: &Config,
        dependencies: RuntimeDependencies,
    ) -> Result<Self, RuntimeError> {
        validate_config(config)?;
        let RuntimeDependencies {
            gateway_activation: activation,
            central_command_keyring: command_keyring_dependency,
            s3_secret_envelope,
        } = dependencies;
        if s3_secret_envelope.is_some() && config.s3_envelope_key_file.is_some() {
            return Err(RuntimeError::Configuration(
                "an injected S3 envelope provider cannot be combined with a local envelope key file"
                    .to_owned(),
            ));
        }
        if !config.development && s3_secret_envelope.is_none() {
            return Err(RuntimeError::Configuration(
                "production mode requires a KMS/HSM-backed S3 secret envelope provider".to_owned(),
            ));
        }
        let local_s3_envelope_key = if s3_secret_envelope.is_none() {
            Some(load_s3_envelope_key(config)?)
        } else {
            None
        };
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
        let s3_ticket_signing_enabled = command_keyring_dependency.is_some();
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
            let mut agent_handler = RegistryAgentApiHandler::with_transport(
                registry.clone(),
                control.clone(),
                data_plane,
                accepting.clone(),
            )
            .with_gateway_registry(gateway_repository.clone());
            if let Some(keyring) = command_keyring_dependency.as_ref() {
                agent_handler = agent_handler.with_central_command_keyring(keyring.clone());
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
        let gateway_control = agent_handler.as_ref().map(|handler| {
            Arc::new(CentralGatewayControl::new(
                gateway_repository.clone(),
                handler.clone(),
                clock.clone(),
            ))
        });
        let authority_lifecycle = authority_store.authority_lifecycle();
        let resource_lifecycle_coordinator =
            if agent_handler.is_some() && command_keyring_dependency.is_some() {
                let agent_repository = authority_store.agent_registry().ok_or_else(|| {
                    RuntimeError::Authority(
                        "SQLite authority has no Agent registry for resource lifecycle".to_owned(),
                    )
                })?;
                let authority_lifecycle = authority_lifecycle.clone().ok_or_else(|| {
                    RuntimeError::Authority(
                        "SQLite authority has no lifecycle cleanup repository".to_owned(),
                    )
                })?;
                Some(Arc::new(ResourceLifecycleCoordinator::new(
                    catalog_repository.clone(),
                    agent_repository,
                    authority_lifecycle,
                    authority_store.objects(),
                    clock.clone(),
                )))
            } else {
                None
            };
        let catalog = CatalogService::new(
            catalog_repository,
            authority_store.publisher(),
            policy.clone(),
            clock.clone(),
        )
        .with_lifecycle_objects(authority_store.objects());
        let catalog = match authority_lifecycle {
            Some(authority_lifecycle) => catalog.with_lifecycle_authority(authority_lifecycle),
            None => catalog,
        };
        let catalog = match s3_secret_envelope {
            Some((envelope, cursor_signing_key)) => {
                catalog.with_s3_secret_envelope(envelope, cursor_signing_key)
            }
            None => catalog.with_s3_envelope_key(
                local_s3_envelope_key.expect("local development envelope key was loaded"),
            ),
        };
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
        let catalog = match command_keyring_dependency.as_ref() {
            Some(keyring) => catalog.with_s3_ticket_keyring(keyring.clone()),
            None => catalog,
        };
        let catalog = catalog.with_gateway_registry(gateway_repository.clone());
        let catalog = match gateway_control.as_ref() {
            Some(gateway_control) => {
                catalog.with_s3_read_revocation_publisher(gateway_control.clone())
            }
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
            resource_lifecycle_coordinator,
            gateway_control,
            gateway_connector_config,
            gateway_connector: Mutex::new(None),
            expiry_reconciler: Mutex::new(None),
            gateway_credential_reconciler: Mutex::new(None),
            job_reconciler: Mutex::new(None),
            resource_lifecycle_reconciler: Mutex::new(None),
            s3_ticket_signing_enabled,
            accepting,
        })
    }

    /// Starts the public listener and the outbound Gateway connector.
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
        let server = self.build_server(config)?;
        let running = match server.start().await {
            Ok(running) => running,
            Err(error) => return Err(RuntimeError::Server(error)),
        };
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
        *self.resource_lifecycle_reconciler.lock().await = self
            .resource_lifecycle_coordinator
            .as_ref()
            .map(|coordinator| ResourceLifecycleReconciler::start(coordinator.clone()));
        self.accepting.store(true, Ordering::Release);
        Ok(running)
    }

    fn build_server(&self, config: &Config) -> Result<Server, RuntimeError> {
        let readiness: Arc<dyn ReadinessProbe> = Arc::new(AuthorityReadiness {
            authority: self.authority.clone(),
            accepting: self.accepting.clone(),
        });
        let storage_execution_enabled =
            self.enrollment_registry.is_some() && self.coordinator.is_some();
        let system = SystemService::new(storage_execution_enabled)
            .with_s3_readonly_access_point(
                storage_execution_enabled && self.s3_ticket_signing_enabled,
            )
            .with_resource_lifecycle(self.resource_lifecycle_coordinator.is_some());
        let mut builder = Server::builder(config.bind.to_string())
            .config(server_config(config)?)
            .head_interceptor(AuthenticationInterceptor::new(self.authenticator.clone()))
            .problem_encoder(NeoEngramProblemEncoder)
            .interface(SystemApiServer::new(SystemController::new(
                Arc::new(system),
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
            .interface(ProjectApiServer::new(ProjectController::new(
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
            )))
            .interface(ResourceLifecycleApiServer::new(
                ResourceLifecycleController::new(self.catalog.clone()),
            ))
            .interface(S3ApiServer::new(S3Controller::new(self.catalog.clone())))
            .interface(S3AuthorizationApiServer::new(
                S3AuthorizationController::new(self.catalog.clone()),
            ));
        if let Some(enrollments) = &self.enrollments {
            builder = builder.interface(StorageEnrollmentApiServer::new(
                StorageEnrollmentController::new(enrollments.clone()),
            ));
        }
        builder.build().map_err(RuntimeError::Server)
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

    async fn stop_resource_lifecycle_reconciler(&self) {
        if let Some(reconciler) = self.resource_lifecycle_reconciler.lock().await.take() {
            reconciler.shutdown().await;
        }
    }

    /// Marks the application unavailable and closes its authority connections.
    pub async fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        self.stop_gateway_connector().await;
        self.stop_expiry_reconciler().await;
        self.stop_gateway_credential_reconciler().await;
        self.stop_job_reconciler().await;
        self.stop_resource_lifecycle_reconciler().await;
        self.authority.close().await;
    }
}

async fn seed_development_tenants(
    catalog: &dyn crate::ControlCatalogRepository,
    configured: &[String],
    now: UnixMillis,
) -> Result<(), RuntimeError> {
    for tenant in configured.iter().filter(|tenant| tenant.as_str() != "*") {
        let tenant_id = neoengram_domain::protocol::TenantId::new(tenant.clone())
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

struct ResourceLifecycleReconciler {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ResourceLifecycleReconciler {
    fn start(coordinator: Arc<ResourceLifecycleCoordinator>) -> Self {
        let (shutdown, mut receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(RESOURCE_LIFECYCLE_RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = receiver.changed() => {
                        if changed.is_err() || *receiver.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        match coordinator.reconcile_once(100).await {
                            Ok(run) if run.transitioned != 0
                                || run.assignments_published != 0
                                || run.blocked != 0 =>
                            {
                                tracing::info!(
                                    examined = run.examined,
                                    transitioned = run.transitioned,
                                    assignments_published = run.assignments_published,
                                    blocked = run.blocked,
                                    "resource lifecycle reconciliation completed"
                                );
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(%error, "resource lifecycle reconciliation failed");
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
            tracing::warn!(%error, "resource lifecycle reconciler task failed");
        }
    }
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
    run_with_dependencies(config, RuntimeDependencies::default()).await
}

/// Runs Central with externally supplied KMS/HSM and Gateway activation ports.
pub async fn run_with_dependencies(
    config: Config,
    dependencies: RuntimeDependencies,
) -> Result<(), RuntimeError> {
    let state = AppState::initialize_with_dependencies(&config, dependencies).await?;
    let running = match state.start_server(&config).await {
        Ok(running) => running,
        Err(error) => {
            state.close().await;
            return Err(error);
        }
    };
    let address = running.local_addr();
    let handle = running.handle();
    tracing::info!(public_address = %address, "NeoEngram HTTP server is ready");
    let waiter = handle.clone();
    let result = tokio::select! {
        result = waiter.wait() => result.map_err(RuntimeError::Server),
        signal = shutdown_signal() => {
            state.accepting.store(false, Ordering::Release);
            signal.map_err(RuntimeError::Signal)
        }
    };
    state.accepting.store(false, Ordering::Release);
    let public_shutdown = handle.shutdown();
    let public_result = public_shutdown.await;
    state.close().await;
    result?;
    public_result.map_err(RuntimeError::Server)
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
    if config
        .s3_envelope_key_file
        .as_ref()
        .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err(RuntimeError::Configuration(
            "S3 envelope key path must be explicit".to_owned(),
        ));
    }
    if !config.development && config.s3_envelope_key_file.is_some() {
        return Err(RuntimeError::Configuration(
            "the local S3 envelope key file is development-only; production injects a KMS/HSM-backed envelope provider"
                .to_owned(),
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
                DEFAULT_DEVELOPMENT_PERMISSIONS,
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

fn load_s3_envelope_key(config: &Config) -> Result<[u8; 32], RuntimeError> {
    let Some(path) = config.s3_envelope_key_file.as_deref() else {
        debug_assert!(
            config.development,
            "local S3 envelope keys are development-only"
        );
        return Ok(crate::service::development_s3_envelope_key());
    };
    let path_metadata = std::fs::symlink_metadata(path).map_err(|error| {
        RuntimeError::Configuration(format!("failed to inspect S3 envelope key file: {error}"))
    })?;
    if path_metadata.file_type().is_symlink() {
        return Err(RuntimeError::Configuration(
            "S3 envelope key file must not be a symbolic link".to_owned(),
        ));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|error| {
        RuntimeError::Configuration(format!("failed to open S3 envelope key file: {error}"))
    })?;
    let metadata = file.metadata().map_err(|error| {
        RuntimeError::Configuration(format!("failed to inspect S3 envelope key file: {error}"))
    })?;
    if !metadata.is_file() || metadata.len() != 32 {
        return Err(RuntimeError::Configuration(
            "S3 envelope key file must be a regular file containing exactly 32 bytes".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        if metadata.uid() != rustix::process::geteuid().as_raw() {
            return Err(RuntimeError::Configuration(
                "S3 envelope key file must be owned by the effective process user".to_owned(),
            ));
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(RuntimeError::Configuration(
                "S3 envelope key file permissions must not grant group or other access".to_owned(),
            ));
        }
    }
    let mut key = [0_u8; 32];
    file.read_exact(&mut key).map_err(|error| {
        RuntimeError::Configuration(format!("failed to read S3 envelope key file: {error}"))
    })?;
    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing).map_err(|error| {
        RuntimeError::Configuration(format!("failed to read S3 envelope key file: {error}"))
    })? != 0
    {
        return Err(RuntimeError::Configuration(
            "S3 envelope key file must contain exactly 32 bytes".to_owned(),
        ));
    }
    Ok(key)
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
                    DEFAULT_DEVELOPMENT_PERMISSIONS,
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
    use neoengram_domain::protocol::CURRENT_WIRE_VERSION;
    use tempfile::TempDir;

    use crate::{
        dto::{
            ActivateGatewayReplicaRequest, CreateGatewayPoolRequest, CreateGatewayReplicaRequest,
        },
        gateway_activation_transport::GatewayBootstrapTransportError,
        service::{
            CentralCommandKeyId, CentralCommandKeyState, CentralCommandSignature,
            CentralCommandSignatureRequest, CentralCommandSigner, CentralCommandSignerError,
            CentralCommandTrustBundle, CentralCommandVerificationKey,
            GatewayReplicaActivationChallenge, GatewayReplicaActivationProof,
            IssuedWorkloadCertificate, WorkloadCertificateIssuerError, WorkloadCertificateRequest,
        },
    };

    use super::*;

    struct UnavailableCommandSigner;

    #[async_trait]
    impl CentralCommandSigner for UnavailableCommandSigner {
        async fn sign(
            &self,
            _request: CentralCommandSignatureRequest,
        ) -> Result<CentralCommandSignature, CentralCommandSignerError> {
            Err(CentralCommandSignerError::Unavailable(
                "test signer is intentionally offline".to_owned(),
            ))
        }
    }

    fn test_command_keyring() -> Arc<CentralCommandKeyring> {
        let generation = neoengram_domain::protocol::CertificateGeneration::new(1);
        let key_id = CentralCommandKeyId::new("central-runtime-test").unwrap();
        let verification_key = CentralCommandVerificationKey::new(
            key_id.clone(),
            generation,
            neoengram_domain::protocol::Ed25519PublicKeySpki::from_public_key_bytes([7; 32]),
            CentralCommandKeyState::Active,
        )
        .unwrap();
        Arc::new(
            CentralCommandKeyring::new(
                Arc::new(UnavailableCommandSigner),
                CentralCommandTrustBundle::new(vec![verification_key]).unwrap(),
                key_id,
                generation,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn command_keyring_is_injected_without_gateway_activation() {
        let authority = TempDir::new().unwrap();
        let mut config = development_agent_config();
        config.authority_dir = authority.path().to_path_buf();
        config.agent_enrollment_enabled = false;
        config.agent_enrollment_keyring_file = None;

        let state = AppState::initialize_with_dependencies(
            &config,
            RuntimeDependencies::default().with_central_command_keyring(test_command_keyring()),
        )
        .await
        .unwrap();

        assert!(state.s3_ticket_signing_enabled);
        state.close().await;
    }

    #[tokio::test]
    async fn default_development_policy_grants_resource_management_permissions() {
        let config = development_agent_config();
        let (_, policy) = authentication_and_policy(&config).await.unwrap();
        let identity = AuthenticatedIdentity::new(
            config.development_principal,
            PrincipalKind::User,
            Arc::<str>::from("development"),
            Arc::<str>::from("development-user"),
        )
        .unwrap();
        let tenant_id = neoengram_domain::protocol::TenantId::new("tenant-s3-development").unwrap();
        let permissions = policy.permission_names(identity.principal(), &tenant_id);

        assert!(policy.is_allowed(identity.principal(), Permission::S3AccessRead, &tenant_id));
        assert!(policy.is_allowed(identity.principal(), Permission::S3AccessManage, &tenant_id));
        assert!(policy.is_allowed(
            identity.principal(),
            Permission::ResourceLifecycleManage,
            &tenant_id
        ));
        assert!(policy.is_allowed(
            identity.principal(),
            Permission::RetentionManage,
            &tenant_id
        ));
        assert!(permissions.iter().any(|name| name == "s3.access.read"));
        assert!(permissions.iter().any(|name| name == "s3.access.manage"));
        assert!(permissions
            .iter()
            .any(|name| name == "resource.lifecycle.manage"));
        assert!(permissions.iter().any(|name| name == "retention.manage"));
    }

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
                    wire_version: CURRENT_WIRE_VERSION.get(),
                    capabilities: neoengram_domain::protocol::gateway_capabilities_v1()
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

        let _ = config;
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
    fn production_rejects_the_development_file_envelope_adapter() {
        let mut config = production_agent_config();
        config.s3_envelope_key_file = Some(PathBuf::from("/tmp/s3-envelope-key"));
        let error = config
            .validate()
            .expect_err("production S3 credential encryption must use the injected KMS port");
        assert!(error.to_string().contains("development-only"));
    }

    #[tokio::test]
    async fn production_runtime_requires_an_injected_s3_envelope_provider() {
        let error = match AppState::initialize_with_dependencies(
            &production_agent_config(),
            RuntimeDependencies::default(),
        )
        .await
        {
            Ok(state) => {
                state.close().await;
                panic!("production unexpectedly initialized without an S3 envelope provider")
            }
            Err(error) => error,
        };
        assert!(error.to_string().contains("KMS/HSM-backed"));
    }

    #[test]
    fn s3_envelope_key_loader_requires_exactly_32_bytes() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("s3-envelope.key");
        std::fs::write(&path, [0x5a; 31]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut config = development_agent_config();
        config.s3_envelope_key_file = Some(path.clone());
        assert!(load_s3_envelope_key(&config).is_err());

        std::fs::write(&path, [0xa5; 32]).unwrap();
        assert_eq!(load_s3_envelope_key(&config).unwrap(), [0xa5; 32]);
    }

    #[cfg(unix)]
    #[test]
    fn s3_envelope_key_loader_rejects_group_or_other_access() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = TempDir::new().unwrap();
        let path = directory.path().join("s3-envelope.key");
        std::fs::write(&path, [0x5a; 32]).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let mut config = development_agent_config();
        config.s3_envelope_key_file = Some(path);
        let error = load_s3_envelope_key(&config).unwrap_err();
        assert!(error.to_string().contains("group or other access"));
    }

    #[cfg(unix)]
    #[test]
    fn s3_envelope_key_loader_rejects_symbolic_links() {
        use std::os::unix::fs::{symlink, PermissionsExt as _};

        let directory = TempDir::new().unwrap();
        let target = directory.path().join("s3-envelope-target.key");
        let link = directory.path().join("s3-envelope.key");
        std::fs::write(&target, [0x5a; 32]).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&target, &link).unwrap();
        let mut config = development_agent_config();
        config.s3_envelope_key_file = Some(link);
        let error = load_s3_envelope_key(&config).unwrap_err();
        assert!(error.to_string().contains("symbolic link"));
    }

    fn production_agent_config() -> Config {
        Config {
            bind: "127.0.0.1:8080".parse().expect("socket address"),
            agent_enrollment_enabled: true,
            agent_enrollment_keyring_file: Some(PathBuf::from("/tmp/enrollment-keyring.json")),
            gateway_tls_ca_file: Some(PathBuf::from("/tmp/gateway-ca.pem")),
            gateway_tls_client_certificate_file: Some(PathBuf::from("/tmp/central-cert.pem")),
            gateway_tls_client_private_key_file: Some(PathBuf::from("/tmp/central-key.pem")),
            gateway_workload_trust_domain: Some("mesh.example.test".to_owned()),
            authority_dir: PathBuf::from("/tmp/neoengram-authority"),
            rbac_file: Some(PathBuf::from("/tmp/neoengram-rbac.json")),
            s3_envelope_key_file: None,
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
            agent_enrollment_keyring_file: Some(PathBuf::from("/tmp/enrollment-keyring.json")),
            gateway_tls_ca_file: None,
            gateway_tls_client_certificate_file: None,
            gateway_tls_client_private_key_file: None,
            gateway_workload_trust_domain: None,
            authority_dir: PathBuf::from("/tmp/neoengram-authority"),
            rbac_file: None,
            s3_envelope_key_file: None,
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
