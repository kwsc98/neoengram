use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use crate::{
    Clock, GatewayCredentialState, GatewayInsertOutcome, GatewayPoolListRequest, GatewayPoolRecord,
    GatewayPoolState, GatewayRegistryRepository, GatewayReplicaCredential,
    GatewayReplicaListRequest, GatewayReplicaRecord, GatewayReplicaState,
    GATEWAY_ACTIVATION_TOKEN_MAX_TTL_MS, GATEWAY_REGISTRY_MAX_PAGE_SIZE,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    CertificateGeneration, EdgeClusterId, GatewayPoolId, GatewayReplicaId, Generation,
    ProtocolVersion, RequestId, ResourceVersion, TaskActor, TaskId, TaskKind, TaskResourceKind,
    TaskResourceLink, TaskResourceRole, TaskScope, TenantId, UnixMillis,
};
use serde::Serialize;

use crate::{
    dto::{
        ActivateGatewayReplicaRequest, CreateGatewayPoolRequest, CreateGatewayReplicaRequest,
        CreateGatewayReplicaResponse, DrainGatewayPoolRequest, GatewayPoolListResponse,
        GatewayPoolResponse, GatewayPoolView, GatewayReplicaListResponse, GatewayReplicaResponse,
        GatewayReplicaView, MutateGatewayReplicaRequest, QueryGatewayPoolListRequest,
        QueryGatewayPoolRequest, QueryGatewayReplicaListRequest, TaskView,
        UpdateGatewayPoolRequest,
    },
    error::{application_error, invalid_request, map_central_error},
    gateway_activation_transport::{
        GatewayBootstrapTransportError, GatewayReplicaActivationClient,
        GatewayReplicaActivationClientError,
    },
    identity::{AuthenticatedIdentity, Permission, StaticRbacPolicy},
    service::{GatewayReplicaActivationError, TaskCoordinator, WorkloadCertificateIssuerError},
};

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = GATEWAY_REGISTRY_MAX_PAGE_SIZE - 1;
const ACTIVATION_TOKEN_BYTES: usize = 32;
const GATEWAY_TASK_TENANT_ID: &str = "gateway-system";

pub struct GatewayRegistryService {
    repository: Arc<dyn GatewayRegistryRepository>,
    policy: Arc<StaticRbacPolicy>,
    clock: Arc<dyn Clock>,
    activation_client: Option<Arc<GatewayReplicaActivationClient>>,
    task_coordinator: Option<Arc<TaskCoordinator>>,
}

impl GatewayRegistryService {
    #[must_use]
    pub fn new(
        repository: Arc<dyn GatewayRegistryRepository>,
        policy: Arc<StaticRbacPolicy>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            repository,
            policy,
            clock,
            activation_client: None,
            task_coordinator: None,
        }
    }

    /// Attaches the Central-initiated activation transport. Keeping this as an explicit builder
    /// step allows deployments without a configured KMS/HSM issuer to remain fail-closed while
    /// preserving the management route and its authorization boundary.
    #[must_use]
    pub fn with_activation_client(
        mut self,
        activation_client: Arc<GatewayReplicaActivationClient>,
    ) -> Self {
        self.activation_client = Some(activation_client);
        self
    }

    /// Attaches the unified operation-task coordinator.  Gateway registry tests and lightweight
    /// compositions may omit it; the production runtime installs the Authority-backed instance.
    #[must_use]
    pub fn with_task_coordinator(mut self, task_coordinator: Arc<TaskCoordinator>) -> Self {
        self.task_coordinator = Some(task_coordinator);
        self
    }

    pub async fn create_pool(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateGatewayPoolRequest,
    ) -> Result<GatewayPoolResponse, Error> {
        self.authorize_manage(identity)?;
        let create_request = request.clone();
        let gateway_pool_id = parse_pool_id(&request.gateway_pool_id)?;
        let edge_cluster_id = parse_cluster_id(&request.edge_cluster_id)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                identity,
                &create_request,
                Some("gateway_pool"),
                Some(gateway_pool_id.as_str()),
            )
            .await?;
        self.link_operation_resource(&task, gateway_pool_id.as_str(), TaskResourceRole::Primary)
            .await?;
        if let Some(existing) = self
            .repository
            .get_pool(&gateway_pool_id)
            .await
            .map_err(map_central_error)?
        {
            if pool_matches_create(&existing, &request, &edge_cluster_id) {
                return Ok(GatewayPoolResponse {
                    gateway_pool: pool_view(&existing),
                    replayed: true,
                    task: self.complete_operation_task(task, identity).await?,
                });
            }
            return Err(invalid_request(
                "gateway_pool_id already belongs to another GatewayPool definition",
            ));
        }
        let now = self.clock.now();
        let actor = identity.principal().clone();
        let record = GatewayPoolRecord {
            gateway_pool_id: gateway_pool_id.clone(),
            edge_cluster_id: edge_cluster_id.clone(),
            display_name: request.display_name,
            agent_endpoint: request.agent_endpoint,
            s3_endpoint: request.s3_endpoint,
            desired_replicas: request.desired_replicas,
            minimum_ready_replicas: request.minimum_ready_replicas,
            state: GatewayPoolState::Provisioning,
            config_generation: Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            created_by: actor.clone(),
            updated_by: actor,
        };
        let (record, replayed) = match self.repository.insert_pool(record).await {
            Ok(GatewayInsertOutcome::Inserted(record)) => (record, false),
            Ok(GatewayInsertOutcome::Existing(record)) => (record, true),
            Err(error) if error.code() == crate::CentralErrorCode::GatewayIdentityConflict => {
                // Two identical creates can race between the initial read and the durable insert.
                // Re-read the authoritative ID and converge only when the complete definition
                // matches; a conflict on another identity or endpoint remains an error.
                let existing = self
                    .repository
                    .get_pool(&gateway_pool_id)
                    .await
                    .map_err(map_central_error)?
                    .ok_or_else(|| map_central_error(error.clone()))?;
                if pool_matches_create(&existing, &create_request, &edge_cluster_id) {
                    (existing, true)
                } else {
                    return Err(invalid_request(
                        "gateway_pool_id already belongs to another GatewayPool definition",
                    ));
                }
            }
            Err(error) => return Err(map_central_error(error)),
        };
        let task = self.complete_operation_task(task, identity).await?;
        Ok(GatewayPoolResponse {
            gateway_pool: pool_view(&record),
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn query_pool(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryGatewayPoolRequest,
    ) -> Result<GatewayPoolResponse, Error> {
        self.authorize_read(identity)?;
        let record = self
            .repository
            .get_pool(&parse_pool_id(&request.gateway_pool_id)?)
            .await
            .map_err(map_central_error)?
            .ok_or_else(gateway_pool_not_found)?;
        Ok(GatewayPoolResponse {
            gateway_pool: pool_view(&record),
            replayed: false,
            task: None,
        })
    }

    pub async fn list_pools(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryGatewayPoolListRequest,
    ) -> Result<GatewayPoolListResponse, Error> {
        self.authorize_read(identity)?;
        let limit = page_size(request.page_size)?;
        let mut records = self
            .repository
            .list_pools(&GatewayPoolListRequest {
                edge_cluster_id: request
                    .edge_cluster_id
                    .as_deref()
                    .map(parse_cluster_id)
                    .transpose()?,
                state: request.state.as_deref().map(parse_pool_state).transpose()?,
                after: request.after.as_deref().map(parse_pool_id).transpose()?,
                limit: limit + 1,
            })
            .await
            .map_err(map_central_error)?;
        let next_after = (records.len() > limit).then(|| {
            records
                .pop()
                .expect("a Gateway page with an extra item is non-empty");
            records
                .last()
                .expect("a positive Gateway page retains an item")
                .gateway_pool_id
                .to_string()
        });
        Ok(GatewayPoolListResponse {
            items: records.iter().map(pool_view).collect(),
            next_after,
        })
    }

    pub async fn update_pool(
        &self,
        identity: &AuthenticatedIdentity,
        request: UpdateGatewayPoolRequest,
    ) -> Result<GatewayPoolResponse, Error> {
        self.authorize_manage(identity)?;
        let task_request = request.clone();
        if request.clear_s3_endpoint && request.s3_endpoint.is_some() {
            return Err(invalid_request(
                "s3_endpoint and clear_s3_endpoint are mutually exclusive",
            ));
        }
        let id = parse_pool_id(&request.gateway_pool_id)?;
        let expected = parse_resource_version(&request.expected_resource_version)?;
        let mut record = self
            .repository
            .get_pool(&id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(gateway_pool_not_found)?;
        if record.resource_version != expected {
            return Err(map_central_error(crate::CentralError::new(
                crate::CentralErrorCode::ConcurrentUpdate,
                "GatewayPool ResourceVersion changed",
            )));
        }
        if request
            .agent_endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint != &record.agent_endpoint)
        {
            return Err(invalid_request(
                "agent_endpoint is immutable until Gateway workload certificate rotation is supported",
            ));
        }
        if request.clear_s3_endpoint && record.s3_endpoint.is_some()
            || request
                .s3_endpoint
                .as_ref()
                .is_some_and(|endpoint| record.s3_endpoint.as_ref() != Some(endpoint))
        {
            return Err(invalid_request(
                "s3_endpoint is immutable until Gateway workload certificate rotation is supported",
            ));
        }
        let (task, task_replayed) = self
            .begin_operation_task(
                identity,
                &task_request,
                Some("gateway_pool"),
                Some(id.as_str()),
            )
            .await?;
        self.link_operation_resource(&task, id.as_str(), TaskResourceRole::Primary)
            .await?;
        if let Some(value) = request.display_name {
            record.display_name = value;
        }
        if let Some(value) = request.agent_endpoint {
            record.agent_endpoint = value;
        }
        if request.clear_s3_endpoint {
            record.s3_endpoint = None;
        } else if let Some(value) = request.s3_endpoint {
            record.s3_endpoint = Some(value);
        }
        if let Some(value) = request.desired_replicas {
            record.desired_replicas = value;
        }
        if let Some(value) = request.minimum_ready_replicas {
            record.minimum_ready_replicas = value;
        }
        if let Some(value) = request.state.as_deref() {
            let state = parse_pool_state(value)?;
            if state == GatewayPoolState::Draining {
                return Err(invalid_request("use the GatewayPool drain action"));
            }
            record.state = state;
        }
        advance_pool(&mut record, identity, self.clock.now())?;
        let expected = expected.get();
        let record = self
            .repository
            .replace_pool(expected, record)
            .await
            .map_err(map_central_error)?;
        let task = self.complete_operation_task(task, identity).await?;
        Ok(GatewayPoolResponse {
            gateway_pool: pool_view(&record),
            replayed: task_replayed,
            task,
        })
    }

    pub async fn drain_pool(
        &self,
        identity: &AuthenticatedIdentity,
        request: DrainGatewayPoolRequest,
    ) -> Result<GatewayPoolResponse, Error> {
        self.authorize_manage(identity)?;
        let task_request = request.clone();
        let id = parse_pool_id(&request.gateway_pool_id)?;
        let expected = parse_resource_version(&request.expected_resource_version)?;
        let mut record = self
            .repository
            .get_pool(&id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(gateway_pool_not_found)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                identity,
                &task_request,
                Some("gateway_pool"),
                Some(id.as_str()),
            )
            .await?;
        self.link_operation_resource(&task, id.as_str(), TaskResourceRole::Primary)
            .await?;
        if record.state == GatewayPoolState::Draining {
            return Ok(GatewayPoolResponse {
                gateway_pool: pool_view(&record),
                replayed: true,
                task: self.complete_operation_task(task, identity).await?,
            });
        }
        if record.resource_version != expected {
            return Err(map_central_error(crate::CentralError::new(
                crate::CentralErrorCode::ConcurrentUpdate,
                "GatewayPool ResourceVersion changed",
            )));
        }
        record.state = GatewayPoolState::Draining;
        advance_pool(&mut record, identity, self.clock.now())?;
        let record = self
            .repository
            .replace_pool(expected.get(), record)
            .await
            .map_err(map_central_error)?;
        let task = self.complete_operation_task(task, identity).await?;
        Ok(GatewayPoolResponse {
            gateway_pool: pool_view(&record),
            replayed: task_replayed,
            task,
        })
    }

    pub async fn create_replica(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateGatewayReplicaRequest,
    ) -> Result<CreateGatewayReplicaResponse, Error> {
        self.authorize_manage(identity)?;
        let create_request = request.clone();
        let replica_id = parse_replica_id(&request.gateway_replica_id)?;
        let pool_id = parse_pool_id(&request.gateway_pool_id)?;
        let pool = self
            .repository
            .get_pool(&pool_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(gateway_pool_not_found)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                identity,
                &create_request,
                Some("gateway_replica"),
                Some(replica_id.as_str()),
            )
            .await?;
        self.link_operation_resource(&task, replica_id.as_str(), TaskResourceRole::Primary)
            .await?;
        if let Some(existing) = self
            .repository
            .get_replica(&replica_id)
            .await
            .map_err(map_central_error)?
        {
            if replica_matches_create(&existing, &request, &pool.edge_cluster_id) {
                return Ok(CreateGatewayReplicaResponse {
                    gateway_replica: replica_view(&existing),
                    activation_token: None,
                    replayed: true,
                    task: self.complete_operation_task(task, identity).await?,
                });
            }
            return Err(invalid_request(
                "gateway_replica_id already belongs to another GatewayReplica definition",
            ));
        }
        let wire_version = ProtocolVersion::new(request.wire_version);
        let capabilities = request
            .capabilities
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if wire_version != neoengram_domain::protocol::CURRENT_WIRE_VERSION {
            return Err(invalid_request(
                "GatewayReplica must advertise exactly the current wire version",
            ));
        }
        if capabilities.len() != request.capabilities.len() {
            return Err(invalid_request(
                "GatewayReplica capabilities must be unique",
            ));
        }
        let token = activation_token()?;
        let now = self.clock.now();
        let expires_at = now
            .get()
            .checked_add(GATEWAY_ACTIVATION_TOKEN_MAX_TTL_MS)
            .ok_or_else(|| invalid_request("Gateway activation token expiry overflow"))?;
        let record = GatewayReplicaRecord {
            gateway_replica_id: replica_id.clone(),
            gateway_pool_id: pool_id,
            edge_cluster_id: pool.edge_cluster_id.clone(),
            control_endpoint: request.control_endpoint,
            peer_endpoint: request.peer_endpoint,
            bootstrap_endpoint: request.bootstrap_endpoint,
            software_version: request.software_version,
            wire_version,
            capabilities,
            last_heartbeat_at_unix_ms: None,
            state: GatewayReplicaState::Pending,
            credential: GatewayReplicaCredential {
                activation_token_digest: ContentDigest::hash(token.as_bytes()),
                activation_created_at_unix_ms: now,
                activation_expires_at_unix_ms: UnixMillis::new(expires_at),
                activation_consumed_at_unix_ms: None,
                public_key_fingerprint: None,
                certificate_generation: None,
                certificate_fingerprint: None,
                certificate_not_after_unix_ms: None,
                certificate: None,
                state: GatewayCredentialState::PendingActivation,
            },
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let (record, activation_token, replayed) =
            match self.repository.insert_replica(record).await {
                Ok(GatewayInsertOutcome::Inserted(record)) => (record, Some(token), false),
                Ok(GatewayInsertOutcome::Existing(record)) => (record, None, true),
                Err(error) if error.code() == crate::CentralErrorCode::GatewayIdentityConflict => {
                    let existing = self
                        .repository
                        .get_replica(&replica_id)
                        .await
                        .map_err(map_central_error)?
                        .ok_or_else(|| map_central_error(error.clone()))?;
                    if replica_matches_create(&existing, &create_request, &pool.edge_cluster_id) {
                        (existing, None, true)
                    } else {
                        return Err(invalid_request(
                        "gateway_replica_id already belongs to another GatewayReplica definition",
                    ));
                    }
                }
                Err(error) => return Err(map_central_error(error)),
            };
        let task = self.complete_operation_task(task, identity).await?;
        Ok(CreateGatewayReplicaResponse {
            gateway_replica: replica_view(&record),
            activation_token,
            replayed: replayed || task_replayed,
            task,
        })
    }

    pub async fn list_replicas(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryGatewayReplicaListRequest,
    ) -> Result<GatewayReplicaListResponse, Error> {
        self.authorize_read(identity)?;
        let limit = page_size(request.page_size)?;
        let mut records = self
            .repository
            .list_replicas(&GatewayReplicaListRequest {
                gateway_pool_id: parse_pool_id(&request.gateway_pool_id)?,
                state: request
                    .state
                    .as_deref()
                    .map(parse_replica_state)
                    .transpose()?,
                after: request.after.as_deref().map(parse_replica_id).transpose()?,
                limit: limit + 1,
            })
            .await
            .map_err(map_central_error)?;
        let next_after = (records.len() > limit).then(|| {
            records
                .pop()
                .expect("a Gateway page with an extra item is non-empty");
            records
                .last()
                .expect("a positive Gateway page retains an item")
                .gateway_replica_id
                .to_string()
        });
        Ok(GatewayReplicaListResponse {
            items: records.iter().map(replica_view).collect(),
            next_after,
        })
    }

    /// Activates a Replica through the bootstrap endpoint persisted in the authoritative
    /// Registry. The caller-supplied endpoint is intentionally not part of the DTO, preventing
    /// an operator or compromised client from redirecting Central's credential delivery.
    pub async fn activate_replica(
        &self,
        identity: &AuthenticatedIdentity,
        request: ActivateGatewayReplicaRequest,
    ) -> Result<GatewayReplicaResponse, Error> {
        self.authorize_manage(identity)?;
        let replica_id = parse_replica_id(&request.gateway_replica_id)?;
        let expected = parse_resource_version(&request.expected_resource_version)?;
        let record = self
            .repository
            .get_replica(&replica_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(gateway_replica_not_found)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                identity,
                &request,
                Some("gateway_replica"),
                Some(replica_id.as_str()),
            )
            .await?;
        self.link_operation_resource(&task, replica_id.as_str(), TaskResourceRole::Primary)
            .await?;
        let token_matches = ContentDigest::hash(request.activation_token.as_bytes())
            == record.credential.activation_token_digest;
        // Activation has a durable prepare/deliver/commit boundary.  A lost response must be
        // recoverable with the original token even though prepare advanced ResourceVersion.  An
        // already-active Replica is also an idempotent success, but only for the exact token
        // digest; a stale caller cannot use activation as an identity oracle.
        if token_matches
            && record.state == GatewayReplicaState::Active
            && record.credential.state == GatewayCredentialState::Active
        {
            return Ok(GatewayReplicaResponse {
                gateway_replica: replica_view(&record),
                replayed: true,
                task: self.complete_operation_task(task, identity).await?,
            });
        }
        let resumable = token_matches
            && record.state == GatewayReplicaState::Pending
            && record.credential.state == GatewayCredentialState::PendingCertificateDelivery;
        if !resumable && record.resource_version != expected {
            return Err(map_central_error(crate::CentralError::new(
                crate::CentralErrorCode::ConcurrentUpdate,
                "GatewayReplica ResourceVersion changed",
            )));
        }
        let activation_client = self.activation_client.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "gateway_activation_unavailable",
                "GATEWAY_ACTIVATION_UNAVAILABLE",
                "Gateway Replica activation is not configured",
                true,
            )
        })?;
        let activated = activation_client
            .activate(&record, &request.activation_token)
            .await
            .map_err(map_gateway_activation_error)?;
        let task = self.complete_operation_task(task, identity).await?;
        Ok(GatewayReplicaResponse {
            gateway_replica: replica_view(&activated.replica),
            replayed: resumable || task_replayed,
            task,
        })
    }

    pub async fn drain_replica(
        &self,
        identity: &AuthenticatedIdentity,
        request: MutateGatewayReplicaRequest,
    ) -> Result<GatewayReplicaResponse, Error> {
        self.mutate_replica(identity, request, GatewayReplicaMutation::Drain)
            .await
    }

    pub async fn revoke_replica(
        &self,
        identity: &AuthenticatedIdentity,
        request: MutateGatewayReplicaRequest,
    ) -> Result<GatewayReplicaResponse, Error> {
        self.mutate_replica(identity, request, GatewayReplicaMutation::Revoke)
            .await
    }

    async fn mutate_replica(
        &self,
        identity: &AuthenticatedIdentity,
        request: MutateGatewayReplicaRequest,
        mutation: GatewayReplicaMutation,
    ) -> Result<GatewayReplicaResponse, Error> {
        self.authorize_manage(identity)?;
        let task_request = (
            match mutation {
                GatewayReplicaMutation::Drain => "drain",
                GatewayReplicaMutation::Revoke => "revoke",
            },
            request.clone(),
        );
        let expected = parse_resource_version(&request.expected_resource_version)?;
        let replica_id = parse_replica_id(&request.gateway_replica_id)?;
        let mut record = self
            .repository
            .get_replica(&replica_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(gateway_replica_not_found)?;
        let (task, task_replayed) = self
            .begin_operation_task(
                identity,
                &task_request,
                Some("gateway_replica"),
                Some(replica_id.as_str()),
            )
            .await?;
        self.link_operation_resource(&task, replica_id.as_str(), TaskResourceRole::Primary)
            .await?;
        let replayed = match mutation {
            GatewayReplicaMutation::Drain if record.state == GatewayReplicaState::Draining => true,
            GatewayReplicaMutation::Revoke if record.state == GatewayReplicaState::Revoked => true,
            GatewayReplicaMutation::Drain => {
                if record.state != GatewayReplicaState::Active {
                    return Err(invalid_request("only an Active GatewayReplica can drain"));
                }
                record.state = GatewayReplicaState::Draining;
                false
            }
            GatewayReplicaMutation::Revoke => {
                record.state = GatewayReplicaState::Revoked;
                record.credential.state = GatewayCredentialState::Revoked;
                if let Some(generation) = record.credential.certificate_generation {
                    record.credential.certificate_generation = Some(CertificateGeneration::new(
                        generation
                            .get()
                            .checked_add(1)
                            .ok_or_else(|| invalid_request("certificate generation overflow"))?,
                    ));
                }
                false
            }
        };
        if replayed {
            return Ok(GatewayReplicaResponse {
                gateway_replica: replica_view(&record),
                replayed: true,
                task: self.complete_operation_task(task, identity).await?,
            });
        }
        if record.resource_version != expected {
            return Err(map_central_error(crate::CentralError::new(
                crate::CentralErrorCode::ConcurrentUpdate,
                "GatewayReplica ResourceVersion changed",
            )));
        }
        let expected_value = expected.get();
        record.resource_version = ResourceVersion::new(
            expected_value
                .checked_add(1)
                .ok_or_else(|| invalid_request("resource version overflow"))?,
        );
        record.updated_at_unix_ms = self.clock.now();
        let record = self
            .repository
            .replace_replica(expected_value, record)
            .await
            .map_err(map_central_error)?;
        let task = self.complete_operation_task(task, identity).await?;
        Ok(GatewayReplicaResponse {
            gateway_replica: replica_view(&record),
            replayed: task_replayed,
            task,
        })
    }

    /// Creates the unified task envelope for a Gateway infrastructure mutation. Gateway registry
    /// requests are control-plane-wide and therefore use the fixed system tenant scope; the
    /// concrete pool or replica is attached through a typed resource link below.
    async fn begin_operation_task<T: Serialize>(
        &self,
        identity: &AuthenticatedIdentity,
        request: &T,
        detail_kind: Option<&str>,
        detail_id: Option<&str>,
    ) -> Result<(Option<TaskView>, bool), Error> {
        let Some(coordinator) = &self.task_coordinator else {
            return Ok((None, false));
        };
        let kind = TaskKind::GatewayLifecycle;
        let task_request_id = derived_task_request_id(kind, request)?;
        let (task, replayed) = coordinator
            .create_root(
                kind,
                TaskScope::new(gateway_task_tenant_id()),
                task_request_id,
                request,
                TaskActor::Principal(identity.principal().clone()),
                detail_kind,
                detail_id,
            )
            .await
            .map_err(map_central_error)?;
        Ok((Some(super::task::task_view(&task)), replayed))
    }

    async fn complete_operation_task(
        &self,
        task: Option<TaskView>,
        identity: &AuthenticatedIdentity,
    ) -> Result<Option<TaskView>, Error> {
        let (Some(coordinator), Some(task)) = (&self.task_coordinator, task) else {
            return Ok(None);
        };
        let task_id = TaskId::new(task.task_id.clone())
            .map_err(|error| invalid_request(format!("task_id: {error}")))?;
        let tenant_id = TenantId::new(task.tenant_id.clone())
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        let current = coordinator
            .repository()
            .get(&tenant_id, &task_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| invalid_request("operation task disappeared"))?;
        let completed = coordinator
            .complete_immediate(&current, TaskActor::Principal(identity.principal().clone()))
            .await
            .map_err(map_central_error)?;
        Ok(Some(super::task::task_view(&completed)))
    }

    async fn link_operation_resource(
        &self,
        task: &Option<TaskView>,
        resource_id: &str,
        role: TaskResourceRole,
    ) -> Result<(), Error> {
        let (Some(coordinator), Some(task)) = (&self.task_coordinator, task) else {
            return Ok(());
        };
        let task_id = TaskId::new(task.task_id.clone())
            .map_err(|error| invalid_request(format!("task_id: {error}")))?;
        let tenant_id = TenantId::new(task.tenant_id.clone())
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        coordinator
            .repository()
            .link_resource(crate::TaskResourceLinkRecord {
                tenant_id,
                link: TaskResourceLink::new(
                    task_id,
                    TaskResourceKind::Gateway,
                    resource_id.to_owned(),
                    role,
                ),
            })
            .await
            .map_err(map_central_error)?;
        Ok(())
    }

    fn authorize_read(&self, identity: &AuthenticatedIdentity) -> Result<(), Error> {
        self.policy
            .authorize_global_identity(identity, Permission::GatewayRead)
    }

    fn authorize_manage(&self, identity: &AuthenticatedIdentity) -> Result<(), Error> {
        self.policy
            .authorize_global_identity(identity, Permission::GatewayManage)
    }
}

fn map_gateway_activation_error(error: GatewayReplicaActivationClientError) -> Error {
    match error {
        GatewayReplicaActivationClientError::Endpoint(message)
        | GatewayReplicaActivationClientError::Transport(
            GatewayBootstrapTransportError::Endpoint(message),
        ) => invalid_request(message),
        GatewayReplicaActivationClientError::Transport(
            GatewayBootstrapTransportError::InsecureEndpoint,
        ) => invalid_request("Gateway bootstrap endpoint must use HTTPS"),
        GatewayReplicaActivationClientError::Transport(
            GatewayBootstrapTransportError::ContentType
            | GatewayBootstrapTransportError::Decode(_)
            | GatewayBootstrapTransportError::UnsupportedVersion,
        ) => invalid_request("Gateway Replica bootstrap response is invalid"),
        GatewayReplicaActivationClientError::Activation(
            GatewayReplicaActivationError::Registry(error),
        ) => map_central_error(error),
        GatewayReplicaActivationClientError::Activation(
            GatewayReplicaActivationError::ConcurrentActivation,
        ) => map_central_error(crate::CentralError::new(
            crate::CentralErrorCode::ConcurrentUpdate,
            "Gateway Replica activation was updated concurrently",
        )),
        GatewayReplicaActivationClientError::Activation(
            GatewayReplicaActivationError::CredentialRejected
            | GatewayReplicaActivationError::InvalidChallenge
            | GatewayReplicaActivationError::ChallengeExpired
            | GatewayReplicaActivationError::ProofInvalid,
        ) => application_error(
            ErrorCategory::Conflict,
            "gateway_activation_rejected",
            "GATEWAY_ACTIVATION_REJECTED",
            "Gateway Replica activation credential was rejected",
            false,
        ),
        GatewayReplicaActivationClientError::Activation(
            GatewayReplicaActivationError::CertificateIssuer(
                WorkloadCertificateIssuerError::Rejected(_),
            ),
        ) => application_error(
            ErrorCategory::Conflict,
            "gateway_certificate_rejected",
            "GATEWAY_CERTIFICATE_REJECTED",
            "Gateway workload certificate request was rejected",
            false,
        ),
        GatewayReplicaActivationClientError::Activation(
            GatewayReplicaActivationError::CertificateIssuer(
                WorkloadCertificateIssuerError::Unavailable(_)
                | WorkloadCertificateIssuerError::Internal(_),
            ),
        )
        | GatewayReplicaActivationClientError::Transport(
            GatewayBootstrapTransportError::Request(_)
            | GatewayBootstrapTransportError::Status(_)
            | GatewayBootstrapTransportError::ResponseTooLarge
            | GatewayBootstrapTransportError::Delivery(_),
        ) => application_error(
            ErrorCategory::Unavailable,
            "gateway_activation_unavailable",
            "GATEWAY_ACTIVATION_UNAVAILABLE",
            "Gateway Replica activation is temporarily unavailable",
            true,
        ),
        GatewayReplicaActivationClientError::Activation(
            GatewayReplicaActivationError::Configuration(_)
            | GatewayReplicaActivationError::Randomness
            | GatewayReplicaActivationError::ResourceVersionOverflow
            | GatewayReplicaActivationError::CertificateMaterialInvalid
            | GatewayReplicaActivationError::CertificateIssuer(
                WorkloadCertificateIssuerError::Invalid(_),
            ),
        )
        | GatewayReplicaActivationClientError::Transport(
            GatewayBootstrapTransportError::ClientConfiguration(_),
        ) => application_error(
            ErrorCategory::Internal,
            "gateway_activation_failed",
            "GATEWAY_ACTIVATION_FAILED",
            "Gateway Replica activation failed",
            false,
        ),
    }
}

#[derive(Clone, Copy)]
enum GatewayReplicaMutation {
    Drain,
    Revoke,
}

fn advance_pool(
    record: &mut GatewayPoolRecord,
    identity: &AuthenticatedIdentity,
    now: UnixMillis,
) -> Result<(), Error> {
    record.config_generation = Generation::new(
        record
            .config_generation
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid_request("GatewayPool config generation overflow"))?,
    );
    record.resource_version = ResourceVersion::new(
        record
            .resource_version
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid_request("GatewayPool resource version overflow"))?,
    );
    record.updated_at_unix_ms = now;
    record.updated_by = identity.principal().clone();
    Ok(())
}

fn pool_matches_create(
    record: &GatewayPoolRecord,
    request: &CreateGatewayPoolRequest,
    cluster: &EdgeClusterId,
) -> bool {
    record.edge_cluster_id == *cluster
        && record.display_name == request.display_name
        && record.agent_endpoint == request.agent_endpoint
        && record.s3_endpoint == request.s3_endpoint
        && record.desired_replicas == request.desired_replicas
        && record.minimum_ready_replicas == request.minimum_ready_replicas
}

fn replica_matches_create(
    record: &GatewayReplicaRecord,
    request: &CreateGatewayReplicaRequest,
    cluster: &EdgeClusterId,
) -> bool {
    record.gateway_pool_id.as_str() == request.gateway_pool_id
        && record.edge_cluster_id == *cluster
        && record.control_endpoint == request.control_endpoint
        && record.peer_endpoint == request.peer_endpoint
        && record.bootstrap_endpoint == request.bootstrap_endpoint
        && record.software_version == request.software_version
        && record.wire_version == ProtocolVersion::new(request.wire_version)
        && record.capabilities == request.capabilities.iter().cloned().collect()
}

fn pool_view(record: &GatewayPoolRecord) -> GatewayPoolView {
    GatewayPoolView {
        gateway_pool_id: record.gateway_pool_id.to_string(),
        edge_cluster_id: record.edge_cluster_id.to_string(),
        display_name: record.display_name.clone(),
        agent_endpoint: record.agent_endpoint.clone(),
        s3_endpoint: record.s3_endpoint.clone(),
        desired_replicas: record.desired_replicas,
        minimum_ready_replicas: record.minimum_ready_replicas,
        state: pool_state_name(record.state).to_owned(),
        config_generation: record.config_generation.to_string(),
        resource_version: record.resource_version.to_string(),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn replica_view(record: &GatewayReplicaRecord) -> GatewayReplicaView {
    GatewayReplicaView {
        gateway_replica_id: record.gateway_replica_id.to_string(),
        gateway_pool_id: record.gateway_pool_id.to_string(),
        edge_cluster_id: record.edge_cluster_id.to_string(),
        control_endpoint: record.control_endpoint.clone(),
        peer_endpoint: record.peer_endpoint.clone(),
        bootstrap_endpoint: record.bootstrap_endpoint.clone(),
        software_version: record.software_version.clone(),
        wire_version: record.wire_version.get(),
        capabilities: record.capabilities.iter().cloned().collect(),
        last_heartbeat_at_unix_ms: record
            .last_heartbeat_at_unix_ms
            .map(|value| value.to_string()),
        state: replica_state_name(record.state).to_owned(),
        credential_state: credential_state_name(record.credential.state).to_owned(),
        certificate_generation: record
            .credential
            .certificate_generation
            .map(|value| value.to_string()),
        certificate_not_after_unix_ms: record
            .credential
            .certificate_not_after_unix_ms
            .map(|value| value.to_string()),
        resource_version: record.resource_version.to_string(),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

fn activation_token() -> Result<String, Error> {
    let mut random = [0_u8; ACTIVATION_TOKEN_BYTES];
    getrandom::fill(&mut random)
        .map_err(|_| invalid_request("secure activation token generation failed"))?;
    Ok(format!("nggw_v1_{}", URL_SAFE_NO_PAD.encode(random)))
}

fn gateway_task_tenant_id() -> TenantId {
    TenantId::new(GATEWAY_TASK_TENANT_ID).expect("gateway task system tenant ID is valid")
}

fn derived_task_request_id<T: Serialize>(kind: TaskKind, request: &T) -> Result<RequestId, Error> {
    let digest = neoengram_domain::jcs_blake3(request)
        .map_err(|error| invalid_request(format!("task request: {error}")))?;
    RequestId::new(format!(
        "{}-{}",
        kind.as_str().replace('.', "-"),
        &digest.to_hex()[..32]
    ))
    .map_err(|error| invalid_request(format!("task request_id: {error}")))
}

fn parse_pool_id(value: &str) -> Result<GatewayPoolId, Error> {
    GatewayPoolId::new(value).map_err(|error| invalid_request(format!("gateway_pool_id: {error}")))
}

fn parse_replica_id(value: &str) -> Result<GatewayReplicaId, Error> {
    GatewayReplicaId::new(value)
        .map_err(|error| invalid_request(format!("gateway_replica_id: {error}")))
}

fn parse_cluster_id(value: &str) -> Result<EdgeClusterId, Error> {
    EdgeClusterId::new(value).map_err(|error| invalid_request(format!("edge_cluster_id: {error}")))
}

fn parse_resource_version(value: &str) -> Result<ResourceVersion, Error> {
    ResourceVersion::from_str(value)
        .map_err(|error| invalid_request(format!("expected_resource_version: {error}")))
}

fn page_size(value: Option<u16>) -> Result<usize, Error> {
    let value = usize::from(value.unwrap_or(DEFAULT_PAGE_SIZE as u16));
    if value == 0 || value > MAX_PAGE_SIZE {
        return Err(invalid_request(format!(
            "page_size must be in 1..={MAX_PAGE_SIZE}"
        )));
    }
    Ok(value)
}

fn parse_pool_state(value: &str) -> Result<GatewayPoolState, Error> {
    match value {
        "provisioning" => Ok(GatewayPoolState::Provisioning),
        "ready" => Ok(GatewayPoolState::Ready),
        "draining" => Ok(GatewayPoolState::Draining),
        "disabled" => Ok(GatewayPoolState::Disabled),
        _ => Err(invalid_request("unsupported GatewayPool state")),
    }
}

fn parse_replica_state(value: &str) -> Result<GatewayReplicaState, Error> {
    match value {
        "pending" => Ok(GatewayReplicaState::Pending),
        "active" => Ok(GatewayReplicaState::Active),
        "draining" => Ok(GatewayReplicaState::Draining),
        "revoked" => Ok(GatewayReplicaState::Revoked),
        _ => Err(invalid_request("unsupported GatewayReplica state")),
    }
}

const fn pool_state_name(value: GatewayPoolState) -> &'static str {
    match value {
        GatewayPoolState::Provisioning => "provisioning",
        GatewayPoolState::Ready => "ready",
        GatewayPoolState::Draining => "draining",
        GatewayPoolState::Disabled => "disabled",
    }
}

const fn replica_state_name(value: GatewayReplicaState) -> &'static str {
    match value {
        GatewayReplicaState::Pending => "pending",
        GatewayReplicaState::Active => "active",
        GatewayReplicaState::Draining => "draining",
        GatewayReplicaState::Revoked => "revoked",
    }
}

const fn credential_state_name(value: GatewayCredentialState) -> &'static str {
    match value {
        GatewayCredentialState::PendingActivation => "pending_activation",
        GatewayCredentialState::PendingCertificateDelivery => "pending_certificate_delivery",
        GatewayCredentialState::Active => "active",
        GatewayCredentialState::Expired => "expired",
        GatewayCredentialState::Revoked => "revoked",
    }
}

fn gateway_pool_not_found() -> Error {
    map_central_error(crate::CentralError::new(
        crate::CentralErrorCode::GatewayPoolNotFound,
        "GatewayPool does not exist",
    ))
}

fn gateway_replica_not_found() -> Error {
    map_central_error(crate::CentralError::new(
        crate::CentralErrorCode::GatewayReplicaNotFound,
        "GatewayReplica does not exist",
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::{InMemoryClock, InMemoryGatewayRegistry, InMemoryTaskRepository, TaskRepository};
    use async_trait::async_trait;
    use neoengram_domain::protocol::{
        Ed25519PublicKeySpki, Ed25519Signature, PrincipalKind, CURRENT_WIRE_VERSION,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tokio::sync::Mutex as AsyncMutex;

    use crate::gateway_activation_transport::GatewayBootstrapTransport;
    use crate::service::{
        GatewayReplicaActivationChallenge, GatewayReplicaActivationProof,
        GatewayReplicaActivationService, IssuedWorkloadCertificate, WorkloadCertificateIssuer,
        WorkloadCertificateIssuerError, WorkloadCertificateRequest,
    };

    use super::*;

    #[tokio::test]
    async fn replica_activation_token_is_returned_once_and_only_its_digest_is_persisted() {
        let (service, repository, identity) = fixture([Permission::GatewayManage]);
        service
            .create_pool(&identity, create_pool_request())
            .await
            .unwrap();
        let request = create_replica_request();
        let created = service
            .create_replica(&identity, request.clone())
            .await
            .unwrap();
        let token = created
            .activation_token
            .expect("first create returns token");
        assert!(token.starts_with("nggw_v1_"));

        let replay = service.create_replica(&identity, request).await.unwrap();
        assert!(replay.replayed);
        assert!(replay.activation_token.is_none());
        let stored = repository
            .get_replica_by_activation_token_digest(&ContentDigest::hash(token.as_bytes()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.gateway_replica_id.as_str(), "replica-a");
        assert!(!format!("{stored:?}").contains(&token));
    }

    #[tokio::test]
    async fn concurrent_pool_and_replica_creates_converge_idempotently() {
        let (service, _repository, identity) = fixture([Permission::GatewayManage]);
        let pool_request = create_pool_request();
        let (first_pool, second_pool) = tokio::join!(
            service.create_pool(&identity, pool_request.clone()),
            service.create_pool(&identity, pool_request),
        );
        let pools = [first_pool.unwrap(), second_pool.unwrap()];
        assert_eq!(
            pools.iter().filter(|response| !response.replayed).count(),
            1
        );
        assert_eq!(pools.iter().filter(|response| response.replayed).count(), 1);

        let replica_request = create_replica_request();
        let (first_replica, second_replica) = tokio::join!(
            service.create_replica(&identity, replica_request.clone()),
            service.create_replica(&identity, replica_request),
        );
        let replicas = [first_replica.unwrap(), second_replica.unwrap()];
        assert_eq!(
            replicas
                .iter()
                .filter(|response| !response.replayed)
                .count(),
            1
        );
        assert_eq!(
            replicas.iter().filter(|response| response.replayed).count(),
            1
        );
        assert_eq!(
            replicas
                .iter()
                .filter(|response| response.activation_token.is_some())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn gateway_mutations_are_recorded_as_completed_operation_tasks() {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        let tasks = Arc::new(InMemoryTaskRepository::default());
        let clock = Arc::new(InMemoryClock::new(100));
        let policy = Arc::new(
            StaticRbacPolicy::one_principal(
                "operator",
                ["*".to_owned()],
                [Permission::GatewayManage],
            )
            .unwrap(),
        );
        let coordinator = Arc::new(TaskCoordinator::new(tasks.clone(), clock.clone()));
        let service = GatewayRegistryService::new(repository, policy, clock)
            .with_task_coordinator(coordinator);
        let identity = identity();

        let response = service
            .create_pool(&identity, create_pool_request())
            .await
            .unwrap();
        let task = response.task.expect("gateway create returns a task");
        assert_eq!(task.task_kind, "gateway.lifecycle");
        assert_eq!(task.state, "succeeded");
        let task_id = TaskId::new(task.task_id).unwrap();
        let tenant_id = TenantId::new(GATEWAY_TASK_TENANT_ID).unwrap();
        let links = tasks.resources(&tenant_id, &task_id).await.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].resource_kind, TaskResourceKind::Gateway);
        assert_eq!(links[0].resource_id, "pool-a");
    }

    #[tokio::test]
    async fn global_gateway_permission_requires_a_wildcard_scope() {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        let policy = Arc::new(
            StaticRbacPolicy::one_principal(
                "operator",
                ["tenant-a".to_owned()],
                [Permission::GatewayManage],
            )
            .unwrap(),
        );
        let service =
            GatewayRegistryService::new(repository, policy, Arc::new(InMemoryClock::new(100)));
        assert!(service
            .create_pool(&identity(), create_pool_request())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn pool_and_replica_mutations_use_resource_version_fences() {
        let (service, _repository, identity) =
            fixture([Permission::GatewayRead, Permission::GatewayManage]);
        let pool = service
            .create_pool(&identity, create_pool_request())
            .await
            .unwrap();
        let mut endpoint_change = UpdateGatewayPoolRequest {
            gateway_pool_id: "pool-a".to_owned(),
            expected_resource_version: pool.gateway_pool.resource_version.clone(),
            display_name: None,
            agent_endpoint: Some("https://rotated.gateway.example".to_owned()),
            s3_endpoint: None,
            clear_s3_endpoint: false,
            desired_replicas: None,
            minimum_ready_replicas: None,
            state: None,
        };
        assert!(service
            .update_pool(&identity, endpoint_change.clone())
            .await
            .is_err());
        endpoint_change.agent_endpoint = None;
        endpoint_change.s3_endpoint = Some("https://gateway-s3.example".to_owned());
        assert!(service
            .update_pool(&identity, endpoint_change)
            .await
            .is_err());
        let updated = service
            .update_pool(
                &identity,
                UpdateGatewayPoolRequest {
                    gateway_pool_id: "pool-a".to_owned(),
                    expected_resource_version: pool.gateway_pool.resource_version,
                    display_name: None,
                    agent_endpoint: None,
                    s3_endpoint: None,
                    clear_s3_endpoint: false,
                    desired_replicas: None,
                    minimum_ready_replicas: None,
                    state: Some("ready".to_owned()),
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.gateway_pool.state, "ready");
        assert!(service
            .drain_pool(
                &identity,
                DrainGatewayPoolRequest {
                    gateway_pool_id: "pool-a".to_owned(),
                    expected_resource_version: "1".to_owned(),
                },
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn pool_drain_retry_with_original_resource_version_is_replayed() {
        let (service, _repository, identity) = fixture([Permission::GatewayManage]);
        let pool = service
            .create_pool(&identity, create_pool_request())
            .await
            .unwrap();
        let request = DrainGatewayPoolRequest {
            gateway_pool_id: pool.gateway_pool.gateway_pool_id.clone(),
            expected_resource_version: pool.gateway_pool.resource_version,
        };

        let first = service
            .drain_pool(&identity, request.clone())
            .await
            .unwrap();
        assert!(!first.replayed);
        assert_eq!(first.gateway_pool.state, "draining");

        let replay = service.drain_pool(&identity, request).await.unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.gateway_pool, first.gateway_pool);
    }

    #[tokio::test]
    async fn replica_drain_and_revoke_retries_with_original_resource_versions_are_replayed() {
        let (service, _repository, identity, _activation_request, activated) =
            activated_replica_fixture().await;

        let drain_request = MutateGatewayReplicaRequest {
            gateway_replica_id: activated.gateway_replica.gateway_replica_id.clone(),
            expected_resource_version: activated.gateway_replica.resource_version,
        };
        let drained = service
            .drain_replica(&identity, drain_request.clone())
            .await
            .unwrap();
        assert!(!drained.replayed);
        assert_eq!(drained.gateway_replica.state, "draining");

        let drain_replay = service
            .drain_replica(&identity, drain_request)
            .await
            .unwrap();
        assert!(drain_replay.replayed);
        assert_eq!(drain_replay.gateway_replica, drained.gateway_replica);

        let revoke_request = MutateGatewayReplicaRequest {
            gateway_replica_id: drained.gateway_replica.gateway_replica_id.clone(),
            expected_resource_version: drained.gateway_replica.resource_version,
        };
        let revoked = service
            .revoke_replica(&identity, revoke_request.clone())
            .await
            .unwrap();
        assert!(!revoked.replayed);
        assert_eq!(revoked.gateway_replica.state, "revoked");
        assert_eq!(revoked.gateway_replica.credential_state, "revoked");

        let revoke_replay = service
            .revoke_replica(&identity, revoke_request)
            .await
            .unwrap();
        assert!(revoke_replay.replayed);
        assert_eq!(revoke_replay.gateway_replica, revoked.gateway_replica);
    }

    #[tokio::test]
    async fn activation_success_retry_requires_the_original_token() {
        let (service, _repository, identity, request, activated) =
            activated_replica_fixture().await;
        assert!(!activated.replayed);
        assert_eq!(activated.gateway_replica.state, "active");

        let replay = service
            .activate_replica(&identity, request.clone())
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.gateway_replica, activated.gateway_replica);

        let rejected = service
            .activate_replica(
                &identity,
                ActivateGatewayReplicaRequest {
                    activation_token: "nggw_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                    ..request
                },
            )
            .await
            .unwrap_err();
        assert_eq!(rejected.category(), ErrorCategory::Conflict);
        assert_eq!(rejected.code().as_str(), "concurrent_update");
    }

    #[tokio::test]
    async fn activation_is_gateway_manage_only_and_uses_persisted_bootstrap_endpoint() {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        let policy = Arc::new(
            StaticRbacPolicy::one_principal(
                "operator",
                ["*".to_owned()],
                [Permission::GatewayManage],
            )
            .unwrap(),
        );
        let clock = Arc::new(InMemoryClock::new(100));
        let registry_service =
            GatewayRegistryService::new(repository.clone(), policy, clock.clone());
        let identity = identity();
        registry_service
            .create_pool(&identity, create_pool_request())
            .await
            .unwrap();
        let mut replica_request = create_replica_request();
        replica_request.bootstrap_endpoint = "https://authoritative.bootstrap.example".into();
        let created = registry_service
            .create_replica(&identity, replica_request)
            .await
            .unwrap();
        let token = created.activation_token.clone().unwrap();

        let activation_service = Arc::new(GatewayReplicaActivationService::new(
            repository,
            Arc::new(TestIssuer),
            clock,
            "mesh.example.test",
        ));
        let transport = Arc::new(RecordingBootstrapTransport::default());
        let client = Arc::new(GatewayReplicaActivationClient::new(
            activation_service,
            transport.clone(),
        ));
        let registry_service = registry_service.with_activation_client(client);
        let response = registry_service
            .activate_replica(
                &identity,
                ActivateGatewayReplicaRequest {
                    gateway_replica_id: "replica-a".into(),
                    expected_resource_version: created.gateway_replica.resource_version,
                    activation_token: token,
                },
            )
            .await
            .unwrap();
        assert_eq!(response.gateway_replica.state, "active");
        assert_eq!(
            transport.prove_endpoint.lock().unwrap().as_deref(),
            Some("https://authoritative.bootstrap.example")
        );
        assert_eq!(
            transport.deliver_endpoint.lock().await.as_deref(),
            Some("https://authoritative.bootstrap.example")
        );

        let read_only = StaticRbacPolicy::one_principal(
            "operator",
            ["*".to_owned()],
            [Permission::GatewayRead],
        )
        .unwrap();
        let unauthorized_service = GatewayRegistryService::new(
            Arc::new(InMemoryGatewayRegistry::new()),
            Arc::new(read_only),
            Arc::new(InMemoryClock::new(100)),
        );
        let denied = unauthorized_service
            .activate_replica(
                &identity,
                ActivateGatewayReplicaRequest {
                    gateway_replica_id: "replica-a".into(),
                    expected_resource_version: "1".into(),
                    activation_token: "nggw_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                },
            )
            .await;
        assert!(denied.is_err());
    }

    struct TestIssuer;

    #[async_trait]
    impl WorkloadCertificateIssuer for TestIssuer {
        async fn issue(
            &self,
            request: WorkloadCertificateRequest,
        ) -> Result<IssuedWorkloadCertificate, WorkloadCertificateIssuerError> {
            Ok(crate::service::test_issue_workload_certificate(request))
        }
    }

    #[derive(Default)]
    struct RecordingBootstrapTransport {
        prove_endpoint: Mutex<Option<String>>,
        deliver_endpoint: AsyncMutex<Option<String>>,
    }

    #[async_trait]
    impl GatewayBootstrapTransport for RecordingBootstrapTransport {
        async fn prove(
            &self,
            bootstrap_endpoint: &str,
            challenge: &GatewayReplicaActivationChallenge,
        ) -> Result<GatewayReplicaActivationProof, GatewayBootstrapTransportError> {
            *self.prove_endpoint.lock().unwrap() = Some(bootstrap_endpoint.to_owned());
            let key = Ed25519KeyPair::from_seed_unchecked(&[7; 32])
                .map_err(|error| GatewayBootstrapTransportError::Request(error.to_string()))?;
            let public_key = Ed25519PublicKeySpki::from_public_key_bytes(
                key.public_key()
                    .as_ref()
                    .try_into()
                    .map_err(|_| GatewayBootstrapTransportError::Request("invalid key".into()))?,
            );
            let signature =
                Ed25519Signature::new(
                    key.sign(&challenge.signing_bytes().map_err(|error| {
                        GatewayBootstrapTransportError::Request(error.to_string())
                    })?)
                    .as_ref()
                    .to_vec(),
                )
                .map_err(|error| GatewayBootstrapTransportError::Request(error.to_string()))?;
            Ok(GatewayReplicaActivationProof::new(public_key, signature))
        }

        async fn deliver_certificate(
            &self,
            bootstrap_endpoint: &str,
            _certificate: &IssuedWorkloadCertificate,
        ) -> Result<(), GatewayBootstrapTransportError> {
            *self.deliver_endpoint.lock().await = Some(bootstrap_endpoint.to_owned());
            Ok(())
        }
    }

    async fn activated_replica_fixture() -> (
        GatewayRegistryService,
        Arc<InMemoryGatewayRegistry>,
        AuthenticatedIdentity,
        ActivateGatewayReplicaRequest,
        GatewayReplicaResponse,
    ) {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        let policy = Arc::new(
            StaticRbacPolicy::one_principal(
                "operator",
                ["*".to_owned()],
                [Permission::GatewayManage],
            )
            .unwrap(),
        );
        let clock = Arc::new(InMemoryClock::new(100));
        let service = GatewayRegistryService::new(repository.clone(), policy, clock.clone());
        let identity = identity();
        service
            .create_pool(&identity, create_pool_request())
            .await
            .unwrap();
        let created = service
            .create_replica(&identity, create_replica_request())
            .await
            .unwrap();
        let request = ActivateGatewayReplicaRequest {
            gateway_replica_id: created.gateway_replica.gateway_replica_id,
            expected_resource_version: created.gateway_replica.resource_version,
            activation_token: created
                .activation_token
                .expect("first create returns an activation token"),
        };
        let activation_service = Arc::new(GatewayReplicaActivationService::new(
            repository.clone(),
            Arc::new(TestIssuer),
            clock,
            "mesh.example.test",
        ));
        let client = Arc::new(GatewayReplicaActivationClient::new(
            activation_service,
            Arc::new(RecordingBootstrapTransport::default()),
        ));
        let service = service.with_activation_client(client);
        let activated = service
            .activate_replica(&identity, request.clone())
            .await
            .unwrap();
        (service, repository, identity, request, activated)
    }

    fn fixture(
        permissions: impl IntoIterator<Item = Permission>,
    ) -> (
        GatewayRegistryService,
        Arc<InMemoryGatewayRegistry>,
        AuthenticatedIdentity,
    ) {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        let policy = Arc::new(
            StaticRbacPolicy::one_principal("operator", ["*".to_owned()], permissions).unwrap(),
        );
        (
            GatewayRegistryService::new(
                repository.clone(),
                policy,
                Arc::new(InMemoryClock::new(100)),
            ),
            repository,
            identity(),
        )
    }

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new(
            "operator",
            PrincipalKind::User,
            Arc::<str>::from("test-issuer"),
            Arc::<str>::from("test-subject"),
        )
        .unwrap()
    }

    fn create_pool_request() -> CreateGatewayPoolRequest {
        CreateGatewayPoolRequest {
            gateway_pool_id: "pool-a".to_owned(),
            edge_cluster_id: "cluster-a".to_owned(),
            display_name: "Primary Gateway".to_owned(),
            agent_endpoint: "https://gateway.example".to_owned(),
            s3_endpoint: None,
            desired_replicas: 2,
            minimum_ready_replicas: 1,
        }
    }

    fn create_replica_request() -> CreateGatewayReplicaRequest {
        CreateGatewayReplicaRequest {
            gateway_replica_id: "replica-a".to_owned(),
            gateway_pool_id: "pool-a".to_owned(),
            control_endpoint: "https://replica.control.example".to_owned(),
            peer_endpoint: "https://replica.peer.example".to_owned(),
            bootstrap_endpoint: "https://replica.bootstrap.example".to_owned(),
            software_version: "0.2.0".to_owned(),
            wire_version: CURRENT_WIRE_VERSION.get(),
            capabilities: neoengram_domain::protocol::gateway_capabilities_v1()
                .into_iter()
                .collect(),
        }
    }
}
