use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use async_trait::async_trait;
use neoengram_core::ContentDigest;
use neoengram_protocol::{AgentId, EdgeClusterId, GatewayPoolId, GatewayReplicaId};

use crate::{
    acquire_route_against, open_agent_session_against, release_route_against, renew_route_against,
    validate_agent_route_owner, validate_agent_route_session, validate_agent_route_target,
    validate_gateway_pool, validate_gateway_pool_replace, validate_gateway_replica,
    validate_gateway_replica_parent_replace, validate_gateway_replica_replace,
    validate_pool_list_request, validate_replica_list_request, validate_route_list_request,
    AcquireAgentRouteLeaseRequest, AcquireAgentSessionRouteRequest, AgentRouteLease,
    AgentRouteLeaseAcquireOutcome, AgentRouteLeaseListRequest, AgentRouteLeaseMutationOutcome,
    AgentSessionRouteAcquireOutcome, CentralError, CentralErrorCode, CentralResult,
    GatewayInsertOutcome, GatewayPoolListRequest, GatewayPoolRecord, GatewayPoolState,
    GatewayRegistryRepository, GatewayReplicaListRequest, GatewayReplicaRecord,
    GatewayReplicaState, InMemoryAgentRegistry, ReleaseAgentRouteLeaseRequest,
    RenewAgentRouteLeaseRequest,
};

#[derive(Debug)]
pub struct InMemoryGatewayRegistry {
    state: Mutex<GatewayRegistryState>,
    agent_registry: Arc<InMemoryAgentRegistry>,
}

#[derive(Debug, Default)]
struct GatewayRegistryState {
    pools: BTreeMap<GatewayPoolId, GatewayPoolRecord>,
    replicas: BTreeMap<GatewayReplicaId, GatewayReplicaRecord>,
    routes: BTreeMap<AgentId, AgentRouteLease>,
}

impl InMemoryGatewayRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::with_agent_registry(Arc::new(InMemoryAgentRegistry::new()))
    }

    #[must_use]
    pub fn with_agent_registry(agent_registry: Arc<InMemoryAgentRegistry>) -> Self {
        Self {
            state: Mutex::new(GatewayRegistryState::default()),
            agent_registry,
        }
    }

    #[must_use]
    pub fn agent_registry(&self) -> Arc<InMemoryAgentRegistry> {
        self.agent_registry.clone()
    }
}

impl Default for InMemoryGatewayRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GatewayRegistryRepository for InMemoryGatewayRegistry {
    async fn get_pool(
        &self,
        gateway_pool_id: &GatewayPoolId,
    ) -> CentralResult<Option<GatewayPoolRecord>> {
        Ok(lock(&self.state)?.pools.get(gateway_pool_id).cloned())
    }

    async fn get_pool_by_edge_cluster(
        &self,
        edge_cluster_id: &EdgeClusterId,
    ) -> CentralResult<Option<GatewayPoolRecord>> {
        Ok(lock(&self.state)?
            .pools
            .values()
            .find(|record| &record.edge_cluster_id == edge_cluster_id)
            .cloned())
    }

    async fn list_pools(
        &self,
        request: &GatewayPoolListRequest,
    ) -> CentralResult<Vec<GatewayPoolRecord>> {
        validate_pool_list_request(request)?;
        Ok(lock(&self.state)?
            .pools
            .iter()
            .filter(|(id, _)| request.after.as_ref().is_none_or(|after| *id > after))
            .filter(|(_, record)| {
                request
                    .edge_cluster_id
                    .as_ref()
                    .is_none_or(|cluster| &record.edge_cluster_id == cluster)
                    && request.state.is_none_or(|state| record.state == state)
            })
            .take(request.limit)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn insert_pool(
        &self,
        record: GatewayPoolRecord,
    ) -> CentralResult<GatewayInsertOutcome<GatewayPoolRecord>> {
        validate_gateway_pool(&record)?;
        if record.resource_version.get() != 1 || record.config_generation.get() != 1 {
            return invalid("new GatewayPool must begin at ResourceVersion/config generation 1");
        }
        let mut state = lock(&self.state)?;
        if let Some(existing) = state.pools.get(&record.gateway_pool_id) {
            return if existing == &record {
                Ok(GatewayInsertOutcome::Existing(existing.clone()))
            } else {
                identity_conflict("GatewayPool ID is already used")
            };
        }
        if state
            .pools
            .values()
            .any(|existing| existing.edge_cluster_id == record.edge_cluster_id)
        {
            return identity_conflict("EdgeCluster already has a GatewayPool");
        }
        ensure_pool_endpoints_unique(&state, &record, None)?;
        state
            .pools
            .insert(record.gateway_pool_id.clone(), record.clone());
        Ok(GatewayInsertOutcome::Inserted(record))
    }

    async fn replace_pool(
        &self,
        expected_resource_version: u64,
        record: GatewayPoolRecord,
    ) -> CentralResult<GatewayPoolRecord> {
        let mut state = lock(&self.state)?;
        let stored = state
            .pools
            .get(&record.gateway_pool_id)
            .cloned()
            .ok_or_else(pool_not_found)?;
        validate_gateway_pool_replace(&stored, expected_resource_version, &record)?;
        ensure_pool_endpoints_unique(&state, &record, Some(&record.gateway_pool_id))?;
        state
            .pools
            .insert(record.gateway_pool_id.clone(), record.clone());
        Ok(record)
    }

    async fn get_replica(
        &self,
        gateway_replica_id: &GatewayReplicaId,
    ) -> CentralResult<Option<GatewayReplicaRecord>> {
        Ok(lock(&self.state)?.replicas.get(gateway_replica_id).cloned())
    }

    async fn get_replica_by_activation_token_digest(
        &self,
        token_digest: &ContentDigest,
    ) -> CentralResult<Option<GatewayReplicaRecord>> {
        Ok(lock(&self.state)?
            .replicas
            .values()
            .find(|record| &record.credential.activation_token_digest == token_digest)
            .cloned())
    }

    async fn list_replicas(
        &self,
        request: &GatewayReplicaListRequest,
    ) -> CentralResult<Vec<GatewayReplicaRecord>> {
        validate_replica_list_request(request)?;
        Ok(lock(&self.state)?
            .replicas
            .iter()
            .filter(|(id, _)| request.after.as_ref().is_none_or(|after| *id > after))
            .filter(|(_, record)| {
                record.gateway_pool_id == request.gateway_pool_id
                    && request.state.is_none_or(|state| record.state == state)
            })
            .take(request.limit)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn insert_replica(
        &self,
        record: GatewayReplicaRecord,
    ) -> CentralResult<GatewayInsertOutcome<GatewayReplicaRecord>> {
        validate_gateway_replica(&record)?;
        if record.resource_version.get() != 1
            || record.state != GatewayReplicaState::Pending
            || record.credential.state != crate::GatewayCredentialState::PendingActivation
            || record.last_heartbeat_at_unix_ms.is_some()
        {
            return invalid("new GatewayReplica must begin pending at ResourceVersion 1");
        }
        let mut state = lock(&self.state)?;
        if let Some(existing) = state.replicas.get(&record.gateway_replica_id) {
            return if existing == &record {
                Ok(GatewayInsertOutcome::Existing(existing.clone()))
            } else {
                identity_conflict("GatewayReplica ID is already used")
            };
        }
        require_replica_pool(&state, &record)?;
        ensure_replica_unique(&state, &record, None)?;
        state
            .replicas
            .insert(record.gateway_replica_id.clone(), record.clone());
        Ok(GatewayInsertOutcome::Inserted(record))
    }

    async fn replace_replica(
        &self,
        expected_resource_version: u64,
        record: GatewayReplicaRecord,
    ) -> CentralResult<GatewayReplicaRecord> {
        let mut state = lock(&self.state)?;
        let stored = state
            .replicas
            .get(&record.gateway_replica_id)
            .cloned()
            .ok_or_else(replica_not_found)?;
        validate_gateway_replica_replace(&stored, expected_resource_version, &record)?;
        let pool = state
            .pools
            .get(&record.gateway_pool_id)
            .ok_or_else(pool_not_found)?;
        validate_gateway_replica_parent_replace(pool, &stored, &record)?;
        ensure_replica_unique(&state, &record, Some(&record.gateway_replica_id))?;
        state
            .replicas
            .insert(record.gateway_replica_id.clone(), record.clone());
        Ok(record)
    }

    async fn get_agent_route(&self, agent_id: &AgentId) -> CentralResult<Option<AgentRouteLease>> {
        Ok(lock(&self.state)?.routes.get(agent_id).cloned())
    }

    async fn list_agent_routes(
        &self,
        request: &AgentRouteLeaseListRequest,
    ) -> CentralResult<Vec<AgentRouteLease>> {
        validate_route_list_request(request)?;
        Ok(lock(&self.state)?
            .routes
            .iter()
            .filter(|(id, _)| request.after.as_ref().is_none_or(|after| *id > after))
            .filter(|(_, lease)| {
                lease.gateway_pool_id == request.gateway_pool_id
                    && request
                        .gateway_replica_id
                        .as_ref()
                        .is_none_or(|replica| &lease.gateway_replica_id == replica)
                    && request
                        .active_at_unix_ms
                        .is_none_or(|now| lease.is_active_at(now))
            })
            .take(request.limit)
            .map(|(_, lease)| lease.clone())
            .collect())
    }

    async fn acquire_agent_route(
        &self,
        request: AcquireAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseAcquireOutcome> {
        let agents = self.agent_registry.lock_records()?;
        let agent = agents
            .values()
            .find(|record| record.enrollment.reserved_agent_id == request.agent_id)
            .ok_or_else(agent_not_found)?;
        validate_agent_route_target(agent, &request)?;
        let mut state = lock(&self.state)?;
        require_route_target(&state, &request)?;
        ensure_request_id_unique(&state, &request.request_id, Some(&request.agent_id))?;
        ensure_connection_unique(&state, &request.connection_id, Some(&request.agent_id))?;
        let outcome = acquire_route_against(state.routes.get(&request.agent_id), &request)?;
        state
            .routes
            .insert(request.agent_id.clone(), outcome.lease.clone());
        Ok(outcome)
    }

    async fn acquire_agent_session_route(
        &self,
        request: AcquireAgentSessionRouteRequest,
    ) -> CentralResult<AgentSessionRouteAcquireOutcome> {
        let mut agents = self.agent_registry.lock_records()?;
        let stored = agents
            .values()
            .find(|record| record.enrollment.reserved_agent_id == request.session.agent_id)
            .cloned()
            .ok_or_else(agent_not_found)?;
        let session = open_agent_session_against(
            stored,
            &request.session,
            request.observed_at_unix_ms,
            request.heartbeat_timeout_ms,
        )?;
        let route_request = request.route_request(&session);
        validate_agent_route_target(&session.record, &route_request)?;

        let mut state = lock(&self.state)?;
        require_route_target(&state, &route_request)?;
        ensure_request_id_unique(
            &state,
            &route_request.request_id,
            Some(&route_request.agent_id),
        )?;
        ensure_connection_unique(
            &state,
            &route_request.connection_id,
            Some(&route_request.agent_id),
        )?;
        let route =
            acquire_route_against(state.routes.get(&route_request.agent_id), &route_request)?;
        if !session.replayed {
            crate::registry_memory::replace_record_locked(
                &mut agents,
                request.session.expected_resource_version.get(),
                session.record.clone(),
            )?;
        }
        if !route.replayed {
            state
                .routes
                .insert(route_request.agent_id, route.lease.clone());
        }
        Ok(AgentSessionRouteAcquireOutcome { session, route })
    }

    async fn renew_agent_route(
        &self,
        request: RenewAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseMutationOutcome> {
        let agents = self.agent_registry.lock_records()?;
        let agent = agents
            .values()
            .find(|record| record.enrollment.reserved_agent_id == request.agent_id)
            .ok_or_else(agent_not_found)?;
        validate_agent_route_session(agent, request.agent_id.clone(), request.session_generation)?;
        let mut state = lock(&self.state)?;
        ensure_request_id_unique(&state, &request.request_id, Some(&request.agent_id))?;
        let stored = state
            .routes
            .get(&request.agent_id)
            .cloned()
            .ok_or_else(route_not_found)?;
        require_route_owner(&state, &stored, request.renewed_at_unix_ms)?;
        let outcome = renew_route_against(&stored, &request)?;
        state
            .routes
            .insert(request.agent_id.clone(), outcome.lease.clone());
        Ok(outcome)
    }

    async fn release_agent_route(
        &self,
        request: ReleaseAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseMutationOutcome> {
        let agents = self.agent_registry.lock_records()?;
        let agent = agents
            .values()
            .find(|record| record.enrollment.reserved_agent_id == request.agent_id)
            .ok_or_else(agent_not_found)?;
        validate_agent_route_session(agent, request.agent_id.clone(), request.session_generation)?;
        let mut state = lock(&self.state)?;
        ensure_request_id_unique(&state, &request.request_id, Some(&request.agent_id))?;
        let stored = state
            .routes
            .get(&request.agent_id)
            .cloned()
            .ok_or_else(route_not_found)?;
        let outcome = release_route_against(&stored, &request)?;
        state
            .routes
            .insert(request.agent_id.clone(), outcome.lease.clone());
        Ok(outcome)
    }
}

fn require_replica_pool(
    state: &GatewayRegistryState,
    record: &GatewayReplicaRecord,
) -> CentralResult<()> {
    let pool = state
        .pools
        .get(&record.gateway_pool_id)
        .ok_or_else(pool_not_found)?;
    if pool.edge_cluster_id != record.edge_cluster_id
        || matches!(
            pool.state,
            GatewayPoolState::Draining | GatewayPoolState::Disabled
        )
    {
        return identity_conflict("GatewayReplica scope differs from its GatewayPool");
    }
    Ok(())
}

fn require_route_target(
    state: &GatewayRegistryState,
    request: &AcquireAgentRouteLeaseRequest,
) -> CentralResult<()> {
    let pool = state
        .pools
        .get(&request.gateway_pool_id)
        .ok_or_else(pool_not_found)?;
    let replica = state
        .replicas
        .get(&request.gateway_replica_id)
        .ok_or_else(replica_not_found)?;
    validate_agent_route_owner(
        pool,
        replica,
        &request.edge_cluster_id,
        &request.gateway_pool_id,
        &request.gateway_replica_id,
        request.acquired_at_unix_ms,
    )
}

fn require_route_owner(
    state: &GatewayRegistryState,
    route: &AgentRouteLease,
    observed_at_unix_ms: neoengram_protocol::UnixMillis,
) -> CentralResult<()> {
    let pool = state
        .pools
        .get(&route.gateway_pool_id)
        .ok_or_else(pool_not_found)?;
    let replica = state
        .replicas
        .get(&route.gateway_replica_id)
        .ok_or_else(replica_not_found)?;
    validate_agent_route_owner(
        pool,
        replica,
        &route.edge_cluster_id,
        &route.gateway_pool_id,
        &route.gateway_replica_id,
        observed_at_unix_ms,
    )
}

fn ensure_pool_endpoints_unique(
    state: &GatewayRegistryState,
    record: &GatewayPoolRecord,
    except: Option<&GatewayPoolId>,
) -> CentralResult<()> {
    if state.pools.values().any(|existing| {
        except != Some(&existing.gateway_pool_id)
            && (existing.agent_endpoint == record.agent_endpoint
                || existing.s3_endpoint.is_some() && existing.s3_endpoint == record.s3_endpoint)
    }) {
        return identity_conflict("GatewayPool endpoint is already registered");
    }
    Ok(())
}

fn ensure_replica_unique(
    state: &GatewayRegistryState,
    record: &GatewayReplicaRecord,
    except: Option<&GatewayReplicaId>,
) -> CentralResult<()> {
    let candidate_endpoints = [
        record.control_endpoint.as_str(),
        record.peer_endpoint.as_str(),
        record.bootstrap_endpoint.as_str(),
    ];
    if state.replicas.values().any(|existing| {
        except != Some(&existing.gateway_replica_id)
            && (replica_endpoints(existing).iter().any(|endpoint| {
                candidate_endpoints
                    .iter()
                    .any(|candidate| endpoint == candidate)
            }) || existing.credential.activation_token_digest
                == record.credential.activation_token_digest)
    }) {
        return identity_conflict(
            "GatewayReplica endpoint or activation token is already registered",
        );
    }
    Ok(())
}

fn replica_endpoints(record: &GatewayReplicaRecord) -> [&str; 3] {
    [
        record.control_endpoint.as_str(),
        record.peer_endpoint.as_str(),
        record.bootstrap_endpoint.as_str(),
    ]
}

fn ensure_connection_unique(
    state: &GatewayRegistryState,
    connection_id: &neoengram_protocol::GatewayConnectionId,
    except: Option<&AgentId>,
) -> CentralResult<()> {
    if state
        .routes
        .values()
        .any(|lease| except != Some(&lease.agent_id) && &lease.connection_id == connection_id)
    {
        return identity_conflict("Gateway connection ID is already bound to another Agent");
    }
    Ok(())
}

fn ensure_request_id_unique(
    state: &GatewayRegistryState,
    request_id: &neoengram_protocol::RequestId,
    except: Option<&AgentId>,
) -> CentralResult<()> {
    if state.routes.values().any(|lease| {
        except != Some(&lease.agent_id)
            && (&lease.acquire_request_id == request_id
                || lease.last_renew_request_id.as_ref() == Some(request_id)
                || lease.release_request_id.as_ref() == Some(request_id))
    }) {
        return identity_conflict("Gateway route RequestId is already bound to another Agent");
    }
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> CentralResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        CentralError::new(
            CentralErrorCode::Internal,
            "in-memory Gateway registry lock poisoned",
        )
    })
}

fn pool_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayPoolNotFound,
        "GatewayPool does not exist",
    )
    .with_retryable(false)
}

fn replica_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayReplicaNotFound,
        "GatewayReplica does not exist",
    )
    .with_retryable(false)
}

fn agent_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayRouteUnavailable,
        "Agent route requires a registered Agent",
    )
    .with_retryable(false)
}

fn route_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayRouteUnavailable,
        "Agent route lease does not exist",
    )
}

fn invalid<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(false))
}

fn identity_conflict<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(CentralErrorCode::GatewayIdentityConflict, message).with_retryable(false))
}
