#![cfg(feature = "authority-sqlite")]

use std::{collections::BTreeSet, path::Path, sync::Arc};

use neoengram_central::{
    open_sqlite_authority, AcquireAgentRouteLeaseRequest, AcquireAgentSessionRouteRequest,
    AgentRegistryRepository, AgentRegistryService, AgentRouteLease, AgentRouteLeaseListRequest,
    CentralErrorCode, CloseAgentSessionRequest, GatewayCredentialState, GatewayInsertOutcome,
    GatewayPoolListRequest, GatewayPoolRecord, GatewayPoolState, GatewayRegistryRepository,
    GatewayReplicaCertificateRecord, GatewayReplicaCredential, GatewayReplicaListRequest,
    GatewayReplicaRecord, GatewayReplicaState, InMemoryClock, InMemoryComponents,
    InMemoryGatewayRegistry, OpenAgentSessionRequest, ReleaseAgentRouteLeaseRequest,
    RenewAgentRouteLeaseRequest, SqliteAuthorityConfig, AGENT_ROUTE_LEASE_MAX_TTL_MS,
};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    AgentBootId, AgentBootstrapProbe, AgentBootstrapProof, AgentBootstrapRequest,
    AgentEnrollmentApprovalRequest, AgentEnrollmentDecision, AgentEnrollmentId,
    AgentEnrollmentTokenCreateRequest, AgentEnrollmentTokenId, AgentId, AgentInstallationId,
    AgentMountId, AgentMountIdentityDigest, CertificateGeneration, Ed25519PublicKeySpki,
    Ed25519Signature, EdgeClusterId, Extensions, GatewayConnectionId, GatewayOpaqueBytes,
    GatewayPoolId, GatewayReplicaId, Generation, MountAccessMode, PrincipalId, PrincipalKind,
    PrincipalRef, PvcIdentityDigest, RequestId, ResourceHealth, ResourceVersion, RouteGeneration,
    SessionGeneration, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId, CURRENT_WIRE_VERSION,
};
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
use tempfile::TempDir;

#[derive(Debug)]
struct ContractResult {
    pool: GatewayPoolRecord,
    replica: GatewayReplicaRecord,
    route: AgentRouteLease,
}

#[tokio::test]
async fn gateway_registry_contract_matches_memory_and_sqlite() {
    let memory_registry = Arc::new(InMemoryGatewayRegistry::new());
    let memory: Arc<dyn GatewayRegistryRepository> = memory_registry.clone();
    let memory_agents: Arc<dyn AgentRegistryRepository> = memory_registry.agent_registry();
    let memory_result = run_contract(memory.clone(), memory_agents.clone()).await;
    assert_contract_state(&memory, &memory_agents, &memory_result).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = sqlite.gateway_repository();
    let agent_repository = sqlite.repository();
    let sqlite_result = run_contract(repository.clone(), agent_repository.clone()).await;
    assert_eq!(sqlite_result.pool, memory_result.pool);
    assert_eq!(sqlite_result.replica, memory_result.replica);
    assert_eq!(sqlite_result.route, memory_result.route);
    sqlite.integrity_check().await.unwrap();
    drop(repository);
    drop(agent_repository);
    drop(sqlite);

    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert_contract_state(
        &reopened.gateway_repository(),
        &reopened.repository(),
        &sqlite_result,
    )
    .await;
    reopened.integrity_check().await.unwrap();
}

#[tokio::test]
async fn gateway_replica_endpoint_identity_contract_matches_memory_and_sqlite() {
    run_replica_endpoint_identity_contract(Arc::new(InMemoryGatewayRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    run_replica_endpoint_identity_contract(sqlite.gateway_repository()).await;
    sqlite.integrity_check().await.unwrap();
}

#[tokio::test]
async fn gateway_replica_pool_state_guard_matches_memory_and_sqlite() {
    run_replica_pool_state_guard(Arc::new(InMemoryGatewayRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    run_replica_pool_state_guard(sqlite.gateway_repository()).await;
    sqlite.integrity_check().await.unwrap();
}

async fn run_replica_pool_state_guard(repository: Arc<dyn GatewayRegistryRepository>) {
    repository
        .insert_pool(pool_record("pool-a", "cluster-a"))
        .await
        .unwrap();
    let pending = replica_record();
    repository.insert_replica(pending.clone()).await.unwrap();
    let active =
        activate_gateway_replica(repository.clone(), replica_record_for("active-disabled")).await;

    // Draining is a terminal parent lifecycle: a replay of an existing identity remains
    // idempotent, while new identities, ordinary metadata changes, and certificate preparation
    // are fenced. Existing Replicas may still complete drain/revoke/expiry cleanup.
    let draining_pending = replica_record_for("draining-pending");
    repository
        .insert_replica(draining_pending.clone())
        .await
        .unwrap();
    let draining_active =
        activate_gateway_replica(repository.clone(), replica_record_for("draining-active")).await;
    let mut draining_pool = pool_record("pool-a", "cluster-a");
    draining_pool.state = GatewayPoolState::Draining;
    draining_pool.config_generation = Generation::new(2);
    draining_pool.resource_version = ResourceVersion::new(2);
    draining_pool.updated_at_unix_ms = UnixMillis::new(101);
    draining_pool.updated_by = principal("gateway-operator-drain");
    repository.replace_pool(1, draining_pool).await.unwrap();

    assert!(matches!(
        repository
            .insert_replica(draining_pending.clone())
            .await
            .unwrap(),
        GatewayInsertOutcome::Existing(_)
    ));
    let error = repository
        .insert_replica(replica_record_for("draining-new"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayIdentityConflict);
    let mut metadata_change = draining_pending.clone();
    metadata_change.software_version = "0.2.1".to_owned();
    metadata_change.resource_version = ResourceVersion::new(2);
    metadata_change.updated_at_unix_ms = UnixMillis::new(102);
    let error = repository
        .replace_replica(1, metadata_change)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayIdentityConflict);
    let error = repository
        .replace_replica(1, prepared_gateway_replica(draining_pending.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayIdentityConflict);

    let mut draining_expired = draining_pending.clone();
    draining_expired.credential.state = GatewayCredentialState::Expired;
    draining_expired.resource_version = ResourceVersion::new(2);
    draining_expired.updated_at_unix_ms = UnixMillis::new(102);
    repository
        .replace_replica(1, draining_expired)
        .await
        .unwrap();
    let mut drained_active = draining_active.clone();
    drained_active.state = GatewayReplicaState::Draining;
    drained_active.resource_version = ResourceVersion::new(4);
    drained_active.updated_at_unix_ms = UnixMillis::new(204);
    repository.replace_replica(3, drained_active).await.unwrap();

    let mut disabled_pool = pool_record("pool-a", "cluster-a");
    disabled_pool.state = GatewayPoolState::Disabled;
    disabled_pool.config_generation = Generation::new(3);
    disabled_pool.resource_version = ResourceVersion::new(3);
    disabled_pool.updated_at_unix_ms = UnixMillis::new(104);
    disabled_pool.updated_by = principal("gateway-operator-b");
    repository.replace_pool(2, disabled_pool).await.unwrap();

    // Exact replay remains idempotent after a Pool is disabled, while a new identity and ordinary
    // metadata replacement remain rejected by the parent lifecycle fence.
    assert!(matches!(
        repository.insert_replica(pending.clone()).await.unwrap(),
        GatewayInsertOutcome::Existing(_)
    ));
    let candidate = replica_record_for("disabled-insert");
    let error = repository.insert_replica(candidate).await.unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayIdentityConflict);

    let mut replacement = pending.clone();
    replacement.software_version = "0.2.1".to_owned();
    replacement.resource_version = ResourceVersion::new(2);
    replacement.updated_at_unix_ms = UnixMillis::new(102);
    let error = repository
        .replace_replica(1, replacement)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayIdentityConflict);

    // Terminal cleanup remains available under a Disabled parent. Pending activation expiry,
    // explicit drain, and credential-generation fencing all preserve endpoint and identity data.
    let mut expired = pending.clone();
    expired.credential.state = GatewayCredentialState::Expired;
    expired.resource_version = ResourceVersion::new(2);
    expired.updated_at_unix_ms = UnixMillis::new(102);
    let expired = repository.replace_replica(1, expired).await.unwrap();
    assert_eq!(expired.credential.state, GatewayCredentialState::Expired);

    let mut revoked_pending = expired.clone();
    revoked_pending.state = GatewayReplicaState::Revoked;
    revoked_pending.credential.state = GatewayCredentialState::Revoked;
    revoked_pending.resource_version = ResourceVersion::new(3);
    revoked_pending.updated_at_unix_ms = UnixMillis::new(103);
    let revoked_pending = repository
        .replace_replica(2, revoked_pending)
        .await
        .unwrap();
    assert_eq!(revoked_pending.state, GatewayReplicaState::Revoked);

    let mut drained = active.clone();
    drained.state = GatewayReplicaState::Draining;
    drained.resource_version = ResourceVersion::new(4);
    drained.updated_at_unix_ms = UnixMillis::new(204);
    let drained = repository.replace_replica(3, drained).await.unwrap();
    assert_eq!(drained.state, GatewayReplicaState::Draining);

    let mut revoked_active = drained.clone();
    revoked_active.state = GatewayReplicaState::Revoked;
    revoked_active.credential.state = GatewayCredentialState::Revoked;
    revoked_active.credential.certificate_generation = Some(CertificateGeneration::new(2));
    revoked_active.resource_version = ResourceVersion::new(5);
    revoked_active.updated_at_unix_ms = UnixMillis::new(205);
    let revoked_active = repository.replace_replica(4, revoked_active).await.unwrap();
    assert_eq!(
        revoked_active.credential.certificate_generation,
        Some(CertificateGeneration::new(2))
    );
    assert_eq!(
        repository
            .get_replica(&pending.gateway_replica_id)
            .await
            .unwrap(),
        Some(revoked_pending)
    );
    assert_eq!(
        repository
            .get_replica(&active.gateway_replica_id)
            .await
            .unwrap(),
        Some(revoked_active)
    );
}

async fn run_replica_endpoint_identity_contract(repository: Arc<dyn GatewayRegistryRepository>) {
    repository
        .insert_pool(pool_record("pool-a", "cluster-a"))
        .await
        .unwrap();

    // A provisioner may still correct an unissued Pending registration.  The immutable SAN
    // boundary starts only when certificate preparation persists identity material.
    let mut pending_correction = replica_record_for("pending-endpoint-correction");
    repository
        .insert_replica(pending_correction.clone())
        .await
        .unwrap();
    pending_correction.control_endpoint =
        "https://pending-endpoint-correction-v2.control.example".to_owned();
    pending_correction.resource_version = ResourceVersion::new(2);
    pending_correction.updated_at_unix_ms = UnixMillis::new(101);
    let corrected = repository
        .replace_replica(1, pending_correction.clone())
        .await
        .expect("unissued Pending Replica endpoints remain correctable");
    assert_eq!(corrected, pending_correction);

    for (case, first_role, second_role) in [
        ("control-peer", 0, 1),
        ("control-bootstrap", 0, 2),
        ("peer-bootstrap", 1, 2),
    ] {
        let mut replica = replica_record_for(&format!("invalid-{case}"));
        let endpoint = replica_endpoint(&replica, first_role).to_owned();
        set_replica_endpoint(&mut replica, second_role, endpoint);
        let error = repository.insert_replica(replica).await.unwrap_err();
        assert_eq!(error.code(), CentralErrorCode::InvalidState, "{case}");
    }

    let owner = replica_record_for("endpoint-owner");
    repository.insert_replica(owner.clone()).await.unwrap();
    for owner_role in 0..3 {
        for candidate_role in 0..3 {
            let id = format!("insert-conflict-{owner_role}-{candidate_role}");
            let mut candidate = replica_record_for(&id);
            set_replica_endpoint(
                &mut candidate,
                candidate_role,
                replica_endpoint(&owner, owner_role).to_owned(),
            );
            let error = repository.insert_replica(candidate).await.unwrap_err();
            assert_eq!(
                error.code(),
                CentralErrorCode::GatewayIdentityConflict,
                "owner role {owner_role}, candidate role {candidate_role}"
            );
        }
    }

    for owner_role in 0..3 {
        for candidate_role in 0..3 {
            let id = format!("replace-conflict-{owner_role}-{candidate_role}");
            let stored = replica_record_for(&id);
            repository.insert_replica(stored.clone()).await.unwrap();
            let mut replacement = stored.clone();
            set_replica_endpoint(
                &mut replacement,
                candidate_role,
                replica_endpoint(&owner, owner_role).to_owned(),
            );
            replacement.resource_version = ResourceVersion::new(2);
            replacement.updated_at_unix_ms = UnixMillis::new(101);
            let error = repository
                .replace_replica(1, replacement)
                .await
                .unwrap_err();
            assert_eq!(
                error.code(),
                CentralErrorCode::GatewayIdentityConflict,
                "owner role {owner_role}, candidate role {candidate_role}"
            );
            assert_eq!(
                repository
                    .get_replica(&stored.gateway_replica_id)
                    .await
                    .unwrap(),
                Some(stored)
            );
        }
    }
}

#[tokio::test]
async fn gateway_replica_endpoint_roles_are_unique_across_backends() {
    let memory = Arc::new(InMemoryGatewayRegistry::new());
    assert_endpoint_conflicts(memory.clone()).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert_endpoint_conflicts(sqlite.gateway_repository()).await;
    sqlite.integrity_check().await.unwrap();
}

async fn assert_endpoint_conflicts(repository: Arc<dyn GatewayRegistryRepository>) {
    repository
        .insert_pool(pool_record("pool-a", "cluster-a"))
        .await
        .unwrap();
    let first = replica_record();
    repository.insert_replica(first.clone()).await.unwrap();

    // Role names are part of the routing authority. A control endpoint cannot be reused as a
    // peer or bootstrap endpoint by another Replica, even though each individual SQL column has
    // its own UNIQUE index.
    let mut cross_role = replica_record_for("replica-cross-role");
    cross_role.peer_endpoint = first.control_endpoint.clone();
    let error = repository.insert_replica(cross_role).await.unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayIdentityConflict);

    // A single Replica must never expose two listener roles at one origin.
    let mut self_conflict = replica_record_for("replica-self-conflict");
    self_conflict.bootstrap_endpoint = self_conflict.control_endpoint.clone();
    let error = repository.insert_replica(self_conflict).await.unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    // Replacement is checked in the same backend contract, not only at create time.
    let mut replace_candidate = replica_record_for("replica-replace");
    repository
        .insert_replica(replace_candidate.clone())
        .await
        .unwrap();
    replace_candidate.peer_endpoint = first.bootstrap_endpoint.clone();
    replace_candidate.resource_version = ResourceVersion::new(2);
    replace_candidate.updated_at_unix_ms = UnixMillis::new(101);
    let error = repository
        .replace_replica(1, replace_candidate)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayIdentityConflict);
}

#[tokio::test]
async fn route_renewal_fences_revoked_or_expired_gateway_owner() {
    let memory_registry = Arc::new(InMemoryGatewayRegistry::new());
    let memory_agents: Arc<dyn AgentRegistryRepository> = memory_registry.agent_registry();
    let _route = setup_owner_route(memory_registry.clone(), memory_agents.clone(), 2_000).await;
    let error = memory_registry
        .renew_agent_route(renew_owner_request("memory-expired", 2_000, 4_000))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let sqlite_registry = sqlite.gateway_repository();
    let sqlite_agents = sqlite.repository();
    let _route = setup_owner_route(sqlite_registry.clone(), sqlite_agents, 2_000).await;
    let error = sqlite_registry
        .renew_agent_route(renew_owner_request("sqlite-expired", 2_000, 4_000))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);

    let memory_registry = Arc::new(InMemoryGatewayRegistry::new());
    let memory_agents: Arc<dyn AgentRegistryRepository> = memory_registry.agent_registry();
    let _route = setup_owner_route(memory_registry.clone(), memory_agents, 10_000).await;
    let mut revoked = memory_registry
        .get_replica(&replica_id())
        .await
        .unwrap()
        .unwrap();
    revoked.state = GatewayReplicaState::Revoked;
    revoked.credential.state = GatewayCredentialState::Revoked;
    revoked.credential.certificate_generation = Some(CertificateGeneration::new(2));
    revoked.resource_version = ResourceVersion::new(4);
    revoked.updated_at_unix_ms = UnixMillis::new(300);
    memory_registry.replace_replica(3, revoked).await.unwrap();
    let error = memory_registry
        .renew_agent_route(renew_owner_request("memory-revoked", 2_000, 4_000))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);

    drop(sqlite);
}

#[tokio::test]
async fn route_timestamp_range_matches_memory_and_sqlite() {
    let memory = Arc::new(InMemoryGatewayRegistry::new());
    run_route_timestamp_range_contract(
        memory.clone(),
        memory.agent_registry() as Arc<dyn AgentRegistryRepository>,
    )
    .await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    run_route_timestamp_range_contract(sqlite.gateway_repository(), sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
}

async fn run_route_timestamp_range_contract(
    repository: Arc<dyn GatewayRegistryRepository>,
    agent_repository: Arc<dyn AgentRegistryRepository>,
) {
    let first_out_of_range = i64::MAX as u64 + 1;
    let certificate_not_after = first_out_of_range + 100_000;
    setup_owner_route(repository.clone(), agent_repository, certificate_not_after).await;

    let mut acquire = acquire_request(
        "route-range-acquire",
        "route-range-connection",
        1,
        first_out_of_range,
        first_out_of_range + 100,
    );
    acquire.gateway_replica_id = replica_id();
    let error = repository.acquire_agent_route(acquire).await.unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    let error = repository
        .renew_agent_route(renew_owner_request(
            "route-range-renew",
            first_out_of_range,
            first_out_of_range + 100,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    let error = repository
        .release_agent_route(ReleaseAgentRouteLeaseRequest {
            request_id: request_id("route-range-release"),
            agent_id: agent_id(),
            gateway_replica_id: replica_id(),
            connection_id: connection_id("owner-fence-connection"),
            session_generation: SessionGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            released_at_unix_ms: UnixMillis::new(first_out_of_range),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);
}

/// Two Gateway replicas may both be reachable, but Central's durable route lease still has one
/// owner.  This exercises the handoff boundary that the network harness cannot prove by itself:
/// an active owner can renew, a second replica cannot steal the live lease, and only an expired
/// lease can be acquired by that second replica.  The old owner remains fenced after the handoff.
#[tokio::test]
async fn dual_replica_route_handoff_is_authoritative_and_expiry_fenced() {
    let memory_registry = Arc::new(InMemoryGatewayRegistry::new());
    let memory_agents: Arc<dyn AgentRegistryRepository> = memory_registry.agent_registry();
    run_dual_replica_handoff(memory_registry.clone(), memory_agents.clone()).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let sqlite_registry = sqlite.gateway_repository();
    let sqlite_agents = sqlite.repository();
    run_dual_replica_handoff(sqlite_registry, sqlite_agents).await;
    sqlite.integrity_check().await.unwrap();
}

async fn run_dual_replica_handoff(
    repository: Arc<dyn GatewayRegistryRepository>,
    agent_repository: Arc<dyn AgentRegistryRepository>,
) {
    let initial = setup_owner_route(repository.clone(), agent_repository.clone(), 20_000).await;
    assert_eq!(initial.gateway_replica_id, replica_id());

    let replica_b = replica_record_for("replica-b");
    activate_gateway_replica(repository.clone(), replica_b).await;

    // A is the sole owner and can extend its own lease while the session fence is current.
    let renewed = repository
        .renew_agent_route(RenewAgentRouteLeaseRequest {
            request_id: request_id("dual-replica-renew-a"),
            agent_id: agent_id(),
            gateway_replica_id: replica_id(),
            connection_id: connection_id("owner-fence-connection"),
            session_generation: SessionGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            renewed_at_unix_ms: UnixMillis::new(1_500),
            lease_expires_at_unix_ms: UnixMillis::new(3_500),
        })
        .await
        .unwrap();
    assert!(!renewed.replayed);
    assert_eq!(
        renewed.lease.lease_expires_at_unix_ms,
        UnixMillis::new(3_500)
    );

    // Replica B is reachable, but a live A lease is an authoritative single-writer fence.
    let mut before_expiry = acquire_request(
        "dual-replica-before-expiry",
        "dual-replica-connection-b",
        1,
        1_600,
        3_600,
    );
    before_expiry.gateway_replica_id = replica_b_id();
    let error = repository
        .acquire_agent_route(before_expiry)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);
    assert!(error.retryable());

    // Once A's lease expires, its own renewal is rejected before B is allowed to take over.
    let error = repository
        .renew_agent_route(RenewAgentRouteLeaseRequest {
            request_id: request_id("dual-replica-renew-expired-a"),
            agent_id: agent_id(),
            gateway_replica_id: replica_id(),
            connection_id: connection_id("owner-fence-connection"),
            session_generation: SessionGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            renewed_at_unix_ms: UnixMillis::new(3_501),
            lease_expires_at_unix_ms: UnixMillis::new(4_500),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteFenced);

    let mut after_expiry = acquire_request(
        "dual-replica-after-expiry",
        "dual-replica-connection-b",
        1,
        3_501,
        4_501,
    );
    after_expiry.gateway_replica_id = replica_b_id();
    let takeover = repository.acquire_agent_route(after_expiry).await.unwrap();
    assert!(!takeover.replayed);
    assert_eq!(takeover.lease.gateway_replica_id, replica_b_id());
    assert_eq!(takeover.lease.route_generation, RouteGeneration::new(2));
    assert_eq!(takeover.fenced, Some(renewed.lease.clone()));

    // Fencing is checked against the new durable owner, so an old A frame cannot renew or release
    // the route after B has acquired it.
    let error = repository
        .renew_agent_route(RenewAgentRouteLeaseRequest {
            request_id: request_id("dual-replica-stale-renew-a"),
            agent_id: agent_id(),
            gateway_replica_id: replica_id(),
            connection_id: connection_id("owner-fence-connection"),
            session_generation: SessionGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            renewed_at_unix_ms: UnixMillis::new(3_600),
            lease_expires_at_unix_ms: UnixMillis::new(4_600),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteFenced);

    let renewed_b = repository
        .renew_agent_route(RenewAgentRouteLeaseRequest {
            request_id: request_id("dual-replica-renew-b"),
            agent_id: agent_id(),
            gateway_replica_id: replica_b_id(),
            connection_id: connection_id("dual-replica-connection-b"),
            session_generation: SessionGeneration::new(1),
            route_generation: RouteGeneration::new(2),
            renewed_at_unix_ms: UnixMillis::new(3_700),
            lease_expires_at_unix_ms: UnixMillis::new(4_700),
        })
        .await
        .unwrap();
    assert!(!renewed_b.replayed);
    assert_eq!(renewed_b.lease.gateway_replica_id, replica_b_id());
    assert!(
        repository
            .renew_agent_route(RenewAgentRouteLeaseRequest {
                request_id: request_id("dual-replica-renew-b"),
                agent_id: agent_id(),
                gateway_replica_id: replica_b_id(),
                connection_id: connection_id("dual-replica-connection-b"),
                session_generation: SessionGeneration::new(1),
                route_generation: RouteGeneration::new(2),
                renewed_at_unix_ms: UnixMillis::new(3_700),
                lease_expires_at_unix_ms: UnixMillis::new(4_700),
            })
            .await
            .unwrap()
            .replayed
    );
}

#[tokio::test]
async fn authority_compositions_expose_gateway_registry() {
    let memory = InMemoryComponents::new(100);
    assert!(memory.authority_store().gateway_registry().is_some());

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert!(sqlite.authority_store().gateway_registry().is_some());
    sqlite.integrity_check().await.unwrap();
}

#[tokio::test]
async fn sqlite_gateway_integrity_rejects_index_payload_drift() {
    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    sqlite
        .gateway_repository()
        .insert_pool(pool_record("pool-a", "cluster-a"))
        .await
        .unwrap();
    drop(sqlite);
    execute_raw(
        directory.path(),
        "UPDATE gateway_pool_records SET state = 'draining' WHERE gateway_pool_id = 'pool-a';",
    )
    .await;

    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("Gateway indexed-column drift must fail closed");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

#[tokio::test]
async fn sqlite_gateway_integrity_rejects_cross_role_endpoint_duplicates() {
    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = sqlite.gateway_repository();
    repository
        .insert_pool(pool_record("pool-a", "cluster-a"))
        .await
        .unwrap();
    let owner = replica_record_for("integrity-owner");
    let duplicate = replica_record_for("integrity-duplicate");
    repository.insert_replica(owner.clone()).await.unwrap();
    repository.insert_replica(duplicate.clone()).await.unwrap();
    drop(repository);
    drop(sqlite);

    rewrite_replica_endpoint_payload(
        directory.path(),
        duplicate.gateway_replica_id.as_str(),
        "control_endpoint",
        &owner.peer_endpoint,
    )
    .await;

    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("cross-role Gateway endpoint duplicates must fail closed");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

#[tokio::test]
async fn sqlite_gateway_integrity_rejects_replica_without_credentials() {
    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = sqlite.gateway_repository();
    repository
        .insert_pool(pool_record("pool-a", "cluster-a"))
        .await
        .unwrap();
    repository.insert_replica(replica_record()).await.unwrap();
    drop(repository);
    drop(sqlite);

    execute_raw(
        directory.path(),
        "DELETE FROM gateway_replica_credentials WHERE gateway_replica_id = 'replica-a';",
    )
    .await;

    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("a GatewayReplica without its credential row must fail closed");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

#[tokio::test]
async fn sqlite_gateway_integrity_rejects_cross_role_endpoint_drift() {
    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = sqlite.gateway_repository();
    repository
        .insert_pool(pool_record("pool-a", "cluster-a"))
        .await
        .unwrap();
    repository.insert_replica(replica_record()).await.unwrap();
    repository
        .insert_replica(replica_record_for("replica-b"))
        .await
        .unwrap();
    drop(repository);
    drop(sqlite);

    // Per-column UNIQUE indexes cannot catch a control endpoint reused as another role. The
    // startup integrity check must still reject this cross-role corruption before serving data.
    execute_raw(
        directory.path(),
        "UPDATE gateway_replica_records
         SET peer_endpoint = (
             SELECT control_endpoint FROM gateway_replica_records
             WHERE gateway_replica_id = 'replica-a'
         )
         WHERE gateway_replica_id = 'replica-b';",
    )
    .await;
    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("cross-role endpoint corruption must fail closed");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

#[tokio::test]
async fn sqlite_gateway_schema_drift_is_rejected() {
    let directory = TempDir::new().unwrap();
    drop(
        open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap(),
    );
    execute_raw(directory.path(), "DROP INDEX agent_route_expiry_keyset;").await;
    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("Gateway schema drift must fail closed");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

#[tokio::test]
async fn sqlite_gateway_schema_drift_rejects_unexpected_views_and_triggers() {
    for (name, statement) in [
        (
            "view",
            "CREATE VIEW unexpected_gateway_view AS SELECT gateway_pool_id FROM gateway_pool_records;",
        ),
        (
            "trigger",
            "CREATE TRIGGER unexpected_gateway_trigger AFTER UPDATE ON gateway_pool_records BEGIN SELECT 1; END;",
        ),
    ] {
        let directory = TempDir::new().unwrap();
        drop(
            open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
                .await
                .unwrap(),
        );
        execute_raw(directory.path(), statement).await;
        let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .err()
            .unwrap_or_else(|| panic!("unexpected Gateway {name} schema object must fail closed"));
        assert_eq!(error.code(), CentralErrorCode::StorageFailure, "{name}");
    }
}

#[tokio::test]
async fn sqlite_route_integrity_rejects_invalid_current_renewal_window() {
    for (case, invalid_expiry) in [("zero", 1_000_u64), ("overlong", 31_001_u64)] {
        let directory = TempDir::new().unwrap();
        let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let route =
            setup_owner_route(sqlite.gateway_repository(), sqlite.repository(), 20_000).await;
        assert_eq!(route.renewed_at_unix_ms, UnixMillis::new(1_000));
        let expected_expiry = if case == "zero" {
            route.renewed_at_unix_ms.get()
        } else {
            route.renewed_at_unix_ms.get() + AGENT_ROUTE_LEASE_MAX_TTL_MS + 1
        };
        assert_eq!(invalid_expiry, expected_expiry);
        drop(sqlite);

        rewrite_route_expiry_payload(directory.path(), invalid_expiry).await;
        let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .err()
            .unwrap_or_else(|| panic!("{case} route renewal window must fail closed"));
        assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    }
}

#[tokio::test]
async fn sqlite_route_integrity_rejects_duplicate_mutation_request_ids() {
    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let route = setup_owner_route(sqlite.gateway_repository(), sqlite.repository(), 20_000).await;
    let duplicate = route.acquire_request_id.to_string();
    drop(sqlite);

    rewrite_route_request_id_payload(directory.path(), &duplicate).await;
    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("duplicate Route mutation RequestIds must fail closed");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

async fn run_contract(
    repository: Arc<dyn GatewayRegistryRepository>,
    agent_repository: Arc<dyn AgentRegistryRepository>,
) -> ContractResult {
    let approved = approve_agent(agent_repository.clone()).await;
    let initial_pool = pool_record("pool-a", "cluster-a");
    assert!(matches!(
        repository.insert_pool(initial_pool.clone()).await.unwrap(),
        GatewayInsertOutcome::Inserted(_)
    ));
    assert!(matches!(
        repository.insert_pool(initial_pool.clone()).await.unwrap(),
        GatewayInsertOutcome::Existing(_)
    ));
    let conflict = repository
        .insert_pool(pool_record("pool-b", "cluster-a"))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), CentralErrorCode::GatewayIdentityConflict);

    let pools = repository
        .list_pools(&GatewayPoolListRequest {
            edge_cluster_id: Some(cluster_id()),
            state: Some(GatewayPoolState::Provisioning),
            after: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(pools.as_slice(), std::slice::from_ref(&initial_pool));

    let mut pool = initial_pool;
    pool.state = GatewayPoolState::Ready;
    pool.config_generation = Generation::new(2);
    pool.resource_version = ResourceVersion::new(2);
    pool.updated_at_unix_ms = UnixMillis::new(110);
    pool.updated_by = principal("gateway-operator-b");
    pool = repository.replace_pool(1, pool).await.unwrap();
    let stale_pool = repository.replace_pool(1, pool.clone()).await.unwrap_err();
    assert_eq!(stale_pool.code(), CentralErrorCode::ConcurrentUpdate);

    let mut changed_agent_endpoint = pool.clone();
    changed_agent_endpoint.agent_endpoint = "https://rotated.agent.example".to_owned();
    changed_agent_endpoint.config_generation = Generation::new(3);
    changed_agent_endpoint.resource_version = ResourceVersion::new(3);
    changed_agent_endpoint.updated_at_unix_ms = UnixMillis::new(111);
    let error = repository
        .replace_pool(2, changed_agent_endpoint)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    let mut changed_s3_endpoint = pool.clone();
    changed_s3_endpoint.s3_endpoint = Some("https://rotated.s3.example".to_owned());
    changed_s3_endpoint.config_generation = Generation::new(3);
    changed_s3_endpoint.resource_version = ResourceVersion::new(3);
    changed_s3_endpoint.updated_at_unix_ms = UnixMillis::new(111);
    let error = repository
        .replace_pool(2, changed_s3_endpoint)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    let pending_replica = replica_record();
    assert!(matches!(
        repository
            .insert_replica(pending_replica.clone())
            .await
            .unwrap(),
        GatewayInsertOutcome::Inserted(_)
    ));
    assert!(matches!(
        repository
            .insert_replica(pending_replica.clone())
            .await
            .unwrap(),
        GatewayInsertOutcome::Existing(_)
    ));
    let verifier = pending_replica
        .credential
        .activation_token_digest
        .to_string();
    let debug = format!("{:?}", pending_replica.credential);
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(&verifier));
    assert_eq!(
        repository
            .get_replica_by_activation_token_digest(
                &pending_replica.credential.activation_token_digest
            )
            .await
            .unwrap(),
        Some(pending_replica.clone())
    );

    let mut revoked_pending = pending_replica.clone();
    revoked_pending.gateway_replica_id = GatewayReplicaId::new("replica-revoked").unwrap();
    revoked_pending.control_endpoint = "https://replica-revoked.control.example".to_owned();
    revoked_pending.peer_endpoint = "https://replica-revoked.peer.example".to_owned();
    revoked_pending.bootstrap_endpoint = "https://replica-revoked.bootstrap.example".to_owned();
    revoked_pending.credential.activation_token_digest =
        ContentDigest::hash(b"activation-token-revoked");
    repository
        .insert_replica(revoked_pending.clone())
        .await
        .unwrap();
    revoked_pending.state = GatewayReplicaState::Revoked;
    revoked_pending.credential.state = GatewayCredentialState::Revoked;
    revoked_pending.resource_version = ResourceVersion::new(2);
    revoked_pending.updated_at_unix_ms = UnixMillis::new(150);
    repository
        .replace_replica(1, revoked_pending)
        .await
        .unwrap();

    let public_key = Ed25519PublicKeySpki::from_public_key_bytes([7; 32]);
    let leaf_certificate = GatewayOpaqueBytes::new(b"gateway-cert".to_vec()).unwrap();
    let issuer_certificate = GatewayOpaqueBytes::new(b"gateway-issuer".to_vec()).unwrap();
    let mut replica = pending_replica;
    replica.credential.public_key_fingerprint = Some(public_key.fingerprint());
    replica.credential.certificate_generation = Some(CertificateGeneration::new(1));
    replica.credential.certificate_fingerprint =
        Some(ContentDigest::hash(leaf_certificate.as_bytes()));
    replica.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(21_600_200));
    replica.credential.certificate = Some(GatewayReplicaCertificateRecord {
        request_id: RequestId::new("gateway-activation-request").unwrap(),
        public_key_spki: public_key,
        certificate_generation: CertificateGeneration::new(1),
        not_before_unix_ms: UnixMillis::new(200),
        not_after_unix_ms: UnixMillis::new(21_600_200),
        server_names: BTreeSet::new(),
        leaf_certificate_der: leaf_certificate,
        issuer_chain_der: vec![issuer_certificate],
    });
    replica.resource_version = ResourceVersion::new(2);
    replica.updated_at_unix_ms = UnixMillis::new(200);
    replica.credential.state = GatewayCredentialState::PendingCertificateDelivery;
    replica = repository.replace_replica(1, replica).await.unwrap();
    assert_eq!(replica.state, GatewayReplicaState::Pending);
    assert!(replica.credential.activation_consumed_at_unix_ms.is_none());

    replica.state = GatewayReplicaState::Active;
    replica.credential.state = GatewayCredentialState::Active;
    replica.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(201));
    replica.resource_version = ResourceVersion::new(3);
    replica.updated_at_unix_ms = UnixMillis::new(201);
    replica = repository.replace_replica(2, replica).await.unwrap();

    // Certificate preparation binds all listener hosts into the SAN set.  A repository caller
    // must not be able to move any one role behind the certificate's back while the Replica is
    // active.  Check every role through the same backend contract.
    for (role, label) in [(0, "control"), (1, "peer"), (2, "bootstrap")] {
        let mut changed_endpoint = replica.clone();
        set_replica_endpoint(
            &mut changed_endpoint,
            role,
            format!("https://rotated-{label}.gateway.example"),
        );
        changed_endpoint.resource_version = ResourceVersion::new(4);
        changed_endpoint.updated_at_unix_ms = UnixMillis::new(202);
        let error = repository
            .replace_replica(3, changed_endpoint)
            .await
            .expect_err("issued Replica endpoint changes must be fenced");
        assert_eq!(error.code(), CentralErrorCode::InvalidState);
    }
    let mut changed_identity = replica.clone();
    changed_identity.credential.public_key_fingerprint =
        Some(ContentDigest::hash(b"different-gateway-key"));
    changed_identity.resource_version = ResourceVersion::new(4);
    changed_identity.updated_at_unix_ms = UnixMillis::new(202);
    let error = repository
        .replace_replica(3, changed_identity)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    let mut changed_certificate = replica.clone();
    changed_certificate.credential.certificate_fingerprint =
        Some(ContentDigest::hash(b"different-gateway-cert"));
    changed_certificate.resource_version = ResourceVersion::new(4);
    changed_certificate.updated_at_unix_ms = UnixMillis::new(202);
    let error = repository
        .replace_replica(3, changed_certificate)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);
    let mut cleared_certificate = replica.clone();
    cleared_certificate.credential.certificate = None;
    cleared_certificate.resource_version = ResourceVersion::new(4);
    cleared_certificate.updated_at_unix_ms = UnixMillis::new(202);
    let error = repository
        .replace_replica(3, cleared_certificate)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    let mut observed = replica.clone();
    observed.last_heartbeat_at_unix_ms = Some(UnixMillis::new(202));
    observed.resource_version = ResourceVersion::new(4);
    observed.updated_at_unix_ms = UnixMillis::new(202);
    let observed = repository.replace_replica(3, observed).await.unwrap();
    let mut cleared_heartbeat = observed.clone();
    cleared_heartbeat.last_heartbeat_at_unix_ms = None;
    cleared_heartbeat.resource_version = ResourceVersion::new(5);
    cleared_heartbeat.updated_at_unix_ms = UnixMillis::new(203);
    let error = repository
        .replace_replica(4, cleared_heartbeat)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);
    replica = observed;
    assert_eq!(
        repository
            .list_replicas(&GatewayReplicaListRequest {
                gateway_pool_id: pool_id(),
                state: Some(GatewayReplicaState::Active),
                after: None,
                limit: 10,
            })
            .await
            .unwrap(),
        [replica.clone()]
    );

    let before_session = acquire_request(
        "route-before-session",
        "connection-before-session",
        1,
        900,
        30_900,
    );
    let error = repository
        .acquire_agent_route(before_session.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteFenced);
    let mut unknown_agent = before_session.clone();
    unknown_agent.agent_id = AgentId::new("agent-missing").unwrap();
    let error = repository
        .acquire_agent_route(unknown_agent)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);
    let mut cross_cluster = before_session.clone();
    cross_cluster.edge_cluster_id = EdgeClusterId::new("cluster-b").unwrap();
    let error = repository
        .acquire_agent_route(cross_cluster)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);
    let mut rejected_atomic = session_route_request(
        "route-rejected-atomic",
        "connection-rejected-atomic",
        "boot-rejected",
        approved.resource_version,
        950,
        30_950,
    );
    rejected_atomic.gateway_replica_id = GatewayReplicaId::new("replica-revoked").unwrap();
    let error = repository
        .acquire_agent_session_route(rejected_atomic)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);
    assert_eq!(
        agent_repository
            .get_by_agent(&agent_id())
            .await
            .unwrap()
            .unwrap(),
        approved
    );

    let first_request = session_route_request(
        "route-acquire-1",
        "connection-1",
        "boot-1",
        approved.resource_version,
        1_000,
        31_000,
    );
    let first = repository
        .acquire_agent_session_route(first_request.clone())
        .await
        .unwrap();
    assert_eq!(first.session.session_generation, SessionGeneration::new(1));
    let acquired = first.route;
    assert!(!acquired.replayed);
    assert!(acquired.fenced.is_none());
    assert_eq!(acquired.lease.route_generation, RouteGeneration::new(1));
    let mut inactive_replica = acquire_request(
        "route-inactive-replica",
        "connection-inactive",
        1,
        1_001,
        31_001,
    );
    inactive_replica.gateway_replica_id = GatewayReplicaId::new("replica-revoked").unwrap();
    let error = repository
        .acquire_agent_route(inactive_replica)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::GatewayRouteUnavailable);
    let replayed = repository
        .acquire_agent_session_route(first_request)
        .await
        .unwrap();
    assert!(replayed.session.replayed);
    assert!(replayed.route.replayed);
    assert_eq!(replayed.route.lease, acquired.lease);

    let conflict = repository
        .acquire_agent_route(acquire_request(
            "route-acquire-conflict",
            "connection-conflict",
            1,
            1_100,
            31_100,
        ))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), CentralErrorCode::GatewayRouteUnavailable);
    assert!(conflict.retryable());

    let takeover_request = session_route_request(
        "route-acquire-2",
        "connection-2",
        "boot-2",
        first.session.record.resource_version,
        1_100,
        31_100,
    );
    let takeover = repository
        .acquire_agent_session_route(takeover_request.clone())
        .await
        .unwrap();
    assert_eq!(
        takeover.session.session_generation,
        SessionGeneration::new(2)
    );
    assert_eq!(
        takeover.route.lease.route_generation,
        RouteGeneration::new(2)
    );
    assert_eq!(takeover.route.fenced, Some(acquired.lease));

    let stale_renew = repository
        .renew_agent_route(RenewAgentRouteLeaseRequest {
            request_id: request_id("route-renew-stale"),
            agent_id: agent_id(),
            gateway_replica_id: replica_id(),
            connection_id: connection_id("connection-1"),
            session_generation: SessionGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            renewed_at_unix_ms: UnixMillis::new(2_000),
            lease_expires_at_unix_ms: UnixMillis::new(32_000),
        })
        .await
        .unwrap_err();
    assert_eq!(stale_renew.code(), CentralErrorCode::GatewayRouteFenced);

    let renew = RenewAgentRouteLeaseRequest {
        request_id: request_id("route-renew-2"),
        agent_id: agent_id(),
        gateway_replica_id: replica_id(),
        connection_id: connection_id("connection-2"),
        session_generation: SessionGeneration::new(2),
        route_generation: RouteGeneration::new(2),
        renewed_at_unix_ms: UnixMillis::new(10_000),
        lease_expires_at_unix_ms: UnixMillis::new(40_000),
    };
    let renewed = repository.renew_agent_route(renew.clone()).await.unwrap();
    assert!(!renewed.replayed);
    assert!(repository.renew_agent_route(renew).await.unwrap().replayed);
    let replayed_acquire = repository
        .acquire_agent_session_route(takeover_request.clone())
        .await
        .unwrap();
    assert!(replayed_acquire.session.replayed);
    assert!(replayed_acquire.route.replayed);
    assert_eq!(replayed_acquire.route.lease, renewed.lease);
    let mut changed_acquire = takeover_request;
    changed_acquire.lease_expires_at_unix_ms = UnixMillis::new(31_000);
    let error = repository
        .acquire_agent_session_route(changed_acquire)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ConcurrentUpdate);

    let early_release = ReleaseAgentRouteLeaseRequest {
        request_id: request_id("route-release-too-early"),
        agent_id: agent_id(),
        gateway_replica_id: replica_id(),
        connection_id: connection_id("connection-2"),
        session_generation: SessionGeneration::new(2),
        route_generation: RouteGeneration::new(2),
        released_at_unix_ms: UnixMillis::new(9_999),
    };
    let error = repository
        .release_agent_route(early_release)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);

    let release = ReleaseAgentRouteLeaseRequest {
        request_id: request_id("route-release-2"),
        agent_id: agent_id(),
        gateway_replica_id: replica_id(),
        connection_id: connection_id("connection-2"),
        session_generation: SessionGeneration::new(2),
        route_generation: RouteGeneration::new(2),
        released_at_unix_ms: UnixMillis::new(12_000),
    };
    let released = repository
        .release_agent_route(release.clone())
        .await
        .unwrap();
    assert!(!released.replayed);
    assert_eq!(
        released.lease.released_at_unix_ms,
        Some(UnixMillis::new(12_000))
    );
    assert!(
        repository
            .release_agent_route(release)
            .await
            .unwrap()
            .replayed
    );

    let third = repository
        .acquire_agent_route(acquire_request(
            "route-acquire-3",
            "connection-3",
            2,
            13_000,
            43_000,
        ))
        .await
        .unwrap();
    assert_eq!(third.lease.route_generation, RouteGeneration::new(3));
    assert_eq!(third.fenced, Some(released.lease));

    let active = repository
        .list_agent_routes(&AgentRouteLeaseListRequest {
            gateway_pool_id: pool_id(),
            gateway_replica_id: Some(replica_id()),
            active_at_unix_ms: Some(UnixMillis::new(13_001)),
            after: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(active.as_slice(), std::slice::from_ref(&third.lease));

    let fourth = repository
        .acquire_agent_route(acquire_request(
            "route-acquire-4",
            "connection-4",
            2,
            44_000,
            74_000,
        ))
        .await
        .unwrap();
    assert_eq!(fourth.lease.route_generation, RouteGeneration::new(4));
    assert_eq!(fourth.fenced, Some(third.lease));

    // Route renewal must consult the current Agent aggregate. Closing the authoritative session
    // fences an otherwise unexpired Gateway lease immediately.
    let current_agent = agent_repository
        .get_by_agent(&agent_id())
        .await
        .unwrap()
        .unwrap();
    AgentRegistryService::new(
        agent_repository.clone(),
        Arc::new(InMemoryClock::new(50_000)),
        50,
    )
    .close_session(CloseAgentSessionRequest {
        agent_id: agent_id(),
        boot_id: AgentBootId::new("boot-2").unwrap(),
        session_generation: SessionGeneration::new(2),
        expected_resource_version: current_agent.resource_version,
    })
    .await
    .unwrap();
    let post_close_renew = repository
        .renew_agent_route(RenewAgentRouteLeaseRequest {
            request_id: request_id("route-renew-after-session-close"),
            agent_id: agent_id(),
            gateway_replica_id: replica_id(),
            connection_id: connection_id("connection-4"),
            session_generation: SessionGeneration::new(2),
            route_generation: RouteGeneration::new(4),
            renewed_at_unix_ms: UnixMillis::new(45_000),
            lease_expires_at_unix_ms: UnixMillis::new(75_000),
        })
        .await
        .unwrap_err();
    assert_eq!(
        post_close_renew.code(),
        CentralErrorCode::GatewayRouteFenced
    );
    let post_close_release = repository
        .release_agent_route(ReleaseAgentRouteLeaseRequest {
            request_id: request_id("route-release-after-session-close"),
            agent_id: agent_id(),
            gateway_replica_id: replica_id(),
            connection_id: connection_id("connection-4"),
            session_generation: SessionGeneration::new(2),
            route_generation: RouteGeneration::new(4),
            released_at_unix_ms: UnixMillis::new(45_000),
        })
        .await
        .unwrap_err();
    assert_eq!(
        post_close_release.code(),
        CentralErrorCode::GatewayRouteFenced
    );

    ContractResult {
        pool,
        replica,
        route: fourth.lease,
    }
}

async fn setup_owner_route(
    repository: Arc<dyn GatewayRegistryRepository>,
    agent_repository: Arc<dyn AgentRegistryRepository>,
    certificate_not_after: u64,
) -> AgentRouteLease {
    let approved = approve_agent(agent_repository).await;
    let initial_pool = pool_record("pool-a", "cluster-a");
    repository.insert_pool(initial_pool).await.unwrap();
    let mut pool = repository.get_pool(&pool_id()).await.unwrap().unwrap();
    pool.state = GatewayPoolState::Ready;
    pool.config_generation = Generation::new(2);
    pool.resource_version = ResourceVersion::new(2);
    pool.updated_at_unix_ms = UnixMillis::new(110);
    pool.updated_by = principal("gateway-owner-fence-test");
    repository.replace_pool(1, pool).await.unwrap();

    let mut replica = replica_record();
    repository.insert_replica(replica.clone()).await.unwrap();
    let public_key = Ed25519PublicKeySpki::from_public_key_bytes([7; 32]);
    let leaf_certificate = GatewayOpaqueBytes::new(b"gateway-owner-fence-cert".to_vec()).unwrap();
    replica.credential.public_key_fingerprint = Some(public_key.fingerprint());
    replica.credential.certificate_generation = Some(CertificateGeneration::new(1));
    replica.credential.certificate_fingerprint =
        Some(ContentDigest::hash(leaf_certificate.as_bytes()));
    replica.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(certificate_not_after));
    replica.credential.certificate = Some(GatewayReplicaCertificateRecord {
        request_id: request_id("gateway-owner-fence-certificate"),
        public_key_spki: public_key,
        certificate_generation: CertificateGeneration::new(1),
        not_before_unix_ms: UnixMillis::new(200),
        not_after_unix_ms: UnixMillis::new(certificate_not_after),
        server_names: BTreeSet::new(),
        leaf_certificate_der: leaf_certificate,
        issuer_chain_der: vec![
            GatewayOpaqueBytes::new(b"gateway-owner-fence-issuer".to_vec()).unwrap(),
        ],
    });
    replica.resource_version = ResourceVersion::new(2);
    replica.updated_at_unix_ms = UnixMillis::new(200);
    replica.credential.state = GatewayCredentialState::PendingCertificateDelivery;
    repository.replace_replica(1, replica).await.unwrap();
    let mut replica = repository
        .get_replica(&replica_id())
        .await
        .unwrap()
        .unwrap();
    replica.state = GatewayReplicaState::Active;
    replica.credential.state = GatewayCredentialState::Active;
    replica.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(201));
    replica.resource_version = ResourceVersion::new(3);
    replica.updated_at_unix_ms = UnixMillis::new(201);
    repository.replace_replica(2, replica).await.unwrap();

    repository
        .acquire_agent_session_route(session_route_request(
            "owner-fence-acquire",
            "owner-fence-connection",
            "owner-fence-boot",
            approved.resource_version,
            1_000,
            3_000,
        ))
        .await
        .unwrap()
        .route
        .lease
}

async fn activate_gateway_replica(
    repository: Arc<dyn GatewayRegistryRepository>,
    replica: GatewayReplicaRecord,
) -> GatewayReplicaRecord {
    repository.insert_replica(replica.clone()).await.unwrap();
    let mut replica = prepared_gateway_replica(replica);
    replica = repository.replace_replica(1, replica).await.unwrap();

    replica.state = GatewayReplicaState::Active;
    replica.credential.state = GatewayCredentialState::Active;
    replica.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(201));
    replica.resource_version = ResourceVersion::new(3);
    replica.updated_at_unix_ms = UnixMillis::new(201);
    repository.replace_replica(2, replica).await.unwrap()
}

fn prepared_gateway_replica(mut replica: GatewayReplicaRecord) -> GatewayReplicaRecord {
    let public_key = Ed25519PublicKeySpki::from_public_key_bytes([8; 32]);
    let leaf_certificate = GatewayOpaqueBytes::new(
        format!("{}-certificate", replica.gateway_replica_id.as_str()).into_bytes(),
    )
    .unwrap();
    replica.credential.public_key_fingerprint = Some(public_key.fingerprint());
    replica.credential.certificate_generation = Some(CertificateGeneration::new(1));
    replica.credential.certificate_fingerprint =
        Some(ContentDigest::hash(leaf_certificate.as_bytes()));
    replica.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(20_000));
    replica.credential.certificate = Some(GatewayReplicaCertificateRecord {
        request_id: RequestId::new(format!(
            "{}-activation-certificate",
            replica.gateway_replica_id.as_str()
        ))
        .unwrap(),
        public_key_spki: public_key,
        certificate_generation: CertificateGeneration::new(1),
        not_before_unix_ms: UnixMillis::new(200),
        not_after_unix_ms: UnixMillis::new(20_000),
        server_names: BTreeSet::new(),
        leaf_certificate_der: leaf_certificate,
        issuer_chain_der: vec![
            GatewayOpaqueBytes::new(b"dual-replica-test-issuer".to_vec()).unwrap(),
        ],
    });
    replica.resource_version = ResourceVersion::new(2);
    replica.updated_at_unix_ms = UnixMillis::new(200);
    replica.credential.state = GatewayCredentialState::PendingCertificateDelivery;
    replica
}

fn renew_owner_request(
    request: &str,
    renewed_at: u64,
    lease_expires_at: u64,
) -> RenewAgentRouteLeaseRequest {
    RenewAgentRouteLeaseRequest {
        request_id: request_id(request),
        agent_id: agent_id(),
        gateway_replica_id: replica_id(),
        connection_id: connection_id("owner-fence-connection"),
        session_generation: SessionGeneration::new(1),
        route_generation: RouteGeneration::new(1),
        renewed_at_unix_ms: UnixMillis::new(renewed_at),
        lease_expires_at_unix_ms: UnixMillis::new(lease_expires_at),
    }
}

async fn assert_contract_state(
    repository: &Arc<dyn GatewayRegistryRepository>,
    agent_repository: &Arc<dyn AgentRegistryRepository>,
    result: &ContractResult,
) {
    assert_eq!(
        repository.get_pool(&pool_id()).await.unwrap(),
        Some(result.pool.clone())
    );
    assert_eq!(
        repository
            .get_pool_by_edge_cluster(&cluster_id())
            .await
            .unwrap(),
        Some(result.pool.clone())
    );
    assert_eq!(
        repository.get_replica(&replica_id()).await.unwrap(),
        Some(result.replica.clone())
    );
    assert_eq!(
        repository.get_agent_route(&agent_id()).await.unwrap(),
        Some(result.route.clone())
    );
    let agent = agent_repository
        .get_by_agent(&agent_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        agent.instance.as_ref().unwrap().session_generation,
        Some(result.route.session_generation)
    );
}

async fn approve_agent(
    repository: Arc<dyn AgentRegistryRepository>,
) -> neoengram_central::AgentRegistryRecord {
    let service = AgentRegistryService::new(repository, Arc::new(InMemoryClock::new(200)), 50);
    service
        .create_token_intent(agent_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent_with_proof(agent_bootstrap_request())
        .await
        .unwrap();
    service
        .decide_enrollment(
            AgentEnrollmentApprovalRequest {
                enrollment_id: enrollment_id(),
                decision_request_id: request_id("gateway-approve-agent"),
                expected_resource_version: pending.record.resource_version,
                decision: AgentEnrollmentDecision::Approve,
                confirm_replacement: false,
                extensions: Extensions::new(),
            },
            principal("gateway-test-operator"),
        )
        .await
        .unwrap()
        .record
}

fn session_route_request(
    route_request: &str,
    connection: &str,
    boot: &str,
    expected_resource_version: ResourceVersion,
    observed_at: u64,
    expires_at: u64,
) -> AcquireAgentSessionRouteRequest {
    AcquireAgentSessionRouteRequest {
        route_request_id: request_id(route_request),
        session: OpenAgentSessionRequest {
            agent_id: agent_id(),
            installation_id: installation_id(),
            boot_id: AgentBootId::new(boot).unwrap(),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version,
        },
        gateway_pool_id: pool_id(),
        gateway_replica_id: replica_id(),
        connection_id: connection_id(connection),
        observed_at_unix_ms: UnixMillis::new(observed_at),
        lease_expires_at_unix_ms: UnixMillis::new(expires_at),
        heartbeat_timeout_ms: 50,
    }
}

fn agent_token_request() -> AgentEnrollmentTokenCreateRequest {
    AgentEnrollmentTokenCreateRequest {
        token_id: AgentEnrollmentTokenId::new("gateway-token-a").unwrap(),
        token_request_id: request_id("gateway-token-request-a"),
        enrollment_id: enrollment_id(),
        tenant_id: TenantId::new("tenant-a").unwrap(),
        edge_cluster_id: cluster_id(),
        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
        volume_descriptor_digest: ContentDigest::hash(b"gateway-volume-descriptor"),
        pvc_identity_digest: PvcIdentityDigest::derive("gateway", "volume-a").unwrap(),
        agent_id: agent_id(),
        agent_mount_id: AgentMountId::new("gateway-mount-a").unwrap(),
        expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
        desired_access_mode: MountAccessMode::ReadWrite,
        bootstrap_token: "gateway-bootstrap-token-with-at-least-32-bytes".to_owned(),
        created_at_unix_ms: UnixMillis::new(100),
        expires_at_unix_ms: UnixMillis::new(900_100),
        extensions: Extensions::new(),
    }
}

fn agent_bootstrap_request() -> AgentBootstrapRequest {
    let key_pair = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
    let public_key = key_pair.public_key().as_ref().try_into().unwrap();
    let proof = AgentBootstrapProof::new(
        Ed25519PublicKeySpki::from_public_key_bytes(public_key),
        Ed25519Signature::from_bytes([0; 64]),
    );
    let mut request = AgentBootstrapRequest {
        bootstrap_request_id: request_id("gateway-bootstrap-request-a"),
        bootstrap_token: "gateway-bootstrap-token-with-at-least-32-bytes".to_owned(),
        installation_id: installation_id(),
        tenant_id: TenantId::new("tenant-a").unwrap(),
        edge_cluster_id: cluster_id(),
        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
        volume_descriptor_digest: ContentDigest::hash(b"gateway-volume-descriptor"),
        agent_version: "0.2.0".to_owned(),
        wire_version: CURRENT_WIRE_VERSION,
        capabilities: vec!["single_volume_v1".to_owned()],
        public_key_fingerprint: proof.public_key_fingerprint(),
        proof,
        probe: AgentBootstrapProbe {
            observed_volume_marker: Some(VolumeMarkerId::new("volume-a").unwrap()),
            marker_matches: true,
            mount_boundary_detected: true,
            mount_identity_digest: mount_identity_digest(),
            access_mode: Some(MountAccessMode::ReadWrite),
            rename_supported: true,
            fsync_supported: true,
            health: ResourceHealth::Ready,
            observed_at_unix_ms: UnixMillis::new(199),
            extensions: Extensions::new(),
        },
        extensions: Extensions::new(),
    };
    let signature = key_pair.sign(&request.signing_bytes().unwrap());
    request.proof.signature = Ed25519Signature::new(signature.as_ref().to_vec()).unwrap();
    request.verify().unwrap();
    request
}

fn enrollment_id() -> AgentEnrollmentId {
    AgentEnrollmentId::new("gateway-enrollment-a").unwrap()
}

fn installation_id() -> AgentInstallationId {
    AgentInstallationId::new("gateway-installation-a").unwrap()
}

fn mount_identity_digest() -> AgentMountIdentityDigest {
    AgentMountIdentityDigest::new(ContentDigest::hash(b"gateway-mount-identity"))
}

fn pool_record(pool: &str, cluster: &str) -> GatewayPoolRecord {
    GatewayPoolRecord {
        gateway_pool_id: GatewayPoolId::new(pool).unwrap(),
        edge_cluster_id: EdgeClusterId::new(cluster).unwrap(),
        display_name: "Primary Gateway".to_owned(),
        agent_endpoint: format!("https://{pool}.agent.example"),
        s3_endpoint: None,
        desired_replicas: 2,
        minimum_ready_replicas: 1,
        state: GatewayPoolState::Provisioning,
        config_generation: Generation::new(1),
        resource_version: ResourceVersion::new(1),
        created_at_unix_ms: UnixMillis::new(100),
        updated_at_unix_ms: UnixMillis::new(100),
        created_by: principal("gateway-operator-a"),
        updated_by: principal("gateway-operator-a"),
    }
}

fn replica_record() -> GatewayReplicaRecord {
    GatewayReplicaRecord {
        gateway_replica_id: replica_id(),
        gateway_pool_id: pool_id(),
        edge_cluster_id: cluster_id(),
        control_endpoint: "https://replica-a.control.example".to_owned(),
        peer_endpoint: "https://replica-a.peer.example".to_owned(),
        bootstrap_endpoint: "https://replica-a.bootstrap.example".to_owned(),
        software_version: "0.2.0".to_owned(),
        wire_version: CURRENT_WIRE_VERSION,
        capabilities: BTreeSet::from(["agent_control_v1".to_owned()]),
        last_heartbeat_at_unix_ms: None,
        state: GatewayReplicaState::Pending,
        credential: GatewayReplicaCredential {
            activation_token_digest: ContentDigest::hash(b"activation-token-a"),
            activation_created_at_unix_ms: UnixMillis::new(100),
            activation_expires_at_unix_ms: UnixMillis::new(900_100),
            activation_consumed_at_unix_ms: None,
            public_key_fingerprint: None,
            certificate_generation: None,
            certificate_fingerprint: None,
            certificate_not_after_unix_ms: None,
            certificate: None,
            state: GatewayCredentialState::PendingActivation,
        },
        resource_version: ResourceVersion::new(1),
        created_at_unix_ms: UnixMillis::new(100),
        updated_at_unix_ms: UnixMillis::new(100),
    }
}

fn replica_record_for(id: &str) -> GatewayReplicaRecord {
    let mut replica = replica_record();
    replica.gateway_replica_id = GatewayReplicaId::new(id).unwrap();
    replica.control_endpoint = format!("https://{id}.control.example");
    replica.peer_endpoint = format!("https://{id}.peer.example");
    replica.bootstrap_endpoint = format!("https://{id}.bootstrap.example");
    replica.credential.activation_token_digest =
        ContentDigest::hash(format!("activation-token-{id}").as_bytes());
    replica
}

fn replica_endpoint(replica: &GatewayReplicaRecord, role: usize) -> &str {
    match role {
        0 => &replica.control_endpoint,
        1 => &replica.peer_endpoint,
        2 => &replica.bootstrap_endpoint,
        _ => unreachable!("endpoint role is bounded by the contract test"),
    }
}

fn set_replica_endpoint(replica: &mut GatewayReplicaRecord, role: usize, endpoint: String) {
    match role {
        0 => replica.control_endpoint = endpoint,
        1 => replica.peer_endpoint = endpoint,
        2 => replica.bootstrap_endpoint = endpoint,
        _ => unreachable!("endpoint role is bounded by the contract test"),
    }
}

fn acquire_request(
    request: &str,
    connection: &str,
    session_generation: u64,
    acquired_at: u64,
    expires_at: u64,
) -> AcquireAgentRouteLeaseRequest {
    AcquireAgentRouteLeaseRequest {
        request_id: request_id(request),
        agent_id: agent_id(),
        edge_cluster_id: cluster_id(),
        gateway_pool_id: pool_id(),
        gateway_replica_id: replica_id(),
        connection_id: connection_id(connection),
        session_generation: SessionGeneration::new(session_generation),
        acquired_at_unix_ms: UnixMillis::new(acquired_at),
        lease_expires_at_unix_ms: UnixMillis::new(expires_at),
    }
}

fn principal(id: &str) -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new(id).unwrap(),
        extensions: Extensions::new(),
    }
}

fn pool_id() -> GatewayPoolId {
    GatewayPoolId::new("pool-a").unwrap()
}

fn replica_id() -> GatewayReplicaId {
    GatewayReplicaId::new("replica-a").unwrap()
}

fn replica_b_id() -> GatewayReplicaId {
    GatewayReplicaId::new("replica-b").unwrap()
}

fn cluster_id() -> EdgeClusterId {
    EdgeClusterId::new("cluster-a").unwrap()
}

fn agent_id() -> AgentId {
    AgentId::new("agent-a").unwrap()
}

fn connection_id(id: &str) -> GatewayConnectionId {
    GatewayConnectionId::new(id).unwrap()
}

fn request_id(id: &str) -> RequestId {
    RequestId::new(id).unwrap()
}

async fn execute_raw(root: &Path, sql: &str) {
    let options = SqliteConnectOptions::new().filename(root.join("authority.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(sql).execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
}

async fn rewrite_route_expiry_payload(root: &Path, lease_expires_at_unix_ms: u64) {
    let options = SqliteConnectOptions::new().filename(root.join("authority.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let payload: Vec<u8> =
        sqlx::query_scalar("SELECT payload FROM agent_route_leases WHERE agent_id = 'agent-a'")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let mut stored: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    stored["value"]["lease_expires_at_unix_ms"] =
        serde_json::Value::String(lease_expires_at_unix_ms.to_string());
    let payload = serde_json::to_vec(&stored).unwrap();
    sqlx::query(
        "UPDATE agent_route_leases
         SET lease_expires_at_unix_ms = ?, payload = ?
         WHERE agent_id = 'agent-a'",
    )
    .bind(lease_expires_at_unix_ms.to_string())
    .bind(payload)
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
}

async fn rewrite_route_request_id_payload(root: &Path, duplicate_request_id: &str) {
    let options = SqliteConnectOptions::new().filename(root.join("authority.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let payload: Vec<u8> =
        sqlx::query_scalar("SELECT payload FROM agent_route_leases WHERE agent_id = 'agent-a'")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let mut stored: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    stored["value"]["last_renew_request_id"] =
        serde_json::Value::String(duplicate_request_id.to_owned());
    let payload = serde_json::to_vec(&stored).unwrap();
    sqlx::query(
        "UPDATE agent_route_leases
         SET last_renew_request_id = ?, payload = ?
         WHERE agent_id = 'agent-a'",
    )
    .bind(duplicate_request_id)
    .bind(payload)
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
}

async fn rewrite_replica_endpoint_payload(
    root: &Path,
    gateway_replica_id: &str,
    column: &str,
    endpoint: &str,
) {
    assert!(matches!(
        column,
        "control_endpoint" | "peer_endpoint" | "bootstrap_endpoint"
    ));
    let options = SqliteConnectOptions::new().filename(root.join("authority.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let payload: Vec<u8> = sqlx::query_scalar(
        "SELECT payload FROM gateway_replica_records WHERE gateway_replica_id = ?",
    )
    .bind(gateway_replica_id)
    .fetch_one(&mut connection)
    .await
    .unwrap();
    let mut stored: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    stored["value"][column] = serde_json::Value::String(endpoint.to_owned());
    let payload = serde_json::to_vec(&stored).unwrap();
    sqlx::query(&format!(
        "UPDATE gateway_replica_records SET {column} = ?, payload = ? \
         WHERE gateway_replica_id = ?"
    ))
    .bind(endpoint)
    .bind(payload)
    .bind(gateway_replica_id)
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
}
