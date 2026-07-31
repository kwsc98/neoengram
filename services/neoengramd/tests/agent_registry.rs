#![cfg(feature = "authority-sqlite")]

use std::{collections::BTreeSet, path::Path, sync::Arc};

use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentBootId, AgentBootstrapProbe, AgentBootstrapRequest, AgentEnrollmentApprovalRequest,
    AgentEnrollmentDecision, AgentEnrollmentId, AgentEnrollmentState,
    AgentEnrollmentTokenCreateRequest, AgentEnrollmentTokenId, AgentId, AgentInstallationId,
    AgentMountId, AgentMountIdentityDigest, AgentMountStatusReport, EdgeClusterId, Extensions,
    MountAccessMode, MountGeneration, OwnerGeneration, PrincipalId, PrincipalKind, PrincipalRef,
    PvcIdentityDigest, RequestId, ResourceHealth, ResourceVersion, SequenceNumber,
    SessionGeneration, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId, PROTOCOL_VERSION_V1,
};
use neoengramd::{
    open_sqlite_agent_registry, open_sqlite_authority, AgentEnrollmentAuditEvent,
    AgentEnrollmentAuditKind, AgentInstanceRecord, AgentInstanceState, AgentRegistryRecord,
    AgentRegistryRepository, AgentRegistryService, CentralErrorCode, CloseAgentSessionRequest,
    CompleteVolumeRecoveryRequest, DerivedVolumeState, ExpireAgentEnrollmentRequest,
    InMemoryAgentRegistry, InMemoryClock, InMemoryComponents, OpenAgentSessionRequest,
    SqliteAgentRegistryConfig, SqliteAuthorityConfig, VolumeOwnerState,
    AGENT_ENROLLMENT_REVIEW_WINDOW_MS,
};
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
use tempfile::TempDir;

const INITIAL_TOKEN: &str = "initial-bootstrap-token-with-at-least-32-bytes";
const REPLACEMENT_TOKEN: &str = "replacement-bootstrap-token-with-32-bytes";
const INDEPENDENT_TOKEN: &str = "independent-bootstrap-token-with-at-least-32-bytes";

#[derive(Debug)]
struct LifecycleResult {
    current: AgentRegistryRecord,
    revoked: AgentRegistryRecord,
}

#[tokio::test]
async fn one_volume_replacement_matches_in_memory_and_sqlite() {
    let memory_repository: Arc<dyn AgentRegistryRepository> =
        Arc::new(InMemoryAgentRegistry::new());
    let memory_clock = Arc::new(InMemoryClock::new(200));
    run_lifecycle(memory_repository, memory_clock).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    let sqlite_clock = Arc::new(InMemoryClock::new(200));
    let result = run_lifecycle(sqlite.repository(), sqlite_clock).await;
    sqlite.integrity_check().await.unwrap();
    drop(sqlite);

    let bytes = std::fs::read(directory.path().join("agent-registry.sqlite3")).unwrap();
    for token in [INITIAL_TOKEN, REPLACEMENT_TOKEN] {
        assert!(!bytes
            .windows(token.len())
            .any(|window| window == token.as_bytes()));
    }

    let reopened = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    let stored_current = reopened
        .repository()
        .get(&replacement_enrollment_id())
        .await
        .unwrap()
        .unwrap();
    let stored_revoked = reopened
        .repository()
        .get(&initial_enrollment_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_current, result.current);
    assert_eq!(stored_revoked, result.revoked);
}

#[tokio::test]
async fn sqlite_registry_rejects_schema_drift_on_reopen() {
    for mutation in [
        "PRAGMA application_id = 305419896",
        "PRAGMA user_version = 99",
        "DROP INDEX agent_registry_public_key_identity",
        "ALTER TABLE agent_registry_records ADD COLUMN unexpected TEXT",
    ] {
        let directory = TempDir::new().unwrap();
        drop(
            open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
                .await
                .unwrap(),
        );
        execute_registry_raw(directory.path(), mutation).await;

        let error = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
            .await
            .err()
            .expect("schema drift must prevent reopening the Agent registry");
        assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    }
}

#[test]
fn sqlite_registry_config_debug_redacts_physical_path() {
    let config = SqliteAgentRegistryConfig::new("/secret/registry/location");
    let debug = format!("{config:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("/secret/registry/location"));
}

#[tokio::test]
async fn registry_debug_redacts_bootstrap_token_verifier() {
    let service = AgentRegistryService::new(
        Arc::new(InMemoryAgentRegistry::new()),
        Arc::new(InMemoryClock::new(100)),
        100,
    );
    let created = service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let verifier = created.record.enrollment.bootstrap_token_digest.to_string();
    let debug = format!("{:?}", created.record);
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(INITIAL_TOKEN));
    assert!(!debug.contains(&verifier));
}

#[tokio::test]
async fn sqlite_registry_rejects_corrupt_pvc_index_columns_on_reopen() {
    for mutation in [
        "UPDATE agent_registry_records SET edge_cluster_id = 'cluster-corrupt'",
        "UPDATE agent_registry_records SET pvc_identity_digest = \
         'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'",
        "UPDATE agent_registry_records SET pvc_binding_role = 'replacement'",
    ] {
        let directory = TempDir::new().unwrap();
        let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
            .await
            .unwrap();
        let service =
            AgentRegistryService::new(sqlite.repository(), Arc::new(InMemoryClock::new(100)), 100);
        service
            .create_token_intent(initial_token_request())
            .await
            .unwrap();
        drop(service);
        drop(sqlite);
        execute_registry_raw(directory.path(), mutation).await;

        let error = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
            .await
            .err()
            .expect("corrupt PVC index column must fail closed during reopen");
        assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    }
}

#[tokio::test]
async fn sqlite_registry_rejects_corrupt_optional_indexes_on_reopen() {
    for mutation in [
        "UPDATE agent_registry_records SET bootstrap_request_id = 'bootstrap-corrupt'",
        "UPDATE agent_registry_records SET decision_request_id = 'decision-corrupt'",
        "UPDATE agent_registry_records SET installation_id = 'installation-corrupt'",
        "UPDATE agent_registry_records SET public_key_fingerprint = 'fingerprint-corrupt'",
    ] {
        let directory = TempDir::new().unwrap();
        let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
            .await
            .unwrap();
        approve_initial_repository(sqlite.repository()).await;
        drop(sqlite);
        execute_registry_raw(directory.path(), mutation).await;

        let error = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
            .await
            .err()
            .expect("corrupt optional index must fail closed during reopen");
        assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    }
}

#[tokio::test]
async fn sqlite_registry_rejects_corrupt_identity_and_session_payload_on_reopen() {
    for mutation in [
        "UPDATE agent_registry_records SET payload = CAST(json_set(CAST(payload AS TEXT), \
         '$.value.candidate.agent_version', 'tampered') AS BLOB)",
        "UPDATE agent_registry_records SET payload = CAST(json_set(CAST(payload AS TEXT), \
         '$.value.instance.session_opened_at_unix_ms', 201) AS BLOB)",
        "UPDATE agent_registry_records SET payload = CAST(json_set(CAST(payload AS TEXT), \
         '$.value.instance.state', 'Revoked') AS BLOB)",
    ] {
        let directory = TempDir::new().unwrap();
        let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
            .await
            .unwrap();
        approve_initial_repository(sqlite.repository()).await;
        drop(sqlite);
        execute_registry_raw(directory.path(), mutation).await;

        let error = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
            .await
            .err()
            .expect("corrupt registry payload must fail closed during reopen");
        assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    }
}

#[tokio::test]
async fn direct_repository_guards_match_in_memory_and_sqlite() {
    assert_direct_repository_guards(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_direct_repository_guards(sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
}

#[tokio::test]
async fn replacement_transition_guards_match_in_memory_and_sqlite() {
    assert_replacement_transition_guards(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_replacement_transition_guards(sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
}

#[tokio::test]
async fn resource_version_exhaustion_is_atomic_in_both_backends() {
    assert_resource_version_exhaustion_is_atomic(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_resource_version_exhaustion_is_atomic(sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
}

#[tokio::test]
async fn authority_integrity_check_includes_the_agent_registry_database() {
    let directory = TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    authority.integrity_check().await.unwrap();

    execute_registry_raw(
        directory.path(),
        "DROP INDEX agent_registry_public_key_identity",
    )
    .await;
    assert_eq!(
        authority.integrity_check().await.unwrap_err().code(),
        CentralErrorCode::StorageFailure
    );
}

#[tokio::test]
async fn token_scope_and_bootstrap_request_identity_are_bound() {
    let repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let clock = Arc::new(InMemoryClock::new(200));
    let service = AgentRegistryService::new(repository, clock, 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();

    let mut wrong_token = initial_bootstrap_request();
    wrong_token.bootstrap_token = "another-bootstrap-token-with-32-bytes".to_owned();
    assert_eq!(
        service
            .bootstrap_agent(wrong_token)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::BootstrapDenied
    );

    let mut wrong_scope = initial_bootstrap_request();
    wrong_scope.storage_volume_id = StorageVolumeId::new("volume-b").unwrap();
    assert_eq!(
        service
            .bootstrap_agent(wrong_scope)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );
    let mut wrong_tenant = initial_bootstrap_request();
    wrong_tenant.tenant_id = TenantId::new("tenant-b").unwrap();
    assert_eq!(
        service
            .bootstrap_agent(wrong_tenant)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );
    let mut wrong_descriptor = initial_bootstrap_request();
    wrong_descriptor.volume_descriptor_digest = ContentDigest::hash(b"another-descriptor");
    assert_eq!(
        service
            .bootstrap_agent(wrong_descriptor)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );

    let accepted = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    assert_eq!(
        accepted.record.enrollment.state,
        AgentEnrollmentState::PendingApproval
    );
    assert!(accepted.record.candidate.is_some());
    assert!(accepted.record.instance.is_none());
    assert_eq!(accepted.record.owner.state, VolumeOwnerState::Inactive);

    let replay = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    assert!(replay.accepted.replayed);
    let token_replay = service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    assert!(token_replay.replayed);
    assert_eq!(token_replay.record, accepted.record);

    let mut reused_request = initial_bootstrap_request();
    reused_request.bootstrap_request_id = RequestId::new("bootstrap-other").unwrap();
    reused_request.installation_id = AgentInstallationId::new("installation-other").unwrap();
    assert_eq!(
        service
            .bootstrap_agent(reused_request)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );
}

#[tokio::test]
async fn read_only_token_intent_is_rejected_without_persistence() {
    let repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let service =
        AgentRegistryService::new(repository.clone(), Arc::new(InMemoryClock::new(100)), 100);
    let mut request = initial_token_request();
    request.desired_access_mode = MountAccessMode::ReadOnly;
    assert_eq!(
        service
            .create_token_intent(request)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ProtocolInvalid
    );
    assert!(repository
        .get(&initial_enrollment_id())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn token_and_review_expiry_are_independent() {
    let repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let clock = Arc::new(InMemoryClock::new(200));
    let service = AgentRegistryService::new(repository.clone(), clock.clone(), 100);
    let mut request = initial_token_request();
    request.expires_at_unix_ms = UnixMillis::new(250);
    service.create_token_intent(request).await.unwrap();
    let bootstrapped = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    assert_eq!(
        bootstrapped.accepted.review_expires_at_unix_ms,
        UnixMillis::new(200 + AGENT_ENROLLMENT_REVIEW_WINDOW_MS)
    );

    clock.set(251);
    service
        .decide_enrollment(
            approval_request(
                initial_enrollment_id(),
                bootstrapped.record.resource_version,
                false,
            ),
            actor(),
        )
        .await
        .unwrap();

    let second_repository: Arc<dyn AgentRegistryRepository> =
        Arc::new(InMemoryAgentRegistry::new());
    let second_clock = Arc::new(InMemoryClock::new(200));
    let second_service =
        AgentRegistryService::new(second_repository.clone(), second_clock.clone(), 100);
    second_service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let pending = second_service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    assert_eq!(
        second_service
            .expire_enrollment(ExpireAgentEnrollmentRequest {
                enrollment_id: initial_enrollment_id(),
                expected_resource_version: pending.record.resource_version,
            })
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::InvalidState
    );
    second_clock.set(200 + AGENT_ENROLLMENT_REVIEW_WINDOW_MS);
    let expired = second_service
        .expire_enrollment(ExpireAgentEnrollmentRequest {
            enrollment_id: initial_enrollment_id(),
            expected_resource_version: pending.record.resource_version,
        })
        .await
        .unwrap();
    assert_eq!(expired.enrollment.state, AgentEnrollmentState::Expired);

    let third_repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let third_clock = Arc::new(InMemoryClock::new(200));
    let third_service =
        AgentRegistryService::new(third_repository.clone(), third_clock.clone(), 100);
    third_service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let third_pending = third_service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    third_clock.set(200 + AGENT_ENROLLMENT_REVIEW_WINDOW_MS);
    assert_eq!(
        third_service
            .decide_enrollment(
                approval_request(
                    initial_enrollment_id(),
                    third_pending.record.resource_version,
                    false,
                ),
                actor(),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::EnrollmentExpired
    );
    assert_eq!(
        third_repository
            .get(&initial_enrollment_id())
            .await
            .unwrap()
            .unwrap()
            .enrollment
            .state,
        AgentEnrollmentState::Expired
    );
}

#[tokio::test]
async fn expired_token_intent_releases_volume_and_pvc_scope() {
    assert_expired_token_releases_scope(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_expired_token_releases_scope(sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
    drop(sqlite);

    let reopened = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .repository()
            .get(&initial_enrollment_id())
            .await
            .unwrap()
            .unwrap()
            .enrollment
            .state,
        AgentEnrollmentState::Expired
    );
}

async fn assert_expired_token_releases_scope(repository: Arc<dyn AgentRegistryRepository>) {
    let clock = Arc::new(InMemoryClock::new(300));
    let service = AgentRegistryService::new(repository.clone(), clock, 100);
    let mut expired = initial_token_request();
    expired.expires_at_unix_ms = UnixMillis::new(250);
    service.create_token_intent(expired).await.unwrap();
    let mut renewed = independent_token_request();
    renewed.tenant_id = tenant_id();
    renewed.edge_cluster_id = edge_cluster_id();
    renewed.storage_volume_id = storage_volume_id();
    renewed.volume_descriptor_digest = volume_descriptor_digest();
    renewed.pvc_identity_digest = pvc_identity_digest();
    renewed.expected_volume_marker = VolumeMarkerId::new("volume-a").unwrap();
    renewed.created_at_unix_ms = UnixMillis::new(300);
    renewed.expires_at_unix_ms = UnixMillis::new(1_200);
    assert!(!service.create_token_intent(renewed).await.unwrap().replayed);
    assert_eq!(
        repository
            .get(&initial_enrollment_id())
            .await
            .unwrap()
            .unwrap()
            .enrollment
            .state,
        AgentEnrollmentState::Expired
    );
    assert_eq!(
        service
            .bootstrap_agent(initial_bootstrap_request())
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::BootstrapDenied
    );
}

#[tokio::test]
async fn review_expiry_shape_is_sqlite_restart_stable() {
    let directory = TempDir::new().unwrap();
    let clock = Arc::new(InMemoryClock::new(200));
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    let service = AgentRegistryService::new(sqlite.repository(), clock.clone(), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    clock.set(200 + AGENT_ENROLLMENT_REVIEW_WINDOW_MS);
    let expired = service
        .expire_enrollment(ExpireAgentEnrollmentRequest {
            enrollment_id: initial_enrollment_id(),
            expected_resource_version: pending.record.resource_version,
        })
        .await
        .unwrap();
    assert!(expired.candidate.is_some());
    drop(service);
    drop(sqlite);

    let reopened = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    let stored = reopened
        .repository()
        .get(&initial_enrollment_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.enrollment.state, AgentEnrollmentState::Expired);
    assert!(stored.candidate.is_some());
    assert!(stored.enrollment.review_expires_at_unix_ms.is_some());
}

#[tokio::test]
async fn expired_token_cannot_create_a_candidate() {
    let repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let clock = Arc::new(InMemoryClock::new(200));
    let service = AgentRegistryService::new(repository.clone(), clock.clone(), 100);
    let mut request = initial_token_request();
    request.expires_at_unix_ms = UnixMillis::new(201);
    service.create_token_intent(request).await.unwrap();
    clock.set(201);
    assert_eq!(
        service
            .bootstrap_agent(initial_bootstrap_request())
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::BootstrapDenied
    );
    let stored = repository
        .get(&initial_enrollment_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.enrollment.state, AgentEnrollmentState::Expired);
    assert!(stored.candidate.is_none());
}

#[tokio::test]
async fn approval_fails_closed_on_unsafe_probe_but_allows_degraded_health() {
    let mut unsafe_probes = Vec::new();
    let mut wrong_marker = healthy_bootstrap_probe();
    wrong_marker.observed_volume_marker = Some(VolumeMarkerId::new("volume-b").unwrap());
    unsafe_probes.push(wrong_marker);
    let mut no_boundary = healthy_bootstrap_probe();
    no_boundary.mount_boundary_detected = false;
    unsafe_probes.push(no_boundary);
    let mut read_only = healthy_bootstrap_probe();
    read_only.access_mode = Some(MountAccessMode::ReadOnly);
    unsafe_probes.push(read_only);
    let mut no_rename = healthy_bootstrap_probe();
    no_rename.rename_supported = false;
    unsafe_probes.push(no_rename);
    let mut no_fsync = healthy_bootstrap_probe();
    no_fsync.fsync_supported = false;
    unsafe_probes.push(no_fsync);
    let mut unavailable = healthy_bootstrap_probe();
    unavailable.health = ResourceHealth::Unavailable;
    unsafe_probes.push(unavailable);

    for probe in unsafe_probes {
        let repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
        let service =
            AgentRegistryService::new(repository.clone(), Arc::new(InMemoryClock::new(200)), 100);
        service
            .create_token_intent(initial_token_request())
            .await
            .unwrap();
        let mut bootstrap = initial_bootstrap_request();
        bootstrap.probe = probe;
        let pending = service.bootstrap_agent(bootstrap).await.unwrap();
        assert_eq!(
            service
                .decide_enrollment(
                    approval_request(
                        initial_enrollment_id(),
                        pending.record.resource_version,
                        false,
                    ),
                    actor(),
                )
                .await
                .unwrap_err()
                .code(),
            CentralErrorCode::EnrollmentProbeFailed
        );
        assert_eq!(
            repository
                .get(&initial_enrollment_id())
                .await
                .unwrap()
                .unwrap()
                .enrollment
                .state,
            AgentEnrollmentState::PendingApproval
        );
    }

    let repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let service = AgentRegistryService::new(repository, Arc::new(InMemoryClock::new(200)), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let mut bootstrap = initial_bootstrap_request();
    bootstrap.probe.health = ResourceHealth::Degraded;
    let pending = service.bootstrap_agent(bootstrap).await.unwrap();
    let approved = service
        .decide_enrollment(
            approval_request(
                initial_enrollment_id(),
                pending.record.resource_version,
                false,
            ),
            actor(),
        )
        .await
        .unwrap();
    assert_eq!(
        approved.record.enrollment.state,
        AgentEnrollmentState::Approved
    );
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Unavailable
    );
}

#[tokio::test]
async fn replacement_confirmation_is_rejected_outside_replacement_approval() {
    let repository: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let service =
        AgentRegistryService::new(repository.clone(), Arc::new(InMemoryClock::new(200)), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap()
        .record;

    for (decision, decision_request_id) in [
        (AgentEnrollmentDecision::Approve, "invalid-initial-approve"),
        (AgentEnrollmentDecision::Reject, "invalid-initial-reject"),
    ] {
        let mut request = approval_request(initial_enrollment_id(), pending.resource_version, true);
        request.decision = decision;
        request.decision_request_id = RequestId::new(decision_request_id).unwrap();
        assert_eq!(
            service
                .decide_enrollment(request, actor())
                .await
                .unwrap_err()
                .code(),
            CentralErrorCode::ProtocolInvalid
        );
    }

    assert_eq!(
        repository
            .get(&initial_enrollment_id())
            .await
            .unwrap()
            .unwrap(),
        pending
    );
}

#[tokio::test]
async fn concurrent_approve_and_reject_have_one_winner() {
    run_decision_race(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    run_decision_race(sqlite.repository()).await;
}

async fn run_decision_race(repository: Arc<dyn AgentRegistryRepository>) {
    let clock = Arc::new(InMemoryClock::new(200));
    let service = Arc::new(AgentRegistryService::new(repository.clone(), clock, 100));
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    let approve = approval_request(
        initial_enrollment_id(),
        pending.record.resource_version,
        false,
    );
    let reject = AgentEnrollmentApprovalRequest {
        enrollment_id: initial_enrollment_id(),
        decision_request_id: RequestId::new("reject-enrollment-a").unwrap(),
        expected_resource_version: pending.record.resource_version,
        decision: AgentEnrollmentDecision::Reject,
        confirm_replacement: false,
        extensions: Extensions::new(),
    };
    let (approved, rejected) = tokio::join!(
        service.decide_enrollment(approve, actor()),
        service.decide_enrollment(reject, actor())
    );
    assert_eq!(
        usize::from(approved.is_ok()) + usize::from(rejected.is_ok()),
        1
    );
    let loser = approved.err().or_else(|| rejected.err()).unwrap();
    assert!(matches!(
        loser.code(),
        CentralErrorCode::ConcurrentUpdate | CentralErrorCode::EnrollmentDecisionConflict
    ));
    let state = repository
        .get(&initial_enrollment_id())
        .await
        .unwrap()
        .unwrap()
        .enrollment
        .state;
    assert!(matches!(
        state,
        AgentEnrollmentState::Approved | AgentEnrollmentState::Rejected
    ));
}

#[tokio::test]
async fn concurrent_sqlite_token_intent_is_idempotent() {
    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    let service = Arc::new(AgentRegistryService::new(
        sqlite.repository(),
        Arc::new(InMemoryClock::new(100)),
        100,
    ));
    let (first, second) = tokio::join!(
        service.create_token_intent(initial_token_request()),
        service.create_token_intent(initial_token_request())
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.record, second.record);
    assert_eq!(
        usize::from(first.replayed) + usize::from(second.replayed),
        1
    );
}

#[tokio::test]
async fn token_request_identity_is_tenant_scoped() {
    assert_cross_tenant_token_request(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_cross_tenant_token_request(sqlite.repository()).await;
}

#[tokio::test]
async fn one_pvc_identity_cannot_map_to_different_storage_volumes() {
    assert_pvc_binding_conflict(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_pvc_binding_conflict(sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
}

async fn assert_pvc_binding_conflict(repository: Arc<dyn AgentRegistryRepository>) {
    let service = AgentRegistryService::new(repository, Arc::new(InMemoryClock::new(100)), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let mut conflicting = independent_token_request();
    conflicting.tenant_id = TenantId::new("tenant-b").unwrap();
    conflicting.pvc_identity_digest = pvc_identity_digest();
    let mut another_cluster = conflicting.clone();
    another_cluster.edge_cluster_id = EdgeClusterId::new("cluster-b").unwrap();
    assert_eq!(
        service
            .create_token_intent(conflicting)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::VolumeOwnerConflict
    );
    assert!(
        !service
            .create_token_intent(another_cluster)
            .await
            .unwrap()
            .replayed
    );
}

#[tokio::test]
async fn concurrent_pvc_claim_has_one_winner_in_both_backends() {
    assert_concurrent_pvc_claim(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_concurrent_pvc_claim(sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
}

async fn assert_concurrent_pvc_claim(repository: Arc<dyn AgentRegistryRepository>) {
    let service = Arc::new(AgentRegistryService::new(
        repository,
        Arc::new(InMemoryClock::new(300)),
        100,
    ));
    let mut expired = initial_token_request();
    expired.expires_at_unix_ms = UnixMillis::new(250);
    service.create_token_intent(expired).await.unwrap();

    let mut first = independent_token_request();
    first.storage_volume_id = storage_volume_id();
    first.volume_descriptor_digest = volume_descriptor_digest();
    first.pvc_identity_digest = pvc_identity_digest();
    first.expected_volume_marker = VolumeMarkerId::new("volume-a").unwrap();
    first.created_at_unix_ms = UnixMillis::new(300);
    first.expires_at_unix_ms = UnixMillis::new(1_200);
    let mut second = first.clone();
    second.token_id = AgentEnrollmentTokenId::new("token-d").unwrap();
    second.token_request_id = RequestId::new("create-token-d").unwrap();
    second.enrollment_id = AgentEnrollmentId::new("enrollment-d").unwrap();
    second.agent_id = AgentId::new("agent-d").unwrap();
    second.agent_mount_id = AgentMountId::new("mount-d").unwrap();
    second.bootstrap_token = "fourth-bootstrap-token-with-at-least-32-bytes".to_owned();
    let (first, second) = tokio::join!(
        service.create_token_intent(first),
        service.create_token_intent(second)
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        first.err().or_else(|| second.err()).unwrap().code(),
        CentralErrorCode::VolumeOwnerConflict
    );
}

#[tokio::test]
async fn concurrent_volume_claim_has_one_winner_in_both_backends() {
    assert_concurrent_volume_claim(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_concurrent_volume_claim(sqlite.repository()).await;
    sqlite.integrity_check().await.unwrap();
}

async fn assert_concurrent_volume_claim(repository: Arc<dyn AgentRegistryRepository>) {
    let service = Arc::new(AgentRegistryService::new(
        repository,
        Arc::new(InMemoryClock::new(100)),
        100,
    ));
    let first = initial_token_request();
    let mut second = independent_token_request();
    second.storage_volume_id = storage_volume_id();
    second.expected_volume_marker = VolumeMarkerId::new("volume-a").unwrap();
    let (first, second) = tokio::join!(
        service.create_token_intent(first),
        service.create_token_intent(second)
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        first.err().or_else(|| second.err()).unwrap().code(),
        CentralErrorCode::VolumeOwnerConflict
    );
}

async fn assert_cross_tenant_token_request(repository: Arc<dyn AgentRegistryRepository>) {
    let service = AgentRegistryService::new(repository, Arc::new(InMemoryClock::new(100)), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let mut other = initial_token_request();
    other.tenant_id = TenantId::new("tenant-b").unwrap();
    other.storage_volume_id = StorageVolumeId::new("volume-b").unwrap();
    other.expected_volume_marker = VolumeMarkerId::new("volume-b").unwrap();
    other.token_id = AgentEnrollmentTokenId::new("token-tenant-b").unwrap();
    other.enrollment_id = AgentEnrollmentId::new("enrollment-tenant-b").unwrap();
    other.agent_id = AgentId::new("agent-tenant-b").unwrap();
    other.agent_mount_id = AgentMountId::new("mount-tenant-b").unwrap();
    other.bootstrap_token = "tenant-b-bootstrap-token-with-32-bytes".to_owned();
    other.volume_descriptor_digest = ContentDigest::hash(b"tenant-b-volume-descriptor");
    other.pvc_identity_digest = PvcIdentityDigest::derive("namespace-b", "claim-b").unwrap();
    let created = service.create_token_intent(other).await.unwrap();
    assert!(!created.replayed);
}

#[tokio::test]
async fn repositories_reject_immutable_identity_changes() {
    assert_immutable_identity(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    assert_immutable_identity(sqlite.repository()).await;
}

async fn assert_immutable_identity(repository: Arc<dyn AgentRegistryRepository>) {
    let service =
        AgentRegistryService::new(repository.clone(), Arc::new(InMemoryClock::new(100)), 100);
    let inserted = service
        .create_token_intent(initial_token_request())
        .await
        .unwrap()
        .record;
    let previous = inserted.resource_version.get();
    let mut tampered = inserted;
    tampered.resource_version = neoengram_protocol::ResourceVersion::new(previous + 1);
    tampered.enrollment.token_id = AgentEnrollmentTokenId::new("another-token-id").unwrap();
    assert_eq!(
        repository
            .replace(previous, tampered)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );
}

#[tokio::test]
async fn registry_wide_bootstrap_candidate_and_decision_identities_are_unique() {
    run_registry_identity_conflicts(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    run_registry_identity_conflicts(sqlite.repository()).await;
}

async fn run_registry_identity_conflicts(repository: Arc<dyn AgentRegistryRepository>) {
    let service =
        AgentRegistryService::new(repository.clone(), Arc::new(InMemoryClock::new(400)), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let initial_pending = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    service
        .create_token_intent(independent_token_request())
        .await
        .unwrap();

    let mut reused_bootstrap_id = independent_bootstrap_request();
    reused_bootstrap_id.bootstrap_request_id = bootstrap_request_id(AgentKind::Initial);
    assert_eq!(
        service
            .bootstrap_agent(reused_bootstrap_id)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::EnrollmentIdReused
    );

    let mut reused_installation = independent_bootstrap_request();
    reused_installation.installation_id = initial_installation_id();
    assert_eq!(
        service
            .bootstrap_agent(reused_installation)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );

    let mut reused_key = independent_bootstrap_request();
    reused_key.public_key_fingerprint = public_key_fingerprint(AgentKind::Initial);
    assert_eq!(
        service
            .bootstrap_agent(reused_key)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );

    let independent_pending = service
        .bootstrap_agent(independent_bootstrap_request())
        .await
        .unwrap();
    let initial_approval = approval_request(
        initial_enrollment_id(),
        initial_pending.record.resource_version,
        false,
    );
    service
        .decide_enrollment(initial_approval.clone(), actor())
        .await
        .unwrap();
    let mut conflicting_decision = AgentEnrollmentApprovalRequest {
        enrollment_id: independent_enrollment_id(),
        decision_request_id: initial_approval.decision_request_id,
        expected_resource_version: independent_pending.record.resource_version,
        decision: AgentEnrollmentDecision::Reject,
        confirm_replacement: false,
        extensions: Extensions::new(),
    };
    assert_eq!(
        service
            .decide_enrollment(conflicting_decision.clone(), actor())
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::EnrollmentDecisionConflict
    );
    conflicting_decision.decision_request_id = RequestId::new("reject-enrollment-c").unwrap();
    service
        .decide_enrollment(conflicting_decision, actor())
        .await
        .unwrap();
}

#[tokio::test]
async fn concurrent_sqlite_token_request_identity_conflict_is_stable() {
    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    let service = Arc::new(AgentRegistryService::new(
        sqlite.repository(),
        Arc::new(InMemoryClock::new(100)),
        100,
    ));
    let mut changed = independent_token_request();
    changed.token_request_id = token_request_id(AgentKind::Initial);
    let (first, second) = tokio::join!(
        service.create_token_intent(initial_token_request()),
        service.create_token_intent(changed)
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let loser = first.err().or_else(|| second.err()).unwrap();
    assert_eq!(loser.code(), CentralErrorCode::EnrollmentIdReused);
}

#[tokio::test]
async fn authority_compositions_expose_registry_and_deduplicate_decision_audit() {
    let memory = InMemoryComponents::new(200);
    let memory_store = memory.authority_store();
    assert!(memory_store.agent_registry().is_some());
    let memory_service =
        AgentRegistryService::from_authority(&memory_store, memory.clock.clone(), 100).unwrap();
    approve_initial(&memory_service).await;
    let memory_events = memory
        .agent_registry
        .enrollment_audit_events()
        .await
        .unwrap();
    assert_eq!(memory_events.len(), 1);
    assert_eq!(memory_events[0].kind, AgentEnrollmentAuditKind::Approved);
    assert!(memory_store.capabilities().atomic_agent_registry_audit);

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let sqlite_store = sqlite.authority_store();
    assert!(sqlite_store.agent_registry().is_some());
    let sqlite_service =
        AgentRegistryService::from_authority(&sqlite_store, Arc::new(InMemoryClock::new(200)), 100)
            .unwrap();
    approve_initial(&sqlite_service).await;
    let events = sqlite.enrollment_audit_events().await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, AgentEnrollmentAuditKind::Approved);
    assert!(sqlite_store.capabilities().atomic_agent_registry_audit);
}

async fn approve_initial(service: &AgentRegistryService) {
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    let decision = approval_request(
        initial_enrollment_id(),
        pending.record.resource_version,
        false,
    );
    service
        .decide_enrollment(decision.clone(), actor())
        .await
        .unwrap();
    let replay = service.decide_enrollment(decision, actor()).await.unwrap();
    assert!(replay.replayed);
}

#[tokio::test]
async fn stale_session_recovers_after_crash_without_owner_takeover() {
    run_crash_recovery(Arc::new(InMemoryAgentRegistry::new())).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_agent_registry(SqliteAgentRegistryConfig::new(directory.path()))
        .await
        .unwrap();
    run_crash_recovery(sqlite.repository()).await;
}

async fn run_crash_recovery(repository: Arc<dyn AgentRegistryRepository>) {
    let clock = Arc::new(InMemoryClock::new(200));
    let service = AgentRegistryService::new(repository, clock.clone(), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    let approved = service
        .decide_enrollment(
            approval_request(
                initial_enrollment_id(),
                pending.record.resource_version,
                false,
            ),
            actor(),
        )
        .await
        .unwrap();
    let first = service
        .open_session(OpenAgentSessionRequest {
            agent_id: initial_agent_id(),
            installation_id: initial_installation_id(),
            boot_id: boot_id("boot-before-crash"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: approved.record.resource_version,
        })
        .await
        .unwrap();
    let ready = service
        .report_mount(mount_report(
            AgentKind::Initial,
            "boot-before-crash",
            first.session_generation,
            1,
            1,
            1,
            "volume-a",
            200,
        ))
        .await
        .unwrap();
    clock.advance(101).unwrap();
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Unavailable
    );

    let restarted = service
        .open_session(OpenAgentSessionRequest {
            agent_id: initial_agent_id(),
            installation_id: initial_installation_id(),
            boot_id: boot_id("boot-after-crash"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: ready.record.resource_version,
        })
        .await
        .unwrap();
    assert_eq!(restarted.session_generation.get(), 2);
    assert_eq!(
        restarted.record.enrollment.state,
        AgentEnrollmentState::Approved
    );
    assert_eq!(restarted.record.owner.state, VolumeOwnerState::Active);
    assert_eq!(restarted.record.owner.owner_generation.get(), 1);
    assert_eq!(
        restarted.record.owner.active_agent_id,
        Some(initial_agent_id())
    );
    assert_eq!(restarted.record.mount.health, ResourceHealth::Unknown);
    assert_eq!(
        service
            .report_mount(mount_report(
                AgentKind::Initial,
                "boot-before-crash",
                first.session_generation,
                1,
                1,
                2,
                "volume-a",
                301,
            ))
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::GenerationMismatch
    );
    service
        .report_mount(mount_report(
            AgentKind::Initial,
            "boot-after-crash",
            restarted.session_generation,
            1,
            1,
            1,
            "volume-a",
            301,
        ))
        .await
        .unwrap();
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Ready
    );
}

async fn approve_initial_repository(
    repository: Arc<dyn AgentRegistryRepository>,
) -> AgentRegistryRecord {
    let service = AgentRegistryService::new(repository, Arc::new(InMemoryClock::new(200)), 100);
    service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    service
        .decide_enrollment(
            approval_request(
                initial_enrollment_id(),
                pending.record.resource_version,
                false,
            ),
            actor(),
        )
        .await
        .unwrap()
        .record
}

async fn assert_direct_repository_guards(repository: Arc<dyn AgentRegistryRepository>) {
    let staging: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let injected_approved = approve_initial_repository(staging).await;
    assert_eq!(
        repository
            .insert_or_load(injected_approved)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::StorageFailure
    );

    let approved = approve_initial_repository(repository.clone()).await;
    let next_resource_version = ResourceVersion::new(approved.resource_version.get() + 1);

    let mut static_identity = approved.clone();
    static_identity.resource_version = next_resource_version;
    static_identity.instance.as_mut().unwrap().agent_version = "tampered".to_owned();

    let mut incomplete_session = approved.clone();
    incomplete_session.resource_version = next_resource_version;
    incomplete_session
        .instance
        .as_mut()
        .unwrap()
        .session_opened_at_unix_ms = Some(UnixMillis::new(201));

    let mut generation_change = approved.clone();
    generation_change.resource_version = next_resource_version;
    generation_change.mount.mount_generation =
        MountGeneration::new(approved.mount.mount_generation.get() + 1);

    let mut direct_revoke = approved.clone();
    direct_revoke.resource_version = next_resource_version;
    direct_revoke.enrollment.state = AgentEnrollmentState::Revoked;
    direct_revoke.enrollment.replaced_by_enrollment_id = Some(independent_enrollment_id());
    direct_revoke.instance.as_mut().unwrap().state = AgentInstanceState::Revoked;
    direct_revoke.owner.active_agent_id = None;
    direct_revoke.owner.active_agent_mount_id = None;
    direct_revoke.owner.state = VolumeOwnerState::Inactive;

    for tampered in [
        static_identity,
        incomplete_session,
        generation_change,
        direct_revoke,
    ] {
        let error = repository
            .replace(approved.resource_version.get(), tampered)
            .await
            .unwrap_err();
        assert_eq!(error.code(), CentralErrorCode::StorageFailure);
        assert_eq!(
            repository
                .get(&initial_enrollment_id())
                .await
                .unwrap()
                .unwrap(),
            approved
        );
    }

    let service =
        AgentRegistryService::new(repository.clone(), Arc::new(InMemoryClock::new(200)), 100);
    let mut expiring = independent_token_request();
    expiring.created_at_unix_ms = UnixMillis::new(100);
    expiring.expires_at_unix_ms = UnixMillis::new(150);
    let issued = service.create_token_intent(expiring).await.unwrap().record;
    let expired = service
        .expire_enrollment(ExpireAgentEnrollmentRequest {
            enrollment_id: independent_enrollment_id(),
            expected_resource_version: issued.resource_version,
        })
        .await
        .unwrap();
    let mut revived = expired.clone();
    revived.resource_version = ResourceVersion::new(expired.resource_version.get() + 1);
    revived.enrollment.state = AgentEnrollmentState::TokenIssued;
    assert_eq!(
        repository
            .replace(expired.resource_version.get(), revived)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::StorageFailure
    );
    assert_eq!(
        repository
            .get(&independent_enrollment_id())
            .await
            .unwrap()
            .unwrap(),
        expired
    );
}

async fn assert_replacement_transition_guards(repository: Arc<dyn AgentRegistryRepository>) {
    let previous = approve_initial_repository(repository.clone()).await;
    let service =
        AgentRegistryService::new(repository.clone(), Arc::new(InMemoryClock::new(200)), 100);
    service
        .create_token_intent(replacement_token_request())
        .await
        .unwrap();
    let pending = service
        .bootstrap_agent(replacement_bootstrap_request())
        .await
        .unwrap()
        .record;
    let (revoked, replacement) = replacement_transition_records(&previous, &pending);

    let mut wrong_link = revoked.clone();
    wrong_link.enrollment.replaced_by_enrollment_id = Some(independent_enrollment_id());
    assert_eq!(
        repository
            .activate_replacement(
                previous.resource_version.get(),
                wrong_link,
                pending.resource_version.get(),
                replacement.clone(),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::StorageFailure
    );

    let mut wrong_revoked_generation = revoked.clone();
    let mut wrong_replacement_generation = replacement.clone();
    let wrong_generation = MountGeneration::new(replacement.mount.mount_generation.get() + 1);
    wrong_revoked_generation.mount.mount_generation = wrong_generation;
    wrong_replacement_generation.mount.mount_generation = wrong_generation;
    assert_eq!(
        repository
            .activate_replacement(
                previous.resource_version.get(),
                wrong_revoked_generation,
                pending.resource_version.get(),
                wrong_replacement_generation,
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::StorageFailure
    );

    assert_eq!(
        repository
            .get(&initial_enrollment_id())
            .await
            .unwrap()
            .unwrap(),
        previous
    );
    assert_eq!(
        repository
            .get(&replacement_enrollment_id())
            .await
            .unwrap()
            .unwrap(),
        pending
    );
}

fn replacement_transition_records(
    previous: &AgentRegistryRecord,
    pending: &AgentRegistryRecord,
) -> (AgentRegistryRecord, AgentRegistryRecord) {
    let next_mount_generation = MountGeneration::new(previous.mount.mount_generation.get() + 1);
    let next_owner_generation = OwnerGeneration::new(previous.owner.owner_generation.get() + 1);
    let mut revoked = previous.clone();
    revoked.resource_version = ResourceVersion::new(previous.resource_version.get() + 1);
    revoked.enrollment.state = AgentEnrollmentState::Revoked;
    revoked.enrollment.replaced_by_enrollment_id = Some(replacement_enrollment_id());
    revoked.instance.as_mut().unwrap().state = AgentInstanceState::Revoked;
    revoked.mount.mount_generation = next_mount_generation;
    revoked.owner.owner_generation = next_owner_generation;
    revoked.owner.active_agent_id = None;
    revoked.owner.active_agent_mount_id = None;
    revoked.owner.state = VolumeOwnerState::Inactive;

    let candidate = pending.candidate.clone().unwrap();
    let instance = AgentInstanceRecord {
        agent_id: replacement_agent_id(),
        installation_id: candidate.installation_id,
        public_key_fingerprint: candidate.public_key_fingerprint,
        agent_version: candidate.agent_version,
        supported_protocol_versions: candidate.supported_protocol_versions,
        capabilities: candidate.capabilities,
        state: AgentInstanceState::Active,
        session_generation: None,
        active_boot_id: None,
        session_open_expected_resource_version: None,
        session_opened_at_unix_ms: None,
        last_heartbeat_at_unix_ms: None,
        last_sequence: None,
    };
    let decision = approval_request(replacement_enrollment_id(), pending.resource_version, true);
    let decision_actor = actor();
    let decided_at = UnixMillis::new(200);
    let mut replacement = pending.clone();
    replacement.resource_version = ResourceVersion::new(pending.resource_version.get() + 1);
    replacement.enrollment.state = AgentEnrollmentState::Approved;
    replacement.enrollment.decided_at_unix_ms = Some(decided_at);
    replacement.enrollment.decided_by = Some(decision_actor.clone());
    replacement.enrollment.decision_request = Some(decision.clone());
    replacement.instance = Some(instance.clone());
    replacement.mount.mount_generation = next_mount_generation;
    replacement.owner.owner_generation = next_owner_generation;
    replacement.owner.active_agent_id = Some(instance.agent_id);
    replacement.owner.active_agent_mount_id = Some(replacement.mount.agent_mount_id.clone());
    replacement.owner.state = VolumeOwnerState::RecoveryRequired;
    replacement.decision_audit_event = Some(AgentEnrollmentAuditEvent {
        event_id: format!(
            "agent-enrollment:{}:{}",
            replacement_enrollment_id(),
            decision.decision_request_id
        ),
        kind: AgentEnrollmentAuditKind::ReplacementApproved,
        tenant_id: tenant_id(),
        enrollment_id: replacement_enrollment_id(),
        storage_volume_id: storage_volume_id(),
        decision_request_id: decision.decision_request_id,
        resource_version: replacement.resource_version,
        actor: decision_actor,
        occurred_at_unix_ms: decided_at,
    });
    (revoked, replacement)
}

async fn assert_resource_version_exhaustion_is_atomic(
    repository: Arc<dyn AgentRegistryRepository>,
) {
    let token_staging: Arc<dyn AgentRegistryRepository> = Arc::new(InMemoryAgentRegistry::new());
    let token_service =
        AgentRegistryService::new(token_staging, Arc::new(InMemoryClock::new(300)), 100);
    let mut issued = token_service
        .create_token_intent(independent_token_request())
        .await
        .unwrap()
        .record;
    issued.resource_version = ResourceVersion::new(u64::MAX);
    repository.insert_or_load(issued.clone()).await.unwrap();

    assert_eq!(
        repository
            .replace(u64::MAX, issued.clone())
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::StorageFailure
    );
    assert_eq!(
        repository
            .expire_stale_token_intents(
                &issued.enrollment.tenant_id,
                &issued.enrollment.storage_volume_id,
                &issued.enrollment.edge_cluster_id,
                &issued.enrollment.pvc_identity_digest,
                UnixMillis::new(u64::MAX),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::StorageFailure
    );
    assert_eq!(
        repository
            .get(&issued.enrollment.enrollment_id)
            .await
            .unwrap()
            .unwrap(),
        issued
    );
}

async fn run_lifecycle(
    repository: Arc<dyn AgentRegistryRepository>,
    clock: Arc<InMemoryClock>,
) -> LifecycleResult {
    let service = AgentRegistryService::new(repository.clone(), clock.clone(), 100);
    let token_intent = service
        .create_token_intent(initial_token_request())
        .await
        .unwrap();
    assert!(!token_intent.replayed);
    assert_eq!(
        token_intent.record.enrollment.state,
        AgentEnrollmentState::TokenIssued
    );
    assert!(token_intent.record.candidate.is_none());
    assert!(token_intent.record.instance.is_none());
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Unavailable
    );

    let bootstrapped = service
        .bootstrap_agent(initial_bootstrap_request())
        .await
        .unwrap();
    assert!(!bootstrapped.accepted.replayed);
    assert!(bootstrapped.record.candidate.is_some());
    assert!(bootstrapped.record.instance.is_none());

    let approval = approval_request(
        initial_enrollment_id(),
        bootstrapped.record.resource_version,
        false,
    );
    let approved = service
        .decide_enrollment(approval.clone(), actor())
        .await
        .unwrap();
    assert!(approved.record.instance.is_some());
    assert_eq!(approved.record.owner.state, VolumeOwnerState::Active);
    let replayed = service
        .decide_enrollment(approval.clone(), actor())
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(
        replayed.record.resource_version,
        approved.record.resource_version
    );
    let mut conflicting_approval = approval;
    conflicting_approval.confirm_replacement = true;
    assert_eq!(
        service
            .decide_enrollment(conflicting_approval, actor())
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::EnrollmentDecisionConflict
    );

    assert_eq!(
        service
            .open_session(OpenAgentSessionRequest {
                agent_id: initial_agent_id(),
                installation_id: initial_installation_id(),
                boot_id: boot_id("boot-wrong-mount"),
                mount_identity_digest: different_mount_identity_digest(),
                expected_resource_version: approved.record.resource_version,
            })
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );

    let opened = service
        .open_session(OpenAgentSessionRequest {
            agent_id: initial_agent_id(),
            installation_id: initial_installation_id(),
            boot_id: boot_id("boot-a"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: approved.record.resource_version,
        })
        .await
        .unwrap();
    assert_eq!(opened.session_generation.get(), 1);
    let replayed_open = service
        .open_session(OpenAgentSessionRequest {
            agent_id: initial_agent_id(),
            installation_id: initial_installation_id(),
            boot_id: boot_id("boot-a"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: approved.record.resource_version,
        })
        .await
        .unwrap();
    assert!(replayed_open.replayed);
    assert_eq!(replayed_open.session_generation, opened.session_generation);
    assert_eq!(
        replayed_open.record.resource_version,
        opened.record.resource_version
    );
    assert_eq!(
        service
            .open_session(OpenAgentSessionRequest {
                agent_id: initial_agent_id(),
                installation_id: initial_installation_id(),
                boot_id: boot_id("boot-a"),
                mount_identity_digest: mount_identity_digest(),
                expected_resource_version: opened.record.resource_version,
            })
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );
    assert_eq!(
        service
            .open_session(OpenAgentSessionRequest {
                agent_id: initial_agent_id(),
                installation_id: initial_installation_id(),
                boot_id: boot_id("boot-cloned-state"),
                mount_identity_digest: mount_identity_digest(),
                expected_resource_version: opened.record.resource_version,
            })
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentSessionActive
    );

    let mut drifted_mount = mount_report(
        AgentKind::Initial,
        "boot-a",
        opened.session_generation,
        1,
        1,
        1,
        "volume-a",
        9_999_999,
    );
    drifted_mount.mount_identity_digest = different_mount_identity_digest();
    assert_eq!(
        service
            .report_mount(drifted_mount)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );

    let wrong_marker = service
        .report_mount(mount_report(
            AgentKind::Initial,
            "boot-a",
            opened.session_generation,
            1,
            1,
            1,
            "wrong-marker",
            9_999_999,
        ))
        .await
        .unwrap();
    assert_eq!(
        wrong_marker.record.mount.health,
        ResourceHealth::Unavailable
    );

    let mut read_only = mount_report(
        AgentKind::Initial,
        "boot-a",
        opened.session_generation,
        1,
        1,
        2,
        "volume-a",
        9_999_999,
    );
    read_only.access_mode = MountAccessMode::ReadOnly;
    let read_only = service.report_mount(read_only).await.unwrap();
    assert_eq!(read_only.record.mount.health, ResourceHealth::Degraded);
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Degraded
    );

    let mut probe_failure = mount_report(
        AgentKind::Initial,
        "boot-a",
        opened.session_generation,
        1,
        1,
        3,
        "volume-a",
        9_999_999,
    );
    probe_failure.health = ResourceHealth::Unavailable;
    let probe_failure = service.report_mount(probe_failure).await.unwrap();
    assert_eq!(
        probe_failure.record.mount.health,
        ResourceHealth::Unavailable
    );

    let ready = service
        .report_mount(mount_report(
            AgentKind::Initial,
            "boot-a",
            opened.session_generation,
            1,
            1,
            4,
            "volume-a",
            9_999_999,
        ))
        .await
        .unwrap();
    assert_eq!(
        ready
            .record
            .instance
            .as_ref()
            .unwrap()
            .last_heartbeat_at_unix_ms,
        Some(UnixMillis::new(200))
    );
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Ready
    );
    let mut changed_replay = mount_report(
        AgentKind::Initial,
        "boot-a",
        opened.session_generation,
        1,
        1,
        4,
        "volume-a",
        9_999_999,
    );
    changed_replay.health = ResourceHealth::Degraded;
    assert_eq!(
        service
            .report_mount(changed_replay)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::GenerationMismatch
    );
    clock.advance(101).unwrap();
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Unavailable
    );
    clock.set(200);

    let closed = service
        .close_session(CloseAgentSessionRequest {
            agent_id: initial_agent_id(),
            boot_id: boot_id("boot-a"),
            session_generation: opened.session_generation,
            expected_resource_version: ready.record.resource_version,
        })
        .await
        .unwrap();
    assert_eq!(
        service
            .report_mount(mount_report(
                AgentKind::Initial,
                "boot-a",
                opened.session_generation,
                1,
                1,
                5,
                "volume-a",
                200,
            ))
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::GenerationMismatch
    );
    let restarted = service
        .open_session(OpenAgentSessionRequest {
            agent_id: initial_agent_id(),
            installation_id: initial_installation_id(),
            boot_id: boot_id("boot-a-restarted"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: closed.resource_version,
        })
        .await
        .unwrap();
    assert_eq!(restarted.session_generation.get(), 2);
    let restarted_ready = service
        .report_mount(mount_report(
            AgentKind::Initial,
            "boot-a-restarted",
            restarted.session_generation,
            1,
            1,
            1,
            "volume-a",
            200,
        ))
        .await
        .unwrap();
    assert_eq!(
        restarted_ready.record.enrollment.state,
        AgentEnrollmentState::Approved
    );
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Ready
    );

    let mut reused_reserved_identity = replacement_token_request();
    reused_reserved_identity.agent_id = initial_agent_id();
    assert_eq!(
        service
            .create_token_intent(reused_reserved_identity)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );
    let replacement_intent = service
        .create_token_intent(replacement_token_request())
        .await
        .unwrap();
    assert_eq!(
        replacement_intent.record.enrollment.replaces_enrollment_id,
        Some(initial_enrollment_id())
    );

    let mut reused_identity = replacement_bootstrap_request();
    reused_identity.installation_id = initial_installation_id();
    reused_identity.public_key_fingerprint = ContentDigest::hash(b"public-key-a");
    assert_eq!(
        service
            .bootstrap_agent(reused_identity)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::AgentIdentityMismatch
    );

    let replacement_candidate = service
        .bootstrap_agent(replacement_bootstrap_request())
        .await
        .unwrap();
    assert!(replacement_candidate.record.instance.is_none());
    assert_eq!(
        replacement_candidate.record.owner.state,
        VolumeOwnerState::Inactive
    );

    assert_eq!(
        service
            .decide_enrollment(
                approval_request(
                    replacement_enrollment_id(),
                    replacement_candidate.record.resource_version,
                    false,
                ),
                actor(),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::VolumeOwnerConflict
    );
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Ready
    );

    let replacement_approved = service
        .decide_enrollment(
            approval_request(
                replacement_enrollment_id(),
                replacement_candidate.record.resource_version,
                true,
            ),
            actor(),
        )
        .await
        .unwrap();
    let revoked = replacement_approved.revoked_record.unwrap();
    assert_eq!(revoked.enrollment.state, AgentEnrollmentState::Revoked);
    assert_eq!(revoked.owner.state, VolumeOwnerState::Inactive);
    assert_eq!(revoked.owner.owner_generation.get(), 2);
    assert_eq!(revoked.mount.mount_generation.get(), 2);
    let revoked_instance = revoked.instance.as_ref().unwrap();
    assert!(revoked_instance.active_boot_id.is_none());
    assert!(revoked_instance.last_heartbeat_at_unix_ms.is_none());
    assert_eq!(
        replacement_approved.record.owner.state,
        VolumeOwnerState::RecoveryRequired
    );
    assert_eq!(replacement_approved.record.owner.owner_generation.get(), 2);
    assert_eq!(replacement_approved.record.mount.mount_generation.get(), 2);

    assert_eq!(
        service
            .open_session(OpenAgentSessionRequest {
                agent_id: initial_agent_id(),
                installation_id: initial_installation_id(),
                boot_id: boot_id("boot-old-retry"),
                mount_identity_digest: mount_identity_digest(),
                expected_resource_version: revoked.resource_version,
            })
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ApprovalRequired
    );
    assert_eq!(
        service
            .report_mount(mount_report(
                AgentKind::Initial,
                "boot-a",
                opened.session_generation,
                1,
                1,
                5,
                "volume-a",
                200,
            ))
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ApprovalRequired
    );

    let replacement_session = service
        .open_session(OpenAgentSessionRequest {
            agent_id: replacement_agent_id(),
            installation_id: replacement_installation_id(),
            boot_id: boot_id("boot-b"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: replacement_approved.record.resource_version,
        })
        .await
        .unwrap();
    assert_eq!(replacement_session.session_generation.get(), 1);
    assert_eq!(
        service
            .report_mount(mount_report(
                AgentKind::Replacement,
                "boot-b",
                replacement_session.session_generation,
                1,
                1,
                1,
                "volume-a",
                200,
            ))
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::GenerationMismatch
    );

    let recovered_mount = service
        .report_mount(mount_report(
            AgentKind::Replacement,
            "boot-b",
            replacement_session.session_generation,
            2,
            2,
            1,
            "volume-a",
            200,
        ))
        .await
        .unwrap();
    assert_eq!(
        service
            .volume_state(&replacement_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Unavailable
    );
    let current = service
        .complete_recovery(CompleteVolumeRecoveryRequest {
            enrollment_id: replacement_enrollment_id(),
            expected_resource_version: recovered_mount.record.resource_version,
            owner_generation: OwnerGeneration::new(2),
        })
        .await
        .unwrap();
    assert_eq!(current.owner.state, VolumeOwnerState::Active);
    assert_eq!(
        service
            .volume_state(&replacement_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Ready
    );
    assert_eq!(
        service
            .volume_state(&initial_enrollment_id())
            .await
            .unwrap(),
        DerivedVolumeState::Unavailable
    );
    assert_eq!(
        repository
            .get_current_by_volume(&tenant_id(), &storage_volume_id())
            .await
            .unwrap()
            .unwrap(),
        current
    );

    LifecycleResult { current, revoked }
}

fn initial_token_request() -> AgentEnrollmentTokenCreateRequest {
    token_request(AgentKind::Initial, INITIAL_TOKEN, 100)
}

fn replacement_token_request() -> AgentEnrollmentTokenCreateRequest {
    token_request(AgentKind::Replacement, REPLACEMENT_TOKEN, 200)
}

fn independent_token_request() -> AgentEnrollmentTokenCreateRequest {
    AgentEnrollmentTokenCreateRequest {
        token_id: AgentEnrollmentTokenId::new("token-c").unwrap(),
        token_request_id: RequestId::new("create-token-c").unwrap(),
        enrollment_id: independent_enrollment_id(),
        tenant_id: tenant_id(),
        edge_cluster_id: edge_cluster_id(),
        storage_volume_id: StorageVolumeId::new("volume-c").unwrap(),
        volume_descriptor_digest: ContentDigest::hash(b"volume-descriptor-c"),
        pvc_identity_digest: PvcIdentityDigest::derive("namespace-a", "claim-c").unwrap(),
        agent_id: AgentId::new("agent-c").unwrap(),
        agent_mount_id: AgentMountId::new("mount-c").unwrap(),
        expected_volume_marker: VolumeMarkerId::new("volume-c").unwrap(),
        desired_access_mode: MountAccessMode::ReadWrite,
        bootstrap_token: INDEPENDENT_TOKEN.to_owned(),
        created_at_unix_ms: UnixMillis::new(300),
        expires_at_unix_ms: UnixMillis::new(1_200),
        extensions: Extensions::new(),
    }
}

fn token_request(
    kind: AgentKind,
    bootstrap_token: &str,
    created_at: u64,
) -> AgentEnrollmentTokenCreateRequest {
    AgentEnrollmentTokenCreateRequest {
        token_id: token_id(kind),
        token_request_id: token_request_id(kind),
        enrollment_id: enrollment_id(kind),
        tenant_id: tenant_id(),
        edge_cluster_id: edge_cluster_id(),
        storage_volume_id: storage_volume_id(),
        volume_descriptor_digest: volume_descriptor_digest(),
        pvc_identity_digest: pvc_identity_digest(),
        agent_id: agent_id(kind),
        agent_mount_id: agent_mount_id(kind),
        expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
        desired_access_mode: MountAccessMode::ReadWrite,
        bootstrap_token: bootstrap_token.to_owned(),
        created_at_unix_ms: UnixMillis::new(created_at),
        expires_at_unix_ms: UnixMillis::new(created_at + 1_000),
        extensions: Extensions::new(),
    }
}

fn initial_bootstrap_request() -> AgentBootstrapRequest {
    bootstrap_request(AgentKind::Initial, INITIAL_TOKEN)
}

fn replacement_bootstrap_request() -> AgentBootstrapRequest {
    bootstrap_request(AgentKind::Replacement, REPLACEMENT_TOKEN)
}

fn independent_bootstrap_request() -> AgentBootstrapRequest {
    let mut request = bootstrap_request(AgentKind::Initial, INDEPENDENT_TOKEN);
    request.bootstrap_request_id = RequestId::new("bootstrap-c").unwrap();
    request.installation_id = AgentInstallationId::new("installation-c").unwrap();
    request.storage_volume_id = StorageVolumeId::new("volume-c").unwrap();
    request.volume_descriptor_digest = ContentDigest::hash(b"volume-descriptor-c");
    request.probe.mount_identity_digest =
        AgentMountIdentityDigest::new(ContentDigest::hash(b"mount-c"));
    request.public_key_fingerprint = ContentDigest::hash(b"public-key-c");
    request.probe.observed_volume_marker = Some(VolumeMarkerId::new("volume-c").unwrap());
    request
}

fn bootstrap_request(kind: AgentKind, bootstrap_token: &str) -> AgentBootstrapRequest {
    AgentBootstrapRequest {
        bootstrap_request_id: bootstrap_request_id(kind),
        bootstrap_token: bootstrap_token.to_owned(),
        installation_id: installation_id(kind),
        tenant_id: tenant_id(),
        edge_cluster_id: edge_cluster_id(),
        storage_volume_id: storage_volume_id(),
        volume_descriptor_digest: volume_descriptor_digest(),
        agent_version: "0.0.1".to_owned(),
        supported_protocol_versions: vec![PROTOCOL_VERSION_V1],
        capabilities: BTreeSet::from(["single_volume_v1".to_owned()]),
        public_key_fingerprint: public_key_fingerprint(kind),
        probe: healthy_bootstrap_probe(),
        extensions: Extensions::new(),
    }
}

fn approval_request(
    enrollment_id: AgentEnrollmentId,
    expected_resource_version: neoengram_protocol::ResourceVersion,
    confirm_replacement: bool,
) -> AgentEnrollmentApprovalRequest {
    let decision_request_id =
        RequestId::new(format!("approve-{}", enrollment_id.as_str())).unwrap();
    AgentEnrollmentApprovalRequest {
        enrollment_id,
        decision_request_id,
        expected_resource_version,
        decision: AgentEnrollmentDecision::Approve,
        confirm_replacement,
        extensions: Extensions::new(),
    }
}

fn healthy_bootstrap_probe() -> AgentBootstrapProbe {
    AgentBootstrapProbe {
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
    }
}

#[derive(Debug, Clone, Copy)]
enum AgentKind {
    Initial,
    Replacement,
}

async fn execute_registry_raw(root: &Path, sql: &str) {
    let options = SqliteConnectOptions::new().filename(root.join("agent-registry.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(sql).execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
}

#[allow(clippy::too_many_arguments)]
fn mount_report(
    kind: AgentKind,
    boot: &str,
    session_generation: SessionGeneration,
    mount_generation: u64,
    owner_generation: u64,
    sequence: u64,
    marker: &str,
    observed_at: u64,
) -> AgentMountStatusReport {
    AgentMountStatusReport {
        agent_id: agent_id(kind),
        installation_id: installation_id(kind),
        boot_id: boot_id(boot),
        session_generation,
        sequence: SequenceNumber::new(sequence),
        agent_mount_id: agent_mount_id(kind),
        storage_volume_id: storage_volume_id(),
        mount_generation: MountGeneration::new(mount_generation),
        owner_generation: OwnerGeneration::new(owner_generation),
        observed_volume_marker: VolumeMarkerId::new(marker).unwrap(),
        mount_identity_digest: mount_identity_digest(),
        access_mode: MountAccessMode::ReadWrite,
        health: ResourceHealth::Ready,
        observed_at_unix_ms: UnixMillis::new(observed_at),
        extensions: Extensions::new(),
    }
}

fn pvc_identity_digest() -> PvcIdentityDigest {
    PvcIdentityDigest::derive("namespace-a", "claim-a").unwrap()
}

fn mount_identity_digest() -> AgentMountIdentityDigest {
    AgentMountIdentityDigest::new(ContentDigest::hash(b"mount-volume-a"))
}

fn different_mount_identity_digest() -> AgentMountIdentityDigest {
    AgentMountIdentityDigest::new(ContentDigest::hash(b"different-mounted-filesystem"))
}

fn actor() -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::User,
        id: PrincipalId::new("tenant-admin").unwrap(),
        extensions: Extensions::new(),
    }
}

fn tenant_id() -> TenantId {
    TenantId::new("tenant-a").unwrap()
}

fn edge_cluster_id() -> EdgeClusterId {
    EdgeClusterId::new("cluster-a").unwrap()
}

fn storage_volume_id() -> StorageVolumeId {
    StorageVolumeId::new("volume-a").unwrap()
}

fn volume_descriptor_digest() -> ContentDigest {
    ContentDigest::hash(b"volume-descriptor-a")
}

fn token_id(kind: AgentKind) -> AgentEnrollmentTokenId {
    AgentEnrollmentTokenId::new(match kind {
        AgentKind::Initial => "token-a",
        AgentKind::Replacement => "token-b",
    })
    .unwrap()
}

fn token_request_id(kind: AgentKind) -> RequestId {
    RequestId::new(match kind {
        AgentKind::Initial => "create-token-a",
        AgentKind::Replacement => "create-token-b",
    })
    .unwrap()
}

fn bootstrap_request_id(kind: AgentKind) -> RequestId {
    RequestId::new(match kind {
        AgentKind::Initial => "bootstrap-a",
        AgentKind::Replacement => "bootstrap-b",
    })
    .unwrap()
}

fn enrollment_id(kind: AgentKind) -> AgentEnrollmentId {
    AgentEnrollmentId::new(match kind {
        AgentKind::Initial => "enrollment-a",
        AgentKind::Replacement => "enrollment-b",
    })
    .unwrap()
}

fn initial_enrollment_id() -> AgentEnrollmentId {
    enrollment_id(AgentKind::Initial)
}

fn replacement_enrollment_id() -> AgentEnrollmentId {
    enrollment_id(AgentKind::Replacement)
}

fn independent_enrollment_id() -> AgentEnrollmentId {
    AgentEnrollmentId::new("enrollment-c").unwrap()
}

fn agent_id(kind: AgentKind) -> AgentId {
    AgentId::new(match kind {
        AgentKind::Initial => "agent-a",
        AgentKind::Replacement => "agent-b",
    })
    .unwrap()
}

fn initial_agent_id() -> AgentId {
    agent_id(AgentKind::Initial)
}

fn replacement_agent_id() -> AgentId {
    agent_id(AgentKind::Replacement)
}

fn installation_id(kind: AgentKind) -> AgentInstallationId {
    AgentInstallationId::new(match kind {
        AgentKind::Initial => "installation-a",
        AgentKind::Replacement => "installation-b",
    })
    .unwrap()
}

fn initial_installation_id() -> AgentInstallationId {
    installation_id(AgentKind::Initial)
}

fn replacement_installation_id() -> AgentInstallationId {
    installation_id(AgentKind::Replacement)
}

fn agent_mount_id(kind: AgentKind) -> AgentMountId {
    AgentMountId::new(match kind {
        AgentKind::Initial => "mount-a",
        AgentKind::Replacement => "mount-b",
    })
    .unwrap()
}

fn public_key_fingerprint(kind: AgentKind) -> ContentDigest {
    ContentDigest::hash(match kind {
        AgentKind::Initial => b"public-key-a",
        AgentKind::Replacement => b"public-key-b",
    })
}

fn boot_id(value: &str) -> AgentBootId {
    AgentBootId::new(value).unwrap()
}
