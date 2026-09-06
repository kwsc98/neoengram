use neoengram_agent::{
    AgentAssignmentState, AgentCertificateState, AgentErrorCode, ApprovedAgentIdentity,
    AssignmentKey, ClaimOutcome, Ledger, LedgerClaim, PrivateKeyMaterial, SqliteLedger,
    SqliteLedgerConfig, SqliteLifecycleJournal, SqliteLifecycleJournalConfig,
    SqliteOutboundReportQueue, SqliteOutboundReportQueueConfig, SqliteSystemIdentityStore,
    SystemIdentitySeed, TerminalEnrollmentOutcome, TerminalEnrollmentState,
};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    AddAssignment, AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration,
    AssignmentId, CommitDataLayout, EdgeClusterId, Extensions, Generation, IndexRevision, JobId,
    MountGeneration, OwnerGeneration, PlacementGeneration, PrincipalId, PrincipalKind,
    PrincipalRef, ProjectId, StorageVolumeId, TaskExecutionFence, TaskId, TenantId, UnixMillis,
    WireIndexVersion, WorkspaceId,
};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[test]
fn persistent_adapters_share_one_state_database_and_lock() {
    use fs2::FileExt;
    use std::fs::OpenOptions;

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("agent-state");
    let agent_id = AgentId::new("agent-a").unwrap();
    let tenant_id = TenantId::new("tenant-a").unwrap();

    // Each public adapter is opened independently, but all of them must use the one state-store
    // handle and contribute their tables to the same current schema.
    let system = SqliteSystemIdentityStore::open(&root).unwrap();
    let ledger = SqliteLedger::open(SqliteLedgerConfig::new(
        &root,
        agent_id.clone(),
        tenant_id.clone(),
    ))
    .unwrap();
    let outbound = SqliteOutboundReportQueue::open(SqliteOutboundReportQueueConfig::new(
        &root,
        agent_id.clone(),
        tenant_id.clone(),
    ))
    .unwrap();
    let lifecycle = SqliteLifecycleJournal::open(SqliteLifecycleJournalConfig::new(
        &root, agent_id, tenant_id,
    ))
    .unwrap();

    let connection = rusqlite::Connection::open(root.join("agent-state.sqlite3")).unwrap();
    let table_count: i64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(table_count, 8);
    drop(connection);

    // A separate opener cannot bypass the state lock while any logical adapter is alive.
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("agent-state.lock"))
        .unwrap();
    assert!(lock.try_lock_exclusive().is_err());

    drop(lifecycle);
    drop(outbound);
    drop(ledger);
    drop(system);
}

#[test]
fn split_state_layout_is_rejected_without_migration() {
    let temporary = tempfile::tempdir().unwrap();
    for legacy_name in [
        "ledger.sqlite3",
        "outbound.sqlite3",
        "system.sqlite3",
        "lifecycle.sqlite3",
        "ledger.lock",
        "outbound.lock",
        "system.lock",
        "lifecycle.lock",
    ] {
        let root = temporary.path().join(legacy_name.replace('.', "-"));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(legacy_name), b"legacy").unwrap();
        assert_eq!(
            SqliteLedger::open(ledger_config(&root)).unwrap_err().code(),
            AgentErrorCode::Internal,
            "legacy state file {legacy_name} must be rejected",
        );
    }

    let nested_root = temporary.path().join("nested");
    std::fs::create_dir_all(nested_root.join("ledger")).unwrap();
    std::fs::write(nested_root.join("ledger/ledger.sqlite3"), b"legacy").unwrap();
    assert_eq!(
        SqliteLedger::open(ledger_config(&nested_root))
            .unwrap_err()
            .code(),
        AgentErrorCode::Internal
    );
}

#[test]
fn sqlite_ledger_recovers_claim_replay_and_cas_after_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("tenant-ledger");
    let assignment = assignment(1);
    let key = AssignmentKey::from_assignment(&assignment);

    let ledger = SqliteLedger::open(ledger_config(&root)).unwrap();
    let claimed = match ledger
        .claim(LedgerClaim {
            assignment: assignment.clone(),
            claimed_at_unix_ms: 100,
        })
        .unwrap()
    {
        ClaimOutcome::Claimed(record) => record,
        ClaimOutcome::Existing(_) => panic!("first claim must be new"),
    };
    assert!(matches!(
        ledger
            .claim(LedgerClaim {
                assignment: assignment.clone(),
                claimed_at_unix_ms: 200,
            })
            .unwrap(),
        ClaimOutcome::Existing(ref record) if record == &claimed
    ));

    let mut reused = assignment.clone();
    reused.request_digest = ContentDigest::from_bytes([99; 32]);
    assert_eq!(
        ledger
            .claim(LedgerClaim {
                assignment: reused,
                claimed_at_unix_ms: 200,
            })
            .unwrap_err()
            .code(),
        AgentErrorCode::JobIdReused
    );

    let mut accepted = claimed.clone();
    accepted.state = AgentAssignmentState::Accepted;
    accepted.updated_at_unix_ms = 101;
    let accepted = ledger.compare_exchange(claimed.revision, accepted).unwrap();
    assert_eq!(accepted.revision, 2);
    assert_eq!(
        ledger
            .compare_exchange(claimed.revision, accepted.clone())
            .unwrap_err()
            .code(),
        AgentErrorCode::LedgerConflict
    );
    // All adapters in one Agent share the state-store handle. A second logical adapter can
    // therefore reopen the same root in-process without acquiring a second lock descriptor.
    SqliteLedger::open(ledger_config(&root))
        .unwrap()
        .integrity_check()
        .unwrap();
    ledger.integrity_check().unwrap();
    drop(ledger);

    let reopened = SqliteLedger::open(ledger_config(&root)).unwrap();
    assert_eq!(reopened.load(&key).unwrap(), Some(accepted));
    reopened.integrity_check().unwrap();
    assert_secure_permissions(&root, &["agent-state.sqlite3", "agent-state.lock"]);
}

#[test]
fn sqlite_ledger_detects_indexed_column_corruption() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("tenant-ledger");
    let assignment = assignment(2);
    let key = AssignmentKey::from_assignment(&assignment);
    let ledger = SqliteLedger::open(ledger_config(&root)).unwrap();
    ledger
        .claim(LedgerClaim {
            assignment,
            claimed_at_unix_ms: 100,
        })
        .unwrap();
    drop(ledger);

    let connection = rusqlite::Connection::open(root.join("agent-state.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE ledger_records SET request_digest = ?1",
            ["00".repeat(32)],
        )
        .unwrap();
    drop(connection);

    let reopened = SqliteLedger::open(ledger_config(&root)).unwrap();
    assert_eq!(
        reopened.load(&key).unwrap_err().code(),
        AgentErrorCode::Internal
    );
}

#[test]
fn sqlite_ledger_rejects_database_and_operation_scope_mismatch() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("tenant-ledger");
    let ledger = SqliteLedger::open(ledger_config(&root)).unwrap();

    let mut wrong_agent = assignment(3);
    wrong_agent.agent_id = AgentId::new("agent-b").unwrap();
    assert_eq!(
        ledger
            .claim(LedgerClaim {
                assignment: wrong_agent,
                claimed_at_unix_ms: 100,
            })
            .unwrap_err()
            .code(),
        AgentErrorCode::ScopeMismatch
    );

    let wrong_key = AssignmentKey {
        tenant_id: TenantId::new("tenant-b").unwrap(),
        job_id: JobId::new("job-3").unwrap(),
    };
    assert_eq!(
        ledger.load(&wrong_key).unwrap_err().code(),
        AgentErrorCode::ScopeMismatch
    );
    drop(ledger);

    let wrong_agent_config = SqliteLedgerConfig::new(
        &root,
        AgentId::new("agent-b").unwrap(),
        TenantId::new("tenant-a").unwrap(),
    );
    assert_eq!(
        SqliteLedger::open(wrong_agent_config).unwrap_err().code(),
        AgentErrorCode::AssignmentMismatch
    );
    let wrong_tenant_config = SqliteLedgerConfig::new(
        &root,
        AgentId::new("agent-a").unwrap(),
        TenantId::new("tenant-b").unwrap(),
    );
    assert_eq!(
        SqliteLedger::open(wrong_tenant_config).unwrap_err().code(),
        AgentErrorCode::AssignmentMismatch
    );
}

#[test]
fn sqlite_ledger_recovers_only_empty_database_missing_identity() {
    let temporary = tempfile::tempdir().unwrap();
    let empty_root = temporary.path().join("empty-ledger");
    drop(SqliteLedger::open(ledger_config(&empty_root)).unwrap());
    let connection = rusqlite::Connection::open(empty_root.join("agent-state.sqlite3")).unwrap();
    connection
        .execute("DELETE FROM ledger_metadata", [])
        .unwrap();
    drop(connection);
    drop(SqliteLedger::open(ledger_config(&empty_root)).unwrap());

    let nonempty_root = temporary.path().join("nonempty-ledger");
    let ledger = SqliteLedger::open(ledger_config(&nonempty_root)).unwrap();
    ledger
        .claim(LedgerClaim {
            assignment: assignment(4),
            claimed_at_unix_ms: 100,
        })
        .unwrap();
    drop(ledger);
    let connection = rusqlite::Connection::open(nonempty_root.join("agent-state.sqlite3")).unwrap();
    connection
        .execute("DELETE FROM ledger_metadata", [])
        .unwrap();
    drop(connection);
    assert_eq!(
        SqliteLedger::open(ledger_config(&nonempty_root))
            .unwrap_err()
            .code(),
        AgentErrorCode::Internal
    );
}

#[test]
fn system_identity_is_immutable_redacted_and_restart_stable() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("system");
    let store = SqliteSystemIdentityStore::open(&root).unwrap();
    assert!(store.load().unwrap().is_none());
    let seed = identity_seed(b"private-key-one");
    let initialized = store.initialize(seed.clone()).unwrap();
    assert_eq!(initialized.revision, 1);
    let debug = format!("{initialized:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("private-key-one"));
    assert_eq!(store.initialize(seed).unwrap(), initialized);

    let approved = ApprovedAgentIdentity::new("agent-a", "enrollment-a").unwrap();
    let bound = store.bind_approved(1, approved.clone()).unwrap();
    assert_eq!(bound.revision, 2);
    assert_eq!(store.bind_approved(1, approved).unwrap(), bound);
    assert_eq!(
        store
            .bind_terminal_enrollment(
                bound.revision,
                TerminalEnrollmentOutcome::new(TerminalEnrollmentState::Rejected, "enrollment-a",)
                    .unwrap(),
            )
            .unwrap_err()
            .code(),
        AgentErrorCode::AssignmentMismatch
    );
    assert_eq!(
        store
            .bind_approved(
                bound.revision,
                ApprovedAgentIdentity::new("agent-b", "enrollment-b").unwrap(),
            )
            .unwrap_err()
            .code(),
        AgentErrorCode::AssignmentMismatch
    );

    let first_certificate = certificate(1, 3, 4, 5);
    let certified = store
        .install_certificate(bound.revision, 0, first_certificate.clone())
        .unwrap();
    let debug = format!("{certified:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("BEGIN CERTIFICATE"));
    assert!(!debug.contains("test"));
    assert_eq!(certified.revision, 3);
    assert_eq!(
        store
            .install_certificate(bound.revision, 0, first_certificate)
            .unwrap(),
        certified
    );
    let stale = store
        .install_certificate(bound.revision, 1, certificate(2, 4, 5, 6))
        .unwrap_err();
    assert_eq!(stale.code(), AgentErrorCode::LedgerConflict);
    store.integrity_check().unwrap();
    SqliteSystemIdentityStore::open(&root)
        .unwrap()
        .integrity_check()
        .unwrap();
    drop(store);

    let reopened = SqliteSystemIdentityStore::open(&root).unwrap();
    assert_eq!(reopened.load().unwrap(), Some(certified));
    assert_eq!(
        reopened
            .initialize(identity_seed(b"another-private-key"))
            .unwrap_err()
            .code(),
        AgentErrorCode::AssignmentMismatch
    );
    assert_secure_permissions(&root, &["agent-state.sqlite3", "agent-state.lock"]);
}

#[test]
fn terminal_enrollment_outcome_is_cas_bound_replayable_and_restart_stable() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("terminal-system");
    let store = SqliteSystemIdentityStore::open(&root).unwrap();
    store.initialize(identity_seed(b"terminal-key")).unwrap();

    let rejected =
        TerminalEnrollmentOutcome::new(TerminalEnrollmentState::Rejected, "enrollment-rejected")
            .unwrap();
    let terminal = store.bind_terminal_enrollment(1, rejected.clone()).unwrap();
    assert_eq!(terminal.revision, 2);
    assert_eq!(terminal.terminal_enrollment, Some(rejected.clone()));
    assert_eq!(
        store.bind_terminal_enrollment(1, rejected).unwrap(),
        terminal
    );
    assert_eq!(
        store
            .bind_terminal_enrollment(
                terminal.revision,
                TerminalEnrollmentOutcome::new(
                    TerminalEnrollmentState::Expired,
                    "enrollment-rejected",
                )
                .unwrap(),
            )
            .unwrap_err()
            .code(),
        AgentErrorCode::AssignmentMismatch
    );
    assert_eq!(
        store
            .bind_approved(
                terminal.revision,
                ApprovedAgentIdentity::new("agent-terminal", "enrollment-rejected").unwrap(),
            )
            .unwrap_err()
            .code(),
        AgentErrorCode::AssignmentMismatch
    );
    assert_eq!(
        store
            .install_certificate(terminal.revision, 0, certificate(1, 1, 1, 1))
            .unwrap_err()
            .code(),
        AgentErrorCode::InvalidState
    );
    store.integrity_check().unwrap();
    drop(store);

    let reopened = SqliteSystemIdentityStore::open(&root).unwrap();
    assert_eq!(reopened.load().unwrap(), Some(terminal));
}

#[test]
fn private_key_material_zeroizes_and_is_marked_for_zeroizing_drop() {
    fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}

    assert_zeroize_on_drop::<PrivateKeyMaterial>();
    let mut private_key = PrivateKeyMaterial::new(b"sensitive-private-key".to_vec()).unwrap();
    private_key.zeroize();
    assert!(private_key.expose_secret().iter().all(|byte| *byte == 0));
}

#[test]
fn system_identity_detects_corruption_on_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("system");
    let store = SqliteSystemIdentityStore::open(&root).unwrap();
    store.initialize(identity_seed(b"private-key-one")).unwrap();
    drop(store);

    let connection = rusqlite::Connection::open(root.join("agent-state.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE system_identity SET installation_id = 'different-installation'",
            [],
        )
        .unwrap();
    drop(connection);

    let reopened = SqliteSystemIdentityStore::open(&root).unwrap();
    assert_eq!(
        reopened.load().unwrap_err().code(),
        AgentErrorCode::Internal
    );
}

fn identity_seed(private_key: &[u8]) -> SystemIdentitySeed {
    SystemIdentitySeed::new(
        "bootstrap-request-a",
        "installation-a",
        PrivateKeyMaterial::new(private_key).unwrap(),
    )
    .unwrap()
}

fn certificate(
    certificate_generation: u64,
    session_generation: u64,
    mount_generation: u64,
    owner_generation: u64,
) -> AgentCertificateState {
    AgentCertificateState {
        certificate_chain_pem: vec![
            "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----".to_owned(),
        ],
        certificate_generation,
        session_generation,
        mount_generation,
        owner_generation,
        public_key_fingerprint: None,
        identity_uri: None,
        not_before_unix_ms: None,
        not_after_unix_ms: None,
        renew_at_unix_ms: None,
    }
}

fn assignment(seed: u8) -> AddAssignment {
    let mut assignment = AddAssignment {
        job_id: JobId::new(format!("job-{seed}")).unwrap(),
        task_fence: TaskExecutionFence::new(
            TaskId::new(format!("task-{seed}")).unwrap(),
            Generation::new(1),
            "scan_changes",
            Generation::new(1),
            Generation::new(1),
        ),
        assignment_id: AssignmentId::new(format!("assignment-{seed}")).unwrap(),
        assignment_generation: AssignmentGeneration::new(1),
        agent_id: AgentId::new("agent-a").unwrap(),
        principal: PrincipalRef {
            kind: PrincipalKind::Service,
            id: PrincipalId::new("service-a").unwrap(),
            extensions: Extensions::new(),
        },
        tenant_id: TenantId::new("tenant-a").unwrap(),
        project_id: ProjectId::new("project-a").unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        workspace_id: WorkspaceId::new("workspace-a").unwrap(),
        edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
        artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
        placement_generation: PlacementGeneration::new(1),
        agent_mount_id: AgentMountId::new("mount-a").unwrap(),
        mount_generation: MountGeneration::new(1),
        owner_generation: OwnerGeneration::new(1),
        expected_index_version: WireIndexVersion {
            revision: IndexRevision::new(7),
            digest: ContentDigest::from_bytes([7; 32]),
            extensions: Extensions::new(),
        },
        data_layout: CommitDataLayout::FastCdc,
        max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
        lease: None,
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(100_000),
        paths: Vec::new(),
        all: true,
        extensions: Extensions::new(),
    };
    assignment.request_digest = assignment.computed_request_digest().unwrap();
    assignment
}

fn ledger_config(root: &std::path::Path) -> SqliteLedgerConfig {
    SqliteLedgerConfig::new(
        root,
        AgentId::new("agent-a").unwrap(),
        TenantId::new("tenant-a").unwrap(),
    )
}

#[cfg(unix)]
fn assert_secure_permissions(root: &std::path::Path, files: &[&str]) {
    use std::os::unix::fs::PermissionsExt;

    assert_eq!(
        std::fs::metadata(root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for file in files {
        assert_eq!(
            std::fs::metadata(root.join(file))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[cfg(not(unix))]
fn assert_secure_permissions(_root: &std::path::Path, _files: &[&str]) {}
