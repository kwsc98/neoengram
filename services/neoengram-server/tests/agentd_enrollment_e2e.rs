use std::{
    env,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use neoengram_agentd::{
    check_health, has_pending_outbound_reports, load_persisted_identity, run_with,
    run_with_transports, AgentConfig, AgentDaemonResult, FilesystemMountObservation, HealthMode,
    LoggingConfig, LoggingFormat, MountProbe, MountProbeCondition, PvcReference,
    RegistrationConfig, ReqwestAgentSessionClient, ReqwestEnrollmentClient, SessionConfig,
    StorageAccessMode, StorageBackendType, StorageConfig,
};
use neoengram_core::ObjectId;
use neoengram_protocol::{
    AgentEnrollmentTokenId, AgentMountIdentityDigest, ContentDigest, EdgeClusterId, JobId,
    MountAccessMode, ResourceHealth, StorageVolumeId, TenantId, VolumeMarkerId,
};
use neoengram_server::{AppState, Config as ServerConfig};
use neoengramd::{open_sqlite_authority, JobKey, PreCommitId, PreCommitKey, SqliteAuthorityConfig};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};
use url::Url;

const API_TOKEN: &str = "agentd-e2e-secret";
const VOLUME_ID: &str = "volume-agentd-e2e";
const HEALTH_FILE: &str = "runtime-health.json";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_agentd_enrolls_over_both_listeners_and_restarts_without_token() {
    let authority = TempDir::new().unwrap();
    let keyring_path = authority.path().join("enrollment-keyring.json");
    write_keyring(&keyring_path);
    let server_config = development_server_config(authority.path().to_path_buf(), keyring_path);
    let state = AppState::initialize(&server_config).await.unwrap();
    let public_server = state.start_server(&server_config).await.unwrap();
    let public_addr = public_server.local_addr();
    let agent_addr = state.agent_local_addr().await.unwrap();
    let client = Client::new();

    let intent = enrollment_intent("agentd-e2e", VOLUME_ID, "agentd-e2e-data");
    let token_response = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/token/create",
        &intent,
    )
    .await;
    let bootstrap_token = token_response["bootstrap_token"].as_str().unwrap();
    let token_id = token_response["token_id"].as_str().unwrap();

    let agent_root = TempDir::new().unwrap();
    let state_dir = agent_root.path().join("state");
    let token_path = agent_root.path().join("bootstrap-token");
    let mount_path = agent_root.path().join("volume");
    std::fs::create_dir(&mount_path).unwrap();
    std::fs::write(
        mount_path.join(".neoengram-volume-marker"),
        format!("{VOLUME_ID}\n"),
    )
    .unwrap();
    std::fs::write(&token_path, bootstrap_token).unwrap();
    let agent_config = agent_config(
        agent_root.path(),
        token_id,
        agent_addr,
        descriptor_digest(&intent),
        VOLUME_ID,
        "agentd-e2e-data",
    );
    let probe = ReadyProbe::new(VOLUME_ID);

    let (first_shutdown, first_task) = spawn_full_agent(agent_config.clone(), probe.clone());
    let pending = wait_for_pending_enrollment(&client, public_addr, VOLUME_ID).await;
    assert_eq!(pending["state"], "pending_approval");

    let enrollment_id = pending["storage_enrollment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let approved = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/approve",
        &json!({
            "tenant_id": "tenant-a",
            "storage_enrollment_id": enrollment_id,
            "approval_request_id": "approve-agentd-e2e",
            "expected_resource_version": pending["resource_version"].clone(),
            "confirm_replacement": false
        }),
    )
    .await;
    assert_eq!(approved["enrollment"]["state"], "approved");

    wait_for_active_phase(&state_dir, "session_ready").await;
    check_health(&state_dir, HealthMode::Startup).unwrap();
    check_health(&state_dir, HealthMode::Live).unwrap();
    check_health(&state_dir, HealthMode::Ready).unwrap();
    stop_agent(first_shutdown, first_task).await;
    assert!(check_health(&state_dir, HealthMode::Startup).is_err());
    assert!(check_health(&state_dir, HealthMode::Live).is_err());

    let persisted = load_persisted_identity(&state_dir).unwrap().unwrap();
    assert_eq!(persisted.revision, 2);
    assert_eq!(
        persisted.approved_enrollment_id.as_deref(),
        Some(enrollment_id.as_str())
    );
    assert!(persisted
        .approved_agent_id
        .as_deref()
        .is_some_and(|agent_id| agent_id.starts_with("ngagent_")));

    std::fs::remove_file(&token_path).unwrap();
    let (restart_shutdown, restart_task) = spawn_full_agent(agent_config, probe);
    wait_for_active_phase(&state_dir, "session_ready").await;
    check_health(&state_dir, HealthMode::Startup).unwrap();
    check_health(&state_dir, HealthMode::Live).unwrap();
    check_health(&state_dir, HealthMode::Ready).unwrap();
    stop_agent(restart_shutdown, restart_task).await;

    let restarted = load_persisted_identity(&state_dir).unwrap().unwrap();
    assert_eq!(restarted, persisted);
    assert!(!token_path.exists());

    public_server.shutdown().await.unwrap();
    state.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn real_agentd_scans_volume_cas_and_publishes_over_h2() {
    const PROJECT_ID: &str = "project-agentd-e2e";
    const ARTIFACT_ID: &str = "artifact-agentd-e2e";
    const PLAYGROUND_ID: &str = "playground-agentd-e2e";
    const DERIVED_PLAYGROUND_ID: &str = "playground-agentd-derived";
    const JOB_ID: &str = "job-agentd-e2e";
    const RESTART_JOB_ID: &str = "job-agentd-e2e-after-restart";
    const SERVER_RESTART_JOB_ID: &str = "job-agentd-e2e-after-server-restart";
    const FILE_CONTENT: &[u8] = b"real Agent vertical slice\n";
    const RESTART_FILE_CONTENT: &[u8] = b"real Agent payload after restart\n";
    const SERVER_RESTART_FILE_CONTENT: &[u8] = b"real payload after authority reopen\n";
    const PRECOMMIT_FILE_CONTENT: &[u8] = b"real workspace payload committed through Pre-commit\n";
    const SECOND_COMMIT_FILE_CONTENT: &[u8] =
        b"real workspace payload modified after the first Commit\n";

    let authority = TempDir::new().unwrap();
    let keyring_path = authority.path().join("enrollment-keyring.json");
    write_keyring(&keyring_path);
    let server_config = development_server_config(authority.path().join("authority"), keyring_path);
    let state = AppState::initialize(&server_config).await.unwrap();
    let public_server = state.start_server(&server_config).await.unwrap();
    let public_addr = public_server.local_addr();
    let agent_addr = state.agent_local_addr().await.unwrap();
    let client = Client::new();

    let intent = enrollment_intent("vertical", VOLUME_ID, "agentd-vertical-data");
    let token_response = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/token/create",
        &intent,
    )
    .await;
    let agent_root = TempDir::new().unwrap();
    let prepared = prepare_persistent_agent(
        agent_root.path(),
        agent_addr,
        &intent,
        &token_response,
        VOLUME_ID,
        "agentd-vertical-data",
    );
    let state_dir = prepared.config.storage.state_dir.clone();
    let mount_path = prepared.config.storage.mount_path.clone();
    let volume_object_root = mount_path.join(".neoengram/objects");
    let token_path = prepared.token_path.clone();
    let mut agent_config = prepared.config.clone();
    let agent_probe = prepared.probe.clone();
    let (agent_shutdown, agent_task) = spawn_full_agent(agent_config.clone(), agent_probe.clone());

    let pending = wait_for_pending_enrollment(&client, public_addr, VOLUME_ID).await;
    let enrollment_id = pending["storage_enrollment_id"].as_str().unwrap();
    let approved = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/approve",
        &json!({
            "tenant_id": "tenant-a",
            "storage_enrollment_id": enrollment_id,
            "approval_request_id": "approve-agentd-vertical",
            "expected_resource_version": pending["resource_version"].clone(),
            "confirm_replacement": false
        }),
    )
    .await;
    assert_eq!(approved["enrollment"]["state"], "approved");

    wait_for_active_phase(&state_dir, "session_ready").await;
    wait_for_volume_ready(&client, public_addr, VOLUME_ID).await;
    check_health(&state_dir, HealthMode::Ready).unwrap();

    let created_artifact = post_public(
        &client,
        public_addr,
        "/api/artifact/create",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "display_name": "Agentd vertical artifact",
            "initialization": { "mode": "empty" }
        }),
    )
    .await;
    assert_eq!(created_artifact["artifact"]["head_commit_id"], Value::Null);

    let created_playground = post_public(
        &client,
        public_addr,
        "/api/playground/create",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID,
            "storage_volume_id": VOLUME_ID,
            "display_name": "Agentd vertical playground"
        }),
    )
    .await;
    assert_eq!(created_playground["playground"]["state"], "creating");
    let ready_playground =
        wait_for_playground_ready(&client, public_addr, PROJECT_ID, ARTIFACT_ID, PLAYGROUND_ID)
            .await;
    let initial_index = ready_playground["index_version"].clone();
    assert_eq!(initial_index["revision"], "0");

    let playground_path = mount_path
        .join("playgrounds")
        .join(PROJECT_ID)
        .join(ARTIFACT_ID)
        .join(PLAYGROUND_ID);
    assert!(playground_path.is_dir());
    std::fs::write(playground_path.join("observed.txt"), FILE_CONTENT).unwrap();

    let created_job = post_public(
        &client,
        public_addr,
        "/api/job/add/create",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID,
            "job_id": JOB_ID,
            "expected_index_version": initial_index.clone(),
            "deadline_unix_ms": deadline_after(Duration::from_secs(60)),
            "paths": [],
            "all": true
        }),
    )
    .await;
    assert_eq!(created_job["job"]["state"], "assigned");

    let succeeded = wait_for_job_succeeded(&client, public_addr, JOB_ID).await;
    assert_eq!(succeeded["decision"]["outcome"], "publish");

    let object_id = ObjectId::for_bytes(FILE_CONTENT);
    let object_path = volume_object_root
        .join("tenants/tenant-a/artifacts")
        .join(ARTIFACT_ID)
        .join("objects")
        .join(object_id.to_hex());
    assert_eq!(std::fs::read(&object_path).unwrap(), FILE_CONTENT);
    assert_no_chunk_payload_files(authority.path());
    assert_no_chunk_payload_files(&state_dir);

    let published_playground = post_public(
        &client,
        public_addr,
        "/api/playground/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID
        }),
    )
    .await;
    let published_index = published_playground["playground"]["index_version"].clone();
    assert_eq!(published_index["revision"], "1");
    assert_ne!(published_index["digest"], initial_index["digest"]);

    stop_agent(agent_shutdown, agent_task).await;
    assert_agent_outbound_empty(&state_dir);
    std::fs::remove_file(&token_path).unwrap();
    let (restart_shutdown, restart_task) =
        spawn_full_agent(agent_config.clone(), agent_probe.clone());
    wait_for_active_phase(&state_dir, "session_ready").await;
    wait_for_volume_ready(&client, public_addr, VOLUME_ID).await;

    std::fs::write(
        playground_path.join("after-restart.txt"),
        RESTART_FILE_CONTENT,
    )
    .unwrap();
    let restarted_job = post_public(
        &client,
        public_addr,
        "/api/job/add/create",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID,
            "job_id": RESTART_JOB_ID,
            "expected_index_version": published_index.clone(),
            "deadline_unix_ms": deadline_after(Duration::from_secs(60)),
            "paths": [],
            "all": true
        }),
    )
    .await;
    assert_eq!(restarted_job["job"]["state"], "assigned");
    wait_for_job_succeeded(&client, public_addr, RESTART_JOB_ID).await;

    let restart_object_id = ObjectId::for_bytes(RESTART_FILE_CONTENT);
    let restart_object_path = volume_object_root
        .join("tenants/tenant-a/artifacts")
        .join(ARTIFACT_ID)
        .join("objects")
        .join(restart_object_id.to_hex());
    assert_eq!(
        std::fs::read(&restart_object_path).unwrap(),
        RESTART_FILE_CONTENT
    );
    let restarted_playground = post_public(
        &client,
        public_addr,
        "/api/playground/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID
        }),
    )
    .await;
    let restarted_index = restarted_playground["playground"]["index_version"].clone();
    assert_eq!(restarted_index["revision"], "2");
    assert_ne!(restarted_index["digest"], published_index["digest"]);
    assert!(!token_path.exists());

    stop_agent(restart_shutdown, restart_task).await;
    assert_agent_outbound_empty(&state_dir);
    public_server.shutdown().await.unwrap();
    state.close().await;

    let restarted_state = AppState::initialize(&server_config).await.unwrap();
    let restarted_public_server = restarted_state.start_server(&server_config).await.unwrap();
    let restarted_public_addr = restarted_public_server.local_addr();
    let restarted_agent_addr = restarted_state.agent_local_addr().await.unwrap();
    agent_config.central_endpoint = Url::parse(&format!("http://{restarted_agent_addr}/")).unwrap();
    let (server_restart_shutdown, server_restart_task) =
        spawn_full_agent(agent_config, agent_probe);
    wait_for_active_phase(&state_dir, "session_ready").await;
    wait_for_volume_ready(&client, restarted_public_addr, VOLUME_ID).await;

    assert_eq!(std::fs::read(&object_path).unwrap(), FILE_CONTENT);
    assert_eq!(
        std::fs::read(&restart_object_path).unwrap(),
        RESTART_FILE_CONTENT
    );
    std::fs::remove_file(playground_path.join("observed.txt")).unwrap();
    std::fs::write(
        playground_path.join("after-server-restart.txt"),
        SERVER_RESTART_FILE_CONTENT,
    )
    .unwrap();
    let server_restarted_job = post_public(
        &client,
        restarted_public_addr,
        "/api/job/add/create",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID,
            "job_id": SERVER_RESTART_JOB_ID,
            "expected_index_version": restarted_index.clone(),
            "deadline_unix_ms": deadline_after(Duration::from_secs(60)),
            "paths": [],
            "all": true
        }),
    )
    .await;
    assert_eq!(server_restarted_job["job"]["state"], "assigned");
    wait_for_job_succeeded(&client, restarted_public_addr, SERVER_RESTART_JOB_ID).await;

    let server_restart_object_id = ObjectId::for_bytes(SERVER_RESTART_FILE_CONTENT);
    let server_restart_object_path = volume_object_root
        .join("tenants/tenant-a/artifacts")
        .join(ARTIFACT_ID)
        .join("objects")
        .join(server_restart_object_id.to_hex());
    assert_eq!(
        std::fs::read(server_restart_object_path).unwrap(),
        SERVER_RESTART_FILE_CONTENT
    );
    let server_restarted_playground = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID
        }),
    )
    .await;
    let server_restarted_index = &server_restarted_playground["playground"]["index_version"];
    assert_eq!(server_restarted_index["revision"], "3");
    assert_ne!(server_restarted_index["digest"], restarted_index["digest"]);
    assert!(!token_path.exists());

    std::fs::write(
        playground_path.join("commit-candidate.txt"),
        PRECOMMIT_FILE_CONTENT,
    )
    .unwrap();
    let started_precommit = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/precommit/start",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID,
            "precommit_request_id": "precommit-request-agentd-e2e",
            "expected_index_version": server_restarted_index.clone()
        }),
    )
    .await;
    assert_eq!(started_precommit["precommit"]["state"], "running");
    assert_eq!(started_precommit["replayed"], false);
    let precommit_id = started_precommit["precommit"]["precommit_id"]
        .as_str()
        .expect("Pre-commit response omitted precommit_id")
        .to_owned();
    let ready_precommit =
        wait_for_precommit_ready(&client, restarted_public_addr, precommit_id.as_str()).await;
    assert_eq!(ready_precommit["phase"], "idle");
    assert_eq!(ready_precommit["blockers"], json!([]));
    let candidate_index = ready_precommit["candidate_index_version"].clone();
    assert_eq!(candidate_index["revision"], "4");
    assert_ne!(candidate_index["digest"], server_restarted_index["digest"]);

    let precommit_object_id = ObjectId::for_bytes(PRECOMMIT_FILE_CONTENT);
    let precommit_object_path = volume_object_root
        .join("tenants/tenant-a/artifacts")
        .join(ARTIFACT_ID)
        .join("objects")
        .join(precommit_object_id.to_hex());
    assert_eq!(
        std::fs::read(precommit_object_path).unwrap(),
        PRECOMMIT_FILE_CONTENT
    );

    let commit_request = json!({
        "tenant_id": "tenant-a",
        "project_id": PROJECT_ID,
        "artifact_id": ARTIFACT_ID,
        "playground_id": PLAYGROUND_ID,
        "commit_request_id": "commit-request-agentd-e2e",
        "precommit_id": precommit_id,
        "expected_candidate_index_version": candidate_index,
        "message": "Commit the real Agent workspace",
        "description": "Created by the real Server-Agent vertical slice",
        "tag_names": ["agentd-e2e"]
    });
    let committed = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/commit/create",
        &commit_request,
    )
    .await;
    assert_eq!(committed["replayed"], false);
    assert_eq!(committed["commit"]["parent_commit_id"], Value::Null);
    assert_eq!(committed["consumed_precommit"]["state"], "committed");
    assert_eq!(
        committed["consumed_precommit"]["candidate_index_version"],
        commit_request["expected_candidate_index_version"]
    );
    let commit_id = committed["commit"]["commit_id"]
        .as_str()
        .expect("Commit response omitted commit_id")
        .to_owned();
    assert_eq!(
        committed["consumed_precommit"]["committed_commit_id"],
        commit_id
    );
    assert_eq!(committed["playground"]["head_commit_id"], commit_id);
    assert_eq!(committed["playground"]["active_precommit_id"], Value::Null);

    let committed_artifact = post_public(
        &client,
        restarted_public_addr,
        "/api/artifact/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID
        }),
    )
    .await;
    let committed_playground = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID
        }),
    )
    .await;
    assert_eq!(committed_artifact["artifact"]["head_commit_id"], commit_id);
    assert_eq!(
        committed_playground["playground"]["head_commit_id"],
        commit_id
    );
    assert_eq!(
        committed_playground["playground"]["index_version"],
        commit_request["expected_candidate_index_version"]
    );
    assert_eq!(
        committed_playground["playground"]["active_precommit_id"],
        Value::Null
    );

    let commit_replay = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/commit/create",
        &commit_request,
    )
    .await;
    assert_eq!(commit_replay["replayed"], true);
    assert_eq!(commit_replay["commit"], committed["commit"]);
    assert_eq!(
        commit_replay["consumed_precommit"]["committed_commit_id"],
        commit_id
    );

    std::fs::write(
        playground_path.join("commit-candidate.txt"),
        SECOND_COMMIT_FILE_CONTENT,
    )
    .unwrap();
    std::fs::remove_file(playground_path.join("after-restart.txt")).unwrap();
    assert_eq!(
        std::fs::read(playground_path.join("after-server-restart.txt")).unwrap(),
        SERVER_RESTART_FILE_CONTENT
    );

    let second_started_precommit = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/precommit/start",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID,
            "precommit_request_id": "precommit-request-agentd-e2e-second",
            "expected_index_version": committed_playground["playground"]["index_version"].clone()
        }),
    )
    .await;
    assert_eq!(second_started_precommit["precommit"]["state"], "running");
    assert_eq!(second_started_precommit["replayed"], false);
    let second_precommit_id = second_started_precommit["precommit"]["precommit_id"]
        .as_str()
        .expect("second Pre-commit response omitted precommit_id")
        .to_owned();
    let second_ready_precommit =
        wait_for_precommit_ready(&client, restarted_public_addr, second_precommit_id.as_str())
            .await;
    assert_eq!(second_ready_precommit["phase"], "idle");
    assert_eq!(second_ready_precommit["blockers"], json!([]));
    let second_candidate_index = second_ready_precommit["candidate_index_version"].clone();
    assert_eq!(second_candidate_index["revision"], "5");
    assert_ne!(
        second_candidate_index["digest"],
        committed_playground["playground"]["index_version"]["digest"]
    );

    let frozen_changes = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/change/list/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID,
            "precommit_id": second_precommit_id,
            "page_size": 100
        }),
    )
    .await;
    assert_eq!(frozen_changes["source"], "precommit");
    assert_eq!(
        frozen_changes["precommit_id"],
        second_started_precommit["precommit"]["precommit_id"]
    );
    assert_eq!(frozen_changes["summary"]["files_added"], "0");
    assert_eq!(frozen_changes["summary"]["files_modified"], "1");
    assert_eq!(frozen_changes["summary"]["files_deleted"], "1");
    let frozen_change_items = frozen_changes["items"]
        .as_array()
        .expect("frozen change list omitted items");
    assert_eq!(frozen_change_items.len(), 2, "{frozen_changes}");
    assert!(frozen_change_items.iter().any(|item| {
        item["path"] == "commit-candidate.txt" && item["change_type"] == "modified"
    }));
    assert!(frozen_change_items
        .iter()
        .any(|item| { item["path"] == "after-restart.txt" && item["change_type"] == "deleted" }));
    assert!(!frozen_change_items
        .iter()
        .any(|item| item["path"] == "after-server-restart.txt"));

    let second_object_id = ObjectId::for_bytes(SECOND_COMMIT_FILE_CONTENT);
    let second_object_path = volume_object_root
        .join("tenants/tenant-a/artifacts")
        .join(ARTIFACT_ID)
        .join("objects")
        .join(second_object_id.to_hex());
    assert_eq!(
        std::fs::read(second_object_path).unwrap(),
        SECOND_COMMIT_FILE_CONTENT
    );

    let second_commit_request = json!({
        "tenant_id": "tenant-a",
        "project_id": PROJECT_ID,
        "artifact_id": ARTIFACT_ID,
        "playground_id": PLAYGROUND_ID,
        "commit_request_id": "commit-request-agentd-e2e-second",
        "precommit_id": second_precommit_id,
        "expected_candidate_index_version": second_candidate_index,
        "message": "Commit a second real Agent workspace revision",
        "description": "Modifies and deletes tracked files after the root Commit",
        "tag_names": ["agentd-e2e", "second"]
    });
    let second_committed = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/commit/create",
        &second_commit_request,
    )
    .await;
    assert_eq!(second_committed["replayed"], false);
    assert_eq!(second_committed["commit"]["parent_commit_id"], commit_id);
    assert_eq!(second_committed["consumed_precommit"]["state"], "committed");
    let second_commit_id = second_committed["commit"]["commit_id"]
        .as_str()
        .expect("second Commit response omitted commit_id")
        .to_owned();
    assert_ne!(second_commit_id, commit_id);
    assert_eq!(
        second_committed["playground"]["head_commit_id"],
        second_commit_id
    );
    assert_eq!(
        second_committed["playground"]["active_precommit_id"],
        Value::Null
    );

    let second_committed_artifact = post_public(
        &client,
        restarted_public_addr,
        "/api/artifact/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID
        }),
    )
    .await;
    let second_committed_playground = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/query",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": PLAYGROUND_ID
        }),
    )
    .await;
    assert_eq!(
        second_committed_artifact["artifact"]["head_commit_id"],
        second_commit_id
    );
    assert_eq!(
        second_committed_playground["playground"]["head_commit_id"],
        second_commit_id
    );
    assert_eq!(
        second_committed_playground["playground"]["index_version"],
        second_commit_request["expected_candidate_index_version"]
    );

    let second_commit_replay = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/commit/create",
        &second_commit_request,
    )
    .await;
    assert_eq!(second_commit_replay["replayed"], true);
    assert_eq!(second_commit_replay["commit"], second_committed["commit"]);

    let derived = post_public(
        &client,
        restarted_public_addr,
        "/api/playground/create",
        &json!({
            "tenant_id": "tenant-a",
            "project_id": PROJECT_ID,
            "artifact_id": ARTIFACT_ID,
            "playground_id": DERIVED_PLAYGROUND_ID,
            "storage_volume_id": VOLUME_ID,
            "display_name": "Materialized from the Artifact Head"
        }),
    )
    .await;
    assert_eq!(derived["playground"]["state"], "creating", "{derived}");
    assert_eq!(derived["playground"]["base_commit_id"], second_commit_id);
    assert_eq!(derived["playground"]["head_commit_id"], second_commit_id);
    let ready_derived = wait_for_playground_ready(
        &client,
        restarted_public_addr,
        PROJECT_ID,
        ARTIFACT_ID,
        DERIVED_PLAYGROUND_ID,
    )
    .await;
    assert_eq!(
        ready_derived["index_version"],
        second_commit_request["expected_candidate_index_version"]
    );
    let derived_path = mount_path
        .join("playgrounds")
        .join(PROJECT_ID)
        .join(ARTIFACT_ID)
        .join(DERIVED_PLAYGROUND_ID);
    assert_eq!(
        std::fs::read(derived_path.join("commit-candidate.txt")).unwrap(),
        SECOND_COMMIT_FILE_CONTENT
    );
    assert_eq!(
        std::fs::read(derived_path.join("after-server-restart.txt")).unwrap(),
        SERVER_RESTART_FILE_CONTENT
    );
    let mut derived_entries = std::fs::read_dir(&derived_path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    derived_entries.sort();
    assert_eq!(
        derived_entries,
        vec![
            "after-server-restart.txt".to_owned(),
            "commit-candidate.txt".to_owned()
        ]
    );

    // Cover the 500 ms durable Decision delivery pass and the Agent's 100 ms report pass.
    sleep(Duration::from_secs(1)).await;
    stop_agent(server_restart_shutdown, server_restart_task).await;
    assert_agent_outbound_empty(&state_dir);
    restarted_public_server.shutdown().await.unwrap();
    restarted_state.close().await;
    assert_finalized_acks(
        &server_config.authority_dir,
        &[JOB_ID, RESTART_JOB_ID, SERVER_RESTART_JOB_ID],
        &[precommit_id.as_str(), second_precommit_id.as_str()],
    )
    .await;
    assert_no_chunk_payload_files(authority.path());
    assert_no_chunk_payload_files(&state_dir);
}

/// Manual acceptance test that preserves both Agents' state for inspection.
///
/// The configured roots must already exist and be absolute. Each invocation creates a new,
/// exclusive child directory below each root and never removes either run directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires NEOENGRAM_AGENT1_ROOT and NEOENGRAM_AGENT2_ROOT"]
async fn two_persistent_agent_roots_enroll_and_restart_without_tokens() {
    let agent1_base = configured_agent_root("NEOENGRAM_AGENT1_ROOT");
    let agent2_base = configured_agent_root("NEOENGRAM_AGENT2_ROOT");
    assert_ne!(
        agent1_base, agent2_base,
        "the two Agent roots must resolve to different directories"
    );

    let agent1_root = create_unique_run_dir(&agent1_base, "agent1");
    let agent2_root = create_unique_run_dir(&agent2_base, "agent2");

    let authority = TempDir::new().unwrap();
    let keyring_path = authority.path().join("enrollment-keyring.json");
    write_keyring(&keyring_path);
    let server_config = development_server_config(authority.path().to_path_buf(), keyring_path);
    let state = AppState::initialize(&server_config).await.unwrap();
    let public_server = state.start_server(&server_config).await.unwrap();
    let public_addr = public_server.local_addr();
    let agent_addr = state.agent_local_addr().await.unwrap();
    let client = Client::new();

    let intent1 = enrollment_intent(
        "manual-agent1",
        "volume-manual-agent1",
        "manual-agent1-data",
    );
    let intent2 = enrollment_intent(
        "manual-agent2",
        "volume-manual-agent2",
        "manual-agent2-data",
    );
    let (token1, token2) = tokio::join!(
        post_public(
            &client,
            public_addr,
            "/api/storage/enrollment/token/create",
            &intent1,
        ),
        post_public(
            &client,
            public_addr,
            "/api/storage/enrollment/token/create",
            &intent2,
        )
    );

    let agent1 = prepare_persistent_agent(
        &agent1_root,
        agent_addr,
        &intent1,
        &token1,
        "volume-manual-agent1",
        "manual-agent1-data",
    );
    let agent2 = prepare_persistent_agent(
        &agent2_root,
        agent_addr,
        &intent2,
        &token2,
        "volume-manual-agent2",
        "manual-agent2-data",
    );

    let (agent1_shutdown, agent1_task) = spawn_agent(agent1.config.clone(), agent1.probe.clone());
    let (agent2_shutdown, agent2_task) = spawn_agent(agent2.config.clone(), agent2.probe.clone());

    let (pending1, pending2) = tokio::join!(
        wait_for_pending_enrollment(&client, public_addr, "volume-manual-agent1"),
        wait_for_pending_enrollment(&client, public_addr, "volume-manual-agent2")
    );
    assert_eq!(pending1["state"], "pending_approval");
    assert_eq!(pending2["state"], "pending_approval");
    let enrollment1 = pending1["storage_enrollment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let enrollment2 = pending2["storage_enrollment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(enrollment1, enrollment2);

    let approve1 = json!({
        "tenant_id": "tenant-a",
        "storage_enrollment_id": enrollment1.clone(),
        "approval_request_id": "approve-manual-agent1",
        "expected_resource_version": pending1["resource_version"].clone(),
        "confirm_replacement": false
    });
    let approve2 = json!({
        "tenant_id": "tenant-a",
        "storage_enrollment_id": enrollment2.clone(),
        "approval_request_id": "approve-manual-agent2",
        "expected_resource_version": pending2["resource_version"].clone(),
        "confirm_replacement": false
    });
    let (approved1, approved2) = tokio::join!(
        post_public(
            &client,
            public_addr,
            "/api/storage/enrollment/approve",
            &approve1,
        ),
        post_public(
            &client,
            public_addr,
            "/api/storage/enrollment/approve",
            &approve2,
        )
    );
    assert_eq!(approved1["enrollment"]["state"], "approved");
    assert_eq!(approved2["enrollment"]["state"], "approved");
    assert_eq!(
        approved1["enrollment"]["storage_volume_id"],
        "volume-manual-agent1"
    );
    assert_eq!(
        approved2["enrollment"]["storage_volume_id"],
        "volume-manual-agent2"
    );

    tokio::join!(
        wait_for_active_phase(&agent1.state_dir, "approved_waiting_certificate"),
        wait_for_active_phase(&agent2.state_dir, "approved_waiting_certificate")
    );
    assert_live_not_ready(&agent1.state_dir);
    assert_live_not_ready(&agent2.state_dir);
    tokio::join!(
        stop_agent(agent1_shutdown, agent1_task),
        stop_agent(agent2_shutdown, agent2_task)
    );

    let persisted1 = load_persisted_identity(&agent1.state_dir)
        .unwrap()
        .expect("Agent 1 identity was not persisted");
    let persisted2 = load_persisted_identity(&agent2.state_dir)
        .unwrap()
        .expect("Agent 2 identity was not persisted");
    assert_eq!(persisted1.revision, 2);
    assert_eq!(persisted2.revision, 2);
    assert_eq!(
        persisted1.approved_enrollment_id.as_deref(),
        Some(enrollment1.as_str())
    );
    assert_eq!(
        persisted2.approved_enrollment_id.as_deref(),
        Some(enrollment2.as_str())
    );
    assert_ne!(persisted1.installation_id, persisted2.installation_id);
    assert_ne!(
        persisted1.bootstrap_request_id,
        persisted2.bootstrap_request_id
    );
    assert_ne!(persisted1.approved_agent_id, persisted2.approved_agent_id);

    std::fs::remove_file(&agent1.token_path).unwrap();
    std::fs::remove_file(&agent2.token_path).unwrap();
    let (agent1_restart_shutdown, agent1_restart_task) = spawn_agent(agent1.config, agent1.probe);
    let (agent2_restart_shutdown, agent2_restart_task) = spawn_agent(agent2.config, agent2.probe);
    tokio::join!(
        wait_for_active_phase(&agent1.state_dir, "approved_waiting_certificate"),
        wait_for_active_phase(&agent2.state_dir, "approved_waiting_certificate")
    );
    assert_live_not_ready(&agent1.state_dir);
    assert_live_not_ready(&agent2.state_dir);
    tokio::join!(
        stop_agent(agent1_restart_shutdown, agent1_restart_task),
        stop_agent(agent2_restart_shutdown, agent2_restart_task)
    );

    assert_eq!(
        load_persisted_identity(&agent1.state_dir).unwrap(),
        Some(persisted1)
    );
    assert_eq!(
        load_persisted_identity(&agent2.state_dir).unwrap(),
        Some(persisted2)
    );
    assert!(!agent1.token_path.exists());
    assert!(!agent2.token_path.exists());

    public_server.shutdown().await.unwrap();
    state.close().await;
}

struct PersistentAgent {
    config: AgentConfig,
    probe: ReadyProbe,
    state_dir: PathBuf,
    token_path: PathBuf,
}

fn prepare_persistent_agent(
    root: &Path,
    agent_addr: std::net::SocketAddr,
    intent: &Value,
    token: &Value,
    volume_id: &str,
    claim_name: &str,
) -> PersistentAgent {
    let volume_dir = root.join("volume");
    std::fs::create_dir(&volume_dir).unwrap();
    std::fs::write(
        volume_dir.join(".neoengram-volume-marker"),
        format!("{volume_id}\n"),
    )
    .unwrap();
    let state_dir = root.join("state");
    let token_path = root.join("bootstrap-token");
    std::fs::write(
        &token_path,
        token["bootstrap_token"]
            .as_str()
            .expect("token response omitted bootstrap_token"),
    )
    .unwrap();
    let config = agent_config(
        root,
        token["token_id"]
            .as_str()
            .expect("token response omitted token_id"),
        agent_addr,
        descriptor_digest(intent),
        volume_id,
        claim_name,
    );
    PersistentAgent {
        config,
        probe: ReadyProbe::new(volume_id),
        state_dir,
        token_path,
    }
}

fn configured_agent_root(variable: &str) -> PathBuf {
    let configured = PathBuf::from(
        env::var_os(variable).unwrap_or_else(|| panic!("{variable} must name an absolute path")),
    );
    assert!(
        configured.is_absolute(),
        "{variable} must be absolute, got {}",
        configured.display()
    );
    let root = configured
        .canonicalize()
        .unwrap_or_else(|error| panic!("failed to resolve {variable}: {error}"));
    assert!(root.is_dir(), "{variable} is not a directory");
    root
}

fn create_unique_run_dir(root: &Path, label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock predates the Unix epoch")
        .as_nanos();
    for attempt in 0_u8..32 {
        let run_dir = root.join(format!(
            "neoengram-enrollment-e2e-{label}-{}-{nonce}-{attempt}",
            std::process::id()
        ));
        match std::fs::create_dir(&run_dir) {
            Ok(()) => return run_dir,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("failed to create {}: {error}", run_dir.display()),
        }
    }
    panic!(
        "failed to allocate a unique run directory below {}",
        root.display()
    );
}

fn assert_live_not_ready(state_dir: &Path) {
    check_health(state_dir, HealthMode::Startup).unwrap();
    check_health(state_dir, HealthMode::Live).unwrap();
    assert!(check_health(state_dir, HealthMode::Ready).is_err());
}

#[derive(Clone)]
struct ReadyProbe {
    observation: FilesystemMountObservation,
}

impl ReadyProbe {
    fn new(volume_id: &str) -> Self {
        Self {
            observation: FilesystemMountObservation {
                observed_volume_marker: Some(VolumeMarkerId::new(volume_id).unwrap()),
                marker_matches: true,
                mount_boundary_detected: true,
                access_mode: Some(MountAccessMode::ReadWrite),
                rename_supported: true,
                fsync_supported: true,
                health: ResourceHealth::Ready,
                available_bytes: Some(1024 * 1024),
                mount_identity_digest: Some(AgentMountIdentityDigest::new(ContentDigest::hash(
                    format!("agentd-e2e-mount-{volume_id}").as_bytes(),
                ))),
                condition: MountProbeCondition::Ready,
            },
        }
    }
}

impl MountProbe for ReadyProbe {
    fn probe(&self) -> FilesystemMountObservation {
        self.observation.clone()
    }
}

fn spawn_agent(
    config: AgentConfig,
    probe: ReadyProbe,
) -> (oneshot::Sender<()>, JoinHandle<AgentDaemonResult<()>>) {
    let client = ReqwestEnrollmentClient::new(config.central_endpoint.clone()).unwrap();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        run_with(config, client, probe, async move {
            let _ = shutdown_receiver.await;
        })
        .await
    });
    (shutdown_sender, task)
}

fn spawn_full_agent(
    config: AgentConfig,
    probe: ReadyProbe,
) -> (oneshot::Sender<()>, JoinHandle<AgentDaemonResult<()>>) {
    let enrollment_client = ReqwestEnrollmentClient::new(config.central_endpoint.clone()).unwrap();
    let session_client = ReqwestAgentSessionClient::new(config.central_endpoint.clone()).unwrap();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        run_with_transports(
            config,
            enrollment_client,
            session_client,
            probe,
            async move {
                let _ = shutdown_receiver.await;
            },
        )
        .await
    });
    (shutdown_sender, task)
}

async fn stop_agent(shutdown: oneshot::Sender<()>, task: JoinHandle<AgentDaemonResult<()>>) {
    let _ = shutdown.send(());
    timeout(Duration::from_secs(10), task)
        .await
        .expect("Agent shutdown timed out")
        .expect("Agent task panicked")
        .expect("Agent shutdown failed");
}

fn assert_agent_outbound_empty(state_dir: &Path) {
    assert!(
        !has_pending_outbound_reports(state_dir, TenantId::new("tenant-a").unwrap()).unwrap(),
        "stopped Agent retained unacknowledged reports"
    );
}

fn assert_no_chunk_payload_files(root: &Path) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                pending.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                name.len() != 64 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "chunk payload escaped the Volume CAS: {}",
                entry.path().display()
            );
        }
    }
}

async fn wait_for_pending_enrollment(
    client: &Client,
    public_addr: std::net::SocketAddr,
    volume_id: &str,
) -> Value {
    timeout(Duration::from_secs(10), async {
        loop {
            let response = post_public(
                client,
                public_addr,
                "/api/storage/enrollment/list/query",
                &json!({"tenant_id": "tenant-a", "page_size": 10}),
            )
            .await;
            if let Some(pending) = response["items"].as_array().and_then(|items| {
                items.iter().find(|item| {
                    item["state"] == "pending_approval" && item["storage_volume_id"] == volume_id
                })
            }) {
                return pending.clone();
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("Agent bootstrap did not become pending")
}

async fn wait_for_volume_ready(
    client: &Client,
    public_addr: std::net::SocketAddr,
    volume_id: &str,
) {
    timeout(Duration::from_secs(15), async {
        loop {
            let response = post_public(
                client,
                public_addr,
                "/api/storage/volume/query",
                &json!({"tenant_id": "tenant-a", "storage_volume_id": volume_id}),
            )
            .await;
            if response["storage_volume"]["state"] == "ready" {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("Agent heartbeat did not make its Volume Ready");
}

async fn wait_for_playground_ready(
    client: &Client,
    public_addr: std::net::SocketAddr,
    project_id: &str,
    artifact_id: &str,
    playground_id: &str,
) -> Value {
    timeout(Duration::from_secs(15), async {
        loop {
            let response = post_public(
                client,
                public_addr,
                "/api/playground/query",
                &json!({
                    "tenant_id": "tenant-a",
                    "project_id": project_id,
                    "artifact_id": artifact_id,
                    "playground_id": playground_id
                }),
            )
            .await;
            match response["playground"]["state"].as_str() {
                Some("ready") => return response["playground"].clone(),
                Some("abnormal") => panic!("Playground materialization failed: {response}"),
                _ => sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .expect("Agent did not materialize the Playground")
}

async fn wait_for_job_succeeded(
    client: &Client,
    public_addr: std::net::SocketAddr,
    job_id: &str,
) -> Value {
    timeout(Duration::from_secs(30), async {
        loop {
            let response = post_public(
                client,
                public_addr,
                "/api/job/query",
                &json!({"tenant_id": "tenant-a", "job_id": job_id}),
            )
            .await;
            match response["job"]["state"].as_str() {
                Some("succeeded") => return response["job"].clone(),
                Some(
                    state @ ("conflicted" | "rejected" | "failed" | "cancelled" | "timed_out"
                    | "recovery_required"),
                ) => panic!("Agent Job entered terminal state {state}: {response}"),
                _ => sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("Agent Job did not succeed")
}

async fn wait_for_precommit_ready(
    client: &Client,
    public_addr: std::net::SocketAddr,
    precommit_id: &str,
) -> Value {
    timeout(Duration::from_secs(30), async {
        loop {
            let response = post_public(
                client,
                public_addr,
                "/api/playground/precommit/query",
                &json!({
                    "tenant_id": "tenant-a",
                    "precommit_id": precommit_id
                }),
            )
            .await;
            match response["precommit"]["state"].as_str() {
                Some("ready") => return response["precommit"].clone(),
                Some(state @ ("abnormal" | "cancelled" | "committed")) => {
                    panic!("Pre-commit entered unexpected state {state}: {response}")
                }
                _ => sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("Agent did not produce a ready Pre-commit")
}

async fn assert_finalized_acks(authority_dir: &Path, job_ids: &[&str], precommit_ids: &[&str]) {
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(authority_dir))
        .await
        .expect("failed to reopen the stopped authority");
    let store = authority.authority_store();
    let jobs = store.jobs();
    for job_id in job_ids {
        let key = JobKey::new(
            TenantId::new("tenant-a").unwrap(),
            JobId::new(*job_id).unwrap(),
        );
        let job = jobs
            .get(&key)
            .await
            .expect("failed to query the stopped authority")
            .expect("published Job disappeared from the stopped authority");
        assert!(
            job.finalized_ack.is_some(),
            "Agent did not durably acknowledge the final Decision for Job {job_id}"
        );
    }
    let precommits = store
        .precommits()
        .expect("stopped authority omitted the Pre-commit repository");
    for precommit_id in precommit_ids {
        let precommit = precommits
            .get(&PreCommitKey::new(
                TenantId::new("tenant-a").unwrap(),
                PreCommitId::new(*precommit_id).unwrap(),
            ))
            .await
            .expect("failed to query the stopped Pre-commit authority")
            .expect("committed Pre-commit disappeared from the stopped authority");
        let job = jobs
            .get(&JobKey::new(
                TenantId::new("tenant-a").unwrap(),
                precommit.job_id,
            ))
            .await
            .expect("failed to query the stopped Pre-commit Job authority")
            .expect("Pre-commit Job disappeared from the stopped authority");
        assert!(
            job.finalized_ack.is_some(),
            "Agent did not durably acknowledge the final Decision for Pre-commit {precommit_id}"
        );
    }
    authority.close().await;
}

async fn wait_for_active_phase(state_dir: &Path, expected: &str) {
    timeout(Duration::from_secs(10), async {
        loop {
            let phase_matches = std::fs::read(state_dir.join(HEALTH_FILE))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|document| document["phase"].as_str().map(str::to_owned))
                .is_some_and(|phase| phase == expected);
            if phase_matches && check_health(state_dir, HealthMode::Live).is_ok() {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        let observed = std::fs::read(state_dir.join(HEALTH_FILE))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|document| document["phase"].as_str().map(str::to_owned));
        panic!("Agent did not enter active phase {expected}; last observed phase was {observed:?}");
    });
}

async fn post_public(
    client: &Client,
    address: std::net::SocketAddr,
    path: &str,
    body: &Value,
) -> Value {
    let response = client
        .post(format!("http://{address}{path}"))
        .header("authorization", format!("Bearer {API_TOKEN}"))
        .header("neoengram-api-version", "1")
        .header("x-request-id", "req:agentd-e2e")
        .json(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    assert_eq!(
        response.headers().get("x-request-id").unwrap(),
        "req:agentd-e2e"
    );
    let json: Value = response.json().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{json}");
    json
}

fn enrollment_intent(label: &str, volume_id: &str, claim_name: &str) -> Value {
    json!({
        "tenant_id": "tenant-a",
        "token_request_id": format!("token-request-{label}"),
        "storage_volume_id": volume_id,
        "display_name": format!("{label} PVC"),
        "edge_cluster_id": "cluster-a",
        "region": "cn-east-1",
        "access_mode": "read_write_many",
        "pvc_reference": {
            "namespace": "neoengram-data",
            "claim_name": claim_name
        }
    })
}

fn descriptor_digest(intent: &Value) -> ContentDigest {
    let material = json!({
        "version": 1,
        "tenant_id": intent["tenant_id"],
        "storage_volume_id": intent["storage_volume_id"],
        "edge_cluster_id": intent["edge_cluster_id"],
        "descriptor": {
            "display_name": intent["display_name"],
            "region": intent["region"],
            "access_mode": intent["access_mode"],
            "pvc_reference": intent["pvc_reference"]
        }
    });
    ContentDigest::hash(serde_json_canonicalizer::to_vec(&material).unwrap())
}

fn deadline_after(duration: Duration) -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock predates the Unix epoch")
        .checked_add(duration)
        .expect("deadline overflows system duration")
        .as_millis()
        .to_string()
}

fn agent_config(
    root: &Path,
    token_id: &str,
    agent_addr: std::net::SocketAddr,
    volume_descriptor_digest: ContentDigest,
    volume_id: &str,
    claim_name: &str,
) -> AgentConfig {
    let mount_path = root.join("volume");
    let state_dir = root.join("state");
    let token_path = root.join("bootstrap-token");
    AgentConfig {
        schema_version: 1,
        protocol_version: 1,
        central_endpoint: Url::parse(&format!("http://{agent_addr}/")).unwrap(),
        tenant_id: TenantId::new("tenant-a").unwrap(),
        edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
        storage_volume_id: StorageVolumeId::new(volume_id).unwrap(),
        volume_descriptor_digest,
        region: "cn-east-1".to_owned(),
        storage: StorageConfig {
            backend_type: StorageBackendType::Pvc,
            access_mode: StorageAccessMode::ReadWriteMany,
            marker_file: mount_path.join(".neoengram-volume-marker"),
            mount_path,
            state_dir,
            expected_volume_marker: VolumeMarkerId::new(volume_id).unwrap(),
            pvc_reference: PvcReference {
                namespace: "neoengram-data".to_owned(),
                claim_name: claim_name.to_owned(),
            },
            hard_minimum_free_bytes: 0,
            ready_minimum_free_bytes: 0,
        },
        registration: RegistrationConfig {
            approval_required: true,
            token_id: AgentEnrollmentTokenId::new(token_id).unwrap(),
            bootstrap_token_file: token_path,
        },
        session: SessionConfig {
            heartbeat_interval_seconds: 10,
            reconnect_max_delay_seconds: 1,
        },
        logging: LoggingConfig {
            format: LoggingFormat::Json,
            level: "info".to_owned(),
        },
    }
}

fn write_keyring(path: &Path) {
    std::fs::write(
        path,
        serde_json::to_vec(&json!({
            "version": 1,
            "active_key_id": "enrollment-key-agentd-e2e",
            "keys": {
                "enrollment-key-agentd-e2e": URL_SAFE_NO_PAD.encode([0x5a_u8; 32])
            }
        }))
        .unwrap(),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn development_server_config(authority_dir: PathBuf, keyring: PathBuf) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        agent_enrollment_enabled: true,
        agent_bind: Some("127.0.0.1:0".parse().unwrap()),
        agent_enrollment_keyring_file: Some(keyring),
        authority_dir,
        rbac_file: None,
        oidc_issuer: None,
        oidc_audience: None,
        oidc_jwks_uri: None,
        development: true,
        development_token: Some(API_TOKEN.to_owned()),
        development_principal: "user-a".to_owned(),
        development_tenants: vec!["tenant-a".to_owned()],
        request_timeout_secs: 5,
        max_request_body_bytes: 64 * 1024,
        max_response_body_bytes: 64 * 1024,
        max_concurrent_requests: 16,
        graceful_shutdown_secs: 2,
    }
}
