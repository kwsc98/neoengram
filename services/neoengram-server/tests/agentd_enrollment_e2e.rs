use std::{
    env,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use neoengram_agentd::{
    check_health, load_persisted_identity, run_with, run_with_transports, AgentConfig,
    AgentDaemonResult, FilesystemMountObservation, HealthMode, LoggingConfig, LoggingFormat,
    MountProbe, MountProbeCondition, PvcReference, RegistrationConfig, ReqwestAgentSessionClient,
    ReqwestEnrollmentClient, SessionConfig, StorageAccessMode, StorageBackendType, StorageConfig,
};
use neoengram_core::ObjectId;
use neoengram_protocol::{
    AgentEnrollmentTokenId, AgentMountIdentityDigest, ContentDigest, EdgeClusterId,
    MountAccessMode, ResourceHealth, StorageVolumeId, TenantId, VolumeMarkerId,
};
use neoengram_server::{AppState, Config as ServerConfig};
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
async fn real_agentd_scans_uploads_and_publishes_over_http() {
    const PROJECT_ID: &str = "project-agentd-e2e";
    const ARTIFACT_ID: &str = "artifact-agentd-e2e";
    const PLAYGROUND_ID: &str = "playground-agentd-e2e";
    const JOB_ID: &str = "job-agentd-e2e";
    const RESTART_JOB_ID: &str = "job-agentd-e2e-after-restart";
    const SERVER_RESTART_JOB_ID: &str = "job-agentd-e2e-after-server-restart";
    const FILE_CONTENT: &[u8] = b"real Agent vertical slice\n";
    const RESTART_FILE_CONTENT: &[u8] = b"real Agent payload after restart\n";
    const SERVER_RESTART_FILE_CONTENT: &[u8] = b"real payload after authority reopen\n";

    let authority = TempDir::new().unwrap();
    let keyring_path = authority.path().join("enrollment-keyring.json");
    write_keyring(&keyring_path);
    let object_store_root = authority.path().join("central-objects");
    let mut server_config =
        development_server_config(authority.path().join("authority"), keyring_path);
    server_config.object_store_root = Some(object_store_root.clone());
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
    let initial_index = created_playground["playground"]["index_version"].clone();
    assert_eq!(initial_index["revision"], "0");

    let playground_path = mount_path
        .join("playgrounds")
        .join(PROJECT_ID)
        .join(ARTIFACT_ID)
        .join(PLAYGROUND_ID);
    std::fs::create_dir_all(&playground_path).unwrap();
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
    let object_path = object_store_root
        .join("tenants/tenant-a/artifacts")
        .join(ARTIFACT_ID)
        .join("objects")
        .join(object_id.to_hex());
    assert_eq!(std::fs::read(&object_path).unwrap(), FILE_CONTENT);

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
    let restart_object_path = object_store_root
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
    let server_restart_object_path = object_store_root
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

    stop_agent(server_restart_shutdown, server_restart_task).await;
    restarted_public_server.shutdown().await.unwrap();
    restarted_state.close().await;
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
    .unwrap_or_else(|_| panic!("Agent did not enter active phase {expected}"));
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
        object_store_root: None,
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
