use std::{
    env,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use neoengram_agentd::{
    check_health, load_persisted_identity, run_with, AgentConfig, AgentDaemonResult,
    FilesystemMountObservation, HealthMode, LoggingConfig, LoggingFormat, MountProbe,
    MountProbeCondition, PvcReference, RegistrationConfig, ReqwestEnrollmentClient, SessionConfig,
    StorageAccessMode, StorageBackendType, StorageConfig,
};
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

    let (first_shutdown, first_task) = spawn_agent(agent_config.clone(), probe.clone());
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

    wait_for_active_phase(&state_dir, "approved_waiting_certificate").await;
    check_health(&state_dir, HealthMode::Startup).unwrap();
    check_health(&state_dir, HealthMode::Live).unwrap();
    assert!(check_health(&state_dir, HealthMode::Ready).is_err());
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
    let (restart_shutdown, restart_task) = spawn_agent(agent_config, probe);
    wait_for_active_phase(&state_dir, "approved_waiting_certificate").await;
    check_health(&state_dir, HealthMode::Startup).unwrap();
    check_health(&state_dir, HealthMode::Live).unwrap();
    assert!(check_health(&state_dir, HealthMode::Ready).is_err());
    stop_agent(restart_shutdown, restart_task).await;

    let restarted = load_persisted_identity(&state_dir).unwrap().unwrap();
    assert_eq!(restarted, persisted);
    assert!(!token_path.exists());

    public_server.shutdown().await.unwrap();
    state.close().await;
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

async fn stop_agent(shutdown: oneshot::Sender<()>, task: JoinHandle<AgentDaemonResult<()>>) {
    shutdown.send(()).unwrap();
    timeout(Duration::from_secs(5), task)
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
