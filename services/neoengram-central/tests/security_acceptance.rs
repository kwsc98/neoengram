use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use fusen_rs::{RunningServer, ServerState};
use neoengram_central::{
    open_sqlite_authority, ArtifactInitialization, ArtifactRecord, CatalogPvcReference,
    PlaygroundRecord, PlaygroundState, SqliteAuthorityConfig, StorageAccessMode,
    StorageBackendType, StorageVolumeRecord, StorageVolumeState, TenantRecord,
};
use neoengram_central::{AppState, Config};
use neoengram_domain::protocol::{
    ArtifactId, EdgeClusterId, PlaygroundId, ProjectId, StorageVolumeId, TenantId, UnixMillis,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const AUTHORIZATION_DENIED: &str = "AUTHORIZATION_DENIED";

struct RawResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl RawResponse {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("response body must be JSON")
    }

    fn request_id(&self) -> Option<&str> {
        self.headers.get("x-request-id").map(String::as_str)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_create_rejects_task_authority_fields_before_persisting() {
    let authority = TempDir::new().unwrap();
    seed_catalog_scopes(authority.path(), &["tenant-a"]).await;
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;
    let address = running.local_addr();

    for field in ["actor", "principal", "request_digest"] {
        let project_id = format!("reserved-{field}");
        let mut request = project_request("tenant-a", &project_id);
        request
            .as_object_mut()
            .unwrap()
            .insert(field.to_owned(), json!("caller-controlled"));

        let response = post_json(address, "/api/project/create", &request).await;
        assert_problem(
            &response,
            422,
            "PROTOCOL_INVALID",
            "urn:neoengram:problem:protocol-invalid",
            false,
        );
        let problem = response.json();
        assert!(
            !serde_json::to_string(&problem)
                .unwrap()
                .contains("caller-controlled"),
            "rejected fields must not echo their values: {problem}"
        );

        let query = post_json(
            address,
            "/api/task/list/query",
            &json!({"tenant_id": "tenant-a", "project_id": project_id}),
        )
        .await;
        assert_eq!(
            query.status,
            200,
            "{}",
            String::from_utf8_lossy(&query.body)
        );
        assert_eq!(query.json()["items"], json!([]));
    }

    stop(state, running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_query_survives_restart_and_omits_execution_secrets() {
    let authority = TempDir::new().unwrap();
    seed_catalog_scopes(authority.path(), &["tenant-a"]).await;
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;
    let address = running.local_addr();

    let request = project_request("tenant-a", "persistent-project");
    let created = post_json(address, "/api/project/create", &request).await;
    assert_eq!(
        created.status,
        200,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let created_task = created.json()["task"].clone();
    assert_public_operation_task_view(&created_task);
    let task_id = created_task["task_id"].as_str().unwrap().to_owned();

    stop(state, running).await;

    let (restarted_state, restarted) = start(&config).await;
    let queried = post_json(
        restarted.local_addr(),
        "/api/task/query",
        &json!({"tenant_id": "tenant-a", "task_id": task_id}),
    )
    .await;
    assert_eq!(
        queried.status,
        200,
        "{}",
        String::from_utf8_lossy(&queried.body)
    );
    let queried_task = queried.json()["task"].clone();
    assert_public_operation_task_view(&queried_task);
    assert_eq!(queried_task, created_task);

    stop(restarted_state, restarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_rbac_is_deny_by_default_and_hides_cross_tenant_existence() {
    let authority = TempDir::new().unwrap();
    seed_catalog_scopes(authority.path(), &["tenant-a", "tenant-b"]).await;
    let mut seed_config = development_config(authority.path().to_path_buf());
    seed_config.development_tenants = vec!["*".to_owned()];
    let (seed_state, seed_server) = start(&seed_config).await;
    let seeded = post_json(
        seed_server.local_addr(),
        "/api/project/create",
        &project_request("tenant-b", "private-project"),
    )
    .await;
    assert_eq!(
        seeded.status,
        200,
        "{}",
        String::from_utf8_lossy(&seeded.body)
    );
    let private_task_id = seeded.json()["task"]["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    stop(seed_state, seed_server).await;

    let policy_path = authority.path().join("rbac.json");
    write_policy(
        &policy_path,
        json!({
            "roles": {
                "operator": {
                    "permissions": ["project.create", "task.read", "task.manage"]
                }
            },
            "bindings": []
        }),
    );
    let mut policy_config = development_config(authority.path().to_path_buf());
    policy_config.rbac_file = Some(policy_path.clone());

    let (missing_state, missing_server) = start(&policy_config).await;
    let missing_binding_query =
        query_private_task(missing_server.local_addr(), &private_task_id).await;
    assert_authorization_denied(&missing_binding_query);
    let missing_binding_create = post_json(
        missing_server.local_addr(),
        "/api/project/create",
        &project_request("tenant-a", "missing-binding-project"),
    )
    .await;
    assert_resource_not_found(&missing_binding_create);
    stop(missing_state, missing_server).await;

    write_policy(
        &policy_path,
        json!({
            "roles": {
                "operator": {
                    "permissions": ["project.create", "task.read", "task.manage"]
                }
            },
            "bindings": [{
                "principal_id": "user-a",
                "roles": ["operator"],
                "tenants": ["*"],
                "disabled": true
            }]
        }),
    );
    let (disabled_state, disabled_server) = start(&policy_config).await;
    let disabled_query = query_private_task(disabled_server.local_addr(), &private_task_id).await;
    assert_authorization_denied(&disabled_query);
    let disabled_create = post_json(
        disabled_server.local_addr(),
        "/api/project/create",
        &project_request("tenant-a", "disabled-project"),
    )
    .await;
    assert_resource_not_found(&disabled_create);
    stop(disabled_state, disabled_server).await;

    write_policy(
        &policy_path,
        json!({
            "roles": {
                "operator": {
                    "permissions": ["project.create", "task.read", "task.manage"]
                }
            },
            "bindings": [{
                "principal_id": "user-a",
                "roles": ["operator"],
                "tenants": ["tenant-a"]
            }]
        }),
    );
    let (scoped_state, scoped_server) = start(&policy_config).await;
    let cross_tenant = query_private_task(scoped_server.local_addr(), &private_task_id).await;
    assert_authorization_denied(&cross_tenant);
    let absent = post_json(
        scoped_server.local_addr(),
        "/api/task/query",
        &json!({"tenant_id": "tenant-b", "task_id": "absent-task"}),
    )
    .await;
    assert_authorization_denied(&absent);

    let expected = problem_without_request_id(&absent);
    for hidden in [&missing_binding_query, &disabled_query, &cross_tenant] {
        assert_eq!(problem_without_request_id(hidden), expected);
    }

    stop(scoped_state, scoped_server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_write_replays_the_same_task_without_exposing_execution_identity() {
    let authority = TempDir::new().unwrap();
    seed_catalog_scopes(authority.path(), &["tenant-a"]).await;
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;

    let request = project_request("tenant-a", "replayed-project");
    let first = post_json(running.local_addr(), "/api/project/create", &request).await;
    let second = post_json(running.local_addr(), "/api/project/create", &request).await;
    for (response, replayed) in [(&first, false), (&second, true)] {
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let body = response.json();
        assert_eq!(body["replayed"], replayed);
        assert_eq!(body["task"]["state"], "succeeded");
        assert_public_operation_task_view(&body["task"]);
        let encoded = serde_json::to_string(&body["task"]).unwrap();
        for secret in [
            "assignment-secret",
            "agent-secret",
            "mount-secret",
            "volume-secret",
            "placement-secret",
        ] {
            assert!(!encoded.contains(secret), "TaskView exposed {secret}");
        }
        for forbidden in [
            "assignment",
            "assignment_id",
            "assignment_generation",
            "agent_id",
            "agent_mount_id",
            "storage_volume_id",
            "artifact_placement_id",
            "mount_generation",
            "owner_generation",
            "placement_generation",
            "lease",
        ] {
            assert!(
                body["task"].get(forbidden).is_none(),
                "TaskView leaked {forbidden}"
            );
        }
    }
    assert_eq!(first.json()["task"], second.json()["task"]);

    stop(state, running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_can_be_withdrawn_while_liveness_remains_available() {
    let authority = TempDir::new().unwrap();
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;
    let address = running.local_addr();

    let ready = exchange(address, "GET", "/health/ready", &[], b"").await;
    assert_eq!(ready.status, 200);
    assert_eq!(ready.json(), json!({"status": "ok"}));

    state.close().await;

    let unavailable = exchange(address, "GET", "/health/ready", &[], b"").await;
    assert_problem(
        &unavailable,
        503,
        "SERVER_NOT_READY",
        "urn:neoengram:problem:server-not-ready",
        true,
    );
    let live = exchange(address, "GET", "/health/live", &[], b"").await;
    assert_eq!(live.status, 200);
    assert_eq!(live.json(), json!({"status": "ok"}));

    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_drains_an_inflight_real_http1_request() {
    let authority = TempDir::new().unwrap();
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;
    let mut stream = TcpStream::connect(running.local_addr()).await.unwrap();
    stream
        .write_all(
            b"POST /api/system/version/query HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Type: application/json\r\n\
              Content-Length: 2\r\n\
              X-Request-ID: req:graceful-drain\r\n\
              Expect: 100-continue\r\n\
              Connection: close\r\n\
              \r\n",
        )
        .await
        .unwrap();
    wait_for_continue(&mut stream).await;
    stream.write_all(b"{").await.unwrap();

    let handle = running.handle();
    let mut shutdown = tokio::spawn(async move { handle.shutdown().await });
    wait_for_state(&running, ServerState::Draining).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for the in-flight request body"
    );

    stream.write_all(b"}").await.unwrap();
    let response = read_response(&mut stream).await;
    assert_eq!(response.status, 200);
    assert_eq!(response.request_id(), Some("req:graceful-drain"));
    assert_eq!(response.json()["api_version"], json!(1));

    shutdown.await.unwrap().unwrap();
    running.wait().await.unwrap();
    state.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_timeout_and_concurrency_limits_return_problem_details() {
    let authority = TempDir::new().unwrap();
    let mut config = development_config(authority.path().to_path_buf());
    config.request_timeout_secs = 1;
    config.max_concurrent_requests = 1;
    let (state, running) = start(&config).await;
    let mut blocked = TcpStream::connect(running.local_addr()).await.unwrap();
    blocked
        .write_all(
            b"POST /api/system/version/query HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Type: application/json\r\n\
              Content-Length: 2\r\n\
              X-Request-ID: req:timeout\r\n\
              Expect: 100-continue\r\n\
              Connection: close\r\n\
              \r\n",
        )
        .await
        .unwrap();
    wait_for_continue(&mut blocked).await;
    blocked.write_all(b"{").await.unwrap();

    let overloaded = exchange(
        running.local_addr(),
        "POST",
        "/api/system/version/query",
        &[("x-request-id", "req:overloaded")],
        b"{}",
    )
    .await;
    assert_problem(
        &overloaded,
        429,
        "OVERLOADED",
        "urn:neoengram:problem:overloaded",
        false,
    );

    let timed_out = read_response(&mut blocked).await;
    assert_problem(
        &timed_out,
        504,
        "DEADLINE_EXCEEDED",
        "urn:neoengram:problem:deadline-exceeded",
        false,
    );
    assert_eq!(timed_out.request_id(), Some("req:timeout"));

    stop(state, running).await;
}

async fn start(config: &Config) -> (AppState, RunningServer) {
    let state = AppState::initialize(config).await.unwrap();
    let running = state.start_server(config).await.unwrap();
    (state, running)
}

async fn stop(state: AppState, running: RunningServer) {
    running.shutdown().await.unwrap();
    state.close().await;
}

async fn seed_catalog_scopes(path: &Path, tenants: &[&str]) {
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(path))
        .await
        .unwrap();
    let catalog = authority
        .authority_store()
        .control_catalog()
        .expect("SQLite authority must compose the control catalog");
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
    let storage_volume_id = StorageVolumeId::new("volume-a").unwrap();
    let now = UnixMillis::new(1_000);
    for tenant in tenants {
        let tenant_id = TenantId::new(*tenant).unwrap();
        catalog
            .insert_tenant(TenantRecord {
                tenant_id: tenant_id.clone(),
                display_name: format!("Tenant {tenant}"),
                description: None,
                resource_version: 1,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .unwrap();
        catalog
            .insert_artifact(ArtifactRecord {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                display_name: "Artifact A".to_owned(),
                description: None,
                initialization: ArtifactInitialization::Empty,
                head_commit_id: None,
                resource_version: 1,
                lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .unwrap();
        catalog
            .insert_storage_volume(StorageVolumeRecord {
                tenant_id: tenant_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                display_name: "Volume A".to_owned(),
                edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
                region: "cn-shanghai".to_owned(),
                backend_type: StorageBackendType::Pvc,
                access_mode: StorageAccessMode::ReadWriteMany,
                allowed_delivery_modes: vec![
                    neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
                    neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
                ],
                hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
                max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
                copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
                pvc_reference: Some(CatalogPvcReference {
                    namespace: "neoengram".to_owned(),
                    claim_name: format!("data-{tenant}"),
                }),
                nfs_reference: None,
                state: StorageVolumeState::Ready,
                resource_version: 1,
                lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .unwrap();
        catalog
            .insert_playground(PlaygroundRecord {
                tenant_id,
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                playground_id: playground_id.clone(),
                storage_volume_id: storage_volume_id.clone(),
                region: "cn-shanghai".to_owned(),
                display_name: "Playground A".to_owned(),
                base_commit_id: None,
                head_commit_id: None,
                state: PlaygroundState::Ready,
                resource_version: 1,
                lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
                relative_root: "playgrounds/project-a/artifact-a/playground-a".to_owned(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
            })
            .await
            .unwrap();
    }
    authority.close().await;
}

fn development_config(authority_dir: PathBuf) -> Config {
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        agent_enrollment_enabled: false,
        agent_enrollment_keyring_file: None,
        gateway_tls_ca_file: None,
        gateway_tls_client_certificate_file: None,
        gateway_tls_client_private_key_file: None,
        gateway_workload_trust_domain: None,
        authority_dir,
        rbac_file: None,
        s3_envelope_key_file: None,
        oidc_issuer: None,
        oidc_audience: None,
        oidc_jwks_uri: None,
        development: true,
        development_token: Some("test-secret".to_owned()),
        development_principal: "user-a".to_owned(),
        development_tenants: vec!["tenant-a".to_owned()],
        request_timeout_secs: 5,
        max_request_body_bytes: 16 * 1_024,
        max_response_body_bytes: 16 * 1_024,
        max_concurrent_requests: 16,
        graceful_shutdown_secs: 2,
    }
}

fn protected_headers() -> [(&'static str, &'static str); 3] {
    [
        ("authorization", "Bearer test-secret"),
        ("neoengram-api-version", "1"),
        ("x-request-id", "req:security-acceptance"),
    ]
}

fn project_request(tenant_id: &str, project_id: &str) -> Value {
    json!({
        "tenant_id": tenant_id,
        "project_id": project_id,
        "display_name": format!("Project {project_id}")
    })
}

fn write_policy(path: &Path, policy: Value) {
    std::fs::write(path, serde_json::to_vec(&policy).unwrap()).unwrap();
}

async fn query_private_task(address: SocketAddr, task_id: &str) -> RawResponse {
    post_json(
        address,
        "/api/task/query",
        &json!({"tenant_id": "tenant-b", "task_id": task_id}),
    )
    .await
}

fn assert_public_operation_task_view(task: &Value) {
    let object = task.as_object().expect("TaskView must be a JSON object");
    for required in [
        "task_id",
        "task_kind",
        "state",
        "phase",
        "tenant_id",
        "request_id",
        "request_digest",
        "actor",
        "attempt",
        "progress",
        "resource_version",
        "origin",
        "executable",
    ] {
        assert!(object.contains_key(required), "TaskView omitted {required}");
    }

    for forbidden in [
        "accepted",
        "agent_id",
        "agent_mount_id",
        "artifact_placement_id",
        "assignment",
        "assignment_generation",
        "assignment_id",
        "assignment_target",
        "fencing_token",
        "generation",
        "lease",
        "manifest",
        "manifests",
        "mount_generation",
        "owner_generation",
        "placement_generation",
        "prepared",
        "publication_candidate",
    ] {
        assert!(
            !object.contains_key(forbidden),
            "TaskView leaked {forbidden}"
        );
    }
}

fn assert_authorization_denied(response: &RawResponse) {
    assert_problem(
        response,
        403,
        AUTHORIZATION_DENIED,
        "urn:neoengram:problem:authorization-denied",
        false,
    );
}

fn assert_resource_not_found(response: &RawResponse) {
    assert_problem(
        response,
        404,
        "RESOURCE_NOT_FOUND",
        "urn:neoengram:problem:resource-not-found",
        false,
    );
}

fn assert_problem(
    response: &RawResponse,
    status: u16,
    code: &str,
    type_uri: &str,
    retryable: bool,
) {
    assert_eq!(
        response.status,
        status,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    assert_eq!(
        response.headers.get("content-type").map(String::as_str),
        Some("application/problem+json")
    );
    let request_id = response
        .request_id()
        .expect("every response has a request ID");
    let problem = response.json();
    assert_eq!(problem["status"], status);
    assert_eq!(problem["code"], code);
    assert_eq!(problem["type"], type_uri);
    assert_eq!(problem["request_id"], request_id);
    assert_eq!(problem["retryable"], retryable);
}

fn problem_without_request_id(response: &RawResponse) -> Value {
    let mut problem = response.json();
    problem
        .as_object_mut()
        .expect("ProblemDetails must be an object")
        .remove("request_id");
    problem
}

async fn post_json(address: SocketAddr, path: &str, body: &Value) -> RawResponse {
    exchange(
        address,
        "POST",
        path,
        &protected_headers(),
        &serde_json::to_vec(body).unwrap(),
    )
    .await
}

async fn exchange(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> RawResponse {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    read_response(&mut stream).await
}

async fn read_response(stream: &mut TcpStream) -> RawResponse {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut received = Vec::new();
        let mut buffer = [0_u8; 2_048];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0, "HTTP response ended before headers completed");
            received.extend_from_slice(&buffer[..read]);
            let Some(head_end) = find_bytes(&received, b"\r\n\r\n") else {
                continue;
            };
            let body_start = head_end + 4;
            let head = std::str::from_utf8(&received[..head_end]).unwrap();
            let status = head
                .lines()
                .next()
                .and_then(|line| line.split_ascii_whitespace().nth(1))
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap();
            let headers: BTreeMap<String, String> = head
                .lines()
                .skip(1)
                .filter_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    Some((name.to_ascii_lowercase(), value.trim().to_owned()))
                })
                .collect();
            let content_length = headers
                .get("content-length")
                .and_then(|value| value.parse::<usize>().ok())
                .expect("bounded Fusen responses carry Content-Length");
            while received.len() < body_start + content_length {
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0, "HTTP response ended before body completed");
                received.extend_from_slice(&buffer[..read]);
            }
            return RawResponse {
                status,
                headers,
                body: received[body_start..body_start + content_length].to_vec(),
            };
        }
    })
    .await
    .expect("server must answer within the test deadline")
}

async fn wait_for_continue(stream: &mut TcpStream) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = Vec::new();
        let mut buffer = [0_u8; 256];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0, "HTTP connection ended before 100 Continue");
            received.extend_from_slice(&buffer[..read]);
            if find_bytes(&received, b"\r\n\r\n").is_some() {
                assert_eq!(received, b"HTTP/1.1 100 Continue\r\n\r\n");
                return;
            }
        }
    })
    .await
    .expect("polling the request body must emit 100 Continue");
}

async fn wait_for_state(server: &RunningServer, expected: ServerState) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.state() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server lifecycle state must advance");
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
