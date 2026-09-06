use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use fusen_rs::ServerState;
use neoengram_central::{
    open_sqlite_authority, ArtifactInitialization, ArtifactRecord, CatalogPvcReference,
    SqliteAuthorityConfig, StorageAccessMode, StorageBackendType, StorageVolumeRecord,
    StorageVolumeState, TenantRecord, WorkspaceRecord, WorkspaceState,
};
use neoengram_central::{AppState, Config};
use neoengram_domain::protocol::{
    ArtifactId, EdgeClusterId, ProjectId, StorageVolumeId, TenantId, UnixMillis, WorkspaceId,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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
async fn public_http_contract_runs_over_a_real_socket_and_drains() {
    let authority = TempDir::new().unwrap();
    seed_job_scope(authority.path()).await;
    let config = development_config(authority.path().to_path_buf());
    let state = AppState::initialize(&config).await.unwrap();
    let running = state.start_server(&config).await.unwrap();
    let address = running.local_addr();

    let live = exchange(address, "GET", "/health/live", &[], b"").await;
    assert_eq!(live.status, 200);
    assert_eq!(live.json(), json!({"status": "ok"}));
    assert!(live.request_id().is_some());

    let ready = exchange(address, "GET", "/health/ready", &[], b"").await;
    assert_eq!(ready.status, 200);
    assert_eq!(ready.json(), json!({"status": "ok"}));

    let version = exchange(
        address,
        "POST",
        "/api/system/version/query",
        &[("x-request-id", "req:version-1")],
        br#"{}"#,
    )
    .await;
    assert_eq!(version.status, 200);
    assert_eq!(version.request_id(), Some("req:version-1"));
    assert_eq!(version.json()["api_version"], json!(1));

    let missing_token = exchange(
        address,
        "POST",
        "/api/task/query",
        &[("neoengram-api-version", "1")],
        br#"{"tenant_id":"tenant-a","task_id":"task-a"}"#,
    )
    .await;
    assert_problem(
        &missing_token,
        401,
        "AUTHENTICATION_REQUIRED",
        "urn:neoengram:problem:authentication-required",
    );

    let missing_version = exchange(
        address,
        "POST",
        "/api/task/query",
        &[("authorization", "Bearer test-secret")],
        br#"{"tenant_id":"tenant-a","task_id":"task-a"}"#,
    )
    .await;
    assert_problem(
        &missing_version,
        422,
        "API_VERSION_UNSUPPORTED",
        "urn:neoengram:problem:api-version-unsupported",
    );
    assert_eq!(
        missing_version.json()["violations"],
        json!([{
            "field": "NeoEngram-API-Version",
            "reason": "unsupported value"
        }])
    );

    let wrong_version = exchange(
        address,
        "POST",
        "/api/task/query",
        &[
            ("authorization", "Bearer test-secret"),
            ("neoengram-api-version", "2"),
        ],
        br#"{"tenant_id":"tenant-a","task_id":"task-a"}"#,
    )
    .await;
    assert_problem(
        &wrong_version,
        422,
        "API_VERSION_UNSUPPORTED",
        "urn:neoengram:problem:api-version-unsupported",
    );
    assert_eq!(
        wrong_version.json()["violations"],
        json!([{
            "field": "NeoEngram-API-Version",
            "reason": "unsupported value"
        }])
    );

    let invalid_json = exchange(
        address,
        "POST",
        "/api/task/query",
        &protected_headers(),
        br#"{"#,
    )
    .await;
    assert_problem(
        &invalid_json,
        422,
        "PROTOCOL_INVALID",
        "urn:neoengram:problem:protocol-invalid",
    );

    let oversized = vec![b'x'; config.max_request_body_bytes + 1];
    let body_limit = exchange(
        address,
        "POST",
        "/api/task/query",
        &protected_headers(),
        &oversized,
    )
    .await;
    assert_problem(
        &body_limit,
        413,
        "PROTOCOL_LIMIT_EXCEEDED",
        "urn:neoengram:problem:payload-too-large",
    );

    // v20 is clean-slate: the removed v1 Job API is a hard route miss, even for an
    // authenticated caller. All lifecycle operations are exposed through /api/task/*.
    let removed_job_route = exchange(
        address,
        "POST",
        "/api/job/query",
        &protected_headers(),
        br#"{"tenant_id":"tenant-a","job_id":"job-a"}"#,
    )
    .await;
    assert_problem(
        &removed_job_route,
        404,
        "ROUTE_NOT_FOUND",
        "urn:neoengram:problem:route-not-found",
    );

    let handle = running.handle();
    running.shutdown().await.unwrap();
    assert_eq!(handle.state(), ServerState::Stopped);
    state.close().await;
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
        max_request_body_bytes: 1_024,
        max_response_body_bytes: 16 * 1_024,
        max_concurrent_requests: 16,
        graceful_shutdown_secs: 2,
    }
}

fn protected_headers() -> [(&'static str, &'static str); 3] {
    [
        ("authorization", "Bearer test-secret"),
        ("neoengram-api-version", "1"),
        ("x-request-id", "req:http-test-1"),
    ]
}

async fn seed_job_scope(path: &Path) {
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(path))
        .await
        .unwrap();
    let catalog = authority
        .authority_store()
        .control_catalog()
        .expect("SQLite authority must compose the control catalog");
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let workspace_id = WorkspaceId::new("workspace-a").unwrap();
    let storage_volume_id = StorageVolumeId::new("volume-a").unwrap();
    let now = UnixMillis::new(1_000);

    catalog
        .insert_tenant(TenantRecord {
            tenant_id: tenant_id.clone(),
            display_name: "Tenant A".to_owned(),
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
                claim_name: "data-a".to_owned(),
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
        .insert_workspace(WorkspaceRecord {
            tenant_id,
            project_id,
            artifact_id,
            workspace_id,
            storage_volume_id,
            region: "cn-shanghai".to_owned(),
            display_name: "Workspace A".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: WorkspaceState::Ready,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            relative_root: "workspaces/project-a/artifact-a/workspace-a".to_owned(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        })
        .await
        .unwrap();
    authority.close().await;
}

fn assert_problem(response: &RawResponse, status: u16, code: &str, type_uri: &str) {
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
    assert!(response.request_id().is_some());
    let problem = response.json();
    assert_eq!(problem["status"], status);
    assert_eq!(problem["code"], code);
    assert_eq!(problem["type"], type_uri);
    assert_eq!(problem["request_id"], response.request_id().unwrap());
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

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
