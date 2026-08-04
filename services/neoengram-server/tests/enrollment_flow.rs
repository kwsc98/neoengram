use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentBootstrapAccepted, AgentBootstrapProbe, AgentBootstrapProof, AgentBootstrapRequest,
    AgentBootstrapStatusRequest, AgentBootstrapStatusResponse, AgentBootstrapStatusState,
    AgentInstallationId, AgentMountIdentityDigest, AgentSignatureAlgorithm, Ed25519PublicKeySpki,
    Ed25519Signature, EdgeClusterId, Extensions, MountAccessMode, ProtocolVersion, RequestId,
    ResourceHealth, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId, PROTOCOL_VERSION_V1,
};
use neoengram_server::{AppState, Config};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use tempfile::TempDir;

const API_TOKEN: &str = "test-secret";
static LAST_TEST_TIME_MS: AtomicU64 = AtomicU64::new(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_admin_and_agent_pop_registration_survive_restart() {
    let authority = TempDir::new().unwrap();
    let keyring_path = authority.path().join("enrollment-keyring.json");
    write_keyring(&keyring_path);
    let config = development_config(authority.path().to_path_buf(), keyring_path);
    let client = Client::builder().build().unwrap();

    let state = AppState::initialize(&config).await.unwrap();
    let public = state.start_server(&config).await.unwrap();
    let public_addr = public.local_addr();
    let agent_addr = state.agent_local_addr().await.unwrap();

    let first_intent = enrollment_intent("vision", "volume-vision", "vision-data");
    let first_token = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/token/create",
        &first_intent,
    )
    .await;
    assert_eq!(first_token.status, StatusCode::OK);
    assert_eq!(first_token.json["replayed"], false);
    let bootstrap_token = first_token.json["bootstrap_token"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(bootstrap_token.starts_with("ngenr_v1_"));

    let first_key = SigningKey::from_bytes(&[7_u8; 32]);
    let first_bootstrap = signed_bootstrap(
        &first_key,
        "bootstrap-vision",
        "installation-vision",
        &bootstrap_token,
        "volume-vision",
        descriptor_digest(&first_intent),
    );
    let mut tampered_bootstrap = first_bootstrap.clone();
    tampered_bootstrap.agent_version = "0.2.0-tampered".to_owned();
    let tampered = post_agent_raw(
        &client,
        agent_addr,
        "/v1/agents/bootstrap",
        serde_json::to_vec(&tampered_bootstrap).unwrap(),
        "agent:tampered-test",
    )
    .await;
    assert_problem(&tampered, StatusCode::FORBIDDEN, "AGENT_BOOTSTRAP_DENIED");

    let duplicate = post_agent_raw(
        &client,
        agent_addr,
        "/v1/agents/bootstrap",
        br#"{"bootstrap_request_id":"a","bootstrap_request_id":"b"}"#.to_vec(),
        "agent:duplicate-test",
    )
    .await;
    assert_problem(
        &duplicate,
        StatusCode::UNPROCESSABLE_ENTITY,
        "PROTOCOL_INVALID",
    );

    let oversized = post_agent_raw(
        &client,
        agent_addr,
        "/v1/agents/bootstrap",
        vec![b'x'; 64 * 1024 + 1],
        "agent:oversized-test",
    )
    .await;
    assert_problem(
        &oversized,
        StatusCode::PAYLOAD_TOO_LARGE,
        "PROTOCOL_LIMIT_EXCEEDED",
    );

    let (first_accepted, concurrent_replay) = tokio::join!(
        post_agent_bootstrap(&client, agent_addr, &first_bootstrap),
        post_agent_bootstrap(&client, agent_addr, &first_bootstrap)
    );
    assert_eq!(
        usize::from(first_accepted.replayed) + usize::from(concurrent_replay.replayed),
        1
    );
    assert_eq!(
        first_accepted.resource_version,
        concurrent_replay.resource_version
    );
    assert_eq!(
        first_accepted.enrollment_id,
        concurrent_replay.enrollment_id
    );
    assert_eq!(
        first_accepted.state,
        neoengram_protocol::AgentEnrollmentState::PendingApproval
    );
    let pending_status_request =
        signed_status(&first_key, "bootstrap-vision", "installation-vision");
    let pending_status =
        post_agent_status(&client, agent_addr, pending_status_request.clone()).await;
    assert_eq!(pending_status.state, AgentBootstrapStatusState::Pending);
    let replayed_status = post_agent_raw(
        &client,
        agent_addr,
        "/v1/agents/bootstrap/status",
        serde_json::to_vec(&pending_status_request).unwrap(),
        "agent:replayed-status",
    )
    .await;
    assert_problem(
        &replayed_status,
        StatusCode::FORBIDDEN,
        "AGENT_BOOTSTRAP_DENIED",
    );

    let stale_status = post_agent_raw(
        &client,
        agent_addr,
        "/v1/agents/bootstrap/status",
        serde_json::to_vec(&signed_status_at(
            &first_key,
            "bootstrap-vision",
            "installation-vision",
            UnixMillis::new(1),
        ))
        .unwrap(),
        "agent:stale-status",
    )
    .await;
    assert_problem(
        &stale_status,
        StatusCode::FORBIDDEN,
        "AGENT_BOOTSTRAP_DENIED",
    );

    let queried = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/query",
        &json!({
            "tenant_id": "tenant-a",
            "storage_enrollment_id": first_accepted.enrollment_id
        }),
    )
    .await;
    assert_eq!(queried.status, StatusCode::OK, "{}", queried.json);
    assert_public_enrollment_view(&queried.json["enrollment"]);
    assert_eq!(queried.json["enrollment"]["state"], "pending_approval");
    assert_eq!(
        queried.json["enrollment"]["proof_of_possession_status"],
        "verified"
    );

    let approved = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/approve",
        &json!({
            "tenant_id": "tenant-a",
            "storage_enrollment_id": first_accepted.enrollment_id,
            "approval_request_id": "approve-vision",
            "expected_resource_version": queried.json["enrollment"]["resource_version"],
            "confirm_replacement": false
        }),
    )
    .await;
    assert_eq!(approved.status, StatusCode::OK, "{}", approved.json);
    assert_eq!(approved.json["enrollment"]["state"], "approved");
    assert_eq!(approved.json["storage_volume"]["state"], "unavailable");
    assert_public_enrollment_view(&approved.json["enrollment"]);
    let approved_replay = post_agent_bootstrap(&client, agent_addr, &first_bootstrap).await;
    assert!(approved_replay.replayed);
    assert_eq!(
        approved_replay.state,
        neoengram_protocol::AgentEnrollmentState::Approved
    );

    let approved_status = post_agent_status(
        &client,
        agent_addr,
        signed_status(&first_key, "bootstrap-vision", "installation-vision"),
    )
    .await;
    assert_eq!(approved_status.state, AgentBootstrapStatusState::Approved);
    assert_eq!(
        approved_status.agent_id,
        Some(first_accepted.agent_id.clone())
    );

    let second_intent = enrollment_intent("archive", "volume-archive", "archive-data");
    let second_token = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/token/create",
        &second_intent,
    )
    .await;
    let second_key = SigningKey::from_bytes(&[9_u8; 32]);
    let second_bootstrap = signed_bootstrap(
        &second_key,
        "bootstrap-archive",
        "installation-archive",
        second_token.json["bootstrap_token"].as_str().unwrap(),
        "volume-archive",
        descriptor_digest(&second_intent),
    );
    let second_accepted = post_agent_bootstrap(&client, agent_addr, &second_bootstrap).await;
    let second_query = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/query",
        &json!({
            "tenant_id": "tenant-a",
            "storage_enrollment_id": second_accepted.enrollment_id
        }),
    )
    .await;
    let rejected = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/reject",
        &json!({
            "tenant_id": "tenant-a",
            "storage_enrollment_id": second_accepted.enrollment_id,
            "rejection_request_id": "reject-archive",
            "expected_resource_version": second_query.json["enrollment"]["resource_version"],
            "reason": "out-of-band PVC review failed"
        }),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::OK, "{}", rejected.json);
    assert_eq!(rejected.json["enrollment"]["state"], "rejected");
    assert!(!rejected.json.to_string().contains("out-of-band"));
    let rejected_status = post_agent_status(
        &client,
        agent_addr,
        signed_status(&second_key, "bootstrap-archive", "installation-archive"),
    )
    .await;
    assert_eq!(rejected_status.state, AgentBootstrapStatusState::Rejected);

    let first_page = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/list/query",
        &json!({"tenant_id": "tenant-a", "page_size": 1}),
    )
    .await;
    assert_eq!(first_page.status, StatusCode::OK, "{}", first_page.json);
    assert_eq!(first_page.json["items"].as_array().unwrap().len(), 1);
    let cursor = first_page.json["next_cursor"].as_str().unwrap();
    let cursor_scope_conflict = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/list/query",
        &json!({
            "tenant_id": "tenant-a",
            "state": "approved",
            "page_size": 1,
            "cursor": cursor
        }),
    )
    .await;
    assert_problem(
        &cursor_scope_conflict,
        StatusCode::CONFLICT,
        "CURSOR_SCOPE_CONFLICT",
    );
    let second_page = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/list/query",
        &json!({"tenant_id": "tenant-a", "page_size": 1, "cursor": cursor}),
    )
    .await;
    assert_eq!(second_page.status, StatusCode::OK, "{}", second_page.json);
    assert_ne!(
        first_page.json["items"][0]["storage_enrollment_id"],
        second_page.json["items"][0]["storage_enrollment_id"]
    );

    let cross_tenant = post_public(
        &client,
        public_addr,
        "/api/storage/enrollment/query",
        &json!({
            "tenant_id": "tenant-b",
            "storage_enrollment_id": first_accepted.enrollment_id
        }),
    )
    .await;
    assert_problem(
        &cross_tenant,
        StatusCode::NOT_FOUND,
        "STORAGE_ENROLLMENT_NOT_FOUND",
    );

    public.shutdown().await.unwrap();
    state.close().await;

    let restarted_state = AppState::initialize(&config).await.unwrap();
    let restarted_public = restarted_state.start_server(&config).await.unwrap();
    let replayed = post_public(
        &client,
        restarted_public.local_addr(),
        "/api/storage/enrollment/token/create",
        &first_intent,
    )
    .await;
    assert_eq!(replayed.status, StatusCode::OK, "{}", replayed.json);
    assert_eq!(replayed.json["replayed"], true);
    assert_eq!(replayed.json["bootstrap_token"], bootstrap_token);
    let status_after_restart = post_agent_status(
        &client,
        restarted_state.agent_local_addr().await.unwrap(),
        signed_status(&first_key, "bootstrap-vision", "installation-vision"),
    )
    .await;
    assert_eq!(
        status_after_restart.state,
        AgentBootstrapStatusState::Approved
    );
    let query_after_restart = post_public(
        &client,
        restarted_public.local_addr(),
        "/api/storage/enrollment/query",
        &json!({
            "tenant_id": "tenant-a",
            "storage_enrollment_id": first_accepted.enrollment_id
        }),
    )
    .await;
    assert_eq!(query_after_restart.status, StatusCode::OK);
    assert_eq!(
        query_after_restart.json["enrollment"]["proof_of_possession_status"],
        "verified"
    );
    assert_public_enrollment_view(&query_after_restart.json["enrollment"]);

    restarted_public.shutdown().await.unwrap();
    restarted_state.close().await;
}

fn enrollment_intent(label: &str, volume_id: &str, claim_name: &str) -> Value {
    json!({
        "tenant_id": "tenant-a",
        "token_request_id": format!("token-request-{label}"),
        "storage_volume_id": volume_id,
        "display_name": format!("{label} dataset PVC"),
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

fn signed_bootstrap(
    key: &SigningKey,
    bootstrap_request_id: &str,
    installation_id: &str,
    bootstrap_token: &str,
    storage_volume_id: &str,
    volume_descriptor_digest: ContentDigest,
) -> AgentBootstrapRequest {
    let public_key_spki =
        Ed25519PublicKeySpki::from_public_key_bytes(key.verifying_key().to_bytes());
    let mut request = AgentBootstrapRequest {
        bootstrap_request_id: RequestId::new(bootstrap_request_id).unwrap(),
        bootstrap_token: bootstrap_token.to_owned(),
        installation_id: AgentInstallationId::new(installation_id).unwrap(),
        tenant_id: TenantId::new("tenant-a").unwrap(),
        edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
        storage_volume_id: StorageVolumeId::new(storage_volume_id).unwrap(),
        volume_descriptor_digest,
        agent_version: "0.2.0".to_owned(),
        supported_protocol_versions: vec![ProtocolVersion::new(1)],
        capabilities: Vec::new(),
        public_key_fingerprint: public_key_spki.fingerprint(),
        proof: placeholder_proof(public_key_spki),
        probe: AgentBootstrapProbe {
            observed_volume_marker: Some(VolumeMarkerId::new(storage_volume_id).unwrap()),
            marker_matches: true,
            mount_boundary_detected: true,
            mount_identity_digest: AgentMountIdentityDigest::new(ContentDigest::hash(
                format!("mount-{storage_volume_id}").as_bytes(),
            )),
            access_mode: Some(MountAccessMode::ReadWrite),
            rename_supported: true,
            fsync_supported: true,
            health: ResourceHealth::Ready,
            observed_at_unix_ms: now(),
            extensions: Extensions::new(),
        },
        extensions: Extensions::new(),
    };
    let signature = key.sign(&request.signing_bytes().unwrap());
    request.proof.signature = Ed25519Signature::from_bytes(signature.to_bytes());
    request
}

fn signed_status(
    key: &SigningKey,
    bootstrap_request_id: &str,
    installation_id: &str,
) -> AgentBootstrapStatusRequest {
    signed_status_at(key, bootstrap_request_id, installation_id, now())
}

fn signed_status_at(
    key: &SigningKey,
    bootstrap_request_id: &str,
    installation_id: &str,
    signed_at_unix_ms: UnixMillis,
) -> AgentBootstrapStatusRequest {
    let public_key_spki =
        Ed25519PublicKeySpki::from_public_key_bytes(key.verifying_key().to_bytes());
    let mut request = AgentBootstrapStatusRequest {
        protocol_version: PROTOCOL_VERSION_V1,
        tenant_id: TenantId::new("tenant-a").unwrap(),
        bootstrap_request_id: RequestId::new(bootstrap_request_id).unwrap(),
        installation_id: AgentInstallationId::new(installation_id).unwrap(),
        signed_at_unix_ms,
        proof: placeholder_proof(public_key_spki),
        extensions: Extensions::new(),
    };
    let signature = key.sign(&request.signing_bytes().unwrap());
    request.proof.signature = Ed25519Signature::from_bytes(signature.to_bytes());
    request
}

fn placeholder_proof(public_key_spki: Ed25519PublicKeySpki) -> AgentBootstrapProof {
    AgentBootstrapProof {
        algorithm: AgentSignatureAlgorithm::Ed25519,
        public_key_spki,
        signature: Ed25519Signature::from_bytes([0_u8; 64]),
        extensions: Extensions::new(),
    }
}

async fn post_agent_bootstrap(
    client: &Client,
    address: std::net::SocketAddr,
    request: &AgentBootstrapRequest,
) -> AgentBootstrapAccepted {
    let response = client
        .post(format!("http://{address}/v1/agents/bootstrap"))
        .header("x-request-id", "agent:bootstrap-test")
        .json(request)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let request_id = response.headers().get("x-request-id").cloned();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert!(request_id.is_some());
    serde_json::from_slice(&bytes).unwrap()
}

async fn post_agent_status(
    client: &Client,
    address: std::net::SocketAddr,
    request: AgentBootstrapStatusRequest,
) -> AgentBootstrapStatusResponse {
    let response = client
        .post(format!("http://{address}/v1/agents/bootstrap/status"))
        .header("x-request-id", "agent:status-test")
        .json(&request)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    AgentBootstrapStatusResponse::decode_json(&bytes).unwrap()
}

async fn post_agent_raw(
    client: &Client,
    address: std::net::SocketAddr,
    path: &str,
    body: Vec<u8>,
    request_id: &str,
) -> JsonResponse {
    let response = client
        .post(format!("http://{address}{path}"))
        .header("content-type", "application/json")
        .header("x-request-id", request_id)
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    assert_eq!(response.headers().get("x-request-id").unwrap(), request_id);
    let json = response.json().await.unwrap();
    JsonResponse { status, json }
}

struct JsonResponse {
    status: StatusCode,
    json: Value,
}

async fn post_public(
    client: &Client,
    address: std::net::SocketAddr,
    path: &str,
    body: &Value,
) -> JsonResponse {
    let response = client
        .post(format!("http://{address}{path}"))
        .header("authorization", format!("Bearer {API_TOKEN}"))
        .header("neoengram-api-version", "1")
        .header("x-request-id", "req:enrollment-test")
        .json(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    assert_eq!(
        response.headers().get("x-request-id").unwrap(),
        "req:enrollment-test"
    );
    let json = response.json().await.unwrap();
    JsonResponse { status, json }
}

fn assert_public_enrollment_view(view: &Value) {
    const FORBIDDEN_FIELDS: &[&str] = &[
        "bootstrap_token",
        "token_key_id",
        "public_key_spki",
        "proof_of_possession",
        "signature",
        "agent_id",
        "agent_mount_id",
        "mount_identity",
        "generation",
        "session",
        "lease",
        "rejection_reason",
    ];
    assert_no_forbidden_fields(view, FORBIDDEN_FIELDS);
    assert!(view.get("display_name").is_some());
    assert!(view.get("probe").is_some());
    assert_eq!(view["proof_of_possession_status"], "verified");
}

fn assert_no_forbidden_fields(value: &Value, forbidden_fields: &[&str]) {
    match value {
        Value::Object(object) => {
            for (field, value) in object {
                assert!(
                    !forbidden_fields.contains(&field.as_str()),
                    "public view leaked {field}: {value}"
                );
                assert_no_forbidden_fields(value, forbidden_fields);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_no_forbidden_fields(value, forbidden_fields);
            }
        }
        _ => {}
    }
}

fn assert_problem(response: &JsonResponse, status: StatusCode, code: &str) {
    assert_eq!(response.status, status, "{}", response.json);
    assert_eq!(response.json["code"], code);
    assert_eq!(response.json["status"], status.as_u16());
}

fn write_keyring(path: &std::path::Path) {
    std::fs::write(
        path,
        serde_json::to_vec(&json!({
            "version": 1,
            "active_key_id": "enrollment-key-a",
            "keys": {
                "enrollment-key-a": URL_SAFE_NO_PAD.encode([0x42_u8; 32])
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

fn development_config(authority_dir: PathBuf, keyring: PathBuf) -> Config {
    Config {
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

fn now() -> UnixMillis {
    let wall_clock = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let monotonic = LAST_TEST_TIME_MS
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
            Some(wall_clock.max(last.saturating_add(1)))
        })
        .unwrap();
    UnixMillis::new(wall_clock.max(monotonic.saturating_add(1)))
}
