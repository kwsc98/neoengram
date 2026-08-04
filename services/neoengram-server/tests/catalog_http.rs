use std::path::PathBuf;

use neoengram_server::{AppState, Config};
use reqwest::StatusCode;
use serde_json::{json, Value};

#[tokio::test]
async fn tenant_and_storage_action_apis_are_authoritative_and_redacted() {
    let directory = tempfile::tempdir().unwrap();
    let config = development_config(directory.path().to_owned());
    let state = AppState::initialize(&config).await.unwrap();
    let running = state.start_server(&config).await.unwrap();
    let client = reqwest::Client::new();
    let base = format!("http://{}", running.local_addr());

    let create_tenant = post(
        &client,
        &base,
        "/api/tenant/create",
        json!({
            "tenant_id": "tenant-a",
            "display_name": "Research",
            "description": "managed data"
        }),
    )
    .await;
    assert_eq!(create_tenant.0, StatusCode::OK);
    assert_eq!(create_tenant.1["replayed"], false);
    let replay = post(
        &client,
        &base,
        "/api/tenant/create",
        json!({
            "tenant_id": "tenant-a",
            "display_name": "Research",
            "description": "managed data"
        }),
    )
    .await;
    assert_eq!(replay.0, StatusCode::OK);
    assert_eq!(replay.1["replayed"], true);

    let volume = post(
        &client,
        &base,
        "/api/storage/volume/create",
        json!({
            "tenant_id": "tenant-a",
            "storage_volume_id": "volume-nfs",
            "display_name": "Private NFS",
            "edge_cluster_id": "cluster-a",
            "region": "cn-shanghai",
            "backend_type": "nfs",
            "access_mode": "read_write_many",
            "nfs_reference": {
                "server": "nfs.internal",
                "export_path": "/exports/private"
            }
        }),
    )
    .await;
    assert_eq!(volume.0, StatusCode::OK);
    let public = &volume.1["storage_volume"];
    assert_eq!(public["state"], "unavailable");
    assert!(public.get("nfs_reference").is_none());
    assert!(public.get("pvc_reference").is_none());

    let list = post(
        &client,
        &base,
        "/api/storage/volume/list/query",
        json!({"tenant_id": "tenant-a", "page_size": 1}),
    )
    .await;
    assert_eq!(list.0, StatusCode::OK);
    assert_eq!(list.1["items"].as_array().unwrap().len(), 1);

    let playground = post(
        &client,
        &base,
        "/api/playground/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "playground_id": "workspace-a",
            "storage_volume_id": "volume-nfs",
            "display_name": "Workspace"
        }),
    )
    .await;
    assert_eq!(playground.0, StatusCode::CONFLICT);
    assert_eq!(playground.1["code"], "STORAGE_VOLUME_NOT_READY");

    running.handle().shutdown().await.unwrap();
    state.close().await;
}

async fn post(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = client
        .post(format!("{base}{path}"))
        .bearer_auth("test-secret")
        .header("NeoEngram-API-Version", "1")
        .header("X-Request-ID", "req:catalog-http")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json().await.unwrap();
    (status, body)
}

fn development_config(authority_dir: PathBuf) -> Config {
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        agent_enrollment_enabled: false,
        agent_bind: None,
        agent_enrollment_keyring_file: None,
        authority_dir,
        object_store_root: None,
        rbac_file: None,
        oidc_issuer: None,
        oidc_audience: None,
        oidc_jwks_uri: None,
        development: true,
        development_token: Some("test-secret".to_owned()),
        development_principal: "user-a".to_owned(),
        development_tenants: vec!["*".to_owned()],
        request_timeout_secs: 5,
        max_request_body_bytes: 64 * 1024,
        max_response_body_bytes: 64 * 1024,
        max_concurrent_requests: 16,
        graceful_shutdown_secs: 2,
    }
}
