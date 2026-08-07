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

    let artifact = post(
        &client,
        &base,
        "/api/artifact/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "display_name": "Authoritative data",
            "initialization": { "mode": "empty" }
        }),
    )
    .await;
    assert_eq!(artifact.0, StatusCode::OK);
    assert_eq!(artifact.1["artifact"]["initialization"]["mode"], "empty");
    assert_eq!(artifact.1["replayed"], false);
    let artifact_replay = post(
        &client,
        &base,
        "/api/artifact/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "display_name": "Authoritative data",
            "initialization": { "mode": "empty" }
        }),
    )
    .await;
    assert_eq!(artifact_replay.0, StatusCode::OK);
    assert_eq!(artifact_replay.1["replayed"], true);

    let artifact_query = post(
        &client,
        &base,
        "/api/artifact/query",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a"
        }),
    )
    .await;
    assert_eq!(artifact_query.0, StatusCode::OK);
    assert_eq!(artifact_query.1["artifact"]["artifact_id"], "artifact-a");
    let commit_graph = post(
        &client,
        &base,
        "/api/artifact/commit/graph/query",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "page_size": 10
        }),
    )
    .await;
    assert_eq!(commit_graph.0, StatusCode::OK);
    assert_eq!(commit_graph.1["graph"]["graph_version"], "1");
    assert_eq!(commit_graph.1["graph"]["nodes"], json!([]));
    let artifact_list = post(
        &client,
        &base,
        "/api/artifact/list/query",
        json!({"tenant_id": "tenant-a", "project_id": "project-a"}),
    )
    .await;
    assert_eq!(artifact_list.0, StatusCode::OK);
    assert_eq!(artifact_list.1["items"].as_array().unwrap().len(), 1);

    let missing_job_scope = post(
        &client,
        &base,
        "/api/job/add/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "playground_id": "workspace-missing",
            "job_id": "job-missing-scope",
            "expected_index_version": {
                "revision": "0",
                "digest": "0000000000000000000000000000000000000000000000000000000000000000"
            },
            "deadline_unix_ms": "4102444800000",
            "paths": [],
            "all": true
        }),
    )
    .await;
    assert_eq!(missing_job_scope.0, StatusCode::NOT_FOUND);
    assert_eq!(missing_job_scope.1["code"], "JOB_NOT_FOUND");

    let derived = post(
        &client,
        &base,
        "/api/artifact/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-derived",
            "display_name": "Derived data",
            "initialization": {
                "mode": "derived",
                "source_project_id": "project-a",
                "source_artifact_id": "artifact-a",
                "source_commit_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            }
        }),
    )
    .await;
    assert_eq!(derived.0, StatusCode::CONFLICT);
    assert_eq!(
        derived.1["code"],
        "ARTIFACT_DERIVED_INITIALIZATION_UNSUPPORTED"
    );

    let missing_artifact = post(
        &client,
        &base,
        "/api/playground/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-missing",
            "playground_id": "workspace-missing",
            "storage_volume_id": "volume-nfs",
            "display_name": "Missing Artifact"
        }),
    )
    .await;
    assert_eq!(missing_artifact.0, StatusCode::NOT_FOUND);
    assert_eq!(missing_artifact.1["code"], "RESOURCE_NOT_FOUND");

    let malformed_base_commit = post(
        &client,
        &base,
        "/api/playground/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "playground_id": "workspace-malformed-commit",
            "storage_volume_id": "volume-nfs",
            "display_name": "Malformed base",
            "base_commit_id": "not-a-commit"
        }),
    )
    .await;
    assert_eq!(malformed_base_commit.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(malformed_base_commit.1["code"], "PROTOCOL_INVALID");

    let missing_base_commit = post(
        &client,
        &base,
        "/api/playground/create",
        json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "playground_id": "workspace-mismatch",
            "storage_volume_id": "volume-nfs",
            "display_name": "Mismatched base",
            "base_commit_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        }),
    )
    .await;
    assert_eq!(missing_base_commit.0, StatusCode::NOT_FOUND);
    assert_eq!(missing_base_commit.1["code"], "RESOURCE_NOT_FOUND");

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
