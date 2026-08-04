use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use fusen_rs::{RunningServer, ServerState};
use neoengram_core::{ContentDigest, LogicalPath};
use neoengram_protocol::{
    AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    DecisionGeneration, EdgeClusterId, Extensions, JobDecision, JobFinalized, JobId, JobState,
    MountGeneration, OwnerGeneration, PlacementGeneration, PlaygroundId, PrincipalId,
    PrincipalKind, PrincipalRef, ProjectId, PublishDecision, ResourceVersion, StorageVolumeId,
    TenantId, UnixMillis, WireIndexVersion,
};
use neoengram_server::{AppState, Config};
use neoengramd::{
    open_sqlite_authority, AddJobSpec, AllowAllAuthorizer, AssignJobRequest, AssignmentTarget,
    ControlPlane, CreateAddJobRequest, InMemoryClock, SqliteAuthorityConfig,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const AUTHORIZATION_DENIED: &str = "AUTHORIZATION_DENIED";
const JOB_NOT_FOUND: &str = "JOB_NOT_FOUND";

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
async fn create_rejects_every_server_owned_root_field_before_persisting() {
    let authority = TempDir::new().unwrap();
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;
    let address = running.local_addr();

    for field in ["actor", "principal", "request_digest"] {
        let job_id = format!("reserved-{field}");
        let mut request = add_request("tenant-a", &job_id);
        request
            .as_object_mut()
            .unwrap()
            .insert(field.to_owned(), json!("caller-controlled"));

        let response = post_json(address, "/api/job/add/create", &request).await;
        assert_problem(
            &response,
            422,
            "PROTOCOL_INVALID",
            "urn:neoengram:problem:protocol-invalid",
            false,
        );
        let problem = response.json();
        assert!(
            problem["violations"][0]["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains(field)),
            "reserved field must be identified without echoing its value: {problem}"
        );

        let query = post_json(
            address,
            "/api/job/query",
            &json!({"tenant_id": "tenant-a", "job_id": job_id}),
        )
        .await;
        assert_problem(
            &query,
            404,
            JOB_NOT_FOUND,
            "urn:neoengram:problem:job-not-found",
            false,
        );
    }

    stop(state, running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_survives_restart_and_job_view_never_exposes_internal_identity_or_leases() {
    let authority = TempDir::new().unwrap();
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;
    let address = running.local_addr();

    let mut request = add_request("tenant-a", "persistent-job");
    request.as_object_mut().unwrap().extend(
        [
            ("client_trace", json!("visible-extension")),
            ("assignment", json!({"secret": "assignment"})),
            ("assignment_id", json!("assignment-secret")),
            ("assignment_generation", json!(7)),
            ("assignment_target", json!({"agent": "agent-secret"})),
            ("agent_id", json!("agent-secret")),
            ("agent_mount_id", json!("mount-secret")),
            ("artifact_placement_id", json!("placement-secret")),
            ("storage_volume_id", json!("volume-secret")),
            ("mount_generation", json!(8)),
            ("generation", json!(9)),
            ("owner_generation", json!(10)),
            ("placement_generation", json!(11)),
            ("lease", json!({"token": "lease-secret"})),
            ("manifest", json!({"object": "manifest-secret"})),
            ("manifests", json!([{"object": "manifest-secret"}])),
            (
                "publication_candidate",
                json!({"digest": "publication-secret"}),
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value)),
    );

    let created = post_json(address, "/api/job/add/create", &request).await;
    assert_eq!(
        created.status,
        200,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let created_job = created.json()["job"].clone();
    assert_public_queued_job_view(&created_job);

    stop(state, running).await;

    let (restarted_state, restarted) = start(&config).await;
    let queried = post_json(
        restarted.local_addr(),
        "/api/job/query",
        &json!({"tenant_id": "tenant-a", "job_id": "persistent-job"}),
    )
    .await;
    assert_eq!(
        queried.status,
        200,
        "{}",
        String::from_utf8_lossy(&queried.body)
    );
    let queried_job = queried.json()["job"].clone();
    assert_public_queued_job_view(&queried_job);
    assert_eq!(queried_job, created_job);

    stop(restarted_state, restarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rbac_missing_disabled_and_cross_tenant_queries_are_deny_by_default_and_hidden() {
    let authority = TempDir::new().unwrap();
    let mut seed_config = development_config(authority.path().to_path_buf());
    seed_config.development_tenants = vec!["*".to_owned()];
    let (seed_state, seed_server) = start(&seed_config).await;
    let seeded = post_json(
        seed_server.local_addr(),
        "/api/job/add/create",
        &add_request("tenant-b", "private-job"),
    )
    .await;
    assert_eq!(
        seeded.status,
        200,
        "{}",
        String::from_utf8_lossy(&seeded.body)
    );
    stop(seed_state, seed_server).await;

    let policy_path = authority.path().join("rbac.json");
    write_policy(
        &policy_path,
        json!({
            "roles": {
                "operator": {
                    "permissions": ["create_add_job", "query_job", "finalize_add"]
                }
            },
            "bindings": []
        }),
    );
    let mut policy_config = development_config(authority.path().to_path_buf());
    policy_config.rbac_file = Some(policy_path.clone());

    let (missing_state, missing_server) = start(&policy_config).await;
    let missing_binding_query = query_private_job(missing_server.local_addr()).await;
    assert_job_not_found(&missing_binding_query);
    let missing_binding_create = post_json(
        missing_server.local_addr(),
        "/api/job/add/create",
        &add_request("tenant-a", "missing-binding-create"),
    )
    .await;
    assert_authorization_denied(&missing_binding_create);
    stop(missing_state, missing_server).await;

    write_policy(
        &policy_path,
        json!({
            "roles": {
                "operator": {
                    "permissions": ["create_add_job", "query_job", "finalize_add"]
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
    let disabled_query = query_private_job(disabled_server.local_addr()).await;
    assert_job_not_found(&disabled_query);
    let disabled_create = post_json(
        disabled_server.local_addr(),
        "/api/job/add/create",
        &add_request("tenant-a", "disabled-create"),
    )
    .await;
    assert_authorization_denied(&disabled_create);
    stop(disabled_state, disabled_server).await;

    write_policy(
        &policy_path,
        json!({
            "roles": {
                "operator": {
                    "permissions": ["create_add_job", "query_job", "finalize_add"]
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
    let cross_tenant = query_private_job(scoped_server.local_addr()).await;
    assert_job_not_found(&cross_tenant);
    let absent = post_json(
        scoped_server.local_addr(),
        "/api/job/query",
        &json!({"tenant_id": "tenant-b", "job_id": "absent-job"}),
    )
    .await;
    assert_job_not_found(&absent);

    let expected = problem_without_request_id(&absent);
    for hidden in [&missing_binding_query, &disabled_query, &cross_tenant] {
        assert_eq!(problem_without_request_id(hidden), expected);
    }

    stop(scoped_state, scoped_server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_finalize_replays_without_exposing_real_assignment_identity() {
    let authority = TempDir::new().unwrap();
    seed_succeeded_job(authority.path()).await;
    let config = development_config(authority.path().to_path_buf());
    let (state, running) = start(&config).await;

    let request = json!({"tenant_id": "tenant-a", "job_id": "terminal-job"});
    let first = post_json(running.local_addr(), "/api/job/add/finalize", &request).await;
    let second = post_json(running.local_addr(), "/api/job/add/finalize", &request).await;
    for response in [&first, &second] {
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let body = response.json();
        assert_eq!(body["replayed"], true);
        assert_eq!(body["job"]["state"], "succeeded");
        assert_eq!(body["decision"]["outcome"], "publish");
        let encoded = serde_json::to_string(&body["job"]).unwrap();
        for secret in [
            "assignment-secret",
            "agent-secret",
            "mount-secret",
            "volume-secret",
            "placement-secret",
        ] {
            assert!(!encoded.contains(secret), "JobView exposed {secret}");
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
                body["job"].get(forbidden).is_none(),
                "JobView leaked {forbidden}"
            );
        }
    }
    assert_eq!(first.body, second.body);

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
    assert_eq!(response.json()["api_versions"], json!([1]));

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

async fn seed_succeeded_job(path: &Path) {
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(path))
        .await
        .unwrap();
    let store = authority.authority_store();
    let principal = PrincipalRef {
        kind: PrincipalKind::User,
        id: PrincipalId::new("user-a").unwrap(),
        extensions: Extensions::new(),
    };
    let expected_index_version = WireIndexVersion {
        revision: neoengram_protocol::IndexRevision::new(0),
        digest: ContentDigest::from_bytes([0; 32]),
        extensions: Extensions::new(),
    };
    let mut spec = AddJobSpec {
        job_id: JobId::new("terminal-job").unwrap(),
        principal: principal.clone(),
        tenant_id: TenantId::new("tenant-a").unwrap(),
        project_id: ProjectId::new("project-a").unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        playground_id: PlaygroundId::new("playground-a").unwrap(),
        expected_index_version,
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(4_102_444_800_000),
        paths: vec![LogicalPath::parse("dataset/images").unwrap()],
        all: false,
        extensions: Extensions::new(),
    };
    spec.request_digest = spec.computed_request_digest().unwrap();
    let control = ControlPlane::new(
        std::sync::Arc::new(AllowAllAuthorizer),
        store.clone(),
        std::sync::Arc::new(InMemoryClock::new(1_000)),
    );
    control
        .create_add_job(CreateAddJobRequest {
            actor: principal.clone(),
            spec: spec.clone(),
        })
        .await
        .unwrap();
    let assigned = control
        .assign_job(AssignJobRequest {
            actor: principal,
            tenant_id: spec.tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target: AssignmentTarget {
                assignment_id: AssignmentId::new("assignment-secret").unwrap(),
                assignment_generation: AssignmentGeneration::new(1),
                agent_id: AgentId::new("agent-secret").unwrap(),
                edge_cluster_id: EdgeClusterId::new("cluster-secret").unwrap(),
                storage_volume_id: StorageVolumeId::new("volume-secret").unwrap(),
                artifact_placement_id: ArtifactPlacementId::new("placement-secret").unwrap(),
                placement_generation: PlacementGeneration::new(2),
                agent_mount_id: AgentMountId::new("mount-secret").unwrap(),
                mount_generation: MountGeneration::new(3),
                owner_generation: OwnerGeneration::new(4),
                lease: None,
            },
        })
        .await
        .unwrap();
    let mut job = assigned.job;
    let assignment = job.assignment.as_ref().unwrap();
    let published_index_version = WireIndexVersion {
        revision: neoengram_protocol::IndexRevision::new(1),
        digest: ContentDigest::hash(b"published-index"),
        extensions: Extensions::new(),
    };
    let decision = JobDecision {
        job_id: job.spec.job_id.clone(),
        assignment_id: assignment.assignment_id.clone(),
        assignment_generation: assignment.assignment_generation,
        decision_generation: DecisionGeneration::new(1),
        decision: PublishDecision::Publish {
            published_index_version,
            extensions: Extensions::new(),
        },
        final_state: JobState::Succeeded,
        extensions: Extensions::new(),
    };
    let finalized = JobFinalized {
        job_id: job.spec.job_id.clone(),
        assignment_id: assignment.assignment_id.clone(),
        assignment_generation: assignment.assignment_generation,
        decision_generation: DecisionGeneration::new(1),
        final_state: JobState::Succeeded,
        finalized_at_unix_ms: UnixMillis::new(2_000),
        extensions: Extensions::new(),
    };
    decision.validate().unwrap();
    finalized.validate().unwrap();
    let previous = job.resource_version.get();
    job.resource_version = ResourceVersion::new(previous + 1);
    job.state = JobState::Succeeded;
    job.decision = Some(decision);
    job.finalized = Some(finalized);
    store.jobs().replace(previous, job).await.unwrap();
    authority.close().await;
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

fn add_request(tenant_id: &str, job_id: &str) -> Value {
    json!({
        "tenant_id": tenant_id,
        "project_id": "project-a",
        "artifact_id": "artifact-a",
        "playground_id": "playground-a",
        "job_id": job_id,
        "expected_index_version": {
            "revision": "0",
            "digest": "0".repeat(64)
        },
        "deadline_unix_ms": "4102444800000",
        "paths": ["dataset/images"],
        "all": false
    })
}

fn write_policy(path: &Path, policy: Value) {
    std::fs::write(path, serde_json::to_vec(&policy).unwrap()).unwrap();
}

async fn query_private_job(address: SocketAddr) -> RawResponse {
    post_json(
        address,
        "/api/job/query",
        &json!({"tenant_id": "tenant-b", "job_id": "private-job"}),
    )
    .await
}

fn assert_public_queued_job_view(job: &Value) {
    let object = job.as_object().expect("JobView must be a JSON object");
    let actual: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    let expected = BTreeSet::from([
        "artifact_id",
        "client_trace",
        "deadline_unix_ms",
        "job_id",
        "operation",
        "playground_id",
        "project_id",
        "resource_version",
        "state",
        "tenant_id",
    ]);
    assert_eq!(
        actual, expected,
        "JobView must remain an explicit whitelist"
    );
    assert_eq!(
        object.get("client_trace"),
        Some(&json!("visible-extension"))
    );
    assert_eq!(object.get("state"), Some(&json!("queued")));

    for forbidden in [
        "accepted",
        "actor",
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
        "principal",
        "publication_candidate",
        "request_digest",
        "storage_volume_id",
    ] {
        assert!(
            !object.contains_key(forbidden),
            "JobView leaked {forbidden}"
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

fn assert_job_not_found(response: &RawResponse) {
    assert_problem(
        response,
        404,
        JOB_NOT_FOUND,
        "urn:neoengram:problem:job-not-found",
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
