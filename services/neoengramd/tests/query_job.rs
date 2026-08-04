use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use neoengram_core::{ContentDigest, IndexVersion, LogicalPath};
use neoengram_protocol::{
    ArtifactId, Extensions, JobId, PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef,
    ProjectId, TenantId, UnixMillis, WireIndexVersion,
};
use neoengramd::{
    Action, Actor, AddJobSpec, AuthorizationRequest, Authorizer, CentralError, CentralErrorCode,
    CentralResult, ControlPlane, CreateAddJobRequest, InMemoryComponents, QueryJobRequest,
};

#[derive(Debug, Clone, Copy)]
enum QueryDecision {
    Allow,
    Unauthorized,
    BackendFailure,
}

struct RecordingAuthorizer {
    decision: QueryDecision,
    queries: Mutex<Vec<AuthorizationRequest>>,
}

impl RecordingAuthorizer {
    fn new(decision: QueryDecision) -> Self {
        Self {
            decision,
            queries: Mutex::new(Vec::new()),
        }
    }

    fn queries(&self) -> Vec<AuthorizationRequest> {
        self.queries.lock().unwrap().clone()
    }
}

#[async_trait]
impl Authorizer for RecordingAuthorizer {
    async fn authorize(&self, request: &AuthorizationRequest) -> CentralResult<()> {
        if request.action != Action::QueryJob {
            return Ok(());
        }
        self.queries.lock().unwrap().push(request.clone());
        match self.decision {
            QueryDecision::Allow => Ok(()),
            QueryDecision::Unauthorized => Err(CentralError::new(
                CentralErrorCode::Unauthorized,
                "query is outside the actor's visible scope",
            )),
            QueryDecision::BackendFailure => Err(CentralError::new(
                CentralErrorCode::StorageFailure,
                "authorization backend is unavailable",
            )),
        }
    }
}

#[tokio::test]
async fn query_returns_the_authoritative_job_and_authorizes_its_persisted_scope() {
    let (control, authorizer, creator, spec) = fixture(QueryDecision::Allow).await;
    let viewer = principal("viewer-a");

    let result = control
        .query_job(query_request(&viewer, &spec))
        .await
        .unwrap();

    assert_eq!(result.job.spec, spec);
    let queries = authorizer.queries();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].actor, Actor::Principal(viewer));
    assert_eq!(queries[0].action, Action::QueryJob);
    assert_eq!(queries[0].tenant_id, result.job.spec.tenant_id);
    assert_eq!(queries[0].artifact_id, result.job.spec.artifact_id);
    assert_eq!(queries[0].playground_id, result.job.spec.playground_id);
    assert_eq!(queries[0].job_id, result.job.spec.job_id);
    assert_ne!(queries[0].actor, Actor::Principal(creator));
}

#[tokio::test]
async fn query_returns_not_found_without_authorizing_a_missing_job() {
    let (control, authorizer, actor, spec) = fixture(QueryDecision::Allow).await;

    let error = control
        .query_job(QueryJobRequest {
            actor,
            tenant_id: spec.tenant_id,
            job_id: JobId::new("missing-job").unwrap(),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), CentralErrorCode::JobNotFound);
    assert!(authorizer.queries().is_empty());
}

#[tokio::test]
async fn query_hides_an_authorization_denial_as_not_found() {
    let (control, authorizer, actor, spec) = fixture(QueryDecision::Unauthorized).await;

    let error = control
        .query_job(query_request(&actor, &spec))
        .await
        .unwrap_err();

    assert_eq!(error.code(), CentralErrorCode::JobNotFound);
    let queries = authorizer.queries();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].tenant_id, spec.tenant_id);
    assert_eq!(queries[0].artifact_id, spec.artifact_id);
    assert_eq!(queries[0].playground_id, spec.playground_id);
    assert_eq!(queries[0].job_id, spec.job_id);
}

#[tokio::test]
async fn query_preserves_non_authorization_failures() {
    let (control, authorizer, actor, spec) = fixture(QueryDecision::BackendFailure).await;

    let error = control
        .query_job(query_request(&actor, &spec))
        .await
        .unwrap_err();

    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    assert_eq!(error.message(), "authorization backend is unavailable");
    assert_eq!(authorizer.queries().len(), 1);
}

async fn fixture(
    decision: QueryDecision,
) -> (
    ControlPlane,
    Arc<RecordingAuthorizer>,
    PrincipalRef,
    AddJobSpec,
) {
    let components = InMemoryComponents::new(100);
    let authorizer = Arc::new(RecordingAuthorizer::new(decision));
    let control = ControlPlane::new(
        authorizer.clone(),
        components.authority_store(),
        components.clock.clone(),
    );
    let creator = principal("creator-a");
    let spec = add_job_spec(&creator);
    control
        .create_add_job(CreateAddJobRequest {
            actor: creator.clone(),
            spec: spec.clone(),
        })
        .await
        .unwrap();
    (control, authorizer, creator, spec)
}

fn add_job_spec(principal: &PrincipalRef) -> AddJobSpec {
    let mut spec = AddJobSpec {
        job_id: JobId::new("query-job-a").unwrap(),
        principal: principal.clone(),
        tenant_id: TenantId::new("tenant-real").unwrap(),
        project_id: ProjectId::new("project-real").unwrap(),
        artifact_id: ArtifactId::new("artifact-real").unwrap(),
        playground_id: PlaygroundId::new("playground-real").unwrap(),
        expected_index_version: WireIndexVersion::from(
            IndexVersion::from_snapshot(0, &[]).unwrap(),
        ),
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(10_000),
        paths: vec![LogicalPath::parse("dataset/file.bin").unwrap()],
        all: false,
        extensions: Extensions::new(),
    };
    spec.request_digest = spec.computed_request_digest().unwrap();
    spec
}

fn principal(id: &str) -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::User,
        id: PrincipalId::new(id).unwrap(),
        extensions: Extensions::new(),
    }
}

fn query_request(actor: &PrincipalRef, spec: &AddJobSpec) -> QueryJobRequest {
    QueryJobRequest {
        actor: actor.clone(),
        tenant_id: spec.tenant_id.clone(),
        job_id: spec.job_id.clone(),
    }
}
