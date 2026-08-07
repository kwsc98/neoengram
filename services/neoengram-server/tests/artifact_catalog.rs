use std::sync::Arc;

use neoengram_core::ContentDigest;
use neoengram_protocol::{
    ArtifactId, EdgeClusterId, PlaygroundId, PrincipalKind, ProjectId, StorageVolumeId, TenantId,
    UnixMillis,
};
use neoengram_server::{
    dto::{
        ArtifactInitialization as ArtifactInitializationBody, CreateArtifactRequest,
        CreatePlaygroundRequest, JsonExtensions, QueryArtifactCommitGraphRequest,
        QueryArtifactListRequest, QueryArtifactRequest,
    },
    AuthenticatedIdentity, CatalogService, Permission, StaticRbacPolicy, SystemService,
};
use neoengramd::{
    ArtifactInitialization, ArtifactRecord, CatalogPvcReference, ControlCatalogRepository,
    InMemoryComponents, PlaygroundRecord, PlaygroundState, StorageAccessMode, StorageBackendType,
    StorageVolumeRecord, StorageVolumeState, TenantRecord,
};

#[test]
fn api_version_advertises_the_minimal_artifact_catalog() {
    let version = SystemService.query_api_version();
    assert!(version
        .capabilities
        .iter()
        .any(|capability| capability == "artifact_catalog"));
    assert!(version
        .capabilities
        .iter()
        .any(|capability| capability == "artifact_commit_graph"));
    assert!(!version
        .capabilities
        .iter()
        .any(|capability| capability == "resource_browser"));
}

#[tokio::test]
async fn artifact_catalog_is_idempotent_paginated_and_tenant_isolated() {
    let components = InMemoryComponents::new(1_000);
    let tenant_a = TenantId::new("tenant-a").unwrap();
    let tenant_b = TenantId::new("tenant-b").unwrap();
    insert_tenant(&components, tenant_a.clone()).await;
    insert_tenant(&components, tenant_b.clone()).await;
    let service = catalog_service(
        &components,
        [tenant_a.to_string()],
        [Permission::ArtifactRead, Permission::ArtifactCreate],
    );
    let identity = identity();

    let request = create_artifact_request("artifact-a", "Artifact A");
    let created = service
        .create_artifact(&identity, request.clone())
        .await
        .unwrap();
    assert!(!created.replayed);
    assert_eq!(
        created.artifact.initialization,
        ArtifactInitializationBody::Empty
    );
    assert!(created.artifact.head_commit_id.is_none());

    let replayed = service.create_artifact(&identity, request).await.unwrap();
    assert!(replayed.replayed);
    components.clock.advance(1).unwrap();
    service
        .create_artifact(
            &identity,
            create_artifact_request("artifact-b", "Artifact B"),
        )
        .await
        .unwrap();

    let first = service
        .list_artifacts(
            &identity,
            QueryArtifactListRequest {
                tenant_id: tenant_a.to_string(),
                project_id: Some("project-a".to_owned()),
                cursor: None,
                page_size: Some(1),
                query: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].artifact_id, "artifact-b");
    let second = service
        .list_artifacts(
            &identity,
            QueryArtifactListRequest {
                tenant_id: tenant_a.to_string(),
                project_id: Some("project-a".to_owned()),
                cursor: first.next_cursor,
                page_size: Some(1),
                query: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].artifact_id, "artifact-a");
    assert!(second.next_cursor.is_none());

    let queried = service
        .query_artifact(
            &identity,
            QueryArtifactRequest {
                tenant_id: tenant_a.to_string(),
                project_id: "project-a".to_owned(),
                artifact_id: "artifact-a".to_owned(),
            },
        )
        .await
        .unwrap();
    assert_eq!(queried.artifact.display_name, "Artifact A");

    let empty_graph = service
        .query_artifact_commit_graph(
            &identity,
            QueryArtifactCommitGraphRequest {
                tenant_id: tenant_a.to_string(),
                project_id: "project-a".to_owned(),
                artifact_id: "artifact-a".to_owned(),
                cursor: None,
                page_size: Some(10),
            },
        )
        .await
        .unwrap();
    assert_eq!(empty_graph.graph.graph_version, "1");
    assert!(empty_graph.graph.head_commit_id.is_none());
    assert!(empty_graph.graph.nodes.is_empty());
    assert!(empty_graph.graph.next_cursor.is_none());

    components
        .control_catalog
        .insert_artifact(ArtifactRecord {
            tenant_id: tenant_b.clone(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            display_name: "Hidden".to_owned(),
            description: None,
            initialization: ArtifactInitialization::Empty,
            head_commit_id: None,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    let hidden = service
        .query_artifact(
            &identity,
            QueryArtifactRequest {
                tenant_id: tenant_b.to_string(),
                project_id: "project-a".to_owned(),
                artifact_id: "artifact-a".to_owned(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(hidden.code().as_str(), "resource_not_found");
    let hidden_graph = service
        .query_artifact_commit_graph(
            &identity,
            QueryArtifactCommitGraphRequest {
                tenant_id: tenant_b.to_string(),
                project_id: "project-a".to_owned(),
                artifact_id: "artifact-a".to_owned(),
                cursor: None,
                page_size: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(hidden_graph.code().as_str(), "resource_not_found");

    let reused = service
        .create_artifact(
            &identity,
            create_artifact_request("artifact-a", "Different name"),
        )
        .await
        .unwrap_err();
    assert_eq!(reused.code().as_str(), "artifact_id_reused");

    let mut cross_project = create_artifact_request("artifact-a", "Artifact A");
    cross_project.project_id = "project-b".to_owned();
    let cross_project = service
        .create_artifact(&identity, cross_project)
        .await
        .unwrap_err();
    assert_eq!(cross_project.code().as_str(), "artifact_id_reused");

    let derived = service
        .create_artifact(
            &identity,
            CreateArtifactRequest {
                tenant_id: tenant_a.to_string(),
                project_id: "project-a".to_owned(),
                artifact_id: "artifact-derived".to_owned(),
                display_name: "Derived".to_owned(),
                description: None,
                initialization: ArtifactInitializationBody::Derived {
                    source_project_id: "project-a".to_owned(),
                    source_artifact_id: "artifact-a".to_owned(),
                    source_commit_id: "b".repeat(64),
                },
                extensions: JsonExtensions::default(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        derived.code().as_str(),
        "artifact_derived_initialization_unsupported"
    );
}

#[tokio::test]
async fn playground_requires_an_artifact_and_freezes_the_authoritative_head() {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let volume_id = StorageVolumeId::new("volume-a").unwrap();
    insert_tenant(&components, tenant_id.clone()).await;
    components
        .control_catalog
        .insert_storage_volume(StorageVolumeRecord {
            tenant_id: tenant_id.clone(),
            storage_volume_id: volume_id.clone(),
            display_name: "Volume A".to_owned(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            region: "cn-shanghai".to_owned(),
            backend_type: StorageBackendType::Pvc,
            access_mode: StorageAccessMode::ReadWriteMany,
            pvc_reference: Some(CatalogPvcReference {
                namespace: "neoengram".to_owned(),
                claim_name: "data-a".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    let service = catalog_service(
        &components,
        [tenant_id.to_string()],
        [Permission::PlaygroundCreate],
    );
    let identity = identity();

    let missing = service
        .create_playground(
            &identity,
            create_playground_request("playground-missing", None),
        )
        .await
        .unwrap_err();
    assert_eq!(missing.code().as_str(), "resource_not_found");

    let head = ContentDigest::from_bytes([0xaa; 32]);
    components
        .control_catalog
        .insert_artifact(ArtifactRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            display_name: "Artifact A".to_owned(),
            description: None,
            initialization: ArtifactInitialization::Empty,
            head_commit_id: Some(head.clone()),
            resource_version: 2,
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_playground(PlaygroundRecord {
            tenant_id,
            project_id,
            artifact_id,
            playground_id: PlaygroundId::new("playground-replay").unwrap(),
            storage_volume_id: volume_id,
            region: "cn-shanghai".to_owned(),
            display_name: "Playground".to_owned(),
            base_commit_id: Some(head.clone()),
            head_commit_id: Some(head.clone()),
            state: PlaygroundState::Ready,
            relative_root: "playgrounds/project-a/artifact-a/playground-replay".to_owned(),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    let omitted_base = service
        .create_playground(
            &identity,
            create_playground_request("playground-replay", None),
        )
        .await
        .unwrap();
    assert!(omitted_base.replayed);
    assert_eq!(
        omitted_base.playground.base_commit_id,
        Some(head.to_string())
    );

    let replay = service
        .create_playground(
            &identity,
            create_playground_request("playground-replay", Some(head.to_string())),
        )
        .await
        .unwrap();
    assert!(replay.replayed);

    let inherited = service
        .create_playground(
            &identity,
            create_playground_request("playground-inherited", None),
        )
        .await
        .unwrap();
    assert!(!inherited.replayed);
    assert_eq!(inherited.playground.state, "creating");
    assert_eq!(inherited.playground.base_commit_id, Some(head.to_string()));
    assert_eq!(inherited.playground.head_commit_id, Some(head.to_string()));

    let missing_commit = service
        .create_playground(
            &identity,
            create_playground_request("playground-mismatch", Some("bb".repeat(32))),
        )
        .await
        .unwrap_err();
    assert_eq!(missing_commit.code().as_str(), "resource_not_found");
}

fn catalog_service(
    components: &InMemoryComponents,
    tenants: impl IntoIterator<Item = String>,
    permissions: impl IntoIterator<Item = Permission>,
) -> CatalogService {
    let policy = Arc::new(StaticRbacPolicy::one_principal("user-a", tenants, permissions).unwrap());
    CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy,
        components.clock.clone(),
    )
    .with_precommits(components.precommits.clone())
}

async fn insert_tenant(components: &InMemoryComponents, tenant_id: TenantId) {
    components
        .control_catalog
        .insert_tenant(TenantRecord {
            tenant_id,
            display_name: "Tenant".to_owned(),
            description: None,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
}

fn create_artifact_request(artifact_id: &str, display_name: &str) -> CreateArtifactRequest {
    CreateArtifactRequest {
        tenant_id: "tenant-a".to_owned(),
        project_id: "project-a".to_owned(),
        artifact_id: artifact_id.to_owned(),
        display_name: display_name.to_owned(),
        description: Some("authoritative data".to_owned()),
        initialization: ArtifactInitializationBody::Empty,
        extensions: JsonExtensions::default(),
    }
}

fn create_playground_request(
    playground_id: &str,
    base_commit_id: Option<String>,
) -> CreatePlaygroundRequest {
    CreatePlaygroundRequest {
        tenant_id: "tenant-a".to_owned(),
        project_id: "project-a".to_owned(),
        artifact_id: "artifact-a".to_owned(),
        playground_id: playground_id.to_owned(),
        storage_volume_id: "volume-a".to_owned(),
        display_name: "Playground".to_owned(),
        base_commit_id,
        extensions: JsonExtensions::default(),
    }
}

fn identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap()
}
