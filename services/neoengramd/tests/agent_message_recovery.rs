#![cfg(feature = "authority-sqlite")]

use std::{path::Path, sync::Arc};

use neoengram_core::{ContentDigest, IndexVersion, LogicalPath};
use neoengram_protocol::{
    AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    ControlMessage, EdgeClusterId, Extensions, MountGeneration, OwnerGeneration,
    PlacementGeneration, PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef, ProjectId,
    SessionGeneration, StorageVolumeId, TenantId, UnixMillis, WireIndexVersion,
};
use neoengramd::{
    open_sqlite_authority, AddJobSpec, AgentReport, AllowAllAuthorizer, AssignJobRequest,
    AssignmentRetireOutcome, AssignmentTarget, AuthorityStore, ControlPlane, CreateAddJobRequest,
    ExpireAddJobRequest, InMemoryClock, InMemoryComponents, ReceiveReportRequest,
    SqliteAuthorityConfig,
};
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
use tempfile::TempDir;

#[tokio::test]
async fn agent_scoped_decision_query_applies_limit_after_filtering() {
    let components = InMemoryComponents::new(100);
    assert_agent_scoped_decision_delivery(components.authority_store(), components.clock.clone())
        .await;

    let directory = TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert_agent_scoped_decision_delivery(
        authority.authority_store(),
        Arc::new(InMemoryClock::new(100)),
    )
    .await;
}

async fn assert_agent_scoped_decision_delivery(store: AuthorityStore, clock: Arc<InMemoryClock>) {
    let control = ControlPlane::new(Arc::new(AllowAllAuthorizer), store.clone(), clock.clone());
    let actor = principal();

    for job_id in ["job-00", "job-01", "job-02"] {
        control
            .create_add_job(CreateAddJobRequest {
                actor: actor.clone(),
                spec: job_spec(job_id, &actor),
            })
            .await
            .unwrap();
    }

    let target_spec = job_spec("job-zz", &actor);
    let target = assignment_target("assignment-zz", "agent-target");
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: target_spec.clone(),
        })
        .await
        .unwrap();
    control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: target_spec.tenant_id.clone(),
            job_id: target_spec.job_id.clone(),
            target: target.clone(),
        })
        .await
        .unwrap();

    clock.set(target_spec.deadline_unix_ms.get());
    let expired = control
        .expire_add_job(ExpireAddJobRequest {
            actor,
            tenant_id: target_spec.tenant_id.clone(),
            job_id: target_spec.job_id.clone(),
        })
        .await
        .unwrap();

    let pending = store
        .jobs()
        .list_pending_decisions_for_agent(&target.agent_id, 1)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].spec.job_id, target_spec.job_id);

    let messages = control
        .poll_agent_messages(&target.agent_id, SessionGeneration::new(1), 1)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    let ControlMessage::Decision(decision) = &messages[0].message else {
        panic!("target Agent must receive its pending decision");
    };
    assert_eq!(decision.job_id, target_spec.job_id);

    control
        .receive_report(ReceiveReportRequest {
            tenant_id: target_spec.tenant_id,
            agent_id: target.agent_id.clone(),
            report: AgentReport::Finalized(expired.finalized.unwrap()),
        })
        .await
        .unwrap();
    assert!(store
        .jobs()
        .list_pending_decisions_for_agent(&target.agent_id, 1)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn sqlite_v2_outbox_migration_preserves_delivery_and_durable_retirement() {
    let directory = TempDir::new().unwrap();
    let actor = principal();
    let spec = job_spec("job-migration", &actor);
    let target = assignment_target("assignment-migration", "agent-migration");

    {
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let control = ControlPlane::new(
            Arc::new(AllowAllAuthorizer),
            authority.authority_store(),
            Arc::new(neoengramd::InMemoryClock::new(100)),
        );
        control
            .create_add_job(CreateAddJobRequest {
                actor: actor.clone(),
                spec: spec.clone(),
            })
            .await
            .unwrap();
        control
            .assign_job(AssignJobRequest {
                actor: actor.clone(),
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: target.clone(),
            })
            .await
            .unwrap();
        authority.close().await;
    }

    downgrade_current_authority_to_v2(directory.path()).await;

    {
        let migrated = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        migrated.integrity_check().await.unwrap();
        let store = migrated.authority_store();
        let pending = store
            .outbox()
            .pending_for_agent(&target.agent_id, 1)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            store
                .outbox()
                .retire(&spec.tenant_id, &target.assignment_id)
                .await
                .unwrap(),
            AssignmentRetireOutcome::Retired
        );
        assert!(store
            .outbox()
            .pending_for_agent(&target.agent_id, 1)
            .await
            .unwrap()
            .is_empty());
        migrated.close().await;
    }

    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let store = reopened.authority_store();
    assert!(store
        .outbox()
        .pending_for_agent(&target.agent_id, 1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .outbox()
            .retire(&spec.tenant_id, &target.assignment_id)
            .await
            .unwrap(),
        AssignmentRetireOutcome::AlreadyRetired
    );
    assert_eq!(reopened.published_assignments().await.unwrap().len(), 1);
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::User,
        id: PrincipalId::new("user-a").unwrap(),
        extensions: Extensions::new(),
    }
}

fn job_spec(job_id: &str, principal: &PrincipalRef) -> AddJobSpec {
    let mut spec = AddJobSpec {
        job_id: neoengram_protocol::JobId::new(job_id).unwrap(),
        principal: principal.clone(),
        tenant_id: TenantId::new("tenant-a").unwrap(),
        project_id: ProjectId::new("project-a").unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        playground_id: PlaygroundId::new("playground-a").unwrap(),
        expected_index_version: WireIndexVersion::from(
            IndexVersion::from_snapshot(0, &[]).unwrap(),
        ),
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(1_000),
        paths: vec![LogicalPath::parse("dataset/file.bin").unwrap()],
        all: false,
        extensions: Extensions::new(),
    };
    spec.request_digest = spec.computed_request_digest().unwrap();
    spec
}

fn assignment_target(assignment_id: &str, agent_id: &str) -> AssignmentTarget {
    AssignmentTarget {
        assignment_id: AssignmentId::new(assignment_id).unwrap(),
        assignment_generation: AssignmentGeneration::new(1),
        agent_id: AgentId::new(agent_id).unwrap(),
        edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
        artifact_placement_id: ArtifactPlacementId::new(format!("placement-{assignment_id}"))
            .unwrap(),
        placement_generation: PlacementGeneration::new(1),
        agent_mount_id: AgentMountId::new("mount-a").unwrap(),
        mount_generation: MountGeneration::new(1),
        owner_generation: OwnerGeneration::new(1),
        lease: None,
    }
}

async fn downgrade_current_authority_to_v2(path: &Path) {
    let options = SqliteConnectOptions::new()
        .filename(path.join("authority.sqlite3"))
        .foreign_keys(false);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE precommit_mutations;
         DROP TABLE commit_records;
         DROP TABLE precommit_records;
         DROP TABLE object_placements;
         ALTER TABLE assignment_outbox RENAME TO assignment_outbox_v4;
         CREATE TABLE assignment_outbox (
             tenant_id TEXT NOT NULL,
             assignment_id TEXT NOT NULL,
             job_id TEXT NOT NULL,
             payload BLOB NOT NULL,
             published INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0, 1)),
             PRIMARY KEY (tenant_id, assignment_id),
             FOREIGN KEY (tenant_id, job_id) REFERENCES control_jobs (tenant_id, job_id)
         ) STRICT;
         INSERT INTO assignment_outbox
             (tenant_id, assignment_id, job_id, payload, published)
             SELECT tenant_id, assignment_id, job_id, payload, published
             FROM assignment_outbox_v4;
         DROP TABLE assignment_outbox_v4;
         DROP TABLE playground_index_records;
         DROP TABLE playground_indexes;
         CREATE TABLE playground_indexes (
             tenant_id TEXT NOT NULL,
             artifact_id TEXT NOT NULL,
             playground_id TEXT NOT NULL,
             revision TEXT NOT NULL CHECK (revision <> '' AND revision NOT GLOB '*[^0-9]*'),
             digest BLOB NOT NULL CHECK (length(digest) = 32),
             PRIMARY KEY (tenant_id, artifact_id, playground_id)
         ) STRICT;
         CREATE TABLE playground_index_records (
             tenant_id TEXT NOT NULL,
             artifact_id TEXT NOT NULL,
             playground_id TEXT NOT NULL,
             path TEXT NOT NULL,
             manifest_id BLOB NOT NULL CHECK (length(manifest_id) = 32),
             total_size TEXT NOT NULL CHECK (total_size <> '' AND total_size NOT GLOB '*[^0-9]*'),
             chunk_count TEXT NOT NULL CHECK (chunk_count <> '' AND chunk_count NOT GLOB '*[^0-9]*'),
             PRIMARY KEY (tenant_id, artifact_id, playground_id, path),
             FOREIGN KEY (tenant_id, artifact_id, playground_id)
                 REFERENCES playground_indexes (tenant_id, artifact_id, playground_id)
                 ON DELETE CASCADE
         ) STRICT;
         PRAGMA user_version = 2;",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
}
