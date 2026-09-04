#![cfg(feature = "authority-sqlite")]

use std::sync::Arc;

use neoengram_central::{
    open_sqlite_authority, CentralErrorCode, InMemoryComponents, SqliteAuthorityConfig,
    TaskEventListRequest, TaskInsertOutcome, TaskListRequest, TaskRelationRecord, TaskRepository,
    TaskResourceLinkRecord,
};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    Extensions, Generation, OperationTask, PrincipalId, PrincipalKind, PrincipalRef, RequestId,
    SequenceNumber, TaskActor, TaskAttempt, TaskAttemptId, TaskEvent, TaskEventId, TaskEventKind,
    TaskId, TaskIssue, TaskKind, TaskRelation, TaskRelationKind, TaskResourceKind,
    TaskResourceLink, TaskResourceRole, TaskScope, TaskState, TenantId, UnixMillis,
};
use tempfile::TempDir;

fn actor() -> TaskActor {
    TaskActor::Principal(PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new("task-test").unwrap(),
        extensions: Extensions::new(),
    })
}

fn task(tenant: &str, task_id: &str, request_id: &str, digest_byte: u8) -> OperationTask {
    OperationTask::new(
        TaskId::new(task_id).unwrap(),
        TaskKind::CommitMaterialize,
        TaskScope::new(TenantId::new(tenant).unwrap()),
        RequestId::new(request_id).unwrap(),
        ContentDigest::from_bytes([digest_byte; 32]),
        actor(),
        UnixMillis::new(1),
        UnixMillis::new(10_000),
    )
}

fn created_event(task: &OperationTask) -> TaskEvent {
    TaskEvent {
        event_id: TaskEventId::new(format!("{}-event-1", task.task_id)).unwrap(),
        task_id: task.task_id.clone(),
        sequence: SequenceNumber::new(1),
        attempt: task.attempt,
        kind: TaskEventKind::Created,
        state: task.state,
        from_state: None,
        to_state: None,
        actor: task.actor.clone(),
        message: Some("created".to_owned()),
        issue: None,
        progress: Some(task.progress_summary),
        occurred_at_unix_ms: task.created_at_unix_ms,
        resource_version: task.resource_version,
    }
}

async fn insert_initial(
    repository: &Arc<dyn TaskRepository>,
    task: OperationTask,
) -> OperationTask {
    let attempt = TaskAttempt::new(
        task.task_id.clone(),
        TaskAttemptId::new(format!("{}-attempt-1", task.task_id)).unwrap(),
        task.attempt,
        task.created_at_unix_ms,
    );
    let event = created_event(&task);
    match repository
        .insert_with_history(task.clone(), Some(attempt), Some(event))
        .await
        .unwrap()
    {
        TaskInsertOutcome::Inserted(value) => assert_eq!(value, task),
        TaskInsertOutcome::Existing(_) => panic!("first task insert unexpectedly replayed"),
    }
    task
}

async fn run_repository_contract(repository: Arc<dyn TaskRepository>) {
    let first = insert_initial(&repository, task("tenant-a", "task-a", "request-a", 0x11)).await;

    // An exact request replay is idempotent, while changing the immutable request digest is not.
    assert!(matches!(
        repository
            .insert_with_history(first.clone(), None, None)
            .await
            .unwrap(),
        TaskInsertOutcome::Existing(_)
    ));
    let mut conflict = first.clone();
    conflict.request_digest = ContentDigest::from_bytes([0x22; 32]);
    let error = repository.insert(conflict).await.unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ConcurrentUpdate);

    // Task, current Attempt, and event change atomically under one CAS in both backends.
    let running = repository
        .transition(
            &first.tenant_id,
            &first.task_id,
            first.resource_version,
            TaskState::Running,
            actor(),
            None,
            Some("execution started".to_owned()),
            UnixMillis::new(2),
        )
        .await
        .unwrap()
        .task;
    assert_eq!(running.state, TaskState::Running);
    assert_eq!(
        repository
            .attempts(&first.tenant_id, &first.task_id)
            .await
            .unwrap()[0]
            .state,
        TaskState::Running
    );
    assert!(repository
        .transition(
            &first.tenant_id,
            &first.task_id,
            first.resource_version,
            TaskState::Verifying,
            actor(),
            None,
            None,
            UnixMillis::new(2),
        )
        .await
        .is_err());

    let second_event = repository
        .list_events(&TaskEventListRequest {
            tenant_id: first.tenant_id.clone(),
            task_id: first.task_id.clone(),
            after_sequence: Some(SequenceNumber::new(1)),
            page_size: 10,
        })
        .await
        .unwrap()
        .items
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        repository
            .append_event(&first.tenant_id, second_event.clone())
            .await
            .unwrap(),
        second_event
    );
    let wrong_sequence = TaskEvent {
        event_id: TaskEventId::new("task-a-event-4").unwrap(),
        sequence: SequenceNumber::new(4),
        ..second_event.clone()
    };
    assert_eq!(
        repository
            .append_event(&first.tenant_id, wrong_sequence)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ConcurrentUpdate
    );
    let events = repository
        .list_events(&TaskEventListRequest {
            tenant_id: first.tenant_id.clone(),
            task_id: first.task_id.clone(),
            after_sequence: None,
            page_size: 10,
        })
        .await
        .unwrap();
    assert_eq!(events.items.len(), 2);
    assert_eq!(events.items[0].sequence, SequenceNumber::new(1));
    assert_eq!(events.items[1].sequence, SequenceNumber::new(2));

    // Retrying retains task identity, creates a new attempt, and appends one event atomically.
    let failed = repository
        .transition(
            &first.tenant_id,
            &first.task_id,
            running.resource_version,
            TaskState::Failed,
            actor(),
            Some(TaskIssue {
                code: "source_unavailable".to_owned(),
                message: "source disconnected".to_owned(),
                retryable: true,
                detail: None,
            }),
            Some("source disconnected".to_owned()),
            UnixMillis::new(3),
        )
        .await
        .unwrap()
        .task;
    let retry = repository
        .retry(
            &first.tenant_id,
            &first.task_id,
            failed.resource_version,
            actor(),
            UnixMillis::new(4),
        )
        .await
        .unwrap();
    assert!(!retry.replayed);
    assert_eq!(retry.task.task_id, first.task_id);
    assert_eq!(retry.task.attempt, Generation::new(2));
    assert_eq!(retry.task.state, TaskState::Queued);
    let attempts = repository
        .attempts(&first.tenant_id, &first.task_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].state, TaskState::Failed);
    assert_eq!(attempts[1].state, TaskState::Queued);
    assert_eq!(
        repository
            .list_events(&TaskEventListRequest {
                tenant_id: first.tenant_id.clone(),
                task_id: first.task_id.clone(),
                after_sequence: None,
                page_size: 10,
            })
            .await
            .unwrap()
            .items
            .len(),
        4
    );

    // Cancel is idempotent and its second invocation does not append another event.
    let cancel = repository
        .cancel(
            &first.tenant_id,
            &first.task_id,
            Some(retry.task.resource_version),
            actor(),
            UnixMillis::new(5),
        )
        .await
        .unwrap();
    assert_eq!(cancel.task.state, TaskState::Cancelled);
    let replay = repository
        .cancel(
            &first.tenant_id,
            &first.task_id,
            None,
            actor(),
            UnixMillis::new(6),
        )
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        repository
            .attempts(&first.tenant_id, &first.task_id)
            .await
            .unwrap()[1]
            .state,
        TaskState::Cancelled
    );
    assert_eq!(
        repository
            .list_events(&TaskEventListRequest {
                tenant_id: first.tenant_id.clone(),
                task_id: first.task_id.clone(),
                after_sequence: None,
                page_size: 10,
            })
            .await
            .unwrap()
            .items
            .len(),
        5
    );

    // Resource links are idempotent and relation insertion rejects cycles.
    let link = TaskResourceLinkRecord {
        tenant_id: first.tenant_id.clone(),
        link: TaskResourceLink::new(
            first.task_id.clone(),
            TaskResourceKind::StorageVolume,
            "volume-a",
            TaskResourceRole::Target,
        ),
    };
    assert!(!repository.link_resource(link.clone()).await.unwrap());
    assert!(repository.link_resource(link).await.unwrap());
    assert_eq!(
        repository
            .list_events(&TaskEventListRequest {
                tenant_id: first.tenant_id.clone(),
                task_id: first.task_id.clone(),
                after_sequence: None,
                page_size: 10,
            })
            .await
            .unwrap()
            .items
            .len(),
        6
    );

    let second = insert_initial(&repository, task("tenant-a", "task-b", "request-b", 0x33)).await;
    let third = insert_initial(&repository, task("tenant-a", "task-c", "request-c", 0x44)).await;
    assert!(!repository
        .add_relation(TaskRelationRecord {
            tenant_id: first.tenant_id.clone(),
            relation: TaskRelation {
                task_id: second.task_id.clone(),
                related_task_id: third.task_id.clone(),
                relation: TaskRelationKind::CausedBy,
            },
        })
        .await
        .unwrap());
    assert!(!repository
        .add_relation(TaskRelationRecord {
            tenant_id: first.tenant_id.clone(),
            relation: TaskRelation {
                task_id: third.task_id.clone(),
                related_task_id: first.task_id.clone(),
                relation: TaskRelationKind::TriggeredBy,
            },
        })
        .await
        .unwrap());
    let cycle = repository
        .add_relation(TaskRelationRecord {
            tenant_id: first.tenant_id.clone(),
            relation: TaskRelation {
                task_id: first.task_id.clone(),
                related_task_id: second.task_id.clone(),
                relation: TaskRelationKind::Parent,
            },
        })
        .await
        .unwrap_err();
    assert_eq!(cycle.code(), CentralErrorCode::ProtocolInvalid);

    // Tenant and filter boundaries never leak another tenant's task.
    let other = insert_initial(&repository, task("tenant-b", "task-z", "request-z", 0x55)).await;
    assert!(repository
        .get(&TenantId::new("tenant-b").unwrap(), &first.task_id)
        .await
        .unwrap()
        .is_none());
    let page = repository
        .list(&TaskListRequest::for_tenant(first.tenant_id.clone()))
        .await
        .unwrap();
    assert!(page
        .items
        .iter()
        .all(|item| item.tenant_id == first.tenant_id));
    assert!(!page.items.iter().any(|item| item.task_id == other.task_id));
}

#[tokio::test]
async fn in_memory_task_repository_contract() {
    eprintln!("starting in-memory task contract");
    let components = InMemoryComponents::new(100);
    eprintln!("created in-memory components");
    run_repository_contract(components.tasks.clone()).await;
    eprintln!("finished in-memory task contract");
}

#[tokio::test]
async fn sqlite_task_repository_contract_and_reopen() {
    let directory = TempDir::new().unwrap();
    {
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        run_repository_contract(authority.authority_store().tasks().unwrap()).await;
        authority.close().await;
    }
    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = reopened.authority_store().tasks().unwrap();
    let task = repository
        .get(
            &TenantId::new("tenant-a").unwrap(),
            &TaskId::new("task-a").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.state, TaskState::Cancelled);
    assert_eq!(
        repository
            .attempts(&task.tenant_id, &task.task_id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        repository
            .list_events(&TaskEventListRequest {
                tenant_id: task.tenant_id.clone(),
                task_id: task.task_id.clone(),
                after_sequence: None,
                page_size: 10,
            })
            .await
            .unwrap()
            .items
            .len(),
        6
    );
    reopened.close().await;
}
