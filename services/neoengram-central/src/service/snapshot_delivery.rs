//! SnapshotDelivery application service.
//!
//! A delivery is deliberately separate from the Snapshot catalog row. This module owns request
//! validation and durable catalog transitions; the Agent materializer consumes the same immutable
//! identity later through the signed assignment protocol.

use std::collections::BTreeSet;

use crate::{
    CatalogInsertOutcome, SnapshotDeliveryListRequest, SnapshotDeliveryMutationKind,
    SnapshotDeliveryMutationRequest, SnapshotDeliveryRecord, SnapshotDeliveryRetentionRoot,
};
use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::{CommitId, ContentDigest, FileRecord, ObjectId};
use neoengram_domain::protocol::{
    DeliveryGeneration, RequestId, SnapshotDeliveryId, SnapshotDeliveryMode, SnapshotDeliveryState,
    SnapshotId, TaskIntent, TaskResourceKind, TaskResourceRole, TaskScope, TaskState, TenantId,
};

use crate::{
    dto::{
        DeleteSnapshotDeliveryRequest, DeleteSnapshotDeliveryResponse,
        QuerySnapshotDeliveryListRequest, QuerySnapshotDeliveryListResponse,
        QuerySnapshotDeliveryRequest, QuerySnapshotDeliveryResponse, ResourceIssueSummary,
        RetrySnapshotDeliveryRequest, RetrySnapshotDeliveryResponse,
        SnapshotDeliveryMode as DeliveryModeBody, SnapshotDeliveryState as DeliveryStateBody,
        SnapshotDeliveryView,
    },
    error::{application_error, invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission},
};

use super::CatalogService;

fn snapshot_delivery_needs_scheduling(state: SnapshotDeliveryState) -> bool {
    matches!(
        state,
        SnapshotDeliveryState::Requested
            | SnapshotDeliveryState::Validating
            | SnapshotDeliveryState::Materializing
            | SnapshotDeliveryState::Deleting
    )
}

fn parse_tenant(value: String) -> Result<TenantId, Error> {
    TenantId::new(value).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}

fn parse_snapshot(value: String) -> Result<SnapshotId, Error> {
    SnapshotId::new(value).map_err(|error| invalid_request(format!("snapshot_id: {error}")))
}

fn parse_delivery(value: String) -> Result<SnapshotDeliveryId, Error> {
    SnapshotDeliveryId::new(value).map_err(|error| invalid_request(format!("delivery_id: {error}")))
}

fn body_mode(value: SnapshotDeliveryMode) -> DeliveryModeBody {
    match value {
        SnapshotDeliveryMode::Fuse => DeliveryModeBody::Fuse,
        SnapshotDeliveryMode::Copy => DeliveryModeBody::Copy,
        SnapshotDeliveryMode::Hardlink => DeliveryModeBody::Hardlink,
    }
}

fn body_state(value: SnapshotDeliveryState) -> DeliveryStateBody {
    match value {
        SnapshotDeliveryState::Requested => DeliveryStateBody::Requested,
        SnapshotDeliveryState::Validating => DeliveryStateBody::Validating,
        SnapshotDeliveryState::Materializing => DeliveryStateBody::Materializing,
        SnapshotDeliveryState::Ready => DeliveryStateBody::Ready,
        SnapshotDeliveryState::Failed => DeliveryStateBody::Failed,
        SnapshotDeliveryState::Deleting => DeliveryStateBody::Deleting,
        SnapshotDeliveryState::Deleted => DeliveryStateBody::Deleted,
    }
}

fn issue(code: &str, message: impl Into<String>, retryable: bool) -> ResourceIssueSummary {
    ResourceIssueSummary {
        code: code.to_owned(),
        message: message.into(),
        retryable,
        occurred_at_unix_ms: None,
    }
}

fn resource_not_found(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::NotFound,
        "resource_not_found",
        "RESOURCE_NOT_FOUND",
        format!("{resource} not found"),
        false,
    )
}

fn view(record: &SnapshotDeliveryRecord) -> SnapshotDeliveryView {
    SnapshotDeliveryView {
        delivery_id: record.delivery_id.to_string(),
        snapshot_id: record.snapshot_id.to_string(),
        commit_id: record.commit_id.to_string(),
        storage_volume_id: record.storage_volume_id.to_string(),
        mode: body_mode(record.mode),
        target_relative_root: record.target_relative_root.to_string(),
        state: body_state(record.state),
        source_index_digest: record.source_index_digest.to_string(),
        delivery_generation: record.delivery_generation.to_string(),
        file_count: record.file_count.to_string(),
        size_bytes: record.size_bytes.to_string(),
        object_set_digest: record.object_set_digest.to_string(),
        resource_version: record.resource_version.to_string(),
        issue: record.issue_code.as_ref().map(|code| {
            issue(
                code,
                record.issue_message.clone().unwrap_or_default(),
                record.issue_retryable,
            )
        }),
        created_at_unix_ms: record.created_at_unix_ms.to_string(),
        updated_at_unix_ms: record.updated_at_unix_ms.to_string(),
    }
}

pub(crate) fn deterministic_delivery_id(
    tenant_id: &TenantId,
    snapshot_id: &SnapshotId,
    mode: SnapshotDeliveryMode,
    request_id: &str,
) -> Result<SnapshotDeliveryId, Error> {
    let mode = match mode {
        SnapshotDeliveryMode::Fuse => "fuse",
        SnapshotDeliveryMode::Copy => "copy",
        SnapshotDeliveryMode::Hardlink => "hardlink",
    };
    let digest =
        blake3::hash(format!("{tenant_id}\0{snapshot_id}\0{mode}\0{request_id}").as_bytes());
    SnapshotDeliveryId::new(format!("delivery-{}", &digest.to_hex()[..32]))
        .map_err(|error| invalid_request(format!("delivery ID: {error}")))
}

fn delivery_mutation_digest(
    kind: SnapshotDeliveryMutationKind,
    tenant_id: &TenantId,
    delivery_id: &SnapshotDeliveryId,
) -> ContentDigest {
    let operation = match kind {
        SnapshotDeliveryMutationKind::Retry => b"retry".as_slice(),
        SnapshotDeliveryMutationKind::Delete => b"delete".as_slice(),
    };
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"neoengram-snapshot-delivery-mutation-v1\0");
    hasher.update(operation);
    hasher.update(b"\0");
    hasher.update(tenant_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(delivery_id.as_str().as_bytes());
    ContentDigest::from_bytes(*hasher.finalize().as_bytes())
}

fn mutation_id_reused() -> Error {
    application_error(
        ErrorCategory::Conflict,
        "snapshot_delivery_request_id_reused",
        "SNAPSHOT_DELIVERY_REQUEST_ID_REUSED",
        "request_id is already bound to a different SnapshotDelivery mutation",
        false,
    )
}

impl CatalogService {
    /// A failed materialization moves its Snapshot to `Abnormal`. Retrying the same immutable
    /// Delivery starts a new materialization attempt, so the aggregate must return to `Creating`
    /// before the coordinator dispatches it. This is deliberately idempotent: a lost response or
    /// a replay after the Delivery CAS may find the Snapshot already restored.
    async fn restore_snapshot_for_retry(
        &self,
        delivery: &SnapshotDeliveryRecord,
    ) -> Result<(), Error> {
        // Only a successfully applied retry (Requested state) may reopen the Snapshot. Keeping
        // this guard here prevents a failed CAS from leaving `Snapshot=Creating` while the
        // Delivery is still Failed.
        if delivery.state != SnapshotDeliveryState::Requested {
            return Ok(());
        }
        let snapshot = self
            .repository
            .get_snapshot_for_lifecycle(&delivery.tenant_id, &delivery.snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        if !snapshot.lifecycle.is_active() {
            return Err(application_error(
                ErrorCategory::Conflict,
                "snapshot_not_active",
                "SNAPSHOT_NOT_ACTIVE",
                "the Snapshot is not active and cannot be retried",
                false,
            ));
        }
        if snapshot.delivery_id != delivery.delivery_id
            || snapshot.commit_id != delivery.commit_id
            || snapshot.storage_volume_id != delivery.storage_volume_id
            || snapshot.delivery_mode != delivery.mode
        {
            return Err(application_error(
                ErrorCategory::Internal,
                "snapshot_delivery_binding_corrupt",
                "INTERNAL",
                "the Snapshot and Delivery immutable identities do not match",
                false,
            ));
        }
        if snapshot.state == crate::SnapshotState::Abnormal {
            self.repository
                .transition_snapshot_state(
                    &snapshot.tenant_id,
                    &snapshot.snapshot_id,
                    crate::SnapshotState::Abnormal,
                    crate::SnapshotState::Creating,
                    self.clock.now(),
                )
                .await
                .map_err(map_central_error)?;
        }
        Ok(())
    }

    pub(crate) async fn best_effort_schedule_snapshot_delivery(
        &self,
        delivery: &SnapshotDeliveryRecord,
    ) {
        if !snapshot_delivery_needs_scheduling(delivery.state) {
            return;
        }
        let Some(coordinator) = &self.coordinator else {
            return;
        };
        if let Err(error) = coordinator.ensure_snapshot_delivery(delivery).await {
            tracing::warn!(
                tenant_id = %delivery.tenant_id,
                snapshot_id = %delivery.snapshot_id,
                delivery_id = %delivery.delivery_id,
                %error,
                "SnapshotDelivery scheduling is pending; a later idempotent replay will retry"
            );
        }
    }

    pub(crate) async fn hardlink_retention_roots(
        &self,
        tenant_id: &TenantId,
        artifact_id: &neoengram_domain::protocol::ArtifactId,
        delivery_id: &SnapshotDeliveryId,
        records: &[FileRecord],
    ) -> Result<Vec<SnapshotDeliveryRetentionRoot>, Error> {
        let mut object_ids = BTreeSet::<ObjectId>::new();
        for record in records {
            if record.total_size == 0 {
                continue;
            }
            let manifest = self
                .indexes
                .manifest(tenant_id, artifact_id, record.manifest_id)
                .await
                .map_err(map_central_error)?
                .ok_or_else(|| {
                    application_error(
                        ErrorCategory::Internal,
                        "snapshot_delivery_manifest_missing",
                        "SNAPSHOT_DELIVERY_MANIFEST_MISSING",
                        "the immutable Manifest required by Hardlink delivery is missing",
                        false,
                    )
                })?;
            for chunk in manifest.chunks {
                object_ids.insert(chunk.object_id);
            }
        }
        Ok(object_ids
            .into_iter()
            .map(|object_id| SnapshotDeliveryRetentionRoot {
                tenant_id: tenant_id.clone(),
                delivery_id: delivery_id.clone(),
                object_id,
            })
            .collect())
    }
}

impl CatalogService {
    pub async fn query_snapshot_delivery(
        &self,
        identity: &AuthenticatedIdentity,
        request: QuerySnapshotDeliveryRequest,
    ) -> Result<QuerySnapshotDeliveryResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotRead, &tenant_id)
            .await?;
        let delivery_id = parse_delivery(request.delivery_id)?;
        let delivery = self
            .repository
            .get_snapshot_delivery(&tenant_id, &delivery_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot delivery"))?;
        Ok(QuerySnapshotDeliveryResponse {
            delivery: view(&delivery),
        })
    }

    pub async fn list_snapshot_deliveries(
        &self,
        identity: &AuthenticatedIdentity,
        request: QuerySnapshotDeliveryListRequest,
    ) -> Result<QuerySnapshotDeliveryListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotRead, &tenant_id)
            .await?;
        let snapshot_id = parse_snapshot(request.snapshot_id)?;
        let items = self
            .repository
            .list_snapshot_deliveries(&SnapshotDeliveryListRequest {
                tenant_id,
                snapshot_id: Some(snapshot_id),
                mode: None,
                state: None,
                limit: request.page_size.unwrap_or(50).min(100),
            })
            .await
            .map_err(map_central_error)?;
        Ok(QuerySnapshotDeliveryListResponse {
            items: items.iter().map(view).collect(),
            next_cursor: None,
        })
    }

    pub async fn retry_snapshot_delivery(
        &self,
        identity: &AuthenticatedIdentity,
        request: RetrySnapshotDeliveryRequest,
    ) -> Result<RetrySnapshotDeliveryResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let delivery_id = parse_delivery(request.delivery_id)?;
        let request_id = RequestId::new(request.request_id)
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let kind = SnapshotDeliveryMutationKind::Retry;
        let request_digest = delivery_mutation_digest(kind, &tenant_id, &delivery_id);
        let current = self
            .repository
            .get_snapshot_delivery(&tenant_id, &delivery_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot delivery"))?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskIntent::SnapshotCreate,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: None,
                    artifact_id: None,
                    object_namespace_id: None,
                    commit_id: Some(CommitId::from_digest(current.commit_id)),
                    workspace_id: None,

                    snapshot_id: Some(current.snapshot_id.clone()),
                    storage_volume_id: Some(current.storage_volume_id.clone()),
                },
                request_id.clone(),
                &task_request,
                identity,
                Some("snapshot_delivery"),
                Some(delivery_id.as_str()),
            )
            .await?;
        self.operation_result(
            &task,
            identity,
            self.link_operation_resource(
                &task,
                TaskResourceKind::SnapshotDelivery,
                delivery_id.to_string(),
                TaskResourceRole::Primary,
            )
            .await,
        )
        .await?;
        let existing_mutation = self
            .operation_result(
                &task,
                identity,
                self.repository
                    .get_snapshot_delivery_mutation(&tenant_id, &request_id)
                    .await
                    .map_err(map_central_error),
            )
            .await?;
        if let Some(receipt) = existing_mutation {
            if receipt.delivery_id != delivery_id
                || receipt.kind != kind
                || receipt.request_digest != request_digest
            {
                let error = mutation_id_reused();
                self.fail_operation_task(&task, identity, &error).await;
                return Err(error);
            }
            self.operation_result(
                &task,
                identity,
                self.restore_snapshot_for_retry(&receipt.delivery).await,
            )
            .await?;
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
            let previous_task = task.clone();
            let task = self
                .operation_result(
                    &previous_task,
                    identity,
                    self.transition_operation_task(
                        task,
                        TaskState::Running,
                        identity,
                        Some("Snapshot Delivery retry scheduled".to_owned()),
                    )
                    .await,
                )
                .await?;
            return Ok(RetrySnapshotDeliveryResponse {
                delivery: view(&receipt.delivery),
                request_replayed: !task.as_ref().is_some_and(|value| value.execution_reused),
                execution_reused: task.as_ref().is_some_and(|value| value.execution_reused),
                task,
            });
        }
        if !matches!(
            current.state,
            SnapshotDeliveryState::Failed | SnapshotDeliveryState::Requested
        ) {
            let error = application_error(
                ErrorCategory::Conflict,
                "delivery_not_retryable",
                "DELIVERY_NOT_RETRYABLE",
                "only a failed or requested delivery can be retried",
                false,
            );
            self.fail_operation_task(&task, identity, &error).await;
            return Err(error);
        }
        if current.state == SnapshotDeliveryState::Failed && !current.issue_retryable {
            let error = application_error(
                ErrorCategory::Conflict,
                "delivery_not_retryable",
                "DELIVERY_NOT_RETRYABLE",
                "the Delivery failed with a deterministic error and cannot be retried",
                false,
            );
            self.fail_operation_task(&task, identity, &error).await;
            return Err(error);
        }
        let desired =
            if current.state == SnapshotDeliveryState::Requested && current.issue_code.is_none() {
                current.clone()
            } else {
                let next_generation = self
                    .operation_result(
                        &task,
                        identity,
                        current
                            .delivery_generation
                            .get()
                            .checked_add(1)
                            .ok_or_else(|| {
                                application_error(
                                    ErrorCategory::Conflict,
                                    "delivery_generation_exhausted",
                                    "DELIVERY_GENERATION_EXHAUSTED",
                                    "SnapshotDelivery retry generation is exhausted",
                                    false,
                                )
                            }),
                    )
                    .await?;
                let mut next = current.clone();
                next.state = if current.issue_code.as_deref() == Some("DELIVERY_DELETE_FAILED") {
                    SnapshotDeliveryState::Deleting
                } else {
                    SnapshotDeliveryState::Requested
                };
                next.delivery_generation = DeliveryGeneration::new(next_generation);
                next.issue_code = None;
                next.issue_message = None;
                next.issue_retryable = false;
                next.updated_at_unix_ms = self.clock.now();
                next
            };
        let outcome = self
            .operation_result(
                &task,
                identity,
                self.repository
                    .apply_snapshot_delivery_mutation_idempotent(SnapshotDeliveryMutationRequest {
                        tenant_id: tenant_id.clone(),
                        request_id,
                        delivery_id,
                        kind,
                        request_digest,
                        expected_resource_version: current.resource_version,
                        desired_delivery: desired,
                    })
                    .await
                    .map_err(map_central_error),
            )
            .await?;
        let (receipt, replayed) = match outcome {
            CatalogInsertOutcome::Inserted(receipt) => (receipt, false),
            CatalogInsertOutcome::Existing(receipt) => (receipt, true),
        };
        self.operation_result(
            &task,
            identity,
            self.restore_snapshot_for_retry(&receipt.delivery).await,
        )
        .await?;
        if replayed {
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
        } else if snapshot_delivery_needs_scheduling(receipt.delivery.state) {
            if let Some(coordinator) = &self.coordinator {
                self.operation_result(
                    &task,
                    identity,
                    coordinator
                        .ensure_snapshot_delivery(&receipt.delivery)
                        .await
                        .map_err(map_central_error),
                )
                .await?;
            }
        }
        let previous_task = task.clone();
        let task = self
            .operation_result(
                &previous_task,
                identity,
                self.transition_operation_task(
                    task,
                    TaskState::Running,
                    identity,
                    Some("Snapshot Delivery retry scheduled".to_owned()),
                )
                .await,
            )
            .await?;
        Ok(RetrySnapshotDeliveryResponse {
            delivery: view(&receipt.delivery),
            request_replayed: (replayed || task_replayed)
                && !task.as_ref().is_some_and(|value| value.execution_reused),
            execution_reused: task.as_ref().is_some_and(|value| value.execution_reused),
            task,
        })
    }

    pub async fn delete_snapshot_delivery(
        &self,
        identity: &AuthenticatedIdentity,
        request: DeleteSnapshotDeliveryRequest,
    ) -> Result<DeleteSnapshotDeliveryResponse, Error> {
        let task_request = request.clone();
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let delivery_id = parse_delivery(request.delivery_id)?;
        let request_id = RequestId::new(request.request_id)
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let kind = SnapshotDeliveryMutationKind::Delete;
        let request_digest = delivery_mutation_digest(kind, &tenant_id, &delivery_id);
        let current = self
            .repository
            .get_snapshot_delivery(&tenant_id, &delivery_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot delivery"))?;
        let (task, task_replayed) = self
            .begin_operation_task(
                TaskIntent::SnapshotDelete,
                TaskScope {
                    tenant_id: tenant_id.clone(),
                    project_id: None,
                    artifact_id: None,
                    object_namespace_id: None,
                    commit_id: Some(CommitId::from_digest(current.commit_id)),
                    workspace_id: None,

                    snapshot_id: Some(current.snapshot_id.clone()),
                    storage_volume_id: Some(current.storage_volume_id.clone()),
                },
                request_id.clone(),
                &task_request,
                identity,
                Some("snapshot_delivery"),
                Some(delivery_id.as_str()),
            )
            .await?;
        self.operation_result(
            &task,
            identity,
            self.link_operation_resource(
                &task,
                TaskResourceKind::SnapshotDelivery,
                delivery_id.to_string(),
                TaskResourceRole::Primary,
            )
            .await,
        )
        .await?;
        let existing_mutation = self
            .operation_result(
                &task,
                identity,
                self.repository
                    .get_snapshot_delivery_mutation(&tenant_id, &request_id)
                    .await
                    .map_err(map_central_error),
            )
            .await?;
        if let Some(receipt) = existing_mutation {
            if receipt.delivery_id != delivery_id
                || receipt.kind != kind
                || receipt.request_digest != request_digest
            {
                let error = mutation_id_reused();
                self.fail_operation_task(&task, identity, &error).await;
                return Err(error);
            }
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
            let previous_task = task.clone();
            let task = self
                .operation_result(
                    &previous_task,
                    identity,
                    self.transition_operation_task(
                        task,
                        TaskState::Running,
                        identity,
                        Some("Snapshot Delivery deletion scheduled".to_owned()),
                    )
                    .await,
                )
                .await?;
            return Ok(DeleteSnapshotDeliveryResponse {
                delivery: view(&receipt.delivery),
                request_replayed: !task.as_ref().is_some_and(|value| value.execution_reused),
                execution_reused: task.as_ref().is_some_and(|value| value.execution_reused),
                task,
            });
        }
        let desired = if matches!(
            current.state,
            SnapshotDeliveryState::Deleted | SnapshotDeliveryState::Deleting
        ) {
            current.clone()
        } else {
            let generation = self
                .operation_result(
                    &task,
                    identity,
                    current
                        .delivery_generation
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| {
                            application_error(
                                ErrorCategory::Conflict,
                                "delivery_generation_exhausted",
                                "DELIVERY_GENERATION_EXHAUSTED",
                                "SnapshotDelivery delete generation is exhausted",
                                false,
                            )
                        }),
                )
                .await?;
            let mut next = current.clone();
            next.state = SnapshotDeliveryState::Deleting;
            next.delivery_generation = DeliveryGeneration::new(generation);
            next.issue_code = None;
            next.issue_message = None;
            next.issue_retryable = false;
            next.updated_at_unix_ms = self.clock.now();
            next
        };
        let outcome = self
            .operation_result(
                &task,
                identity,
                self.repository
                    .apply_snapshot_delivery_mutation_idempotent(SnapshotDeliveryMutationRequest {
                        tenant_id: tenant_id.clone(),
                        request_id,
                        delivery_id,
                        kind,
                        request_digest,
                        expected_resource_version: current.resource_version,
                        desired_delivery: desired,
                    })
                    .await
                    .map_err(map_central_error),
            )
            .await?;
        let (receipt, replayed) = match outcome {
            CatalogInsertOutcome::Inserted(receipt) => (receipt, false),
            CatalogInsertOutcome::Existing(receipt) => (receipt, true),
        };
        if replayed {
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
        } else if snapshot_delivery_needs_scheduling(receipt.delivery.state) {
            if let Some(coordinator) = &self.coordinator {
                self.operation_result(
                    &task,
                    identity,
                    coordinator
                        .ensure_snapshot_delivery(&receipt.delivery)
                        .await
                        .map_err(map_central_error),
                )
                .await?;
            }
        }
        let previous_task = task.clone();
        let task = self
            .operation_result(
                &previous_task,
                identity,
                self.transition_operation_task(
                    task,
                    TaskState::Running,
                    identity,
                    Some("Snapshot Delivery deletion scheduled".to_owned()),
                )
                .await,
            )
            .await?;
        Ok(DeleteSnapshotDeliveryResponse {
            delivery: view(&receipt.delivery),
            request_replayed: (replayed || task_replayed)
                && !task.as_ref().is_some_and(|value| value.execution_reused),
            execution_reused: task.as_ref().is_some_and(|value| value.execution_reused),
            task,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_incomplete_snapshot_deliveries_need_scheduling() {
        for state in [
            SnapshotDeliveryState::Requested,
            SnapshotDeliveryState::Validating,
            SnapshotDeliveryState::Materializing,
            SnapshotDeliveryState::Deleting,
        ] {
            assert!(snapshot_delivery_needs_scheduling(state));
        }
        for state in [
            SnapshotDeliveryState::Ready,
            SnapshotDeliveryState::Failed,
            SnapshotDeliveryState::Deleted,
        ] {
            assert!(!snapshot_delivery_needs_scheduling(state));
        }
    }
}
