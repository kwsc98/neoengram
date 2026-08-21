//! SnapshotDelivery application service.
//!
//! A delivery is deliberately separate from the Snapshot catalog row. This module owns request
//! validation and durable catalog transitions; the Agent materializer consumes the same immutable
//! identity later through the signed assignment protocol.

use std::collections::BTreeSet;

use crate::{
    CatalogInsertOutcome, SnapshotDeliveryInsertOutcome, SnapshotDeliveryInsertRequest,
    SnapshotDeliveryListRequest, SnapshotDeliveryMutationKind, SnapshotDeliveryMutationRequest,
    SnapshotDeliveryRecord, SnapshotDeliveryRetentionRoot, SnapshotState, StorageVolumeState,
};
use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::{ContentDigest, FileRecord, ObjectId};
use neoengram_domain::protocol::{
    BackendId, CommitDataLayout, DeliveryGeneration, SnapshotDeliveryId, SnapshotDeliveryMode,
    SnapshotDeliveryOperation, SnapshotDeliveryState, SnapshotId, StorageVolumeId, TenantId,
};

use crate::{
    dto::{
        CreateSnapshotDeliveryRequest, CreateSnapshotDeliveryResponse,
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

const HARDLINK_REQUIRES_WHOLE_FILE: &str = "HARDLINK_REQUIRES_WHOLE_FILE";
const HARDLINK_UNSAFE_VOLUME: &str = "HARDLINK_UNSAFE_VOLUME";
const DELIVERY_MODE_NOT_ALLOWED: &str = "DELIVERY_MODE_NOT_ALLOWED";
const WHOLE_FILE_SIZE_LIMIT_EXCEEDED: &str = "WHOLE_FILE_SIZE_LIMIT_EXCEEDED";

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

fn parse_volume(value: String) -> Result<StorageVolumeId, Error> {
    StorageVolumeId::new(value)
        .map_err(|error| invalid_request(format!("target_storage_volume_id: {error}")))
}

fn to_mode(value: DeliveryModeBody) -> SnapshotDeliveryMode {
    match value {
        DeliveryModeBody::Fuse => SnapshotDeliveryMode::Fuse,
        DeliveryModeBody::Copy => SnapshotDeliveryMode::Copy,
        DeliveryModeBody::Hardlink => SnapshotDeliveryMode::Hardlink,
    }
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

fn deterministic_delivery_id(
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
    async fn best_effort_schedule_snapshot_delivery(&self, delivery: &SnapshotDeliveryRecord) {
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

    async fn hardlink_retention_roots(
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
    pub async fn create_snapshot_delivery(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateSnapshotDeliveryRequest,
    ) -> Result<CreateSnapshotDeliveryResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let snapshot_id = parse_snapshot(request.snapshot_id)?;
        let target_storage_volume_id = parse_volume(request.target_storage_volume_id)?;
        let mode = to_mode(request.mode);
        let request_id = neoengram_domain::protocol::RequestId::new(request.request_id)
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        if let Some(existing) = self
            .repository
            .get_snapshot_delivery_by_create_request_id(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            if existing.snapshot_id != snapshot_id
                || existing.mode != mode
                || existing.storage_volume_id != target_storage_volume_id
            {
                return Err(mutation_id_reused());
            }
            self.best_effort_schedule_snapshot_delivery(&existing).await;
            return Ok(CreateSnapshotDeliveryResponse {
                delivery: view(&existing),
                replayed: true,
            });
        }
        let snapshot = self
            .repository
            .get_snapshot(&tenant_id, &snapshot_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot"))?;
        if !snapshot.lifecycle.is_active() {
            return Err(application_error(
                ErrorCategory::Conflict,
                "snapshot_lifecycle_fenced",
                "SNAPSHOT_LIFECYCLE_FENCED",
                "the Snapshot is not active",
                false,
            ));
        }
        if snapshot.state != SnapshotState::Ready {
            return Err(application_error(
                ErrorCategory::Conflict,
                "snapshot_not_ready",
                "SNAPSHOT_NOT_READY",
                "only a Ready Snapshot can create a delivery",
                false,
            ));
        }
        let precommits = self.precommits.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "commit_authority_unavailable",
                "COMMIT_AUTHORITY_UNAVAILABLE",
                "Commit authority is unavailable",
                true,
            )
        })?;
        let commit = precommits
            .get_commit(
                &tenant_id,
                &snapshot.project_id,
                &snapshot.artifact_id,
                neoengram_domain::core::CommitId::from_digest(snapshot.commit_id),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("commit"))?;
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &target_storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("storage volume"))?;
        if !volume.lifecycle.is_active() {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_lifecycle_fenced",
                "STORAGE_VOLUME_LIFECYCLE_FENCED",
                "the target StorageVolume is not active",
                false,
            ));
        }
        if volume.state != StorageVolumeState::Ready {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "the target StorageVolume is not ready",
                true,
            ));
        }
        // Delivery is visible only after the target Volume has a published complete object set.
        // Replication and Delivery are separate authority operations: a queued or partial target
        // must never be treated as readable merely because the Snapshot exists.
        let placement = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "Placement authority is unavailable",
                true,
            )
        })?;
        let backend_id = BackendId::new(target_storage_volume_id.to_string())
            .map_err(|error| invalid_request(format!("target_storage_volume_id: {error}")))?;
        let published = placement
            .get_placement_set(&tenant_id, &snapshot.commit_id, &backend_id)
            .await
            .map_err(map_central_error)?
            .is_some_and(|set| {
                set.published() && set.storage_volume_id.as_ref() == Some(&target_storage_volume_id)
            });
        if !published {
            return Err(application_error(
                ErrorCategory::Conflict,
                "snapshot_target_volume_has_no_commit_data",
                "SNAPSHOT_TARGET_VOLUME_HAS_NO_COMMIT_DATA",
                "the target StorageVolume does not hold this Commit's immutable objects; replicate the Commit first",
                false,
            ));
        }
        if !volume.allowed_delivery_modes.contains(&mode) {
            return Err(application_error(
                ErrorCategory::Conflict,
                "delivery_mode_not_allowed",
                DELIVERY_MODE_NOT_ALLOWED,
                "the requested delivery mode is disabled by the StorageVolume policy",
                false,
            ));
        }
        if mode == SnapshotDeliveryMode::Hardlink
            && commit.data_layout != CommitDataLayout::WholeFile
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "hardlink_requires_whole_file",
                HARDLINK_REQUIRES_WHOLE_FILE,
                "Hardlink delivery requires a WholeFile Commit",
                false,
            ));
        }
        if mode == SnapshotDeliveryMode::Hardlink
            && matches!(
                volume.hardlink_policy,
                neoengram_domain::protocol::HardlinkPolicy::Disabled
            )
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "hardlink_unsafe_volume",
                HARDLINK_UNSAFE_VOLUME,
                "StorageVolume has not enabled a sealed hardlink policy",
                false,
            ));
        }
        let delivery_id =
            deterministic_delivery_id(&tenant_id, &snapshot_id, mode, request_id.as_str())?;
        let target_relative_root = SnapshotDeliveryOperation::canonical_target_relative_root(
            &snapshot.project_id,
            &snapshot.artifact_id,
            &snapshot_id,
            &delivery_id,
        )
        .map_err(|error| invalid_request(error.to_string()))?;
        let now = self.clock.now();
        let records = &commit.records;
        let file_count =
            u64::try_from(records.len()).map_err(|_| invalid_request("file count exceeds u64"))?;
        let size_bytes = records
            .iter()
            .try_fold(0_u64, |total, record| total.checked_add(record.total_size))
            .ok_or_else(|| invalid_request("snapshot size exceeds u64"))?;
        if commit.data_layout == CommitDataLayout::WholeFile
            && records
                .iter()
                .any(|record| record.total_size > volume.max_whole_file_bytes.get())
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "whole_file_size_limit_exceeded",
                WHOLE_FILE_SIZE_LIMIT_EXCEEDED,
                "the Commit contains a file that exceeds the StorageVolume WholeFile size policy",
                false,
            ));
        }
        let coordinator = self.coordinator.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "snapshot_delivery_unavailable",
                "SNAPSHOT_DELIVERY_UNAVAILABLE",
                "SnapshotDelivery execution is not configured",
                true,
            )
        })?;
        coordinator
            .preflight_snapshot_delivery(&tenant_id, &target_storage_volume_id, mode)
            .await
            .map_err(map_central_error)?;
        let retention_roots = if mode == SnapshotDeliveryMode::Hardlink {
            self.hardlink_retention_roots(&tenant_id, &snapshot.artifact_id, &delivery_id, records)
                .await?
        } else {
            Vec::new()
        };
        let record = SnapshotDeliveryRecord {
            tenant_id: tenant_id.clone(),
            delivery_id,
            create_request_id: request_id.clone(),
            snapshot_id: snapshot_id.clone(),
            commit_id: snapshot.commit_id,
            storage_volume_id: target_storage_volume_id,
            mode,
            target_relative_root,
            state: SnapshotDeliveryState::Requested,
            source_index_digest: commit.index_version.digest,
            delivery_generation: DeliveryGeneration::new(1),
            file_count,
            size_bytes,
            // Delivery is a physical view of the Commit, so its object-set identity must be the
            // immutable Commit identity. Recomputing a second file-record digest here would let
            // Delivery and Replication disagree about which objects are required.
            object_set_digest: commit.object_set_digest,
            resource_version: 1,
            issue_code: None,
            issue_message: None,
            issue_retryable: false,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let outcome = self
            .repository
            .insert_snapshot_delivery_idempotent(SnapshotDeliveryInsertRequest {
                record,
                request_id,
                retention_roots,
            })
            .await
            .map_err(map_central_error)?;
        let (delivery, replayed) = match outcome {
            SnapshotDeliveryInsertOutcome::Inserted(record) => (record, false),
            SnapshotDeliveryInsertOutcome::Existing(record) => (record, true),
        };
        self.best_effort_schedule_snapshot_delivery(&delivery).await;
        Ok(CreateSnapshotDeliveryResponse {
            delivery: view(&delivery),
            replayed,
        })
    }

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
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let delivery_id = parse_delivery(request.delivery_id)?;
        let request_id = neoengram_domain::protocol::RequestId::new(request.request_id)
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let kind = SnapshotDeliveryMutationKind::Retry;
        let request_digest = delivery_mutation_digest(kind, &tenant_id, &delivery_id);
        if let Some(receipt) = self
            .repository
            .get_snapshot_delivery_mutation(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            if receipt.delivery_id != delivery_id
                || receipt.kind != kind
                || receipt.request_digest != request_digest
            {
                return Err(mutation_id_reused());
            }
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
            return Ok(RetrySnapshotDeliveryResponse {
                delivery: view(&receipt.delivery),
                replayed: true,
            });
        }
        let current = self
            .repository
            .get_snapshot_delivery(&tenant_id, &delivery_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot delivery"))?;
        if !matches!(
            current.state,
            SnapshotDeliveryState::Failed | SnapshotDeliveryState::Requested
        ) {
            return Err(application_error(
                ErrorCategory::Conflict,
                "delivery_not_retryable",
                "DELIVERY_NOT_RETRYABLE",
                "only a failed or requested delivery can be retried",
                false,
            ));
        }
        if current.state == SnapshotDeliveryState::Failed && !current.issue_retryable {
            return Err(application_error(
                ErrorCategory::Conflict,
                "delivery_not_retryable",
                "DELIVERY_NOT_RETRYABLE",
                "the Delivery failed with a deterministic error and cannot be retried",
                false,
            ));
        }
        let desired =
            if current.state == SnapshotDeliveryState::Requested && current.issue_code.is_none() {
                current.clone()
            } else {
                let next_generation = current
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
                    })?;
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
            .repository
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
            .map_err(map_central_error)?;
        let (receipt, replayed) = match outcome {
            CatalogInsertOutcome::Inserted(receipt) => (receipt, false),
            CatalogInsertOutcome::Existing(receipt) => (receipt, true),
        };
        if replayed {
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
        } else if snapshot_delivery_needs_scheduling(receipt.delivery.state) {
            if let Some(coordinator) = &self.coordinator {
                coordinator
                    .ensure_snapshot_delivery(&receipt.delivery)
                    .await
                    .map_err(map_central_error)?;
            }
        }
        Ok(RetrySnapshotDeliveryResponse {
            delivery: view(&receipt.delivery),
            replayed,
        })
    }

    pub async fn delete_snapshot_delivery(
        &self,
        identity: &AuthenticatedIdentity,
        request: DeleteSnapshotDeliveryRequest,
    ) -> Result<DeleteSnapshotDeliveryResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let delivery_id = parse_delivery(request.delivery_id)?;
        let request_id = neoengram_domain::protocol::RequestId::new(request.request_id)
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let kind = SnapshotDeliveryMutationKind::Delete;
        let request_digest = delivery_mutation_digest(kind, &tenant_id, &delivery_id);
        if let Some(receipt) = self
            .repository
            .get_snapshot_delivery_mutation(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            if receipt.delivery_id != delivery_id
                || receipt.kind != kind
                || receipt.request_digest != request_digest
            {
                return Err(mutation_id_reused());
            }
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
            return Ok(DeleteSnapshotDeliveryResponse {
                delivery: view(&receipt.delivery),
                replayed: true,
            });
        }
        let current = self
            .repository
            .get_snapshot_delivery(&tenant_id, &delivery_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| resource_not_found("snapshot delivery"))?;
        let desired = if matches!(
            current.state,
            SnapshotDeliveryState::Deleted | SnapshotDeliveryState::Deleting
        ) {
            current.clone()
        } else {
            let generation = current
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
                })?;
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
            .repository
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
            .map_err(map_central_error)?;
        let (receipt, replayed) = match outcome {
            CatalogInsertOutcome::Inserted(receipt) => (receipt, false),
            CatalogInsertOutcome::Existing(receipt) => (receipt, true),
        };
        if replayed {
            self.best_effort_schedule_snapshot_delivery(&receipt.delivery)
                .await;
        } else if snapshot_delivery_needs_scheduling(receipt.delivery.state) {
            if let Some(coordinator) = &self.coordinator {
                coordinator
                    .ensure_snapshot_delivery(&receipt.delivery)
                    .await
                    .map_err(map_central_error)?;
            }
        }
        Ok(DeleteSnapshotDeliveryResponse {
            delivery: view(&receipt.delivery),
            replayed,
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
