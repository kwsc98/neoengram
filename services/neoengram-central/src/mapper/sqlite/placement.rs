use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
use neoengram_domain::protocol::materialization::{
    MaterializationBatch, MaterializationBatchState, MaterializationJob, MaterializationJobKey,
    MaterializationJobState, MaterializationLeaseState, MaterializationObject,
    MaterializationObjectReceipt, MaterializationObjectState, ObjectPlacement as ObjectPlacementV2,
    ObjectReadLease, ObjectRef, PlacementHealthObservation, PlacementHealthState, StagingLease,
    VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    object_read_lease_id, staging_lease_id, AgentId, ArchiveId, ArtifactId, BackendId,
    CommitObject, CommitObjectSet, CommitPlacementSet, CommitPlacementSetState, DataHealth,
    DecimalU64, EdgeClusterId, GatewayPoolId, Generation, MountGeneration, ObjectEncoding,
    ObjectPlacement, ObjectSet, PlacementGeneration, PlacementId, PlacementSetId, PlacementState,
    RegionId, ReplicationId, ReplicationObjectState, ReplicationState, RequestId, RouteGeneration,
    SessionGeneration, StorageVolumeId, TenantId, TransferId, TransferRouteId, UnixMillis,
    WorkspaceId, WorkspaceLifecycle,
};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection, SqlitePool};

use super::authority::{
    decode, digest_from_blob, encode, storage_corruption, storage_error, SqliteAuthorityStore,
};
use crate::{
    same_retry_request, valid_replication_transition, validate_replication_checkpoints,
    validate_replication_publication, validate_replication_record, CancelReplicationRequest,
    CentralError, CentralErrorCode, CentralResult, CommitAvailabilityRecord,
    FinalizeReplicationRequest, FinalizeReplicationResult,
    MaterializationLeaseExpiryReconciliation, MaterializationPlan,
    MaterializationPlanInsertOutcome, MaterializationPlanReplacement, PlacementRepository,
    RefreshReplicationRoutesRequest, ReplicationObjectRecord, ReplicationRecord,
    ReplicationRouteBinding, ReplicationStateTransitionRequest, RetryReplicationRequest,
    RetryReplicationResult, WorkspaceRecord,
};

fn as_i64(value: UnixMillis) -> CentralResult<i64> {
    i64::try_from(value.get()).map_err(|_| {
        CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            "placement timestamp exceeds SQLite integer range",
        )
    })
}

fn unix_ms(value: i64, field: &str) -> CentralResult<UnixMillis> {
    u64::try_from(value)
        .map(UnixMillis::new)
        .map_err(|_| storage_corruption(format!("stored {field} is negative")))
}

fn replication_state_name(state: ReplicationState) -> &'static str {
    match state {
        ReplicationState::Queued => "queued",
        ReplicationState::Planning => "planning",
        ReplicationState::Transferring => "transferring",
        ReplicationState::Verifying => "verifying",
        ReplicationState::Published => "published",
        ReplicationState::Failed => "failed",
        ReplicationState::Cancelled => "cancelled",
    }
}

fn parse_replication_state(value: &str) -> CentralResult<ReplicationState> {
    match value {
        "queued" => Ok(ReplicationState::Queued),
        "planning" => Ok(ReplicationState::Planning),
        "transferring" => Ok(ReplicationState::Transferring),
        "verifying" => Ok(ReplicationState::Verifying),
        "published" => Ok(ReplicationState::Published),
        "failed" => Ok(ReplicationState::Failed),
        "cancelled" => Ok(ReplicationState::Cancelled),
        _ => Err(storage_corruption(format!(
            "stored replication state {value:?} is unknown"
        ))),
    }
}

fn replication_object_state_name(state: ReplicationObjectState) -> &'static str {
    match state {
        ReplicationObjectState::Queued => "queued",
        ReplicationObjectState::Transferring => "transferring",
        ReplicationObjectState::Verified => "verified",
        ReplicationObjectState::Failed => "failed",
    }
}

fn parse_replication_object_state(value: &str) -> CentralResult<ReplicationObjectState> {
    match value {
        "queued" => Ok(ReplicationObjectState::Queued),
        "transferring" => Ok(ReplicationObjectState::Transferring),
        "verified" => Ok(ReplicationObjectState::Verified),
        "failed" => Ok(ReplicationObjectState::Failed),
        _ => Err(storage_corruption(format!(
            "stored replication object state {value:?} is unknown"
        ))),
    }
}

fn workspace_lifecycle_name(state: WorkspaceLifecycle) -> &'static str {
    match state {
        WorkspaceLifecycle::Provisioning => "provisioning",
        WorkspaceLifecycle::Active => "active",
        WorkspaceLifecycle::Unavailable => "unavailable",
        WorkspaceLifecycle::Deleting => "deleting",
        WorkspaceLifecycle::Deleted => "deleted",
    }
}

fn parse_workspace_lifecycle(value: &str) -> CentralResult<WorkspaceLifecycle> {
    match value {
        "provisioning" => Ok(WorkspaceLifecycle::Provisioning),
        "active" => Ok(WorkspaceLifecycle::Active),
        "unavailable" => Ok(WorkspaceLifecycle::Unavailable),
        "deleting" => Ok(WorkspaceLifecycle::Deleting),
        "deleted" => Ok(WorkspaceLifecycle::Deleted),
        _ => Err(storage_corruption(format!(
            "stored workspace lifecycle {value:?} is unknown"
        ))),
    }
}

/// Validates the child identities of a materialization aggregate before any SQLite mutation.
/// The initial insert path performs the same checks inline; keeping this focused validator here
/// lets replacement plans use the transaction boundary without accepting a malformed child set.
fn validate_materialization_plan_shape(
    plan: &MaterializationPlan,
    object_set: &ObjectSet,
) -> CentralResult<()> {
    let job = &plan.job;
    let namespace = &job.key.object_namespace_id;
    let expected_objects = object_set
        .objects
        .iter()
        .map(|object| (object.object_id, object))
        .collect::<BTreeMap<_, _>>();
    let mut object_ids = BTreeSet::new();
    for object in &plan.objects {
        object.validate().map_err(protocol_invalid)?;
        let expected = expected_objects
            .get(&object.object.object_id)
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization Object is not part of the Commit ObjectSet",
                )
                .with_retryable(false)
            })?;
        if object.materialization_id != job.materialization_id
            || object.plan_revision != job.plan_revision
            || object.object.object_namespace_id != *namespace
            || object.object.size != expected.size
            || object.object.encoding != expected.encoding
            || object.object.ordinal != expected.ordinal
            || !object_ids.insert(object.object.object_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Object does not match its Job fence or is duplicated",
            )
            .with_retryable(false));
        }
    }
    if object_ids.len() != expected_objects.len() {
        return Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "materialization plan must include every Commit Object",
        )
        .with_retryable(false));
    }

    let mut batch_ids = BTreeSet::new();
    let mut assigned = BTreeMap::new();
    for batch in &plan.batches {
        batch.validate().map_err(protocol_invalid)?;
        if batch.materialization_id != job.materialization_id
            || batch.plan_revision != job.plan_revision
            || batch.target.tenant_id != job.key.tenant_id
            || batch.target.object_namespace_id != *namespace
            || batch.target.storage_volume_id != job.key.target_storage_volume_id
            || !batch_ids.insert(batch.batch_id.clone())
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Batch does not match its Job fence or is duplicated",
            )
            .with_retryable(false));
        }
        for object_id in &batch.object_ids {
            if !object_ids.contains(object_id)
                || assigned
                    .insert(*object_id, batch.batch_id.clone())
                    .is_some()
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization Batch object list is invalid or overlaps another Batch",
                )
                .with_retryable(false));
            }
        }
    }
    for object in &plan.objects {
        if let Some(batch_id) = &object.current_batch_id {
            if assigned.get(&object.object.object_id) != Some(batch_id) {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization Object current Batch does not match its plan",
                )
                .with_retryable(false));
            }
        }
    }

    let mut read_lease_ids = BTreeSet::new();
    for lease in &plan.object_read_leases {
        lease.validate_for_acquisition().map_err(protocol_invalid)?;
        if lease.materialization_id != job.materialization_id
            || lease.plan_revision != job.plan_revision
            || lease.tenant_id != job.key.tenant_id
            || lease.object_namespace_id != *namespace
            || !batch_ids.contains(&lease.batch_id)
            || !object_ids.contains(&lease.object_id)
            || !read_lease_ids.insert(lease.lease_id.clone())
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "object read lease does not match its materialization plan",
            )
            .with_retryable(false));
        }
        let object = plan
            .objects
            .iter()
            .find(|object| {
                object.object.object_id == lease.object_id
                    && object.current_batch_id.as_ref() == Some(&lease.batch_id)
            })
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "object read lease is not assigned to its Batch",
                )
                .with_retryable(false)
            })?;
        if object.primary_source.as_ref() != Some(&lease.placement_id)
            && !object.fallback_sources.contains(&lease.placement_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "object read lease placement is not selected for its object task",
            )
            .with_retryable(false));
        }
    }

    let mut staging_lease_ids = BTreeSet::new();
    for lease in &plan.staging_leases {
        lease.validate_for_acquisition().map_err(protocol_invalid)?;
        if lease.materialization_id != job.materialization_id
            || lease.plan_revision != job.plan_revision
            || lease.tenant_id != job.key.tenant_id
            || lease.object_namespace_id != *namespace
            || lease.target_storage_volume_id != job.key.target_storage_volume_id
            || !object_ids.contains(&lease.object_id)
            || !staging_lease_ids.insert(lease.lease_id.clone())
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "staging lease does not match its materialization plan",
            )
            .with_retryable(false));
        }
        let object = plan
            .objects
            .iter()
            .find(|object| object.object.object_id == lease.object_id)
            .expect("object ID was checked above");
        lease
            .validate_against_object(object)
            .map_err(protocol_invalid)?;
    }
    for object in &plan.objects {
        if object.complete() {
            continue;
        }
        let Some(batch_id) = &object.current_batch_id else {
            continue;
        };
        if !plan
            .object_read_leases
            .iter()
            .any(|lease| lease.batch_id == *batch_id && lease.object_id == object.object.object_id)
            || !plan
                .staging_leases
                .iter()
                .any(|lease| lease.object_id == object.object.object_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "every assigned materialization Object requires source and staging leases",
            )
            .with_retryable(false));
        }
    }
    Ok(())
}

/// Resolves every plan read lease to its exact durable Placement while the caller owns the plan
/// publication transaction. The indexed `storage_volume_id` is deliberately derived from the
/// Placement, not from the Batch's primary route: fallback leases may protect another Volume,
/// while the primary lease must still match the signed Batch source fence.
async fn materialization_read_lease_volumes(
    connection: &mut SqliteConnection,
    batches: &[MaterializationBatch],
    objects: &[MaterializationObject],
    leases: &[ObjectReadLease],
) -> CentralResult<BTreeMap<String, StorageVolumeId>> {
    let mut volumes = BTreeMap::new();
    for lease in leases {
        let object = objects
            .iter()
            .find(|object| {
                object.object.object_id == lease.object_id
                    && object.current_batch_id.as_ref() == Some(&lease.batch_id)
            })
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "object read lease is not assigned to its Batch",
                )
                .with_retryable(false)
            })?;
        let batch = batches
            .iter()
            .find(|batch| batch.batch_id == lease.batch_id)
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "object read lease references an unknown Batch",
                )
                .with_retryable(false)
            })?;
        let row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain \
             FROM object_placements WHERE tenant_id = ? AND object_namespace_id = ? \
               AND placement_id = ? AND object_id = ? AND placement_generation = ? LIMIT 1",
        )
        .bind(lease.tenant_id.as_str())
        .bind(lease.object_namespace_id.as_str())
        .bind(lease.placement_id.as_str())
        .bind(lease.object_id.as_bytes().as_slice())
        .bind(v2_i64(
            lease.placement_generation.get(),
            "placement_generation",
        )?)
        .fetch_optional(&mut *connection)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "object read lease placement is not registered",
            )
            .with_retryable(false)
        })?;
        let placement = decode_v2_object_placement(&row)?;
        placement
            .validate_against(&object.object)
            .map_err(protocol_invalid)?;
        if !placement.readable() {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "object read lease placement is not readable",
            )
            .with_retryable(false));
        }
        let volume = placement.storage_volume_id.clone().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "object read lease source must reference a StorageVolume",
            )
            .with_retryable(false)
        })?;
        if object.primary_source.as_ref() == Some(&lease.placement_id)
            && (placement.storage_volume_id != batch.source.storage_volume_id
                || placement.archive_id != batch.source.archive_id
                || placement.placement_generation != batch.source.placement_generation)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "object read lease primary Placement does not match its Batch source fence",
            )
            .with_retryable(false));
        }
        volumes.insert(lease.lease_id.to_string(), volume);
    }
    Ok(volumes)
}

/// A replan publishes the next revision in one operation, but its state is logically reached
/// through the durable `Planning` phase. Accept that two-step path at the aggregate boundary so
/// retries can move a recoverable/failed Job directly to the state selected by the planner.
fn materialization_state_transition_allowed(
    current: MaterializationJobState,
    next: MaterializationJobState,
) -> bool {
    // A completed Job is normally terminal, but a later integrity observation can make its
    // derived target Coverage partial. The explicit retry path then reopens it for one fenced
    // planning revision; all other terminal transitions remain rejected by the domain state
    // machine.
    (current == MaterializationJobState::Complete && next == MaterializationJobState::Planning)
        || current.can_transition_to(next)
        || (current.can_transition_to(MaterializationJobState::Planning)
            && MaterializationJobState::Planning.can_transition_to(next))
}

impl SqliteAuthorityStore {
    async fn get_materialization_for_namespace(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Option<MaterializationJob>> {
        sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
             FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(materialization_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(|row| decode_v2_materialization(&row))
        .transpose()
    }

    async fn get_active_materialization_for_target(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        commit_id: &CommitId,
        target_storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<Option<MaterializationJob>> {
        sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
             FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? \
               AND target_storage_volume_id = ? AND state IN \
               ('queued', 'planning', 'waiting_for_sources', 'materializing', 'verifying', 'stalled') \
             ORDER BY updated_at_unix_ms DESC, materialization_id DESC LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(commit_id.digest().as_bytes().as_slice())
        .bind(target_storage_volume_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(|row| decode_v2_materialization(&row))
        .transpose()
    }

    async fn release_receipt_leases(
        &self,
        receipt: &MaterializationObjectReceipt,
        _batch: &MaterializationBatch,
        task: &MaterializationObject,
    ) -> CentralResult<()> {
        let source_ids = task
            .primary_source
            .iter()
            .chain(task.fallback_sources.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        for source_id in source_ids {
            let lease_id = object_read_lease_id(
                &receipt.materialization_id,
                &receipt.batch_id,
                receipt.plan_revision,
                receipt.batch_attempt,
                &receipt.object_namespace_id,
                receipt.object_id,
                &source_id,
            )?;
            self.release_object_read_lease(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &lease_id,
            )
            .await?;
        }
        let lease_id = staging_lease_id(
            &receipt.materialization_id,
            receipt.plan_revision,
            &receipt.object_namespace_id,
            receipt.object_id,
        )?;
        self.release_staging_lease(&receipt.tenant_id, &receipt.object_namespace_id, &lease_id)
            .await?;
        Ok(())
    }

    async fn replay_materialization_receipt(
        &self,
        receipt: &MaterializationObjectReceipt,
    ) -> CentralResult<Option<ObjectPlacementV2>> {
        let row = sqlx::query(
            "SELECT payload, tenant_id, object_namespace_id, receipt_id, materialization_id, batch_id, \
             plan_revision, batch_attempt, object_id, size, encoding, verified_digest, \
             target_storage_volume_id, target_placement_generation, committed_offset, verified_at_unix_ms \
             FROM materialization_receipts WHERE tenant_id = ? AND object_namespace_id = ? AND receipt_id = ? LIMIT 1",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.receipt_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let old = decode_v2_materialization_receipt(&row)?;
        if old != *receipt {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization receipt ID is already in use",
            )
            .with_retryable(false));
        }
        let placement_row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
             WHERE tenant_id = ? AND object_namespace_id = ? AND object_id = ? \
               AND storage_volume_id = ? AND placement_generation = ? LIMIT 1",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.object_id.as_bytes().as_slice())
        .bind(receipt.target_storage_volume_id.as_str())
        .bind(v2_i64(
            receipt.target_placement_generation.get(),
            "target_placement_generation",
        )?)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| storage_corruption("materialization receipt has no Placement"))?;
        let placement = decode_v2_object_placement(&placement_row)?;
        if placement.object_id != receipt.object_id
            || placement.object_namespace_id != receipt.object_namespace_id
            || placement.size != receipt.size
            || placement.encoding != receipt.encoding
            || placement.verified_digest != receipt.verified_digest
            || placement.storage_volume_id.as_ref() != Some(&receipt.target_storage_volume_id)
            || placement.placement_generation != receipt.target_placement_generation
            || !placement.readable()
        {
            return Err(storage_corruption(
                "materialization receipt Placement disagrees with its evidence",
            ));
        }
        let batch = self
            .list_materialization_batches(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .into_iter()
            .find(|candidate| candidate.batch_id == receipt.batch_id)
            .ok_or_else(|| storage_corruption("materialization receipt replay has no Batch"))?;
        let task = self
            .list_materialization_objects(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .into_iter()
            .find(|candidate| {
                candidate.object.object_namespace_id == receipt.object_namespace_id
                    && candidate.object.object_id == receipt.object_id
            })
            .ok_or_else(|| {
                storage_corruption("materialization receipt replay has no Object task")
            })?;
        self.release_receipt_leases(receipt, &batch, &task).await?;
        Ok(Some(placement))
    }
}

fn protocol_invalid(error: impl std::fmt::Display) -> CentralError {
    CentralError::new(CentralErrorCode::ProtocolInvalid, error.to_string()).with_retryable(false)
}

fn decode_optional_id<T, E: std::fmt::Display>(
    row: &SqliteRow,
    column: &str,
    parser: fn(String) -> Result<T, E>,
) -> CentralResult<Option<T>> {
    row.try_get::<Option<String>, _>(column)
        .map_err(storage_error)?
        .map(parser)
        .transpose()
        .map_err(|error| storage_corruption(format!("stored replication {column}: {error}")))
}

fn object_encoding_name(encoding: ObjectEncoding) -> &'static str {
    match encoding {
        ObjectEncoding::Raw => "raw",
        ObjectEncoding::Zstd => "zstd",
    }
}

fn parse_object_encoding(value: &str) -> CentralResult<ObjectEncoding> {
    match value {
        "raw" => Ok(ObjectEncoding::Raw),
        "zstd" => Ok(ObjectEncoding::Zstd),
        _ => Err(storage_corruption(format!(
            "stored object encoding {value:?} is unknown"
        ))),
    }
}

fn placement_set_state_name(state: CommitPlacementSetState) -> &'static str {
    match state {
        CommitPlacementSetState::Staged => "staged",
        CommitPlacementSetState::Published => "published",
        CommitPlacementSetState::Retiring => "retiring",
        CommitPlacementSetState::Deleted => "deleted",
    }
}

fn parse_placement_set_state(value: &str) -> CentralResult<CommitPlacementSetState> {
    match value {
        "staged" => Ok(CommitPlacementSetState::Staged),
        "published" => Ok(CommitPlacementSetState::Published),
        "retiring" => Ok(CommitPlacementSetState::Retiring),
        "deleted" => Ok(CommitPlacementSetState::Deleted),
        _ => Err(storage_corruption(format!(
            "stored placement set state {value:?} is unknown"
        ))),
    }
}

fn placement_state_name(state: PlacementState) -> &'static str {
    match state {
        PlacementState::Verified => "verified",
        PlacementState::Retiring => "retiring",
        PlacementState::Deleted => "deleted",
        PlacementState::Lost => "lost",
    }
}

fn parse_placement_state(value: &str) -> CentralResult<PlacementState> {
    match value {
        "verified" => Ok(PlacementState::Verified),
        "retiring" => Ok(PlacementState::Retiring),
        "deleted" => Ok(PlacementState::Deleted),
        "lost" => Ok(PlacementState::Lost),
        _ => Err(storage_corruption(format!(
            "stored object placement state {value:?} is unknown"
        ))),
    }
}

fn v2_placement_state_name(
    state: neoengram_domain::protocol::materialization::ObjectPlacementState,
) -> &'static str {
    use neoengram_domain::protocol::materialization::ObjectPlacementState;
    match state {
        ObjectPlacementState::Verified => "verified",
        ObjectPlacementState::Retiring => "retiring",
        ObjectPlacementState::Deleted => "deleted",
        ObjectPlacementState::Lost => "lost",
    }
}

fn parse_v2_placement_state(
    value: &str,
) -> CentralResult<neoengram_domain::protocol::materialization::ObjectPlacementState> {
    use neoengram_domain::protocol::materialization::ObjectPlacementState;
    match value {
        "verified" => Ok(ObjectPlacementState::Verified),
        "retiring" => Ok(ObjectPlacementState::Retiring),
        "deleted" => Ok(ObjectPlacementState::Deleted),
        "lost" => Ok(ObjectPlacementState::Lost),
        _ => Err(storage_corruption(format!(
            "stored v2 placement state {value:?} is unknown"
        ))),
    }
}

fn placement_health_state_name(state: PlacementHealthState) -> &'static str {
    match state {
        PlacementHealthState::Healthy => "healthy",
        PlacementHealthState::Missing => "missing",
        PlacementHealthState::Corrupt => "corrupt",
        PlacementHealthState::Orphan => "orphan",
        PlacementHealthState::Unknown => "unknown",
    }
}

fn parse_placement_health_state(value: &str) -> CentralResult<PlacementHealthState> {
    match value {
        "healthy" => Ok(PlacementHealthState::Healthy),
        "missing" => Ok(PlacementHealthState::Missing),
        "corrupt" => Ok(PlacementHealthState::Corrupt),
        "orphan" => Ok(PlacementHealthState::Orphan),
        "unknown" => Ok(PlacementHealthState::Unknown),
        _ => Err(storage_corruption(format!(
            "stored placement health state {value:?} is unknown"
        ))),
    }
}

fn coverage_state_name(
    state: neoengram_domain::protocol::materialization::CoverageState,
) -> &'static str {
    use neoengram_domain::protocol::materialization::CoverageState;
    match state {
        CoverageState::Partial => "partial",
        CoverageState::Complete => "complete",
        CoverageState::Retiring => "retiring",
        CoverageState::Deleted => "deleted",
    }
}

fn parse_coverage_state(
    value: &str,
) -> CentralResult<neoengram_domain::protocol::materialization::CoverageState> {
    use neoengram_domain::protocol::materialization::CoverageState;
    match value {
        "partial" => Ok(CoverageState::Partial),
        "complete" => Ok(CoverageState::Complete),
        "retiring" => Ok(CoverageState::Retiring),
        "deleted" => Ok(CoverageState::Deleted),
        _ => Err(storage_corruption(format!(
            "stored Coverage state {value:?} is unknown"
        ))),
    }
}

fn materialization_job_state_name(state: MaterializationJobState) -> &'static str {
    match state {
        MaterializationJobState::Queued => "queued",
        MaterializationJobState::Planning => "planning",
        MaterializationJobState::WaitingForSources => "waiting_for_sources",
        MaterializationJobState::Materializing => "materializing",
        MaterializationJobState::Verifying => "verifying",
        MaterializationJobState::Complete => "complete",
        MaterializationJobState::Stalled => "stalled",
        MaterializationJobState::Failed => "failed",
        MaterializationJobState::Cancelled => "cancelled",
    }
}

fn parse_materialization_job_state(value: &str) -> CentralResult<MaterializationJobState> {
    match value {
        "queued" => Ok(MaterializationJobState::Queued),
        "planning" => Ok(MaterializationJobState::Planning),
        "waiting_for_sources" => Ok(MaterializationJobState::WaitingForSources),
        "materializing" => Ok(MaterializationJobState::Materializing),
        "verifying" => Ok(MaterializationJobState::Verifying),
        "complete" => Ok(MaterializationJobState::Complete),
        "stalled" => Ok(MaterializationJobState::Stalled),
        "failed" => Ok(MaterializationJobState::Failed),
        "cancelled" => Ok(MaterializationJobState::Cancelled),
        _ => Err(storage_corruption(format!(
            "stored materialization Job state {value:?} is unknown"
        ))),
    }
}

fn materialization_batch_state_name(state: MaterializationBatchState) -> &'static str {
    match state {
        MaterializationBatchState::Queued => "queued",
        MaterializationBatchState::Assigned => "assigned",
        MaterializationBatchState::Transferring => "transferring",
        MaterializationBatchState::Verifying => "verifying",
        MaterializationBatchState::Succeeded => "succeeded",
        MaterializationBatchState::Failed => "failed",
    }
}

fn parse_materialization_batch_state(value: &str) -> CentralResult<MaterializationBatchState> {
    match value {
        "queued" => Ok(MaterializationBatchState::Queued),
        "assigned" => Ok(MaterializationBatchState::Assigned),
        "transferring" => Ok(MaterializationBatchState::Transferring),
        "verifying" => Ok(MaterializationBatchState::Verifying),
        "succeeded" => Ok(MaterializationBatchState::Succeeded),
        "failed" => Ok(MaterializationBatchState::Failed),
        _ => Err(storage_corruption(format!(
            "stored materialization Batch state {value:?} is unknown"
        ))),
    }
}

fn materialization_object_state_name(state: MaterializationObjectState) -> &'static str {
    match state {
        MaterializationObjectState::Missing => "missing",
        MaterializationObjectState::Reserved => "reserved",
        MaterializationObjectState::Transferring => "transferring",
        MaterializationObjectState::Verified => "verified",
        MaterializationObjectState::Published => "published",
        MaterializationObjectState::AlreadyPresent => "already_present",
        MaterializationObjectState::Failed => "failed",
    }
}

fn parse_materialization_object_state(value: &str) -> CentralResult<MaterializationObjectState> {
    match value {
        "missing" => Ok(MaterializationObjectState::Missing),
        "reserved" => Ok(MaterializationObjectState::Reserved),
        "transferring" => Ok(MaterializationObjectState::Transferring),
        "verified" => Ok(MaterializationObjectState::Verified),
        "published" => Ok(MaterializationObjectState::Published),
        "already_present" => Ok(MaterializationObjectState::AlreadyPresent),
        "failed" => Ok(MaterializationObjectState::Failed),
        _ => Err(storage_corruption(format!(
            "stored materialization Object state {value:?} is unknown"
        ))),
    }
}

fn materialization_lease_state_name(state: MaterializationLeaseState) -> &'static str {
    match state {
        MaterializationLeaseState::Active => "active",
        MaterializationLeaseState::Released => "released",
        MaterializationLeaseState::Expired => "expired",
    }
}

fn parse_materialization_lease_state(value: &str) -> CentralResult<MaterializationLeaseState> {
    match value {
        "active" => Ok(MaterializationLeaseState::Active),
        "released" => Ok(MaterializationLeaseState::Released),
        "expired" => Ok(MaterializationLeaseState::Expired),
        _ => Err(storage_corruption(format!(
            "stored materialization lease state {value:?} is unknown"
        ))),
    }
}

fn v2_i64(value: u64, field: &str) -> CentralResult<i64> {
    i64::try_from(value).map_err(|_| {
        CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            format!("{field} exceeds SQLite integer range"),
        )
    })
}

/// Returns the wall-clock time used for derived v2 projections that do not carry timestamps in
/// their protocol value (for example, Coverage summaries). Materialization children inherit the
/// parent Job timestamps instead; this fallback keeps standalone projection writes sortable and
/// prevents the SQLite timestamp columns from being left at their zero sentinel.
fn current_unix_ms() -> CentralResult<UnixMillis> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            storage_corruption(format!("system clock is before Unix epoch: {error}"))
        })?;
    let millis = u64::try_from(elapsed.as_millis())
        .map_err(|_| storage_corruption("system clock timestamp exceeds u64 range"))?;
    Ok(UnixMillis::new(millis))
}

fn v2_decode<T: serde::de::DeserializeOwned>(row: &SqliteRow, field: &str) -> CentralResult<T> {
    let payload = row
        .try_get::<Vec<u8>, _>("payload")
        .map_err(storage_error)?;
    decode(&payload).map_err(|error| {
        storage_corruption(format!("stored v2 {field} payload is invalid: {error}"))
    })
}

fn decode_v2_object_placement(row: &SqliteRow) -> CentralResult<ObjectPlacementV2> {
    let placement: ObjectPlacementV2 = v2_decode(row, "object placement")?;
    placement.validate().map_err(protocol_invalid)?;
    let state = parse_v2_placement_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    if state != placement.state
        || row
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?
            != placement.tenant_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != placement.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("placement_id")
            .map_err(storage_error)?
            != placement.placement_id.as_str()
        || row
            .try_get::<Vec<u8>, _>("object_id")
            .map_err(storage_error)?
            != placement.object_id.as_bytes().to_vec()
        || row
            .try_get::<String, _>("storage_volume_id")
            .map_err(storage_error)?
            != placement
                .storage_volume_id
                .as_ref()
                .map(StorageVolumeId::as_str)
                .unwrap_or_default()
        || u64::try_from(row.try_get::<i64, _>("size").map_err(storage_error)?)
            .map_err(|_| storage_corruption("stored v2 object placement size is negative"))?
            != placement.size.get()
        || row
            .try_get::<String, _>("encoding")
            .map_err(storage_error)?
            != object_encoding_name(placement.encoding)
        || row
            .try_get::<Vec<u8>, _>("verified_digest")
            .map_err(storage_error)?
            != placement.verified_digest.as_bytes().to_vec()
        || u64::try_from(
            row.try_get::<i64, _>("placement_generation")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 object placement generation is negative"))?
            != placement.placement_generation.get()
        || row
            .try_get::<String, _>("failure_domain")
            .map_err(storage_error)?
            != placement.failure_domain
    {
        return Err(storage_corruption(
            "v2 object placement indexed identity disagrees with its payload",
        ));
    }
    Ok(placement)
}

fn decode_placement_health_observation(
    row: &SqliteRow,
) -> CentralResult<PlacementHealthObservation> {
    let observation: PlacementHealthObservation = v2_decode(row, "placement health observation")?;
    observation.validate().map_err(protocol_invalid)?;
    let state = parse_placement_health_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    let observed_size = u64::try_from(
        row.try_get::<i64, _>("observed_size")
            .map_err(storage_error)?,
    )
    .map_err(|_| storage_corruption("stored observed size is negative"))?;
    let observed_at = u64::try_from(
        row.try_get::<i64, _>("observed_at_unix_ms")
            .map_err(storage_error)?,
    )
    .map_err(|_| storage_corruption("stored observation timestamp is negative"))?;
    if state != observation.state
        || row
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?
            != observation.tenant_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != observation.object_namespace_id.as_str()
        || row.try_get::<String, _>("scan_id").map_err(storage_error)?
            != observation.scan_id.as_str()
        || row
            .try_get::<String, _>("placement_id")
            .map_err(storage_error)?
            != observation.placement_id.as_str()
        || row
            .try_get::<Vec<u8>, _>("object_id")
            .map_err(storage_error)?
            != observation.object_id.as_bytes().to_vec()
        || row
            .try_get::<String, _>("storage_volume_id")
            .map_err(storage_error)?
            != observation.storage_volume_id.as_str()
        || u64::try_from(
            row.try_get::<i64, _>("placement_generation")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored observation generation is negative"))?
            != observation.placement_generation.get()
        || observed_size != observation.observed_size.get()
        || row
            .try_get::<Vec<u8>, _>("observed_digest")
            .map_err(storage_error)?
            != observation.observed_digest.as_bytes().to_vec()
        || observed_at != observation.observed_at_unix_ms.get()
    {
        return Err(storage_corruption(
            "placement health indexed identity disagrees with its payload",
        ));
    }
    Ok(observation)
}

fn same_v2_placement_evidence(left: &ObjectPlacementV2, right: &ObjectPlacementV2) -> bool {
    left.tenant_id == right.tenant_id
        && left.object_namespace_id == right.object_namespace_id
        && left.object_id == right.object_id
        && left.size == right.size
        && left.encoding == right.encoding
        && left.verified_digest == right.verified_digest
        && left.storage_volume_id == right.storage_volume_id
        && left.archive_id == right.archive_id
        && left.placement_generation == right.placement_generation
        && left.state == right.state
        && left.failure_domain == right.failure_domain
}

fn decode_v2_coverage(row: &SqliteRow) -> CentralResult<VolumeCommitCoverage> {
    let coverage: VolumeCommitCoverage = v2_decode(row, "coverage")?;
    coverage.validate().map_err(protocol_invalid)?;
    let state = parse_coverage_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    if state != coverage.state
        || row
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?
            != coverage.tenant_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != coverage.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("storage_volume_id")
            .map_err(storage_error)?
            != coverage.storage_volume_id.as_str()
        || row
            .try_get::<Vec<u8>, _>("commit_id")
            .map_err(storage_error)?
            != coverage.commit_id.digest().as_bytes().to_vec()
        || row
            .try_get::<Vec<u8>, _>("object_set_digest")
            .map_err(storage_error)?
            != coverage.object_set_digest.as_bytes().to_vec()
        || u64::try_from(
            row.try_get::<i64, _>("placement_generation")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 Coverage generation is negative"))?
            != coverage.placement_generation.get()
        || u64::try_from(
            row.try_get::<i64, _>("object_count")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 Coverage object_count is negative"))?
            != coverage.object_count.get()
        || u64::try_from(
            row.try_get::<i64, _>("verified_object_count")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 Coverage verified_object_count is negative"))?
            != coverage.verified_object_count.get()
        || u64::try_from(
            row.try_get::<i64, _>("total_bytes")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 Coverage total_bytes is negative"))?
            != coverage.total_bytes.get()
        || u64::try_from(
            row.try_get::<i64, _>("verified_bytes")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 Coverage verified_bytes is negative"))?
            != coverage.verified_bytes.get()
    {
        return Err(storage_corruption(
            "v2 Coverage indexed identity disagrees with its payload",
        ));
    }
    Ok(coverage)
}

fn decode_v2_materialization(row: &SqliteRow) -> CentralResult<MaterializationJob> {
    let job: MaterializationJob = v2_decode(row, "materialization")?;
    job.validate().map_err(protocol_invalid)?;
    let state = parse_materialization_job_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    if state != job.state
        || row
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?
            != job.key.tenant_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != job.key.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("target_storage_volume_id")
            .map_err(storage_error)?
            != job.key.target_storage_volume_id.as_str()
        || row
            .try_get::<String, _>("artifact_id")
            .map_err(storage_error)?
            != job.artifact_id.as_str()
        || row
            .try_get::<Vec<u8>, _>("commit_id")
            .map_err(storage_error)?
            != job.key.commit_id.digest().as_bytes().to_vec()
        || row
            .try_get::<String, _>("coverage_goal")
            .map_err(storage_error)?
            != coverage_goal_parts(job.key.coverage_goal)?.0
        || u64::try_from(
            row.try_get::<i64, _>("coverage_goal_value")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored materialization coverage goal is negative"))?
            != coverage_goal_parts(job.key.coverage_goal)?.1
    {
        return Err(storage_corruption(
            "v2 materialization indexed identity disagrees with its payload",
        ));
    }
    Ok(job)
}

fn decode_v2_batch(row: &SqliteRow) -> CentralResult<MaterializationBatch> {
    let batch: MaterializationBatch = v2_decode(row, "materialization batch")?;
    batch.validate().map_err(protocol_invalid)?;
    let state = parse_materialization_batch_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    if state != batch.state
        || row
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?
            != batch.target.tenant_id.as_str()
        || row
            .try_get::<String, _>("target_storage_volume_id")
            .map_err(storage_error)?
            != batch.target.storage_volume_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != batch.target.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("materialization_id")
            .map_err(storage_error)?
            != batch.materialization_id.as_str()
        || u64::try_from(
            row.try_get::<i64, _>("plan_revision")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 Batch plan revision is negative"))?
            != batch.plan_revision.get()
        || u64::try_from(row.try_get::<i64, _>("attempt").map_err(storage_error)?)
            .map_err(|_| storage_corruption("stored v2 Batch attempt is negative"))?
            != batch.batch_attempt.get()
        || row
            .try_get::<Vec<u8>, _>("manifest_digest")
            .map_err(storage_error)?
            != batch.manifest_digest.as_bytes().to_vec()
        || row
            .try_get::<Option<String>, _>("source_storage_volume_id")
            .map_err(storage_error)?
            != batch
                .source
                .storage_volume_id
                .as_ref()
                .map(ToString::to_string)
    {
        return Err(storage_corruption(
            "v2 materialization Batch indexed identity disagrees with its payload",
        ));
    }
    Ok(batch)
}

fn decode_v2_object(row: &SqliteRow) -> CentralResult<MaterializationObject> {
    let object: MaterializationObject = v2_decode(row, "materialization object")?;
    object.validate().map_err(protocol_invalid)?;
    let state = parse_materialization_object_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    if state != object.state
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != object.object.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("staging_key")
            .map_err(storage_error)?
            != object.staging_key
        || row
            .try_get::<Vec<u8>, _>("object_id")
            .map_err(storage_error)?
            != object.object.object_id.as_bytes().to_vec()
        || u64::try_from(row.try_get::<i64, _>("size").map_err(storage_error)?)
            .map_err(|_| storage_corruption("stored v2 materialization object size is negative"))?
            != object.object.size.get()
        || row
            .try_get::<String, _>("encoding")
            .map_err(storage_error)?
            != object_encoding_name(object.object.encoding)
        || u64::try_from(
            row.try_get::<i64, _>("confirmed_offset")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 materialization object offset is negative"))?
            != object.confirmed_offset.get()
        || u64::try_from(
            row.try_get::<i64, _>("plan_revision")
                .map_err(storage_error)?,
        )
        .map_err(|_| {
            storage_corruption("stored v2 materialization object plan revision is negative")
        })? != object.plan_revision.get()
        || u64::try_from(row.try_get::<i64, _>("attempt").map_err(storage_error)?).map_err(
            |_| storage_corruption("stored v2 materialization object attempt is negative"),
        )? != object.attempt.get()
    {
        return Err(storage_corruption(
            "v2 materialization Object indexed identity disagrees with its payload",
        ));
    }
    Ok(object)
}

fn decode_v2_materialization_receipt(
    row: &SqliteRow,
) -> CentralResult<MaterializationObjectReceipt> {
    let receipt: MaterializationObjectReceipt = v2_decode(row, "materialization receipt")?;
    let object = ObjectRef::new(
        receipt.object_namespace_id.clone(),
        receipt.object_id,
        receipt.size.get(),
        receipt.encoding,
        0,
    );
    receipt
        .validate_against(&object)
        .map_err(protocol_invalid)?;
    if row
        .try_get::<String, _>("tenant_id")
        .map_err(storage_error)?
        != receipt.tenant_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != receipt.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("receipt_id")
            .map_err(storage_error)?
            != receipt.receipt_id.as_str()
        || row
            .try_get::<String, _>("materialization_id")
            .map_err(storage_error)?
            != receipt.materialization_id.as_str()
        || row
            .try_get::<String, _>("batch_id")
            .map_err(storage_error)?
            != receipt.batch_id.as_str()
        || u64::try_from(
            row.try_get::<i64, _>("plan_revision")
                .map_err(storage_error)?,
        )
        .map_err(|_| {
            storage_corruption("stored materialization receipt plan revision is negative")
        })? != receipt.plan_revision.get()
        || u64::try_from(
            row.try_get::<i64, _>("batch_attempt")
                .map_err(storage_error)?,
        )
        .map_err(|_| {
            storage_corruption("stored materialization receipt batch attempt is negative")
        })? != receipt.batch_attempt.get()
        || row
            .try_get::<Vec<u8>, _>("object_id")
            .map_err(storage_error)?
            != receipt.object_id.as_bytes().to_vec()
        || u64::try_from(row.try_get::<i64, _>("size").map_err(storage_error)?)
            .map_err(|_| storage_corruption("stored materialization receipt size is negative"))?
            != receipt.size.get()
        || row
            .try_get::<String, _>("encoding")
            .map_err(storage_error)?
            != object_encoding_name(receipt.encoding)
        || row
            .try_get::<Vec<u8>, _>("verified_digest")
            .map_err(storage_error)?
            != receipt.verified_digest.as_bytes().to_vec()
        || row
            .try_get::<String, _>("target_storage_volume_id")
            .map_err(storage_error)?
            != receipt.target_storage_volume_id.as_str()
        || u64::try_from(
            row.try_get::<i64, _>("target_placement_generation")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored materialization receipt generation is negative"))?
            != receipt.target_placement_generation.get()
        || u64::try_from(
            row.try_get::<i64, _>("committed_offset")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored materialization receipt offset is negative"))?
            != receipt.committed_offset.get()
        || u64::try_from(
            row.try_get::<i64, _>("verified_at_unix_ms")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored materialization receipt timestamp is negative"))?
            != receipt.verified_at_unix_ms.get()
    {
        return Err(storage_corruption(
            "materialization receipt indexed identity disagrees with its payload",
        ));
    }
    Ok(receipt)
}

fn decode_v2_read_lease(row: &SqliteRow) -> CentralResult<ObjectReadLease> {
    let lease: ObjectReadLease = v2_decode(row, "object read lease")?;
    lease.validate().map_err(protocol_invalid)?;
    let state = parse_materialization_lease_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    if state != lease.state
        || row
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?
            != lease.tenant_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != lease.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("materialization_id")
            .map_err(storage_error)?
            != lease.materialization_id.as_str()
        || row
            .try_get::<Option<String>, _>("batch_id")
            .map_err(storage_error)?
            != Some(lease.batch_id.as_str().to_owned())
        || row
            .try_get::<Vec<u8>, _>("object_id")
            .map_err(storage_error)?
            != lease.object_id.as_bytes().to_vec()
        || row
            .try_get::<String, _>("placement_id")
            .map_err(storage_error)?
            != lease.placement_id.as_str()
        || u64::try_from(
            row.try_get::<i64, _>("placement_generation")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 read lease generation is negative"))?
            != lease.placement_generation.get()
        || u64::try_from(
            row.try_get::<i64, _>("expires_at_unix_ms")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 read lease expiry is negative"))?
            != lease.expires_at_unix_ms.get()
    {
        return Err(storage_corruption(
            "v2 read lease indexed identity disagrees with its payload",
        ));
    }
    Ok(lease)
}

fn decode_v2_staging_lease(row: &SqliteRow) -> CentralResult<StagingLease> {
    let lease: StagingLease = v2_decode(row, "staging lease")?;
    lease.validate().map_err(protocol_invalid)?;
    let state = parse_materialization_lease_state(
        row.try_get::<String, _>("state")
            .map_err(storage_error)?
            .as_str(),
    )?;
    if state != lease.state
        || row
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?
            != lease.tenant_id.as_str()
        || row
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?
            != lease.object_namespace_id.as_str()
        || row
            .try_get::<String, _>("target_storage_volume_id")
            .map_err(storage_error)?
            != lease.target_storage_volume_id.as_str()
        || row
            .try_get::<String, _>("materialization_id")
            .map_err(storage_error)?
            != lease.materialization_id.as_str()
        || row
            .try_get::<Vec<u8>, _>("object_id")
            .map_err(storage_error)?
            != lease.object_id.as_bytes().to_vec()
        || row
            .try_get::<String, _>("staging_key")
            .map_err(storage_error)?
            != lease.staging_key
        || u64::try_from(
            row.try_get::<i64, _>("target_placement_generation")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 staging lease generation is negative"))?
            != lease.target_placement_generation.get()
        || u64::try_from(
            row.try_get::<i64, _>("expires_at_unix_ms")
                .map_err(storage_error)?,
        )
        .map_err(|_| storage_corruption("stored v2 staging lease expiry is negative"))?
            != lease.expires_at_unix_ms.get()
    {
        return Err(storage_corruption(
            "v2 staging lease indexed identity disagrees with its payload",
        ));
    }
    Ok(lease)
}

fn coverage_goal_parts(
    goal: neoengram_domain::protocol::materialization::CoverageGoal,
) -> CentralResult<(&'static str, u64)> {
    match goal {
        neoengram_domain::protocol::materialization::CoverageGoal::Complete => Ok(("complete", 0)),
        neoengram_domain::protocol::materialization::CoverageGoal::ObjectCount(value) => {
            Ok(("object_count", value.get()))
        }
        neoengram_domain::protocol::materialization::CoverageGoal::ByteCount(value) => {
            Ok(("byte_count", value.get()))
        }
    }
}

fn u64_from_i64(value: i64, field: &str) -> CentralResult<u64> {
    u64::try_from(value).map_err(|_| storage_corruption(format!("stored {field} is negative")))
}

fn digest_from_object_id(bytes: Vec<u8>, field: &str) -> CentralResult<ObjectId> {
    Ok(ObjectId::from_digest(digest_from_blob(bytes, field)?))
}

fn decode_commit_object(row: &SqliteRow) -> CentralResult<CommitObject> {
    let object_id = digest_from_object_id(
        row.try_get::<Vec<u8>, _>("object_id")
            .map_err(storage_error)?,
        "commit object_id",
    )?;
    let size = u64_from_i64(
        row.try_get::<i64, _>("size").map_err(storage_error)?,
        "commit object size",
    )?;
    let encoding = parse_object_encoding(
        row.try_get::<String, _>("encoding")
            .map_err(storage_error)?
            .as_str(),
    )?;
    let ordinal = u64_from_i64(
        row.try_get::<i64, _>("ordinal").map_err(storage_error)?,
        "commit object ordinal",
    )?;
    Ok(CommitObject::new(object_id, size, encoding, ordinal))
}

fn decode_placement_set(row: &SqliteRow) -> CentralResult<CommitPlacementSet> {
    let storage_volume_id = row
        .try_get::<Option<String>, _>("storage_volume_id")
        .map_err(storage_error)?
        .map(StorageVolumeId::new)
        .transpose()
        .map_err(|error| storage_corruption(format!("stored placement Volume ID: {error}")))?;
    let archive_id = row
        .try_get::<Option<String>, _>("archive_id")
        .map_err(storage_error)?
        .map(ArchiveId::new)
        .transpose()
        .map_err(|error| storage_corruption(format!("stored placement Archive ID: {error}")))?;
    let commit_id = CommitId::from_digest(digest_from_blob(
        row.try_get::<Vec<u8>, _>("commit_id")
            .map_err(storage_error)?,
        "placement set commit_id",
    )?);
    Ok(CommitPlacementSet {
        placement_set_id: PlacementSetId::new(
            row.try_get::<String, _>("placement_set_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored placement set ID: {error}")))?,
        tenant_id: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored placement set tenant ID: {error}")))?,
        commit_id,
        backend_id: BackendId::new(
            row.try_get::<String, _>("backend_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored placement set backend ID: {error}")))?,
        storage_volume_id,
        archive_id,
        object_set_digest: digest_from_blob(
            row.try_get::<Vec<u8>, _>("object_set_digest")
                .map_err(storage_error)?,
            "placement set object_set_digest",
        )?,
        object_count: DecimalU64::new(u64_from_i64(
            row.try_get::<i64, _>("object_count")
                .map_err(storage_error)?,
            "placement set object_count",
        )?),
        verified_object_count: DecimalU64::new(u64_from_i64(
            row.try_get::<i64, _>("verified_object_count")
                .map_err(storage_error)?,
            "placement set verified_object_count",
        )?),
        placement_generation: PlacementGeneration::new(u64_from_i64(
            row.try_get::<i64, _>("placement_generation")
                .map_err(storage_error)?,
            "placement set placement_generation",
        )?),
        state: parse_placement_set_state(
            row.try_get::<String, _>("state")
                .map_err(storage_error)?
                .as_str(),
        )?,
    })
}

fn decode_object_placement(row: &SqliteRow) -> CentralResult<ObjectPlacement> {
    let storage_volume_id = row
        .try_get::<Option<String>, _>("storage_volume_id")
        .map_err(storage_error)?
        .map(StorageVolumeId::new)
        .transpose()
        .map_err(|error| {
            storage_corruption(format!("stored object placement Volume ID: {error}"))
        })?;
    let archive_id = row
        .try_get::<Option<String>, _>("archive_id")
        .map_err(storage_error)?
        .map(ArchiveId::new)
        .transpose()
        .map_err(|error| {
            storage_corruption(format!("stored object placement Archive ID: {error}"))
        })?;
    let edge_cluster_id = row
        .try_get::<Option<String>, _>("edge_cluster_id")
        .map_err(storage_error)?
        .map(EdgeClusterId::new)
        .transpose()
        .map_err(|error| {
            storage_corruption(format!("stored object placement EdgeCluster ID: {error}"))
        })?;
    let gateway_pool_id = row
        .try_get::<Option<String>, _>("gateway_pool_id")
        .map_err(storage_error)?
        .map(GatewayPoolId::new)
        .transpose()
        .map_err(|error| {
            storage_corruption(format!("stored object placement GatewayPool ID: {error}"))
        })?;
    let region = row
        .try_get::<Option<String>, _>("region")
        .map_err(storage_error)?
        .map(RegionId::new)
        .transpose()
        .map_err(|error| {
            storage_corruption(format!("stored object placement Region ID: {error}"))
        })?;
    Ok(ObjectPlacement {
        tenant_id: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| {
            storage_corruption(format!("stored object placement tenant ID: {error}"))
        })?,
        object_id: digest_from_object_id(
            row.try_get::<Vec<u8>, _>("object_id")
                .map_err(storage_error)?,
            "object placement object_id",
        )?,
        backend_id: BackendId::new(
            row.try_get::<String, _>("backend_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| {
            storage_corruption(format!("stored object placement backend ID: {error}"))
        })?,
        storage_volume_id,
        archive_id,
        edge_cluster_id,
        gateway_pool_id,
        region,
        placement_generation: PlacementGeneration::new(u64_from_i64(
            row.try_get::<i64, _>("placement_generation")
                .map_err(storage_error)?,
            "object placement placement_generation",
        )?),
        state: parse_placement_state(
            row.try_get::<String, _>("state")
                .map_err(storage_error)?
                .as_str(),
        )?,
        verified_size: DecimalU64::new(u64_from_i64(
            row.try_get::<i64, _>("verified_size")
                .map_err(storage_error)?,
            "object placement verified_size",
        )?),
        verified_digest: digest_from_blob(
            row.try_get::<Vec<u8>, _>("verified_digest")
                .map_err(storage_error)?,
            "object placement verified_digest",
        )?,
        failure_domain: row
            .try_get::<String, _>("failure_domain")
            .map_err(storage_error)?,
    })
}

fn placement_id_for(placement: &ObjectPlacement) -> CentralResult<PlacementId> {
    let mut input = Vec::with_capacity(128);
    input.extend_from_slice(b"neoengram-placement-v1\0");
    input.extend_from_slice(placement.tenant_id.as_str().as_bytes());
    input.push(0);
    input.extend_from_slice(placement.object_id.as_bytes());
    input.extend_from_slice(placement.backend_id.as_str().as_bytes());
    input.extend_from_slice(&placement.placement_generation.get().to_be_bytes());
    let digest = blake3::hash(&input);
    PlacementId::new(format!("placement-{}", &digest.to_hex()[..32])).map_err(protocol_invalid)
}

async fn validate_published_placement_set(
    store: &SqliteAuthorityStore,
    placement_set: &CommitPlacementSet,
) -> CentralResult<()> {
    let commit_digest = placement_set.commit_id.digest();
    let Some(object_set) = store
        .get_commit_object_set(&placement_set.tenant_id, &commit_digest)
        .await?
    else {
        return Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "published PlacementSet requires a stored Commit ObjectSet",
        )
        .with_retryable(false));
    };
    if object_set.object_set.object_set_digest != placement_set.object_set_digest
        || object_set.object_set.object_count() as u64 != placement_set.object_count.get()
    {
        return Err(CentralError::new(
            CentralErrorCode::InvalidState,
            "published PlacementSet does not match the Commit ObjectSet",
        )
        .with_retryable(false));
    }
    for object in &object_set.object_set.objects {
        let placements = store
            .object_placements(&placement_set.tenant_id, &object.object_id)
            .await?;
        let complete = placements.iter().any(|placement| {
            placement.backend_id == placement_set.backend_id
                && placement.placement_generation == placement_set.placement_generation
                && placement.state == PlacementState::Verified
                && placement.verified_size.get() == object.size.get()
                && placement.verified_digest == object.object_id.digest()
                && placement.storage_volume_id == placement_set.storage_volume_id
                && placement.archive_id == placement_set.archive_id
        });
        if !complete {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                format!(
                    "published PlacementSet is missing verified object {}",
                    object.object_id
                ),
            )
            .with_retryable(false));
        }
    }
    Ok(())
}

const PLACEMENT_SET_COLUMNS: &str = "tenant_id, placement_set_id, commit_id, backend_id, \
    storage_volume_id, archive_id, object_set_digest, object_count, verified_object_count, \
    placement_generation, state";
const OBJECT_PLACEMENT_COLUMNS: &str = "tenant_id, placement_id, object_id, backend_id, \
    storage_volume_id, archive_id, edge_cluster_id, gateway_pool_id, region, placement_generation, \
    state, verified_size, verified_digest, failure_domain";

fn decode_replication(row: &SqliteRow) -> CentralResult<ReplicationRecord> {
    Ok(ReplicationRecord {
        tenant_id: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored replication tenant ID: {error}")))?,
        replication_id: ReplicationId::new(
            row.try_get::<String, _>("replication_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored replication ID: {error}")))?,
        artifact_id: decode_optional_id(row, "artifact_id", ArtifactId::new)?,
        commit_id: digest_from_blob(
            row.try_get::<Vec<u8>, _>("commit_id")
                .map_err(storage_error)?,
            "replication commit_id",
        )?,
        target_backend_id: row
            .try_get::<String, _>("target_backend_id")
            .map_err(storage_error)?,
        target_storage_volume_id: StorageVolumeId::new(
            row.try_get::<String, _>("target_storage_volume_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| {
            storage_corruption(format!("stored replication target Volume ID: {error}"))
        })?,
        source_placement_set_id: decode_optional_id(
            row,
            "source_placement_set_id",
            PlacementSetId::new,
        )?,
        source_backend_id: decode_optional_id(row, "source_backend_id", BackendId::new)?,
        source_storage_volume_id: decode_optional_id(
            row,
            "source_storage_volume_id",
            StorageVolumeId::new,
        )?,
        source_edge_cluster_id: decode_optional_id(
            row,
            "source_edge_cluster_id",
            EdgeClusterId::new,
        )?,
        source_gateway_pool_id: decode_optional_id(
            row,
            "source_gateway_pool_id",
            GatewayPoolId::new,
        )?,
        source_placement_generation: row
            .try_get::<Option<i64>, _>("source_placement_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "source placement_generation"))
            .transpose()?
            .map(PlacementGeneration::new),
        source_agent_id: decode_optional_id(row, "source_agent_id", AgentId::new)?,
        source_session_generation: row
            .try_get::<Option<i64>, _>("source_session_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "source session_generation"))
            .transpose()?
            .map(SessionGeneration::new),
        source_mount_generation: row
            .try_get::<Option<i64>, _>("source_mount_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "source mount_generation"))
            .transpose()?
            .map(MountGeneration::new),
        source_route_generation: row
            .try_get::<Option<i64>, _>("source_route_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "source route_generation"))
            .transpose()?
            .map(RouteGeneration::new),
        target_edge_cluster_id: decode_optional_id(
            row,
            "target_edge_cluster_id",
            EdgeClusterId::new,
        )?,
        target_gateway_pool_id: decode_optional_id(
            row,
            "target_gateway_pool_id",
            GatewayPoolId::new,
        )?,
        target_placement_generation: row
            .try_get::<Option<i64>, _>("target_placement_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "target placement_generation"))
            .transpose()?
            .map(PlacementGeneration::new),
        target_agent_id: decode_optional_id(row, "target_agent_id", AgentId::new)?,
        target_session_generation: row
            .try_get::<Option<i64>, _>("target_session_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "target session_generation"))
            .transpose()?
            .map(SessionGeneration::new),
        target_mount_generation: row
            .try_get::<Option<i64>, _>("target_mount_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "target mount_generation"))
            .transpose()?
            .map(MountGeneration::new),
        target_route_generation: row
            .try_get::<Option<i64>, _>("target_route_generation")
            .map_err(storage_error)?
            .map(|value| u64_from_i64(value, "target route_generation"))
            .transpose()?
            .map(RouteGeneration::new),
        transfer_route_id: decode_optional_id(row, "transfer_route_id", TransferRouteId::new)?,
        transfer_id: decode_optional_id(row, "transfer_id", TransferId::new)?,
        target_placement_set_id: decode_optional_id(
            row,
            "target_placement_set_id",
            PlacementSetId::new,
        )?,
        staging_id: row
            .try_get::<Option<String>, _>("staging_id")
            .map_err(storage_error)?,
        object_set_digest: digest_from_blob(
            row.try_get::<Vec<u8>, _>("object_set_digest")
                .map_err(storage_error)?,
            "replication object_set_digest",
        )?,
        state: parse_replication_state(
            row.try_get::<String, _>("state")
                .map_err(storage_error)?
                .as_str(),
        )?,
        request_id: RequestId::new(
            row.try_get::<String, _>("request_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored replication request ID: {error}")))?,
        completed_objects: row
            .try_get::<i64, _>("completed_objects")
            .map_err(storage_error)
            .and_then(|value| {
                u64::try_from(value)
                    .map_err(|_| storage_corruption("completed_objects is negative"))
            })?,
        total_objects: row
            .try_get::<i64, _>("total_objects")
            .map_err(storage_error)
            .and_then(|value| {
                u64::try_from(value).map_err(|_| storage_corruption("total_objects is negative"))
            })?,
        completed_bytes: row
            .try_get::<i64, _>("completed_bytes")
            .map_err(storage_error)
            .and_then(|value| {
                u64::try_from(value).map_err(|_| storage_corruption("completed_bytes is negative"))
            })?,
        total_bytes: row
            .try_get::<i64, _>("total_bytes")
            .map_err(storage_error)
            .and_then(|value| {
                u64::try_from(value).map_err(|_| storage_corruption("total_bytes is negative"))
            })?,
        attempt: row
            .try_get::<i64, _>("attempt")
            .map_err(storage_error)
            .and_then(|value| {
                u64::try_from(value)
                    .map_err(|_| storage_corruption("replication attempt is invalid"))
            })?,
        issue_code: row
            .try_get::<Option<String>, _>("error_code")
            .map_err(storage_error)?,
        issue_message: row
            .try_get::<Option<String>, _>("error_message")
            .map_err(storage_error)?,
        created_at_unix_ms: unix_ms(
            row.try_get::<i64, _>("created_at_unix_ms")
                .map_err(storage_error)?,
            "replication created_at_unix_ms",
        )?,
        updated_at_unix_ms: unix_ms(
            row.try_get::<i64, _>("updated_at_unix_ms")
                .map_err(storage_error)?,
            "replication updated_at_unix_ms",
        )?,
    })
}

fn decode_workspace(row: &SqliteRow) -> CentralResult<WorkspaceRecord> {
    let base_commit_id = row
        .try_get::<Option<Vec<u8>>, _>("base_commit_id")
        .map_err(storage_error)?
        .map(|bytes| digest_from_blob(bytes, "workspace base_commit_id"))
        .transpose()?;
    Ok(WorkspaceRecord {
        tenant_id: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored workspace tenant ID: {error}")))?,
        workspace_id: WorkspaceId::new(
            row.try_get::<String, _>("workspace_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored workspace ID: {error}")))?,
        project_id: neoengram_domain::protocol::ProjectId::new(
            row.try_get::<String, _>("project_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored workspace project ID: {error}")))?,
        artifact_id: neoengram_domain::protocol::ArtifactId::new(
            row.try_get::<String, _>("artifact_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored workspace Artifact ID: {error}")))?,
        base_commit_id,
        target_storage_volume_id: StorageVolumeId::new(
            row.try_get::<String, _>("target_storage_volume_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| {
            storage_corruption(format!("stored workspace target Volume ID: {error}"))
        })?,
        request_id: RequestId::new(
            row.try_get::<String, _>("request_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| storage_corruption(format!("stored workspace request ID: {error}")))?,
        lifecycle: parse_workspace_lifecycle(
            row.try_get::<String, _>("lifecycle")
                .map_err(storage_error)?
                .as_str(),
        )?,
        created_at_unix_ms: unix_ms(
            row.try_get::<i64, _>("created_at_unix_ms")
                .map_err(storage_error)?,
            "workspace created_at_unix_ms",
        )?,
        updated_at_unix_ms: unix_ms(
            row.try_get::<i64, _>("updated_at_unix_ms")
                .map_err(storage_error)?,
            "workspace updated_at_unix_ms",
        )?,
    })
}

const REPLICATION_COLUMNS: &str = "tenant_id, replication_id, commit_id, target_backend_id, \
    target_storage_volume_id, source_placement_set_id, source_backend_id, source_storage_volume_id, \
    source_edge_cluster_id, source_gateway_pool_id, source_placement_generation, source_agent_id, \
    source_session_generation, source_mount_generation, source_route_generation, target_edge_cluster_id, \
    target_gateway_pool_id, target_placement_generation, target_agent_id, target_session_generation, \
    target_mount_generation, target_route_generation, transfer_route_id, \
    transfer_id, target_placement_set_id, staging_id, object_set_digest, state, request_id, \
    attempt, completed_objects, total_objects, completed_bytes, total_bytes, error_code, \
    error_message, created_at_unix_ms, updated_at_unix_ms, \
    (SELECT artifact_id FROM legacy_replication_artifacts AS a WHERE a.tenant_id = legacy_replications.tenant_id \
      AND a.replication_id = legacy_replications.replication_id) AS artifact_id";
const WORKSPACE_COLUMNS: &str = "tenant_id, workspace_id, request_id, project_id, artifact_id, \
    base_commit_id, target_storage_volume_id, lifecycle, created_at_unix_ms, updated_at_unix_ms";

fn route_binding_matches_record(
    record: &ReplicationRecord,
    binding: &ReplicationRouteBinding,
    source: bool,
) -> bool {
    if source {
        record.source_edge_cluster_id.as_ref() == Some(&binding.edge_cluster_id)
            && record.source_gateway_pool_id.as_ref() == Some(&binding.gateway_pool_id)
            && record.source_agent_id.as_ref() == Some(&binding.agent_id)
            && record.source_session_generation == Some(binding.session_generation)
            && record.source_mount_generation == Some(binding.mount_generation)
            && record.source_route_generation == Some(binding.route_generation)
    } else {
        record.target_edge_cluster_id.as_ref() == Some(&binding.edge_cluster_id)
            && record.target_gateway_pool_id.as_ref() == Some(&binding.gateway_pool_id)
            && record.target_agent_id.as_ref() == Some(&binding.agent_id)
            && record.target_session_generation == Some(binding.session_generation)
            && record.target_mount_generation == Some(binding.mount_generation)
            && record.target_route_generation == Some(binding.route_generation)
    }
}

async fn refresh_replication_routes_cas(
    pool: &SqlitePool,
    request: &RefreshReplicationRoutesRequest,
    expected_updated_at_unix_ms: UnixMillis,
) -> CentralResult<u64> {
    let result = sqlx::query(
        "UPDATE legacy_replications SET source_edge_cluster_id = ?, source_gateway_pool_id = ?, \
         source_session_generation = ?, source_mount_generation = ?, source_route_generation = ?, \
         target_edge_cluster_id = ?, target_gateway_pool_id = ?, target_session_generation = ?, \
         target_mount_generation = ?, target_route_generation = ?, updated_at_unix_ms = ? \
         WHERE tenant_id = ? AND replication_id = ? AND attempt = ? \
         AND source_edge_cluster_id = ? AND source_gateway_pool_id = ? AND source_agent_id = ? \
         AND source_session_generation = ? AND source_mount_generation = ? AND source_route_generation = ? \
         AND target_edge_cluster_id = ? AND target_gateway_pool_id = ? AND target_agent_id = ? \
         AND target_session_generation = ? AND target_mount_generation = ? AND target_route_generation = ? \
         AND updated_at_unix_ms = ? \
         AND state IN ('queued', 'planning', 'transferring', 'verifying')",
    )
    .bind(request.source.edge_cluster_id.as_str())
    .bind(request.source.gateway_pool_id.as_str())
    .bind(i64::try_from(request.source.session_generation.get()).map_err(|_| {
        protocol_invalid("source session generation exceeds SQLite range")
    })?)
    .bind(i64::try_from(request.source.mount_generation.get()).map_err(|_| {
        protocol_invalid("source mount generation exceeds SQLite range")
    })?)
    .bind(i64::try_from(request.source.route_generation.get()).map_err(|_| {
        protocol_invalid("source route generation exceeds SQLite range")
    })?)
    .bind(request.target.edge_cluster_id.as_str())
    .bind(request.target.gateway_pool_id.as_str())
    .bind(i64::try_from(request.target.session_generation.get()).map_err(|_| {
        protocol_invalid("target session generation exceeds SQLite range")
    })?)
    .bind(i64::try_from(request.target.mount_generation.get()).map_err(|_| {
        protocol_invalid("target mount generation exceeds SQLite range")
    })?)
    .bind(i64::try_from(request.target.route_generation.get()).map_err(|_| {
        protocol_invalid("target route generation exceeds SQLite range")
    })?)
    .bind(as_i64(request.updated_at_unix_ms)?)
    .bind(request.tenant_id.as_str())
    .bind(request.replication_id.as_str())
    .bind(i64::try_from(request.expected_attempt).map_err(|_| {
        protocol_invalid("replication attempt exceeds SQLite range")
    })?)
    .bind(request.expected_source.edge_cluster_id.as_str())
    .bind(request.expected_source.gateway_pool_id.as_str())
    .bind(request.expected_source.agent_id.as_str())
    .bind(
        i64::try_from(request.expected_source.session_generation.get()).map_err(|_| {
            protocol_invalid("source session generation exceeds SQLite range")
        })?,
    )
    .bind(
        i64::try_from(request.expected_source.mount_generation.get()).map_err(|_| {
            protocol_invalid("source mount generation exceeds SQLite range")
        })?,
    )
    .bind(
        i64::try_from(request.expected_source.route_generation.get()).map_err(|_| {
            protocol_invalid("source route generation exceeds SQLite range")
        })?,
    )
    .bind(request.expected_target.edge_cluster_id.as_str())
    .bind(request.expected_target.gateway_pool_id.as_str())
    .bind(request.expected_target.agent_id.as_str())
    .bind(
        i64::try_from(request.expected_target.session_generation.get()).map_err(|_| {
            protocol_invalid("target session generation exceeds SQLite range")
        })?,
    )
    .bind(
        i64::try_from(request.expected_target.mount_generation.get()).map_err(|_| {
            protocol_invalid("target mount generation exceeds SQLite range")
        })?,
    )
    .bind(
        i64::try_from(request.expected_target.route_generation.get()).map_err(|_| {
            protocol_invalid("target route generation exceeds SQLite range")
        })?,
    )
    .bind(as_i64(expected_updated_at_unix_ms)?)
    .execute(pool)
    .await
    .map_err(storage_error)?;
    Ok(result.rows_affected())
}

async fn cancel_replication_cas(
    pool: &SqlitePool,
    request: &CancelReplicationRequest,
    expected_updated_at_unix_ms: UnixMillis,
) -> CentralResult<u64> {
    let result = sqlx::query(
        "UPDATE legacy_replications SET state = 'cancelled', error_code = ?, error_message = ?, \
         updated_at_unix_ms = ? WHERE tenant_id = ? AND replication_id = ? AND attempt = ? \
         AND updated_at_unix_ms = ? \
         AND state NOT IN ('published', 'failed', 'cancelled')",
    )
    .bind("REPLICATION_CANCELLED")
    .bind("replication was cancelled by the caller")
    .bind(as_i64(request.updated_at_unix_ms)?)
    .bind(request.tenant_id.as_str())
    .bind(request.replication_id.as_str())
    .bind(
        i64::try_from(request.expected_attempt)
            .map_err(|_| protocol_invalid("replication attempt exceeds SQLite range"))?,
    )
    .bind(as_i64(expected_updated_at_unix_ms)?)
    .execute(pool)
    .await
    .map_err(storage_error)?;
    Ok(result.rows_affected())
}

fn decode_replication_object(row: &SqliteRow) -> CentralResult<ReplicationObjectRecord> {
    Ok(ReplicationObjectRecord {
        tenant_id: TenantId::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| {
            storage_corruption(format!("stored replication object tenant ID: {error}"))
        })?,
        replication_id: ReplicationId::new(
            row.try_get::<String, _>("replication_id")
                .map_err(storage_error)?,
        )
        .map_err(|error| {
            storage_corruption(format!("stored replication object Replication ID: {error}"))
        })?,
        object_id: digest_from_object_id(
            row.try_get::<Vec<u8>, _>("object_id")
                .map_err(storage_error)?,
            "replication object object_id",
        )?,
        offset: u64_from_i64(
            row.try_get::<i64, _>("offset").map_err(storage_error)?,
            "replication object offset",
        )?,
        state: parse_replication_object_state(
            row.try_get::<String, _>("state")
                .map_err(storage_error)?
                .as_str(),
        )?,
        retry_count: u64_from_i64(
            row.try_get::<i64, _>("retry_count")
                .map_err(storage_error)?,
            "replication object retry_count",
        )?,
        updated_at_unix_ms: unix_ms(
            row.try_get::<i64, _>("updated_at_unix_ms")
                .map_err(storage_error)?,
            "replication object updated_at_unix_ms",
        )?,
    })
}

#[async_trait]
impl PlacementRepository for SqliteAuthorityStore {
    async fn insert_object_placement_v2(
        &self,
        placement: ObjectPlacementV2,
    ) -> CentralResult<ObjectPlacementV2> {
        placement.validate().map_err(protocol_invalid)?;
        let volume = placement.storage_volume_id.clone().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "v2 object placements currently require a StorageVolume",
            )
            .with_retryable(false)
        })?;
        let key = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
             WHERE tenant_id = ? AND object_namespace_id = ? AND object_id = ? \
               AND storage_volume_id = ? AND placement_generation = ?",
        )
        .bind(placement.tenant_id.as_str())
        .bind(placement.object_namespace_id.as_str())
        .bind(placement.object_id.as_bytes().as_slice())
        .bind(volume.as_str())
        .bind(v2_i64(placement.placement_generation.get(), "placement_generation")?)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if let Some(row) = key {
            let existing = decode_v2_object_placement(&row)?;
            return if existing == placement {
                Ok(existing)
            } else if same_v2_placement_evidence(&existing, &placement) {
                // A duplicate receipt may choose a different receipt-derived PlacementId. The
                // durable physical identity is still one namespace/object/Volume/generation;
                // return the first row so both concurrent callers converge on one Placement.
                Ok(existing)
            } else {
                Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "v2 object placement identity is already bound to different metadata",
                )
                .with_retryable(false))
            };
        }
        let payload = encode(&placement)?;
        let now = current_unix_ms()?;
        let result = sqlx::query(
            "INSERT INTO object_placements \
             (tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, \
              storage_volume_id, placement_generation, state, failure_domain, \
              created_at_unix_ms, updated_at_unix_ms, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(placement.tenant_id.as_str())
        .bind(placement.object_namespace_id.as_str())
        .bind(placement.placement_id.as_str())
        .bind(placement.object_id.as_bytes().as_slice())
        .bind(v2_i64(placement.size.get(), "object size")?)
        .bind(object_encoding_name(placement.encoding))
        .bind(placement.verified_digest.as_bytes().as_slice())
        .bind(volume.as_str())
        .bind(v2_i64(placement.placement_generation.get(), "placement_generation")?)
        .bind(v2_placement_state_name(placement.state))
        .bind(&placement.failure_domain)
        .bind(v2_i64(now.get(), "created_at_unix_ms")?)
        .bind(v2_i64(now.get(), "updated_at_unix_ms")?)
        .bind(payload)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(placement),
            Err(error) if is_unique(&error) => {
                let row = sqlx::query(
                    "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
                     WHERE tenant_id = ? AND object_namespace_id = ? AND placement_id = ?",
                )
                .bind(placement.tenant_id.as_str())
                .bind(placement.object_namespace_id.as_str())
                .bind(placement.placement_id.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?
                .or(sqlx::query(
                    "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
                     WHERE tenant_id = ? AND object_namespace_id = ? AND object_id = ? \
                       AND storage_volume_id = ? AND placement_generation = ?",
                )
                .bind(placement.tenant_id.as_str())
                .bind(placement.object_namespace_id.as_str())
                .bind(placement.object_id.as_bytes().as_slice())
                .bind(volume.as_str())
                .bind(v2_i64(placement.placement_generation.get(), "placement_generation")?)
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?)
                .ok_or_else(|| storage_corruption("v2 placement uniqueness conflict has no row"))?;
                let existing = decode_v2_object_placement(&row)?;
                if same_v2_placement_evidence(&existing, &placement) {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "v2 object placement identity is already bound to different metadata",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn object_placements_v2(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        object_id: &ObjectId,
    ) -> CentralResult<Vec<ObjectPlacementV2>> {
        let rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain \
             FROM object_placements \
             WHERE tenant_id = ? AND object_namespace_id = ? AND object_id = ? \
             ORDER BY storage_volume_id, placement_generation",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.iter().map(decode_v2_object_placement).collect()
    }

    async fn record_placement_health_observation(
        &self,
        observation: PlacementHealthObservation,
    ) -> CentralResult<PlacementHealthObservation> {
        observation.validate().map_err(protocol_invalid)?;
        let placement_row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain \
             FROM object_placements WHERE tenant_id = ? AND object_namespace_id = ? \
               AND placement_id = ? AND object_id = ? AND storage_volume_id = ? \
               AND placement_generation = ? LIMIT 1",
        )
        .bind(observation.tenant_id.as_str())
        .bind(observation.object_namespace_id.as_str())
        .bind(observation.placement_id.as_str())
        .bind(observation.object_id.as_bytes().as_slice())
        .bind(observation.storage_volume_id.as_str())
        .bind(v2_i64(
            observation.placement_generation.get(),
            "placement_generation",
        )?)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "integrity observation references an unknown placement",
            )
        })?;
        let placement = decode_v2_object_placement(&placement_row)?;
        if observation.state == PlacementHealthState::Healthy
            && observation.observed_size != placement.size
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "healthy integrity observation size differs from placement evidence",
            ));
        }

        let existing_row = sqlx::query(
            "SELECT payload, tenant_id, object_namespace_id, scan_id, placement_id, object_id, \
                    storage_volume_id, placement_generation, state, observed_size, observed_digest, \
                    observed_at_unix_ms, detail \
             FROM placement_health_observations \
             WHERE tenant_id = ? AND object_namespace_id = ? AND scan_id = ? AND placement_id = ? LIMIT 1",
        )
        .bind(observation.tenant_id.as_str())
        .bind(observation.object_namespace_id.as_str())
        .bind(observation.scan_id.as_str())
        .bind(observation.placement_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if let Some(row) = existing_row {
            let existing = decode_placement_health_observation(&row)?;
            if existing == observation {
                return Ok(existing);
            }
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "integrity scan observation identity is already bound to different metadata",
            )
            .with_retryable(false));
        }

        let latest_row = sqlx::query(
            "SELECT payload, tenant_id, object_namespace_id, scan_id, placement_id, object_id, \
                    storage_volume_id, placement_generation, state, observed_size, observed_digest, \
                    observed_at_unix_ms, detail \
             FROM placement_health_observations \
             WHERE tenant_id = ? AND object_namespace_id = ? AND placement_id = ? \
               AND placement_generation = ? \
             ORDER BY observed_at_unix_ms DESC, scan_id DESC LIMIT 1",
        )
        .bind(observation.tenant_id.as_str())
        .bind(observation.object_namespace_id.as_str())
        .bind(observation.placement_id.as_str())
        .bind(v2_i64(
            observation.placement_generation.get(),
            "placement_generation",
        )?)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if let Some(row) = latest_row {
            let latest = decode_placement_health_observation(&row)?;
            if observation.observed_at_unix_ms < latest.observed_at_unix_ms {
                return Ok(latest);
            }
            if observation.observed_at_unix_ms == latest.observed_at_unix_ms {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "integrity observations have conflicting timestamps",
                ));
            }
        }

        let payload = encode(&observation)?;
        sqlx::query(
            "INSERT INTO placement_health_observations \
             (tenant_id, object_namespace_id, scan_id, placement_id, object_id, storage_volume_id, \
              placement_generation, state, observed_size, observed_digest, observed_at_unix_ms, detail, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(observation.tenant_id.as_str())
        .bind(observation.object_namespace_id.as_str())
        .bind(observation.scan_id.as_str())
        .bind(observation.placement_id.as_str())
        .bind(observation.object_id.as_bytes().as_slice())
        .bind(observation.storage_volume_id.as_str())
        .bind(v2_i64(
            observation.placement_generation.get(),
            "placement_generation",
        )?)
        .bind(placement_health_state_name(observation.state))
        .bind(v2_i64(observation.observed_size.get(), "observed_size")?)
        .bind(observation.observed_digest.as_bytes().as_slice())
        .bind(v2_i64(
            observation.observed_at_unix_ms.get(),
            "observed_at_unix_ms",
        )?)
        .bind(observation.detail.as_deref())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(observation)
    }

    async fn latest_placement_health(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        placement_id: &PlacementId,
        placement_generation: PlacementGeneration,
    ) -> CentralResult<Option<PlacementHealthObservation>> {
        let row = sqlx::query(
            "SELECT payload, tenant_id, object_namespace_id, scan_id, placement_id, object_id, \
                    storage_volume_id, placement_generation, state, observed_size, observed_digest, \
                    observed_at_unix_ms, detail \
             FROM placement_health_observations \
             WHERE tenant_id = ? AND object_namespace_id = ? AND placement_id = ? \
               AND placement_generation = ? \
             ORDER BY observed_at_unix_ms DESC, scan_id DESC LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(placement_id.as_str())
        .bind(v2_i64(placement_generation.get(), "placement_generation")?)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.as_ref()
            .map(decode_placement_health_observation)
            .transpose()
    }

    async fn upsert_volume_commit_coverage(
        &self,
        coverage: VolumeCommitCoverage,
    ) -> CentralResult<VolumeCommitCoverage> {
        coverage.validate().map_err(protocol_invalid)?;
        let object_set = self
            .get_commit_object_set(&coverage.tenant_id, &coverage.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "coverage references an unknown Commit ObjectSet",
                )
            })?;
        coverage
            .validate_against(&object_set.object_set)
            .map_err(protocol_invalid)?;
        let placement_rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain \
             FROM object_placements \
             WHERE tenant_id = ? AND object_namespace_id = ? AND storage_volume_id = ? \
               AND placement_generation = ?",
        )
        .bind(coverage.tenant_id.as_str())
        .bind(coverage.object_namespace_id.as_str())
        .bind(coverage.storage_volume_id.as_str())
        .bind(v2_i64(
            coverage.placement_generation.get(),
            "placement_generation",
        )?)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        // Health observations are the latest evidence about whether a Placement is physically
        // readable. Coverage is derived from healthy evidence, not from the durable Placement
        // row alone; otherwise a scrub-reported missing object could not demote a cached summary.
        let mut placements = Vec::with_capacity(placement_rows.len());
        for row in &placement_rows {
            let placement = decode_v2_object_placement(row)?;
            let unhealthy = self
                .latest_placement_health(
                    &placement.tenant_id,
                    &coverage.object_namespace_id,
                    &placement.placement_id,
                    placement.placement_generation,
                )
                .await?
                .is_some_and(|observation| {
                    matches!(
                        observation.state,
                        PlacementHealthState::Missing | PlacementHealthState::Corrupt
                    )
                });
            if !unhealthy {
                placements.push(placement);
            }
        }
        let recomputed = VolumeCommitCoverage::from_placements(
            coverage.tenant_id.clone(),
            coverage.object_namespace_id.clone(),
            coverage.commit_id,
            coverage.storage_volume_id.clone(),
            coverage.placement_generation,
            &object_set.object_set,
            &placements,
        )
        .map_err(protocol_invalid)?;
        if coverage.object_set_digest != recomputed.object_set_digest
            || coverage.object_count != recomputed.object_count
            || coverage.verified_object_count != recomputed.verified_object_count
            || coverage.total_bytes != recomputed.total_bytes
            || coverage.verified_bytes != recomputed.verified_bytes
            || (matches!(
                coverage.state,
                neoengram_domain::protocol::materialization::CoverageState::Partial
                    | neoengram_domain::protocol::materialization::CoverageState::Complete
            ) && coverage.state != recomputed.state)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "coverage does not match verified object placement evidence",
            )
            .with_retryable(false));
        }
        let payload = encode(&coverage)?;
        let now = current_unix_ms()?;
        let result = sqlx::query(
            "INSERT INTO volume_commit_coverages \
             (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation, \
              object_set_digest, object_count, verified_object_count, total_bytes, verified_bytes, \
              state, created_at_unix_ms, updated_at_unix_ms, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation) \
             DO UPDATE SET object_set_digest = excluded.object_set_digest, \
                 object_count = excluded.object_count, verified_object_count = excluded.verified_object_count, \
                 total_bytes = excluded.total_bytes, verified_bytes = excluded.verified_bytes, \
                 state = excluded.state, updated_at_unix_ms = excluded.updated_at_unix_ms, payload = excluded.payload",
        )
        .bind(coverage.tenant_id.as_str())
        .bind(coverage.object_namespace_id.as_str())
        .bind(coverage.commit_id.digest().as_bytes().as_slice())
        .bind(coverage.storage_volume_id.as_str())
        .bind(v2_i64(coverage.placement_generation.get(), "placement_generation")?)
        .bind(coverage.object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(coverage.object_count.get(), "object_count")?)
        .bind(v2_i64(coverage.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(coverage.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(coverage.verified_bytes.get(), "verified_bytes")?)
        .bind(coverage_state_name(coverage.state))
        .bind(v2_i64(now.get(), "created_at_unix_ms")?)
        .bind(v2_i64(now.get(), "updated_at_unix_ms")?)
        .bind(payload)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(coverage),
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn volume_commit_coverages(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        commit_id: &ContentDigest,
    ) -> CentralResult<Vec<VolumeCommitCoverage>> {
        let rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation, object_set_digest, object_count, verified_object_count, total_bytes, verified_bytes \
             FROM volume_commit_coverages \
             WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? \
             ORDER BY storage_volume_id, placement_generation",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.iter().map(decode_v2_coverage).collect()
    }

    async fn insert_materialization(
        &self,
        job: MaterializationJob,
    ) -> CentralResult<MaterializationJob> {
        job.validate().map_err(protocol_invalid)?;
        let (goal, goal_value) = coverage_goal_parts(job.key.coverage_goal)?;
        if let Some(existing) = self
            .get_materialization_for_namespace(
                &job.key.tenant_id,
                &job.key.object_namespace_id,
                &job.materialization_id,
            )
            .await?
        {
            return if existing == job {
                Ok(existing)
            } else {
                Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization ID is already bound to different metadata",
                )
                .with_retryable(false))
            };
        }
        if let Some(existing) = self.get_materialization_by_key(&job.key).await? {
            if existing == job {
                return Ok(existing);
            }
            return Err(CentralError::new(
                if existing.state.terminal() {
                    CentralErrorCode::InvalidState
                } else {
                    CentralErrorCode::ReplicationAlreadyActive
                },
                "materialization idempotency key is already bound to different metadata",
            )
            .with_retryable(false));
        }
        if self
            .get_active_materialization_for_target(
                &job.key.tenant_id,
                &job.key.object_namespace_id,
                &job.key.commit_id,
                &job.key.target_storage_volume_id,
            )
            .await?
            .is_some()
        {
            return Err(CentralError::new(
                CentralErrorCode::ReplicationAlreadyActive,
                "a materialization for this Commit target is already active",
            )
            .with_retryable(false));
        }
        let object_set_digest = self
            .get_commit_object_set(&job.key.tenant_id, &job.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?
            .object_set
            .object_set_digest;
        let payload = encode(&job)?;
        let result = sqlx::query(
            "INSERT INTO materializations \
             (tenant_id, materialization_id, object_namespace_id, artifact_id, commit_id, \
              target_storage_volume_id, coverage_goal, coverage_goal_value, object_set_digest, plan_revision, state, \
              object_count, total_bytes, verified_object_count, verified_bytes, missing_object_count, missing_bytes, \
              source_count, deadline_unix_ms, request_id, payload, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(job.key.tenant_id.as_str())
        .bind(job.materialization_id.as_str())
        .bind(job.key.object_namespace_id.as_str())
        .bind(job.artifact_id.as_str())
        .bind(job.key.commit_id.digest().as_bytes().as_slice())
        .bind(job.key.target_storage_volume_id.as_str())
        .bind(goal)
        .bind(v2_i64(goal_value, "coverage_goal_value")?)
        .bind(object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(job.plan_revision.get(), "plan_revision")?)
        .bind(materialization_job_state_name(job.state))
        .bind(v2_i64(job.object_count.get(), "object_count")?)
        .bind(v2_i64(job.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(job.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(job.verified_bytes.get(), "verified_bytes")?)
        .bind(v2_i64(job.missing_object_count.get(), "missing_object_count")?)
        .bind(v2_i64(job.missing_bytes.get(), "missing_bytes")?)
        .bind(v2_i64(job.source_count.get(), "source_count")?)
        .bind(v2_i64(job.deadline_unix_ms.get(), "deadline_unix_ms")?)
        .bind(job.materialization_id.as_str())
        .bind(payload)
        .bind(v2_i64(job.created_at_unix_ms.get(), "created_at_unix_ms")?)
        .bind(v2_i64(job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(job),
            Err(error) if is_unique(&error) => {
                // The key and materialization ID are both unique.  A conflict on either must
                // return an exact replay only; silently returning a different Job would allow a
                // terminal row or another tenant's ID to be mistaken for this request.
                let existing = sqlx::query(
                    "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
                     FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? LIMIT 1",
                )
                .bind(job.key.tenant_id.as_str())
                .bind(job.key.object_namespace_id.as_str())
                .bind(job.materialization_id.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?
                .map(|row| decode_v2_materialization(&row))
                .transpose()?;
                if let Some(existing) = existing {
                    return if existing == job {
                        Ok(existing)
                    } else {
                        Err(CentralError::new(
                            CentralErrorCode::InvalidState,
                            "materialization ID is already bound to different metadata",
                        )
                        .with_retryable(false))
                    };
                }
                let existing = self
                    .get_materialization_by_key(&job.key)
                    .await?
                    .or(self
                        .get_active_materialization_for_target(
                            &job.key.tenant_id,
                            &job.key.object_namespace_id,
                            &job.key.commit_id,
                            &job.key.target_storage_volume_id,
                        )
                        .await?)
                    .ok_or_else(|| {
                        storage_corruption("materialization uniqueness conflict has no row")
                    })?;
                if existing == job {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        if existing.state.terminal() {
                            CentralErrorCode::InvalidState
                        } else {
                            CentralErrorCode::ReplicationAlreadyActive
                        },
                        "materialization idempotency key is already bound to different metadata",
                    ))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn insert_materialization_plan(
        &self,
        plan: MaterializationPlan,
    ) -> CentralResult<MaterializationPlanInsertOutcome> {
        // Serialize aggregate publication with receipt publication. Both paths mutate the
        // materialization/object/placement protection boundary and must not overwrite one another.
        let _gate = self.materialization_receipt_gate.lock().await;
        let MaterializationPlan {
            job,
            batches,
            objects,
            object_read_leases,
            staging_leases,
            coverage,
        } = plan;
        job.validate().map_err(protocol_invalid)?;
        coverage.validate().map_err(protocol_invalid)?;
        if coverage.tenant_id != job.key.tenant_id
            || coverage.object_namespace_id != job.key.object_namespace_id
            || coverage.commit_id != job.key.commit_id
            || coverage.storage_volume_id != job.key.target_storage_volume_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Coverage identity does not match its Job",
            )
            .with_retryable(false));
        }
        let object_set = self
            .get_commit_object_set(&job.key.tenant_id, &job.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        coverage
            .validate_against(&object_set.object_set)
            .map_err(protocol_invalid)?;
        let expected_objects = object_set
            .object_set
            .objects
            .iter()
            .map(|object| (object.object_id, object))
            .collect::<BTreeMap<_, _>>();
        let mut object_ids = BTreeSet::new();
        for object in &objects {
            object.validate().map_err(protocol_invalid)?;
            let expected = expected_objects
                .get(&object.object.object_id)
                .ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::InvalidState,
                        "materialization Object is not part of the Commit ObjectSet",
                    )
                    .with_retryable(false)
                })?;
            if object.materialization_id != job.materialization_id
                || object.plan_revision != job.plan_revision
                || object.object.object_namespace_id != job.key.object_namespace_id
                || object.object.object_id != expected.object_id
                || object.object.size != expected.size
                || object.object.encoding != expected.encoding
                || object.object.ordinal != expected.ordinal
                || !object_ids.insert(object.object.object_id)
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization Object does not match its Job fence or is duplicated",
                )
                .with_retryable(false));
            }
        }
        if object_ids.len() != expected_objects.len() {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization plan must include every Commit Object",
            )
            .with_retryable(false));
        }
        let mut batch_ids = BTreeSet::new();
        let mut assigned_objects = BTreeMap::new();
        for batch in &batches {
            batch.validate().map_err(protocol_invalid)?;
            if batch.materialization_id != job.materialization_id
                || batch.plan_revision != job.plan_revision
                || batch.target.tenant_id != job.key.tenant_id
                || batch.target.object_namespace_id != job.key.object_namespace_id
                || batch.target.storage_volume_id != job.key.target_storage_volume_id
                || !batch_ids.insert(batch.batch_id.clone())
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization Batch does not match its Job fence or is duplicated",
                )
                .with_retryable(false));
            }
            for object_id in &batch.object_ids {
                if !object_ids.contains(object_id)
                    || assigned_objects
                        .insert(*object_id, batch.batch_id.clone())
                        .is_some()
                {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "materialization Batch object list is invalid or overlaps another Batch",
                    )
                    .with_retryable(false));
                }
            }
        }
        for object in &objects {
            if let Some(batch_id) = &object.current_batch_id {
                if assigned_objects.get(&object.object.object_id) != Some(batch_id) {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "materialization Object current Batch does not match its plan",
                    )
                    .with_retryable(false));
                }
            }
        }
        let mut read_lease_ids = BTreeSet::new();
        for lease in &object_read_leases {
            lease.validate_for_acquisition().map_err(protocol_invalid)?;
            if lease.materialization_id != job.materialization_id
                || lease.plan_revision != job.plan_revision
                || lease.tenant_id != job.key.tenant_id
                || lease.object_namespace_id != job.key.object_namespace_id
                || !batch_ids.contains(&lease.batch_id)
                || !object_ids.contains(&lease.object_id)
                || !read_lease_ids.insert(lease.lease_id.clone())
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "object read lease does not match its materialization plan",
                )
                .with_retryable(false));
            }
            let batch = batches
                .iter()
                .find(|batch| batch.batch_id == lease.batch_id)
                .expect("batch ID was checked above");
            let Some(object) = objects.iter().find(|object| {
                object.object.object_id == lease.object_id
                    && object.current_batch_id.as_ref() == Some(&lease.batch_id)
            }) else {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "object read lease is not assigned to its Batch",
                )
                .with_retryable(false));
            };
            let source_selected = object.primary_source.as_ref() == Some(&lease.placement_id)
                || object.fallback_sources.contains(&lease.placement_id);
            if !batch.object_ids.contains(&lease.object_id) || !source_selected {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "object read lease placement is not selected for its object task",
                )
                .with_retryable(false));
            }
        }
        let mut staging_lease_ids = BTreeSet::new();
        for lease in &staging_leases {
            lease.validate_for_acquisition().map_err(protocol_invalid)?;
            if lease.materialization_id != job.materialization_id
                || lease.plan_revision != job.plan_revision
                || lease.tenant_id != job.key.tenant_id
                || lease.object_namespace_id != job.key.object_namespace_id
                || lease.target_storage_volume_id != job.key.target_storage_volume_id
                || !object_ids.contains(&lease.object_id)
                || !staging_lease_ids.insert(lease.lease_id.clone())
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "staging lease does not match its materialization plan",
                )
                .with_retryable(false));
            }
            let object = objects
                .iter()
                .find(|object| object.object.object_id == lease.object_id)
                .expect("object ID was checked above");
            lease
                .validate_against_object(object)
                .map_err(protocol_invalid)?;
        }
        for object in &objects {
            if object.complete() {
                continue;
            }
            let Some(batch_id) = &object.current_batch_id else {
                continue;
            };
            if !object_read_leases.iter().any(|lease| {
                lease.batch_id == *batch_id && lease.object_id == object.object.object_id
            }) || !staging_leases
                .iter()
                .any(|lease| lease.object_id == object.object.object_id)
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "every assigned materialization Object requires source and staging leases",
                )
                .with_retryable(false));
            }
        }

        let (goal, goal_value) = coverage_goal_parts(job.key.coverage_goal)?;
        let object_set_digest = object_set.object_set.object_set_digest;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let lease_volumes = materialization_read_lease_volumes(
            &mut transaction,
            &batches,
            &objects,
            &object_read_leases,
        )
        .await?;

        // The transaction is the publication boundary. Existing exact rows are accepted so a
        // retried request can repair an interrupted pre-v2 write without creating duplicates.
        let existing_job = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
             FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? LIMIT 1",
        )
        .bind(job.key.tenant_id.as_str())
        .bind(job.key.object_namespace_id.as_str())
        .bind(job.materialization_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(|row| decode_v2_materialization(&row))
        .transpose()?;
        if let Some(existing) = &existing_job {
            if existing != &job {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization ID is already bound to different metadata",
                )
                .with_retryable(false));
            }
        } else {
            let existing_key = sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
                 FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? \
                   AND target_storage_volume_id = ? AND coverage_goal = ? AND coverage_goal_value = ? LIMIT 1",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(job.key.commit_id.digest().as_bytes().as_slice())
            .bind(job.key.target_storage_volume_id.as_str())
            .bind(goal)
            .bind(v2_i64(goal_value, "coverage_goal_value")?)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(|row| decode_v2_materialization(&row))
            .transpose()?;
            if let Some(existing) = existing_key {
                return Err(CentralError::new(
                    if existing.state.terminal() {
                        CentralErrorCode::InvalidState
                    } else {
                        CentralErrorCode::ReplicationAlreadyActive
                    },
                    "materialization idempotency key is already bound to different metadata",
                )
                .with_retryable(false));
            }
            // The partial target index deliberately ignores coverage thresholds: a target may
            // have only one active Job for this Commit.  Check the broader identity here so a
            // concurrent insert with a different threshold is reported as a stable conflict
            // instead of leaking a raw SQLite uniqueness error.
            let active_target = sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
                 FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? \
                   AND target_storage_volume_id = ? AND state IN \
                   ('queued', 'planning', 'waiting_for_sources', 'materializing', 'verifying', 'stalled') \
                 ORDER BY updated_at_unix_ms DESC, materialization_id DESC LIMIT 1",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(job.key.commit_id.digest().as_bytes().as_slice())
            .bind(job.key.target_storage_volume_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(|row| decode_v2_materialization(&row))
            .transpose()?;
            if active_target.is_some() {
                return Err(CentralError::new(
                    CentralErrorCode::ReplicationAlreadyActive,
                    "a materialization for this Commit target is already active",
                )
                .with_retryable(false));
            }
            let payload = encode(&job)?;
            sqlx::query(
                "INSERT INTO materializations \
                 (tenant_id, materialization_id, object_namespace_id, artifact_id, commit_id, \
                  target_storage_volume_id, coverage_goal, coverage_goal_value, object_set_digest, plan_revision, state, \
                  object_count, total_bytes, verified_object_count, verified_bytes, missing_object_count, missing_bytes, \
                  source_count, deadline_unix_ms, request_id, payload, created_at_unix_ms, updated_at_unix_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.materialization_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(job.artifact_id.as_str())
            .bind(job.key.commit_id.digest().as_bytes().as_slice())
            .bind(job.key.target_storage_volume_id.as_str())
            .bind(goal)
            .bind(v2_i64(goal_value, "coverage_goal_value")?)
            .bind(object_set_digest.as_bytes().as_slice())
            .bind(v2_i64(job.plan_revision.get(), "plan_revision")?)
            .bind(materialization_job_state_name(job.state))
            .bind(v2_i64(job.object_count.get(), "object_count")?)
            .bind(v2_i64(job.total_bytes.get(), "total_bytes")?)
            .bind(v2_i64(job.verified_object_count.get(), "verified_object_count")?)
            .bind(v2_i64(job.verified_bytes.get(), "verified_bytes")?)
            .bind(v2_i64(job.missing_object_count.get(), "missing_object_count")?)
            .bind(v2_i64(job.missing_bytes.get(), "missing_bytes")?)
            .bind(v2_i64(job.source_count.get(), "source_count")?)
            .bind(v2_i64(job.deadline_unix_ms.get(), "deadline_unix_ms")?)
            .bind(job.materialization_id.as_str())
            .bind(payload)
            .bind(v2_i64(job.created_at_unix_ms.get(), "created_at_unix_ms")?)
            .bind(v2_i64(job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                if is_unique(&error) {
                    CentralError::new(
                        CentralErrorCode::ReplicationAlreadyActive,
                        "a materialization for this Commit target is already active",
                    )
                    .with_retryable(false)
                } else {
                    storage_error(error)
                }
            })?;
        }

        for batch in &batches {
            let existing = sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id \
                 FROM materialization_batches WHERE tenant_id = ? AND object_namespace_id = ? AND batch_id = ? LIMIT 1",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(batch.batch_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(|row| decode_v2_batch(&row))
            .transpose()?;
            if let Some(existing) = existing {
                if existing != *batch {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "materialization Batch ID is already bound to different metadata",
                    )
                    .with_retryable(false));
                }
                continue;
            }
            let payload = encode(batch)?;
            sqlx::query(
                "INSERT INTO materialization_batches \
                 (tenant_id, object_namespace_id, batch_id, materialization_id, plan_revision, attempt, source_storage_volume_id, \
                  target_storage_volume_id, manifest_digest, state, payload, created_at_unix_ms, updated_at_unix_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(batch.batch_id.as_str())
            .bind(batch.materialization_id.as_str())
            .bind(v2_i64(batch.plan_revision.get(), "plan_revision")?)
            .bind(v2_i64(batch.batch_attempt.get(), "batch_attempt")?)
            .bind(batch.source.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
            .bind(batch.target.storage_volume_id.as_str())
            .bind(batch.manifest_digest.as_bytes().as_slice())
            .bind(materialization_batch_state_name(batch.state))
            .bind(payload)
            .bind(v2_i64(job.created_at_unix_ms.get(), "created_at_unix_ms")?)
            .bind(v2_i64(job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        for object in &objects {
            let existing = sqlx::query(
                "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt \
                 FROM materialization_objects WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ? LIMIT 1",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.materialization_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(object.object.object_id.as_bytes().as_slice())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(|row| decode_v2_object(&row))
            .transpose()?;
            if let Some(existing) = existing {
                if existing != *object {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "materialization Object ID is already bound to different metadata",
                    )
                    .with_retryable(false));
                }
                continue;
            }
            let payload = encode(object)?;
            sqlx::query(
                "INSERT INTO materialization_objects \
                 (tenant_id, materialization_id, object_namespace_id, object_id, size, encoding, \
                  staging_key, confirmed_offset, state, current_batch_id, plan_revision, attempt, \
                  payload, created_at_unix_ms, updated_at_unix_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(object.materialization_id.as_str())
            .bind(object.object.object_namespace_id.as_str())
            .bind(object.object.object_id.as_bytes().as_slice())
            .bind(v2_i64(object.object.size.get(), "object size")?)
            .bind(object_encoding_name(object.object.encoding))
            .bind(&object.staging_key)
            .bind(v2_i64(object.confirmed_offset.get(), "confirmed_offset")?)
            .bind(materialization_object_state_name(object.state))
            .bind(object.current_batch_id.as_ref().map(|id| id.as_str()))
            .bind(v2_i64(object.plan_revision.get(), "plan_revision")?)
            .bind(v2_i64(object.attempt.get(), "attempt")?)
            .bind(payload)
            .bind(v2_i64(job.created_at_unix_ms.get(), "created_at_unix_ms")?)
            .bind(v2_i64(job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        for lease in &object_read_leases {
            let existing = sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, materialization_id, batch_id, object_id, placement_id, placement_generation, expires_at_unix_ms \
                 FROM object_read_leases WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ? LIMIT 1",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(lease.lease_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(|row| decode_v2_read_lease(&row))
            .transpose()?;
            if let Some(existing) = existing {
                if existing != *lease {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "object read lease ID is already in use",
                    )
                    .with_retryable(false));
                }
                continue;
            }
            let storage_volume_id = lease_volumes
                .get(lease.lease_id.as_str())
                .ok_or_else(|| storage_corruption("object read lease source volume is missing"))?;
            let payload = encode(lease)?;
            sqlx::query(
                "INSERT INTO object_read_leases \
                 (tenant_id, lease_id, materialization_id, batch_id, object_namespace_id, object_id, \
                  placement_id, storage_volume_id, placement_generation, expires_at_unix_ms, state, payload) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(lease.tenant_id.as_str())
            .bind(lease.lease_id.as_str())
            .bind(lease.materialization_id.as_str())
            .bind(lease.batch_id.as_str())
            .bind(lease.object_namespace_id.as_str())
            .bind(lease.object_id.as_bytes().as_slice())
            .bind(lease.placement_id.as_str())
            .bind(storage_volume_id.as_str())
            .bind(v2_i64(lease.placement_generation.get(), "placement_generation")?)
            .bind(v2_i64(lease.expires_at_unix_ms.get(), "expires_at_unix_ms")?)
            .bind(materialization_lease_state_name(lease.state))
            .bind(payload)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        for lease in &staging_leases {
            let existing = sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, object_id, staging_key, target_placement_generation, expires_at_unix_ms \
                 FROM staging_leases WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ? LIMIT 1",
            )
            .bind(job.key.tenant_id.as_str())
            .bind(job.key.object_namespace_id.as_str())
            .bind(lease.lease_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .map(|row| decode_v2_staging_lease(&row))
            .transpose()?;
            if let Some(existing) = existing {
                if existing != *lease {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "staging lease ID is already in use",
                    )
                    .with_retryable(false));
                }
                continue;
            }
            let payload = encode(lease)?;
            sqlx::query(
                "INSERT INTO staging_leases \
                 (tenant_id, lease_id, materialization_id, object_namespace_id, object_id, target_storage_volume_id, \
                  target_placement_generation, staging_key, expires_at_unix_ms, state, payload) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(lease.tenant_id.as_str())
            .bind(lease.lease_id.as_str())
            .bind(lease.materialization_id.as_str())
            .bind(lease.object_namespace_id.as_str())
            .bind(lease.object_id.as_bytes().as_slice())
            .bind(lease.target_storage_volume_id.as_str())
            .bind(v2_i64(lease.target_placement_generation.get(), "target_placement_generation")?)
            .bind(&lease.staging_key)
            .bind(v2_i64(lease.expires_at_unix_ms.get(), "expires_at_unix_ms")?)
            .bind(materialization_lease_state_name(lease.state))
            .bind(payload)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }

        // Coverage remains a derived summary, but it is published in this same transaction so a
        // reader can never observe a new Job and stale coverage (or vice versa).
        let placement_rows = sqlx::query(
            "SELECT p.payload, p.state, p.tenant_id, p.object_namespace_id, p.placement_id, p.object_id, p.size, p.encoding, p.verified_digest, p.storage_volume_id, p.placement_generation, p.failure_domain \
             FROM object_placements p WHERE p.tenant_id = ? AND p.object_namespace_id = ? AND p.storage_volume_id = ? AND p.placement_generation = ? \
               AND NOT EXISTS (SELECT 1 FROM placement_health_observations h \
                 WHERE h.tenant_id = p.tenant_id AND h.object_namespace_id = p.object_namespace_id \
                   AND h.placement_id = p.placement_id AND h.placement_generation = p.placement_generation \
                   AND h.observed_at_unix_ms = (SELECT MAX(h2.observed_at_unix_ms) FROM placement_health_observations h2 \
                     WHERE h2.tenant_id = p.tenant_id AND h2.object_namespace_id = p.object_namespace_id \
                       AND h2.placement_id = p.placement_id AND h2.placement_generation = p.placement_generation) \
                   AND h.state IN ('missing', 'corrupt'))",
        )
        .bind(coverage.tenant_id.as_str())
        .bind(coverage.object_namespace_id.as_str())
        .bind(coverage.storage_volume_id.as_str())
        .bind(v2_i64(coverage.placement_generation.get(), "placement_generation")?)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let placements = placement_rows
            .iter()
            .map(decode_v2_object_placement)
            .collect::<CentralResult<Vec<_>>>()?;
        let recomputed = VolumeCommitCoverage::from_placements(
            coverage.tenant_id.clone(),
            coverage.object_namespace_id.clone(),
            coverage.commit_id,
            coverage.storage_volume_id.clone(),
            coverage.placement_generation,
            &object_set.object_set,
            &placements,
        )
        .map_err(protocol_invalid)?;
        if coverage.object_set_digest != recomputed.object_set_digest
            || coverage.object_count != recomputed.object_count
            || coverage.verified_object_count != recomputed.verified_object_count
            || coverage.total_bytes != recomputed.total_bytes
            || coverage.verified_bytes != recomputed.verified_bytes
            || (matches!(
                coverage.state,
                neoengram_domain::protocol::materialization::CoverageState::Partial
                    | neoengram_domain::protocol::materialization::CoverageState::Complete
            ) && coverage.state != recomputed.state)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Coverage does not match Placement evidence",
            )
            .with_retryable(false));
        }
        let payload = encode(&coverage)?;
        sqlx::query(
            "INSERT INTO volume_commit_coverages \
             (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation, \
             object_set_digest, object_count, verified_object_count, total_bytes, verified_bytes, \
             state, created_at_unix_ms, updated_at_unix_ms, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation) \
             DO UPDATE SET object_set_digest = excluded.object_set_digest, object_count = excluded.object_count, \
                 verified_object_count = excluded.verified_object_count, total_bytes = excluded.total_bytes, \
                 verified_bytes = excluded.verified_bytes, state = excluded.state, \
                 updated_at_unix_ms = excluded.updated_at_unix_ms, payload = excluded.payload",
        )
        .bind(coverage.tenant_id.as_str())
        .bind(coverage.object_namespace_id.as_str())
        .bind(coverage.commit_id.digest().as_bytes().as_slice())
        .bind(coverage.storage_volume_id.as_str())
        .bind(v2_i64(coverage.placement_generation.get(), "placement_generation")?)
        .bind(coverage.object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(coverage.object_count.get(), "object_count")?)
        .bind(v2_i64(coverage.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(coverage.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(coverage.verified_bytes.get(), "verified_bytes")?)
        .bind(coverage_state_name(coverage.state))
        .bind(v2_i64(job.created_at_unix_ms.get(), "created_at_unix_ms")?)
        .bind(v2_i64(job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(payload)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(if existing_job.is_some() {
            MaterializationPlanInsertOutcome::Existing(job)
        } else {
            MaterializationPlanInsertOutcome::Inserted(job)
        })
    }

    async fn replace_materialization_plan(
        &self,
        request: MaterializationPlanReplacement,
    ) -> CentralResult<MaterializationPlanInsertOutcome> {
        // Serialize replanning with receipt publication. Replanning retires leases and replaces
        // the active Batch fence, so it must share the same aggregate publication boundary.
        let _gate = self.materialization_receipt_gate.lock().await;
        let expected_revision = request.expected_plan_revision;
        let plan = request.plan;
        let next_revision = expected_revision
            .get()
            .checked_add(1)
            .map(Generation::new)
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization plan revision is exhausted",
                )
            })?;
        plan.job.validate().map_err(protocol_invalid)?;
        plan.coverage.validate().map_err(protocol_invalid)?;
        if plan.job.plan_revision != next_revision {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replacement materialization plan must advance exactly one revision",
            )
            .with_retryable(false));
        }
        if plan.coverage.tenant_id != plan.job.key.tenant_id
            || plan.coverage.object_namespace_id != plan.job.key.object_namespace_id
            || plan.coverage.commit_id != plan.job.key.commit_id
            || plan.coverage.storage_volume_id != plan.job.key.target_storage_volume_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "replacement Coverage identity does not match its Job",
            )
            .with_retryable(false));
        }
        let object_set = self
            .get_commit_object_set(&plan.job.key.tenant_id, &plan.job.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        plan.coverage
            .validate_against(&object_set.object_set)
            .map_err(protocol_invalid)?;
        validate_materialization_plan_shape(&plan, &object_set.object_set)?;

        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let lease_volumes = materialization_read_lease_volumes(
            &mut transaction,
            &plan.batches,
            &plan.objects,
            &plan.object_read_leases,
        )
        .await?;
        let current = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
             FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? LIMIT 1",
        )
        .bind(plan.job.key.tenant_id.as_str())
        .bind(plan.job.key.object_namespace_id.as_str())
        .bind(plan.job.materialization_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(|row| decode_v2_materialization(&row))
        .transpose()?;
        let current = current.ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "materialization not found",
            )
        })?;
        if current.plan_revision != expected_revision {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision changed",
            ));
        }
        if current.key != plan.job.key || current.materialization_id != plan.job.materialization_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "replacement materialization identity is invalid",
            )
            .with_retryable(false));
        }
        if !materialization_state_transition_allowed(current.state, plan.job.state) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "replacement materialization state transition is invalid",
            )
            .with_retryable(false));
        }

        // Read and validate every existing child before changing the parent row. This makes
        // malformed or stale replacements fail without even advancing the Job CAS fence.
        let old_object_rows = sqlx::query(
            "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt \
             FROM materialization_objects WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? \
             ORDER BY object_id",
        )
        .bind(plan.job.key.tenant_id.as_str())
        .bind(plan.job.materialization_id.as_str())
        .bind(plan.job.key.object_namespace_id.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let old_objects = old_object_rows
            .iter()
            .map(decode_v2_object)
            .collect::<CentralResult<Vec<_>>>()?;
        let old_by_id = old_objects
            .iter()
            .map(|object| (object.object.object_id, object))
            .collect::<BTreeMap<_, _>>();
        for object in &plan.objects {
            let previous = old_by_id.get(&object.object.object_id).ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization replacement is missing an existing Object row",
                )
            })?;
            if previous.plan_revision != expected_revision {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object belongs to a different plan revision",
                ));
            }
            if previous.object != object.object || previous.staging_key != object.staging_key {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization Object identity or staging key cannot change",
                )
                .with_retryable(false));
            }
            // A completed target may be reset only for an integrity repair plan: the parent must
            // reopen through Planning, and the new task must start at byte zero.
            let integrity_repair_reset = current.state == MaterializationJobState::Complete
                && plan.job.state != MaterializationJobState::Complete
                && previous.complete()
                && object.confirmed_offset.get() == 0
                && matches!(
                    object.state,
                    MaterializationObjectState::Missing | MaterializationObjectState::Reserved
                );
            if object.confirmed_offset < previous.confirmed_offset && !integrity_repair_reset {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object confirmed offset cannot move backwards",
                ));
            }
            if previous.complete() && !object.complete() && !integrity_repair_reset {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "completed materialization Object cannot regress",
                )
                .with_retryable(false));
            }
            let expected_attempt = previous.attempt.get().checked_add(1).ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object attempt is exhausted",
                )
            })?;
            if object.attempt.get() != expected_attempt {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object attempt must advance exactly one step",
                ));
            }
        }

        let payload = encode(&plan.job)?;
        let result = sqlx::query(
            "UPDATE materializations SET plan_revision = ?, state = ?, object_set_digest = ?, \
                 object_count = ?, total_bytes = ?, verified_object_count = ?, verified_bytes = ?, \
                 missing_object_count = ?, missing_bytes = ?, source_count = ?, deadline_unix_ms = ?, \
                 payload = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND plan_revision = ?",
        )
        .bind(v2_i64(plan.job.plan_revision.get(), "plan_revision")?)
        .bind(materialization_job_state_name(plan.job.state))
        .bind(object_set.object_set.object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(plan.job.object_count.get(), "object_count")?)
        .bind(v2_i64(plan.job.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(plan.job.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(plan.job.verified_bytes.get(), "verified_bytes")?)
        .bind(v2_i64(plan.job.missing_object_count.get(), "missing_object_count")?)
        .bind(v2_i64(plan.job.missing_bytes.get(), "missing_bytes")?)
        .bind(v2_i64(plan.job.source_count.get(), "source_count")?)
        .bind(v2_i64(plan.job.deadline_unix_ms.get(), "deadline_unix_ms")?)
        .bind(payload)
        .bind(v2_i64(plan.job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(plan.job.key.tenant_id.as_str())
        .bind(plan.job.key.object_namespace_id.as_str())
        .bind(plan.job.materialization_id.as_str())
        .bind(v2_i64(expected_revision.get(), "expected_plan_revision")?)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() != 1 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision changed",
            ));
        }

        // Decode active batches and leases before retirement so their payload state remains in
        // lockstep with the indexed state column. Every mutation below is part of this transaction.
        let old_batch_rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id \
             FROM materialization_batches WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? \
             ORDER BY batch_id",
        )
        .bind(plan.job.key.tenant_id.as_str())
        .bind(plan.job.key.object_namespace_id.as_str())
        .bind(plan.job.materialization_id.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let old_batches = old_batch_rows
            .iter()
            .map(decode_v2_batch)
            .collect::<CentralResult<Vec<_>>>()?;
        let old_read_rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, materialization_id, batch_id, object_id, placement_id, placement_generation, expires_at_unix_ms \
             FROM object_read_leases WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? \
             ORDER BY lease_id",
        )
        .bind(plan.job.key.tenant_id.as_str())
        .bind(plan.job.key.object_namespace_id.as_str())
        .bind(plan.job.materialization_id.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let old_read_leases = old_read_rows
            .iter()
            .map(decode_v2_read_lease)
            .collect::<CentralResult<Vec<_>>>()?;
        let old_staging_rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, object_id, staging_key, target_placement_generation, expires_at_unix_ms \
             FROM staging_leases WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? \
             ORDER BY lease_id",
        )
        .bind(plan.job.key.tenant_id.as_str())
        .bind(plan.job.key.object_namespace_id.as_str())
        .bind(plan.job.materialization_id.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let old_staging_leases = old_staging_rows
            .iter()
            .map(decode_v2_staging_lease)
            .collect::<CentralResult<Vec<_>>>()?;

        for old in old_batches.iter().filter(|batch| {
            !matches!(
                batch.state,
                MaterializationBatchState::Succeeded | MaterializationBatchState::Failed
            )
        }) {
            let mut retired = old.clone();
            retired.state = MaterializationBatchState::Failed;
            let payload = encode(&retired)?;
            let result = sqlx::query(
                "UPDATE materialization_batches SET state = ?, payload = ?, updated_at_unix_ms = ? \
                 WHERE tenant_id = ? AND object_namespace_id = ? AND batch_id = ? \
                   AND plan_revision = ? AND attempt = ? AND state = ?",
            )
            .bind(materialization_batch_state_name(retired.state))
            .bind(payload)
            .bind(v2_i64(
                plan.job.updated_at_unix_ms.get(),
                "updated_at_unix_ms",
            )?)
            .bind(old.target.tenant_id.as_str())
            .bind(old.target.object_namespace_id.as_str())
            .bind(old.batch_id.as_str())
            .bind(v2_i64(old.plan_revision.get(), "plan_revision")?)
            .bind(v2_i64(old.batch_attempt.get(), "attempt")?)
            .bind(materialization_batch_state_name(old.state))
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if result.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Batch changed during plan replacement",
                ));
            }
        }
        for old in old_read_leases
            .iter()
            .filter(|lease| lease.state == MaterializationLeaseState::Active)
        {
            let mut retired = old.clone();
            retired.state = MaterializationLeaseState::Released;
            let payload = encode(&retired)?;
            let result = sqlx::query(
                "UPDATE object_read_leases SET state = ?, payload = ? \
                 WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ? AND state = ?",
            )
            .bind(materialization_lease_state_name(retired.state))
            .bind(payload)
            .bind(old.tenant_id.as_str())
            .bind(old.object_namespace_id.as_str())
            .bind(old.lease_id.as_str())
            .bind(materialization_lease_state_name(old.state))
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if result.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "object read lease changed during plan replacement",
                ));
            }
        }
        for old in old_staging_leases
            .iter()
            .filter(|lease| lease.state == MaterializationLeaseState::Active)
        {
            let mut retired = old.clone();
            retired.state = MaterializationLeaseState::Released;
            let payload = encode(&retired)?;
            let result = sqlx::query(
                "UPDATE staging_leases SET state = ?, payload = ? \
                 WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ? AND state = ?",
            )
            .bind(materialization_lease_state_name(retired.state))
            .bind(payload)
            .bind(old.tenant_id.as_str())
            .bind(old.object_namespace_id.as_str())
            .bind(old.lease_id.as_str())
            .bind(materialization_lease_state_name(old.state))
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if result.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "staging lease changed during plan replacement",
                ));
            }
        }

        // Child identities include plan revision/attempt in the payload. New batches and leases
        // are inserted, while stable object rows are advanced with a fenced update.
        for batch in &plan.batches {
            let payload = encode(batch)?;
            sqlx::query(
                "INSERT INTO materialization_batches (tenant_id, object_namespace_id, batch_id, materialization_id, plan_revision, attempt, source_storage_volume_id, target_storage_volume_id, manifest_digest, state, payload, created_at_unix_ms, updated_at_unix_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(plan.job.key.tenant_id.as_str())
            .bind(plan.job.key.object_namespace_id.as_str())
            .bind(batch.batch_id.as_str())
            .bind(batch.materialization_id.as_str())
            .bind(v2_i64(batch.plan_revision.get(), "plan_revision")?)
            .bind(v2_i64(batch.batch_attempt.get(), "batch_attempt")?)
            .bind(batch.source.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
            .bind(batch.target.storage_volume_id.as_str())
            .bind(batch.manifest_digest.as_bytes().as_slice())
            .bind(materialization_batch_state_name(batch.state))
            .bind(payload)
            .bind(v2_i64(plan.job.created_at_unix_ms.get(), "created_at_unix_ms")?)
            .bind(v2_i64(plan.job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        for object in &plan.objects {
            let payload = encode(object)?;
            let previous = old_by_id
                .get(&object.object.object_id)
                .expect("object rows were validated above");
            let result = sqlx::query(
                "UPDATE materialization_objects SET size = ?, encoding = ?, staging_key = ?, \
                 confirmed_offset = ?, state = ?, current_batch_id = ?, plan_revision = ?, attempt = ?, payload = ?, updated_at_unix_ms = ? \
                 WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ? \
                   AND plan_revision = ? AND attempt = ?",
            )
            .bind(v2_i64(object.object.size.get(), "object size")?)
            .bind(object_encoding_name(object.object.encoding))
            .bind(&object.staging_key)
            .bind(v2_i64(object.confirmed_offset.get(), "confirmed_offset")?)
            .bind(materialization_object_state_name(object.state))
            .bind(object.current_batch_id.as_ref().map(|id| id.as_str()))
            .bind(v2_i64(object.plan_revision.get(), "plan_revision")?)
            .bind(v2_i64(object.attempt.get(), "attempt")?)
            .bind(payload)
            .bind(v2_i64(plan.job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
            .bind(plan.job.key.tenant_id.as_str())
            .bind(object.materialization_id.as_str())
            .bind(object.object.object_namespace_id.as_str())
            .bind(object.object.object_id.as_bytes().as_slice())
            .bind(v2_i64(previous.plan_revision.get(), "expected_plan_revision")?)
            .bind(v2_i64(previous.attempt.get(), "expected_attempt")?)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if result.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization Object changed during plan replacement",
                ));
            }
        }
        for lease in &plan.object_read_leases {
            let payload = encode(lease)?;
            let storage_volume_id =
                lease_volumes.get(lease.lease_id.as_str()).ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::InvalidState,
                        "object read lease source must reference a StorageVolume",
                    )
                    .with_retryable(false)
                })?;
            sqlx::query(
                "INSERT INTO object_read_leases (tenant_id, lease_id, materialization_id, batch_id, object_namespace_id, object_id, placement_id, storage_volume_id, placement_generation, expires_at_unix_ms, state, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(lease.tenant_id.as_str())
            .bind(lease.lease_id.as_str())
            .bind(lease.materialization_id.as_str())
            .bind(lease.batch_id.as_str())
            .bind(lease.object_namespace_id.as_str())
            .bind(lease.object_id.as_bytes().as_slice())
            .bind(lease.placement_id.as_str())
            .bind(storage_volume_id.as_str())
            .bind(v2_i64(lease.placement_generation.get(), "placement_generation")?)
            .bind(v2_i64(lease.expires_at_unix_ms.get(), "expires_at_unix_ms")?)
            .bind(materialization_lease_state_name(lease.state))
            .bind(payload)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        for lease in &plan.staging_leases {
            let payload = encode(lease)?;
            sqlx::query(
                "INSERT INTO staging_leases (tenant_id, lease_id, materialization_id, object_namespace_id, object_id, target_storage_volume_id, target_placement_generation, staging_key, expires_at_unix_ms, state, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(lease.tenant_id.as_str())
            .bind(lease.lease_id.as_str())
            .bind(lease.materialization_id.as_str())
            .bind(lease.object_namespace_id.as_str())
            .bind(lease.object_id.as_bytes().as_slice())
            .bind(lease.target_storage_volume_id.as_str())
            .bind(v2_i64(lease.target_placement_generation.get(), "target_placement_generation")?)
            .bind(&lease.staging_key)
            .bind(v2_i64(lease.expires_at_unix_ms.get(), "expires_at_unix_ms")?)
            .bind(materialization_lease_state_name(lease.state))
            .bind(payload)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        let placement_rows = sqlx::query(
            "SELECT p.payload, p.state, p.tenant_id, p.object_namespace_id, p.placement_id, p.object_id, p.size, p.encoding, p.verified_digest, p.storage_volume_id, p.placement_generation, p.failure_domain FROM object_placements p WHERE p.tenant_id = ? AND p.object_namespace_id = ? AND p.storage_volume_id = ? AND p.placement_generation = ? \
               AND NOT EXISTS (SELECT 1 FROM placement_health_observations h \
                 WHERE h.tenant_id = p.tenant_id AND h.object_namespace_id = p.object_namespace_id \
                   AND h.placement_id = p.placement_id AND h.placement_generation = p.placement_generation \
                   AND h.observed_at_unix_ms = (SELECT MAX(h2.observed_at_unix_ms) FROM placement_health_observations h2 \
                     WHERE h2.tenant_id = p.tenant_id AND h2.object_namespace_id = p.object_namespace_id \
                       AND h2.placement_id = p.placement_id AND h2.placement_generation = p.placement_generation) \
                   AND h.state IN ('missing', 'corrupt'))",
        )
        .bind(plan.coverage.tenant_id.as_str())
        .bind(plan.coverage.object_namespace_id.as_str())
        .bind(plan.coverage.storage_volume_id.as_str())
        .bind(v2_i64(plan.coverage.placement_generation.get(), "placement_generation")?)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let placements = placement_rows
            .iter()
            .map(decode_v2_object_placement)
            .collect::<CentralResult<Vec<_>>>()?;
        let recomputed = VolumeCommitCoverage::from_placements(
            plan.coverage.tenant_id.clone(),
            plan.coverage.object_namespace_id.clone(),
            plan.coverage.commit_id,
            plan.coverage.storage_volume_id.clone(),
            plan.coverage.placement_generation,
            &object_set.object_set,
            &placements,
        )
        .map_err(protocol_invalid)?;
        if recomputed != plan.coverage {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "replacement Coverage does not match Placement evidence",
            )
            .with_retryable(false));
        }
        let coverage_payload = encode(&plan.coverage)?;
        sqlx::query(
            "INSERT INTO volume_commit_coverages (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation, object_set_digest, object_count, verified_object_count, total_bytes, verified_bytes, state, created_at_unix_ms, updated_at_unix_ms, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation) DO UPDATE SET object_set_digest = excluded.object_set_digest, object_count = excluded.object_count, verified_object_count = excluded.verified_object_count, total_bytes = excluded.total_bytes, verified_bytes = excluded.verified_bytes, state = excluded.state, updated_at_unix_ms = excluded.updated_at_unix_ms, payload = excluded.payload",
        )
        .bind(plan.coverage.tenant_id.as_str())
        .bind(plan.coverage.object_namespace_id.as_str())
        .bind(plan.coverage.commit_id.digest().as_bytes().as_slice())
        .bind(plan.coverage.storage_volume_id.as_str())
        .bind(v2_i64(plan.coverage.placement_generation.get(), "placement_generation")?)
        .bind(plan.coverage.object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(plan.coverage.object_count.get(), "object_count")?)
        .bind(v2_i64(plan.coverage.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(plan.coverage.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(plan.coverage.verified_bytes.get(), "verified_bytes")?)
        .bind(coverage_state_name(plan.coverage.state))
        .bind(v2_i64(plan.job.created_at_unix_ms.get(), "created_at_unix_ms")?)
        .bind(v2_i64(plan.job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(coverage_payload)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(MaterializationPlanInsertOutcome::Inserted(plan.job))
    }

    async fn get_materialization(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Option<MaterializationJob>> {
        sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
             FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(materialization_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(|row| decode_v2_materialization(&row))
        .transpose()
    }

    async fn get_materialization_by_key(
        &self,
        key: &MaterializationJobKey,
    ) -> CentralResult<Option<MaterializationJob>> {
        let (goal, goal_value) = coverage_goal_parts(key.coverage_goal)?;
        sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
             FROM materializations \
             WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? \
               AND target_storage_volume_id = ? AND coverage_goal = ? AND coverage_goal_value = ? \
             ORDER BY updated_at_unix_ms DESC LIMIT 1",
        )
        .bind(key.tenant_id.as_str())
        .bind(key.object_namespace_id.as_str())
        .bind(key.commit_id.digest().as_bytes().as_slice())
        .bind(key.target_storage_volume_id.as_str())
        .bind(goal)
        .bind(v2_i64(goal_value, "coverage_goal_value")?)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(|row| decode_v2_materialization(&row))
        .transpose()
    }

    async fn list_materializations(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        commit_id: &ContentDigest,
        target_storage_volume_id: Option<&StorageVolumeId>,
    ) -> CentralResult<Vec<MaterializationJob>> {
        let rows = if let Some(target) = target_storage_volume_id {
            sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
                 FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? \
                   AND target_storage_volume_id = ? ORDER BY materialization_id",
            )
            .bind(tenant_id.as_str())
            .bind(object_namespace_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .bind(target.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        } else {
            sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, artifact_id, commit_id, coverage_goal, coverage_goal_value \
                 FROM materializations WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? \
                 ORDER BY materialization_id",
            )
            .bind(tenant_id.as_str())
            .bind(object_namespace_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
        };
        rows.iter().map(decode_v2_materialization).collect()
    }

    async fn replace_materialization(
        &self,
        tenant_id: &TenantId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
        expected_plan_revision: neoengram_domain::protocol::Generation,
        job: MaterializationJob,
    ) -> CentralResult<MaterializationJob> {
        // Receipt publication updates the same Job/object/Placement aggregate while holding this
        // gate. Serialize ordinary Job state transitions with that boundary as well; otherwise a
        // concurrent failure/cancel update could validate an old revision and overwrite receipt
        // progress after the receipt transaction commits.
        let _gate = self.materialization_receipt_gate.lock().await;
        job.validate().map_err(protocol_invalid)?;
        let _ = coverage_goal_parts(job.key.coverage_goal)?;
        if &job.key.tenant_id != tenant_id || &job.materialization_id != materialization_id {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization replacement identity does not match its key",
            )
            .with_retryable(false));
        }
        if job.plan_revision < expected_plan_revision {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision moved backwards",
            ));
        }
        let current = self
            .get_materialization_for_namespace(
                tenant_id,
                &job.key.object_namespace_id,
                materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if current.key != job.key {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization replacement cannot change its immutable target key",
            )
            .with_retryable(false));
        }
        if !current.state.can_transition_to(job.state) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization state transition is not allowed",
            )
            .with_retryable(false));
        }
        if job.plan_revision > Generation::new(expected_plan_revision.get().saturating_add(1)) {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision advanced by more than one",
            ));
        }
        let object_set_digest = self
            .get_commit_object_set(&job.key.tenant_id, &job.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?
            .object_set
            .object_set_digest;
        let payload = encode(&job)?;
        let result = sqlx::query(
            "UPDATE materializations SET plan_revision = ?, state = ?, object_set_digest = ?, \
                 object_count = ?, total_bytes = ?, verified_object_count = ?, verified_bytes = ?, \
                 missing_object_count = ?, missing_bytes = ?, source_count = ?, deadline_unix_ms = ?, \
                 payload = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND plan_revision = ?",
        )
        .bind(v2_i64(job.plan_revision.get(), "plan_revision")?)
        .bind(materialization_job_state_name(job.state))
        .bind(object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(job.object_count.get(), "object_count")?)
        .bind(v2_i64(job.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(job.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(job.verified_bytes.get(), "verified_bytes")?)
        .bind(v2_i64(job.missing_object_count.get(), "missing_object_count")?)
        .bind(v2_i64(job.missing_bytes.get(), "missing_bytes")?)
        .bind(v2_i64(job.source_count.get(), "source_count")?)
        .bind(v2_i64(job.deadline_unix_ms.get(), "deadline_unix_ms")?)
        .bind(payload)
        .bind(v2_i64(job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(tenant_id.as_str())
        .bind(job.key.object_namespace_id.as_str())
        .bind(materialization_id.as_str())
        .bind(v2_i64(expected_plan_revision.get(), "expected_plan_revision")?)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization plan revision changed",
            ));
        }
        Ok(job)
    }

    async fn insert_materialization_batch(
        &self,
        batch: MaterializationBatch,
    ) -> CentralResult<MaterializationBatch> {
        batch.validate().map_err(protocol_invalid)?;
        let parent = self
            .get_materialization(
                &batch.target.tenant_id,
                &batch.target.object_namespace_id,
                &batch.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != batch.target.object_namespace_id
            || parent.key.target_storage_volume_id != batch.target.storage_volume_id
            || parent.plan_revision != batch.plan_revision
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "batch target does not match its materialization",
            )
            .with_retryable(false));
        }
        let payload = encode(&batch)?;
        let result = sqlx::query(
            "INSERT INTO materialization_batches \
             (tenant_id, object_namespace_id, batch_id, materialization_id, plan_revision, attempt, source_storage_volume_id, \
              target_storage_volume_id, manifest_digest, state, payload, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(batch.target.tenant_id.as_str())
        .bind(batch.target.object_namespace_id.as_str())
        .bind(batch.batch_id.as_str())
        .bind(batch.materialization_id.as_str())
        .bind(v2_i64(batch.plan_revision.get(), "plan_revision")?)
        .bind(v2_i64(batch.batch_attempt.get(), "batch_attempt")?)
        .bind(batch.source.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
        .bind(batch.target.storage_volume_id.as_str())
        .bind(batch.manifest_digest.as_bytes().as_slice())
        .bind(materialization_batch_state_name(batch.state))
        .bind(payload)
        .bind(v2_i64(parent.created_at_unix_ms.get(), "created_at_unix_ms")?)
        .bind(v2_i64(parent.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(batch),
            Err(error) if is_unique(&error) => {
                let row = sqlx::query(
                    "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id FROM materialization_batches \
                     WHERE tenant_id = ? AND object_namespace_id = ? AND batch_id = ?",
                )
                .bind(batch.target.tenant_id.as_str())
                .bind(batch.target.object_namespace_id.as_str())
                .bind(batch.batch_id.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| storage_corruption("materialization Batch uniqueness conflict has no row"))?;
                let existing = decode_v2_batch(&row)?;
                if existing == batch {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "materialization batch ID is already bound to different metadata",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn replace_materialization_batch(
        &self,
        request: crate::MaterializationBatchCasRequest,
    ) -> CentralResult<MaterializationBatch> {
        request.batch.validate().map_err(protocol_invalid)?;
        if request.batch.materialization_id != request.materialization_id
            || request.batch.batch_id != request.batch_id
            || request.batch.target.tenant_id != request.tenant_id
            || request.batch.target.object_namespace_id != request.object_namespace_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Batch replacement identity does not match its key",
            )
            .with_retryable(false));
        }
        let parent = self
            .get_materialization(
                &request.tenant_id,
                &request.object_namespace_id,
                &request.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != request.object_namespace_id
            || parent.key.target_storage_volume_id != request.batch.target.storage_volume_id
            || parent.plan_revision != request.expected_plan_revision
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Batch target or revision does not match its parent",
            )
            .with_retryable(false));
        }
        let row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id \
             FROM materialization_batches WHERE tenant_id = ? AND object_namespace_id = ? AND batch_id = ? LIMIT 1",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.object_namespace_id.as_str())
        .bind(request.batch_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "materialization Batch not found",
            )
        })?;
        let current = decode_v2_batch(&row)?;
        if current.plan_revision != request.expected_plan_revision
            || current.batch_attempt != request.expected_batch_attempt
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Batch revision or attempt changed",
            ));
        }
        if request.batch.plan_revision != request.expected_plan_revision
            || request.batch.batch_attempt < request.expected_batch_attempt
            || request.batch.batch_attempt
                > Generation::new(request.expected_batch_attempt.get().saturating_add(1))
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Batch advanced by more than one attempt",
            ));
        }
        if !current.state.can_transition_to(request.batch.state) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Batch state transition is not allowed",
            )
            .with_retryable(false));
        }
        let payload = encode(&request.batch)?;
        let result = sqlx::query(
            "UPDATE materialization_batches SET plan_revision = ?, attempt = ?, source_storage_volume_id = ?, \
             target_storage_volume_id = ?, manifest_digest = ?, state = ?, payload = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND object_namespace_id = ? AND batch_id = ? \
               AND plan_revision = ? AND attempt = ?",
        )
        .bind(v2_i64(
            request.batch.plan_revision.get(),
            "plan_revision",
        )?)
        .bind(v2_i64(
            request.batch.batch_attempt.get(),
            "batch_attempt",
        )?)
        .bind(
            request
                .batch
                .source
                .storage_volume_id
                .as_ref()
                .map(StorageVolumeId::as_str),
        )
        .bind(request.batch.target.storage_volume_id.as_str())
        .bind(request.batch.manifest_digest.as_bytes().as_slice())
        .bind(materialization_batch_state_name(request.batch.state))
        .bind(payload)
        .bind(v2_i64(parent.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(request.tenant_id.as_str())
        .bind(request.object_namespace_id.as_str())
        .bind(request.batch_id.as_str())
        .bind(v2_i64(
            request.expected_plan_revision.get(),
            "expected_plan_revision",
        )?)
        .bind(v2_i64(
            request.expected_batch_attempt.get(),
            "expected_batch_attempt",
        )?)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Batch revision or attempt changed",
            ));
        }
        Ok(request.batch)
    }

    async fn list_materialization_batches(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Vec<MaterializationBatch>> {
        let rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id FROM materialization_batches \
             WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? ORDER BY batch_id",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(materialization_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.iter()
            .map(decode_v2_batch)
            .collect::<CentralResult<Vec<_>>>()
    }

    async fn list_active_materialization_batches_for_agent(
        &self,
        tenant_id: &TenantId,
        agent_id: &AgentId,
    ) -> CentralResult<Vec<MaterializationBatch>> {
        // Target agent identity is part of the signed JSON payload rather than a separately
        // mutable SQL column. Decode the bounded tenant slice first, then apply the same strict
        // state/identity filter as the in-memory authority.
        let rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id FROM materialization_batches \
             WHERE tenant_id = ? ORDER BY materialization_id, plan_revision, batch_id",
        )
        .bind(tenant_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let mut batches = rows
            .iter()
            .map(decode_v2_batch)
            .collect::<CentralResult<Vec<_>>>()?;
        batches.retain(|batch| {
            batch.target.tenant_id == *tenant_id
                && batch.target.agent_id == *agent_id
                && matches!(
                    batch.state,
                    MaterializationBatchState::Queued
                        | MaterializationBatchState::Assigned
                        | MaterializationBatchState::Transferring
                        | MaterializationBatchState::Verifying
                )
        });
        Ok(batches)
    }

    async fn insert_materialization_object(
        &self,
        tenant_id: &TenantId,
        object: MaterializationObject,
    ) -> CentralResult<MaterializationObject> {
        object.validate().map_err(protocol_invalid)?;
        let parent = sqlx::query(
            "SELECT tenant_id, object_namespace_id, target_storage_volume_id, commit_id FROM materializations \
             WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(object.object.object_namespace_id.as_str())
        .bind(object.materialization_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| CentralError::new(CentralErrorCode::ResourceNotFound, "materialization not found"))?;
        let tenant_id = parent
            .try_get::<String, _>("tenant_id")
            .map_err(storage_error)?;
        let namespace = parent
            .try_get::<String, _>("object_namespace_id")
            .map_err(storage_error)?;
        if namespace != object.object.object_namespace_id.as_str() {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization object namespace does not match its parent",
            )
            .with_retryable(false));
        }
        let tenant = TenantId::new(tenant_id.clone()).map_err(|error| {
            storage_corruption(format!("stored materialization tenant ID: {error}"))
        })?;
        let parent_job = self
            .get_materialization(
                &tenant,
                &object.object.object_namespace_id,
                &object.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent_job.plan_revision != object.plan_revision {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization object plan revision does not match its parent",
            )
            .with_retryable(false));
        }
        let commit_id = CommitId::from_digest(digest_from_blob(
            parent
                .try_get::<Vec<u8>, _>("commit_id")
                .map_err(storage_error)?,
            "materialization commit_id",
        )?);
        let object_set = self
            .get_commit_object_set(&tenant, &commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        let expected = object_set
            .object_set
            .objects
            .iter()
            .find(|candidate| candidate.object_id == object.object.object_id)
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization object is not part of the Commit ObjectSet",
                )
                .with_retryable(false)
            })?;
        if expected.size != object.object.size
            || expected.encoding != object.object.encoding
            || expected.ordinal != object.object.ordinal
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization object metadata disagrees with the Commit ObjectSet",
            )
            .with_retryable(false));
        }
        let payload = encode(&object)?;
        let result = sqlx::query(
            "INSERT INTO materialization_objects \
             (tenant_id, materialization_id, object_namespace_id, object_id, size, encoding, \
              staging_key, confirmed_offset, state, current_batch_id, plan_revision, attempt, \
              payload, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&tenant_id)
        .bind(object.materialization_id.as_str())
        .bind(object.object.object_namespace_id.as_str())
        .bind(object.object.object_id.as_bytes().as_slice())
        .bind(v2_i64(object.object.size.get(), "object size")?)
        .bind(object_encoding_name(object.object.encoding))
        .bind(&object.staging_key)
        .bind(v2_i64(object.confirmed_offset.get(), "confirmed_offset")?)
        .bind(materialization_object_state_name(object.state))
        .bind(object.current_batch_id.as_ref().map(|id| id.as_str()))
        .bind(v2_i64(object.plan_revision.get(), "plan_revision")?)
        .bind(v2_i64(object.attempt.get(), "attempt")?)
        .bind(payload)
        .bind(v2_i64(
            parent_job.created_at_unix_ms.get(),
            "created_at_unix_ms",
        )?)
        .bind(v2_i64(
            parent_job.updated_at_unix_ms.get(),
            "updated_at_unix_ms",
        )?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(object),
            Err(error) if is_unique(&error) => {
                let row = sqlx::query(
                    "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt FROM materialization_objects \
                     WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ?",
                )
                .bind(&tenant_id)
                .bind(object.materialization_id.as_str())
                .bind(object.object.object_namespace_id.as_str())
                .bind(object.object.object_id.as_bytes().as_slice())
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| storage_corruption("materialization Object uniqueness conflict has no row"))?;
                let existing = decode_v2_object(&row)?;
                if existing == object {
                    Ok(existing)
                } else if existing
                    .plan_revision
                    .get()
                    .checked_add(1)
                    .is_some_and(|next| object.plan_revision.get() == next)
                    && object.object == existing.object
                    && object.staging_key == existing.staging_key
                    && object.confirmed_offset >= existing.confirmed_offset
                    && (!existing.complete()
                        || (object.complete()
                            && object.confirmed_offset == existing.confirmed_offset))
                    && existing
                        .attempt
                        .get()
                        .checked_add(1)
                        .is_some_and(|next| object.attempt.get() == next)
                {
                    // A retry/replan reuses the stable object row and staging key. Keep the
                    // checkpoint and completion fence while advancing exactly one plan/attempt.
                    let payload = encode(&object)?;
                    let result = sqlx::query(
                        "UPDATE materialization_objects SET size = ?, encoding = ?, staging_key = ?, \
                         confirmed_offset = ?, state = ?, current_batch_id = ?, plan_revision = ?, attempt = ?, payload = ?, updated_at_unix_ms = ? \
                         WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ? \
                           AND plan_revision = ? AND attempt = ?",
                    )
                    .bind(v2_i64(object.object.size.get(), "object size")?)
                    .bind(object_encoding_name(object.object.encoding))
                    .bind(&object.staging_key)
                    .bind(v2_i64(object.confirmed_offset.get(), "confirmed_offset")?)
                    .bind(materialization_object_state_name(object.state))
                    .bind(object.current_batch_id.as_ref().map(|id| id.as_str()))
                    .bind(v2_i64(object.plan_revision.get(), "plan_revision")?)
                    .bind(v2_i64(object.attempt.get(), "attempt")?)
                    .bind(payload)
                    .bind(v2_i64(parent_job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
                    .bind(&tenant_id)
                    .bind(object.materialization_id.as_str())
                    .bind(object.object.object_namespace_id.as_str())
                    .bind(object.object.object_id.as_bytes().as_slice())
                    .bind(v2_i64(existing.plan_revision.get(), "expected_plan_revision")?)
                    .bind(v2_i64(existing.attempt.get(), "expected_attempt")?)
                    .execute(&self.pool)
                    .await
                    .map_err(storage_error)?;
                    if result.rows_affected() == 0 {
                        Err(CentralError::new(
                            CentralErrorCode::ConcurrentUpdate,
                            "materialization object changed during replanning",
                        ))
                    } else {
                        Ok(object)
                    }
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::ConcurrentUpdate,
                        "materialization object identity, checkpoint, or plan revision changed",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn replace_materialization_object(
        &self,
        request: crate::MaterializationObjectCasRequest,
    ) -> CentralResult<MaterializationObject> {
        request.object.validate().map_err(protocol_invalid)?;
        if request.object.materialization_id != request.materialization_id
            || request.object.object.object_namespace_id != request.object_namespace_id
            || request.object.object.object_id != request.object_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Object replacement identity does not match its key",
            )
            .with_retryable(false));
        }
        let parent = self
            .get_materialization(
                &request.tenant_id,
                &request.object_namespace_id,
                &request.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != request.object_namespace_id
            || parent.plan_revision != request.expected_plan_revision
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Object parent revision does not match",
            )
            .with_retryable(false));
        }
        let row = sqlx::query(
            "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt \
             FROM materialization_objects WHERE tenant_id = ? AND materialization_id = ? \
               AND object_namespace_id = ? AND object_id = ? LIMIT 1",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.materialization_id.as_str())
        .bind(request.object_namespace_id.as_str())
        .bind(request.object_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "materialization Object not found",
            )
        })?;
        let current = decode_v2_object(&row)?;
        if current.plan_revision != request.expected_plan_revision
            || current.attempt != request.expected_attempt
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object revision or attempt changed",
            ));
        }
        if current.object != request.object.object
            || current.staging_key != request.object.staging_key
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Object immutable metadata cannot change",
            )
            .with_retryable(false));
        }
        if request.object.confirmed_offset < current.confirmed_offset {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object confirmed offset cannot move backwards",
            ));
        }
        if request.object.plan_revision != request.expected_plan_revision
            || request.object.attempt < request.expected_attempt
            || request.object.attempt
                > Generation::new(request.expected_attempt.get().saturating_add(1))
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object advanced by more than one attempt",
            ));
        }
        if !current.state.can_transition_to(request.object.state) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Object state transition is not allowed",
            )
            .with_retryable(false));
        }
        let payload = encode(&request.object)?;
        let result = sqlx::query(
            "UPDATE materialization_objects SET size = ?, encoding = ?, staging_key = ?, \
             confirmed_offset = ?, state = ?, current_batch_id = ?, plan_revision = ?, attempt = ?, payload = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ? \
               AND plan_revision = ? AND attempt = ?",
        )
        .bind(v2_i64(request.object.object.size.get(), "object size")?)
        .bind(object_encoding_name(request.object.object.encoding))
        .bind(&request.object.staging_key)
        .bind(v2_i64(
            request.object.confirmed_offset.get(),
            "confirmed_offset",
        )?)
        .bind(materialization_object_state_name(request.object.state))
        .bind(
            request
                .object
                .current_batch_id
                .as_ref()
                .map(|id| id.as_str()),
        )
        .bind(v2_i64(
            request.object.plan_revision.get(),
            "plan_revision",
        )?)
        .bind(v2_i64(request.object.attempt.get(), "attempt")?)
        .bind(payload)
        .bind(v2_i64(parent.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(request.tenant_id.as_str())
        .bind(request.materialization_id.as_str())
        .bind(request.object_namespace_id.as_str())
        .bind(request.object_id.as_bytes().as_slice())
        .bind(v2_i64(
            request.expected_plan_revision.get(),
            "expected_plan_revision",
        )?)
        .bind(v2_i64(request.expected_attempt.get(), "expected_attempt")?)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object revision or attempt changed",
            ));
        }
        Ok(request.object)
    }

    async fn list_materialization_objects(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        materialization_id: &neoengram_domain::protocol::MaterializationId,
    ) -> CentralResult<Vec<MaterializationObject>> {
        let rows = sqlx::query(
            "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt FROM materialization_objects \
             WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? ORDER BY object_id",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(materialization_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.iter()
            .map(decode_v2_object)
            .collect::<CentralResult<Vec<_>>>()
    }

    async fn get_materialization_receipt(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        receipt_id: &neoengram_domain::protocol::ObjectReceiptId,
    ) -> CentralResult<Option<MaterializationObjectReceipt>> {
        let row = sqlx::query(
            "SELECT payload, tenant_id, object_namespace_id, receipt_id, materialization_id, batch_id, \
             plan_revision, batch_attempt, object_id, size, encoding, verified_digest, \
             target_storage_volume_id, target_placement_generation, committed_offset, verified_at_unix_ms \
             FROM materialization_receipts WHERE tenant_id = ? AND object_namespace_id = ? AND receipt_id = ? LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(receipt_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(|row| decode_v2_materialization_receipt(&row))
            .transpose()
    }

    async fn insert_object_read_lease(
        &self,
        lease: ObjectReadLease,
    ) -> CentralResult<ObjectReadLease> {
        lease.validate_for_acquisition().map_err(protocol_invalid)?;
        let parent = self
            .get_materialization(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != lease.object_namespace_id {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease namespace mismatch",
            )
            .with_retryable(false));
        }
        if parent.plan_revision != lease.plan_revision {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease plan revision does not match its parent",
            )
            .with_retryable(false));
        }
        let batch_row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id \
             FROM materialization_batches WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND batch_id = ? LIMIT 1",
        )
        .bind(lease.tenant_id.as_str())
        .bind(lease.object_namespace_id.as_str())
        .bind(lease.materialization_id.as_str())
        .bind(lease.batch_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(batch_row) = batch_row else {
            return Err(CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "read lease batch is not registered for this materialization",
            )
            .with_retryable(false));
        };
        let batch = decode_v2_batch(&batch_row)?;
        if batch.plan_revision != lease.plan_revision
            || batch.target.storage_volume_id != parent.key.target_storage_volume_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease batch does not match its materialization",
            )
            .with_retryable(false));
        }
        if !batch.object_ids.contains(&lease.object_id) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease object is not included in its batch manifest",
            )
            .with_retryable(false));
        }
        let objects = self
            .list_materialization_objects(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )
            .await?;
        let Some(object) = objects.iter().find(|object| {
            object.object.object_id == lease.object_id
                && object.current_batch_id.as_ref() == Some(&lease.batch_id)
        }) else {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "object read lease is not assigned to its Batch",
            )
            .with_retryable(false));
        };
        let source_selected = batch.source.placement_id == lease.placement_id
            || object.fallback_sources.contains(&lease.placement_id)
            || object.primary_source.as_ref() == Some(&lease.placement_id);
        if !source_selected {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "object read lease placement is not selected for its object task",
            )
            .with_retryable(false));
        }
        if object.primary_source.as_ref() == Some(&lease.placement_id)
            && batch.source.placement_generation != lease.placement_generation
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease source generation differs from its Batch fence",
            )
            .with_retryable(false));
        }
        let object_set = self
            .get_commit_object_set(&parent.key.tenant_id, &parent.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization references an unknown Commit ObjectSet",
                )
            })?;
        let expected = object_set
            .object_set
            .objects
            .iter()
            .find(|object| object.object_id == lease.object_id)
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "read lease object is not part of the Commit ObjectSet",
                )
                .with_retryable(false)
            })?;
        if expected.object_id != lease.object_id {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease object identity mismatch",
            )
            .with_retryable(false));
        }
        let placement_row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain \
             FROM object_placements \
             WHERE tenant_id = ? AND object_namespace_id = ? AND placement_id = ? \
               AND object_id = ? AND placement_generation = ? LIMIT 1",
        )
        .bind(lease.tenant_id.as_str())
        .bind(lease.object_namespace_id.as_str())
        .bind(lease.placement_id.as_str())
        .bind(lease.object_id.as_bytes().as_slice())
        .bind(v2_i64(
            lease.placement_generation.get(),
            "placement_generation",
        )?)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(placement_row) = placement_row else {
            return Err(CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "read lease placement is not registered",
            )
            .with_retryable(false));
        };
        let placement = decode_v2_object_placement(&placement_row)?;
        if !placement.readable() {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease placement is not readable",
            )
            .with_retryable(false));
        }
        if placement.size.get() != expected.size.get() || placement.encoding != expected.encoding {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease placement metadata disagrees with the Commit ObjectSet",
            )
            .with_retryable(false));
        }
        if placement.storage_volume_id.is_none() {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease placement must reference a StorageVolume",
            )
            .with_retryable(false));
        }
        if object.primary_source.as_ref() == Some(&lease.placement_id)
            && (placement.storage_volume_id != batch.source.storage_volume_id
                || placement.archive_id != batch.source.archive_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "read lease placement Volume does not match its Batch source fence",
            )
            .with_retryable(false));
        }
        let payload = encode(&lease)?;
        let result = sqlx::query(
            "INSERT INTO object_read_leases \
             (tenant_id, lease_id, materialization_id, batch_id, object_namespace_id, object_id, \
              placement_id, storage_volume_id, placement_generation, expires_at_unix_ms, state, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(lease.tenant_id.as_str())
        .bind(lease.lease_id.as_str())
        .bind(lease.materialization_id.as_str())
        .bind(lease.batch_id.as_str())
        .bind(lease.object_namespace_id.as_str())
        .bind(lease.object_id.as_bytes().as_slice())
        .bind(lease.placement_id.as_str())
        .bind(
            placement
                .storage_volume_id
                .as_ref()
                .map(StorageVolumeId::as_str)
                .ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::ProtocolInvalid,
                        "read lease source placement must reference a StorageVolume",
                    )
                    .with_retryable(false)
                })?,
        )
        .bind(v2_i64(
            lease.placement_generation.get(),
            "placement_generation",
        )?)
        .bind(v2_i64(
            lease.expires_at_unix_ms.get(),
            "expires_at_unix_ms",
        )?)
        .bind(materialization_lease_state_name(lease.state))
        .bind(payload)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(lease),
            Err(error) if is_unique(&error) => {
                let row = sqlx::query("SELECT payload, state, tenant_id, object_namespace_id, materialization_id, batch_id, object_id, placement_id, placement_generation, expires_at_unix_ms FROM object_read_leases WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ?")
                    .bind(lease.tenant_id.as_str())
                    .bind(lease.object_namespace_id.as_str())
                    .bind(lease.lease_id.as_str())
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| storage_corruption("read lease uniqueness conflict has no row"))?;
                let existing = decode_v2_read_lease(&row)?;
                if existing == lease {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "read lease ID is already in use",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn insert_staging_lease(&self, lease: StagingLease) -> CentralResult<StagingLease> {
        lease.validate_for_acquisition().map_err(protocol_invalid)?;
        let parent = self
            .get_materialization(
                &lease.tenant_id,
                &lease.object_namespace_id,
                &lease.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != lease.object_namespace_id
            || parent.key.target_storage_volume_id != lease.target_storage_volume_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "staging lease target mismatch",
            )
            .with_retryable(false));
        }
        let object_row = sqlx::query(
            "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt \
             FROM materialization_objects \
             WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? \
               AND object_id = ? LIMIT 1",
        )
        .bind(lease.tenant_id.as_str())
        .bind(lease.materialization_id.as_str())
        .bind(lease.object_namespace_id.as_str())
        .bind(lease.object_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(object_row) = object_row else {
            return Err(CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "staging lease object is not registered",
            )
            .with_retryable(false));
        };
        let object = decode_v2_object(&object_row)?;
        if object.staging_key != lease.staging_key || object.plan_revision != lease.plan_revision {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "staging lease does not match materialization object",
            )
            .with_retryable(false));
        }
        let Some(batch_id) = object.current_batch_id.as_ref() else {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "staging lease object has no current batch",
            )
            .with_retryable(false));
        };
        let batch_row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id \
             FROM materialization_batches WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND batch_id = ? LIMIT 1",
        )
        .bind(lease.tenant_id.as_str())
        .bind(lease.object_namespace_id.as_str())
        .bind(lease.materialization_id.as_str())
        .bind(batch_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(batch_row) = batch_row else {
            return Err(CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "staging lease object current batch is not registered",
            )
            .with_retryable(false));
        };
        let batch = decode_v2_batch(&batch_row)?;
        if batch.plan_revision != lease.plan_revision
            || batch.target.storage_volume_id != lease.target_storage_volume_id
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "staging lease batch does not match its materialization",
            )
            .with_retryable(false));
        }
        if !batch.object_ids.contains(&lease.object_id) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "staging lease object is not included in its batch manifest",
            )
            .with_retryable(false));
        }
        if batch.target.placement_generation != lease.target_placement_generation {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "staging lease target generation does not match its batch target fence",
            )
            .with_retryable(false));
        }
        let payload = encode(&lease)?;
        let result = sqlx::query(
            "INSERT INTO staging_leases \
             (tenant_id, lease_id, materialization_id, object_namespace_id, object_id, \
              target_storage_volume_id, target_placement_generation, staging_key, \
              expires_at_unix_ms, state, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(lease.tenant_id.as_str())
        .bind(lease.lease_id.as_str())
        .bind(lease.materialization_id.as_str())
        .bind(lease.object_namespace_id.as_str())
        .bind(lease.object_id.as_bytes().as_slice())
        .bind(lease.target_storage_volume_id.as_str())
        .bind(v2_i64(
            lease.target_placement_generation.get(),
            "target_placement_generation",
        )?)
        .bind(&lease.staging_key)
        .bind(v2_i64(
            lease.expires_at_unix_ms.get(),
            "expires_at_unix_ms",
        )?)
        .bind(materialization_lease_state_name(lease.state))
        .bind(payload)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(lease),
            Err(error) if is_unique(&error) => {
                let row = sqlx::query("SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, object_id, staging_key, target_placement_generation, expires_at_unix_ms FROM staging_leases WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ?")
                    .bind(lease.tenant_id.as_str())
                    .bind(lease.object_namespace_id.as_str())
                    .bind(lease.lease_id.as_str())
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| storage_corruption("staging lease uniqueness conflict has no row"))?;
                let existing = decode_v2_staging_lease(&row)?;
                if existing == lease {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "staging lease ID is already in use",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn reconcile_materialization_leases(
        &self,
        now_unix_ms: UnixMillis,
    ) -> CentralResult<MaterializationLeaseExpiryReconciliation> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let now = v2_i64(now_unix_ms.get(), "now_unix_ms")?;
        let read_rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, materialization_id, batch_id, \
                    object_id, placement_id, placement_generation, expires_at_unix_ms \
             FROM object_read_leases \
             WHERE state = 'active' AND expires_at_unix_ms <= ? \
             ORDER BY tenant_id, object_namespace_id, lease_id",
        )
        .bind(now)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let mut result = MaterializationLeaseExpiryReconciliation::default();
        for row in read_rows {
            let mut lease = decode_v2_read_lease(&row)?;
            if lease.state != MaterializationLeaseState::Active
                || lease.expires_at_unix_ms.get() > now_unix_ms.get()
            {
                return Err(storage_corruption(
                    "active object read lease expiry index disagrees with its payload",
                ));
            }
            lease.state = MaterializationLeaseState::Expired;
            let payload = encode(&lease)?;
            let update = sqlx::query(
                "UPDATE object_read_leases SET state = ?, payload = ? \
                 WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ? \
                   AND state = 'active' AND expires_at_unix_ms <= ?",
            )
            .bind(materialization_lease_state_name(lease.state))
            .bind(payload)
            .bind(lease.tenant_id.as_str())
            .bind(lease.object_namespace_id.as_str())
            .bind(lease.lease_id.as_str())
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if update.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "object read lease changed during expiry reconciliation",
                ));
            }
            result.expired_object_read_leases += 1;
        }

        let staging_rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, \
                    materialization_id, object_id, staging_key, target_placement_generation, \
                    expires_at_unix_ms \
             FROM staging_leases \
             WHERE state = 'active' AND expires_at_unix_ms <= ? \
             ORDER BY tenant_id, object_namespace_id, lease_id",
        )
        .bind(now)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        for row in staging_rows {
            let mut lease = decode_v2_staging_lease(&row)?;
            if lease.state != MaterializationLeaseState::Active
                || lease.expires_at_unix_ms.get() > now_unix_ms.get()
            {
                return Err(storage_corruption(
                    "active staging lease expiry index disagrees with its payload",
                ));
            }
            lease.state = MaterializationLeaseState::Expired;
            let payload = encode(&lease)?;
            let update = sqlx::query(
                "UPDATE staging_leases SET state = ?, payload = ? \
                 WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ? \
                   AND state = 'active' AND expires_at_unix_ms <= ?",
            )
            .bind(materialization_lease_state_name(lease.state))
            .bind(payload)
            .bind(lease.tenant_id.as_str())
            .bind(lease.object_namespace_id.as_str())
            .bind(lease.lease_id.as_str())
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if update.rows_affected() != 1 {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "staging lease changed during expiry reconciliation",
                ));
            }
            result.expired_staging_leases += 1;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(result)
    }

    async fn record_materialization_receipt(
        &self,
        request: crate::MaterializationReceiptRequest,
    ) -> CentralResult<ObjectPlacementV2> {
        let receipt = request.receipt;
        let object = request.object;
        object.validate().map_err(protocol_invalid)?;
        receipt
            .validate_against(&object)
            .map_err(protocol_invalid)?;
        let _gate = self.materialization_receipt_gate.lock().await;
        if let Some(placement) = self.replay_materialization_receipt(&receipt).await? {
            return Ok(placement);
        }
        let parent = self
            .get_materialization(
                &receipt.tenant_id,
                &receipt.object_namespace_id,
                &receipt.materialization_id,
            )
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization not found",
                )
            })?;
        if parent.key.object_namespace_id != receipt.object_namespace_id
            || parent.key.target_storage_volume_id != receipt.target_storage_volume_id
            || parent.plan_revision != receipt.plan_revision
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization receipt does not match its parent",
            )
            .with_retryable(false));
        }
        // The receipt verification time is the logical mutation time for the target placement,
        // object checkpoint, and derived Coverage. Keep it monotonic with the parent Job clock.
        let receipt_updated_at_unix_ms = UnixMillis::new(
            parent
                .updated_at_unix_ms
                .get()
                .max(receipt.verified_at_unix_ms.get()),
        );
        let batch_row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id \
             FROM materialization_batches WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND batch_id = ? LIMIT 1",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(receipt.batch_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "materialization Batch not found",
            )
        })?;
        let batch = decode_v2_batch(&batch_row)?;
        if batch.plan_revision != receipt.plan_revision
            || batch.batch_attempt != receipt.batch_attempt
            || batch.target.tenant_id != receipt.tenant_id
            || batch.target.object_namespace_id != receipt.object_namespace_id
            || batch.target.placement_generation != receipt.target_placement_generation
            || batch.target.storage_volume_id != receipt.target_storage_volume_id
            || !batch.object_ids.contains(&receipt.object_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization receipt does not match its Batch fence",
            )
            .with_retryable(false));
        }
        if receipt.verified_at_unix_ms.get() >= batch.deadline_unix_ms.get() {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "receipt verification must occur before the materialization batch deadline",
            )
            .with_retryable(false));
        }
        let object_row = sqlx::query(
            "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt \
             FROM materialization_objects WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ? LIMIT 1",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.object_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "materialization Object not found",
            )
        })?;
        let task = decode_v2_object(&object_row)?;
        if task.object != object || task.plan_revision != receipt.plan_revision {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization receipt does not match its Object task",
            )
            .with_retryable(false));
        }
        if !task.complete()
            && !task.state.can_transition_to(
                neoengram_domain::protocol::materialization::MaterializationObjectState::Verified,
            )
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Object cannot be verified from its current state",
            )
            .with_retryable(false));
        }
        if receipt.batch_attempt < task.attempt
            || receipt.batch_attempt > Generation::new(task.attempt.get().saturating_add(1))
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt attempt is stale or skipped",
            )
            .with_retryable(false));
        }
        let object_set = self
            .get_commit_object_set(&receipt.tenant_id, &parent.key.commit_id.digest())
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "materialization Commit ObjectSet not found",
                )
            })?;
        let expected = object_set
            .object_set
            .objects
            .iter()
            .find(|candidate| candidate.object_id == receipt.object_id)
            .copied()
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "receipt object is not part of the Commit ObjectSet",
                )
                .with_retryable(false)
            })?;
        if expected.object_id != object.object_id
            || expected.size != object.size
            || expected.encoding != object.encoding
            || expected.ordinal != object.ordinal
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "receipt ObjectRef disagrees with the Commit ObjectSet",
            )
            .with_retryable(false));
        }

        // The receipt, placement, checkpoint, Job progress and derived Coverage form one durable
        // publication boundary.  All reads below are repeated through this transaction where a
        // concurrent planner could otherwise change the active Batch fence after validation.
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let existing = sqlx::query(
            "SELECT payload, tenant_id, object_namespace_id, receipt_id, materialization_id, batch_id, \
             plan_revision, batch_attempt, object_id, size, encoding, verified_digest, \
             target_storage_volume_id, target_placement_generation, committed_offset, verified_at_unix_ms \
             FROM materialization_receipts WHERE tenant_id = ? AND object_namespace_id = ? AND receipt_id = ? LIMIT 1",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.receipt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(row) = existing {
            let old = decode_v2_materialization_receipt(&row)?;
            if old != receipt {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "materialization receipt ID is already in use",
                )
                .with_retryable(false));
            }
            let placement_row = sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
                 WHERE tenant_id = ? AND object_namespace_id = ? AND object_id = ? \
                   AND storage_volume_id = ? AND placement_generation = ? LIMIT 1",
            )
            .bind(receipt.tenant_id.as_str())
            .bind(receipt.object_namespace_id.as_str())
            .bind(receipt.object_id.as_bytes().as_slice())
            .bind(receipt.target_storage_volume_id.as_str())
            .bind(v2_i64(
                receipt.target_placement_generation.get(),
                "target_placement_generation",
            )?)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| storage_corruption("materialization receipt has no Placement"))?;
            let placement = decode_v2_object_placement(&placement_row)?;
            if placement.object_id != receipt.object_id
                || placement.object_namespace_id != receipt.object_namespace_id
                || placement.size != receipt.size
                || placement.encoding != receipt.encoding
                || placement.verified_digest != receipt.verified_digest
                || placement.storage_volume_id.as_ref() != Some(&receipt.target_storage_volume_id)
                || placement.placement_generation != receipt.target_placement_generation
                || !placement.readable()
            {
                return Err(storage_corruption(
                    "materialization receipt Placement disagrees with its evidence",
                ));
            }
            transaction.commit().await.map_err(storage_error)?;
            let batch = self
                .list_materialization_batches(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &receipt.materialization_id,
                )
                .await?
                .into_iter()
                .find(|candidate| candidate.batch_id == receipt.batch_id)
                .ok_or_else(|| storage_corruption("materialization receipt replay has no Batch"))?;
            let task = self
                .list_materialization_objects(
                    &receipt.tenant_id,
                    &receipt.object_namespace_id,
                    &receipt.materialization_id,
                )
                .await?
                .into_iter()
                .find(|candidate| {
                    candidate.object.object_namespace_id == receipt.object_namespace_id
                        && candidate.object.object_id == receipt.object_id
                })
                .ok_or_else(|| {
                    storage_corruption("materialization receipt replay has no Object task")
                })?;
            self.release_receipt_leases(&receipt, &batch, &task).await?;
            return Ok(placement);
        }

        let batch_row = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, plan_revision, attempt, manifest_digest, source_storage_volume_id \
             FROM materialization_batches WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND batch_id = ? LIMIT 1",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(receipt.batch_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "materialization Batch not found",
            )
        })?;
        let batch = decode_v2_batch(&batch_row)?;
        if batch.plan_revision != receipt.plan_revision
            || batch.batch_attempt != receipt.batch_attempt
            || batch.target.tenant_id != receipt.tenant_id
            || batch.target.object_namespace_id != receipt.object_namespace_id
            || batch.target.placement_generation != receipt.target_placement_generation
            || batch.target.storage_volume_id != receipt.target_storage_volume_id
            || !batch.object_ids.contains(&receipt.object_id)
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt does not match the active Batch fence",
            )
            .with_retryable(false));
        }

        let object_row = sqlx::query(
            "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt \
             FROM materialization_objects WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ? LIMIT 1",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.object_id.as_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "materialization Object not found",
            )
        })?;
        let task = decode_v2_object(&object_row)?;
        if task.object != object
            || task.plan_revision != receipt.plan_revision
            || receipt.batch_attempt < task.attempt
            || receipt.batch_attempt > Generation::new(task.attempt.get().saturating_add(1))
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization receipt does not match the current Object fence",
            )
            .with_retryable(false));
        }
        if task.current_batch_id.as_ref() != Some(&receipt.batch_id) {
            // Source-grouped batches can race for one object after a retry or scheduler replay.
            // A losing receipt may converge only when this exact plan/attempt already has a
            // complete, matching target Placement; stale attempts remain fenced.
            if receipt.batch_attempt != task.attempt {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization receipt belongs to an obsolete Batch or attempt",
                )
                .with_retryable(false));
            }
            let target_evidence = sqlx::query(
                "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
                 WHERE tenant_id = ? AND object_namespace_id = ? AND object_id = ? \
                   AND storage_volume_id = ? AND placement_generation = ? LIMIT 1",
            )
            .bind(receipt.tenant_id.as_str())
            .bind(receipt.object_namespace_id.as_str())
            .bind(receipt.object_id.as_bytes().as_slice())
            .bind(receipt.target_storage_volume_id.as_str())
            .bind(v2_i64(
                receipt.target_placement_generation.get(),
                "target_placement_generation",
            )?)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let target_has_evidence = target_evidence
                .as_ref()
                .map(decode_v2_object_placement)
                .transpose()?
                .is_some_and(|placement| placement.readable() && placement.matches_ref(&object));
            if !target_has_evidence {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "materialization receipt belongs to an obsolete Batch",
                )
                .with_retryable(false));
            }
        }
        let object_id = crate::placement_authority::materialization_target_placement_id(&receipt)?;
        let placement = ObjectPlacementV2 {
            placement_id: object_id,
            tenant_id: receipt.tenant_id.clone(),
            object_namespace_id: receipt.object_namespace_id.clone(),
            object_id: receipt.object_id,
            size: receipt.size,
            encoding: receipt.encoding,
            verified_digest: receipt.verified_digest,
            storage_volume_id: Some(receipt.target_storage_volume_id.clone()),
            archive_id: None,
            placement_generation: receipt.target_placement_generation,
            state: neoengram_domain::protocol::materialization::ObjectPlacementState::Verified,
            failure_domain: format!("volume:{}", receipt.target_storage_volume_id),
        };
        let placement_payload = encode(&placement)?;
        let placement_result = sqlx::query(
            "INSERT INTO object_placements \
             (tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, \
              storage_volume_id, placement_generation, state, failure_domain, created_at_unix_ms, updated_at_unix_ms, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(placement.tenant_id.as_str())
        .bind(placement.object_namespace_id.as_str())
        .bind(placement.placement_id.as_str())
        .bind(placement.object_id.as_bytes().as_slice())
        .bind(v2_i64(placement.size.get(), "object size")?)
        .bind(object_encoding_name(placement.encoding))
        .bind(placement.verified_digest.as_bytes().as_slice())
        .bind(placement.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
        .bind(v2_i64(placement.placement_generation.get(), "placement_generation")?)
        .bind(v2_placement_state_name(placement.state))
        .bind(&placement.failure_domain)
        .bind(v2_i64(parent.created_at_unix_ms.get(), "created_at_unix_ms")?)
        .bind(v2_i64(receipt_updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(placement_payload)
        .execute(&mut *transaction)
        .await;
        let stored_placement = match placement_result {
            Ok(_) => placement.clone(),
            Err(error) if is_unique(&error) => {
                let row = sqlx::query(
                    "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
                     WHERE tenant_id = ? AND object_namespace_id = ? AND object_id = ? AND storage_volume_id = ? AND placement_generation = ? LIMIT 1",
                )
                .bind(placement.tenant_id.as_str())
                .bind(placement.object_namespace_id.as_str())
                .bind(placement.object_id.as_bytes().as_slice())
                .bind(placement.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
                .bind(v2_i64(placement.placement_generation.get(), "placement_generation")?)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage_error)?;
                let row = if let Some(row) = row {
                    row
                } else {
                    sqlx::query(
                        "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain FROM object_placements \
                         WHERE tenant_id = ? AND object_namespace_id = ? AND placement_id = ? LIMIT 1",
                    )
                    .bind(placement.tenant_id.as_str())
                    .bind(placement.object_namespace_id.as_str())
                    .bind(placement.placement_id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| {
                        storage_corruption("v2 placement uniqueness conflict has no row")
                    })?
                };
                let existing = decode_v2_object_placement(&row)?;
                if existing == placement || same_v2_placement_evidence(&existing, &placement) {
                    existing
                } else {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "v2 object placement identity is already bound to different metadata",
                    )
                    .with_retryable(false));
                }
            }
            Err(error) => return Err(storage_error(error)),
        };
        let receipt_payload = encode(&receipt)?;
        let insert = sqlx::query(
            "INSERT INTO materialization_receipts (tenant_id, object_namespace_id, receipt_id, materialization_id, batch_id, plan_revision, batch_attempt, object_id, size, encoding, verified_digest, target_storage_volume_id, target_placement_generation, committed_offset, verified_at_unix_ms, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.receipt_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(receipt.batch_id.as_str())
        .bind(v2_i64(receipt.plan_revision.get(), "plan_revision")?)
        .bind(v2_i64(receipt.batch_attempt.get(), "batch_attempt")?)
        .bind(receipt.object_id.as_bytes().as_slice())
        .bind(v2_i64(receipt.size.get(), "size")?)
        .bind(object_encoding_name(receipt.encoding))
        .bind(receipt.verified_digest.as_bytes().as_slice())
        .bind(receipt.target_storage_volume_id.as_str())
        .bind(v2_i64(receipt.target_placement_generation.get(), "target_placement_generation")?)
        .bind(v2_i64(receipt.committed_offset.get(), "committed_offset")?)
        .bind(v2_i64(receipt.verified_at_unix_ms.get(), "verified_at_unix_ms")?)
        .bind(receipt_payload)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = insert {
            if is_unique(&error) {
                let row = sqlx::query(
                    "SELECT payload, tenant_id, object_namespace_id, receipt_id, materialization_id, batch_id, \
                     plan_revision, batch_attempt, object_id, size, encoding, verified_digest, \
                     target_storage_volume_id, target_placement_generation, committed_offset, verified_at_unix_ms \
                     FROM materialization_receipts WHERE tenant_id = ? AND object_namespace_id = ? AND receipt_id = ? LIMIT 1",
                )
                .bind(receipt.tenant_id.as_str())
                .bind(receipt.object_namespace_id.as_str())
                .bind(receipt.receipt_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage_error)?;
                let row = if let Some(row) = row {
                    row
                } else {
                    sqlx::query(
                        "SELECT payload, tenant_id, object_namespace_id, receipt_id, materialization_id, batch_id, \
                         plan_revision, batch_attempt, object_id, size, encoding, verified_digest, \
                         target_storage_volume_id, target_placement_generation, committed_offset, verified_at_unix_ms \
                         FROM materialization_receipts WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND batch_id = ? AND object_id = ? LIMIT 1",
                    )
                    .bind(receipt.tenant_id.as_str())
                    .bind(receipt.object_namespace_id.as_str())
                    .bind(receipt.materialization_id.as_str())
                    .bind(receipt.batch_id.as_str())
                    .bind(receipt.object_id.as_bytes().as_slice())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| {
                        storage_corruption("materialization receipt uniqueness conflict has no row")
                    })?
                };
                let old = decode_v2_materialization_receipt(&row)?;
                if old != receipt {
                    return Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "materialization receipt ID is already in use",
                    )
                    .with_retryable(false));
                }
                transaction.commit().await.map_err(storage_error)?;
                self.release_receipt_leases(&receipt, &batch, &task).await?;
                return Ok(stored_placement);
            }
            return Err(storage_error(error));
        }

        let mut next_object = task.clone();
        next_object.confirmed_offset = DecimalU64::new(
            task.confirmed_offset
                .get()
                .max(receipt.committed_offset.get()),
        );
        if !task.complete() {
            next_object.state =
                neoengram_domain::protocol::materialization::MaterializationObjectState::Verified;
        }
        next_object.attempt = task.attempt.max(receipt.batch_attempt);
        let next_object_payload = encode(&next_object)?;
        // The receipt may belong to a competing Batch that already lost the object CAS.  In that
        // case the transaction-local `task` is authoritative: bind the update to its current
        // Batch, not to the losing receipt's Batch, so convergence advances the same durable task
        // without reopening the planner race.
        let current_batch_id = task.current_batch_id.as_ref().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object has no active Batch fence",
            )
        })?;
        let updated_object = sqlx::query(
            "UPDATE materialization_objects SET confirmed_offset = ?, state = ?, attempt = ?, payload = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? AND object_id = ? \
               AND current_batch_id = ? AND plan_revision = ? AND attempt = ? AND state = ?",
        )
        .bind(v2_i64(next_object.confirmed_offset.get(), "confirmed_offset")?)
        .bind(materialization_object_state_name(next_object.state))
        .bind(v2_i64(next_object.attempt.get(), "attempt")?)
        .bind(next_object_payload)
        .bind(v2_i64(
            receipt_updated_at_unix_ms.get(),
            "updated_at_unix_ms",
        )?)
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.object_id.as_bytes().as_slice())
        .bind(current_batch_id.as_str())
        .bind(v2_i64(receipt.plan_revision.get(), "plan_revision")?)
        .bind(v2_i64(task.attempt.get(), "expected_attempt")?)
        .bind(materialization_object_state_name(task.state))
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if updated_object.rows_affected() == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Object revision or batch changed",
            ));
        }

        let task_rows = sqlx::query(
            "SELECT payload, state, object_namespace_id, staging_key, object_id, size, encoding, confirmed_offset, plan_revision, attempt \
             FROM materialization_objects WHERE tenant_id = ? AND materialization_id = ? AND object_namespace_id = ? ORDER BY object_id",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let tasks = task_rows
            .iter()
            .map(decode_v2_object)
            .collect::<CentralResult<Vec<_>>>()?;
        let verified_objects = tasks.iter().filter(|task| task.complete()).count() as u64;
        let verified_bytes = tasks
            .iter()
            .filter(|task| task.complete())
            .map(|task| task.object.size.get())
            .sum::<u64>();
        let mut next_job = parent.clone();
        next_job.verified_object_count = DecimalU64::new(verified_objects);
        next_job.verified_bytes = DecimalU64::new(verified_bytes);
        next_job.missing_object_count =
            DecimalU64::new(parent.object_count.get().saturating_sub(verified_objects));
        next_job.missing_bytes =
            DecimalU64::new(parent.total_bytes.get().saturating_sub(verified_bytes));
        next_job.state = if next_job.key.coverage_goal.satisfied_by(
            next_job.verified_object_count.get(),
            next_job.verified_bytes.get(),
            next_job.object_count.get(),
            next_job.total_bytes.get(),
        ) {
            MaterializationJobState::Complete
        } else {
            match parent.state {
                MaterializationJobState::Queued
                | MaterializationJobState::Planning
                | MaterializationJobState::WaitingForSources
                | MaterializationJobState::Materializing
                | MaterializationJobState::Verifying => MaterializationJobState::Verifying,
                state => state,
            }
        };
        if !parent.state.can_transition_to(next_job.state) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "materialization Job state transition is not allowed",
            )
            .with_retryable(false));
        }
        next_job.updated_at_unix_ms = receipt_updated_at_unix_ms;
        let next_job_payload = encode(&next_job)?;
        let object_set_digest = object_set.object_set.object_set_digest;
        let updated_job = sqlx::query(
            "UPDATE materializations SET state = ?, object_set_digest = ?, object_count = ?, total_bytes = ?, \
                 verified_object_count = ?, verified_bytes = ?, missing_object_count = ?, missing_bytes = ?, \
                 source_count = ?, deadline_unix_ms = ?, payload = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND object_namespace_id = ? AND materialization_id = ? AND plan_revision = ? AND state = ?",
        )
        .bind(materialization_job_state_name(next_job.state))
        .bind(object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(next_job.object_count.get(), "object_count")?)
        .bind(v2_i64(next_job.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(next_job.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(next_job.verified_bytes.get(), "verified_bytes")?)
        .bind(v2_i64(next_job.missing_object_count.get(), "missing_object_count")?)
        .bind(v2_i64(next_job.missing_bytes.get(), "missing_bytes")?)
        .bind(v2_i64(next_job.source_count.get(), "source_count")?)
        .bind(v2_i64(next_job.deadline_unix_ms.get(), "deadline_unix_ms")?)
        .bind(next_job_payload)
        .bind(v2_i64(next_job.updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.materialization_id.as_str())
        .bind(v2_i64(receipt.plan_revision.get(), "plan_revision")?)
        .bind(materialization_job_state_name(parent.state))
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if updated_job.rows_affected() == 0 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "materialization Job plan revision changed",
            ));
        }

        let placement_rows = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, placement_id, object_id, size, encoding, verified_digest, storage_volume_id, placement_generation, failure_domain \
             FROM object_placements WHERE tenant_id = ? AND object_namespace_id = ? AND storage_volume_id = ? AND placement_generation = ? ORDER BY object_id, placement_id",
        )
        .bind(receipt.tenant_id.as_str())
        .bind(receipt.object_namespace_id.as_str())
        .bind(receipt.target_storage_volume_id.as_str())
        .bind(v2_i64(receipt.target_placement_generation.get(), "placement_generation")?)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let object_ids = object_set
            .object_set
            .objects
            .iter()
            .map(|object| object.object_id)
            .collect::<std::collections::BTreeSet<_>>();
        let placements = placement_rows
            .iter()
            .map(decode_v2_object_placement)
            .collect::<CentralResult<Vec<_>>>()?
            .into_iter()
            .filter(|candidate| object_ids.contains(&candidate.object_id))
            .collect::<Vec<_>>();
        let coverage = VolumeCommitCoverage::from_placements(
            receipt.tenant_id.clone(),
            receipt.object_namespace_id.clone(),
            parent.key.commit_id,
            receipt.target_storage_volume_id.clone(),
            receipt.target_placement_generation,
            &object_set.object_set,
            &placements,
        )
        .map_err(protocol_invalid)?;
        let existing_coverage = sqlx::query(
            "SELECT payload, state, tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation, object_set_digest, object_count, verified_object_count, total_bytes, verified_bytes \
             FROM volume_commit_coverages WHERE tenant_id = ? AND object_namespace_id = ? AND commit_id = ? AND storage_volume_id = ? AND placement_generation = ? LIMIT 1",
        )
        .bind(coverage.tenant_id.as_str())
        .bind(coverage.object_namespace_id.as_str())
        .bind(coverage.commit_id.digest().as_bytes().as_slice())
        .bind(coverage.storage_volume_id.as_str())
        .bind(v2_i64(coverage.placement_generation.get(), "placement_generation")?)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(row) = existing_coverage {
            let existing = decode_v2_coverage(&row)?;
            if existing.state
                == neoengram_domain::protocol::materialization::CoverageState::Complete
                && coverage.state
                    != neoengram_domain::protocol::materialization::CoverageState::Complete
            {
                transaction.commit().await.map_err(storage_error)?;
                self.release_receipt_leases(&receipt, &batch, &task).await?;
                return Ok(stored_placement);
            }
        }
        let coverage_payload = encode(&coverage)?;
        sqlx::query(
            "INSERT INTO volume_commit_coverages \
             (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation, object_set_digest, object_count, verified_object_count, total_bytes, verified_bytes, state, created_at_unix_ms, updated_at_unix_ms, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, object_namespace_id, commit_id, storage_volume_id, placement_generation) \
             DO UPDATE SET object_set_digest = excluded.object_set_digest, object_count = excluded.object_count, verified_object_count = excluded.verified_object_count, total_bytes = excluded.total_bytes, verified_bytes = excluded.verified_bytes, state = excluded.state, updated_at_unix_ms = excluded.updated_at_unix_ms, payload = excluded.payload",
        )
        .bind(coverage.tenant_id.as_str())
        .bind(coverage.object_namespace_id.as_str())
        .bind(coverage.commit_id.digest().as_bytes().as_slice())
        .bind(coverage.storage_volume_id.as_str())
        .bind(v2_i64(coverage.placement_generation.get(), "placement_generation")?)
        .bind(coverage.object_set_digest.as_bytes().as_slice())
        .bind(v2_i64(coverage.object_count.get(), "object_count")?)
        .bind(v2_i64(coverage.verified_object_count.get(), "verified_object_count")?)
        .bind(v2_i64(coverage.total_bytes.get(), "total_bytes")?)
        .bind(v2_i64(coverage.verified_bytes.get(), "verified_bytes")?)
        .bind(coverage_state_name(coverage.state))
        .bind(v2_i64(parent.created_at_unix_ms.get(), "created_at_unix_ms")?)
        .bind(v2_i64(receipt_updated_at_unix_ms.get(), "updated_at_unix_ms")?)
        .bind(coverage_payload)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        self.release_receipt_leases(&receipt, &batch, &task).await?;
        Ok(stored_placement)
    }

    async fn release_object_read_lease(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        lease_id: &neoengram_domain::protocol::LeaseId,
    ) -> CentralResult<Option<ObjectReadLease>> {
        let row = sqlx::query("SELECT payload, state, tenant_id, object_namespace_id, materialization_id, batch_id, object_id, placement_id, placement_generation, expires_at_unix_ms FROM object_read_leases WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ?")
            .bind(tenant_id.as_str())
            .bind(object_namespace_id.as_str())
            .bind(lease_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut lease = decode_v2_read_lease(&row)?;
        if lease.state == MaterializationLeaseState::Active {
            lease.state = MaterializationLeaseState::Released;
        }
        let payload = encode(&lease)?;
        sqlx::query("UPDATE object_read_leases SET state = ?, payload = ? WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ?")
            .bind(materialization_lease_state_name(lease.state))
            .bind(payload)
            .bind(tenant_id.as_str())
            .bind(object_namespace_id.as_str())
            .bind(lease_id.as_str())
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(Some(lease))
    }

    async fn release_staging_lease(
        &self,
        tenant_id: &TenantId,
        object_namespace_id: &neoengram_domain::protocol::ObjectNamespaceId,
        lease_id: &neoengram_domain::protocol::LeaseId,
    ) -> CentralResult<Option<StagingLease>> {
        let row = sqlx::query("SELECT payload, state, tenant_id, object_namespace_id, target_storage_volume_id, materialization_id, object_id, staging_key, target_placement_generation, expires_at_unix_ms FROM staging_leases WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ?")
            .bind(tenant_id.as_str())
            .bind(object_namespace_id.as_str())
            .bind(lease_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut lease = decode_v2_staging_lease(&row)?;
        if lease.state == MaterializationLeaseState::Active {
            lease.state = MaterializationLeaseState::Released;
        }
        let payload = encode(&lease)?;
        sqlx::query(
            "UPDATE staging_leases SET state = ?, payload = ? WHERE tenant_id = ? AND object_namespace_id = ? AND lease_id = ?",
        )
        .bind(materialization_lease_state_name(lease.state))
        .bind(payload)
        .bind(tenant_id.as_str())
        .bind(object_namespace_id.as_str())
        .bind(lease_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(Some(lease))
    }

    async fn get_commit_object_set(
        &self,
        tenant_id: &TenantId,
        commit_id: &ContentDigest,
    ) -> CentralResult<Option<CommitObjectSet>> {
        let Some(metadata) = sqlx::query(
            "SELECT object_set_digest, object_count FROM commit_object_sets \
             WHERE tenant_id = ? AND commit_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let expected_digest = digest_from_blob(
            metadata
                .try_get::<Vec<u8>, _>("object_set_digest")
                .map_err(storage_error)?,
            "Commit object_set_digest",
        )?;
        let expected_count = u64_from_i64(
            metadata
                .try_get::<i64, _>("object_count")
                .map_err(storage_error)?,
            "Commit object_count",
        )?;
        let rows = sqlx::query(
            "SELECT object_id, size, encoding, ordinal FROM commit_objects \
             WHERE tenant_id = ? AND commit_id = ? ORDER BY ordinal ASC",
        )
        .bind(tenant_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let objects = rows
            .iter()
            .map(decode_commit_object)
            .collect::<CentralResult<Vec<_>>>()?;
        let object_set = ObjectSet::new(objects).map_err(|error| {
            storage_corruption(format!("stored Commit object set is invalid: {error}"))
        })?;
        if object_set.object_count() as u64 != expected_count
            || object_set.object_set_digest != expected_digest
        {
            return Err(storage_corruption(
                "stored Commit object set metadata does not match its objects",
            ));
        }
        Ok(Some(CommitObjectSet {
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(*commit_id),
            object_set,
        }))
    }

    async fn insert_commit_object_set(
        &self,
        object_set: CommitObjectSet,
    ) -> CentralResult<CommitObjectSet> {
        object_set.validate().map_err(protocol_invalid)?;
        let tenant_id = object_set.tenant_id.clone();
        let commit_id = object_set.commit_id.digest();
        if let Some(existing) = self.get_commit_object_set(&tenant_id, &commit_id).await? {
            if existing == object_set {
                return Ok(existing);
            }
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "Commit object set is already bound to different metadata",
            )
            .with_retryable(false));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query(
            "INSERT INTO commit_object_sets \
             (tenant_id, commit_id, object_set_digest, object_count) VALUES (?, ?, ?, ?)",
        )
        .bind(tenant_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .bind(
            object_set
                .object_set
                .object_set_digest
                .as_bytes()
                .as_slice(),
        )
        .bind(
            i64::try_from(object_set.object_set.object_count()).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "object_count exceeds SQLite range",
                )
            })?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if is_unique(&error) {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Commit object set is already bound to different metadata",
                )
                .with_retryable(false)
            } else {
                storage_error(error)
            }
        })?;
        for object in &object_set.object_set.objects {
            sqlx::query(
                "INSERT INTO objects (tenant_id, object_id, size, encoding, created_at_unix_ms) \
                 VALUES (?, ?, ?, ?, 0) ON CONFLICT (tenant_id, object_id) DO NOTHING",
            )
            .bind(tenant_id.as_str())
            .bind(object.object_id.as_bytes().as_slice())
            .bind(i64::try_from(object.size.get()).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "object size exceeds SQLite range",
                )
            })?)
            .bind(object_encoding_name(object.encoding))
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let stored = sqlx::query(
                "SELECT size, encoding FROM objects WHERE tenant_id = ? AND object_id = ?",
            )
            .bind(tenant_id.as_str())
            .bind(object.object_id.as_bytes().as_slice())
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let stored_size = stored.try_get::<i64, _>("size").map_err(storage_error)?;
            let stored_encoding = stored
                .try_get::<String, _>("encoding")
                .map_err(storage_error)?;
            if stored_size != i64::try_from(object.size.get()).unwrap_or(i64::MAX)
                || stored_encoding != object_encoding_name(object.encoding)
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Object identity is already bound to different size or encoding",
                )
                .with_retryable(false));
            }
            sqlx::query(
                "INSERT INTO commit_objects (tenant_id, commit_id, ordinal, object_id, size, encoding) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .bind(i64::try_from(object.ordinal.get()).map_err(|_| {
                CentralError::new(CentralErrorCode::ProtocolInvalid, "object ordinal exceeds SQLite range")
            })?)
            .bind(object.object_id.as_bytes().as_slice())
            .bind(i64::try_from(object.size.get()).map_err(|_| {
                CentralError::new(CentralErrorCode::ProtocolInvalid, "object size exceeds SQLite range")
            })?)
            .bind(object_encoding_name(object.encoding))
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                if is_unique(&error) {
                    CentralError::new(
                        CentralErrorCode::InvalidState,
                        "Commit object set conflicts with existing metadata",
                    )
                    .with_retryable(false)
                } else {
                    storage_error(error)
                }
            })?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(object_set)
    }

    async fn get_placement_set(
        &self,
        tenant_id: &TenantId,
        commit_id: &ContentDigest,
        backend_id: &BackendId,
    ) -> CentralResult<Option<CommitPlacementSet>> {
        let sql = format!(
            "SELECT {PLACEMENT_SET_COLUMNS} FROM legacy_commit_placement_sets \
             WHERE tenant_id = ? AND commit_id = ? AND backend_id = ?",
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .bind(backend_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(|row| decode_placement_set(&row))
            .transpose()
    }

    async fn published_placement_sets(
        &self,
        tenant_id: &TenantId,
        commit_id: &ContentDigest,
    ) -> CentralResult<Vec<CommitPlacementSet>> {
        let sql = format!(
            "SELECT {PLACEMENT_SET_COLUMNS} FROM legacy_commit_placement_sets \
             WHERE tenant_id = ? AND commit_id = ? AND state = 'published' \
             ORDER BY backend_id",
        );
        let rows = sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        rows.iter().map(decode_placement_set).collect()
    }

    async fn commit_placement_sets(
        &self,
        tenant_id: &TenantId,
        commit_id: &ContentDigest,
    ) -> CentralResult<Vec<CommitPlacementSet>> {
        let sql = format!(
            "SELECT {PLACEMENT_SET_COLUMNS} FROM legacy_commit_placement_sets \
             WHERE tenant_id = ? AND commit_id = ? ORDER BY backend_id, placement_generation",
        );
        let rows = sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        rows.iter().map(decode_placement_set).collect()
    }

    async fn insert_placement_set(
        &self,
        placement_set: CommitPlacementSet,
    ) -> CentralResult<CommitPlacementSet> {
        placement_set.validate().map_err(protocol_invalid)?;
        if placement_set.published() {
            validate_published_placement_set(self, &placement_set).await?;
        }
        let tenant_id = placement_set.tenant_id.clone();
        let commit_id = placement_set.commit_id.digest();
        if let Some(existing) = self
            .get_placement_set(&tenant_id, &commit_id, &placement_set.backend_id)
            .await?
        {
            if existing == placement_set {
                return Ok(existing);
            }
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "Commit PlacementSet is already bound to different metadata",
            )
            .with_retryable(false));
        }
        let result = sqlx::query(
            "INSERT INTO legacy_commit_placement_sets \
             (tenant_id, placement_set_id, commit_id, backend_id, storage_volume_id, archive_id, \
              object_set_digest, object_count, verified_object_count, placement_generation, state, \
              created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)",
        )
        .bind(tenant_id.as_str())
        .bind(placement_set.placement_set_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .bind(placement_set.backend_id.as_str())
        .bind(
            placement_set
                .storage_volume_id
                .as_ref()
                .map(StorageVolumeId::as_str),
        )
        .bind(placement_set.archive_id.as_ref().map(ArchiveId::as_str))
        .bind(placement_set.object_set_digest.as_bytes().as_slice())
        .bind(
            i64::try_from(placement_set.object_count.get()).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "object_count exceeds SQLite range",
                )
            })?,
        )
        .bind(
            i64::try_from(placement_set.verified_object_count.get()).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "verified_object_count exceeds SQLite range",
                )
            })?,
        )
        .bind(
            i64::try_from(placement_set.placement_generation.get()).map_err(|_| {
                CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "placement_generation exceeds SQLite range",
                )
            })?,
        )
        .bind(placement_set_state_name(placement_set.state))
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(placement_set),
            Err(error) if is_unique(&error) => {
                let existing = self
                    .get_placement_set(&tenant_id, &commit_id, &placement_set.backend_id)
                    .await?
                    .ok_or_else(|| {
                        storage_corruption("PlacementSet uniqueness conflict has no row")
                    })?;
                if existing == placement_set {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "Commit PlacementSet is already bound to different metadata",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn publish_initial_placement(
        &self,
        object_set: CommitObjectSet,
        placements: Vec<ObjectPlacement>,
        placement_set: CommitPlacementSet,
    ) -> CentralResult<(CommitObjectSet, CommitPlacementSet)> {
        object_set.validate().map_err(protocol_invalid)?;
        placement_set.validate().map_err(protocol_invalid)?;
        if !placement_set.published()
            || object_set.tenant_id != placement_set.tenant_id
            || object_set.commit_id != placement_set.commit_id
            || object_set.object_set.object_set_digest != placement_set.object_set_digest
            || object_set.object_set.object_count() as u64 != placement_set.object_count.get()
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "initial PlacementSet does not match the Commit ObjectSet",
            )
            .with_retryable(false));
        }
        let placement_by_object = placements
            .iter()
            .map(|placement| (placement.object_id, placement))
            .collect::<std::collections::BTreeMap<_, _>>();
        if placement_by_object.len() != placements.len()
            || placement_by_object.len() != object_set.object_set.object_count()
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "initial PlacementSet must contain exactly one copy of every Commit object",
            )
            .with_retryable(false));
        }
        for object in &object_set.object_set.objects {
            let Some(placement) = placement_by_object.get(&object.object_id) else {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "initial PlacementSet is missing a Commit object",
                )
                .with_retryable(false));
            };
            placement.validate().map_err(protocol_invalid)?;
            if placement.tenant_id != object_set.tenant_id
                || placement.backend_id != placement_set.backend_id
                || placement.storage_volume_id != placement_set.storage_volume_id
                || placement.archive_id != placement_set.archive_id
                || placement.placement_generation != placement_set.placement_generation
                || placement.state != PlacementState::Verified
                || placement.verified_size != object.size
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "initial object Placement does not match its publication fence",
                )
                .with_retryable(false));
            }
        }

        let tenant_id = object_set.tenant_id.clone();
        let commit_id = object_set.commit_id.digest();
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some(existing) = sqlx::query(&format!(
            "SELECT {PLACEMENT_SET_COLUMNS} FROM legacy_commit_placement_sets \
             WHERE tenant_id = ? AND commit_id = ? AND state = 'published' \
             ORDER BY backend_id LIMIT 1",
        ))
        .bind(tenant_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        {
            let existing_set = decode_placement_set(&existing)?;
            let Some(metadata) = sqlx::query(
                "SELECT object_set_digest, object_count FROM commit_object_sets \
                 WHERE tenant_id = ? AND commit_id = ?",
            )
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            else {
                return Err(storage_corruption(
                    "published PlacementSet is missing its Commit ObjectSet",
                ));
            };
            let expected_digest = digest_from_blob(
                metadata
                    .try_get::<Vec<u8>, _>("object_set_digest")
                    .map_err(storage_error)?,
                "Commit object_set_digest",
            )?;
            let expected_count = u64_from_i64(
                metadata
                    .try_get::<i64, _>("object_count")
                    .map_err(storage_error)?,
                "Commit object_count",
            )?;
            let rows = sqlx::query(
                "SELECT object_id, size, encoding, ordinal FROM commit_objects \
                 WHERE tenant_id = ? AND commit_id = ? ORDER BY ordinal ASC",
            )
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .fetch_all(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let stored_object_set = ObjectSet::new(
                rows.iter()
                    .map(decode_commit_object)
                    .collect::<CentralResult<Vec<_>>>()?,
            )
            .map_err(|error| {
                storage_corruption(format!("stored Commit object set is invalid: {error}"))
            })?;
            if stored_object_set.object_set_digest != expected_digest
                || stored_object_set.object_count() as u64 != expected_count
            {
                return Err(storage_corruption(
                    "stored Commit object set metadata does not match its objects",
                ));
            }
            let stored = CommitObjectSet {
                tenant_id: tenant_id.clone(),
                commit_id: CommitId::from_digest(commit_id),
                object_set: stored_object_set,
            };
            if stored != object_set {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Commit object set is already bound to different metadata",
                )
                .with_retryable(false));
            }
            transaction.rollback().await.map_err(storage_error)?;
            return Ok((stored, existing_set));
        }
        sqlx::query(
            "INSERT INTO commit_object_sets \
             (tenant_id, commit_id, object_set_digest, object_count) VALUES (?, ?, ?, ?) \
             ON CONFLICT (tenant_id, commit_id) DO NOTHING",
        )
        .bind(tenant_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .bind(
            object_set
                .object_set
                .object_set_digest
                .as_bytes()
                .as_slice(),
        )
        .bind(
            i64::try_from(object_set.object_set.object_count())
                .map_err(|_| protocol_invalid("object_count exceeds SQLite range"))?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let stored_set = sqlx::query(
            "SELECT object_set_digest, object_count FROM commit_object_sets \
             WHERE tenant_id = ? AND commit_id = ?",
        )
        .bind(tenant_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if digest_from_blob(
            stored_set
                .try_get::<Vec<u8>, _>("object_set_digest")
                .map_err(storage_error)?,
            "Commit object_set_digest",
        )? != object_set.object_set.object_set_digest
            || u64_from_i64(
                stored_set
                    .try_get::<i64, _>("object_count")
                    .map_err(storage_error)?,
                "Commit object_count",
            )? != object_set.object_set.object_count() as u64
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "Commit object set is already bound to different metadata",
            )
            .with_retryable(false));
        }

        for object in &object_set.object_set.objects {
            let size = i64::try_from(object.size.get())
                .map_err(|_| protocol_invalid("object size exceeds SQLite range"))?;
            let ordinal = i64::try_from(object.ordinal.get())
                .map_err(|_| protocol_invalid("object ordinal exceeds SQLite range"))?;
            sqlx::query(
                "INSERT INTO objects (tenant_id, object_id, size, encoding, created_at_unix_ms) \
                 VALUES (?, ?, ?, ?, 0) ON CONFLICT (tenant_id, object_id) DO NOTHING",
            )
            .bind(tenant_id.as_str())
            .bind(object.object_id.as_bytes().as_slice())
            .bind(size)
            .bind(object_encoding_name(object.encoding))
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let stored_object = sqlx::query(
                "SELECT size, encoding FROM objects WHERE tenant_id = ? AND object_id = ?",
            )
            .bind(tenant_id.as_str())
            .bind(object.object_id.as_bytes().as_slice())
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if stored_object
                .try_get::<i64, _>("size")
                .map_err(storage_error)?
                != size
                || stored_object
                    .try_get::<String, _>("encoding")
                    .map_err(storage_error)?
                    != object_encoding_name(object.encoding)
            {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Object identity is already bound to different metadata",
                )
                .with_retryable(false));
            }
            sqlx::query(
                "INSERT INTO commit_objects \
                 (tenant_id, commit_id, ordinal, object_id, size, encoding) \
                 VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (tenant_id, commit_id, ordinal) DO NOTHING",
            )
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .bind(ordinal)
            .bind(object.object_id.as_bytes().as_slice())
            .bind(size)
            .bind(object_encoding_name(object.encoding))
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let stored_commit_object = sqlx::query(
                "SELECT object_id, size, encoding, ordinal FROM commit_objects \
                 WHERE tenant_id = ? AND commit_id = ? AND ordinal = ?",
            )
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .bind(ordinal)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if decode_commit_object(&stored_commit_object)? != *object {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Commit object ordinal is already bound to different metadata",
                )
                .with_retryable(false));
            }
        }

        for placement in &placements {
            let placement_id = placement_id_for(placement)?;
            sqlx::query(
                "INSERT INTO legacy_placement_objects \
                 (tenant_id, placement_id, object_id, backend_id, storage_volume_id, archive_id, \
                  edge_cluster_id, gateway_pool_id, region, placement_generation, state, \
                  verified_size, verified_digest, failure_domain, created_at_unix_ms, \
                  updated_at_unix_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(tenant_id.as_str())
            .bind(placement_id.as_str())
            .bind(placement.object_id.as_bytes().as_slice())
            .bind(placement.backend_id.as_str())
            .bind(
                placement
                    .storage_volume_id
                    .as_ref()
                    .map(StorageVolumeId::as_str),
            )
            .bind(placement.archive_id.as_ref().map(ArchiveId::as_str))
            .bind(
                placement
                    .edge_cluster_id
                    .as_ref()
                    .map(EdgeClusterId::as_str),
            )
            .bind(
                placement
                    .gateway_pool_id
                    .as_ref()
                    .map(GatewayPoolId::as_str),
            )
            .bind(placement.region.as_ref().map(RegionId::as_str))
            .bind(
                i64::try_from(placement.placement_generation.get())
                    .map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?,
            )
            .bind(placement_state_name(placement.state))
            .bind(
                i64::try_from(placement.verified_size.get())
                    .map_err(|_| protocol_invalid("verified_size exceeds SQLite range"))?,
            )
            .bind(placement.verified_digest.as_bytes().as_slice())
            .bind(&placement.failure_domain)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let stored = sqlx::query(&format!(
                "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM legacy_placement_objects \
                 WHERE tenant_id = ? AND object_id = ? AND backend_id = ? \
                   AND placement_generation = ?",
            ))
            .bind(tenant_id.as_str())
            .bind(placement.object_id.as_bytes().as_slice())
            .bind(placement.backend_id.as_str())
            .bind(
                i64::try_from(placement.placement_generation.get())
                    .map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?,
            )
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if decode_object_placement(&stored)? != *placement {
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "object Placement is already bound to different metadata",
                )
                .with_retryable(false));
            }
        }

        sqlx::query(
            "INSERT INTO legacy_commit_placement_sets \
             (tenant_id, placement_set_id, commit_id, backend_id, storage_volume_id, archive_id, \
              object_set_digest, object_count, verified_object_count, placement_generation, state, \
              created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0) ON CONFLICT DO NOTHING",
        )
        .bind(tenant_id.as_str())
        .bind(placement_set.placement_set_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .bind(placement_set.backend_id.as_str())
        .bind(
            placement_set
                .storage_volume_id
                .as_ref()
                .map(StorageVolumeId::as_str),
        )
        .bind(placement_set.archive_id.as_ref().map(ArchiveId::as_str))
        .bind(placement_set.object_set_digest.as_bytes().as_slice())
        .bind(
            i64::try_from(placement_set.object_count.get())
                .map_err(|_| protocol_invalid("object_count exceeds SQLite range"))?,
        )
        .bind(
            i64::try_from(placement_set.verified_object_count.get())
                .map_err(|_| protocol_invalid("verified_object_count exceeds SQLite range"))?,
        )
        .bind(
            i64::try_from(placement_set.placement_generation.get())
                .map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?,
        )
        .bind(placement_set_state_name(placement_set.state))
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let stored = sqlx::query(&format!(
            "SELECT {PLACEMENT_SET_COLUMNS} FROM legacy_commit_placement_sets \
             WHERE tenant_id = ? AND commit_id = ? AND backend_id = ?",
        ))
        .bind(tenant_id.as_str())
        .bind(commit_id.as_bytes().as_slice())
        .bind(placement_set.backend_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if decode_placement_set(&stored)? != placement_set {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "Commit PlacementSet is already bound to different metadata",
            )
            .with_retryable(false));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok((object_set, placement_set))
    }

    async fn insert_object_placement(
        &self,
        placement: ObjectPlacement,
    ) -> CentralResult<ObjectPlacement> {
        placement.validate().map_err(protocol_invalid)?;
        let tenant_id = placement.tenant_id.clone();
        let placement_id = placement_id_for(&placement)?;
        let result = sqlx::query(
            "INSERT INTO legacy_placement_objects \
             (tenant_id, placement_id, object_id, backend_id, storage_volume_id, archive_id, \
              edge_cluster_id, gateway_pool_id, region, placement_generation, state, verified_size, \
              verified_digest, failure_domain, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)",
        )
        .bind(tenant_id.as_str())
        .bind(placement_id.as_str())
        .bind(placement.object_id.as_bytes().as_slice())
        .bind(placement.backend_id.as_str())
        .bind(placement.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
        .bind(placement.archive_id.as_ref().map(ArchiveId::as_str))
        .bind(placement.edge_cluster_id.as_ref().map(EdgeClusterId::as_str))
        .bind(placement.gateway_pool_id.as_ref().map(GatewayPoolId::as_str))
        .bind(placement.region.as_ref().map(RegionId::as_str))
        .bind(i64::try_from(placement.placement_generation.get()).map_err(|_| {
            CentralError::new(CentralErrorCode::ProtocolInvalid, "placement_generation exceeds SQLite range")
        })?)
        .bind(placement_state_name(placement.state))
        .bind(i64::try_from(placement.verified_size.get()).map_err(|_| {
            CentralError::new(CentralErrorCode::ProtocolInvalid, "verified_size exceeds SQLite range")
        })?)
        .bind(placement.verified_digest.as_bytes().as_slice())
        .bind(&placement.failure_domain)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(placement),
            Err(error) if is_unique(&error) => {
                let row = sqlx::query(&format!(
                    "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM legacy_placement_objects \
                     WHERE tenant_id = ? AND object_id = ? AND backend_id = ? AND placement_generation = ?",
                ))
                .bind(tenant_id.as_str())
                .bind(placement.object_id.as_bytes().as_slice())
                .bind(placement.backend_id.as_str())
                .bind(i64::try_from(placement.placement_generation.get()).map_err(|_| {
                    CentralError::new(CentralErrorCode::ProtocolInvalid, "placement_generation exceeds SQLite range")
                })?)
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| storage_corruption("object placement uniqueness conflict has no row"))?;
                let existing = decode_object_placement(&row)?;
                if existing == placement {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "object Placement is already bound to different metadata",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn set_object_placement_state(
        &self,
        tenant_id: &TenantId,
        object_id: &ObjectId,
        backend_id: &BackendId,
        placement_generation: PlacementGeneration,
        state: PlacementState,
    ) -> CentralResult<ObjectPlacement> {
        // Placement deletion is a one-way GC boundary. Keep SQLite's transition semantics
        // aligned with the in-memory authority: a deleted copy may only be replayed as deleted,
        // never resurrected as readable, retiring, or lost.
        let current = sqlx::query(&format!(
            "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM legacy_placement_objects \
             WHERE tenant_id = ? AND object_id = ? AND backend_id = ? AND placement_generation = ?"
        ))
        .bind(tenant_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .bind(backend_id.as_str())
        .bind(
            i64::try_from(placement_generation.get())
                .map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(|row| decode_object_placement(&row))
        .transpose()?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "object placement does not exist",
            )
            .with_retryable(false)
        })?;
        if matches!(current.state, PlacementState::Deleted)
            && !matches!(state, PlacementState::Deleted)
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "deleted object placement cannot be revived",
            )
            .with_retryable(false));
        }
        let result = sqlx::query(
            "UPDATE legacy_placement_objects SET state = ?, updated_at_unix_ms = updated_at_unix_ms \
             WHERE tenant_id = ? AND object_id = ? AND backend_id = ? AND placement_generation = ?",
        )
        .bind(placement_state_name(state))
        .bind(tenant_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .bind(backend_id.as_str())
        .bind(
            i64::try_from(placement_generation.get())
                .map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?,
        )
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "object placement does not exist",
            )
            .with_retryable(false));
        }
        sqlx::query(&format!(
            "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM legacy_placement_objects \
             WHERE tenant_id = ? AND object_id = ? AND backend_id = ? AND placement_generation = ?"
        ))
        .bind(tenant_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .bind(backend_id.as_str())
        .bind(
            i64::try_from(placement_generation.get())
                .map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(|row| decode_object_placement(&row))
        .transpose()?
        .ok_or_else(|| storage_corruption("updated object placement disappeared"))
    }

    async fn object_placements(
        &self,
        tenant_id: &TenantId,
        object_id: &ObjectId,
    ) -> CentralResult<Vec<ObjectPlacement>> {
        let sql = format!(
            "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM legacy_placement_objects \
             WHERE tenant_id = ? AND object_id = ? ORDER BY backend_id, placement_generation",
        );
        let rows = sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(object_id.as_bytes().as_slice())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        rows.iter().map(decode_object_placement).collect()
    }

    async fn get_replication(
        &self,
        tenant_id: &TenantId,
        replication_id: &ReplicationId,
    ) -> CentralResult<Option<ReplicationRecord>> {
        let sql = format!(
            "SELECT {REPLICATION_COLUMNS} FROM legacy_replications WHERE tenant_id = ? AND replication_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(replication_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(|row| decode_replication(&row))
            .transpose()
    }

    async fn get_replication_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<ReplicationRecord>> {
        let sql = format!(
            "SELECT {REPLICATION_COLUMNS} FROM legacy_replications WHERE tenant_id = ? AND request_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(request_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(|row| decode_replication(&row))
            .transpose()
    }

    async fn list_replications_for_commit(
        &self,
        tenant_id: &TenantId,
        commit_id: &ContentDigest,
    ) -> CentralResult<Vec<ReplicationRecord>> {
        let sql = format!(
            "SELECT {REPLICATION_COLUMNS} FROM legacy_replications \
             WHERE tenant_id = ? AND commit_id = ? ORDER BY created_at_unix_ms, replication_id",
        );
        let rows = sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(commit_id.as_bytes().as_slice())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        rows.iter().map(decode_replication).collect()
    }

    async fn list_replications_for_agent(
        &self,
        tenant_id: &TenantId,
        agent_id: &AgentId,
    ) -> CentralResult<Vec<ReplicationRecord>> {
        let sql = format!(
            "SELECT {REPLICATION_COLUMNS} FROM legacy_replications \
             WHERE tenant_id = ? AND target_agent_id = ? ORDER BY created_at_unix_ms, replication_id",
        );
        let rows = sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(agent_id.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        rows.iter().map(decode_replication).collect()
    }

    async fn refresh_replication_routes(
        &self,
        request: RefreshReplicationRoutesRequest,
    ) -> CentralResult<ReplicationRecord> {
        let current = self
            .get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "replication does not exist",
                )
                .with_retryable(false)
            })?;
        if current.attempt != request.expected_attempt {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            )
            .with_retryable(false));
        }
        if !matches!(
            current.state,
            ReplicationState::Queued
                | ReplicationState::Planning
                | ReplicationState::Transferring
                | ReplicationState::Verifying
        ) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "terminal replication cannot refresh its route",
            )
            .with_retryable(false));
        }
        if !route_binding_matches_record(&current, &request.expected_source, true)
            || !route_binding_matches_record(&current, &request.expected_target, false)
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication route changed concurrently",
            )
            .with_retryable(false));
        }
        if request.source.session_generation.get() == 0
            || request.source.mount_generation.get() == 0
            || request.source.route_generation.get() == 0
            || request.target.session_generation.get() == 0
            || request.target.mount_generation.get() == 0
            || request.target.route_generation.get() == 0
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "replication route generations must be positive",
            )
            .with_retryable(false));
        }
        if request.source.agent_id != request.expected_source.agent_id
            || request.target.agent_id != request.expected_target.agent_id
            || request.source.edge_cluster_id != request.expected_source.edge_cluster_id
            || request.target.edge_cluster_id != request.expected_target.edge_cluster_id
            || request.source.gateway_pool_id != request.expected_source.gateway_pool_id
            || request.target.gateway_pool_id != request.expected_target.gateway_pool_id
            || request.source.mount_generation != request.expected_source.mount_generation
            || request.target.mount_generation != request.expected_target.mount_generation
            || request.source.session_generation.get()
                < request.expected_source.session_generation.get()
            || request.target.session_generation.get()
                < request.expected_target.session_generation.get()
            || request.source.route_generation.get()
                < request.expected_source.route_generation.get()
            || request.target.route_generation.get()
                < request.expected_target.route_generation.get()
        {
            return Err(CentralError::new(
                CentralErrorCode::AssignmentMismatch,
                "replication route refresh cannot change Agent or mount identity",
            )
            .with_retryable(false));
        }
        if request.updated_at_unix_ms < current.updated_at_unix_ms {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            )
            .with_retryable(false));
        }
        if refresh_replication_routes_cas(&self.pool, &request, current.updated_at_unix_ms).await?
            != 1
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication route changed before it could be refreshed",
            )
            .with_retryable(false));
        }
        self.get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| storage_corruption("refreshed replication disappeared"))
    }

    async fn insert_replication(
        &self,
        record: ReplicationRecord,
    ) -> CentralResult<ReplicationRecord> {
        validate_replication_record(&record)?;
        if record.created_at_unix_ms > record.updated_at_unix_ms {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "replication timestamps are out of order",
            ));
        }
        // The replication row and its artifact namespace are one identity.  Keep both writes in
        // the same transaction so a crash cannot leave an assignment-visible row without the
        // scope required to open its artifact CAS.
        let mut transaction = if matches!(
            record.state,
            ReplicationState::Queued
                | ReplicationState::Planning
                | ReplicationState::Transferring
                | ReplicationState::Verifying
        ) {
            // Claim checks must start as a writer transaction.  A deferred transaction can read
            // an unclaimed target, then lose the race to finalize before its INSERT commits.
            self.pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage_error)?
        } else {
            self.pool.begin().await.map_err(storage_error)?
        };
        if matches!(
            record.state,
            ReplicationState::Queued
                | ReplicationState::Planning
                | ReplicationState::Transferring
                | ReplicationState::Verifying
        ) {
            let target_placement_exists: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM legacy_commit_placement_sets \
                 WHERE tenant_id = ? AND commit_id = ? AND backend_id = ? LIMIT 1",
            )
            .bind(record.tenant_id.as_str())
            .bind(record.commit_id.as_bytes().as_slice())
            .bind(&record.target_backend_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if target_placement_exists.is_some() {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(CentralError::new(
                    CentralErrorCode::ReplicationAlreadyActive,
                    "a Commit PlacementSet already targets this backend",
                )
                .with_retryable(false));
            }
        }
        let result = sqlx::query(
            "INSERT INTO legacy_replications \
             (tenant_id, replication_id, commit_id, target_backend_id, target_storage_volume_id, \
              target_archive_id, source_placement_set_id, source_backend_id, source_storage_volume_id, \
              source_edge_cluster_id, source_gateway_pool_id, source_placement_generation, \
              source_agent_id, source_session_generation, source_mount_generation, source_route_generation, \
              target_edge_cluster_id, target_gateway_pool_id, target_placement_generation, \
              target_agent_id, target_session_generation, target_mount_generation, target_route_generation, \
              transfer_route_id, transfer_id, target_placement_set_id, staging_id, object_set_digest, \
              state, request_id, attempt, completed_objects, total_objects, completed_bytes, total_bytes, \
              error_code, error_message, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.replication_id.as_str())
        .bind(record.commit_id.as_bytes().as_slice())
        .bind(&record.target_backend_id)
        .bind(record.target_storage_volume_id.as_str())
        .bind(record.source_placement_set_id.as_ref().map(PlacementSetId::as_str))
        .bind(record.source_backend_id.as_ref().map(BackendId::as_str))
        .bind(record.source_storage_volume_id.as_ref().map(StorageVolumeId::as_str))
        .bind(record.source_edge_cluster_id.as_ref().map(EdgeClusterId::as_str))
        .bind(record.source_gateway_pool_id.as_ref().map(GatewayPoolId::as_str))
        .bind(record.source_placement_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.source_agent_id.as_ref().map(AgentId::as_str))
        .bind(record.source_session_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.source_mount_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.source_route_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.target_edge_cluster_id.as_ref().map(EdgeClusterId::as_str))
        .bind(record.target_gateway_pool_id.as_ref().map(GatewayPoolId::as_str))
        .bind(record.target_placement_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.target_agent_id.as_ref().map(AgentId::as_str))
        .bind(record.target_session_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.target_mount_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.target_route_generation.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)))
        .bind(record.transfer_route_id.as_ref().map(TransferRouteId::as_str))
        .bind(record.transfer_id.as_ref().map(TransferId::as_str))
        .bind(record.target_placement_set_id.as_ref().map(PlacementSetId::as_str))
        .bind(record.staging_id.as_deref())
        .bind(record.object_set_digest.as_bytes().as_slice())
        .bind(replication_state_name(record.state))
        .bind(record.request_id.as_str())
        .bind(i64::try_from(record.attempt).map_err(|_| protocol_invalid("replication attempt exceeds SQLite range"))?)
        .bind(i64::try_from(record.completed_objects).map_err(|_| {
            CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "completed_objects exceeds SQLite range",
            )
        })?)
        .bind(i64::try_from(record.total_objects).map_err(|_| {
            CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "total_objects exceeds SQLite range",
            )
        })?)
        .bind(i64::try_from(record.completed_bytes).map_err(|_| protocol_invalid("completed_bytes exceeds SQLite range"))?)
        .bind(i64::try_from(record.total_bytes).map_err(|_| protocol_invalid("total_bytes exceeds SQLite range"))?)
        .bind(&record.issue_code)
        .bind(&record.issue_message)
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&mut *transaction)
        .await;
        match result {
            Ok(_) => {
                if let Some(artifact_id) = &record.artifact_id {
                    sqlx::query(
                        "INSERT INTO legacy_replication_artifacts (tenant_id, replication_id, artifact_id) \
                         VALUES (?, ?, ?)",
                    )
                    .bind(record.tenant_id.as_str())
                    .bind(record.replication_id.as_str())
                    .bind(artifact_id.as_str())
                    .execute(&mut *transaction)
                    .await
                    .map_err(storage_error)?;
                }
                transaction.commit().await.map_err(storage_error)?;
                Ok(record)
            }
            Err(error) if is_unique(&error) => {
                transaction.rollback().await.map_err(storage_error)?;
                if let Some(existing) = self
                    .get_replication_by_request_id(&record.tenant_id, &record.request_id)
                    .await?
                {
                    return if existing.replication_id == record.replication_id
                        && existing.artifact_id == record.artifact_id
                        && existing.commit_id == record.commit_id
                        && existing.target_storage_volume_id == record.target_storage_volume_id
                        && existing.object_set_digest == record.object_set_digest
                    {
                        Ok(existing)
                    } else {
                        Err(CentralError::new(
                            CentralErrorCode::InvalidState,
                            "replication request ID is already bound to another payload",
                        )
                        .with_retryable(false))
                    };
                }
                if self
                    .list_replications_for_commit(&record.tenant_id, &record.commit_id)
                    .await?
                    .iter()
                    .any(|existing| {
                        existing.target_backend_id == record.target_backend_id
                            && matches!(
                                existing.state,
                                ReplicationState::Queued
                                    | ReplicationState::Planning
                                    | ReplicationState::Transferring
                                    | ReplicationState::Verifying
                            )
                    })
                {
                    return Err(CentralError::new(
                        CentralErrorCode::ReplicationAlreadyActive,
                        "an active replication already targets this Commit and backend",
                    )
                    .with_retryable(false));
                }
                Err(storage_error(error))
            }
            Err(error) => {
                transaction.rollback().await.map_err(storage_error)?;
                Err(storage_error(error))
            }
        }
    }

    async fn transition_replication(
        &self,
        request: ReplicationStateTransitionRequest,
    ) -> CentralResult<ReplicationRecord> {
        if !valid_replication_transition(request.expected_state, request.next_state) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "invalid Replication state transition",
            )
            .with_retryable(false));
        }
        let current = self
            .get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "replication does not exist",
                )
                .with_retryable(false)
            })?;
        if current.state != request.expected_state || current.attempt != request.expected_attempt {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication state or attempt changed concurrently",
            )
            .with_retryable(false));
        }
        if request.completed_objects < current.completed_objects
            || request.completed_bytes < current.completed_bytes
            || request.completed_objects > current.total_objects
            || request.completed_bytes > current.total_bytes
            || request.updated_at_unix_ms < current.updated_at_unix_ms
        {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication progress cannot move backwards or exceed its frozen total",
            )
            .with_retryable(false));
        }
        let result = sqlx::query(
            "UPDATE legacy_replications SET state = ?, completed_objects = ?, completed_bytes = ?, \
             error_code = ?, error_message = ?, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND replication_id = ? AND state = ? AND attempt = ? \
               AND completed_objects = ? AND completed_bytes = ? AND updated_at_unix_ms = ?",
        )
        .bind(replication_state_name(request.next_state))
        .bind(
            i64::try_from(request.completed_objects)
                .map_err(|_| protocol_invalid("completed_objects exceeds SQLite range"))?,
        )
        .bind(
            i64::try_from(request.completed_bytes)
                .map_err(|_| protocol_invalid("completed_bytes exceeds SQLite range"))?,
        )
        .bind(request.issue_code)
        .bind(request.issue_message)
        .bind(as_i64(request.updated_at_unix_ms)?)
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .bind(replication_state_name(request.expected_state))
        .bind(
            i64::try_from(request.expected_attempt)
                .map_err(|_| protocol_invalid("replication attempt exceeds SQLite range"))?,
        )
        .bind(
            i64::try_from(current.completed_objects)
                .map_err(|_| protocol_invalid("completed_objects exceeds SQLite range"))?,
        )
        .bind(
            i64::try_from(current.completed_bytes)
                .map_err(|_| protocol_invalid("completed_bytes exceeds SQLite range"))?,
        )
        .bind(as_i64(current.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() != 1 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication state changed before it could be persisted",
            )
            .with_retryable(false));
        }
        self.get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| storage_corruption("updated replication disappeared"))
    }

    async fn retry_replication(
        &self,
        request: RetryReplicationRequest,
    ) -> CentralResult<RetryReplicationResult> {
        // Retry receipts and the attempt CAS share one writer transaction.  This makes the
        // request ID the linearization point: a duplicate can return its original snapshot even
        // after another retry has advanced the live Replication row.
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage_error)?;
        if let Some(row) = sqlx::query(
            "SELECT request_payload, result_payload FROM legacy_replication_retry_mutations \
             WHERE tenant_id = ? AND request_id = ?",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.request_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        {
            let stored_request: RetryReplicationRequest = decode(
                &row.try_get::<Vec<u8>, _>("request_payload")
                    .map_err(storage_error)?,
            )?;
            let stored_result: ReplicationRecord = decode(
                &row.try_get::<Vec<u8>, _>("result_payload")
                    .map_err(storage_error)?,
            )?;
            transaction.rollback().await.map_err(storage_error)?;
            return if same_retry_request(&stored_request, &request) {
                Ok(RetryReplicationResult {
                    replication: stored_result,
                    replayed: true,
                })
            } else {
                Err(CentralError::new(
                    CentralErrorCode::ReplicationRetryRequestReused,
                    "replication retry request ID is already bound to another payload",
                )
                .with_retryable(false))
            };
        }

        let current = sqlx::query(&format!(
            "SELECT {REPLICATION_COLUMNS} FROM legacy_replications \
             WHERE tenant_id = ? AND replication_id = ?",
        ))
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(|row| decode_replication(&row))
        .transpose()?
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ResourceNotFound,
                "replication does not exist",
            )
            .with_retryable(false)
        })?;
        if current.attempt != request.expected_attempt {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            )
            .with_retryable(false));
        }
        if !matches!(
            current.state,
            ReplicationState::Failed | ReplicationState::Cancelled
        ) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "only failed or cancelled Replications can be retried",
            )
            .with_retryable(false));
        }
        if request.updated_at_unix_ms < current.updated_at_unix_ms {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            )
            .with_retryable(false));
        }
        let next_attempt = current.attempt.checked_add(1).ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "replication attempt exhausted",
            )
            .with_retryable(false)
        })?;
        let active_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM legacy_replications \
             WHERE tenant_id = ? AND commit_id = ? AND target_backend_id = ? \
               AND replication_id <> ? \
               AND state IN ('queued', 'planning', 'transferring', 'verifying') LIMIT 1",
        )
        .bind(current.tenant_id.as_str())
        .bind(current.commit_id.as_bytes().as_slice())
        .bind(&current.target_backend_id)
        .bind(current.replication_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if active_exists.is_some() {
            return Err(CentralError::new(
                CentralErrorCode::ReplicationAlreadyActive,
                "an active replication already targets this Commit and backend",
            )
            .with_retryable(false));
        }
        let target_placement_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM legacy_commit_placement_sets \
             WHERE tenant_id = ? AND commit_id = ? AND backend_id = ? LIMIT 1",
        )
        .bind(current.tenant_id.as_str())
        .bind(current.commit_id.as_bytes().as_slice())
        .bind(&current.target_backend_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if target_placement_exists.is_some() {
            return Err(CentralError::new(
                CentralErrorCode::ReplicationAlreadyActive,
                "a Commit PlacementSet already targets this backend",
            )
            .with_retryable(false));
        }
        let update = sqlx::query(
            "UPDATE legacy_replications SET state = 'queued', attempt = ?, error_code = NULL, \
             error_message = NULL, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND replication_id = ? AND state = ? AND attempt = ?",
        )
        .bind(
            i64::try_from(next_attempt)
                .map_err(|_| protocol_invalid("replication attempt exceeds SQLite range"))?,
        )
        .bind(as_i64(request.updated_at_unix_ms)?)
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .bind(replication_state_name(current.state))
        .bind(
            i64::try_from(request.expected_attempt)
                .map_err(|_| protocol_invalid("replication attempt exceeds SQLite range"))?,
        )
        .execute(&mut *transaction)
        .await;
        let update = match update {
            Ok(result) => result,
            Err(error) if is_unique(&error) => {
                return Err(CentralError::new(
                    CentralErrorCode::ReplicationAlreadyActive,
                    "an active replication already targets this Commit and backend",
                )
                .with_retryable(false));
            }
            Err(error) => return Err(storage_error(error)),
        };
        if update.rows_affected() != 1 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication changed before retry could be persisted",
            )
            .with_retryable(false));
        }
        let result = sqlx::query(&format!(
            "SELECT {REPLICATION_COLUMNS} FROM legacy_replications \
             WHERE tenant_id = ? AND replication_id = ?",
        ))
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(|row| decode_replication(&row))
        .transpose()?
        .ok_or_else(|| storage_corruption("retried replication disappeared"))?;
        sqlx::query(
            "INSERT INTO legacy_replication_retry_mutations \
             (tenant_id, request_id, replication_id, request_payload, result_payload) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.request_id.as_str())
        .bind(request.replication_id.as_str())
        .bind(encode(&request)?)
        .bind(encode(&result)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(RetryReplicationResult {
            replication: result,
            replayed: false,
        })
    }

    async fn cancel_replication(
        &self,
        request: CancelReplicationRequest,
    ) -> CentralResult<ReplicationRecord> {
        let current = self
            .get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "replication does not exist",
                )
                .with_retryable(false)
            })?;
        if current.attempt != request.expected_attempt {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            )
            .with_retryable(false));
        }
        if current.state == ReplicationState::Cancelled {
            return Ok(current);
        }
        if matches!(
            current.state,
            ReplicationState::Published | ReplicationState::Failed
        ) {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "terminal Replication cannot be cancelled",
            )
            .with_retryable(false));
        }
        if request.updated_at_unix_ms < current.updated_at_unix_ms {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            )
            .with_retryable(false));
        }
        if cancel_replication_cas(&self.pool, &request, current.updated_at_unix_ms).await? != 1 {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication changed before cancellation could be persisted",
            )
            .with_retryable(false));
        }
        self.get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| storage_corruption("cancelled replication disappeared"))
    }

    async fn finalize_replication(
        &self,
        request: FinalizeReplicationRequest,
    ) -> CentralResult<FinalizeReplicationResult> {
        let current = self
            .get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ResourceNotFound,
                    "replication does not exist",
                )
                .with_retryable(false)
            })?;
        if current.attempt != request.expected_attempt {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication attempt changed concurrently",
            )
            .with_retryable(false));
        }
        if current.state == ReplicationState::Published {
            if current.target_placement_set_id.as_ref()
                == Some(&request.placement_set.placement_set_id)
            {
                let stored = self
                    .get_placement_set(
                        &request.tenant_id,
                        &current.commit_id,
                        &request.placement_set.backend_id,
                    )
                    .await?;
                if stored.as_ref() == Some(&request.placement_set) {
                    return Ok(FinalizeReplicationResult {
                        replication: current,
                        placement_set: request.placement_set,
                        replayed: true,
                    });
                }
            }
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "published Replication is bound to another PlacementSet",
            )
            .with_retryable(false));
        }
        if current.state != ReplicationState::Verifying {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "only verifying Replications can be finalized",
            )
            .with_retryable(false));
        }
        if current.completed_objects != current.total_objects
            || current.completed_bytes != current.total_bytes
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "all replication objects must be verified before finalize",
            )
            .with_retryable(false));
        }
        if request.finalized_at_unix_ms < current.updated_at_unix_ms {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication timestamp cannot move backwards",
            )
            .with_retryable(false));
        }
        let object_set = self
            .get_commit_object_set(&request.tenant_id, &current.commit_id)
            .await?
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Commit ObjectSet is missing",
                )
                .with_retryable(false)
            })?;
        validate_replication_publication(
            &current,
            &object_set,
            &request.placements,
            &request.placement_set,
        )?;

        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // Acquire SQLite's writer lock before reading checkpoints. This keeps a concurrent Agent
        // checkpoint update from landing between validation and the publication writes.
        let fence = sqlx::query(
            "UPDATE legacy_replications SET updated_at_unix_ms = updated_at_unix_ms \
             WHERE tenant_id = ? AND replication_id = ? AND state = 'verifying' AND attempt = ?",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .bind(
            i64::try_from(request.expected_attempt)
                .map_err(|_| protocol_invalid("replication attempt exceeds SQLite range"))?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if fence.rows_affected() != 1 {
            transaction.rollback().await.map_err(storage_error)?;
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication changed before checkpoint validation",
            )
            .with_retryable(false));
        }
        let current_row = sqlx::query(
            "SELECT state, attempt FROM legacy_replications WHERE tenant_id = ? AND replication_id = ?",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| storage_corruption("replication disappeared during finalize"))?;
        let current_state = current_row
            .try_get::<String, _>("state")
            .map_err(storage_error)?;
        let current_attempt = current_row
            .try_get::<i64, _>("attempt")
            .map_err(storage_error)?;
        if current_state != "verifying"
            || u64::try_from(current_attempt)
                .map_err(|_| storage_corruption("stored replication attempt is negative"))?
                != request.expected_attempt
        {
            transaction.rollback().await.map_err(storage_error)?;
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication state or attempt changed during finalize",
            )
            .with_retryable(false));
        }
        let conflicting_active_target: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM legacy_replications \
             WHERE tenant_id = ? AND commit_id = ? AND target_backend_id = ? \
               AND replication_id <> ? \
               AND state IN ('queued', 'planning', 'transferring', 'verifying') LIMIT 1",
        )
        .bind(request.tenant_id.as_str())
        .bind(current.commit_id.as_bytes().as_slice())
        .bind(request.placement_set.backend_id.as_str())
        .bind(request.replication_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if conflicting_active_target.is_some() {
            transaction.rollback().await.map_err(storage_error)?;
            return Err(CentralError::new(
                CentralErrorCode::ReplicationAlreadyActive,
                "an active replication already targets this Commit and backend",
            )
            .with_retryable(false));
        }
        let checkpoint_rows = sqlx::query(
            "SELECT tenant_id, replication_id, object_id, offset, state, retry_count, updated_at_unix_ms \
             FROM legacy_replication_objects WHERE tenant_id = ? AND replication_id = ? ORDER BY object_id",
        )
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let checkpoints = checkpoint_rows
            .iter()
            .map(decode_replication_object)
            .collect::<CentralResult<Vec<_>>>()?;
        validate_replication_checkpoints(&current, &object_set, &checkpoints)?;

        for placement in &request.placements {
            let placement_id = placement_id_for(placement)?;
            sqlx::query(
                "INSERT INTO legacy_placement_objects \
                 (tenant_id, placement_id, object_id, backend_id, storage_volume_id, archive_id, \
                  edge_cluster_id, gateway_pool_id, region, placement_generation, state, \
                  verified_size, verified_digest, failure_domain, created_at_unix_ms, updated_at_unix_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0) ON CONFLICT DO NOTHING",
            )
            .bind(placement.tenant_id.as_str())
            .bind(placement_id.as_str())
            .bind(placement.object_id.as_bytes().as_slice())
            .bind(placement.backend_id.as_str())
            .bind(placement.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
            .bind(placement.archive_id.as_ref().map(ArchiveId::as_str))
            .bind(placement.edge_cluster_id.as_ref().map(EdgeClusterId::as_str))
            .bind(placement.gateway_pool_id.as_ref().map(GatewayPoolId::as_str))
            .bind(placement.region.as_ref().map(RegionId::as_str))
            .bind(i64::try_from(placement.placement_generation.get()).map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?)
            .bind(placement_state_name(placement.state))
            .bind(i64::try_from(placement.verified_size.get()).map_err(|_| protocol_invalid("verified_size exceeds SQLite range"))?)
            .bind(placement.verified_digest.as_bytes().as_slice())
            .bind(&placement.failure_domain)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            let stored = sqlx::query(&format!(
                "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM legacy_placement_objects \
                 WHERE tenant_id = ? AND object_id = ? AND backend_id = ? AND placement_generation = ?",
            ))
            .bind(placement.tenant_id.as_str())
            .bind(placement.object_id.as_bytes().as_slice())
            .bind(placement.backend_id.as_str())
            .bind(i64::try_from(placement.placement_generation.get()).map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| storage_corruption("target object Placement disappeared during finalize"))?;
            if decode_object_placement(&stored)? != *placement {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(CentralError::new(
                    CentralErrorCode::InvalidState,
                    "target ObjectPlacement is already bound to different metadata",
                )
                .with_retryable(false));
            }
        }

        let placement_set = &request.placement_set;
        sqlx::query(
            "INSERT INTO legacy_commit_placement_sets \
             (tenant_id, placement_set_id, commit_id, backend_id, storage_volume_id, archive_id, \
              object_set_digest, object_count, verified_object_count, placement_generation, state, \
              created_at_unix_ms, updated_at_unix_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0) \
              ON CONFLICT DO NOTHING",
        )
        .bind(placement_set.tenant_id.as_str())
        .bind(placement_set.placement_set_id.as_str())
        .bind(placement_set.commit_id.digest().as_bytes().as_slice())
        .bind(placement_set.backend_id.as_str())
        .bind(placement_set.storage_volume_id.as_ref().map(StorageVolumeId::as_str))
        .bind(placement_set.archive_id.as_ref().map(ArchiveId::as_str))
        .bind(placement_set.object_set_digest.as_bytes().as_slice())
        .bind(i64::try_from(placement_set.object_count.get()).map_err(|_| protocol_invalid("object_count exceeds SQLite range"))?)
        .bind(i64::try_from(placement_set.verified_object_count.get()).map_err(|_| protocol_invalid("verified_object_count exceeds SQLite range"))?)
        .bind(i64::try_from(placement_set.placement_generation.get()).map_err(|_| protocol_invalid("placement_generation exceeds SQLite range"))?)
        .bind(placement_set_state_name(placement_set.state))
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let stored_set = sqlx::query(&format!(
            "SELECT {PLACEMENT_SET_COLUMNS} FROM legacy_commit_placement_sets \
             WHERE tenant_id = ? AND commit_id = ? AND backend_id = ?",
        ))
        .bind(placement_set.tenant_id.as_str())
        .bind(placement_set.commit_id.digest().as_bytes().as_slice())
        .bind(placement_set.backend_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| storage_corruption("target PlacementSet disappeared during finalize"))?;
        if decode_placement_set(&stored_set)? != *placement_set {
            transaction.rollback().await.map_err(storage_error)?;
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "target PlacementSet is already bound to different metadata",
            )
            .with_retryable(false));
        }
        let result = sqlx::query(
            "UPDATE legacy_replications SET state = 'published', target_placement_set_id = ?, \
             completed_objects = total_objects, completed_bytes = total_bytes, error_code = NULL, \
             error_message = NULL, updated_at_unix_ms = ? \
             WHERE tenant_id = ? AND replication_id = ? AND state = 'verifying' AND attempt = ?",
        )
        .bind(placement_set.placement_set_id.as_str())
        .bind(as_i64(request.finalized_at_unix_ms)?)
        .bind(request.tenant_id.as_str())
        .bind(request.replication_id.as_str())
        .bind(
            i64::try_from(request.expected_attempt)
                .map_err(|_| protocol_invalid("replication attempt exceeds SQLite range"))?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() != 1 {
            transaction.rollback().await.map_err(storage_error)?;
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "replication changed before publication could be persisted",
            )
            .with_retryable(false));
        }
        transaction.commit().await.map_err(storage_error)?;
        let replication = self
            .get_replication(&request.tenant_id, &request.replication_id)
            .await?
            .ok_or_else(|| storage_corruption("published replication disappeared"))?;
        Ok(FinalizeReplicationResult {
            replication,
            placement_set: placement_set.clone(),
            replayed: false,
        })
    }

    async fn upsert_replication_object(
        &self,
        record: ReplicationObjectRecord,
    ) -> CentralResult<ReplicationObjectRecord> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // Acquire the writer lock while fencing terminal Replications. This makes the state check
        // and checkpoint write one atomic operation relative to finalize/cancel.
        let fence =
            sqlx::query(
                "UPDATE legacy_replications SET updated_at_unix_ms = updated_at_unix_ms \
             WHERE tenant_id = ? AND replication_id = ? AND attempt = ? \
               AND state NOT IN ('published', 'failed', 'cancelled')",
            )
            .bind(record.tenant_id.as_str())
            .bind(record.replication_id.as_str())
            .bind(i64::try_from(record.retry_count).map_err(|_| {
                protocol_invalid("replication object retry_count exceeds SQLite range")
            })?)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        if fence.rows_affected() != 1 {
            transaction.rollback().await.map_err(storage_error)?;
            let current = self
                .get_replication(&record.tenant_id, &record.replication_id)
                .await?;
            let (code, message) = match current {
                Some(replication) if replication.attempt != record.retry_count => (
                    CentralErrorCode::ConcurrentUpdate,
                    "replication object checkpoint attempt is stale",
                ),
                _ => (
                    CentralErrorCode::InvalidState,
                    "Replication is missing or terminal; object checkpoints are no longer accepted",
                ),
            };
            return Err(CentralError::new(code, message).with_retryable(false));
        }
        let object_id = record.object_id;
        let existing = sqlx::query(
            "SELECT tenant_id, replication_id, object_id, offset, state, retry_count, updated_at_unix_ms \
             FROM legacy_replication_objects WHERE tenant_id = ? AND replication_id = ? AND object_id = ?",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.replication_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(|row| decode_replication_object(&row))
        .transpose()?;
        if let Some(existing) = existing {
            if existing == record {
                return Ok(existing);
            }
            if record.offset < existing.offset
                || record.retry_count < existing.retry_count
                || record.updated_at_unix_ms < existing.updated_at_unix_ms
            {
                return Err(CentralError::new(
                    CentralErrorCode::ConcurrentUpdate,
                    "replication object checkpoint cannot move backwards",
                )
                .with_retryable(false));
            }
        }
        sqlx::query(
            "INSERT INTO legacy_replication_objects \
             (tenant_id, replication_id, object_id, offset, state, retry_count, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, replication_id, object_id) DO UPDATE SET \
             offset = excluded.offset, state = excluded.state, retry_count = excluded.retry_count, \
             updated_at_unix_ms = excluded.updated_at_unix_ms",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.replication_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .bind(i64::try_from(record.offset).map_err(|_| protocol_invalid("replication object offset exceeds SQLite range"))?)
        .bind(replication_object_state_name(record.state))
        .bind(i64::try_from(record.retry_count).map_err(|_| protocol_invalid("replication object retry_count exceeds SQLite range"))?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn list_replication_objects(
        &self,
        tenant_id: &TenantId,
        replication_id: &ReplicationId,
    ) -> CentralResult<Vec<ReplicationObjectRecord>> {
        let rows = sqlx::query(
            "SELECT tenant_id, replication_id, object_id, offset, state, retry_count, updated_at_unix_ms \
             FROM legacy_replication_objects WHERE tenant_id = ? AND replication_id = ? ORDER BY object_id",
        )
        .bind(tenant_id.as_str())
        .bind(replication_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.iter().map(decode_replication_object).collect()
    }

    async fn get_workspace(
        &self,
        tenant_id: &TenantId,
        workspace_id: &WorkspaceId,
    ) -> CentralResult<Option<WorkspaceRecord>> {
        let sql = format!(
            "SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE tenant_id = ? AND workspace_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(workspace_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(|row| decode_workspace(&row))
            .transpose()
    }

    async fn get_workspace_by_request_id(
        &self,
        tenant_id: &TenantId,
        request_id: &RequestId,
    ) -> CentralResult<Option<WorkspaceRecord>> {
        let sql = format!(
            "SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE tenant_id = ? AND request_id = ?"
        );
        sqlx::query(&sql)
            .bind(tenant_id.as_str())
            .bind(request_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(|row| decode_workspace(&row))
            .transpose()
    }

    async fn insert_workspace(&self, record: WorkspaceRecord) -> CentralResult<WorkspaceRecord> {
        if record.created_at_unix_ms > record.updated_at_unix_ms {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "workspace timestamps are out of order",
            ));
        }
        let result = sqlx::query(
            "INSERT INTO workspaces \
             (tenant_id, workspace_id, request_id, project_id, artifact_id, base_commit_id, \
              target_storage_volume_id, lifecycle, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.workspace_id.as_str())
        .bind(record.request_id.as_str())
        .bind(record.project_id.as_str())
        .bind(record.artifact_id.as_str())
        .bind(
            record
                .base_commit_id
                .map(|digest| digest.as_bytes().to_vec()),
        )
        .bind(record.target_storage_volume_id.as_str())
        .bind(workspace_lifecycle_name(record.lifecycle))
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(record),
            Err(error) if is_unique(&error) => {
                let existing = self
                    .get_workspace_by_request_id(&record.tenant_id, &record.request_id)
                    .await?
                    .ok_or_else(|| {
                        storage_corruption("workspace uniqueness conflict has no row")
                    })?;
                if existing.workspace_id == record.workspace_id
                    && existing.project_id == record.project_id
                    && existing.artifact_id == record.artifact_id
                    && existing.base_commit_id == record.base_commit_id
                    && existing.target_storage_volume_id == record.target_storage_volume_id
                {
                    Ok(existing)
                } else {
                    Err(CentralError::new(
                        CentralErrorCode::InvalidState,
                        "workspace request ID is already bound to another payload",
                    )
                    .with_retryable(false))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn commit_availability(
        &self,
        tenant_id: &TenantId,
        commit_id: &ContentDigest,
    ) -> CentralResult<CommitAvailabilityRecord> {
        let Some(object_set) = self.get_commit_object_set(tenant_id, commit_id).await? else {
            return Ok(CommitAvailabilityRecord {
                tenant_id: tenant_id.clone(),
                commit_id: *commit_id,
                data_health: DataHealth::Unavailable,
                verified_placements: 0,
                missing_objects: 0,
                verified_storage_volume_ids: Vec::new(),
            });
        };
        // A complete placement set is identified by backend, physical identity and
        // generation. Objects from different generations must never be combined into
        // a synthetic replica.  The candidate count includes archive-backed sets; the
        // response's volume list remains a convenience projection for local callers.
        let object_count = u64::try_from(object_set.object_set.objects.len())
            .map_err(|_| storage_corruption("object count exceeds u64"))?;
        let mut verified_candidates =
            std::collections::BTreeMap::<(String, Option<String>, Option<String>, u64), u64>::new();
        for placement in self.published_placement_sets(tenant_id, commit_id).await? {
            if placement.object_set_digest == object_set.object_set.object_set_digest
                && placement.object_count.get() == object_count
            {
                verified_candidates
                    .entry((
                        placement.backend_id.to_string(),
                        placement.storage_volume_id.map(|value| value.to_string()),
                        placement.archive_id.map(|value| value.to_string()),
                        placement.placement_generation.get(),
                    ))
                    .or_default();
            }
        }
        let mut missing_objects = 0_u64;
        let mut degraded = false;
        for object in &object_set.object_set.objects {
            let rows = sqlx::query(
                "SELECT p.backend_id, p.state, p.storage_volume_id, p.archive_id, p.placement_generation, \
                        p.verified_size, p.verified_digest FROM legacy_placement_objects p \
                 WHERE p.tenant_id = ? AND p.object_id = ? \
                   AND EXISTS (SELECT 1 FROM legacy_commit_placement_sets s \
                     WHERE s.tenant_id = p.tenant_id AND s.commit_id = ? \
                       AND s.backend_id = p.backend_id \
                       AND s.placement_generation = p.placement_generation \
                       AND (s.storage_volume_id = p.storage_volume_id \
                            OR (s.storage_volume_id IS NULL AND p.storage_volume_id IS NULL)) \
                       AND (s.archive_id = p.archive_id \
                            OR (s.archive_id IS NULL AND p.archive_id IS NULL)) \
                       AND s.state = 'published')",
            )
            .bind(tenant_id.as_str())
            .bind(object.object_id.as_bytes().as_slice())
            .bind(commit_id.as_bytes().as_slice())
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
            let mut verified = 0_u64;
            for row in &rows {
                let backend = row
                    .try_get::<String, _>("backend_id")
                    .map_err(storage_error)?;
                let state = row.try_get::<String, _>("state").map_err(storage_error)?;
                let volume = row
                    .try_get::<Option<String>, _>("storage_volume_id")
                    .map_err(storage_error)?;
                let archive = row
                    .try_get::<Option<String>, _>("archive_id")
                    .map_err(storage_error)?;
                let generation = u64::try_from(
                    row.try_get::<i64, _>("placement_generation")
                        .map_err(storage_error)?,
                )
                .map_err(|_| storage_corruption("stored placement generation is negative"))?;
                let verified_size = u64::try_from(
                    row.try_get::<i64, _>("verified_size")
                        .map_err(storage_error)?,
                )
                .map_err(|_| storage_corruption("stored verified placement size is negative"))?;
                let verified_digest = row
                    .try_get::<Vec<u8>, _>("verified_digest")
                    .map_err(storage_error)?;
                let metadata_matches = verified_size == object.size.get()
                    && verified_digest.as_slice() == object.object_id.digest().as_bytes();
                if state == "verified" && metadata_matches {
                    verified += 1;
                    let key = (backend, volume, archive, generation);
                    *verified_candidates.entry(key).or_default() += 1;
                } else {
                    // A row marked verified with corrupt metadata is not readable.  Keep
                    // the Commit degraded when another complete copy exists, or unavailable
                    // when this was the last readable copy.
                    degraded = true;
                }
            }
            if verified == 0 {
                missing_objects = missing_objects
                    .checked_add(1)
                    .ok_or_else(|| storage_corruption("missing object count exceeds u64"))?;
            }
        }
        let complete_candidates = verified_candidates
            .into_iter()
            .filter(|(_, count)| *count == object_count)
            .collect::<Vec<_>>();
        let verified_storage_volume_ids = complete_candidates
            .iter()
            .filter_map(|((_, volume, _, _), _)| volume.clone())
            .map(|volume| {
                StorageVolumeId::new(volume).map_err(|error| {
                    storage_corruption(format!("stored placement volume ID: {error}"))
                })
            })
            .collect::<CentralResult<Vec<_>>>()?;
        let data_health = if missing_objects > 0 {
            DataHealth::Unavailable
        } else if degraded {
            DataHealth::Degraded
        } else {
            DataHealth::Available
        };
        Ok(CommitAvailabilityRecord {
            tenant_id: tenant_id.clone(),
            commit_id: *commit_id,
            data_health,
            verified_placements: u64::try_from(complete_candidates.len())
                .map_err(|_| storage_corruption("verified placement count exceeds u64"))?,
            missing_objects,
            verified_storage_volume_ids,
        })
    }
}

fn is_unique(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == "2067" || code == "1555" || code == "19")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{open_sqlite_authority, SqliteAuthorityConfig};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use tempfile::TempDir;

    #[tokio::test]
    async fn replication_and_workspace_records_survive_reopen() {
        let directory = TempDir::new().unwrap();
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let store = authority.authority_store().placement().unwrap();
        let tenant_id = TenantId::new("tenant-placement").unwrap();
        let request_id = RequestId::new("replication-request-1").unwrap();
        let commit_id = ContentDigest::from_bytes([7; 32]);
        let now = UnixMillis::new(10);
        let record = ReplicationRecord {
            tenant_id: tenant_id.clone(),
            replication_id: ReplicationId::new("replication-test-1").unwrap(),
            artifact_id: Some(ArtifactId::new("artifact-a").unwrap()),
            commit_id,
            target_backend_id: "volume-backend-a".to_owned(),
            target_storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            source_placement_set_id: None,
            source_backend_id: None,
            source_storage_volume_id: None,
            source_edge_cluster_id: None,
            source_gateway_pool_id: None,
            source_placement_generation: None,
            source_agent_id: None,
            source_session_generation: None,
            source_mount_generation: None,
            source_route_generation: None,
            target_edge_cluster_id: None,
            target_gateway_pool_id: None,
            target_placement_generation: None,
            target_agent_id: None,
            target_session_generation: None,
            target_mount_generation: None,
            target_route_generation: None,
            transfer_route_id: None,
            transfer_id: None,
            target_placement_set_id: None,
            staging_id: None,
            object_set_digest: ContentDigest::from_bytes([8; 32]),
            state: ReplicationState::Queued,
            request_id: request_id.clone(),
            attempt: 1,
            completed_objects: 0,
            total_objects: 3,
            completed_bytes: 0,
            total_bytes: 17,
            issue_code: None,
            issue_message: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let inserted = store.insert_replication(record.clone()).await.unwrap();
        assert_eq!(inserted, record);
        let replication_object = ReplicationObjectRecord {
            tenant_id: tenant_id.clone(),
            replication_id: record.replication_id.clone(),
            object_id: ObjectId::from_bytes([9; 32]),
            offset: 128,
            state: ReplicationObjectState::Transferring,
            retry_count: 1,
            updated_at_unix_ms: UnixMillis::new(11),
        };
        assert_eq!(
            store
                .upsert_replication_object(replication_object.clone())
                .await
                .unwrap(),
            replication_object
        );
        assert_eq!(
            store
                .list_replication_objects(&tenant_id, &record.replication_id)
                .await
                .unwrap(),
            vec![replication_object.clone()]
        );
        assert_eq!(
            store
                .get_replication_by_request_id(&tenant_id, &request_id)
                .await
                .unwrap(),
            Some(record)
        );
        let workspace = WorkspaceRecord {
            tenant_id: tenant_id.clone(),
            workspace_id: WorkspaceId::new("workspace-test-1").unwrap(),
            project_id: neoengram_domain::protocol::ProjectId::new("project-a").unwrap(),
            artifact_id: neoengram_domain::protocol::ArtifactId::new("artifact-a").unwrap(),
            base_commit_id: Some(commit_id),
            target_storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            request_id: RequestId::new("workspace-request-1").unwrap(),
            lifecycle: WorkspaceLifecycle::Provisioning,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        store.insert_workspace(workspace.clone()).await.unwrap();
        assert_eq!(
            store
                .get_workspace(&tenant_id, &workspace.workspace_id)
                .await
                .unwrap(),
            Some(workspace)
        );
        authority.close().await;
        let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let reopened_store = reopened.authority_store().placement().unwrap();
        assert!(reopened_store
            .get_replication(
                &tenant_id,
                &ReplicationId::new("replication-test-1").unwrap()
            )
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            reopened_store
                .list_replication_objects(
                    &tenant_id,
                    &ReplicationId::new("replication-test-1").unwrap(),
                )
                .await
                .unwrap(),
            vec![replication_object]
        );
        reopened.close().await;
    }

    #[tokio::test]
    async fn route_refresh_and_cancel_cas_reject_stale_reads_in_both_directions() {
        let directory = TempDir::new().unwrap();
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let store = authority.authority_store().placement().unwrap();
        let tenant_id = TenantId::new("tenant-route-cancel-race").unwrap();
        let now = UnixMillis::new(10);
        let old_source = ReplicationRouteBinding {
            edge_cluster_id: EdgeClusterId::new("edge-source-race").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-source-race").unwrap(),
            agent_id: AgentId::new("agent-source-race").unwrap(),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(2),
            route_generation: RouteGeneration::new(3),
        };
        let old_target = ReplicationRouteBinding {
            edge_cluster_id: EdgeClusterId::new("edge-target-race").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-target-race").unwrap(),
            agent_id: AgentId::new("agent-target-race").unwrap(),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(2),
            route_generation: RouteGeneration::new(4),
        };
        let record = ReplicationRecord {
            tenant_id: tenant_id.clone(),
            replication_id: ReplicationId::new("replication-route-cancel-race").unwrap(),
            artifact_id: Some(ArtifactId::new("artifact-route-cancel-race").unwrap()),
            commit_id: ContentDigest::from_bytes([0x57; 32]),
            target_backend_id: "backend-target-race".to_owned(),
            target_storage_volume_id: StorageVolumeId::new("volume-target-race").unwrap(),
            source_placement_set_id: Some(PlacementSetId::new("placement-source-race").unwrap()),
            source_backend_id: Some(BackendId::new("backend-source-race").unwrap()),
            source_storage_volume_id: Some(StorageVolumeId::new("volume-source-race").unwrap()),
            source_edge_cluster_id: Some(old_source.edge_cluster_id.clone()),
            source_gateway_pool_id: Some(old_source.gateway_pool_id.clone()),
            source_placement_generation: Some(PlacementGeneration::new(1)),
            source_agent_id: Some(old_source.agent_id.clone()),
            source_session_generation: Some(old_source.session_generation),
            source_mount_generation: Some(old_source.mount_generation),
            source_route_generation: Some(old_source.route_generation),
            target_edge_cluster_id: Some(old_target.edge_cluster_id.clone()),
            target_gateway_pool_id: Some(old_target.gateway_pool_id.clone()),
            target_placement_generation: Some(PlacementGeneration::new(1)),
            target_agent_id: Some(old_target.agent_id.clone()),
            target_session_generation: Some(old_target.session_generation),
            target_mount_generation: Some(old_target.mount_generation),
            target_route_generation: Some(old_target.route_generation),
            transfer_route_id: None,
            transfer_id: None,
            target_placement_set_id: None,
            staging_id: None,
            object_set_digest: ContentDigest::from_bytes([0x58; 32]),
            state: ReplicationState::Transferring,
            request_id: RequestId::new("request-route-cancel-race").unwrap(),
            attempt: 1,
            completed_objects: 0,
            total_objects: 1,
            completed_bytes: 0,
            total_bytes: 1,
            issue_code: None,
            issue_message: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        store.insert_replication(record.clone()).await.unwrap();

        // Both operations below begin from this same stale record snapshot.
        let stale_updated_at = store
            .get_replication(&tenant_id, &record.replication_id)
            .await
            .unwrap()
            .unwrap()
            .updated_at_unix_ms;
        let new_source = ReplicationRouteBinding {
            session_generation: SessionGeneration::new(5),
            route_generation: RouteGeneration::new(6),
            ..old_source.clone()
        };
        let new_target = ReplicationRouteBinding {
            session_generation: SessionGeneration::new(7),
            route_generation: RouteGeneration::new(8),
            ..old_target.clone()
        };
        let request = RefreshReplicationRoutesRequest {
            tenant_id: tenant_id.clone(),
            replication_id: record.replication_id.clone(),
            expected_attempt: 1,
            expected_source: old_source.clone(),
            expected_target: old_target.clone(),
            source: new_source.clone(),
            target: new_target.clone(),
            updated_at_unix_ms: UnixMillis::new(20),
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new().filename(directory.path().join("authority.sqlite3")),
            )
            .await
            .unwrap();
        let affected = refresh_replication_routes_cas(&pool, &request, stale_updated_at)
            .await
            .unwrap();
        assert_eq!(affected, 1);

        // A cancellation that read the old timestamp must not overwrite the newer route refresh
        // with an older timestamp while the attempt and active state still happen to match.
        let stale_cancel = CancelReplicationRequest {
            tenant_id: tenant_id.clone(),
            replication_id: record.replication_id.clone(),
            expected_attempt: 1,
            updated_at_unix_ms: UnixMillis::new(15),
        };
        assert_eq!(
            cancel_replication_cas(&pool, &stale_cancel, stale_updated_at)
                .await
                .unwrap(),
            0
        );

        let refreshed = store
            .get_replication(&tenant_id, &record.replication_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(refreshed.state, ReplicationState::Transferring);
        assert_eq!(refreshed.updated_at_unix_ms, UnixMillis::new(20));
        assert_eq!(
            refreshed.source_session_generation,
            Some(new_source.session_generation)
        );
        assert_eq!(
            refreshed.target_session_generation,
            Some(new_target.session_generation)
        );

        // Conversely, a route refresh that read the active row cannot write after cancellation,
        // even when both mutations use the same millisecond timestamp.
        store
            .cancel_replication(CancelReplicationRequest {
                tenant_id: tenant_id.clone(),
                replication_id: record.replication_id.clone(),
                expected_attempt: 1,
                updated_at_unix_ms: UnixMillis::new(20),
            })
            .await
            .unwrap();
        let post_cancel_refresh = RefreshReplicationRoutesRequest {
            tenant_id: tenant_id.clone(),
            replication_id: record.replication_id.clone(),
            expected_attempt: 1,
            expected_source: new_source.clone(),
            expected_target: new_target.clone(),
            source: ReplicationRouteBinding {
                session_generation: SessionGeneration::new(9),
                route_generation: RouteGeneration::new(10),
                ..new_source
            },
            target: ReplicationRouteBinding {
                session_generation: SessionGeneration::new(11),
                route_generation: RouteGeneration::new(12),
                ..new_target
            },
            updated_at_unix_ms: UnixMillis::new(20),
        };
        assert_eq!(
            refresh_replication_routes_cas(&pool, &post_cancel_refresh, UnixMillis::new(20))
                .await
                .unwrap(),
            0
        );
        let cancelled = store
            .get_replication(&tenant_id, &record.replication_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.state, ReplicationState::Cancelled);
        assert_eq!(cancelled.updated_at_unix_ms, UnixMillis::new(20));
        pool.close().await;
        authority.close().await;
    }

    #[tokio::test]
    async fn object_set_and_published_placement_drive_availability() {
        let directory = TempDir::new().unwrap();
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let store = authority.authority_store().placement().unwrap();
        let tenant_id = TenantId::new("tenant-health").unwrap();
        let object_id = ObjectId::from_bytes([3; 32]);
        let commit_id = ContentDigest::from_bytes([4; 32]);
        let object_set = ObjectSet::new(vec![CommitObject::new(
            object_id,
            12,
            ObjectEncoding::Raw,
            0,
        )])
        .unwrap();
        store
            .insert_commit_object_set(CommitObjectSet {
                tenant_id: tenant_id.clone(),
                commit_id: CommitId::from_digest(commit_id),
                object_set: object_set.clone(),
            })
            .await
            .unwrap();
        let placement = ObjectPlacement {
            tenant_id: tenant_id.clone(),
            object_id,
            backend_id: BackendId::new("backend-health").unwrap(),
            storage_volume_id: Some(StorageVolumeId::new("volume-health").unwrap()),
            archive_id: None,
            edge_cluster_id: None,
            gateway_pool_id: None,
            region: None,
            placement_generation: PlacementGeneration::new(1),
            state: PlacementState::Verified,
            verified_size: DecimalU64::new(12),
            verified_digest: object_id.digest(),
            failure_domain: "host-health".to_owned(),
        };
        store.insert_object_placement(placement).await.unwrap();
        store
            .insert_placement_set(CommitPlacementSet {
                placement_set_id: PlacementSetId::new("placement-set-health").unwrap(),
                tenant_id: tenant_id.clone(),
                commit_id: CommitId::from_digest(commit_id),
                backend_id: BackendId::new("backend-health").unwrap(),
                storage_volume_id: Some(StorageVolumeId::new("volume-health").unwrap()),
                archive_id: None,
                object_set_digest: object_set.object_set_digest,
                object_count: DecimalU64::new(1),
                verified_object_count: DecimalU64::new(1),
                placement_generation: PlacementGeneration::new(1),
                state: CommitPlacementSetState::Published,
            })
            .await
            .unwrap();
        let availability = store
            .commit_availability(&tenant_id, &commit_id)
            .await
            .unwrap();
        assert_eq!(availability.data_health, DataHealth::Available);
        assert_eq!(availability.verified_placements, 1);
        assert_eq!(availability.missing_objects, 0);

        // An object from a newer placement generation must not satisfy an older published
        // PlacementSet.  Generation fencing keeps stale copies out of availability decisions.
        store
            .insert_object_placement(ObjectPlacement {
                tenant_id: tenant_id.clone(),
                object_id,
                backend_id: BackendId::new("backend-health").unwrap(),
                storage_volume_id: Some(StorageVolumeId::new("volume-health").unwrap()),
                archive_id: None,
                edge_cluster_id: None,
                gateway_pool_id: None,
                region: None,
                placement_generation: PlacementGeneration::new(2),
                state: PlacementState::Verified,
                verified_size: DecimalU64::new(12),
                verified_digest: object_id.digest(),
                failure_domain: "host-health".to_owned(),
            })
            .await
            .unwrap();
        let fenced = store
            .commit_availability(&tenant_id, &commit_id)
            .await
            .unwrap();
        assert_eq!(fenced.data_health, DataHealth::Available);
        assert_eq!(fenced.verified_placements, 1);
        assert_eq!(fenced.missing_objects, 0);

        store
            .set_object_placement_state(
                &tenant_id,
                &object_id,
                &BackendId::new("backend-health").unwrap(),
                PlacementGeneration::new(1),
                PlacementState::Lost,
            )
            .await
            .unwrap();
        let unavailable = store
            .commit_availability(&tenant_id, &commit_id)
            .await
            .unwrap();
        assert_eq!(unavailable.data_health, DataHealth::Unavailable);
        assert_eq!(unavailable.missing_objects, 1);
        store
            .set_object_placement_state(
                &tenant_id,
                &object_id,
                &BackendId::new("backend-health").unwrap(),
                PlacementGeneration::new(1),
                PlacementState::Deleted,
            )
            .await
            .unwrap();
        let revive = store
            .set_object_placement_state(
                &tenant_id,
                &object_id,
                &BackendId::new("backend-health").unwrap(),
                PlacementGeneration::new(1),
                PlacementState::Verified,
            )
            .await
            .unwrap_err();
        assert_eq!(revive.code(), CentralErrorCode::InvalidState);
        authority.close().await;
    }

    #[tokio::test]
    async fn initial_placement_publish_rolls_back_every_metadata_row_on_conflict() {
        let directory = TempDir::new().unwrap();
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let store = authority.authority_store().placement().unwrap();
        let tenant_id = TenantId::new("tenant-atomic").unwrap();
        let commit_id = ContentDigest::from_bytes([41; 32]);
        let first = ObjectId::from_bytes([42; 32]);
        let second = ObjectId::from_bytes([43; 32]);
        let object_set = ObjectSet::new(vec![
            CommitObject::new(first, 3, ObjectEncoding::Raw, 0),
            CommitObject::new(second, 5, ObjectEncoding::Raw, 1),
        ])
        .unwrap();
        let backend_id = BackendId::new("backend-atomic").unwrap();
        let volume_id = StorageVolumeId::new("volume-atomic").unwrap();
        let generation = PlacementGeneration::new(1);
        let placement = |object_id: ObjectId, failure_domain: &str| ObjectPlacement {
            tenant_id: tenant_id.clone(),
            object_id,
            backend_id: backend_id.clone(),
            storage_volume_id: Some(volume_id.clone()),
            archive_id: None,
            edge_cluster_id: None,
            gateway_pool_id: None,
            region: None,
            placement_generation: generation,
            state: PlacementState::Verified,
            verified_size: DecimalU64::new(if object_id == first { 3 } else { 5 }),
            verified_digest: object_id.digest(),
            failure_domain: failure_domain.to_owned(),
        };
        // This conflicting row is deliberately written before the atomic publish. The second
        // object will fail identity validation after the first object has been staged.
        store
            .insert_object_placement(placement(second, "preexisting-host"))
            .await
            .unwrap();
        let placements = vec![placement(first, "new-host"), placement(second, "new-host")];
        let placement_set = CommitPlacementSet {
            placement_set_id: PlacementSetId::new("placement-set-atomic").unwrap(),
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(commit_id),
            backend_id: backend_id.clone(),
            storage_volume_id: Some(volume_id),
            archive_id: None,
            object_set_digest: object_set.object_set_digest,
            object_count: DecimalU64::new(2),
            verified_object_count: DecimalU64::new(2),
            placement_generation: generation,
            state: CommitPlacementSetState::Published,
        };
        let error = store
            .publish_initial_placement(
                CommitObjectSet {
                    tenant_id: tenant_id.clone(),
                    commit_id: CommitId::from_digest(commit_id),
                    object_set,
                },
                placements,
                placement_set,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), CentralErrorCode::InvalidState);
        assert!(store
            .get_commit_object_set(&tenant_id, &commit_id)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .object_placements(&tenant_id, &first)
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .published_placement_sets(&tenant_id, &commit_id)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .object_placements(&tenant_id, &second)
                .await
                .unwrap()
                .len(),
            1,
            "the pre-existing conflicting row is outside this publish transaction"
        );
        authority.close().await;
    }
}
