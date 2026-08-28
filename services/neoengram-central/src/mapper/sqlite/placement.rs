use async_trait::async_trait;
use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    AgentId, ArchiveId, ArtifactId, BackendId, CommitObject, CommitObjectSet, CommitPlacementSet,
    CommitPlacementSetState, DataHealth, DecimalU64, EdgeClusterId, GatewayPoolId, MountGeneration,
    ObjectEncoding, ObjectPlacement, ObjectSet, PlacementGeneration, PlacementId, PlacementSetId,
    PlacementState, RegionId, ReplicationId, ReplicationObjectState, ReplicationState, RequestId,
    RouteGeneration, SessionGeneration, StorageVolumeId, TenantId, TransferId, TransferRouteId,
    UnixMillis, WorkspaceId, WorkspaceLifecycle,
};
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};

use super::authority::{
    decode, digest_from_blob, encode, storage_corruption, storage_error, SqliteAuthorityStore,
};
use crate::{
    same_retry_request, valid_replication_transition, validate_replication_checkpoints,
    validate_replication_publication, validate_replication_record, CancelReplicationRequest,
    CentralError, CentralErrorCode, CentralResult, CommitAvailabilityRecord,
    FinalizeReplicationRequest, FinalizeReplicationResult, PlacementRepository,
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
    (SELECT artifact_id FROM replication_artifacts AS a WHERE a.tenant_id = replications.tenant_id \
      AND a.replication_id = replications.replication_id) AS artifact_id";
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
        "UPDATE replications SET source_edge_cluster_id = ?, source_gateway_pool_id = ?, \
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
        "UPDATE replications SET state = 'cancelled', error_code = ?, error_message = ?, \
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
            "SELECT {PLACEMENT_SET_COLUMNS} FROM commit_placement_sets \
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
            "SELECT {PLACEMENT_SET_COLUMNS} FROM commit_placement_sets \
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
            "SELECT {PLACEMENT_SET_COLUMNS} FROM commit_placement_sets \
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
            "INSERT INTO commit_placement_sets \
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
            "SELECT {PLACEMENT_SET_COLUMNS} FROM commit_placement_sets \
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
                "INSERT INTO placement_objects \
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
                "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM placement_objects \
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
            "INSERT INTO commit_placement_sets \
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
            "SELECT {PLACEMENT_SET_COLUMNS} FROM commit_placement_sets \
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
            "INSERT INTO placement_objects \
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
                    "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM placement_objects \
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
            "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM placement_objects \
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
            "UPDATE placement_objects SET state = ?, updated_at_unix_ms = updated_at_unix_ms \
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
            "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM placement_objects \
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
            "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM placement_objects \
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
            "SELECT {REPLICATION_COLUMNS} FROM replications WHERE tenant_id = ? AND replication_id = ?"
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
            "SELECT {REPLICATION_COLUMNS} FROM replications WHERE tenant_id = ? AND request_id = ?"
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
            "SELECT {REPLICATION_COLUMNS} FROM replications \
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
            "SELECT {REPLICATION_COLUMNS} FROM replications \
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
                "SELECT 1 FROM commit_placement_sets \
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
            "INSERT INTO replications \
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
                        "INSERT INTO replication_artifacts (tenant_id, replication_id, artifact_id) \
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
            "UPDATE replications SET state = ?, completed_objects = ?, completed_bytes = ?, \
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
            "SELECT request_payload, result_payload FROM replication_retry_mutations \
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
            "SELECT {REPLICATION_COLUMNS} FROM replications \
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
            "SELECT 1 FROM replications \
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
            "SELECT 1 FROM commit_placement_sets \
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
            "UPDATE replications SET state = 'queued', attempt = ?, error_code = NULL, \
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
            "SELECT {REPLICATION_COLUMNS} FROM replications \
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
            "INSERT INTO replication_retry_mutations \
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
            "UPDATE replications SET updated_at_unix_ms = updated_at_unix_ms \
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
            "SELECT state, attempt FROM replications WHERE tenant_id = ? AND replication_id = ?",
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
            "SELECT 1 FROM replications \
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
             FROM replication_objects WHERE tenant_id = ? AND replication_id = ? ORDER BY object_id",
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
                "INSERT INTO placement_objects \
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
                "SELECT {OBJECT_PLACEMENT_COLUMNS} FROM placement_objects \
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
            "INSERT INTO commit_placement_sets \
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
            "SELECT {PLACEMENT_SET_COLUMNS} FROM commit_placement_sets \
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
            "UPDATE replications SET state = 'published', target_placement_set_id = ?, \
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
                "UPDATE replications SET updated_at_unix_ms = updated_at_unix_ms \
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
             FROM replication_objects WHERE tenant_id = ? AND replication_id = ? AND object_id = ?",
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
            "INSERT INTO replication_objects \
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
             FROM replication_objects WHERE tenant_id = ? AND replication_id = ? ORDER BY object_id",
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
                        p.verified_size, p.verified_digest FROM placement_objects p \
                 WHERE p.tenant_id = ? AND p.object_id = ? \
                   AND EXISTS (SELECT 1 FROM commit_placement_sets s \
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
