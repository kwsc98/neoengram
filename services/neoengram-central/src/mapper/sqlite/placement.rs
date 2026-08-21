use async_trait::async_trait;
use neoengram_domain::core::{CommitId, ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    ArchiveId, BackendId, CommitObject, CommitObjectSet, CommitPlacementSet,
    CommitPlacementSetState, DataHealth, DecimalU64, EdgeClusterId, GatewayPoolId, ObjectEncoding,
    ObjectPlacement, ObjectSet, PlacementGeneration, PlacementId, PlacementSetId, PlacementState,
    RegionId, ReplicationId, ReplicationObjectState, ReplicationState, RequestId, StorageVolumeId,
    TenantId, UnixMillis, WorkspaceId, WorkspaceLifecycle,
};
use sqlx::{sqlite::SqliteRow, Row};

use super::authority::{digest_from_blob, storage_corruption, storage_error, SqliteAuthorityStore};
use crate::{
    CentralError, CentralErrorCode, CentralResult, CommitAvailabilityRecord, PlacementRepository,
    ReplicationObjectRecord, ReplicationRecord, WorkspaceRecord,
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
    target_storage_volume_id, object_set_digest, state, request_id, completed_objects, \
    total_objects, error_code, error_message, created_at_unix_ms, updated_at_unix_ms";
const WORKSPACE_COLUMNS: &str = "tenant_id, workspace_id, request_id, project_id, artifact_id, \
    base_commit_id, target_storage_volume_id, lifecycle, created_at_unix_ms, updated_at_unix_ms";

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

    async fn insert_replication(
        &self,
        record: ReplicationRecord,
    ) -> CentralResult<ReplicationRecord> {
        if record.created_at_unix_ms > record.updated_at_unix_ms {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "replication timestamps are out of order",
            ));
        }
        let result = sqlx::query(
            "INSERT INTO replications \
             (tenant_id, replication_id, commit_id, target_backend_id, target_storage_volume_id, \
              target_archive_id, object_set_digest, state, request_id, completed_objects, \
              total_objects, error_code, error_message, created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.replication_id.as_str())
        .bind(record.commit_id.as_bytes().as_slice())
        .bind(&record.target_backend_id)
        .bind(record.target_storage_volume_id.as_str())
        .bind(record.object_set_digest.as_bytes().as_slice())
        .bind(replication_state_name(record.state))
        .bind(record.request_id.as_str())
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
        .bind(&record.issue_code)
        .bind(&record.issue_message)
        .bind(as_i64(record.created_at_unix_ms)?)
        .bind(as_i64(record.updated_at_unix_ms)?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(record),
            Err(error) if is_unique(&error) => {
                let existing = self
                    .get_replication_by_request_id(&record.tenant_id, &record.request_id)
                    .await?
                    .ok_or_else(|| {
                        storage_corruption("replication uniqueness conflict has no row")
                    })?;
                if existing.replication_id == record.replication_id
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
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    async fn upsert_replication_object(
        &self,
        record: ReplicationObjectRecord,
    ) -> CentralResult<ReplicationObjectRecord> {
        if self
            .get_replication(&record.tenant_id, &record.replication_id)
            .await?
            .is_none()
        {
            return Err(CentralError::new(
                CentralErrorCode::InvalidState,
                "replication object references a missing Replication",
            )
            .with_retryable(false));
        }
        let object_id = record.object_id;
        let existing = sqlx::query(
            "SELECT tenant_id, replication_id, object_id, offset, state, retry_count, updated_at_unix_ms \
             FROM replication_objects WHERE tenant_id = ? AND replication_id = ? AND object_id = ?",
        )
        .bind(record.tenant_id.as_str())
        .bind(record.replication_id.as_str())
        .bind(object_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
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
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
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
        let mut verified_candidates =
            std::collections::BTreeMap::<(String, Option<String>, Option<String>, u64), u64>::new();
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
        let object_count = u64::try_from(object_set.object_set.objects.len())
            .map_err(|_| storage_corruption("object count exceeds u64"))?;
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
            commit_id,
            target_backend_id: "volume-backend-a".to_owned(),
            target_storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            object_set_digest: ContentDigest::from_bytes([8; 32]),
            state: ReplicationState::Queued,
            request_id: request_id.clone(),
            completed_objects: 0,
            total_objects: 3,
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
