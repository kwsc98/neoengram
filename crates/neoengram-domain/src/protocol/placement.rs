//! Placement-first metadata contracts.
//!
//! A Commit and Snapshot are logical, immutable references.  They do not carry a Volume or
//! Region.  The physical copy of an object is represented by [`ObjectPlacement`], while a
//! [`CommitPlacementSet`] is the atomic, complete-object-set publication fence used by delivery
//! and S3 readers.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ArchiveId, BackendId, CommitId, ContentDigest, DecimalU64, EdgeClusterId, GatewayPoolId,
    ObjectSetDigest, PlacementGeneration, PlacementSetId, ProtocolError, ProtocolResult, RegionId,
    StorageVolumeId, TenantId,
};
use crate::{ObjectId, ObjectSpec};

/// Logical Commit lifecycle.  Data health is tracked separately by [`DataHealth`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitLifecycle {
    Active,
    Deleting,
    Deleted,
}

/// Logical Snapshot lifecycle.  A Snapshot is only a reference to a Commit.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotLifecycle {
    Active,
    Deleting,
    Deleted,
}

/// Workspace lifecycle.  A workspace owns the writable data plane for a Volume.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceLifecycle {
    Provisioning,
    Active,
    Unavailable,
    Deleting,
    Deleted,
}

/// Delivery lifecycle, independent from replication state.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeliveryLifecycle {
    Queued,
    Materializing,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

/// Explicit object replication state.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicationState {
    Queued,
    Planning,
    Transferring,
    Verifying,
    Published,
    Failed,
    Cancelled,
}

/// Durable progress state for one object inside a Replication.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicationObjectState {
    Queued,
    Transferring,
    Verified,
    Failed,
}

/// State of one object copy.  `Lost` is an explicit administrative declaration; a failed Agent
/// must not silently turn a missing object into a logical deletion.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PlacementState {
    Verified,
    Retiring,
    Deleted,
    Lost,
}

/// Atomic state of a complete Commit object set on one backend.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitPlacementSetState {
    Staged,
    Published,
    Retiring,
    Deleted,
}

/// Data availability is intentionally separate from logical lifecycle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DataHealth {
    Available,
    Degraded,
    Unavailable,
}

/// More explicit name used by Commit/Snapshot APIs.
pub type CommitDataHealth = DataHealth;
/// More explicit name used by placement resolvers.
pub type PlacementDataHealth = DataHealth;

/// Encoding of an immutable object in a CAS backend.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectEncoding {
    Raw,
    Zstd,
}

/// One immutable object required by a Commit.  The ordinal is part of the ObjectSet identity and
/// makes transfer planning deterministic.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct CommitObject {
    pub object_id: ObjectId,
    pub size: DecimalU64,
    pub encoding: ObjectEncoding,
    pub ordinal: DecimalU64,
}

impl CommitObject {
    #[must_use]
    pub const fn new(
        object_id: ObjectId,
        size: u64,
        encoding: ObjectEncoding,
        ordinal: u64,
    ) -> Self {
        Self {
            object_id,
            size: DecimalU64::new(size),
            encoding,
            ordinal: DecimalU64::new(ordinal),
        }
    }

    #[must_use]
    pub const fn object_spec(self) -> ObjectSpec {
        ObjectSpec::new(self.object_id, self.size.get())
    }
}

/// Canonical object set attached to an immutable Commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectSet {
    pub object_set_digest: ObjectSetDigest,
    pub objects: Vec<CommitObject>,
}

impl ObjectSet {
    /// Constructs an ObjectSet and computes its deterministic digest.
    pub fn new(objects: Vec<CommitObject>) -> ProtocolResult<Self> {
        let object_set_digest = Self::digest_for(&objects)?;
        let set = Self {
            object_set_digest,
            objects,
        };
        set.validate()?;
        Ok(set)
    }

    /// Computes the tenant-independent digest of object IDs, sizes, encodings and ordinals.
    pub fn digest_for(objects: &[CommitObject]) -> ProtocolResult<ContentDigest> {
        validate_objects(objects)?;
        let mut bytes = Vec::with_capacity(32 + objects.len() * 50);
        bytes.extend_from_slice(b"neoengram-object-set-v1\0");
        for object in objects {
            bytes.extend_from_slice(object.object_id.as_bytes());
            bytes.extend_from_slice(&object.size.get().to_be_bytes());
            bytes.extend_from_slice(&object.ordinal.get().to_be_bytes());
            bytes.push(match object.encoding {
                ObjectEncoding::Raw => 0,
                ObjectEncoding::Zstd => 1,
            });
        }
        Ok(ContentDigest::hash(bytes))
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        let expected = Self::digest_for(&self.objects)?;
        if self.object_set_digest != expected {
            return Err(ProtocolError::InvalidDigest(format!(
                "object_set_digest does not match object list: expected {expected}, got {}",
                self.object_set_digest
            )));
        }
        Ok(())
    }

    #[must_use]
    pub fn object_count(&self) -> usize {
        self.objects.len()
    }

    pub fn total_bytes(&self) -> ProtocolResult<u64> {
        self.objects.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.size.get())
                .ok_or_else(|| ProtocolError::InvalidField {
                    field: "objects",
                    reason: "total ObjectSet size exceeds u64".to_owned(),
                })
        })
    }
}

/// Wire/persistence envelope for staged Commit object metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommitObjectSet {
    pub tenant_id: TenantId,
    pub commit_id: CommitId,
    pub object_set: ObjectSet,
}

impl CommitObjectSet {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.object_set.validate()
    }
}

/// One physical, content-addressed copy of an Object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectPlacement {
    pub tenant_id: TenantId,
    pub object_id: ObjectId,
    pub backend_id: BackendId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_volume_id: Option<StorageVolumeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_id: Option<ArchiveId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_cluster_id: Option<EdgeClusterId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_pool_id: Option<GatewayPoolId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<RegionId>,
    pub placement_generation: PlacementGeneration,
    pub state: PlacementState,
    pub verified_size: DecimalU64,
    pub verified_digest: ContentDigest,
    pub failure_domain: String,
}

impl ObjectPlacement {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.storage_volume_id.is_some() == self.archive_id.is_some() {
            return Err(ProtocolError::InvalidField {
                field: "storage_volume_id/archive_id",
                reason: "exactly one physical storage target is required".to_owned(),
            });
        }
        if self.placement_generation.get() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "placement_generation",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.verified_digest != self.object_id.digest() {
            return Err(ProtocolError::InvalidDigest(
                "verified_digest must equal object_id".to_owned(),
            ));
        }
        if self.failure_domain.is_empty() || self.failure_domain.len() > 256 {
            return Err(ProtocolError::InvalidField {
                field: "failure_domain",
                reason: "must contain 1..=256 bytes".to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn readable(&self) -> bool {
        matches!(self.state, PlacementState::Verified)
    }
}

/// A complete-object-set publication fence for one backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommitPlacementSet {
    pub placement_set_id: PlacementSetId,
    pub tenant_id: TenantId,
    pub commit_id: CommitId,
    pub backend_id: BackendId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_volume_id: Option<StorageVolumeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_id: Option<ArchiveId>,
    pub object_set_digest: ObjectSetDigest,
    pub object_count: DecimalU64,
    pub verified_object_count: DecimalU64,
    pub placement_generation: PlacementGeneration,
    pub state: CommitPlacementSetState,
}

impl CommitPlacementSet {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.storage_volume_id.is_some() == self.archive_id.is_some() {
            return Err(ProtocolError::InvalidField {
                field: "storage_volume_id/archive_id",
                reason: "exactly one physical storage target is required".to_owned(),
            });
        }
        if self.placement_generation.get() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "placement_generation",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.verified_object_count.get() > self.object_count.get() {
            return Err(ProtocolError::InvalidField {
                field: "verified_object_count",
                reason: "cannot exceed object_count".to_owned(),
            });
        }
        if matches!(self.state, CommitPlacementSetState::Published)
            && self.verified_object_count != self.object_count
        {
            return Err(ProtocolError::InvalidField {
                field: "state",
                reason: "published placement sets must contain every object".to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn published(&self) -> bool {
        matches!(self.state, CommitPlacementSetState::Published)
    }
}

/// Computes the health of a Commit from its required ObjectSet and known placements.
///
/// A placement set with one verified copy is healthy (`available`).  A Commit becomes
/// `degraded` when all objects remain readable but a copy is retiring/lost/deleted or the caller
/// asks for more than one replica.  It becomes `unavailable` as soon as one required object has no
/// verified readable copy.
pub fn commit_data_health(
    object_set: &ObjectSet,
    placements: &BTreeMap<ObjectId, Vec<ObjectPlacement>>,
    required_replicas: usize,
) -> ProtocolResult<DataHealth> {
    object_set.validate()?;
    let required_replicas = required_replicas.max(1);
    let mut degraded = false;
    for object in &object_set.objects {
        let copies =
            placements
                .get(&object.object_id)
                .ok_or_else(|| ProtocolError::InvalidField {
                    field: "placements",
                    reason: format!("missing placements for object {}", object.object_id),
                })?;
        let mut readable = 0_usize;
        for placement in copies {
            placement.validate()?;
            if placement.object_id != object.object_id
                || placement.verified_size.get() != object.size.get()
            {
                return Err(ProtocolError::InvalidField {
                    field: "placements",
                    reason: "placement object identity or size differs from ObjectSet".to_owned(),
                });
            }
            if placement.readable() {
                readable += 1;
            } else {
                degraded = true;
            }
        }
        if readable == 0 {
            return Ok(DataHealth::Unavailable);
        }
        if readable < required_replicas {
            degraded = true;
        }
    }
    Ok(if degraded {
        DataHealth::Degraded
    } else {
        DataHealth::Available
    })
}

fn validate_objects(objects: &[CommitObject]) -> ProtocolResult<()> {
    let mut ids = BTreeSet::new();
    for (index, object) in objects.iter().enumerate() {
        let expected_ordinal = u64::try_from(index).map_err(|_| ProtocolError::InvalidField {
            field: "ordinal",
            reason: "object count exceeds u64".to_owned(),
        })?;
        if object.ordinal.get() != expected_ordinal {
            return Err(ProtocolError::InvalidField {
                field: "ordinal",
                reason: "object ordinals must be contiguous and start at zero".to_owned(),
            });
        }
        if !ids.insert(object.object_id) {
            return Err(ProtocolError::InvalidField {
                field: "object_id",
                reason: "an ObjectSet cannot contain duplicate objects".to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> ObjectId {
        ObjectId::from_bytes([byte; 32])
    }

    fn placement(object_id: ObjectId, state: PlacementState) -> ObjectPlacement {
        ObjectPlacement {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_id,
            backend_id: BackendId::new("volume-backend").unwrap(),
            storage_volume_id: Some(StorageVolumeId::new("volume-a").unwrap()),
            archive_id: None,
            edge_cluster_id: None,
            gateway_pool_id: None,
            region: None,
            placement_generation: PlacementGeneration::new(1),
            state,
            verified_size: DecimalU64::new(4),
            verified_digest: object_id.digest(),
            failure_domain: "host-a".to_owned(),
        }
    }

    #[test]
    fn object_set_digest_and_strict_validation() {
        let set =
            ObjectSet::new(vec![CommitObject::new(id(1), 4, ObjectEncoding::Raw, 0)]).unwrap();
        assert_eq!(set.object_count(), 1);
        assert!(serde_json::from_str::<ObjectSet>(&format!(
            r#"{{"object_set_digest":"{}","objects":[],"unknown":1}}"#,
            set.object_set_digest
        ))
        .is_err());
    }

    #[test]
    fn health_distinguishes_missing_and_degraded_copies() {
        let set =
            ObjectSet::new(vec![CommitObject::new(id(1), 4, ObjectEncoding::Raw, 0)]).unwrap();
        let mut map = BTreeMap::new();
        map.insert(id(1), vec![placement(id(1), PlacementState::Verified)]);
        assert_eq!(
            commit_data_health(&set, &map, 1).unwrap(),
            DataHealth::Available
        );
        assert_eq!(
            commit_data_health(&set, &map, 2).unwrap(),
            DataHealth::Degraded
        );
        map.insert(id(1), vec![placement(id(1), PlacementState::Lost)]);
        assert_eq!(
            commit_data_health(&set, &map, 1).unwrap(),
            DataHealth::Unavailable
        );
    }
}
