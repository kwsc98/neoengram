//! Clean-slate v2 contracts for object-level placement and multi-source materialization.
//!
//! A Commit is a logical object set.  This module deliberately models the durable unit as one
//! verified object on one storage Volume (or archive), and models a complete Commit on a Volume as
//! a derived [`VolumeCommitCoverage`].  Central owns these records and signs short-lived batch
//! capabilities; Agents own bytes and Gateways only relay the QUIC stream.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    placement::{CommitObject, ObjectEncoding, ObjectSet},
    validation::{
        validate_collection_limit, validate_extension_keys, validate_nonempty_limited,
        validate_positive,
    },
};
use crate::{
    domain_separated_jcs_bytes, jcs_blake3, AgentId, ArchiveId, ArtifactId, BackendId,
    CentralSignedPayload, CommitId, ContentDigest, DecimalU64, EdgeClusterId, GatewayConnectionId,
    GatewayPoolId, Generation, IntegrityScanId, MaterializationBatchId, MaterializationId,
    MountGeneration, ObjectId, ObjectNamespaceId, ObjectTicketId, PlacementGeneration, PlacementId,
    ProtocolError, ProtocolResult, RegionId, RouteGeneration, SessionGeneration, StorageVolumeId,
    TaskAttemptId, TaskId, TenantId, UnixMillis,
};

/// Version advertised by materialization-specific control and transfer contracts.
pub const MATERIALIZATION_PROTOCOL_VERSION: u16 = 2;
/// Capability required on both Agent sessions participating in a v2 batch.
pub const COMMIT_MATERIALIZATION_CAPABILITY_V2: &str = "commit_materialization_v2";
/// QUIC ALPN for the v2 source/target object stream.
pub const MATERIALIZATION_TRANSFER_ALPN_V2: &str = "neoengram-transfer-v2";
/// Maximum number of objects in one manifest page.
pub const MAX_MATERIALIZATION_MANIFEST_OBJECTS: usize = 4096;
/// Maximum number of objects in one source-grouped batch. Batches are paged, so this is larger
/// than one manifest page; keeping the cap explicit prevents a valid multi-page manifest from
/// being rejected by `MaterializationBatch::validate`.
pub const MAX_MATERIALIZATION_BATCH_OBJECTS: usize = MAX_MATERIALIZATION_MANIFEST_OBJECTS * 65_535;
/// Maximum number of source placements retained in one object task.
pub const MAX_MATERIALIZATION_SOURCES: usize = 256;
/// Maximum number of preferred regions in a durability policy.
pub const MAX_DURABILITY_REGIONS: usize = 64;
/// Maximum user-visible diagnostic attached to a materialization record.
pub const MAX_MATERIALIZATION_ERROR_BYTES: usize = 4096;
/// Maximum stable staging-key length accepted on the wire.
pub const MAX_STAGING_KEY_BYTES: usize = 512;
const MATERIALIZATION_TICKET_SIGNING_DOMAIN: &str = "neoengram-materialization-ticket-v2";

/// Schema root containing every public v2 object-materialization contract.  The individual
/// structs remain the preferred Rust API; this enum exists only to make one strict JSON Schema
/// document that can be published to Central, Agent, and Gateway implementations.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum MaterializationProtocolSchema {
    ObjectRef(ObjectRef),
    NamespaceObjectSet(NamespaceObjectSet),
    ObjectPlacement(ObjectPlacement),
    VolumeCommitCoverage(VolumeCommitCoverage),
    IntegrityScanReport(IntegrityScanReport),
    DurabilityPolicy(DurabilityPolicy),
    MaterializationJob(MaterializationJob),
    MaterializationAssignment(MaterializationAssignment),
    MaterializationReport(MaterializationReport),
    MaterializationBatch(MaterializationBatch),
    MaterializationObject(MaterializationObject),
    BatchManifest(BatchManifest),
    BatchManifestPage(BatchManifestPage),
    MaterializationBatchTicket(MaterializationBatchTicket),
    SignedMaterializationBatchTicket(SignedMaterializationBatchTicket),
    MaterializationObjectReceipt(MaterializationObjectReceipt),
    ObjectReadLease(ObjectReadLease),
    StagingLease(StagingLease),
    CommitAvailability(CommitAvailability),
}

fn invalid(field: &'static str, reason: impl Into<String>) -> ProtocolError {
    ProtocolError::InvalidField {
        field,
        reason: reason.into(),
    }
}

fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    max: usize,
) -> ProtocolResult<()> {
    if let Some(value) = value {
        validate_nonempty_limited(field, value, max)?;
    }
    Ok(())
}

/// A reference to one exact object required by a Commit ObjectSet.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub object_namespace_id: ObjectNamespaceId,
    pub object_id: ObjectId,
    pub size: DecimalU64,
    pub encoding: ObjectEncoding,
    pub ordinal: DecimalU64,
}

impl ObjectRef {
    #[must_use]
    pub const fn new(
        object_namespace_id: ObjectNamespaceId,
        object_id: ObjectId,
        size: u64,
        encoding: ObjectEncoding,
        ordinal: u64,
    ) -> Self {
        Self {
            object_namespace_id,
            object_id,
            size: DecimalU64::new(size),
            encoding,
            ordinal: DecimalU64::new(ordinal),
        }
    }

    #[must_use]
    pub fn from_commit_object(
        object_namespace_id: ObjectNamespaceId,
        object: CommitObject,
    ) -> Self {
        Self::new(
            object_namespace_id,
            object.object_id,
            object.size.get(),
            object.encoding,
            object.ordinal.get(),
        )
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        // ObjectNamespaceId/ObjectId are validated at deserialization/construction time.  Keep the
        // explicit check here so callers that build an ObjectRef from trusted storage still get a
        // stable field-level error for an empty identifier.
        if self.object_namespace_id.as_str().is_empty() {
            return Err(invalid("object_namespace_id", "must not be empty"));
        }
        Ok(())
    }

    #[must_use]
    pub fn object_spec(&self) -> crate::ObjectSpec {
        crate::ObjectSpec::new(self.object_id, self.size.get())
    }
}

/// Namespace-scoped form of a Commit ObjectSet.  The content digest intentionally remains the
/// same ObjectSet digest as v1; namespace and tenant are explicit authorization fields around it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NamespaceObjectSet {
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub commit_id: CommitId,
    pub object_set_digest: ContentDigest,
    pub objects: Vec<ObjectRef>,
}

/// Explicit name used by materialization APIs for the namespace-scoped logical object set.
pub type CommitObjectSet = NamespaceObjectSet;

impl NamespaceObjectSet {
    pub fn from_object_set(
        tenant_id: TenantId,
        object_namespace_id: ObjectNamespaceId,
        commit_id: CommitId,
        object_set: &ObjectSet,
    ) -> ProtocolResult<Self> {
        object_set.validate()?;
        let objects = object_set
            .objects
            .iter()
            .copied()
            .map(|object| ObjectRef::from_commit_object(object_namespace_id.clone(), object))
            .collect();
        let set = Self {
            tenant_id,
            object_namespace_id,
            commit_id,
            object_set_digest: object_set.object_set_digest,
            objects,
        };
        set.validate()?;
        Ok(set)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        let mut objects = Vec::with_capacity(self.objects.len());
        let mut seen = BTreeSet::new();
        for (index, object) in self.objects.iter().enumerate() {
            object.validate()?;
            if object.object_namespace_id != self.object_namespace_id {
                return Err(invalid(
                    "objects",
                    "all objects must use the object-set namespace",
                ));
            }
            if object.ordinal.get() != index as u64 || !seen.insert(object.object_id) {
                return Err(invalid(
                    "objects",
                    "object ordinals must be contiguous and IDs unique",
                ));
            }
            objects.push(CommitObject::new(
                object.object_id,
                object.size.get(),
                object.encoding,
                object.ordinal.get(),
            ));
        }
        let expected = ObjectSet::digest_for(&objects)?;
        if expected != self.object_set_digest {
            return Err(ProtocolError::InvalidDigest(
                "object_set_digest does not match namespace object list".to_owned(),
            ));
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
                .ok_or_else(|| invalid("total_bytes", "exceeds u64"))
        })
    }
}

/// State of an object-level physical copy.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectPlacementState {
    Verified,
    Retiring,
    Deleted,
    Lost,
}

/// Short spelling used by placement repositories for the v2 object state.
pub type PlacementState = ObjectPlacementState;

/// A single durable, namespace-scoped physical object copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectPlacement {
    pub placement_id: PlacementId,
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub object_id: ObjectId,
    pub size: DecimalU64,
    pub encoding: ObjectEncoding,
    pub verified_digest: ContentDigest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_volume_id: Option<StorageVolumeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_id: Option<ArchiveId>,
    pub placement_generation: PlacementGeneration,
    pub state: ObjectPlacementState,
    pub failure_domain: String,
}

/// Result of checking one physical object against the immutable Placement evidence.
///
/// This is deliberately separate from `ObjectPlacementState`: a transient I/O error must not
/// silently retire durable evidence, while an independently confirmed missing or corrupt file
/// must be excluded from source selection until a later scan proves it healthy again.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PlacementHealthState {
    Healthy,
    Missing,
    Corrupt,
    Orphan,
    Unknown,
}

impl PlacementHealthState {
    #[must_use]
    pub const fn source_eligible(self) -> bool {
        matches!(self, Self::Healthy)
    }
}

/// Durable observation emitted by an Agent Volume scrub.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PlacementHealthObservation {
    pub scan_id: IntegrityScanId,
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub placement_id: PlacementId,
    pub object_id: ObjectId,
    pub storage_volume_id: StorageVolumeId,
    pub placement_generation: PlacementGeneration,
    pub state: PlacementHealthState,
    pub observed_size: DecimalU64,
    pub observed_digest: ContentDigest,
    pub observed_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl PlacementHealthObservation {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("placement_generation", self.placement_generation.get())?;
        if self.observed_at_unix_ms.get() == 0 {
            return Err(invalid("observed_at_unix_ms", "must be positive"));
        }
        if self.observed_digest != self.object_id.digest()
            && matches!(self.state, PlacementHealthState::Healthy)
        {
            return Err(ProtocolError::InvalidDigest(
                "healthy observation digest must equal object_id".to_owned(),
            ));
        }
        if let Some(detail) = &self.detail {
            validate_nonempty_limited("detail", detail, MAX_MATERIALIZATION_ERROR_BYTES)?;
        }
        Ok(())
    }
}

/// Aggregate state of one explicit or scheduled Volume integrity scan.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum VolumeIntegrityScanState {
    Queued,
    Running,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VolumeIntegrityScan {
    pub scan_id: IntegrityScanId,
    pub tenant_id: TenantId,
    pub storage_volume_id: StorageVolumeId,
    pub placement_generation: PlacementGeneration,
    pub state: VolumeIntegrityScanState,
    pub checked_objects: DecimalU64,
    pub healthy_objects: DecimalU64,
    pub missing_objects: DecimalU64,
    pub corrupt_objects: DecimalU64,
    pub orphan_objects: DecimalU64,
    pub started_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Agent-to-Central report for one completed Volume scrub.
///
/// The scan aggregate is useful for operator diagnostics while the per-placement observations
/// are the durable input to source selection and target coverage.  Agent/session/mount identity
/// is supplied by the authenticated channel frame; Central still checks every scope field before
/// accepting the observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntegrityScanReport {
    pub scan: VolumeIntegrityScan,
    pub mount_generation: MountGeneration,
    #[schemars(length(max = MAX_MATERIALIZATION_BATCH_OBJECTS))]
    pub observations: Vec<PlacementHealthObservation>,
    #[serde(default, flatten)]
    pub extensions: crate::Extensions,
}

impl IntegrityScanReport {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.scan.validate()?;
        validate_positive("mount_generation", self.mount_generation.get())?;
        if self.observations.len() > MAX_MATERIALIZATION_BATCH_OBJECTS {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "integrity observation count",
                limit: MAX_MATERIALIZATION_BATCH_OBJECTS,
                actual: self.observations.len(),
            });
        }
        let mut seen = BTreeSet::new();
        for observation in &self.observations {
            observation.validate()?;
            if observation.scan_id != self.scan.scan_id
                || observation.tenant_id != self.scan.tenant_id
                || observation.storage_volume_id != self.scan.storage_volume_id
                || !seen.insert(observation.placement_id.clone())
            {
                return Err(invalid(
                    "observations",
                    "observation identity does not match the scan or is duplicated",
                ));
            }
        }
        validate_extension_keys(
            &self.extensions,
            &["scan", "mount_generation", "observations"],
        )
    }
}

impl VolumeIntegrityScan {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("placement_generation", self.placement_generation.get())?;
        if self.started_at_unix_ms.get() == 0 {
            return Err(invalid("started_at_unix_ms", "must be positive"));
        }
        for (name, count) in [
            ("healthy_objects", self.healthy_objects),
            ("missing_objects", self.missing_objects),
            ("corrupt_objects", self.corrupt_objects),
            ("orphan_objects", self.orphan_objects),
        ] {
            if count.get() > self.checked_objects.get() && name != "orphan_objects" {
                return Err(invalid(name, "cannot exceed checked_objects"));
            }
        }
        if let Some(finished) = self.finished_at_unix_ms {
            if finished.get() < self.started_at_unix_ms.get() {
                return Err(invalid("finished_at_unix_ms", "cannot precede start"));
            }
        }
        if let Some(error) = &self.error {
            validate_nonempty_limited("error", error, MAX_MATERIALIZATION_ERROR_BYTES)?;
        }
        Ok(())
    }
}

impl ObjectPlacement {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.storage_volume_id.is_some() == self.archive_id.is_some() {
            return Err(invalid(
                "storage_volume_id/archive_id",
                "exactly one physical storage target is required",
            ));
        }
        validate_positive("placement_generation", self.placement_generation.get())?;
        if self.verified_digest != self.object_id.digest() {
            return Err(ProtocolError::InvalidDigest(
                "verified_digest must equal object_id".to_owned(),
            ));
        }
        validate_nonempty_limited("failure_domain", &self.failure_domain, 256)?;
        Ok(())
    }

    /// Validate this durable fact against the exact object descriptor requested by a Commit.
    ///
    /// A placement is intentionally not tied to a Commit (the same immutable object may be
    /// referenced by many Commits), so source selection must perform this comparison before
    /// reading bytes.
    pub fn validate_against(&self, object: &ObjectRef) -> ProtocolResult<()> {
        self.validate()?;
        if !self.matches_ref(object) {
            return Err(invalid(
                "object",
                "placement does not match namespace, object, size, encoding, or digest",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn readable(&self) -> bool {
        matches!(self.state, ObjectPlacementState::Verified)
    }

    #[must_use]
    pub fn matches_ref(&self, object: &ObjectRef) -> bool {
        self.object_namespace_id == object.object_namespace_id
            && self.object_id == object.object_id
            && self.size == object.size
            && self.encoding == object.encoding
            && self.verified_digest == object.object_id.digest()
    }

    #[must_use]
    pub fn target_key(
        &self,
    ) -> (
        ObjectNamespaceId,
        ObjectId,
        Option<StorageVolumeId>,
        Option<ArchiveId>,
    ) {
        (
            self.object_namespace_id.clone(),
            self.object_id,
            self.storage_volume_id.clone(),
            self.archive_id.clone(),
        )
    }
}

/// Derived coverage state for one Commit on one target Volume.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CoverageState {
    Partial,
    Complete,
    Retiring,
    Deleted,
}

impl CoverageState {
    #[must_use]
    pub const fn complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Recomputable summary of verified objects on one Volume for one Commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VolumeCommitCoverage {
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub commit_id: CommitId,
    pub storage_volume_id: StorageVolumeId,
    pub placement_generation: PlacementGeneration,
    pub object_set_digest: ContentDigest,
    pub object_count: DecimalU64,
    pub verified_object_count: DecimalU64,
    pub total_bytes: DecimalU64,
    pub verified_bytes: DecimalU64,
    pub state: CoverageState,
}

impl VolumeCommitCoverage {
    #[must_use]
    pub const fn commit_digest(&self) -> ContentDigest {
        self.commit_id.digest()
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("placement_generation", self.placement_generation.get())?;
        if self.verified_object_count.get() > self.object_count.get() {
            return Err(invalid(
                "verified_object_count",
                "cannot exceed object_count",
            ));
        }
        if self.verified_bytes.get() > self.total_bytes.get() {
            return Err(invalid("verified_bytes", "cannot exceed total_bytes"));
        }
        if self.state.complete()
            && (self.verified_object_count != self.object_count
                || self.verified_bytes != self.total_bytes)
        {
            return Err(invalid(
                "state",
                "complete coverage must account for every object and byte",
            ));
        }
        if matches!(self.state, CoverageState::Partial)
            && self.verified_object_count == self.object_count
            && self.verified_bytes == self.total_bytes
        {
            return Err(invalid(
                "state",
                "coverage with every object and byte verified must be complete",
            ));
        }
        Ok(())
    }

    pub fn validate_against(&self, object_set: &ObjectSet) -> ProtocolResult<()> {
        self.validate()?;
        object_set.validate()?;
        if self.object_set_digest != object_set.object_set_digest {
            return Err(ProtocolError::InvalidDigest(
                "coverage object_set_digest does not match Commit ObjectSet".to_owned(),
            ));
        }
        let count = u64::try_from(object_set.objects.len())
            .map_err(|_| invalid("object_count", "does not fit u64"))?;
        if self.object_count.get() != count {
            return Err(invalid("object_count", "does not match Commit ObjectSet"));
        }
        let bytes = object_set.total_bytes()?;
        if self.total_bytes.get() != bytes {
            return Err(invalid("total_bytes", "does not match Commit ObjectSet"));
        }
        Ok(())
    }

    /// Recomputes coverage solely from verified object placements.  A caller cannot promote a
    /// partial Volume by supplying aggregate counters that disagree with the object evidence.
    pub fn from_placements(
        tenant_id: TenantId,
        object_namespace_id: ObjectNamespaceId,
        commit_id: CommitId,
        storage_volume_id: StorageVolumeId,
        placement_generation: PlacementGeneration,
        object_set: &ObjectSet,
        placements: &[ObjectPlacement],
    ) -> ProtocolResult<Self> {
        object_set.validate()?;
        validate_positive("placement_generation", placement_generation.get())?;
        let total_bytes = object_set.total_bytes()?;
        let mut by_object = BTreeMap::<ObjectId, &ObjectPlacement>::new();
        for placement in placements {
            placement.validate()?;
            if placement.tenant_id != tenant_id
                || placement.object_namespace_id != object_namespace_id
                || placement.storage_volume_id.as_ref() != Some(&storage_volume_id)
                || placement.placement_generation != placement_generation
            {
                continue;
            }
            if placement.readable() && by_object.insert(placement.object_id, placement).is_some() {
                return Err(invalid(
                    "placements",
                    "a Volume/generation cannot contain duplicate object evidence",
                ));
            }
        }
        let mut verified_bytes = 0_u64;
        for object in &object_set.objects {
            if let Some(placement) = by_object.get(&object.object_id) {
                if placement.size.get() == object.size.get()
                    && placement.encoding == object.encoding
                {
                    verified_bytes = verified_bytes
                        .checked_add(object.size.get())
                        .ok_or_else(|| invalid("verified_bytes", "exceeds u64"))?;
                }
            }
        }
        let verified_object_count = object_set
            .objects
            .iter()
            .filter(|object| {
                by_object.get(&object.object_id).is_some_and(|placement| {
                    placement.size == object.size && placement.encoding == object.encoding
                })
            })
            .count();
        let object_count = object_set.objects.len();
        let state = if verified_object_count == object_count {
            CoverageState::Complete
        } else {
            CoverageState::Partial
        };
        let coverage = Self {
            tenant_id,
            object_namespace_id,
            commit_id,
            storage_volume_id,
            placement_generation,
            object_set_digest: object_set.object_set_digest,
            object_count: DecimalU64::new(
                u64::try_from(object_count)
                    .map_err(|_| invalid("object_count", "does not fit u64"))?,
            ),
            verified_object_count: DecimalU64::new(
                u64::try_from(verified_object_count)
                    .map_err(|_| invalid("verified_object_count", "does not fit u64"))?,
            ),
            total_bytes: DecimalU64::new(total_bytes),
            verified_bytes: DecimalU64::new(verified_bytes),
            state,
        };
        coverage.validate_against(object_set)?;
        Ok(coverage)
    }
}

/// A policy describing the minimum object durability desired by a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DurabilityPolicy {
    pub min_replicas: DecimalU64,
    pub min_failure_domains: DecimalU64,
    #[serde(default)]
    #[schemars(length(max = MAX_DURABILITY_REGIONS))]
    pub preferred_regions: Vec<RegionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_units: Option<DecimalU64>,
}

impl Default for DurabilityPolicy {
    fn default() -> Self {
        Self {
            min_replicas: DecimalU64::new(1),
            min_failure_domains: DecimalU64::new(1),
            preferred_regions: Vec::new(),
            max_cost_units: None,
        }
    }
}

impl DurabilityPolicy {
    #[must_use]
    pub const fn new(min_replicas: u64, min_failure_domains: u64) -> Self {
        Self {
            min_replicas: DecimalU64::new(min_replicas),
            min_failure_domains: DecimalU64::new(min_failure_domains),
            preferred_regions: Vec::new(),
            max_cost_units: None,
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("min_replicas", self.min_replicas.get())?;
        validate_positive("min_failure_domains", self.min_failure_domains.get())?;
        if self.min_failure_domains > self.min_replicas {
            return Err(invalid("min_failure_domains", "cannot exceed min_replicas"));
        }
        validate_collection_limit(
            "preferred_regions",
            self.preferred_regions.len(),
            MAX_DURABILITY_REGIONS,
        )?;
        let mut regions = BTreeSet::new();
        for region in &self.preferred_regions {
            if !regions.insert(region) {
                return Err(invalid("preferred_regions", "regions must be unique"));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn satisfied_by<'a, I>(&self, placements: I) -> bool
    where
        I: IntoIterator<Item = &'a ObjectPlacement>,
    {
        let mut count = 0_usize;
        let mut domains = BTreeSet::new();
        let mut placement_ids = BTreeSet::new();
        for placement in placements {
            // Invalid evidence cannot contribute to a durability decision. Ignore it here; the
            // repository/report path can surface the detailed validation error separately.
            if placement.readable()
                && placement.validate().is_ok()
                && placement_ids.insert(&placement.placement_id)
            {
                count += 1;
                domains.insert(placement.failure_domain.as_str());
            }
        }
        let min_replicas = usize::try_from(self.min_replicas.get()).unwrap_or(usize::MAX);
        let min_failure_domains =
            usize::try_from(self.min_failure_domains.get()).unwrap_or(usize::MAX);
        count >= min_replicas && domains.len() >= min_failure_domains
    }
}

/// Desired target coverage for a materialization request.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CoverageGoal {
    Complete,
    ObjectCount(DecimalU64),
    ByteCount(DecimalU64),
}

impl CoverageGoal {
    pub fn validate(self) -> ProtocolResult<()> {
        match self {
            Self::Complete => Ok(()),
            Self::ObjectCount(value) => {
                validate_positive("coverage_goal.object_count", value.get())
            }
            Self::ByteCount(value) => validate_positive("coverage_goal.byte_count", value.get()),
        }
    }

    /// Returns whether the requested target has enough verified materialized data.
    ///
    /// `Complete` is intentionally stricter than the threshold goals: it requires every object
    /// and byte in the Commit ObjectSet. Threshold goals are useful for staged hydration and may
    /// complete while the target Volume still has a partial Coverage; readers must continue to
    /// gate on `VolumeCommitCoverage::Complete`.
    #[must_use]
    pub fn satisfied_by(
        self,
        verified_objects: u64,
        verified_bytes: u64,
        total_objects: u64,
        total_bytes: u64,
    ) -> bool {
        match self {
            Self::Complete => verified_objects == total_objects && verified_bytes == total_bytes,
            Self::ObjectCount(required) => {
                required.get() <= total_objects && verified_objects >= required.get()
            }
            Self::ByteCount(required) => {
                required.get() <= total_bytes && verified_bytes >= required.get()
            }
        }
    }

    /// Validates a goal against the requested Commit ObjectSet totals.
    pub fn validate_against_totals(
        self,
        total_objects: u64,
        total_bytes: u64,
    ) -> ProtocolResult<()> {
        self.validate()?;
        let valid = match self {
            Self::Complete => true,
            Self::ObjectCount(required) => required.get() <= total_objects,
            Self::ByteCount(required) => required.get() <= total_bytes,
        };
        if !valid {
            return Err(invalid(
                "coverage_goal",
                "threshold cannot exceed the Commit ObjectSet total",
            ));
        }
        Ok(())
    }
}

/// Stable idempotency identity for one target materialization intent.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct MaterializationJobKey {
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub commit_id: CommitId,
    pub target_storage_volume_id: StorageVolumeId,
    pub coverage_goal: CoverageGoal,
}

impl MaterializationJobKey {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.coverage_goal.validate()
    }
}

/// User-visible parent task for hydrating one target Volume from one or more sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationJob {
    pub materialization_id: MaterializationId,
    /// Unified operation identity that owns this domain detail record.
    pub operation_task_id: TaskId,
    /// Attempt identity used when Central fences data-plane reports.
    pub task_attempt_id: TaskAttemptId,
    pub key: MaterializationJobKey,
    pub artifact_id: ArtifactId,
    pub state: MaterializationJobState,
    pub plan_revision: Generation,
    pub object_count: DecimalU64,
    pub total_bytes: DecimalU64,
    pub verified_object_count: DecimalU64,
    pub verified_bytes: DecimalU64,
    pub missing_object_count: DecimalU64,
    pub missing_bytes: DecimalU64,
    pub source_count: DecimalU64,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue: Option<String>,
}

/// Parent materialization state.  `Stalled` is recoverable; Failed and Cancelled are terminal.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MaterializationJobState {
    Queued,
    Planning,
    WaitingForSources,
    Materializing,
    Verifying,
    Complete,
    Stalled,
    Failed,
    Cancelled,
}

impl MaterializationJobState {
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Cancelled)
    }

    /// Returns whether a durable compare-and-swap may move a Job between these states.
    /// Replaying an already-applied state is intentionally accepted as an idempotent operation.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        if self as u8 == next as u8 {
            return true;
        }
        matches!(
            (self, next),
            (
                Self::Queued,
                Self::Planning | Self::Stalled | Self::Cancelled | Self::Failed
            ) | (
                Self::Planning,
                Self::WaitingForSources
                    | Self::Materializing
                    | Self::Stalled
                    | Self::Failed
                    | Self::Cancelled
            ) | (
                Self::WaitingForSources,
                Self::Planning
                    | Self::Materializing
                    | Self::Stalled
                    | Self::Failed
                    | Self::Cancelled
            ) | (
                Self::Materializing,
                Self::Planning | Self::Verifying | Self::Stalled | Self::Failed | Self::Cancelled
            ) | (
                Self::Verifying,
                Self::Planning | Self::Complete | Self::Stalled | Self::Failed | Self::Cancelled
            ) | (
                Self::Stalled,
                Self::Planning | Self::Failed | Self::Cancelled
            ) | (Self::Failed, Self::Planning)
        )
    }
}

impl MaterializationJob {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.key.validate()?;
        // The initial v2 namespace mapping is one namespace per Artifact. Keep this check at the
        // Job boundary so a forged request cannot redirect a Commit into another namespace.
        if self.key.object_namespace_id != ObjectNamespaceId::from_artifact(&self.artifact_id) {
            return Err(invalid(
                "artifact_id/object_namespace_id",
                "the initial namespace mapping must equal artifact_id",
            ));
        }
        validate_positive("plan_revision", self.plan_revision.get())?;
        if self.created_at_unix_ms.get() == 0
            || self.updated_at_unix_ms.get() < self.created_at_unix_ms.get()
            || self.deadline_unix_ms.get() <= self.created_at_unix_ms.get()
        {
            return Err(invalid(
                "deadline_unix_ms",
                "timestamps or deadline are invalid",
            ));
        }
        let object_progress = self
            .verified_object_count
            .get()
            .checked_add(self.missing_object_count.get())
            .ok_or_else(|| invalid("progress", "object progress counters overflow"))?;
        let byte_progress = self
            .verified_bytes
            .get()
            .checked_add(self.missing_bytes.get())
            .ok_or_else(|| invalid("progress", "byte progress counters overflow"))?;
        if object_progress != self.object_count.get() || byte_progress != self.total_bytes.get() {
            return Err(invalid(
                "progress",
                "verified and missing progress must exactly partition requested totals",
            ));
        }
        if matches!(self.state, MaterializationJobState::Complete)
            && !self.key.coverage_goal.satisfied_by(
                self.verified_object_count.get(),
                self.verified_bytes.get(),
                self.object_count.get(),
                self.total_bytes.get(),
            )
        {
            return Err(invalid(
                "state",
                "complete Job does not satisfy its coverage goal",
            ));
        }
        validate_optional_text(
            "issue",
            self.issue.as_deref(),
            MAX_MATERIALIZATION_ERROR_BYTES,
        )
    }

    #[must_use]
    pub fn idempotency_key(&self) -> &MaterializationJobKey {
        &self.key
    }
}

/// State of one source-grouped transfer batch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MaterializationBatchState {
    Queued,
    Assigned,
    Transferring,
    Verifying,
    Succeeded,
    Failed,
}

impl MaterializationBatchState {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        if self as u8 == next as u8 {
            return true;
        }
        matches!(
            (self, next),
            (Self::Queued, Self::Assigned | Self::Failed)
                | (Self::Assigned, Self::Transferring | Self::Failed)
                | (Self::Transferring, Self::Verifying | Self::Failed)
                | (Self::Verifying, Self::Succeeded | Self::Failed)
                // Retry creates a new attempt but may reuse the durable batch identity.
                | (Self::Failed, Self::Queued)
        )
    }
}

/// State of one target object task.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MaterializationObjectState {
    Missing,
    Reserved,
    Transferring,
    Verified,
    Published,
    AlreadyPresent,
    Failed,
}

impl MaterializationObjectState {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        if self as u8 == next as u8 {
            return true;
        }
        matches!(
            (self, next),
            (
                Self::Missing,
                Self::Reserved | Self::AlreadyPresent | Self::Failed
            ) | (
                Self::Reserved,
                Self::Transferring | Self::Verified | Self::Missing | Self::Failed
            ) | (Self::Transferring, Self::Verified | Self::Failed)
                | (Self::Verified, Self::Published)
                | (Self::Failed, Self::Reserved | Self::Missing)
        )
    }
}

/// A source route and its current fenced network identity.  The route is shared by every object
/// in a Batch; each object's exact Placement identity is carried by the signed manifest page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationSource {
    pub placement_id: PlacementId,
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub storage_volume_id: Option<StorageVolumeId>,
    pub archive_id: Option<ArchiveId>,
    pub agent_id: AgentId,
    pub edge_cluster_id: EdgeClusterId,
    pub gateway_pool_id: GatewayPoolId,
    pub placement_generation: PlacementGeneration,
    pub session_generation: SessionGeneration,
    pub mount_generation: MountGeneration,
    pub route_generation: RouteGeneration,
}

impl MaterializationSource {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.storage_volume_id.is_some() == self.archive_id.is_some() {
            return Err(invalid(
                "source.storage_volume_id/archive_id",
                "exactly one source target is required",
            ));
        }
        validate_positive(
            "source.placement_generation",
            self.placement_generation.get(),
        )?;
        validate_positive("source.session_generation", self.session_generation.get())?;
        validate_positive("source.mount_generation", self.mount_generation.get())?;
        validate_positive("source.route_generation", self.route_generation.get())
    }
}

/// Target Volume and its current fenced route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationTarget {
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub storage_volume_id: StorageVolumeId,
    pub agent_id: AgentId,
    pub edge_cluster_id: EdgeClusterId,
    pub gateway_pool_id: GatewayPoolId,
    pub placement_generation: PlacementGeneration,
    pub session_generation: SessionGeneration,
    pub mount_generation: MountGeneration,
    pub route_generation: RouteGeneration,
}

impl MaterializationTarget {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive(
            "target.placement_generation",
            self.placement_generation.get(),
        )?;
        validate_positive("target.session_generation", self.session_generation.get())?;
        validate_positive("target.mount_generation", self.mount_generation.get())?;
        validate_positive("target.route_generation", self.route_generation.get())
    }
}

/// One source-grouped batch inside a [`MaterializationJob`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationBatch {
    pub batch_id: MaterializationBatchId,
    pub materialization_id: MaterializationId,
    pub plan_revision: Generation,
    pub batch_attempt: Generation,
    pub source: MaterializationSource,
    pub target: MaterializationTarget,
    pub manifest_digest: ContentDigest,
    pub object_ids: Vec<ObjectId>,
    pub object_count: DecimalU64,
    pub total_bytes: DecimalU64,
    pub state: MaterializationBatchState,
    pub max_bytes: DecimalU64,
    pub deadline_unix_ms: UnixMillis,
}

impl MaterializationBatch {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.source.validate()?;
        self.target.validate()?;
        if self.source.tenant_id != self.target.tenant_id
            || self.source.object_namespace_id != self.target.object_namespace_id
        {
            return Err(invalid(
                "source/target",
                "source and target must use the same tenant and object namespace",
            ));
        }
        if self.source.storage_volume_id.as_ref() == Some(&self.target.storage_volume_id) {
            return Err(invalid(
                "source/target",
                "source and target storage volumes must differ",
            ));
        }
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive("batch_attempt", self.batch_attempt.get())?;
        validate_collection_limit(
            "object_ids",
            self.object_ids.len(),
            MAX_MATERIALIZATION_BATCH_OBJECTS,
        )?;
        let mut previous = None;
        for object_id in &self.object_ids {
            if previous.is_some_and(|value| value >= *object_id) {
                return Err(invalid(
                    "object_ids",
                    "object IDs must be unique and sorted",
                ));
            }
            previous = Some(*object_id);
        }
        let object_count = u64::try_from(self.object_ids.len())
            .map_err(|_| invalid("object_count", "does not fit u64"))?;
        if self.object_count.get() != object_count {
            return Err(invalid("object_count", "does not match object_ids"));
        }
        if self.max_bytes.get() < self.total_bytes.get() {
            return Err(invalid("max_bytes", "cannot be less than total_bytes"));
        }
        if self.deadline_unix_ms.get() == 0 {
            return Err(invalid("deadline_unix_ms", "must be positive"));
        }
        Ok(())
    }
}

/// A stable per-object task persisted beneath a MaterializationJob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationObject {
    pub materialization_id: MaterializationId,
    pub object: ObjectRef,
    pub state: MaterializationObjectState,
    pub staging_key: String,
    pub confirmed_offset: DecimalU64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_source: Option<PlacementId>,
    #[serde(default)]
    #[schemars(length(max = MAX_MATERIALIZATION_SOURCES))]
    pub fallback_sources: Vec<PlacementId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_batch_id: Option<MaterializationBatchId>,
    pub plan_revision: Generation,
    pub attempt: Generation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl MaterializationObject {
    #[must_use]
    pub fn expected_staging_key(
        materialization_id: &MaterializationId,
        object_namespace_id: &ObjectNamespaceId,
        object_id: ObjectId,
    ) -> String {
        format!(
            "materialization/{}/{}/{}",
            materialization_id, object_namespace_id, object_id
        )
    }

    pub fn new(
        materialization_id: MaterializationId,
        object: ObjectRef,
        plan_revision: Generation,
    ) -> Self {
        let staging_key = Self::expected_staging_key(
            &materialization_id,
            &object.object_namespace_id,
            object.object_id,
        );
        Self {
            materialization_id,
            object,
            state: MaterializationObjectState::Missing,
            staging_key,
            confirmed_offset: DecimalU64::new(0),
            primary_source: None,
            fallback_sources: Vec::new(),
            current_batch_id: None,
            plan_revision,
            attempt: Generation::new(1),
            last_error: None,
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.object.validate()?;
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive("attempt", self.attempt.get())?;
        if self.confirmed_offset.get() > self.object.size.get() {
            return Err(invalid("confirmed_offset", "cannot exceed object size"));
        }
        if self.complete() && self.confirmed_offset != self.object.size {
            return Err(invalid(
                "confirmed_offset",
                "a completed object must have a full durable checkpoint",
            ));
        }
        validate_nonempty_limited("staging_key", &self.staging_key, MAX_STAGING_KEY_BYTES)?;
        let expected = Self::expected_staging_key(
            &self.materialization_id,
            &self.object.object_namespace_id,
            self.object.object_id,
        );
        if self.staging_key != expected {
            return Err(invalid(
                "staging_key",
                "must be the deterministic materialization key",
            ));
        }
        validate_collection_limit(
            "fallback_sources",
            self.fallback_sources.len(),
            MAX_MATERIALIZATION_SOURCES,
        )?;
        let mut sources = BTreeSet::new();
        for source in &self.fallback_sources {
            if !sources.insert(source) {
                return Err(invalid(
                    "fallback_sources",
                    "source placements must be unique",
                ));
            }
            if self.primary_source.as_ref() == Some(source) {
                return Err(invalid(
                    "fallback_sources",
                    "primary source must not be repeated",
                ));
            }
        }
        validate_optional_text(
            "last_error",
            self.last_error.as_deref(),
            MAX_MATERIALIZATION_ERROR_BYTES,
        )
    }

    #[must_use]
    pub const fn complete(&self) -> bool {
        matches!(
            self.state,
            MaterializationObjectState::Verified
                | MaterializationObjectState::Published
                | MaterializationObjectState::AlreadyPresent
        )
    }
}

/// A paged, exact list of ObjectRefs selected for one batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BatchManifestPage {
    pub materialization_id: MaterializationId,
    pub batch_id: MaterializationBatchId,
    pub plan_revision: Generation,
    pub batch_attempt: Generation,
    pub object_namespace_id: ObjectNamespaceId,
    pub page_number: DecimalU64,
    pub page_count: DecimalU64,
    #[schemars(length(max = MAX_MATERIALIZATION_MANIFEST_OBJECTS))]
    pub objects: Vec<ObjectRef>,
    /// Central-selected physical source for each object in this page.  The bindings are part of
    /// the page digest, so a source Agent cannot substitute another Placement while retaining a
    /// valid signed batch ticket.  Generic manifest helpers may omit bindings (for example an
    /// in-process protocol fixture); production materialization assignments always include one
    /// binding per object.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = MAX_MATERIALIZATION_MANIFEST_OBJECTS))]
    pub source_placements: Vec<MaterializationManifestSource>,
    pub page_digest: ContentDigest,
}

/// Object-to-Placement binding carried by a materialization manifest page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationManifestSource {
    pub object_id: ObjectId,
    pub placement_id: PlacementId,
    pub placement_generation: PlacementGeneration,
}

impl MaterializationManifestSource {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive(
            "source_placements.placement_generation",
            self.placement_generation.get(),
        )
    }
}

impl BatchManifestPage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        materialization_id: MaterializationId,
        batch_id: MaterializationBatchId,
        plan_revision: Generation,
        batch_attempt: Generation,
        object_namespace_id: ObjectNamespaceId,
        page_number: u64,
        page_count: u64,
        objects: Vec<ObjectRef>,
    ) -> ProtocolResult<Self> {
        Self::new_with_sources(
            materialization_id,
            batch_id,
            plan_revision,
            batch_attempt,
            object_namespace_id,
            page_number,
            page_count,
            objects,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_sources(
        materialization_id: MaterializationId,
        batch_id: MaterializationBatchId,
        plan_revision: Generation,
        batch_attempt: Generation,
        object_namespace_id: ObjectNamespaceId,
        page_number: u64,
        page_count: u64,
        objects: Vec<ObjectRef>,
        source_placements: Vec<MaterializationManifestSource>,
    ) -> ProtocolResult<Self> {
        let mut page = Self {
            materialization_id,
            batch_id,
            plan_revision,
            batch_attempt,
            object_namespace_id,
            page_number: DecimalU64::new(page_number),
            page_count: DecimalU64::new(page_count),
            objects,
            source_placements,
            page_digest: ContentDigest::from_bytes([0; 32]),
        };
        page.page_digest = page.digest_for()?;
        page.validate()?;
        Ok(page)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive("batch_attempt", self.batch_attempt.get())?;
        if self.page_count.get() == 0 || self.page_number.get() >= self.page_count.get() {
            return Err(invalid(
                "page_number/page_count",
                "page number must be within page count",
            ));
        }
        validate_collection_limit(
            "objects",
            self.objects.len(),
            MAX_MATERIALIZATION_MANIFEST_OBJECTS,
        )?;
        let mut seen = BTreeSet::new();
        let mut previous_ordinal = None;
        for object in &self.objects {
            object.validate()?;
            if object.object_namespace_id != self.object_namespace_id {
                return Err(invalid(
                    "objects",
                    "all objects must use the manifest namespace",
                ));
            }
            if !seen.insert(object.object_id) {
                return Err(invalid(
                    "objects",
                    "object IDs must be unique within a page",
                ));
            }
            if previous_ordinal.is_some_and(|ordinal| ordinal >= object.ordinal) {
                return Err(invalid("objects", "objects must be ordered by ordinal"));
            }
            previous_ordinal = Some(object.ordinal);
        }
        if !self.source_placements.is_empty() && self.source_placements.len() != self.objects.len()
        {
            return Err(invalid(
                "source_placements",
                "a bound manifest page must include exactly one source per object",
            ));
        }
        let object_ids = self
            .objects
            .iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let mut source_ids = BTreeSet::new();
        for source in &self.source_placements {
            source.validate()?;
            if !object_ids.contains(&source.object_id) || !source_ids.insert(source.object_id) {
                return Err(invalid(
                    "source_placements",
                    "source bindings must identify each page object exactly once",
                ));
            }
        }
        if self.page_digest != self.digest_for()? {
            return Err(ProtocolError::InvalidDigest(
                "manifest page digest does not match page contents".to_owned(),
            ));
        }
        Ok(())
    }

    fn digest_for(&self) -> ProtocolResult<ContentDigest> {
        #[derive(Serialize)]
        struct DigestInput<'a> {
            materialization_id: &'a MaterializationId,
            batch_id: &'a MaterializationBatchId,
            plan_revision: Generation,
            batch_attempt: Generation,
            object_namespace_id: &'a ObjectNamespaceId,
            page_number: DecimalU64,
            page_count: DecimalU64,
            objects: &'a [ObjectRef],
            source_placements: &'a [MaterializationManifestSource],
        }
        jcs_blake3(&DigestInput {
            materialization_id: &self.materialization_id,
            batch_id: &self.batch_id,
            plan_revision: self.plan_revision,
            batch_attempt: self.batch_attempt,
            object_namespace_id: &self.object_namespace_id,
            page_number: self.page_number,
            page_count: self.page_count,
            objects: &self.objects,
            source_placements: &self.source_placements,
        })
    }
}

/// Batch manifest descriptor.  The descriptor is small and is what a signed Ticket binds; pages
/// carry the potentially larger exact ObjectRef list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BatchManifest {
    pub materialization_id: MaterializationId,
    pub batch_id: MaterializationBatchId,
    pub plan_revision: Generation,
    pub batch_attempt: Generation,
    pub object_namespace_id: ObjectNamespaceId,
    pub page_count: DecimalU64,
    pub object_count: DecimalU64,
    pub total_bytes: DecimalU64,
    pub manifest_digest: ContentDigest,
}

/// Bounded Central-to-Agent command for one source-grouped materialization batch.
///
/// The exact ObjectSet never travels in this control message.  The signed Ticket commits the
/// manifest digest and the pages carry only the objects selected by the planner for this batch.
/// Reconnects may deliver the same command again; the target Agent uses the stable materialization
/// and object identities to resume its durable staging offsets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationAssignment {
    /// Unified operation identity copied from the signed ticket and durable Job.
    pub operation_task_id: TaskId,
    /// Exact execution attempt authorized for this assignment.
    pub task_attempt_id: TaskAttemptId,
    pub signed_ticket: SignedMaterializationBatchTicket,
    pub batch: MaterializationBatch,
    pub manifest: BatchManifest,
    #[schemars(length(max = 65_535))]
    pub pages: Vec<BatchManifestPage>,
    #[serde(default, flatten)]
    pub extensions: crate::Extensions,
}

impl MaterializationAssignment {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.signed_ticket.validate()?;
        if self.operation_task_id != self.signed_ticket.ticket.operation_task_id
            || self.task_attempt_id != self.signed_ticket.ticket.task_attempt_id
        {
            return Err(invalid(
                "task_identity",
                "assignment task identity does not match its signed ticket",
            ));
        }
        self.batch.validate()?;
        self.manifest.validate()?;
        if self.pages.is_empty() || self.pages.len() > 65_535 {
            return Err(invalid(
                "pages",
                "assignment must contain 1..=65535 manifest pages",
            ));
        }
        validate_materialization_assignment(
            &self.signed_ticket.ticket,
            &self.batch,
            &self.manifest,
            &self.pages,
        )?;
        validate_extension_keys(
            &self.extensions,
            &["signed_ticket", "batch", "manifest", "pages"],
        )
    }
}

/// Agent-to-Central object-level report for a materialization batch.
///
/// Receipts are emitted only after the target backend has completed its durability barrier.  A
/// failed report is advisory and never creates a Placement; Central keeps the batch retryable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "event",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MaterializationReport {
    Receipt {
        receipt: MaterializationObjectReceipt,
        /// The target fence copied from the signed batch ticket.  Keeping it on every report
        /// makes a delayed receipt/failure self-describing; Central still compares it with the
        /// durable Batch and the authenticated session/route before applying the report.
        target: MaterializationTarget,
        #[serde(default, flatten)]
        extensions: crate::Extensions,
    },
    Failed {
        operation_task_id: TaskId,
        task_attempt_id: TaskAttemptId,
        materialization_id: MaterializationId,
        batch_id: MaterializationBatchId,
        plan_revision: Generation,
        batch_attempt: Generation,
        tenant_id: TenantId,
        object_namespace_id: ObjectNamespaceId,
        /// The complete target fence from the signed assignment.  Failure reports must not rely
        /// on whatever route happens to be current when Central receives them.
        target: MaterializationTarget,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object_id: Option<ObjectId>,
        issue_code: String,
        issue_message: String,
        #[serde(default, flatten)]
        extensions: crate::Extensions,
    },
}

impl MaterializationReport {
    #[must_use]
    pub fn tenant_id(&self) -> &TenantId {
        match self {
            Self::Receipt { receipt, .. } => &receipt.tenant_id,
            Self::Failed { tenant_id, .. } => tenant_id,
        }
    }

    #[must_use]
    pub fn materialization_id(&self) -> &MaterializationId {
        match self {
            Self::Receipt { receipt, .. } => &receipt.materialization_id,
            Self::Failed {
                materialization_id, ..
            } => materialization_id,
        }
    }

    #[must_use]
    pub fn batch_attempt(&self) -> Generation {
        match self {
            Self::Receipt { receipt, .. } => receipt.batch_attempt,
            Self::Failed { batch_attempt, .. } => *batch_attempt,
        }
    }

    #[must_use]
    pub fn batch_id(&self) -> &MaterializationBatchId {
        match self {
            Self::Receipt { receipt, .. } => &receipt.batch_id,
            Self::Failed { batch_id, .. } => batch_id,
        }
    }

    #[must_use]
    pub fn plan_revision(&self) -> Generation {
        match self {
            Self::Receipt { receipt, .. } => receipt.plan_revision,
            Self::Failed { plan_revision, .. } => *plan_revision,
        }
    }

    #[must_use]
    pub fn object_namespace_id(&self) -> &ObjectNamespaceId {
        match self {
            Self::Receipt { receipt, .. } => &receipt.object_namespace_id,
            Self::Failed {
                object_namespace_id,
                ..
            } => object_namespace_id,
        }
    }

    #[must_use]
    pub fn operation_task_id(&self) -> &TaskId {
        match self {
            Self::Receipt { receipt, .. } => &receipt.operation_task_id,
            Self::Failed {
                operation_task_id, ..
            } => operation_task_id,
        }
    }

    #[must_use]
    pub fn task_attempt_id(&self) -> &TaskAttemptId {
        match self {
            Self::Receipt { receipt, .. } => &receipt.task_attempt_id,
            Self::Failed {
                task_attempt_id, ..
            } => task_attempt_id,
        }
    }

    #[must_use]
    pub fn target(&self) -> &MaterializationTarget {
        match self {
            Self::Receipt { target, .. } | Self::Failed { target, .. } => target,
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Receipt {
                receipt,
                target,
                extensions,
            } => {
                // The object identity is checked against the authoritative batch by Central. The
                // receipt still performs all local size/digest/durability checks here.
                receipt.validate()?;
                target.validate()?;
                if target.tenant_id != receipt.tenant_id
                    || target.object_namespace_id != receipt.object_namespace_id
                    || target.storage_volume_id != receipt.target_storage_volume_id
                    || target.placement_generation != receipt.target_placement_generation
                {
                    return Err(invalid(
                        "target",
                        "report target does not match receipt target identity",
                    ));
                }
                validate_extension_keys(extensions, &["receipt"])
            }
            Self::Failed {
                operation_task_id,
                task_attempt_id,
                materialization_id,
                batch_id,
                plan_revision,
                batch_attempt,
                tenant_id,
                object_namespace_id,
                target,
                object_id: _,
                issue_code,
                issue_message,
                extensions,
            } => {
                validate_positive("plan_revision", plan_revision.get())?;
                validate_positive("batch_attempt", batch_attempt.get())?;
                validate_nonempty_limited("issue_code", issue_code, 128)?;
                validate_nonempty_limited(
                    "issue_message",
                    issue_message,
                    MAX_MATERIALIZATION_ERROR_BYTES,
                )?;
                if materialization_id.as_str().is_empty()
                    || batch_id.as_str().is_empty()
                    || tenant_id.as_str().is_empty()
                    || object_namespace_id.as_str().is_empty()
                {
                    return Err(invalid("identity", "report identities must not be empty"));
                }
                if operation_task_id.as_str().is_empty() || task_attempt_id.as_str().is_empty() {
                    return Err(invalid(
                        "task_identity",
                        "report task identities must not be empty",
                    ));
                }
                target.validate()?;
                if target.tenant_id != *tenant_id
                    || target.object_namespace_id != *object_namespace_id
                {
                    return Err(invalid(
                        "target",
                        "report target does not match report tenant or namespace",
                    ));
                }
                validate_extension_keys(
                    extensions,
                    &[
                        "materialization_id",
                        "operation_task_id",
                        "task_attempt_id",
                        "batch_id",
                        "plan_revision",
                        "batch_attempt",
                        "tenant_id",
                        "object_namespace_id",
                        "object_id",
                        "issue_code",
                        "issue_message",
                    ],
                )
            }
        }
    }

    /// Validates a report against the exact assignment that authorized its batch. Central must
    /// perform this check before recording a receipt or advancing a batch: local receipt
    /// evidence alone cannot prevent a stale attempt from publishing into a newer plan.
    pub fn validate_for_assignment(
        &self,
        assignment: &MaterializationAssignment,
    ) -> ProtocolResult<()> {
        self.validate()?;
        assignment.validate()?;
        let ticket = &assignment.signed_ticket.ticket;
        match self {
            Self::Receipt {
                receipt, target, ..
            } => {
                let object = assignment
                    .pages
                    .iter()
                    .flat_map(|page| page.objects.iter())
                    .find(|object| object.object_id == receipt.object_id)
                    .ok_or_else(|| {
                        invalid(
                            "receipt.object_id",
                            "object is not present in the assignment",
                        )
                    })?;
                if target != &ticket.target {
                    return Err(invalid(
                        "target",
                        "receipt target fence differs from its assignment",
                    ));
                }
                receipt.validate_against_ticket(ticket, object)
            }
            Self::Failed {
                operation_task_id,
                task_attempt_id,
                materialization_id,
                batch_id,
                plan_revision,
                batch_attempt,
                tenant_id,
                object_namespace_id,
                target,
                object_id,
                ..
            } => {
                if materialization_id != &ticket.materialization_id
                    || operation_task_id != &ticket.operation_task_id
                    || task_attempt_id != &ticket.task_attempt_id
                    || batch_id != &ticket.batch_id
                    || plan_revision != &ticket.plan_revision
                    || batch_attempt != &ticket.batch_attempt
                    || tenant_id != &ticket.tenant_id
                    || object_namespace_id != &ticket.object_namespace_id
                    || target != &ticket.target
                {
                    return Err(invalid(
                        "report",
                        "failure report identity or generation differs from its assignment",
                    ));
                }
                if let Some(object_id) = object_id {
                    if !assignment
                        .pages
                        .iter()
                        .flat_map(|page| page.objects.iter())
                        .any(|object| object.object_id == *object_id)
                    {
                        return Err(invalid(
                            "object_id",
                            "failure report object is not present in the assignment",
                        ));
                    }
                }
                Ok(())
            }
        }
    }
}

/// Shared assignment validation kept public so binary transfer and NDJSON adapters cannot drift.
pub fn validate_materialization_assignment(
    ticket: &MaterializationBatchTicket,
    batch: &MaterializationBatch,
    manifest: &BatchManifest,
    pages: &[BatchManifestPage],
) -> ProtocolResult<()> {
    ticket.validate()?;
    batch.validate()?;
    manifest.validate()?;
    if ticket.materialization_id != batch.materialization_id
        || ticket.batch_id != batch.batch_id
        || ticket.plan_revision != batch.plan_revision
        || ticket.batch_attempt != batch.batch_attempt
        || ticket.source != batch.source
        || ticket.target != batch.target
        || manifest.object_namespace_id != batch.source.object_namespace_id
        || ticket.manifest_digest != manifest.manifest_digest
        || batch.manifest_digest != manifest.manifest_digest
    {
        return Err(invalid(
            "assignment",
            "ticket, batch and manifest identities do not match",
        ));
    }
    ticket.validate_against_manifest(manifest)?;
    let reconstructed = BatchManifest::from_pages(pages)?;
    if reconstructed != *manifest {
        return Err(invalid(
            "pages",
            "manifest descriptor does not match supplied pages",
        ));
    }
    let mut object_ids = pages
        .iter()
        .flat_map(|page| page.objects.iter().map(|object| object.object_id))
        .collect::<Vec<_>>();
    object_ids.sort_unstable();
    let mut expected = batch.object_ids.clone();
    expected.sort_unstable();
    if object_ids != expected {
        return Err(invalid(
            "pages",
            "manifest objects do not match the batch object plan",
        ));
    }
    // A v2 batch is source-grouped, but a source Volume can contain several Placement IDs.  The
    // signed manifest therefore has to bind every non-empty object to its exact Placement.  Keep
    // the zero-object case valid for idempotent/control-channel fixtures; there is no payload that
    // could be widened in that case.
    if !object_ids.is_empty()
        && pages
            .iter()
            .any(|page| page.source_placements.len() != page.objects.len())
    {
        return Err(invalid(
            "source_placements",
            "every non-empty materialization batch object must bind one source Placement",
        ));
    }
    if manifest.object_count != batch.object_count || manifest.total_bytes != batch.total_bytes {
        return Err(invalid(
            "manifest",
            "manifest counters do not match the batch plan",
        ));
    }
    for source in pages.iter().flat_map(|page| page.source_placements.iter()) {
        if source.placement_generation != ticket.source.placement_generation {
            return Err(invalid(
                "source_placements",
                "object source placement generation differs from the signed source route",
            ));
        }
    }
    Ok(())
}

impl BatchManifest {
    pub fn from_pages(pages: &[BatchManifestPage]) -> ProtocolResult<Self> {
        if pages.is_empty() {
            return Err(invalid("pages", "at least one manifest page is required"));
        }
        // Page order is part of the signed manifest digest.  Do not normalize an out-of-order
        // stream here: accepting it would allow two different wire representations to describe
        // the same manifest and would hide transport/reassembly bugs from callers.
        let first = pages
            .first()
            .ok_or_else(|| invalid("pages", "at least one manifest page is required"))?;
        if first.page_count.get() != pages.len() as u64 {
            return Err(invalid(
                "page_count",
                "manifest descriptor must include every declared page",
            ));
        }
        let mut seen_objects = BTreeSet::new();
        let mut previous_ordinal = None;
        for (index, page) in pages.iter().enumerate() {
            page.validate()?;
            if page.materialization_id != first.materialization_id
                || page.batch_id != first.batch_id
                || page.plan_revision != first.plan_revision
                || page.batch_attempt != first.batch_attempt
                || page.object_namespace_id != first.object_namespace_id
                || page.page_count != first.page_count
                || page.page_number.get() != index as u64
            {
                return Err(invalid(
                    "pages",
                    "manifest pages do not form one ordered batch",
                ));
            }
            for object in &page.objects {
                if !seen_objects.insert(object.object_id) {
                    return Err(invalid(
                        "pages",
                        "an object may occur only once in a batch manifest",
                    ));
                }
                // Object ordinals come from the Commit ObjectSet and are globally unique. Keep
                // the page stream canonically ordered as well; otherwise two valid page sets can
                // describe the same objects with different ordering and digest inputs.
                if previous_ordinal.is_some_and(|ordinal| ordinal >= object.ordinal) {
                    return Err(invalid(
                        "pages",
                        "manifest objects must be globally ordered by ordinal",
                    ));
                }
                previous_ordinal = Some(object.ordinal);
            }
        }
        let mut objects = Vec::new();
        for page in pages {
            objects.extend(page.objects.iter().cloned());
        }
        let object_count = u64::try_from(objects.len())
            .map_err(|_| invalid("object_count", "does not fit u64"))?;
        let total_bytes = objects.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.size.get())
                .ok_or_else(|| invalid("total_bytes", "exceeds u64"))
        })?;
        let manifest_digest = Self::digest_for_pages(pages)?;
        let manifest = Self {
            materialization_id: first.materialization_id.clone(),
            batch_id: first.batch_id.clone(),
            plan_revision: first.plan_revision,
            batch_attempt: first.batch_attempt,
            object_namespace_id: first.object_namespace_id.clone(),
            page_count: first.page_count,
            object_count: DecimalU64::new(object_count),
            total_bytes: DecimalU64::new(total_bytes),
            manifest_digest,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn paginate(
        materialization_id: MaterializationId,
        batch_id: MaterializationBatchId,
        plan_revision: Generation,
        batch_attempt: Generation,
        object_namespace_id: ObjectNamespaceId,
        objects: Vec<ObjectRef>,
        page_size: usize,
    ) -> ProtocolResult<(Self, Vec<BatchManifestPage>)> {
        Self::paginate_with_sources(
            materialization_id,
            batch_id,
            plan_revision,
            batch_attempt,
            object_namespace_id,
            objects,
            Vec::new(),
            page_size,
        )
    }

    /// Paginates a manifest and binds every object to the exact source Placement selected by
    /// Central.  Bindings are optional only for the generic `paginate` helper used by protocol
    /// fixtures; passing a non-empty list requires one binding for every object.
    #[allow(clippy::too_many_arguments)]
    pub fn paginate_with_sources(
        materialization_id: MaterializationId,
        batch_id: MaterializationBatchId,
        plan_revision: Generation,
        batch_attempt: Generation,
        object_namespace_id: ObjectNamespaceId,
        objects: Vec<ObjectRef>,
        source_placements: Vec<MaterializationManifestSource>,
        page_size: usize,
    ) -> ProtocolResult<(Self, Vec<BatchManifestPage>)> {
        if page_size == 0 || page_size > MAX_MATERIALIZATION_MANIFEST_OBJECTS {
            return Err(invalid("page_size", "must be between 1 and manifest limit"));
        }
        validate_collection_limit(
            "objects",
            objects.len(),
            MAX_MATERIALIZATION_MANIFEST_OBJECTS * 65_535,
        )?;
        let mut objects = objects;
        for object in &objects {
            object.validate()?;
            if object.object_namespace_id != object_namespace_id {
                return Err(invalid(
                    "objects",
                    "all objects must use the manifest namespace",
                ));
            }
        }
        objects.sort_by_key(|object| object.ordinal);
        if objects
            .windows(2)
            .any(|pair| pair[0].object_id == pair[1].object_id)
        {
            return Err(invalid("objects", "object IDs must be unique"));
        }
        let mut source_by_object = BTreeMap::new();
        for source in source_placements {
            source.validate()?;
            if source_by_object.insert(source.object_id, source).is_some() {
                return Err(invalid(
                    "source_placements",
                    "source bindings must be unique per object",
                ));
            }
        }
        if !source_by_object.is_empty()
            && (source_by_object.len() != objects.len()
                || objects
                    .iter()
                    .any(|object| !source_by_object.contains_key(&object.object_id)))
        {
            return Err(invalid(
                "source_placements",
                "a bound manifest must include one source binding for every object",
            ));
        }
        let page_count = objects.len().div_ceil(page_size).max(1);
        let page_count_u64 =
            u64::try_from(page_count).map_err(|_| invalid("page_count", "does not fit u64"))?;
        let mut pages = Vec::with_capacity(page_count);
        for (page_number, chunk) in objects.chunks(page_size).enumerate() {
            let bindings = chunk
                .iter()
                .filter_map(|object| source_by_object.get(&object.object_id).cloned())
                .collect();
            pages.push(BatchManifestPage::new_with_sources(
                materialization_id.clone(),
                batch_id.clone(),
                plan_revision,
                batch_attempt,
                object_namespace_id.clone(),
                page_number as u64,
                page_count_u64,
                chunk.to_vec(),
                bindings,
            )?);
        }
        if pages.is_empty() {
            pages.push(BatchManifestPage::new(
                materialization_id,
                batch_id,
                plan_revision,
                batch_attempt,
                object_namespace_id,
                0,
                1,
                Vec::new(),
            )?);
        }
        let manifest = Self::from_pages(&pages)?;
        Ok((manifest, pages))
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive("batch_attempt", self.batch_attempt.get())?;
        if self.page_count.get() == 0 {
            return Err(invalid("page_count", "must be positive"));
        }
        Ok(())
    }

    /// Recomputes this descriptor from its pages and rejects any count, identity, or digest
    /// mismatch. A descriptor alone cannot prove its digest because pages intentionally carry the
    /// large object list; receivers should call this before opening object streams.
    pub fn validate_against_pages(&self, pages: &[BatchManifestPage]) -> ProtocolResult<()> {
        self.validate()?;
        let expected = Self::from_pages(pages)?;
        if self != &expected {
            return Err(ProtocolError::InvalidDigest(
                "manifest descriptor does not match its pages".to_owned(),
            ));
        }
        Ok(())
    }

    fn digest_for_pages(pages: &[BatchManifestPage]) -> ProtocolResult<ContentDigest> {
        #[derive(Serialize)]
        struct DigestInput<'a> {
            materialization_id: &'a MaterializationId,
            batch_id: &'a MaterializationBatchId,
            plan_revision: Generation,
            batch_attempt: Generation,
            object_namespace_id: &'a ObjectNamespaceId,
            page_digests: Vec<ContentDigest>,
        }
        let first = pages
            .first()
            .ok_or_else(|| invalid("pages", "empty manifest"))?;
        jcs_blake3(&DigestInput {
            materialization_id: &first.materialization_id,
            batch_id: &first.batch_id,
            plan_revision: first.plan_revision,
            batch_attempt: first.batch_attempt,
            object_namespace_id: &first.object_namespace_id,
            page_digests: pages.iter().map(|page| page.page_digest).collect(),
        })
    }
}

/// Central-issued, short-lived capability for exactly one source-grouped materialization batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationBatchTicket {
    pub ticket_id: ObjectTicketId,
    pub operation_task_id: TaskId,
    pub task_attempt_id: TaskAttemptId,
    pub materialization_id: MaterializationId,
    pub batch_id: MaterializationBatchId,
    pub plan_revision: Generation,
    pub batch_attempt: Generation,
    pub tenant_id: TenantId,
    pub artifact_id: ArtifactId,
    pub object_namespace_id: ObjectNamespaceId,
    pub commit_id: CommitId,
    pub manifest_digest: ContentDigest,
    pub source: MaterializationSource,
    pub target: MaterializationTarget,
    pub max_bytes: DecimalU64,
    pub deadline_unix_ms: UnixMillis,
    pub capability: String,
}

impl MaterializationBatchTicket {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive("batch_attempt", self.batch_attempt.get())?;
        if self.operation_task_id.as_str().is_empty() || self.task_attempt_id.as_str().is_empty() {
            return Err(invalid(
                "task_identity",
                "ticket task identities must not be empty",
            ));
        }
        self.source.validate()?;
        self.target.validate()?;
        if self.source.tenant_id != self.tenant_id
            || self.source.object_namespace_id != self.object_namespace_id
            || self.target.tenant_id != self.tenant_id
            || self.target.object_namespace_id != self.object_namespace_id
        {
            return Err(invalid(
                "source/target",
                "ticket identities do not match tenant/namespace",
            ));
        }
        if self.object_namespace_id != ObjectNamespaceId::from_artifact(&self.artifact_id) {
            return Err(invalid(
                "artifact_id/object_namespace_id",
                "the initial namespace mapping must equal artifact_id",
            ));
        }
        if self.source.storage_volume_id.as_ref() == Some(&self.target.storage_volume_id) {
            return Err(invalid(
                "source/target",
                "source and target storage volumes must differ",
            ));
        }
        if self.capability != COMMIT_MATERIALIZATION_CAPABILITY_V2 {
            return Err(ProtocolError::UnsupportedMessageType(
                self.capability.clone(),
            ));
        }
        if self.max_bytes.get() == 0 || self.deadline_unix_ms.get() == 0 {
            return Err(invalid("max_bytes/deadline_unix_ms", "must be positive"));
        }
        Ok(())
    }

    /// Canonical bytes covered by the Central signature.  Domain separation prevents a ticket from
    /// being replayed as another signed payload type.
    pub fn payload_bytes(&self) -> ProtocolResult<Vec<u8>> {
        self.validate()?;
        domain_separated_jcs_bytes(MATERIALIZATION_TICKET_SIGNING_DOMAIN, self)
    }

    /// Validates the binding between this capability and the exact manifest descriptor sent on
    /// the transfer stream. The ticket intentionally carries only the manifest digest, so callers
    /// must perform this check after receiving the descriptor and before accepting any object
    /// request.
    pub fn validate_against_manifest(&self, manifest: &BatchManifest) -> ProtocolResult<()> {
        self.validate()?;
        manifest.validate()?;
        if self.materialization_id != manifest.materialization_id
            || self.batch_id != manifest.batch_id
            || self.plan_revision != manifest.plan_revision
            || self.batch_attempt != manifest.batch_attempt
            || self.object_namespace_id != manifest.object_namespace_id
            || self.manifest_digest != manifest.manifest_digest
        {
            return Err(invalid(
                "manifest",
                "manifest identity or digest does not match the batch ticket",
            ));
        }
        if self.max_bytes.get() < manifest.total_bytes.get() {
            return Err(invalid(
                "max_bytes",
                "ticket byte limit cannot be less than manifest total",
            ));
        }
        Ok(())
    }
}

/// A v2 ticket plus the Central signature envelope transported through Gateway hops.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignedMaterializationBatchTicket {
    pub ticket: MaterializationBatchTicket,
    pub central_signature: CentralSignedPayload,
}

/// Generic names used by transport adapters while retaining the explicit Batch terminology in
/// the canonical type.
pub type MaterializationTicket = MaterializationBatchTicket;
pub type SignedMaterializationTicket = SignedMaterializationBatchTicket;

impl SignedMaterializationBatchTicket {
    pub fn new(
        ticket: MaterializationBatchTicket,
        central_signature: CentralSignedPayload,
    ) -> ProtocolResult<Self> {
        ticket.validate()?;
        central_signature.validate()?;
        if central_signature.expires_at_unix_ms != ticket.deadline_unix_ms {
            return Err(invalid(
                "central_signature.expires_at_unix_ms",
                "signature expiry must equal the batch ticket deadline",
            ));
        }
        let payload = ticket.payload_bytes()?;
        if central_signature.payload.as_bytes() != payload.as_slice() {
            return Err(invalid(
                "central_signature",
                "signature payload does not match ticket",
            ));
        }
        Ok(Self {
            ticket,
            central_signature,
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.ticket.validate()?;
        self.central_signature.validate()?;
        if self.central_signature.expires_at_unix_ms != self.ticket.deadline_unix_ms {
            return Err(invalid(
                "central_signature.expires_at_unix_ms",
                "signature expiry must equal the batch ticket deadline",
            ));
        }
        if self.central_signature.payload.as_bytes() != self.ticket.payload_bytes()?.as_slice() {
            return Err(invalid(
                "central_signature",
                "signature payload does not match ticket",
            ));
        }
        Ok(())
    }

    /// Validates the signed capability and its manifest binding in one operation.
    pub fn validate_against_manifest(&self, manifest: &BatchManifest) -> ProtocolResult<()> {
        self.validate()?;
        self.ticket.validate_against_manifest(manifest)
    }
}

/// Object-level durable receipt emitted by a target Agent after its durability barrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializationObjectReceipt {
    pub receipt_id: crate::ObjectReceiptId,
    pub operation_task_id: TaskId,
    pub task_attempt_id: TaskAttemptId,
    pub materialization_id: MaterializationId,
    pub batch_id: MaterializationBatchId,
    pub plan_revision: Generation,
    pub batch_attempt: Generation,
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub object_id: ObjectId,
    pub size: DecimalU64,
    pub encoding: ObjectEncoding,
    pub verified_digest: ContentDigest,
    pub target_storage_volume_id: StorageVolumeId,
    pub target_placement_generation: PlacementGeneration,
    pub committed_offset: DecimalU64,
    pub verified_at_unix_ms: UnixMillis,
}

/// Short spelling used by receipt repositories and Agent adapters.
pub type ObjectReceipt = MaterializationObjectReceipt;

impl MaterializationObjectReceipt {
    /// Validates receipt-local evidence before checking it against a Commit object or ticket.
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.operation_task_id.as_str().is_empty() || self.task_attempt_id.as_str().is_empty() {
            return Err(invalid(
                "task_identity",
                "receipt task identities must not be empty",
            ));
        }
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive("batch_attempt", self.batch_attempt.get())?;
        validate_positive(
            "target_placement_generation",
            self.target_placement_generation.get(),
        )?;
        if self.committed_offset != self.size || self.verified_at_unix_ms.get() == 0 {
            return Err(invalid(
                "committed_offset/verified_at_unix_ms",
                "receipt is not durable",
            ));
        }
        if self.verified_digest != self.object_id.digest() {
            return Err(ProtocolError::InvalidDigest(
                "verified_digest must equal object_id".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn validate_against(&self, object: &ObjectRef) -> ProtocolResult<()> {
        self.validate()?;
        if self.object_namespace_id != object.object_namespace_id
            || self.object_id != object.object_id
            || self.size != object.size
            || self.encoding != object.encoding
        {
            return Err(invalid("object", "receipt does not match ObjectRef"));
        }
        Ok(())
    }

    /// Validates receipt identity and fencing against the ticket that authorized its batch.
    /// Report/finalize handlers should use this check so an old attempt cannot be accepted merely
    /// because its object bytes happen to hash correctly.
    pub fn validate_against_ticket(
        &self,
        ticket: &MaterializationBatchTicket,
        object: &ObjectRef,
    ) -> ProtocolResult<()> {
        ticket.validate()?;
        self.validate_against(object)?;
        if self.materialization_id != ticket.materialization_id
            || self.operation_task_id != ticket.operation_task_id
            || self.task_attempt_id != ticket.task_attempt_id
            || self.batch_id != ticket.batch_id
            || self.plan_revision != ticket.plan_revision
            || self.batch_attempt != ticket.batch_attempt
            || self.tenant_id != ticket.tenant_id
            || self.object_namespace_id != ticket.object_namespace_id
            || self.target_storage_volume_id != ticket.target.storage_volume_id
            || self.target_placement_generation != ticket.target.placement_generation
        {
            return Err(invalid(
                "receipt",
                "receipt identity or generation does not match the batch ticket",
            ));
        }
        if self.verified_at_unix_ms.get() >= ticket.deadline_unix_ms.get() {
            return Err(invalid(
                "verified_at_unix_ms",
                "receipt verification must occur before the batch ticket deadline",
            ));
        }
        Ok(())
    }
}

/// Derives the durable Placement identity for a materialized object on its target Volume.
///
/// A receipt identifies one report and is therefore intentionally not part of this identity.
/// The physical copy is instead keyed by the tenant, namespace, object, target Volume, and
/// placement generation.  This lets retries, source failover, and competing batches converge on
/// one Placement row.  Keeping the derivation in the domain crate prevents Central and Agent
/// implementations from assigning different identities to the same durable bytes.
pub fn materialization_target_placement_id(
    receipt: &MaterializationObjectReceipt,
) -> ProtocolResult<PlacementId> {
    #[derive(Serialize)]
    struct PlacementIdentity<'a> {
        kind: &'static str,
        tenant_id: &'a TenantId,
        object_namespace_id: &'a ObjectNamespaceId,
        object_id: ObjectId,
        storage_volume_id: &'a StorageVolumeId,
        placement_generation: PlacementGeneration,
    }

    let digest = jcs_blake3(&PlacementIdentity {
        kind: "materialization-target-placement-v2",
        tenant_id: &receipt.tenant_id,
        object_namespace_id: &receipt.object_namespace_id,
        object_id: receipt.object_id,
        storage_volume_id: &receipt.target_storage_volume_id,
        placement_generation: receipt.target_placement_generation,
    })?;
    PlacementId::new(format!(
        "materialization-placement-{}",
        &digest.to_hex()[..32]
    ))
}

/// Lifecycle of a source/target protection lease.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MaterializationLeaseState {
    Active,
    Released,
    Expired,
}

/// Protects a source object placement from GC while it is being read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectReadLease {
    pub lease_id: crate::LeaseId,
    pub materialization_id: MaterializationId,
    pub batch_id: MaterializationBatchId,
    pub plan_revision: Generation,
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub object_id: ObjectId,
    pub placement_id: PlacementId,
    pub placement_generation: PlacementGeneration,
    pub expires_at_unix_ms: UnixMillis,
    pub state: MaterializationLeaseState,
}

impl ObjectReadLease {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive("placement_generation", self.placement_generation.get())?;
        if self.expires_at_unix_ms.get() == 0 {
            return Err(invalid("expires_at_unix_ms", "must be positive"));
        }
        Ok(())
    }

    /// Validates a lease before it is inserted as a new protection record.
    ///
    /// Released and expired leases remain valid persisted history, but accepting either state on
    /// insertion would let a caller create a lease which never protects the source object.  Keep
    /// this stricter check separate from [`Self::validate`] so decoders can still read terminal
    /// lease records during recovery and GC.
    pub fn validate_for_acquisition(&self) -> ProtocolResult<()> {
        self.validate()?;
        if self.state != MaterializationLeaseState::Active {
            return Err(invalid(
                "state",
                "a newly inserted object read lease must be active",
            ));
        }
        Ok(())
    }

    pub fn validate_against_placement(&self, placement: &ObjectPlacement) -> ProtocolResult<()> {
        self.validate()?;
        placement.validate()?;
        if self.tenant_id != placement.tenant_id
            || self.object_namespace_id != placement.object_namespace_id
            || self.object_id != placement.object_id
            || self.placement_id != placement.placement_id
            || self.placement_generation != placement.placement_generation
        {
            return Err(invalid(
                "placement",
                "read lease does not match the fenced placement",
            ));
        }
        Ok(())
    }
}

/// Derives the durable identity of the source protection lease for one object attempt.
///
/// The identity intentionally includes the batch attempt: a replan must be able to acquire a
/// fresh source lease while the old lease remains as immutable release history.  The object
/// staging identity is separate and remains stable across these attempts.
pub fn object_read_lease_id(
    materialization_id: &MaterializationId,
    batch_id: &MaterializationBatchId,
    plan_revision: Generation,
    batch_attempt: Generation,
    object_namespace_id: &ObjectNamespaceId,
    object_id: ObjectId,
    placement_id: &PlacementId,
) -> ProtocolResult<crate::LeaseId> {
    #[derive(Serialize)]
    struct LeaseIdentity<'a> {
        kind: &'static str,
        materialization_id: &'a MaterializationId,
        batch_id: &'a MaterializationBatchId,
        plan_revision: Generation,
        batch_attempt: Generation,
        object_namespace_id: &'a ObjectNamespaceId,
        object_id: ObjectId,
        placement_id: &'a PlacementId,
    }
    let digest = jcs_blake3(&LeaseIdentity {
        kind: "object_read",
        materialization_id,
        batch_id,
        plan_revision,
        batch_attempt,
        object_namespace_id,
        object_id,
        placement_id,
    })?;
    crate::LeaseId::new(format!("object-read-{}", &digest.to_hex()[..32]))
}

/// Protects target staging bytes until a MaterializationObject is published or cleaned up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StagingLease {
    pub lease_id: crate::LeaseId,
    pub materialization_id: MaterializationId,
    pub plan_revision: Generation,
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub object_id: ObjectId,
    pub target_storage_volume_id: StorageVolumeId,
    pub target_placement_generation: PlacementGeneration,
    pub staging_key: String,
    pub expires_at_unix_ms: UnixMillis,
    pub state: MaterializationLeaseState,
}

impl StagingLease {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("plan_revision", self.plan_revision.get())?;
        validate_positive(
            "target_placement_generation",
            self.target_placement_generation.get(),
        )?;
        validate_nonempty_limited("staging_key", &self.staging_key, MAX_STAGING_KEY_BYTES)?;
        let expected = MaterializationObject::expected_staging_key(
            &self.materialization_id,
            &self.object_namespace_id,
            self.object_id,
        );
        if self.staging_key != expected {
            return Err(invalid(
                "staging_key",
                "must be the deterministic materialization key",
            ));
        }
        if self.expires_at_unix_ms.get() == 0 {
            return Err(invalid("expires_at_unix_ms", "must be positive"));
        }
        Ok(())
    }

    /// Validates a lease before it is inserted as a new staging protection record.  Terminal
    /// states are accepted by [`Self::validate`] for persisted-record decoding, but are not valid
    /// acquisition requests.
    pub fn validate_for_acquisition(&self) -> ProtocolResult<()> {
        self.validate()?;
        if self.state != MaterializationLeaseState::Active {
            return Err(invalid(
                "state",
                "a newly inserted staging lease must be active",
            ));
        }
        Ok(())
    }

    pub fn validate_against_object(&self, object: &MaterializationObject) -> ProtocolResult<()> {
        self.validate()?;
        object.validate()?;
        if self.materialization_id != object.materialization_id
            || self.object_namespace_id != object.object.object_namespace_id
            || self.object_id != object.object.object_id
            || self.staging_key != object.staging_key
            || self.plan_revision != object.plan_revision
        {
            return Err(invalid(
                "object",
                "staging lease does not match the materialization object",
            ));
        }
        Ok(())
    }
}

/// Derives the durable identity of the staging protection lease for one object plan.
///
/// Unlike the source lease, this identity is independent of the batch/source attempt.  A retry
/// therefore creates a new plan-scoped lease without changing the stable staging key or offset,
/// and a delayed release from an older plan cannot release the new lease.
pub fn staging_lease_id(
    materialization_id: &MaterializationId,
    plan_revision: Generation,
    object_namespace_id: &ObjectNamespaceId,
    object_id: ObjectId,
) -> ProtocolResult<crate::LeaseId> {
    #[derive(Serialize)]
    struct LeaseIdentity<'a> {
        kind: &'static str,
        materialization_id: &'a MaterializationId,
        plan_revision: Generation,
        object_namespace_id: &'a ObjectNamespaceId,
        object_id: ObjectId,
    }
    let digest = jcs_blake3(&LeaseIdentity {
        kind: "staging",
        materialization_id,
        plan_revision,
        object_namespace_id,
        object_id,
    })?;
    crate::LeaseId::new(format!("staging-{}", &digest.to_hex()[..32]))
}

/// Compatibility spelling used by GC implementations; both names carry the same v2 contract.
pub type MaterializationLease = StagingLease;

/// Independent dimensions returned by availability queries.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AvailabilityStatus {
    Available,
    Degraded,
    Unavailable,
    Unknown,
}

/// Availability view intentionally separates global content from target readiness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommitAvailability {
    pub tenant_id: TenantId,
    pub object_namespace_id: ObjectNamespaceId,
    pub commit_id: CommitId,
    pub content_presence: AvailabilityStatus,
    pub source_serving: AvailabilityStatus,
    pub durability: AvailabilityStatus,
    pub target_coverage: CoverageState,
    pub view_readiness: ViewReadiness,
    pub complete_volume_count: DecimalU64,
}

/// Whether a target local view can be exposed to Workspace, Delivery, or S3 readers.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ViewReadiness {
    NotReady,
    Ready,
}

impl CommitAvailability {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.view_readiness == ViewReadiness::Ready && !self.target_coverage.complete() {
            return Err(invalid(
                "view_readiness",
                "a partial target cannot be Ready",
            ));
        }
        if self.target_coverage.complete() && self.complete_volume_count.get() == 0 {
            return Err(invalid(
                "complete_volume_count",
                "a complete target coverage requires at least one complete volume",
            ));
        }
        Ok(())
    }
}

/// Computes a target coverage summary from object references and verified placements.
pub fn recompute_coverage(
    tenant_id: TenantId,
    object_namespace_id: ObjectNamespaceId,
    commit_id: CommitId,
    storage_volume_id: StorageVolumeId,
    placement_generation: PlacementGeneration,
    object_set: &ObjectSet,
    placements: &[ObjectPlacement],
) -> ProtocolResult<VolumeCommitCoverage> {
    VolumeCommitCoverage::from_placements(
        tenant_id,
        object_namespace_id,
        commit_id,
        storage_volume_id,
        placement_generation,
        object_set,
        placements,
    )
}

// Keep these imports in the module's public API documentation and make accidental regressions in
// endpoint identity obvious to downstream implementers.  They are intentionally not serialized
// fields of the v2 contracts themselves.
#[allow(dead_code)]
fn _backend_marker(_: BackendId, _: GatewayConnectionId) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(byte: u8, ordinal: u64) -> ObjectRef {
        ObjectRef::new(
            ObjectNamespaceId::new("artifact-a").unwrap(),
            ObjectId::from_bytes([byte; 32]),
            4,
            ObjectEncoding::Raw,
            ordinal,
        )
    }

    fn source() -> MaterializationSource {
        MaterializationSource {
            placement_id: PlacementId::new("placement-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
            storage_volume_id: Some(StorageVolumeId::new("volume-a").unwrap()),
            archive_id: None,
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("gateway-a").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            route_generation: RouteGeneration::new(1),
        }
    }

    fn target() -> MaterializationTarget {
        MaterializationTarget {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
            agent_id: AgentId::new("agent-b").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-b").unwrap(),
            gateway_pool_id: GatewayPoolId::new("gateway-b").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            session_generation: SessionGeneration::new(2),
            mount_generation: MountGeneration::new(2),
            route_generation: RouteGeneration::new(2),
        }
    }

    fn operation_task_id() -> TaskId {
        TaskId::new("task-materialization-test").unwrap()
    }

    fn task_attempt_id() -> TaskAttemptId {
        TaskAttemptId::new("task-materialization-test-attempt-1").unwrap()
    }

    fn empty_assignment() -> MaterializationAssignment {
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let materialization_id = MaterializationId::new("materialization-assignment").unwrap();
        let batch_id = MaterializationBatchId::new("batch-assignment").unwrap();
        let (manifest, pages) = BatchManifest::paginate(
            materialization_id.clone(),
            batch_id.clone(),
            Generation::new(1),
            Generation::new(1),
            namespace.clone(),
            Vec::new(),
            1,
        )
        .unwrap();
        let ticket = MaterializationBatchTicket {
            ticket_id: ObjectTicketId::new("ticket-assignment").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: materialization_id.clone(),
            batch_id: batch_id.clone(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            object_namespace_id: namespace,
            commit_id: CommitId::from_bytes([9; 32]),
            manifest_digest: manifest.manifest_digest,
            source: source(),
            target: target(),
            max_bytes: DecimalU64::new(1),
            deadline_unix_ms: UnixMillis::new(2_000),
            capability: COMMIT_MATERIALIZATION_CAPABILITY_V2.to_owned(),
        };
        let payload = ticket.payload_bytes().unwrap();
        let central_signature = CentralSignedPayload {
            key_id: "central-key".to_owned(),
            certificate_generation: crate::CertificateGeneration::new(1),
            signed_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: ticket.deadline_unix_ms,
            payload_digest: ContentDigest::hash(&payload),
            payload: crate::GatewayOpaqueBytes::new(payload).unwrap(),
            signature: crate::Ed25519Signature::from_bytes([0; 64]),
            extensions: crate::Extensions::new(),
        };
        let signed_ticket =
            SignedMaterializationBatchTicket::new(ticket, central_signature).unwrap();
        let batch = MaterializationBatch {
            batch_id,
            materialization_id,
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            source: source(),
            target: target(),
            manifest_digest: manifest.manifest_digest,
            object_ids: Vec::new(),
            object_count: DecimalU64::new(0),
            total_bytes: DecimalU64::new(0),
            state: MaterializationBatchState::Queued,
            max_bytes: DecimalU64::new(1),
            deadline_unix_ms: UnixMillis::new(2_000),
        };
        MaterializationAssignment {
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            signed_ticket,
            batch,
            manifest,
            pages,
            extensions: crate::Extensions::new(),
        }
    }

    #[test]
    fn namespace_is_explicit_and_artifact_mapping_is_infallible() {
        let artifact = ArtifactId::new("artifact-a").unwrap();
        let namespace: ObjectNamespaceId = artifact.clone().into();
        assert_eq!(namespace.as_str(), artifact.as_str());
        assert_ne!(namespace, ObjectNamespaceId::new("artifact-b").unwrap());
    }

    #[test]
    fn namespace_object_set_keeps_commit_digest_semantics() {
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let object_set = ObjectSet::new(vec![CommitObject::new(
            ObjectId::from_bytes([1; 32]),
            4,
            ObjectEncoding::Raw,
            0,
        )])
        .unwrap();
        let namespaced = NamespaceObjectSet::from_object_set(
            TenantId::new("tenant-a").unwrap(),
            namespace,
            CommitId::from_bytes([9; 32]),
            &object_set,
        )
        .unwrap();
        assert_eq!(namespaced.object_count(), 1);
        assert_eq!(namespaced.total_bytes().unwrap(), 4);
        assert!(
            serde_json::from_value::<NamespaceObjectSet>(serde_json::json!({
                "tenant_id": "tenant-a",
                "object_namespace_id": "artifact-a",
                "commit_id": "0909090909090909090909090909090909090909090909090909090909090909",
                "object_set_digest": namespaced.object_set_digest,
                "objects": namespaced.objects,
                "unknown": true
            }))
            .is_err()
        );
    }

    #[test]
    fn coverage_is_recomputed_from_verified_object_evidence() {
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let object_set = ObjectSet::new(vec![
            CommitObject::new(ObjectId::from_bytes([1; 32]), 4, ObjectEncoding::Raw, 0),
            CommitObject::new(ObjectId::from_bytes([2; 32]), 4, ObjectEncoding::Raw, 1),
        ])
        .unwrap();
        let mut placements = Vec::new();
        for (byte, id) in [(1, "placement-a"), (2, "placement-b")] {
            placements.push(ObjectPlacement {
                placement_id: PlacementId::new(id).unwrap(),
                tenant_id: TenantId::new("tenant-a").unwrap(),
                object_namespace_id: namespace.clone(),
                object_id: ObjectId::from_bytes([byte; 32]),
                size: DecimalU64::new(4),
                encoding: ObjectEncoding::Raw,
                verified_digest: ObjectId::from_bytes([byte; 32]).digest(),
                storage_volume_id: Some(StorageVolumeId::new("volume-a").unwrap()),
                archive_id: None,
                placement_generation: PlacementGeneration::new(1),
                state: ObjectPlacementState::Verified,
                failure_domain: "host-a".to_owned(),
            });
        }
        let coverage = recompute_coverage(
            TenantId::new("tenant-a").unwrap(),
            namespace,
            CommitId::from_bytes([9; 32]),
            StorageVolumeId::new("volume-a").unwrap(),
            PlacementGeneration::new(1),
            &object_set,
            &placements,
        )
        .unwrap();
        assert_eq!(coverage.state, CoverageState::Complete);
        assert_eq!(coverage.verified_object_count.get(), 2);
    }

    #[test]
    fn paged_manifest_digest_and_ticket_are_strict() {
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let (manifest, pages) = BatchManifest::paginate(
            MaterializationId::new("materialization-a").unwrap(),
            MaterializationBatchId::new("batch-a").unwrap(),
            Generation::new(1),
            Generation::new(1),
            namespace.clone(),
            vec![object(1, 0), object(2, 1)],
            1,
        )
        .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(manifest.object_count.get(), 2);
        assert!(pages.iter().all(|page| page.validate().is_ok()));

        let ticket = MaterializationBatchTicket {
            ticket_id: ObjectTicketId::new("ticket-a").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            object_namespace_id: namespace,
            commit_id: CommitId::from_bytes([9; 32]),
            manifest_digest: manifest.manifest_digest,
            source: source(),
            target: target(),
            max_bytes: DecimalU64::new(8),
            deadline_unix_ms: UnixMillis::new(2_000),
            capability: COMMIT_MATERIALIZATION_CAPABILITY_V2.to_owned(),
        };
        ticket.validate().unwrap();
        ticket.validate_against_manifest(&manifest).unwrap();
        manifest.validate_against_pages(&pages).unwrap();
        assert!(!ticket.payload_bytes().unwrap().is_empty());

        let mut wrong_manifest = manifest.clone();
        wrong_manifest.batch_id = MaterializationBatchId::new("batch-other").unwrap();
        assert!(ticket.validate_against_manifest(&wrong_manifest).is_err());
        wrong_manifest = manifest.clone();
        wrong_manifest.manifest_digest = ContentDigest::from_bytes([0; 32]);
        assert!(wrong_manifest.validate_against_pages(&pages).is_err());
    }

    #[test]
    fn signed_ticket_requires_signature_expiry_to_match_deadline() {
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let (manifest, _pages) = BatchManifest::paginate(
            MaterializationId::new("materialization-a").unwrap(),
            MaterializationBatchId::new("batch-a").unwrap(),
            Generation::new(1),
            Generation::new(1),
            namespace.clone(),
            vec![object(1, 0)],
            1,
        )
        .unwrap();
        let ticket = MaterializationBatchTicket {
            ticket_id: ObjectTicketId::new("ticket-a").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            object_namespace_id: namespace,
            commit_id: CommitId::from_bytes([9; 32]),
            manifest_digest: manifest.manifest_digest,
            source: source(),
            target: target(),
            max_bytes: DecimalU64::new(4),
            deadline_unix_ms: UnixMillis::new(2_000),
            capability: COMMIT_MATERIALIZATION_CAPABILITY_V2.to_owned(),
        };
        let payload = ticket.payload_bytes().unwrap();
        let central_signature = CentralSignedPayload {
            key_id: "central-key".to_owned(),
            certificate_generation: crate::CertificateGeneration::new(1),
            signed_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: ticket.deadline_unix_ms,
            payload_digest: ContentDigest::hash(&payload),
            payload: crate::GatewayOpaqueBytes::new(payload).unwrap(),
            signature: crate::Ed25519Signature::from_bytes([0; 64]),
            extensions: crate::Extensions::new(),
        };
        let signed = SignedMaterializationBatchTicket::new(ticket, central_signature).unwrap();
        assert!(signed.validate().is_ok());

        let mut forged = signed.clone();
        forged.central_signature.expires_at_unix_ms = UnixMillis::new(2_001);
        assert!(forged.validate().is_err());
        let mut forged = signed;
        forged.ticket.deadline_unix_ms = UnixMillis::new(2_001);
        assert!(forged.validate().is_err());
    }

    #[test]
    fn ticket_signature_binds_operation_task_identity() {
        let signed = {
            let assignment = empty_assignment();
            assignment.signed_ticket
        };
        let mut forged = signed;
        forged.ticket.operation_task_id = TaskId::new("task-other").unwrap();
        assert!(forged.validate().is_err());

        let mut forged = empty_assignment().signed_ticket;
        forged.ticket.task_attempt_id = TaskAttemptId::new("task-other-attempt-1").unwrap();
        assert!(forged.validate().is_err());
    }

    #[test]
    fn assignment_and_reports_fence_operation_task_identity() {
        let assignment = empty_assignment();
        assignment.validate().unwrap();

        let mut forged_assignment = assignment.clone();
        forged_assignment.operation_task_id = TaskId::new("task-other").unwrap();
        assert!(forged_assignment.validate().is_err());
        let mut forged_assignment = assignment.clone();
        forged_assignment.task_attempt_id = TaskAttemptId::new("task-other-attempt-1").unwrap();
        assert!(forged_assignment.validate().is_err());

        let base = MaterializationReport::Failed {
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: assignment.batch.materialization_id.clone(),
            batch_id: assignment.batch.batch_id.clone(),
            plan_revision: assignment.batch.plan_revision,
            batch_attempt: assignment.batch.batch_attempt,
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
            target: target(),
            object_id: None,
            issue_code: "SOURCE_UNAVAILABLE".to_owned(),
            issue_message: "source route unavailable".to_owned(),
            extensions: crate::Extensions::new(),
        };
        base.validate_for_assignment(&assignment).unwrap();

        let mut forged_report = base.clone();
        if let MaterializationReport::Failed {
            operation_task_id, ..
        } = &mut forged_report
        {
            *operation_task_id = TaskId::new("task-other").unwrap();
        }
        assert!(forged_report.validate_for_assignment(&assignment).is_err());

        let mut forged_report = base;
        if let MaterializationReport::Failed {
            task_attempt_id, ..
        } = &mut forged_report
        {
            *task_attempt_id = TaskAttemptId::new("task-other-attempt-1").unwrap();
        }
        assert!(forged_report.validate_for_assignment(&assignment).is_err());
    }

    #[test]
    fn materialization_report_carries_and_validates_target_fence() {
        let object = object(1, 0);
        let receipt = MaterializationObjectReceipt {
            receipt_id: crate::ObjectReceiptId::new("receipt-report").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: object.object_namespace_id.clone(),
            object_id: object.object_id,
            size: object.size,
            encoding: object.encoding,
            verified_digest: object.object_id.digest(),
            target_storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
            target_placement_generation: PlacementGeneration::new(1),
            committed_offset: object.size,
            verified_at_unix_ms: UnixMillis::new(2),
        };
        let report = MaterializationReport::Receipt {
            receipt,
            target: target(),
            extensions: crate::Extensions::new(),
        };
        report.validate().unwrap();

        let mut inconsistent = report;
        if let MaterializationReport::Receipt { target, .. } = &mut inconsistent {
            target.storage_volume_id = StorageVolumeId::new("volume-other").unwrap();
        }
        assert!(inconsistent.validate().is_err());
    }

    #[test]
    fn manifest_rejects_duplicate_ordinals_across_pages() {
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let materialization_id = MaterializationId::new("materialization-a").unwrap();
        let batch_id = MaterializationBatchId::new("batch-a").unwrap();
        let first = BatchManifestPage::new(
            materialization_id.clone(),
            batch_id.clone(),
            Generation::new(1),
            Generation::new(1),
            namespace.clone(),
            0,
            2,
            vec![object(1, 0)],
        )
        .unwrap();
        let second = BatchManifestPage::new(
            materialization_id,
            batch_id,
            Generation::new(1),
            Generation::new(1),
            namespace,
            1,
            2,
            vec![object(2, 0)],
        )
        .unwrap();
        assert!(BatchManifest::from_pages(&[first, second]).is_err());
    }

    #[test]
    fn manifest_rejects_out_of_order_pages_instead_of_normalizing_them() {
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let materialization_id = MaterializationId::new("materialization-a").unwrap();
        let batch_id = MaterializationBatchId::new("batch-a").unwrap();
        let first = BatchManifestPage::new(
            materialization_id.clone(),
            batch_id.clone(),
            Generation::new(1),
            Generation::new(1),
            namespace.clone(),
            0,
            2,
            vec![object(1, 0)],
        )
        .unwrap();
        let second = BatchManifestPage::new(
            materialization_id,
            batch_id,
            Generation::new(1),
            Generation::new(1),
            namespace,
            1,
            2,
            vec![object(2, 1)],
        )
        .unwrap();
        assert!(BatchManifest::from_pages(&[second, first]).is_err());
    }

    #[test]
    fn staging_key_cannot_be_retargeted() {
        let mut task = MaterializationObject::new(
            MaterializationId::new("materialization-a").unwrap(),
            object(1, 0),
            Generation::new(1),
        );
        task.validate().unwrap();
        task.staging_key.push_str("-other");
        assert!(task.validate().is_err());
    }

    #[test]
    fn placement_matching_checks_all_object_identity_fields() {
        let placement = ObjectPlacement {
            placement_id: PlacementId::new("placement-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
            object_id: ObjectId::from_bytes([1; 32]),
            size: DecimalU64::new(4),
            encoding: ObjectEncoding::Raw,
            verified_digest: ObjectId::from_bytes([1; 32]).digest(),
            storage_volume_id: Some(StorageVolumeId::new("volume-a").unwrap()),
            archive_id: None,
            placement_generation: PlacementGeneration::new(1),
            state: ObjectPlacementState::Verified,
            failure_domain: "host-a".to_owned(),
        };
        placement.validate_against(&object(1, 0)).unwrap();
        let mut wrong_size = object(1, 0);
        wrong_size.size = DecimalU64::new(5);
        assert!(placement.validate_against(&wrong_size).is_err());
        let mut wrong_namespace = object(1, 0);
        wrong_namespace.object_namespace_id = ObjectNamespaceId::new("artifact-b").unwrap();
        assert!(placement.validate_against(&wrong_namespace).is_err());
    }

    #[test]
    fn coverage_state_cannot_claim_partial_after_all_objects_are_verified() {
        let coverage = VolumeCommitCoverage {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
            commit_id: CommitId::from_bytes([9; 32]),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            object_set_digest: ContentDigest::from_bytes([8; 32]),
            object_count: DecimalU64::new(1),
            verified_object_count: DecimalU64::new(1),
            total_bytes: DecimalU64::new(4),
            verified_bytes: DecimalU64::new(4),
            state: CoverageState::Partial,
        };
        assert!(coverage.validate().is_err());
    }

    #[test]
    fn durability_policy_rejects_impossible_domain_requirement_and_bad_evidence() {
        assert!(DurabilityPolicy::new(1, 2).validate().is_err());
        let mut invalid_placement = ObjectPlacement {
            placement_id: PlacementId::new("placement-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
            object_id: ObjectId::from_bytes([1; 32]),
            size: DecimalU64::new(4),
            encoding: ObjectEncoding::Raw,
            verified_digest: ObjectId::from_bytes([1; 32]).digest(),
            storage_volume_id: Some(StorageVolumeId::new("volume-a").unwrap()),
            archive_id: None,
            placement_generation: PlacementGeneration::new(1),
            state: ObjectPlacementState::Verified,
            failure_domain: "host-a".to_owned(),
        };
        invalid_placement.verified_digest = ContentDigest::from_bytes([0; 32]);
        assert!(!DurabilityPolicy::default().satisfied_by([&invalid_placement]));
        invalid_placement.verified_digest = invalid_placement.object_id.digest();
        let two_copy_policy = DurabilityPolicy::new(2, 1);
        assert!(!two_copy_policy.satisfied_by([&invalid_placement, &invalid_placement]));
    }

    #[test]
    fn coverage_goals_have_threshold_semantics() {
        let object_goal = CoverageGoal::ObjectCount(DecimalU64::new(2));
        assert!(object_goal.satisfied_by(2, 4, 3, 10));
        assert!(!object_goal.satisfied_by(1, 10, 3, 10));
        assert!(object_goal.validate_against_totals(3, 10).is_ok());
        assert!(object_goal.validate_against_totals(1, 10).is_err());

        let byte_goal = CoverageGoal::ByteCount(DecimalU64::new(8));
        assert!(byte_goal.satisfied_by(1, 8, 3, 10));
        assert!(!byte_goal.satisfied_by(3, 7, 3, 10));
        assert!(CoverageGoal::Complete.satisfied_by(3, 10, 3, 10));
        assert!(!CoverageGoal::Complete.satisfied_by(3, 9, 3, 10));
    }

    #[test]
    fn threshold_complete_job_may_keep_partial_commit_counters() {
        let mut job = MaterializationJob {
            materialization_id: MaterializationId::new("materialization-threshold").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            key: MaterializationJobKey {
                tenant_id: TenantId::new("tenant-a").unwrap(),
                object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
                commit_id: CommitId::from_bytes([9; 32]),
                target_storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
                coverage_goal: CoverageGoal::ObjectCount(DecimalU64::new(1)),
            },
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            state: MaterializationJobState::Complete,
            plan_revision: Generation::new(1),
            object_count: DecimalU64::new(2),
            total_bytes: DecimalU64::new(10),
            verified_object_count: DecimalU64::new(1),
            verified_bytes: DecimalU64::new(4),
            missing_object_count: DecimalU64::new(1),
            missing_bytes: DecimalU64::new(6),
            source_count: DecimalU64::new(1),
            created_at_unix_ms: UnixMillis::new(1),
            updated_at_unix_ms: UnixMillis::new(1),
            deadline_unix_ms: UnixMillis::new(2),
            issue: None,
        };
        job.validate().unwrap();
        let mut forged = job.clone();
        forged.missing_object_count = DecimalU64::new(0);
        assert!(forged.validate().is_err());
        job.key.coverage_goal = CoverageGoal::Complete;
        assert!(job.validate().is_err());
    }

    #[test]
    fn job_and_batch_fence_namespace_and_target_identity() {
        let mut job = MaterializationJob {
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            key: MaterializationJobKey {
                tenant_id: TenantId::new("tenant-a").unwrap(),
                object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
                commit_id: CommitId::from_bytes([9; 32]),
                target_storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
                coverage_goal: CoverageGoal::Complete,
            },
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            state: MaterializationJobState::Queued,
            plan_revision: Generation::new(1),
            object_count: DecimalU64::new(1),
            total_bytes: DecimalU64::new(4),
            verified_object_count: DecimalU64::new(0),
            verified_bytes: DecimalU64::new(0),
            missing_object_count: DecimalU64::new(1),
            missing_bytes: DecimalU64::new(4),
            source_count: DecimalU64::new(0),
            created_at_unix_ms: UnixMillis::new(1),
            updated_at_unix_ms: UnixMillis::new(1),
            deadline_unix_ms: UnixMillis::new(2),
            issue: None,
        };
        job.validate().unwrap();
        job.artifact_id = ArtifactId::new("artifact-b").unwrap();
        assert!(job.validate().is_err());

        let mut batch = MaterializationBatch {
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            source: source(),
            target: target(),
            manifest_digest: ContentDigest::from_bytes([7; 32]),
            object_ids: vec![ObjectId::from_bytes([1; 32])],
            object_count: DecimalU64::new(1),
            total_bytes: DecimalU64::new(4),
            state: MaterializationBatchState::Queued,
            max_bytes: DecimalU64::new(4),
            deadline_unix_ms: UnixMillis::new(2),
        };
        batch.validate().unwrap();
        batch.target.storage_volume_id = StorageVolumeId::new("volume-a").unwrap();
        assert!(batch.validate().is_err());
    }

    #[test]
    fn completed_object_requires_a_full_checkpoint() {
        let mut task = MaterializationObject::new(
            MaterializationId::new("materialization-a").unwrap(),
            object(1, 0),
            Generation::new(1),
        );
        task.state = MaterializationObjectState::Published;
        assert!(task.validate().is_err());
        task.confirmed_offset = task.object.size;
        task.validate().unwrap();
    }

    #[test]
    fn receipt_requires_content_digest_and_durable_barrier() {
        let object = object(1, 0);
        let mut receipt = MaterializationObjectReceipt {
            receipt_id: crate::ObjectReceiptId::new("receipt-a").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: object.object_namespace_id.clone(),
            object_id: object.object_id,
            size: object.size,
            encoding: object.encoding,
            verified_digest: object.object_id.digest(),
            target_storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
            target_placement_generation: PlacementGeneration::new(1),
            committed_offset: object.size,
            verified_at_unix_ms: UnixMillis::new(2),
        };
        receipt.validate_against(&object).unwrap();
        receipt.verified_digest = ContentDigest::from_bytes([0; 32]);
        assert!(receipt.validate().is_err());
        receipt.verified_digest = object.object_id.digest();
        receipt.committed_offset = DecimalU64::new(0);
        assert!(receipt.validate().is_err());
    }

    #[test]
    fn receipt_verification_must_precede_ticket_deadline() {
        let object = object(1, 0);
        let mut receipt = MaterializationObjectReceipt {
            receipt_id: crate::ObjectReceiptId::new("receipt-deadline").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: object.object_namespace_id.clone(),
            object_id: object.object_id,
            size: object.size,
            encoding: object.encoding,
            verified_digest: object.object_id.digest(),
            target_storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
            target_placement_generation: PlacementGeneration::new(1),
            committed_offset: object.size,
            verified_at_unix_ms: UnixMillis::new(2_000),
        };
        let mut ticket = MaterializationBatchTicket {
            ticket_id: ObjectTicketId::new("ticket-deadline").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            object_namespace_id: ObjectNamespaceId::new("artifact-a").unwrap(),
            commit_id: CommitId::from_bytes([9; 32]),
            manifest_digest: ContentDigest::from_bytes([7; 32]),
            source: source(),
            target: target(),
            max_bytes: DecimalU64::new(4),
            deadline_unix_ms: UnixMillis::new(2_001),
            capability: COMMIT_MATERIALIZATION_CAPABILITY_V2.to_owned(),
        };
        receipt.validate_against_ticket(&ticket, &object).unwrap();
        receipt.verified_at_unix_ms = UnixMillis::new(2_001);
        assert!(receipt.validate_against_ticket(&ticket, &object).is_err());
        receipt.verified_at_unix_ms = UnixMillis::new(2_000);
        ticket.deadline_unix_ms = UnixMillis::new(1_999);
        assert!(receipt.validate_against_ticket(&ticket, &object).is_err());
    }

    #[test]
    fn materialization_placement_identity_is_receipt_independent_and_target_scoped() {
        let object = object(1, 0);
        let receipt = MaterializationObjectReceipt {
            receipt_id: crate::ObjectReceiptId::new("receipt-a").unwrap(),
            operation_task_id: operation_task_id(),
            task_attempt_id: task_attempt_id(),
            materialization_id: MaterializationId::new("materialization-a").unwrap(),
            batch_id: MaterializationBatchId::new("batch-a").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            object_namespace_id: object.object_namespace_id.clone(),
            object_id: object.object_id,
            size: object.size,
            encoding: object.encoding,
            verified_digest: object.object_id.digest(),
            target_storage_volume_id: StorageVolumeId::new("volume-b").unwrap(),
            target_placement_generation: PlacementGeneration::new(1),
            committed_offset: object.size,
            verified_at_unix_ms: UnixMillis::new(2),
        };
        let first = materialization_target_placement_id(&receipt).unwrap();

        let mut replay = receipt.clone();
        replay.receipt_id = crate::ObjectReceiptId::new("receipt-b").unwrap();
        replay.materialization_id = MaterializationId::new("materialization-b").unwrap();
        replay.batch_id = MaterializationBatchId::new("batch-b").unwrap();
        assert_eq!(first, materialization_target_placement_id(&replay).unwrap());

        let mut different_target = receipt;
        different_target.target_storage_volume_id = StorageVolumeId::new("volume-c").unwrap();
        assert_ne!(
            first,
            materialization_target_placement_id(&different_target).unwrap()
        );
    }

    #[test]
    fn state_transitions_are_explicit_and_idempotent() {
        assert!(
            MaterializationJobState::Queued.can_transition_to(MaterializationJobState::Planning)
        );
        assert!(
            MaterializationJobState::Failed.can_transition_to(MaterializationJobState::Planning)
        );
        assert!(
            !MaterializationJobState::Queued.can_transition_to(MaterializationJobState::Complete)
        );
        assert!(MaterializationBatchState::Transferring
            .can_transition_to(MaterializationBatchState::Verifying));
        assert!(!MaterializationBatchState::Succeeded
            .can_transition_to(MaterializationBatchState::Queued));
        assert!(MaterializationObjectState::Published
            .can_transition_to(MaterializationObjectState::Published));
        assert!(!MaterializationObjectState::Published
            .can_transition_to(MaterializationObjectState::Transferring));
    }
}
